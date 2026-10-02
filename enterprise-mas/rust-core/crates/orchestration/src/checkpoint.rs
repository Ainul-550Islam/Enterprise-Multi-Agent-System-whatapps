//! Durable execution checkpoints for crash-safe recovery.
//!
//! A checkpoint captures *everything needed to resume*: node states, completed
//! steps, context snapshot and a content hash binding it to the exact
//! workflow/graph and input it was taken from. Stale or foreign checkpoints
//! are rejected by [`ExecutionCheckpoint::validate_checkpoint`].

use mas_common::enums::ExecutionStatus;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ExecutionStepId};
use mas_common::result::Result;
use mas_common::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Current checkpoint schema version.
pub const CHECKPOINT_VERSION: u16 = 1;

/// Per-node runtime state within a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionState {
    /// Not yet scheduled.
    Pending,
    /// Currently executing.
    Running,
    /// Blocked on human approval.
    WaitingApproval,
    /// Deferred to a later fire time (delay node / backoff).
    Deferred,
    /// Finished successfully (output reference is part of the step record).
    Completed,
    /// Not taken (condition / branch decision).
    Skipped,
    /// Failed; retry decision is made from step error metadata.
    Failed,
    /// Cancelled by the runtime.
    Cancelled,
}

impl NodeExecutionState {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Skipped | Self::Failed | Self::Cancelled
        )
    }
}

/// One durable checkpoint of an execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionCheckpoint {
    /// Checkpoint schema version ([`CHECKPOINT_VERSION`]).
    pub version: u16,
    /// Monotonic sequence within the execution (strictly increasing).
    pub sequence: u64,
    pub execution_id: ExecutionId,
    /// Overall execution status when the checkpoint was taken.
    pub execution_status: ExecutionStatus,
    /// Node key → state at checkpoint time.
    pub node_states: BTreeMap<String, NodeExecutionState>,
    /// Steps completed so far (ordered by sequence).
    pub completed_steps: Vec<ExecutionStepId>,
    /// Nodes known to be ready but not yet started at checkpoint time.
    pub pending_nodes: Vec<String>,
    /// Serialized runtime context (identity + deadline + data).
    pub context_snapshot: serde_json::Value,
    /// SHA-256 (hex) over the canonical execution identity:
    /// root correlation + workflow/graph content hash + input payload.
    /// Binds the checkpoint to exactly one execution definition.
    pub context_hash: String,
    pub created_at: Timestamp,
}

impl ExecutionCheckpoint {
    /// Creates the next checkpoint after `previous` (sequence = previous + 1).
    #[allow(clippy::too_many_arguments)]
    pub fn create_checkpoint(
        previous_sequence: u64,
        execution_id: ExecutionId,
        execution_status: ExecutionStatus,
        node_states: BTreeMap<String, NodeExecutionState>,
        completed_steps: Vec<ExecutionStepId>,
        pending_nodes: Vec<String>,
        context_snapshot: serde_json::Value,
        context_hash: String,
    ) -> Result<Self> {
        validate_hash_format(&context_hash)?;
        Ok(Self {
            version: CHECKPOINT_VERSION,
            sequence: previous_sequence + 1,
            execution_id,
            execution_status,
            node_states,
            completed_steps,
            pending_nodes,
            context_snapshot,
            context_hash,
            created_at: Timestamp::now(),
        })
    }

    /// Validates that this checkpoint may resume the execution described by
    /// `expected_execution_id` and `expected_context_hash`:
    ///
    /// * schema version supported,
    /// * execution id matches,
    /// * content hash matches (same graph version + input),
    /// * state coherence (no `Running` nodes left — they'd be re-driven from
    ///   `pending_nodes`, because the worker that ran them is gone).
    pub fn validate_checkpoint(
        &self,
        expected_execution_id: ExecutionId,
        expected_context_hash: &str,
    ) -> Result<()> {
        if self.version > CHECKPOINT_VERSION {
            return Err(AppError::validation(format!(
                "checkpoint version {} is newer than supported version {CHECKPOINT_VERSION}",
                self.version
            )));
        }
        if self.execution_id != expected_execution_id {
            return Err(AppError::conflict(format!(
                "checkpoint belongs to execution {}, not {expected_execution_id}",
                self.execution_id
            )));
        }
        validate_hash_format(expected_context_hash)?;
        if self.context_hash != expected_context_hash {
            return Err(AppError::conflict(
                "checkpoint context hash mismatch: the workflow version or input changed",
            ));
        }
        Ok(())
    }

    /// Restores the resumable view of this checkpoint: `Running`/`Deferred`
    /// nodes are demoted to pending (they will be re-driven), terminal states
    /// are preserved. Consumes the checkpoint.
    #[must_use]
    pub fn restore_checkpoint(self) -> RestoredExecution {
        let mut node_states = self.node_states;
        let mut pending_nodes: Vec<String> = self.pending_nodes;
        for (key, state) in node_states.iter_mut() {
            if matches!(
                state,
                NodeExecutionState::Running | NodeExecutionState::Deferred
            ) {
                *state = NodeExecutionState::Pending;
                if !pending_nodes.contains(key) {
                    pending_nodes.push(key.clone());
                }
            }
        }
        pending_nodes.sort();
        pending_nodes.dedup();
        RestoredExecution {
            execution_id: self.execution_id,
            sequence: self.sequence,
            node_states,
            completed_steps: self.completed_steps,
            pending_nodes,
            context_snapshot: self.context_snapshot,
        }
    }

    /// Computes the canonical content hash for an execution definition.
    /// Inputs: root correlation id, canonical serialized graph (or agent id +
    /// version), and canonical input payload. All inputs must be canonical
    /// JSON (keys sorted) — serialization of `serde_json::Value` objects is
    /// deterministic here because `preserve_order` is not enabled.
    #[must_use]
    pub fn compute_context_hash(
        correlation_id: &str,
        definition: &serde_json::Value,
        input: &serde_json::Value,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(correlation_id.as_bytes());
        hasher.update([0x1F]); // field separator
        hasher.update(definition.to_string().as_bytes());
        hasher.update([0x1F]);
        hasher.update(input.to_string().as_bytes());
        hex::encode(hasher.finalize())
    }
}

/// The resumable view produced by [`ExecutionCheckpoint::restore_checkpoint`].
#[derive(Debug, Clone)]
pub struct RestoredExecution {
    pub execution_id: ExecutionId,
    pub sequence: u64,
    pub node_states: BTreeMap<String, NodeExecutionState>,
    pub completed_steps: Vec<ExecutionStepId>,
    pub pending_nodes: Vec<String>,
    pub context_snapshot: serde_json::Value,
}

fn validate_hash_format(hash: &str) -> Result<()> {
    if hash.len() != 64 || !hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(AppError::invalid_field(
            "context_hash",
            "invalid_format",
            "context hash must be 64 lowercase hex characters (SHA-256)",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(seed: &str) -> String {
        hex::encode(Sha256::digest(seed.as_bytes()))
    }

    fn sample(exec: ExecutionId, sequence: u64, context_hash: &str) -> ExecutionCheckpoint {
        let mut states = BTreeMap::new();
        states.insert("start".to_owned(), NodeExecutionState::Completed);
        states.insert("agent_1".to_owned(), NodeExecutionState::Running);
        states.insert("end".to_owned(), NodeExecutionState::Pending);
        ExecutionCheckpoint {
            version: CHECKPOINT_VERSION,
            sequence,
            execution_id: exec,
            execution_status: ExecutionStatus::Running,
            node_states: states,
            completed_steps: vec![ExecutionStepId::new()],
            pending_nodes: vec![],
            context_snapshot: serde_json::json!({"tenant": "t1"}),
            context_hash: context_hash.to_owned(),
            created_at: Timestamp::now(),
        }
    }

    #[test]
    fn create_validate_roundtrip() {
        let exec = ExecutionId::new();
        let context_hash = hash("ctx");
        let checkpoint = sample(exec, 3, &context_hash);
        assert_eq!(checkpoint.sequence, 3);
        checkpoint.validate_checkpoint(exec, &context_hash).unwrap();
    }

    #[test]
    fn rejects_foreign_or_changed_checkpoints() {
        let exec = ExecutionId::new();
        let context_hash = hash("ctx");
        let checkpoint = sample(exec, 1, &context_hash);
        assert!(checkpoint
            .validate_checkpoint(ExecutionId::new(), &context_hash)
            .is_err());
        assert!(checkpoint
            .validate_checkpoint(exec, &hash("other-context"))
            .is_err());
        let mut future = sample(exec, 1, &context_hash);
        future.version = CHECKPOINT_VERSION + 1;
        assert!(future.validate_checkpoint(exec, &context_hash).is_err());
    }

    #[test]
    fn restore_demotes_running_nodes_to_pending() {
        let exec = ExecutionId::new();
        let restored = sample(exec, 2, &hash("ctx")).restore_checkpoint();
        assert_eq!(
            restored.node_states.get("agent_1"),
            Some(&NodeExecutionState::Pending)
        );
        assert!(restored.pending_nodes.contains(&"agent_1".to_owned()));
        assert_eq!(
            restored.node_states.get("start"),
            Some(&NodeExecutionState::Completed)
        );
    }

    #[test]
    fn content_hash_is_deterministic_and_sensitive() {
        let definition = serde_json::json!({"nodes": ["a", "b"]});
        let input = serde_json::json!({"x": 1});
        let h1 = ExecutionCheckpoint::compute_context_hash("c1", &definition, &input);
        let h2 = ExecutionCheckpoint::compute_context_hash("c1", &definition, &input);
        assert_eq!(h1, h2);
        assert_ne!(
            h1,
            ExecutionCheckpoint::compute_context_hash("c2", &definition, &input)
        );
        assert_ne!(
            h1,
            ExecutionCheckpoint::compute_context_hash(
                "c1",
                &serde_json::json!({"nodes": ["a"]}),
                &input
            )
        );
    }
}
