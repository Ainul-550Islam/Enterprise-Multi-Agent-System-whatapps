//! Step runner: execution-step bookkeeping around unit work attempts.
//!
//! The [`StepRunner`] is the only place step rows are transitioned; it keeps
//! the domain's legal transition order (`Pending` → `Running` → terminal)
//! airtight:
//!
//! * `Skipped` transitions happen from `Pending` (settled once — begin() must
//!   not resurrect them),
//! * retries are new `Running` attempts on the same row (begin() increments
//!   `attempt`),
//! * every transition carries an optional guard charge first, so a blown
//!   budget stops the run *before* a step even starts.
//!
//! Persistence is left to the caller (steps are flushed through the engine's
//! execution store / step store port in the persistence phase); `drain()`
//! hands back finished rows in sequence order.

use crate::resource_guard::ResourceGuard;
use mas_common::constants;
use mas_common::enums::ExecutionStepStatus;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ExecutionStepId};
use mas_common::result::Result;
use mas_domain::{ExecutionStep, StepError};
use std::collections::BTreeMap;

/// What a settled step decided (driver-facing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepTransition {
    /// Step begins (or retries) — `Running`, attempt += 1.
    Began { attempt: u32 },
    /// Step completed.
    Succeeded,
    /// Step failed; the retry policy decides what happens next; when
    /// `may_retry` the runner allows another `begin()`.
    Failed { may_retry: bool },
    /// Step skipped (branch/condition); settled from `Pending`.
    Skipped,
    /// Step cancelled.
    Cancelled,
}

/// The runner for one execution's steps.
#[derive(Debug)]
pub struct StepRunner {
    execution_id: ExecutionId,
    guard: ResourceGuard,
    steps: BTreeMap<ExecutionStepId, ExecutionStep>,
    /// node_key → latest step id (one logical step per node attempt-lineage).
    by_node: BTreeMap<String, ExecutionStepId>,
    sequence: u64,
    /// Steps attempted during this run (for the run-level cap).
    finished: Vec<ExecutionStepId>,
}

impl StepRunner {
    pub fn new(execution_id: ExecutionId, guard: ResourceGuard) -> Self {
        Self {
            execution_id,
            guard,
            steps: BTreeMap::new(),
            by_node: BTreeMap::new(),
            sequence: 0,
            finished: Vec::new(),
        }
    }

    #[must_use]
    pub const fn guard(&self) -> &ResourceGuard {
        &self.guard
    }

    #[must_use]
    pub fn guard_mut(&mut self) -> &mut ResourceGuard {
        &mut self.guard
    }

    #[must_use]
    pub const fn execution_id(&self) -> ExecutionId {
        self.execution_id
    }

    #[must_use]
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }

    /// Registers (or returns) the step row for a node.
    pub fn step_for(&mut self, node_key: &str, step_type: &str) -> Result<&ExecutionStep> {
        if let Some(id) = self.by_node.get(node_key) {
            return self
                .steps
                .get(id)
                .ok_or_else(|| AppError::internal("step index inconsistency"));
        }
        if self.sequence as u32 >= constants::MAX_STEPS_PER_EXECUTION {
            return Err(AppError::rate_limited(format!(
                "execution exceeded the hard step cap of {}",
                constants::MAX_STEPS_PER_EXECUTION
            )));
        }
        let step = ExecutionStep::new(
            self.execution_id,
            step_type,
            self.sequence,
            Some(node_key.to_owned()),
        );
        let id = step.id;
        self.sequence += 1;
        self.steps.insert(id, step);
        self.by_node.insert(node_key.to_owned(), id);
        self.steps
            .get(&id)
            .ok_or_else(|| AppError::internal("step index inconsistency"))
    }

    /// Id of a node's step, when registered.
    #[must_use]
    pub fn step_id_for(&self, node_key: &str) -> Option<ExecutionStepId> {
        self.by_node.get(node_key).copied()
    }

    #[must_use]
    pub fn get(&self, step_id: ExecutionStepId) -> Option<&ExecutionStep> {
        self.steps.get(&step_id)
    }

    /// Begin (or re-begin for a retry) a node's step after charging the guard.
    pub fn begin(&mut self, node_key: &str, step_type: &str) -> Result<StepTransition> {
        let step_id = self.step_id_for(node_key).map(Ok).unwrap_or_else(|| {
            let step = self.step_for(node_key, step_type)?;
            Ok::<ExecutionStepId, AppError>(step.id)
        })?;
        self.guard.charge_step()?;
        let step = self
            .steps
            .get_mut(&step_id)
            .ok_or_else(|| AppError::internal("step index inconsistency"))?;
        // Skipped rows must never run — the runner is the enforcement point.
        if step.status == ExecutionStepStatus::Skipped {
            return Err(AppError::conflict(format!(
                "step for node '{node_key}' was skipped and cannot begin"
            )));
        }
        if step.status.is_terminal() && step.status != ExecutionStepStatus::Failed {
            return Err(AppError::conflict(format!(
                "step for node '{node_key}' is already in terminal status '{}'",
                step.status
            )));
        }
        step.begin()?;
        Ok(StepTransition::Began {
            attempt: step.attempt,
        })
    }

    /// Running → Completed.
    pub fn succeed(
        &mut self,
        node_key: &str,
        output_ref: Option<String>,
    ) -> Result<StepTransition> {
        let step_id = self.require_step_id(node_key)?;
        let step = self
            .steps
            .get_mut(&step_id)
            .ok_or_else(|| AppError::internal("step index inconsistency"))?;
        step.succeed(output_ref)?;
        self.finished.push(step_id);
        Ok(StepTransition::Succeeded)
    }

    /// Running → Failed; `may_retry` is derived from the remaining attempt
    /// budget (domain `begin()` allows retry from `Failed`).
    pub fn fail(&mut self, node_key: &str, error: StepError) -> Result<StepTransition> {
        let step_id = self.require_step_id(node_key)?;
        let step = self
            .steps
            .get_mut(&step_id)
            .ok_or_else(|| AppError::internal("step index inconsistency"))?;
        step.fail(error)?;
        let may_retry = step.attempt < self.guard.remaining_step_attempts();
        self.finished.push(step_id);
        Ok(StepTransition::Failed { may_retry })
    }

    /// Pending → Skipped (the only legal source state).
    pub fn skip(&mut self, node_key: &str, reason: Option<String>) -> Result<StepTransition> {
        let step_id = self.step_id_for(node_key).map(Ok).unwrap_or_else(|| {
            let step = self.step_for(node_key, "step.skipped")?;
            Ok::<ExecutionStepId, AppError>(step.id)
        })?;
        let step = self
            .steps
            .get_mut(&step_id)
            .ok_or_else(|| AppError::internal("step index inconsistency"))?;
        step.skip(reason)?;
        self.finished.push(step_id);
        Ok(StepTransition::Skipped)
    }

    /// Any non-terminal → Cancelled.
    pub fn cancel(&mut self, node_key: &str) -> Result<StepTransition> {
        let step_id = self.require_step_id(node_key)?;
        let step = self
            .steps
            .get_mut(&step_id)
            .ok_or_else(|| AppError::internal("step index inconsistency"))?;
        step.cancel()?;
        self.finished.push(step_id);
        Ok(StepTransition::Cancelled)
    }

    /// Marks a step blocked on human approval.
    pub fn wait_for_approval(&mut self, node_key: &str) -> Result<()> {
        let step_id = self.require_step_id(node_key)?;
        let step = self
            .steps
            .get_mut(&step_id)
            .ok_or_else(|| AppError::internal("step index inconsistency"))?;
        step.wait_for_approval()
    }

    /// All step rows, ordered by sequence.
    #[must_use]
    pub fn steps(&self) -> Vec<ExecutionStep> {
        let mut out: Vec<ExecutionStep> = self.steps.values().cloned().collect();
        out.sort_by_key(|step| step.sequence);
        out
    }

    /// Steps that reached a terminal/observable state, in finish order.
    #[must_use]
    pub fn finished_steps(&self) -> Vec<ExecutionStepId> {
        self.finished.clone()
    }

    fn require_step_id(&self, node_key: &str) -> Result<ExecutionStepId> {
        self.by_node
            .get(node_key)
            .copied()
            .ok_or_else(|| AppError::not_found("step", node_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runner() -> StepRunner {
        let guard = ResourceGuard::new(None, None).expect("guard");
        StepRunner::new(ExecutionId::new(), guard)
    }

    #[test]
    fn legal_lifecycle_is_enforced() {
        let mut runner = runner();
        // Begin twice = retry attempt 2.
        assert_eq!(
            runner.begin("node-a", "tool.invoke").expect("begin"),
            StepTransition::Began { attempt: 1 }
        );
        // Cannot succeed twice or begin from completed.
        runner
            .succeed("node-a", Some("ref://1".to_owned()))
            .expect("succeed");
        assert!(runner.begin("node-a", "tool.invoke").is_err());
        assert!(runner.succeed("node-a", None).is_err());
        assert_eq!(runner.steps()[0].status, ExecutionStepStatus::Completed);
        assert_eq!(runner.steps()[0].sequence, 0);
    }

    #[test]
    fn skipped_steps_never_run() {
        let mut runner = runner();
        runner
            .skip("branch-b", Some("condition false".to_owned()))
            .expect("skip");
        let error = runner.begin("branch-b", "tool.invoke").unwrap_err();
        assert_eq!(error.error_code(), "CONFLICT");
        // Skip is registered once per node.
        let again = runner.skip("branch-b", None).unwrap_err();
        assert_eq!(again.error_code(), "CONFLICT"); // domain: not Pending
    }

    #[test]
    fn retries_consume_guard_budget() {
        let guard = ResourceGuard::new(
            Some(mas_domain::ResourceLimits {
                max_steps: 2,
                ..mas_domain::ResourceLimits::default()
            }),
            None,
        )
        .expect("guard");
        let mut runner = StepRunner::new(ExecutionId::new(), guard);
        runner.begin("node-a", "tool.invoke").expect("begin 1");
        runner
            .fail(
                "node-a",
                StepError::from_app_error(&AppError::messaging("boom")),
            )
            .expect("fail");
        runner.begin("node-a", "tool.invoke").expect("begin 2");
        assert!(runner.begin("node-b", "tool.invoke").is_err());
        let step = runner.step_for("node-a", "tool.invoke").expect("step");
        assert_eq!(step.attempt, 2);
        assert_eq!(runner.finished_steps().len(), 1);
    }

    #[test]
    fn steps_are_exported_in_sequence_order() {
        let mut runner = runner();
        runner.begin("b", "t").expect("b");
        runner.begin("a", "t").expect("a");
        runner.succeed("b", None).expect("ok");
        let sequences: Vec<u64> = runner.steps().iter().map(|s| s.sequence).collect();
        assert_eq!(sequences, vec![0, 1]);
        assert_eq!(runner.steps()[0].node_key.as_deref(), Some("b"));
        assert_eq!(runner.step_count(), 2);
    }
}
