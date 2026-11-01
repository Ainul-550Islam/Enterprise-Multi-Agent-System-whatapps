//! PURPOSE:
//! The `Execution` aggregate: one complete workflow/agent run, and the only
//! place allowed to change an execution's status.
//!
//! This file is the durability anchor of the whole pipeline. Every status the
//! platform ever reports for an execution originates from a transition applied
//! here, which means every status carries an actor, a timestamp, a version and
//! (on failure) a classified reason.
//!
//! Layer / ownership
//! -----------------
//! `mas-domain`. Pure: no database, no broker, no transport. The application
//! layer orchestrates persistence around the transitions this aggregate
//! performs; the worker and the orchestration engine perform them too, but
//! only through these methods.
//!
//! Production responsibility
//! -------------------------
//! * Enforce the canonical execution state machine (see
//!   [`crate::state_machine::ExecutionStateMachine`]). Illegal moves fail with
//!   `Conflict` — they never coerce and never silently no-op.
//! * Make cancellation durable and idempotent: requesting it is a flag that
//!   can be set repeatedly; applying it is a terminal transition that refuses
//!   to reopen a completed or already-cancelled execution.
//! * Keep a monotonic `version` so a stale worker — one that lost ownership —
//!   cannot overwrite a newer state. The version bumps on **every** applied
//!   transition, including the cancellation flag.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT persist anything, publish events, or talk to a clock service.
//! * It must NOT add lifecycle states beyond the ones the domain requires.
//! * It must NOT store secrets: `failure` is a truncated, already-redacted
//!   summary produced by callers.
//!
//! Child executions (agent delegation) form a tree via
//! `parent_execution_id` / `root_execution_id`; the root is its own root.

use mas_common::constants;
use mas_common::enums::ExecutionStatus;
use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, ExecutionId, OrganizationId, ProjectId, TenantId, WorkflowId, WorkflowVersionId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::state_machine::{
    illegal_transition, terminal_conflict, AggregateKind, CancellationOutcome, CancellationSignal,
    ExecutionStateMachine, StateTransition, TransitionActor, TransitionFailure, TransitionLog,
};

/// Accumulated resource consumption of an execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceUsageSummary {
    pub steps_executed: u32,
    pub tool_calls: u32,
    pub tokens_input: u64,
    pub tokens_output: u64,
    /// Wall-clock runtime accumulated while running (ms; excludes pauses).
    pub active_time_ms: u64,
}

impl ResourceUsageSummary {
    #[must_use]
    pub const fn total_tokens(&self) -> u64 {
        self.tokens_input + self.tokens_output
    }
}

/// Serde default for the version column: versions are 1-based, and rows
/// written before versioning existed must hydrate as version 1 rather than 0.
const fn default_version() -> u64 {
    1
}

/// The execution aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: ExecutionId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    /// Definition source: a published workflow version…
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_version_id: Option<WorkflowVersionId>,
    /// …or an agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Delegation tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<ExecutionId>,
    pub root_execution_id: ExecutionId,
    /// End-to-end tracing key supplied at intake.
    pub correlation_id: String,
    pub status: ExecutionStatus,
    /// Coordinated stop flag: the runtime checks this and transitions to
    /// `Cancelled` at the next safe point.
    cancellation_requested: bool,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    #[serde(default)]
    pub resource_usage: ResourceUsageSummary,
    pub created_by: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    /// Monotonic version, bumped by every applied transition. It is the
    /// fence a stale worker trips over: a write carrying an older version is
    /// rejected rather than clobbering newer state.
    #[serde(default = "default_version")]
    pub version: u64,
    /// Transitions applied since the last drain. Diagnostics and outbox
    /// source only — never persisted with the aggregate.
    #[serde(default, skip_serializing)]
    transitions: TransitionLog,
}

impl Execution {
    /// Creates a root execution in `Pending`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        workflow_id: Option<WorkflowId>,
        agent_id: Option<AgentId>,
        input: serde_json::Value,
        correlation_id: impl Into<String>,
        created_by: impl Into<String>,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        let created_by = created_by.into();
        validation::validate_non_empty("correlation_id", &correlation_id)?;
        validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        validation::validate_non_empty("created_by", &created_by)?;
        if workflow_id.is_none() && agent_id.is_none() {
            return Err(AppError::invalid_field(
                "execution",
                "missing_target",
                "execution requires a workflow or agent target",
            ));
        }
        let now = Timestamp::now();
        let id = ExecutionId::new();
        Ok(Self {
            id,
            tenant_id,
            organization_id,
            project_id,
            workflow_id,
            workflow_version_id: None,
            agent_id,
            parent_execution_id: None,
            root_execution_id: id, // root is its own root
            correlation_id,
            status: ExecutionStatus::Pending,
            cancellation_requested: false,
            input,
            output: None,
            failure: None,
            started_at: None,
            finished_at: None,
            deadline: None,
            resource_usage: ResourceUsageSummary::default(),
            created_by,
            created_at: now,
            updated_at: now,
            version: 1,
            transitions: TransitionLog::new(),
        })
    }

    /// Derives a child execution (agent delegation). Links into the same
    /// correlation tree and enforces depth/size limits indirectly via IDs.
    pub fn spawn_child(&self, agent_id: AgentId, input: serde_json::Value) -> Result<Self> {
        if self.status.is_terminal() {
            return Err(AppError::conflict(format!(
                "execution {} in status '{}' cannot delegate",
                self.id, self.status
            )));
        }
        let now = Timestamp::now();
        Ok(Self {
            id: ExecutionId::new(),
            tenant_id: self.tenant_id,
            organization_id: self.organization_id,
            project_id: self.project_id,
            workflow_id: None,
            workflow_version_id: None,
            agent_id: Some(agent_id),
            parent_execution_id: Some(self.id),
            root_execution_id: self.root_execution_id,
            correlation_id: self.correlation_id.clone(),
            status: ExecutionStatus::Pending,
            cancellation_requested: false,
            input,
            output: None,
            failure: None,
            started_at: None,
            finished_at: None,
            deadline: self.deadline, // children never outlive the parent's deadline
            resource_usage: ResourceUsageSummary::default(),
            created_by: format!("execution:{}", self.id),
            created_at: now,
            updated_at: now,
            version: 1,
            transitions: TransitionLog::new(),
        })
    }

    /// `true` when this execution started the tree (no parent).
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.id == self.root_execution_id
    }

    #[must_use]
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }

    /// The monotonic version (1-based; every applied transition bumps it).
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Transitions recorded since the last drain (diagnostics only).
    #[must_use]
    pub fn transitions(&self) -> &TransitionLog {
        &self.transitions
    }

    /// Removes and returns the recorded transitions, for the caller to persist
    /// as audit + outbox rows.
    pub fn take_transitions(&mut self) -> Vec<StateTransition> {
        self.transitions.drain()
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|deadline| !deadline.is_future())
    }

    pub fn set_deadline(&mut self, deadline: Timestamp) -> Result<()> {
        if !deadline.is_future() {
            return Err(AppError::invalid_field(
                "deadline",
                "out_of_range",
                "deadline must be in the future",
            ));
        }
        self.deadline = Some(deadline);
        self.touch();
        Ok(())
    }

    /// Runtime accounting hook (adds to the running totals, enforcing hard caps).
    pub fn record_usage(&mut self, usage: ResourceUsageSummary) -> Result<()> {
        let next_steps = self
            .resource_usage
            .steps_executed
            .saturating_add(usage.steps_executed);
        let next_tools = self
            .resource_usage
            .tool_calls
            .saturating_add(usage.tool_calls);
        if next_steps > constants::MAX_STEPS_PER_EXECUTION {
            return Err(AppError::rate_limited(
                "execution exceeded maximum step count",
            ));
        }
        if next_tools > constants::MAX_TOOL_CALLS_PER_EXECUTION {
            return Err(AppError::rate_limited(
                "execution exceeded maximum tool calls",
            ));
        }
        self.resource_usage.steps_executed = next_steps;
        self.resource_usage.tool_calls = next_tools;
        self.resource_usage.tokens_input = self
            .resource_usage
            .tokens_input
            .saturating_add(usage.tokens_input);
        self.resource_usage.tokens_output = self
            .resource_usage
            .tokens_output
            .saturating_add(usage.tokens_output);
        self.resource_usage.active_time_ms = self
            .resource_usage
            .active_time_ms
            .saturating_add(usage.active_time_ms);
        self.touch();
        Ok(())
    }

    // -- lifecycle -----------------------------------------------------------

    /// Pending → Queued: the execution is durable, committed and visible to
    /// the dispatcher, but no runner owns it yet.
    pub fn enqueue(&mut self, actor: &TransitionActor) -> Result<StateTransition> {
        self.apply(ExecutionStatus::Queued, actor, None, "enqueue")
    }

    /// Pending|Queued → Running.
    pub fn start(&mut self, actor: &TransitionActor) -> Result<StateTransition> {
        if self.is_expired() {
            return Err(AppError::timeout("execution deadline passed before start"));
        }
        let transition = self.apply(ExecutionStatus::Running, actor, None, "start")?;
        self.started_at = Some(transition.occurred_at);
        Ok(transition)
    }

    /// Running → Paused.
    pub fn pause(&mut self, actor: &TransitionActor) -> Result<StateTransition> {
        self.apply(ExecutionStatus::Paused, actor, None, "pause")
    }

    /// Paused → Running.
    pub fn resume(&mut self, actor: &TransitionActor) -> Result<StateTransition> {
        if self.is_expired() {
            return Err(AppError::timeout("execution deadline passed while paused"));
        }
        self.apply(ExecutionStatus::Running, actor, None, "resume")
    }

    /// Sets the cooperative cancellation flag.
    ///
    /// Idempotent: a second request returns `already_requested = true` and
    /// does **not** bump the version or raise — repeated operator or API calls
    /// must not turn into spurious conflicts. A terminal execution cannot be
    /// flagged at all: there is nothing left to stop.
    pub fn request_cancellation(&mut self, actor: &TransitionActor) -> Result<CancellationSignal> {
        if self.status.is_terminal() {
            return Err(terminal_conflict(
                AggregateKind::Execution,
                &self.id.to_string(),
                "request_cancellation",
                self.status.as_str(),
            ));
        }
        let already_requested = self.cancellation_requested;
        if !already_requested {
            self.cancellation_requested = true;
            self.version = self.version.saturating_add(1);
        }
        let now = Timestamp::now();
        self.updated_at = now;
        Ok(CancellationSignal {
            aggregate: AggregateKind::Execution,
            aggregate_id: self.id.to_string(),
            actor: actor.clone(),
            requested_at: now,
            version: self.version,
            already_requested,
        })
    }

    /// Immediate transition to `Cancelled` (the runtime honored the flag, or
    /// an operator forced it). Terminal and idempotent.
    ///
    /// A completed or failed execution is **never** reopened: the request is
    /// rejected rather than being turned into a late cancellation that would
    /// erase a real outcome.
    pub fn cancel(&mut self, actor: &TransitionActor) -> Result<CancellationOutcome> {
        if self.status == ExecutionStatus::Cancelled {
            return Ok(CancellationOutcome::AlreadyCancelled);
        }
        let transition = self.apply(ExecutionStatus::Cancelled, actor, None, "cancel")?;
        self.cancellation_requested = true;
        self.finished_at = Some(transition.occurred_at);
        Ok(CancellationOutcome::Cancelled(transition))
    }

    /// Running|Paused → Completed (terminal). Output must be a JSON value
    /// already validated by the execution layer.
    ///
    /// A cancellation request is honoured over completion: an execution that
    /// was asked to stop must end as cancelled, never as completed.
    pub fn complete(
        &mut self,
        actor: &TransitionActor,
        output: Option<serde_json::Value>,
    ) -> Result<StateTransition> {
        if self.cancellation_requested {
            return Err(AppError::conflict(
                "cancellation was requested; execution must end as cancelled",
            ));
        }
        let transition = self.apply(ExecutionStatus::Completed, actor, None, "complete")?;
        self.output = output;
        self.finished_at = Some(transition.occurred_at);
        Ok(transition)
    }

    /// Running|Paused → Failed (terminal), carrying a classified failure.
    ///
    /// The failure is *classified*, not merely described: the retry decision
    /// downstream keys off `failure.retryable`, never off prose matching.
    pub fn fail(
        &mut self,
        actor: &TransitionActor,
        failure: TransitionFailure,
    ) -> Result<StateTransition> {
        let mut message = failure.message.clone();
        message.truncate(4096);
        self.failure = Some(message);
        let transition = self.apply(ExecutionStatus::Failed, actor, Some(failure), "fail")?;
        self.finished_at = Some(transition.occurred_at);
        Ok(transition)
    }

    /// Wall time since start (None if never started).
    #[must_use]
    pub fn elapsed(&self) -> Option<Duration> {
        let started = self.started_at?;
        let end = self.finished_at.unwrap_or_else(Timestamp::now);
        end.duration_since(&started)
    }

    // -- the single transition gate -----------------------------------------

    /// Applies one canonical transition: validates it against the state
    /// machine, mutates the status, bumps the version, and records the
    /// transition. Every public status change funnels through here, so no
    /// caller can bypass validation by editing `status` some other way.
    fn apply(
        &mut self,
        to: ExecutionStatus,
        actor: &TransitionActor,
        failure: Option<TransitionFailure>,
        action: &str,
    ) -> Result<StateTransition> {
        let from = self.status;
        if !ExecutionStateMachine::can_transition(from, to) {
            let error = if from.is_terminal() {
                terminal_conflict(
                    AggregateKind::Execution,
                    &self.id.to_string(),
                    action,
                    from.as_str(),
                )
            } else {
                illegal_transition(
                    AggregateKind::Execution,
                    &self.id.to_string(),
                    action,
                    from.as_str(),
                    to.as_str(),
                )
            };
            return Err(error);
        }
        let now = Timestamp::now();
        self.status = to;
        self.version = self.version.saturating_add(1);
        self.updated_at = now;
        let transition = StateTransition {
            aggregate: AggregateKind::Execution,
            aggregate_id: self.id.to_string(),
            from: from.as_str().to_owned(),
            to: to.as_str().to_owned(),
            actor: actor.clone(),
            occurred_at: now,
            version: self.version,
            failure,
            correlation_id: Some(self.correlation_id.clone()),
        };
        self.transitions.record(transition.clone());
        Ok(transition)
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::ActorKind;

    fn actor() -> TransitionActor {
        TransitionActor::worker("w-1").expect("actor")
    }

    fn execution() -> Execution {
        Execution::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            Some(WorkflowId::new()),
            None,
            serde_json::json!({"n": 1}),
            "corr-1",
            "user-1",
        )
        .expect("execution")
    }

    fn failure() -> TransitionFailure {
        TransitionFailure::terminal("agent_unavailable", "no route to host").expect("failure")
    }

    #[test]
    fn new_executions_start_pending_at_version_one() {
        let execution = execution();
        assert_eq!(execution.status, ExecutionStatus::Pending);
        assert_eq!(execution.version(), 1);
        assert!(execution.transitions().is_empty());
        assert!(execution.is_root());
    }

    #[test]
    fn happy_path_records_every_transition_with_actor_and_version() {
        let mut execution = execution();
        let queued = execution.enqueue(&actor()).expect("enqueue");
        assert_eq!(queued.from, "pending");
        assert_eq!(queued.to, "queued");
        assert_eq!(queued.version, 2);
        assert_eq!(queued.actor.kind, ActorKind::Worker);

        let running = execution.start(&actor()).expect("start");
        assert_eq!(running.to, "running");
        assert_eq!(running.version, 3);
        assert!(execution.started_at.is_some());

        let completed = execution
            .complete(&actor(), Some(serde_json::json!({"ok": true})))
            .expect("complete");
        assert_eq!(completed.to, "completed");
        assert_eq!(completed.version, 4);
        assert!(execution.finished_at.is_some());
        assert_eq!(execution.output, Some(serde_json::json!({"ok": true})));

        let log = execution.take_transitions();
        assert_eq!(log.len(), 3);
        assert!(execution.transitions().is_empty(), "drain is destructive");
    }

    #[test]
    fn illegal_transitions_fail_safely_and_change_nothing() {
        let mut execution = execution();
        let version_before = execution.version();
        let error = execution
            .complete(&actor(), None)
            .expect_err("pending cannot complete");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(execution.status, ExecutionStatus::Pending);
        assert_eq!(
            execution.version(),
            version_before,
            "a rejected transition must not bump the version"
        );
        assert!(execution.transitions().is_empty());
    }

    #[test]
    fn terminal_executions_are_never_reopened() {
        let mut execution = execution();
        execution.enqueue(&actor()).expect("enqueue");
        execution.start(&actor()).expect("start");
        execution.complete(&actor(), None).expect("complete");

        for (label, result) in [
            ("start", execution.start(&actor()).map(|_| ())),
            ("pause", execution.pause(&actor()).map(|_| ())),
            ("enqueue", execution.enqueue(&actor()).map(|_| ())),
            ("fail", execution.fail(&actor(), failure()).map(|_| ())),
        ] {
            let error = result.expect_err(label);
            assert!(matches!(error, AppError::Conflict(_)), "{label}: {error:?}");
        }
        assert_eq!(execution.status, ExecutionStatus::Completed);

        // Cancelling a completed execution is refused, not silently applied.
        assert!(execution.cancel(&actor()).is_err());
        assert_eq!(execution.status, ExecutionStatus::Completed);
    }

    #[test]
    fn cancellation_is_idempotent_and_never_reopens() {
        let mut execution = execution();
        execution.enqueue(&actor()).expect("enqueue");
        execution.start(&actor()).expect("start");

        let first = execution
            .request_cancellation(&actor())
            .expect("first request");
        assert!(!first.already_requested);
        let version_after_first = execution.version();

        let second = execution
            .request_cancellation(&actor())
            .expect("second request is a no-op, not an error");
        assert!(second.already_requested);
        assert_eq!(
            execution.version(),
            version_after_first,
            "a repeated signal must not bump the version"
        );

        assert!(matches!(
            execution.cancel(&actor()).expect("cancel"),
            CancellationOutcome::Cancelled(_)
        ));
        assert_eq!(execution.status, ExecutionStatus::Cancelled);
        assert!(matches!(
            execution.cancel(&actor()).expect("repeat cancel"),
            CancellationOutcome::AlreadyCancelled
        ));

        // A terminal execution cannot even be flagged.
        assert!(execution.request_cancellation(&actor()).is_err());
    }

    #[test]
    fn cancellation_requested_blocks_completion() {
        let mut execution = execution();
        execution.enqueue(&actor()).expect("enqueue");
        execution.start(&actor()).expect("start");
        execution.request_cancellation(&actor()).expect("flag");

        let error = execution
            .complete(&actor(), None)
            .expect_err("a cancelled run must not complete");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(execution.status, ExecutionStatus::Running);
    }

    #[test]
    fn failure_transitions_carry_classified_information() {
        let mut execution = execution();
        execution.enqueue(&actor()).expect("enqueue");
        execution.start(&actor()).expect("start");
        let transition = execution
            .fail(
                &actor(),
                TransitionFailure::transient("rate_limited", "429").expect("f"),
            )
            .expect("fail");
        assert_eq!(transition.to, "failed");
        assert!(transition.is_failure());
        assert!(transition.is_retryable());
        assert_eq!(
            transition.failure.as_ref().map(|f| f.code.clone()),
            Some("rate_limited".to_owned())
        );
        assert!(execution.failure.is_some());
    }

    #[test]
    fn pause_resume_round_trip() {
        let mut execution = execution();
        execution.enqueue(&actor()).expect("enqueue");
        execution.start(&actor()).expect("start");
        assert_eq!(execution.pause(&actor()).expect("pause").to, "paused");
        assert_eq!(execution.resume(&actor()).expect("resume").to, "running");
        assert!(
            execution.pause(&actor()).is_ok() && execution.pause(&actor()).is_err(),
            "pausing twice is illegal"
        );
    }

    #[test]
    fn paused_executions_may_still_fail() {
        let mut execution = execution();
        execution.enqueue(&actor()).expect("enqueue");
        execution.start(&actor()).expect("start");
        execution.pause(&actor()).expect("pause");
        assert_eq!(
            execution.fail(&actor(), failure()).expect("fail").to,
            "failed"
        );
    }

    #[test]
    fn transitions_are_not_serialized_into_the_aggregate() {
        // Encoded before any local binding can shadow the `execution()` fixture.
        let mut legacy_json = serde_json::to_value(execution()).expect("a fresh execution encodes");
        legacy_json
            .as_object_mut()
            .expect("object")
            .remove("version");

        let mut execution = execution();
        execution.enqueue(&actor()).expect("enqueue");
        let encoded = serde_json::to_value(&execution).expect("encode");
        assert!(
            encoded.get("transitions").is_none(),
            "the transition log is an in-memory drain queue, never persisted state"
        );
        assert_eq!(encoded.get("version").and_then(|v| v.as_u64()), Some(2));

        // A row written before versioning existed hydrates at version 1, not 0:
        // a zero version would make every compare-and-swap fence meaningless.
        let legacy: Execution =
            serde_json::from_value(legacy_json).expect("a pre-versioning row must still hydrate");
        assert_eq!(legacy.version(), 1);
        assert!(legacy.transitions().is_empty());
    }
}
