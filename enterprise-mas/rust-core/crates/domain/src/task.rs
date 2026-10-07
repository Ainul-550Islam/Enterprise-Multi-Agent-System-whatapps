//! PURPOSE:
//! The `Task` aggregate: one unit of dispatched durable work, and the only
//! place allowed to change a task's status or ownership.
//!
//! A task is what a worker actually claims. It must therefore contain
//! *everything* needed to recover it after a restart: the execution it
//! belongs to, its tenant/project scope, the target runtime, the attempt
//! counts, the ownership fence, and every lifecycle timestamp. If any of
//! those lived only in memory or only in a broker frame, a process restart
//! would silently lose it.
//!
//! Layer / ownership
//! -----------------
//! `mas-domain`. Pure. Persistence, claiming and dispatch live in
//! `mas-persistence` / `mas-worker` and drive this aggregate's methods.
//!
//! Production responsibility
//! -------------------------
//! * Enforce the canonical task state machine (see
//!   [`crate::state_machine::TaskStateMachine`]).
//! * Own the **ownership fence**: `locked_by` + `locked_until` + `version`
//!   together let a repository reject a write from a worker that no longer
//!   owns the task. A stale worker must never transition a task it lost.
//! * Derive attempt budget from durable state (`attempt_count` /
//!   `max_attempts`), never from broker delivery metadata. The broker is a
//!   wake-up hint; it is not an authority on how many times work ran.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT persist anything or talk to infrastructure.
//! * It must NOT keep a second, parallel notion of task state. There is one
//!   status field and one state machine.
//! * It must NOT store secrets in `last_error`; it is a redacted summary.

use mas_common::enums::{TaskPriority, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, AgentVersionId, ExecutionId, OrganizationId, ProjectId, TaskAttemptId, TaskId,
    TenantId, WorkflowId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::state_machine::{
    illegal_transition, terminal_conflict, AggregateKind, CancellationOutcome, StateTransition,
    TaskStateMachine, TransitionActor, TransitionFailure, TransitionLog,
};

/// Serde default for the version column (versions are 1-based).
const fn default_version() -> u64 {
    1
}

/// Legacy serialized tasks without a correlation key receive a fresh local
/// diagnostic key; persisted tasks hydrate the authoritative database value.
fn default_correlation_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// The task aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    /// Exactly one of `agent_id` / `workflow_id` addresses the target runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Immutable agent-version selection captured at submission and retained
    /// for every delivery/retry. Workflow tasks and legacy unpinned tasks omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version_id: Option<AgentVersionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    /// Owning execution, when the task was spawned by one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    /// Platform request id that initiated the durable task, retained across
    /// broker delivery and retries for end-to-end diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<uuid::Uuid>,
    /// End-to-end correlation key from the accepted submission. This is
    /// materialized separately from the input and is never trusted from it.
    #[serde(default = "default_correlation_id")]
    pub correlation_id: String,
    /// The requested operation (e.g. `agent.run`, `tool.invoke`, `workflow.step`).
    pub operation: String,
    #[serde(default)]
    pub input: serde_json::Value,
    pub priority: TaskPriority,
    pub status: TaskStatus,
    /// Client-supplied duplicate-submission guard (unique per tenant).
    pub idempotency_key: String,
    /// Earliest time the task may be claimed (retry backoff).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    pub max_attempts: u32,
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    /// Safe (redacted, truncated) description of the last failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Stable machine-readable code for the last failure (retry decisions key
    /// off this, never off prose).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_code: Option<String>,
    /// Worker identity currently owning the task (`None` when unclaimed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked_by: Option<String>,
    /// When the current ownership expires (lease/heartbeat window).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked_until: Option<Timestamp>,
    /// The attempt row this ownership belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_id: Option<TaskAttemptId>,
    pub created_by: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    /// Monotonic version, bumped by every applied transition.
    #[serde(default = "default_version")]
    pub version: u64,
    /// Transitions applied since the last drain (never persisted).
    #[serde(default, skip_serializing)]
    transitions: TransitionLog,
}

impl Task {
    /// Creates a task in `Pending` (not yet queued).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        agent_id: Option<AgentId>,
        workflow_id: Option<WorkflowId>,
        operation: impl Into<String>,
        input: serde_json::Value,
        priority: TaskPriority,
        idempotency_key: impl Into<String>,
        created_by: impl Into<String>,
    ) -> Result<Self> {
        let operation = operation.into();
        let idempotency_key = idempotency_key.into();
        let created_by = created_by.into();
        let id = TaskId::new();
        validation::validate_non_empty("operation", &operation)?;
        validation::validate_length("operation", &operation, 1, 128)?;
        validation::validate_non_empty("idempotency_key", &idempotency_key)?;
        validation::validate_length(
            "idempotency_key",
            &idempotency_key,
            1,
            mas_common::constants::MAX_IDEMPOTENCY_KEY_LENGTH,
        )?;
        validation::validate_non_empty("created_by", &created_by)?;
        if agent_id.is_none() && workflow_id.is_none() {
            return Err(AppError::invalid_field(
                "task",
                "missing_target",
                "task must target an agent or a workflow",
            ));
        }
        if agent_id.is_some() && workflow_id.is_some() {
            return Err(AppError::invalid_field(
                "task",
                "ambiguous_target",
                "task targets either an agent or a workflow, never both",
            ));
        }
        let now = Timestamp::now();
        Ok(Self {
            id,
            tenant_id,
            organization_id,
            project_id,
            agent_id,
            agent_version_id: None,
            workflow_id,
            execution_id: None,
            request_id: None,
            correlation_id: id.to_string(),
            operation,
            input,
            priority,
            status: TaskStatus::Pending,
            idempotency_key,
            not_before: None,
            deadline: None,
            max_attempts: mas_common::constants::DEFAULT_MAX_RETRIES + 1,
            attempt_count: 0,
            queued_at: None,
            started_at: None,
            finished_at: None,
            last_error: None,
            last_error_code: None,
            locked_by: None,
            locked_until: None,
            last_attempt_id: None,
            created_by,
            created_at: now,
            updated_at: now,
            version: 1,
            transitions: TransitionLog::new(),
        })
    }

    /// Pins a published agent version for this durable task.
    ///
    /// The pin is selected at submission, not when a worker happens to claim
    /// the task. Durable retries therefore continue to run the exact same
    /// immutable version even if the agent publishes a newer version later.
    pub fn with_agent_version_id(mut self, version_id: AgentVersionId) -> Result<Self> {
        if self.agent_id.is_none() {
            return Err(AppError::invalid_field(
                "agent_version_id",
                "workflow_task",
                "only agent tasks can pin an agent version",
            ));
        }
        self.agent_version_id = Some(version_id);
        Ok(self)
    }

    /// Links the task to the execution it serves.
    ///
    /// Called during atomic submission: the execution and its first task are
    /// created together, so the link is set before either is visible.
    pub fn attach_execution(&mut self, execution_id: ExecutionId) -> Result<()> {
        if self.execution_id.is_some() && self.execution_id != Some(execution_id) {
            return Err(AppError::conflict(format!(
                "task {} already belongs to another execution",
                self.id
            )));
        }
        self.execution_id = Some(execution_id);
        self.touch();
        Ok(())
    }

    /// Binds the originating request id before the task is committed.
    #[must_use]
    pub fn with_request_id(mut self, request_id: uuid::Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Binds the accepted request's correlation id before the task is
    /// committed, so later broker deliveries retain the original trace.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Result<Self> {
        let correlation_id = correlation_id.into();
        validation::validate_non_empty("correlation_id", &correlation_id)?;
        validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        self.correlation_id = correlation_id;
        Ok(self)
    }

    /// Whether the task blew its deadline.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|deadline| !deadline.is_future())
    }

    /// Remaining attempts (excluding the current one).
    #[must_use]
    pub const fn remaining_attempts(&self) -> u32 {
        self.max_attempts.saturating_sub(self.attempt_count)
    }

    /// Whether another attempt is permitted by the durable budget.
    ///
    /// Deliberately derived from `attempt_count`, which is bumped only by a
    /// real start transition — never from a broker redelivery counter.
    #[must_use]
    pub const fn has_attempts_remaining(&self) -> bool {
        self.attempt_count < self.max_attempts
    }

    /// The monotonic version (1-based).
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Transitions recorded since the last drain.
    #[must_use]
    pub fn transitions(&self) -> &TransitionLog {
        &self.transitions
    }

    /// Removes and returns the recorded transitions for persistence.
    pub fn take_transitions(&mut self) -> Vec<StateTransition> {
        self.transitions.drain()
    }

    /// Earliest time the task may be claimed.
    #[must_use]
    pub fn available_at(&self) -> Timestamp {
        self.not_before.unwrap_or(self.created_at)
    }

    /// Whether the task is currently claimable: queued, not before its
    /// availability time, and not holding a live ownership fence.
    #[must_use]
    pub fn is_claimable_at(&self, now: &Timestamp) -> bool {
        if self.status != TaskStatus::Queued {
            return false;
        }
        if self.available_at().is_after(now) {
            return false;
        }
        !self.is_owned_at(now)
    }

    /// Whether a live ownership fence exists at `now`.
    #[must_use]
    pub fn is_owned_at(&self, now: &Timestamp) -> bool {
        match (self.locked_by.as_deref(), self.locked_until) {
            (Some(_), None) => true,
            (Some(_), Some(until)) => until.is_after(now),
            (None, _) => false,
        }
    }

    /// Whether `worker` currently owns this task at `now`.
    #[must_use]
    pub fn is_owned_by_at(&self, worker: &str, now: &Timestamp) -> bool {
        self.locked_by.as_deref() == Some(worker) && self.is_owned_at(now)
    }

    /// Installs an ownership fence. Only a claim (or a renew) may do this; it
    /// is the durable half of "exactly one worker runs this task".
    pub fn assign_ownership(
        &mut self,
        worker: &str,
        until: Timestamp,
        attempt_id: TaskAttemptId,
    ) -> Result<()> {
        if worker.trim().is_empty() {
            return Err(AppError::invalid_field(
                "locked_by",
                "empty",
                "ownership requires a worker identity",
            ));
        }
        self.locked_by = Some(worker.to_owned());
        self.locked_until = Some(until);
        self.last_attempt_id = Some(attempt_id);
        self.touch();
        Ok(())
    }

    /// Extends an existing fence. Refuses to steal: only the current owner may
    /// renew, so a second worker cannot silently take over a live task.
    pub fn renew_ownership(&mut self, worker: &str, until: Timestamp) -> Result<()> {
        match self.locked_by.as_deref() {
            Some(owner) if owner == worker => {
                self.locked_until = Some(until);
                self.touch();
                Ok(())
            },
            Some(_) | None => Err(AppError::conflict(format!(
                "worker '{worker}' cannot renew ownership of task {}; it does not own it",
                self.id
            ))),
        }
    }

    /// Clears the fence (settlement, or an expired lease being reclaimed).
    pub fn release_ownership(&mut self) {
        self.locked_by = None;
        self.locked_until = None;
        self.touch();
    }

    // -- state transitions ---------------------------------------------------

    /// Pending → Queued.
    pub fn queue(&mut self, actor: &TransitionActor) -> Result<StateTransition> {
        if self.is_expired() {
            return Err(AppError::validation("task deadline has already passed"));
        }
        let transition = self.apply(TaskStatus::Queued, actor, None, "queue")?;
        self.queued_at = Some(transition.occurred_at);
        Ok(transition)
    }

    /// Queued → Running for `worker`, consuming one attempt and installing
    /// the ownership fence in the same move.
    ///
    /// Ownership and the status change are one operation on purpose: a task
    /// that is `running` but unowned is unreclaimable after a crash, and a
    /// task that is owned but `queued` can be double-claimed.
    pub fn start(
        &mut self,
        actor: &TransitionActor,
        worker: &str,
        lease_until: Timestamp,
        attempt_id: TaskAttemptId,
    ) -> Result<StateTransition> {
        if self.is_expired() {
            return Err(AppError::timeout("task deadline has passed before start"));
        }
        if !self.has_attempts_remaining() {
            return Err(AppError::conflict(format!(
                "task {} has exhausted its {} attempts",
                self.id, self.max_attempts
            )));
        }
        let transition = self.apply(TaskStatus::Running, actor, None, "start")?;
        self.attempt_count = self.attempt_count.saturating_add(1);
        self.started_at = Some(transition.occurred_at);
        self.finished_at = None;
        self.assign_ownership(worker, lease_until, attempt_id)?;
        Ok(transition)
    }

    /// Running → Completed (terminal), clearing the fence.
    pub fn complete(&mut self, actor: &TransitionActor) -> Result<StateTransition> {
        let transition = self.apply(TaskStatus::Completed, actor, None, "complete")?;
        self.finished_at = Some(transition.occurred_at);
        self.last_error = None;
        self.last_error_code = None;
        self.release_ownership();
        Ok(transition)
    }

    /// Running → Failed with a classified failure.
    pub fn fail(
        &mut self,
        actor: &TransitionActor,
        failure: TransitionFailure,
    ) -> Result<StateTransition> {
        let mut message = failure.message.clone();
        message.truncate(2048);
        self.last_error = Some(message);
        self.last_error_code = Some(failure.code.clone());
        let transition = self.apply(TaskStatus::Failed, actor, Some(failure), "fail")?;
        self.finished_at = Some(transition.occurred_at);
        self.release_ownership();
        Ok(transition)
    }

    /// Failed → Queued after `delay`, when attempts remain. Use
    /// [`Task::dead_letter`] otherwise.
    pub fn retry(
        &mut self,
        actor: &TransitionActor,
        delay: std::time::Duration,
    ) -> Result<StateTransition> {
        // Retrying is only meaningful after a failure. Guarding it explicitly
        // keeps `pending → queued` (the normal queueing move) from being
        // reachable under the name "retry", which would let a caller re-queue
        // work that has never run.
        if self.status != TaskStatus::Failed {
            return Err(illegal_transition(
                AggregateKind::Task,
                &self.id.to_string(),
                "retry",
                self.status.as_str(),
                TaskStatus::Queued.as_str(),
            ));
        }
        if !self.has_attempts_remaining() {
            return Err(AppError::conflict(
                "task has no remaining attempts; dead-letter it instead",
            ));
        }
        let transition = self.apply(TaskStatus::Queued, actor, None, "retry")?;
        let now = transition.occurred_at;
        self.queued_at = Some(now);
        self.started_at = None;
        self.finished_at = None;
        self.not_before = now.checked_add(delay);
        Ok(transition)
    }

    /// Failed → DeadLettered (terminal).
    pub fn dead_letter(
        &mut self,
        actor: &TransitionActor,
        reason: impl Into<String>,
    ) -> Result<StateTransition> {
        let mut reason = reason.into();
        reason.truncate(2048);
        let transition = self.apply(TaskStatus::DeadLettered, actor, None, "dead-letter")?;
        self.last_error = Some(format!("dead-lettered: {reason}"));
        self.release_ownership();
        Ok(transition)
    }

    /// Pending|Queued|Running → Cancelled (terminal, idempotent).
    ///
    /// A completed, failed or dead-lettered task is never reopened: cancelling
    /// one is refused rather than erasing a real outcome.
    pub fn cancel(&mut self, actor: &TransitionActor) -> Result<CancellationOutcome> {
        if self.status == TaskStatus::Cancelled {
            return Ok(CancellationOutcome::AlreadyCancelled);
        }
        let transition = self.apply(TaskStatus::Cancelled, actor, None, "cancel")?;
        self.finished_at = Some(transition.occurred_at);
        self.release_ownership();
        Ok(CancellationOutcome::Cancelled(transition))
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    // -- the single transition gate -----------------------------------------

    /// Applies one canonical task transition. Every public status change
    /// funnels through here so validation cannot be bypassed.
    fn apply(
        &mut self,
        to: TaskStatus,
        actor: &TransitionActor,
        failure: Option<TransitionFailure>,
        action: &str,
    ) -> Result<StateTransition> {
        let from = self.status;
        if !TaskStateMachine::can_transition(from, to) {
            let error = if from.is_terminal() {
                terminal_conflict(
                    AggregateKind::Task,
                    &self.id.to_string(),
                    action,
                    from.as_str(),
                )
            } else {
                illegal_transition(
                    AggregateKind::Task,
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
            aggregate: AggregateKind::Task,
            aggregate_id: self.id.to_string(),
            from: from.as_str().to_owned(),
            to: to.as_str().to_owned(),
            actor: actor.clone(),
            occurred_at: now,
            version: self.version,
            failure,
            correlation_id: None,
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
    use mas_common::ids::WorkflowId;

    fn actor() -> TransitionActor {
        TransitionActor::worker("w-1").expect("actor")
    }

    fn task() -> Task {
        Task::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            None,
            Some(WorkflowId::new()),
            "execution.run",
            serde_json::json!({}),
            TaskPriority::High,
            "idem-1",
            "svc-test",
        )
        .expect("task")
    }

    fn failure(retryable: bool) -> TransitionFailure {
        TransitionFailure::new("upstream_unavailable", "bus is down", retryable).expect("failure")
    }

    fn future(secs: u64) -> Timestamp {
        Timestamp::now()
            .checked_add(std::time::Duration::from_secs(secs))
            .expect("timestamp")
    }

    #[test]
    fn new_tasks_are_pending_unowned_and_unreclaimed() {
        let task = task();
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.version(), 1);
        assert!(task.locked_by.is_none());
        assert_eq!(task.attempt_count, 0);
        assert!(task.has_attempts_remaining());
        assert!(
            !task.is_claimable_at(&Timestamp::now()),
            "pending is not claimable"
        );
    }

    #[test]
    fn queue_then_claim_installs_the_ownership_fence() {
        let mut task = task();
        let now = Timestamp::now();
        task.queue(&actor()).expect("queue");
        assert!(task.is_claimable_at(&now));

        let attempt = TaskAttemptId::new();
        let lease = future(30);
        let started = task.start(&actor(), "w-1", lease, attempt).expect("start");
        assert_eq!(started.to, "running");
        assert_eq!(task.attempt_count, 1);
        assert_eq!(task.locked_by.as_deref(), Some("w-1"));
        assert_eq!(task.last_attempt_id, Some(attempt));
        assert!(task.is_owned_by_at("w-1", &now));
        assert!(!task.is_owned_by_at("w-2", &now));
        assert!(
            !task.is_claimable_at(&now),
            "an owned task must never be claimable"
        );
    }

    #[test]
    fn ownership_renewal_refuses_to_steal() {
        let mut task = task();
        task.queue(&actor()).expect("queue");
        task.start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect("start");

        assert!(task.renew_ownership("w-1", future(60)).is_ok());
        let error = task
            .renew_ownership("w-2", future(60))
            .expect_err("another worker must not renew someone else's lease");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(task.locked_by.as_deref(), Some("w-1"));
    }

    #[test]
    fn expired_leases_are_claimable_again() {
        let now = Timestamp::now();
        let mut task = task();
        task.queue(&actor()).expect("queue");
        task.start(&actor(), "w-1", now, TaskAttemptId::new())
            .expect("start");
        assert!(
            !task.is_owned_at(&now),
            "a lease that expires exactly now has lapsed: it must be renewed before expiry"
        );
        let earlier = Timestamp::from_unix_ms(now.to_unix_ms() - 1).expect("timestamp");
        assert!(
            task.is_owned_at(&earlier),
            "a lease is live until its expiry"
        );
        assert!(
            !task.is_owned_at(&future(1)),
            "an expired lease frees the task"
        );
    }

    #[test]
    fn attempt_budget_comes_from_durable_state_not_the_broker() {
        let mut task = task();
        task.max_attempts = 2;
        task.queue(&actor()).expect("queue");
        task.start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect("attempt 1");

        task.fail(&actor(), failure(true)).expect("fail");
        assert!(task.has_attempts_remaining());
        task.retry(&actor(), std::time::Duration::from_secs(5))
            .expect("retry");
        assert!(task.not_before.is_some(), "retry schedules a backoff");
        assert!(task.started_at.is_none());

        task.start(&actor(), "w-2", future(30), TaskAttemptId::new())
            .expect("attempt 2");
        assert_eq!(task.attempt_count, 2);
        assert!(!task.has_attempts_remaining());

        task.fail(&actor(), failure(true)).expect("fail");
        let error = task
            .retry(&actor(), std::time::Duration::ZERO)
            .expect_err("no attempts left");
        assert!(matches!(error, AppError::Conflict(_)));
        assert!(
            task.dead_letter(&actor(), "attempts exhausted").is_ok(),
            "exhausted tasks go to the dead-letter queue"
        );
        assert_eq!(task.status, TaskStatus::DeadLettered);
    }

    #[test]
    fn a_zero_attempt_budget_refuses_to_start_at_all() {
        let mut task = task();
        task.max_attempts = 0;
        task.queue(&actor()).expect("queue");

        let version_before = task.version();
        let error = task
            .start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect_err("no attempt budget");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(task.status, TaskStatus::Queued);
        assert_eq!(task.attempt_count, 0, "a refused start consumes no attempt");
        assert_eq!(task.version(), version_before);
        assert!(
            task.locked_by.is_none(),
            "a refused start installs no fence"
        );
    }

    #[test]
    fn retrying_is_only_legal_after_a_failure() {
        let mut task = task();
        task.queue(&actor()).expect("queue");
        let error = task
            .retry(&actor(), std::time::Duration::ZERO)
            .expect_err("a queued task has nothing to retry");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(task.status, TaskStatus::Queued);

        task.start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect("start");
        let error = task
            .retry(&actor(), std::time::Duration::ZERO)
            .expect_err("a running task has nothing to retry");
        assert!(matches!(error, AppError::Conflict(_)));
    }

    #[test]
    fn exhausting_the_budget_dead_letters_instead_of_looping() {
        let mut task = task();
        task.max_attempts = 2;
        task.queue(&actor()).expect("queue");
        task.start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect("attempt 1");
        task.fail(&actor(), failure(true)).expect("fail");
        task.retry(&actor(), std::time::Duration::ZERO)
            .expect("retry after attempt 1");
        task.start(&actor(), "w-2", future(30), TaskAttemptId::new())
            .expect("attempt 2");
        task.fail(&actor(), failure(true)).expect("fail");

        assert_eq!(task.attempt_count, 2);
        assert!(
            task.retry(&actor(), std::time::Duration::ZERO).is_err(),
            "no retries remain"
        );
        assert!(task.dead_letter(&actor(), "attempts exhausted").is_ok());
        assert_eq!(task.status, TaskStatus::DeadLettered);
    }

    #[test]
    fn settlement_clears_the_fence_and_records_the_failure_class() {
        let mut task = task();
        task.queue(&actor()).expect("queue");
        task.start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect("start");
        let transition = task.fail(&actor(), failure(true)).expect("fail");
        assert!(transition.is_retryable());
        assert_eq!(
            task.last_error_code.as_deref(),
            Some("upstream_unavailable")
        );
        assert!(task.locked_by.is_none(), "failure releases the fence");

        task.retry(&actor(), std::time::Duration::ZERO)
            .expect("retry");
        task.start(&actor(), "w-2", future(30), TaskAttemptId::new())
            .expect("start");
        let completed = task.complete(&actor()).expect("complete");
        assert_eq!(completed.to, "completed");
        assert!(task.last_error.is_none(), "success clears the last error");
        assert!(task.locked_by.is_none());
    }

    #[test]
    fn cancel_is_idempotent_and_never_reopens_a_finished_task() {
        let mut cancelled_task = task();
        cancelled_task.queue(&actor()).expect("queue");
        assert!(matches!(
            cancelled_task.cancel(&actor()).expect("first cancel"),
            CancellationOutcome::Cancelled(_)
        ));
        assert!(matches!(
            cancelled_task.cancel(&actor()).expect("second cancel"),
            CancellationOutcome::AlreadyCancelled
        ));

        let mut completed_task = task();
        completed_task.queue(&actor()).expect("queue");
        completed_task
            .start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect("start");
        completed_task.complete(&actor()).expect("complete");
        let error = completed_task
            .cancel(&actor())
            .expect_err("a completed task must not be cancelled");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(completed_task.status, TaskStatus::Completed);
    }

    #[test]
    fn illegal_moves_fail_safely_and_leave_state_untouched() {
        let mut task = task();
        let version_before = task.version();
        assert!(matches!(
            task.complete(&actor())
                .expect_err("pending cannot complete"),
            AppError::Conflict(_)
        ));
        assert!(matches!(
            task.retry(&actor(), std::time::Duration::ZERO)
                .expect_err("pending cannot retry"),
            AppError::Conflict(_)
        ));
        assert!(matches!(
            task.dead_letter(&actor(), "nope")
                .expect_err("pending cannot dead-letter"),
            AppError::Conflict(_)
        ));
        assert_eq!(task.status, TaskStatus::Pending);
        assert_eq!(task.version(), version_before);
        assert!(task.transitions().is_empty());
    }

    #[test]
    fn transitions_are_recorded_and_drainable() {
        let mut task = task();
        task.queue(&actor()).expect("queue");
        task.start(&actor(), "w-1", future(30), TaskAttemptId::new())
            .expect("start");
        task.complete(&actor()).expect("complete");
        let log = task.take_transitions();
        assert_eq!(log.len(), 3);
        assert_eq!(log.first().map(|t| t.to.clone()), Some("queued".to_owned()));
        assert_eq!(
            log.last().map(|t| t.to.clone()),
            Some("completed".to_owned())
        );
        assert!(task.transitions().is_empty());
    }

    #[test]
    fn transitions_are_never_persisted_with_the_aggregate() {
        let mut task = task();
        task.queue(&actor()).expect("queue");
        let encoded = serde_json::to_value(&task).expect("encode");
        assert!(encoded.get("transitions").is_none());
        assert_eq!(encoded.get("version").and_then(|v| v.as_u64()), Some(2));
    }

    #[test]
    fn expired_deadlines_block_queue_and_start() {
        let mut task = task();
        task.deadline = Some(Timestamp::from_unix_ms(1).expect("past"));
        assert!(task.is_expired());
        assert!(
            task.queue(&actor()).is_err(),
            "an expired task cannot be queued"
        );
    }

    #[test]
    fn execution_attachment_is_once_only() {
        let mut task = task();
        let execution = ExecutionId::new();
        task.attach_execution(execution).expect("attach");
        task.attach_execution(execution)
            .expect("reattaching the same execution is a no-op");
        assert!(
            task.attach_execution(ExecutionId::new()).is_err(),
            "a task cannot be re-parented"
        );
    }

    #[test]
    fn targets_are_exactly_one_of_agent_or_workflow() {
        let base = || {
            (
                TenantId::new(),
                OrganizationId::new(),
                ProjectId::new(),
                TaskPriority::Normal,
            )
        };
        let (t, o, p, pr) = base();
        assert!(Task::new(
            t,
            o,
            p,
            None,
            None,
            "execution.run",
            serde_json::json!({}),
            pr,
            "k",
            "u"
        )
        .is_err());
        let (t, o, p, pr) = base();
        assert!(Task::new(
            t,
            o,
            p,
            Some(AgentId::new()),
            Some(WorkflowId::new()),
            "execution.run",
            serde_json::json!({}),
            pr,
            "k",
            "u"
        )
        .is_err());
        let (t, o, p, pr) = base();
        assert!(Task::new(
            t,
            o,
            p,
            Some(AgentId::new()),
            None,
            "execution.run",
            serde_json::json!({}),
            pr,
            "k",
            "u"
        )
        .is_ok());
    }
}
