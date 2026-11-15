//! PURPOSE:
//! The durable execution port: every multi-write state change in the
//! execution pipeline, expressed as one atomic operation.
//!
//! This is the seam that makes atomicity *structural* rather than aspirational.
//! The application service never performs three writes and hopes; it calls one
//! method here and the implementation is required to do the whole unit inside a
//! single transaction. A caller physically cannot create an execution without
//! its task, its attempt, its audit row and its outbox rows.
//!
//! Layer / ownership
//! -----------------
//! `mas-application` owns the port and the vocabulary. `mas-persistence` owns
//! the only production implementation (Postgres + `UnitOfWork`). The
//! in-memory implementation in [`crate::durable_memory`] is a faithful
//! reference used by tests; production composition roots must wire Postgres.
//!
//! Production responsibility
//! -------------------------
//! * Submission: validate → idempotency (DB-enforced) → execution → task →
//!   attempt → audit → outbox → commit. Nothing is reported successful before
//!   the commit succeeds.
//! * Claiming: never return more work than the caller's remaining capacity,
//!   never return a task another worker owns, never return a task before its
//!   availability time.
//! * Settlement: persist terminal state (execution, task, attempt, usage,
//!   audit, outbox) in one transaction, **before** the caller acknowledges the
//!   broker message.
//! * Stale-worker protection: every settlement carries the version the caller
//!   observed. A mismatch means the caller lost ownership and its write is
//!   rejected instead of overwriting newer state.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT offer a "best effort" variant of any operation. If the
//!   transaction cannot commit, the operation failed.
//! * It must NOT accept attempt counts derived from broker delivery metadata.
//! * It must NOT expose a second, competing execution lifecycle.

use mas_common::enums::{ExecutionStatus, TaskPriority, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, AgentVersionId, ExecutionId, OrganizationId, ProjectId, TaskAttemptId, TaskId,
    TenantId, WorkflowId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::QuotaDimension;
use mas_domain::{
    AttemptStatus, CancellationSignal, Execution, StateTransition, Task, TaskAttempt,
    TransitionActor, TransitionFailure,
};
use mas_events::outbox::OutboxRecord;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Submission
// ---------------------------------------------------------------------------

/// A fully validated submission request, ready to be written as one unit.
///
/// Everything the transaction needs is here — including the pre-computed
/// fingerprint and an injected clock reading — so the transaction itself
/// performs no validation I/O and cannot be half-applied by a caller that
/// forgot a step.
#[derive(Debug, Clone)]
pub struct SubmissionIntent {
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub workflow_id: Option<WorkflowId>,
    pub agent_id: Option<AgentId>,
    /// Version selected by the Rust runtime catalog at submission. Required for
    /// agent tasks so retries never float to a subsequently published version.
    pub agent_version_id: Option<AgentVersionId>,
    /// Request body. Hashed into `fingerprint`; never stored verbatim in the
    /// idempotency ledger.
    pub input: Value,
    /// Client duplicate guard. `None` disables idempotency for this submission.
    pub idempotency_key: Option<String>,
    /// Canonical request digest (see [`crate::fingerprint`]).
    pub fingerprint: String,
    pub operation: String,
    pub priority: TaskPriority,
    pub max_attempts: u32,
    pub deadline: Option<Timestamp>,
    pub correlation_id: String,
    /// The originating API request id, propagated through outbox and retries.
    pub request_id: uuid::Uuid,
    pub created_by: String,
    /// The actor recorded on every transition this unit produces.
    pub actor: TransitionActor,
    /// Whether to create the first task attempt eagerly. `false` when the task
    /// is only queued and will be claimed (and thus attempted) by a worker.
    pub create_initial_attempt: bool,
    /// Injected clock reading, so the whole unit shares one timestamp.
    pub now: Timestamp,
}

impl SubmissionIntent {
    /// Rejects structurally invalid intents before any transaction opens.
    pub fn validate(&self) -> Result<()> {
        if self.workflow_id.is_none() && self.agent_id.is_none() {
            return Err(AppError::invalid_field(
                "submission",
                "missing_target",
                "a submission must target a workflow or an agent",
            ));
        }
        if self.workflow_id.is_some() && self.agent_id.is_some() {
            return Err(AppError::invalid_field(
                "submission",
                "ambiguous_target",
                "a submission targets either a workflow or an agent, never both",
            ));
        }
        if self.agent_id.is_some() && self.agent_version_id.is_none() {
            return Err(AppError::invalid_field(
                "agent_version_id",
                "missing_pin",
                "agent submissions must pin a published version before durable submission",
            ));
        }
        if self.agent_id.is_none() && self.agent_version_id.is_some() {
            return Err(AppError::invalid_field(
                "agent_version_id",
                "workflow_target",
                "workflow submissions cannot pin an agent version",
            ));
        }
        if self.operation.trim().is_empty() {
            return Err(AppError::invalid_field(
                "operation",
                "required",
                "a submission must name the operation its task will run",
            ));
        }
        mas_common::validation::validate_non_empty("correlation_id", &self.correlation_id)?;
        mas_common::validation::validate_length("correlation_id", &self.correlation_id, 1, 256)?;
        if self.max_attempts == 0 {
            return Err(AppError::invalid_field(
                "max_attempts",
                "out_of_range",
                "max_attempts must be at least 1",
            ));
        }
        if self.fingerprint.is_empty() {
            return Err(AppError::invalid_field(
                "fingerprint",
                "required",
                "a submission must carry its canonical fingerprint",
            ));
        }
        Ok(())
    }
}

/// Whether a submission created new work or replayed existing work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionKind {
    /// New execution, task and (optionally) attempt were committed.
    Created,
    /// The idempotency key matched with an identical fingerprint; the
    /// original result was returned and nothing new was created.
    Replayed,
}

impl SubmissionKind {
    /// Stable metric/log token.
    #[must_use]
    pub const fn metric_token(&self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Replayed => "replayed",
        }
    }
}

/// The idempotency fact an in-memory submission gate needs before it
/// decides whether a quota-capacity check is required.
///
/// PostgreSQL intentionally reports [`Self::Unknown`]: its authoritative
/// request transaction resolves the durable unique key and counter mutation
/// together. The in-memory reference implementation reports a precise answer
/// while the control plane's local submission gate is held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionAdmission {
    /// The implementation has no safe non-mutating idempotency probe.
    Unknown,
    /// No prior idempotency record exists; this request may create work.
    New,
    /// The existing record has the same fingerprint and will replay.
    Replayed,
    /// The existing key has a different fingerprint and will conflict.
    Conflicting,
}

/// What one atomic submission produced.
#[derive(Debug, Clone)]
pub struct SubmissionReceipt {
    /// Created or replayed.
    pub kind: SubmissionKind,
    pub execution: Execution,
    pub task: Task,
    /// The initial attempt, when one was created.
    pub attempt: Option<TaskAttempt>,
    /// Every transition the unit applied (audit + outbox source).
    pub transitions: Vec<StateTransition>,
    /// Outbox rows that committed with the state change.
    pub events: Vec<OutboxRecord>,
}

// ---------------------------------------------------------------------------
// Claiming
// ---------------------------------------------------------------------------

/// A worker asking for work.
#[derive(Debug, Clone)]
pub struct ClaimQuery {
    /// The requesting worker's identity (recorded as the owner).
    pub worker: String,
    /// **Hard ceiling** on how many tasks may be returned. The implementation
    /// must never fetch more than this, and callers must compute it as
    /// `max_in_flight - active_attempts`.
    pub capacity: usize,
    /// Lease expiry for the ownership fence.
    pub lease_until: Timestamp,
    /// Injected clock reading.
    pub now: Timestamp,
    /// Optional tenant restriction (used by per-tenant workers; `None` drains
    /// across tenants, which is the default production shape).
    pub tenant_id: Option<TenantId>,
    /// Optional single-task restriction.
    ///
    /// A broker message names one task. The worker reacting to it must claim
    /// *that* task durably, not whichever task happens to sit at the head of
    /// the queue. `None` claims any eligible task.
    pub task_id: Option<TaskId>,
}

/// One claimed task, with the attempt that proves the claim.
#[derive(Debug, Clone)]
pub struct TaskClaim {
    pub task: Task,
    pub attempt: TaskAttempt,
    /// When this claim's ownership expires unless renewed.
    pub lease_until: Timestamp,
}

// ---------------------------------------------------------------------------
// Settlement
// ---------------------------------------------------------------------------

/// Token usage and cost metadata for one run (spec: authoritative, never
/// synthetic).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionUsage {
    /// Provider identifier (`openai`, `anthropic`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Model identifier as reported by the runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub tool_calls: u64,
    pub steps: u64,
    /// Cost in micros of the billing currency, when the runtime knows it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_micros: Option<u64>,
}

impl ExecutionUsage {
    /// Whether anything was actually consumed (an all-zero observation is not
    /// a usage record — it is noise).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.tokens_input == 0 && self.tokens_output == 0 && self.tool_calls == 0 && self.steps == 0
    }

    /// Expands into the quota observations this run consumed.
    ///
    /// Only non-zero dimensions produce a record, so the usage ledger never
    /// accumulates synthetic zeroes.
    pub fn observations(&self) -> Vec<UsageObservation> {
        let mut observations = Vec::new();
        let total_tokens = self.tokens_input.saturating_add(self.tokens_output);
        if total_tokens > 0 {
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "tokens_input".to_owned(),
                serde_json::json!(self.tokens_input),
            );
            metadata.insert(
                "tokens_output".to_owned(),
                serde_json::json!(self.tokens_output),
            );
            if let Some(provider) = &self.provider {
                metadata.insert("provider".to_owned(), serde_json::json!(provider));
            }
            if let Some(model) = &self.model {
                metadata.insert("model".to_owned(), serde_json::json!(model));
            }
            if let Some(cost) = self.cost_micros {
                metadata.insert("cost_micros".to_owned(), serde_json::json!(cost));
            }
            observations.push(UsageObservation {
                dimension: QuotaDimension::Tokens,
                quantity: total_tokens,
                metadata,
            });
        }
        if self.tool_calls > 0 {
            observations.push(UsageObservation {
                dimension: QuotaDimension::ToolInvocations,
                quantity: self.tool_calls,
                metadata: serde_json::Map::new(),
            });
        }
        if self.steps > 0 {
            let mut metadata = serde_json::Map::new();
            metadata.insert("steps".to_owned(), serde_json::json!(self.steps));
            observations.push(UsageObservation {
                dimension: QuotaDimension::Executions,
                quantity: self.steps,
                metadata,
            });
        }
        observations
    }
}

/// One quota observation derived from a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageObservation {
    pub dimension: QuotaDimension,
    pub quantity: u64,
    pub metadata: serde_json::Map<String, Value>,
}

/// What happened when the runtime finished a task.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    /// The run produced a result.
    Succeeded {
        /// The exact JSON value returned by the handler. JSON `null` is a
        /// valid result and is distinct from a failed or uncompleted task.
        result: Value,
        /// Usage actually observed. `None`/empty means "no metered
        /// consumption" and produces no usage records.
        usage: ExecutionUsage,
    },
    /// The run failed, classified.
    Failed {
        /// Classified failure (code, safe message, retry class).
        failure: TransitionFailure,
    },
    /// The run observed a cancellation and stopped.
    Cancelled,
}

/// How the owning execution should be finalized, when the settled task was
/// its terminal unit of work.
#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    /// Terminal success.
    Completed,
    /// Terminal failure, classified.
    Failed { failure: TransitionFailure },
    /// Terminal cancellation.
    Cancelled,
}

/// The durable write that must land **before** a broker message is acked.
#[derive(Debug, Clone)]
pub struct TaskSettlement {
    pub tenant_id: TenantId,
    pub task_id: TaskId,
    pub attempt_id: TaskAttemptId,
    /// The worker that owns the attempt. A mismatch is a stale-worker write.
    pub worker: String,
    /// The task version the caller observed. A mismatch is a stale write.
    pub expected_version: u64,
    pub outcome: TaskOutcome,
    /// Finalize the owning execution in the same transaction, when this task
    /// was its last unit of work.
    pub execution: Option<ExecutionFinalization>,
    pub actor: TransitionActor,
    /// Injected clock reading.
    pub now: Timestamp,
}

/// Execution finalization paired with the task settlement.
#[derive(Debug, Clone)]
pub struct ExecutionFinalization {
    pub execution_id: ExecutionId,
    pub outcome: ExecutionOutcome,
}

/// What one atomic settlement produced.
#[derive(Debug, Clone)]
pub struct SettlementReceipt {
    pub task: Task,
    pub attempt: TaskAttempt,
    /// The execution, when one was finalized in the same transaction.
    pub execution: Option<Execution>,
    /// `true` when the task was requeued for another attempt.
    pub requeued: bool,
    /// `true` when the task was dead-lettered.
    pub dead_lettered: bool,
    pub transitions: Vec<StateTransition>,
    pub events: Vec<OutboxRecord>,
    /// Usage observations committed with this settlement (empty on failure:
    /// a failed execution must never generate a success-shaped meter).
    pub usage: Vec<UsageObservation>,
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

/// A durable cancellation request.
#[derive(Debug, Clone)]
pub struct CancellationRequest {
    pub tenant_id: TenantId,
    pub execution_id: ExecutionId,
    pub actor: TransitionActor,
    pub correlation_id: String,
    pub now: Timestamp,
}

/// How a cancellation was applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationResult {
    /// The execution moved to `cancelled` (it had not started, or was queued).
    Cancelled,
    /// The execution is running; the cooperative flag was set and the runtime
    /// will settle it as cancelled.
    SignalSet,
    /// It was already cancelled — an idempotent no-op.
    AlreadyCancelled,
}

/// What one atomic cancellation produced.
#[derive(Debug, Clone)]
pub struct CancellationReceipt {
    pub result: CancellationResult,
    pub execution: Execution,
    /// The task that was cancelled alongside the execution, if any.
    pub task: Option<Task>,
    pub signal: Option<CancellationSignal>,
    pub transitions: Vec<StateTransition>,
    pub events: Vec<OutboxRecord>,
}

// ---------------------------------------------------------------------------
// Execution transitions (the non-submission, non-terminal lifecycle moves)
// ---------------------------------------------------------------------------

/// One legal execution lifecycle move, performed as a single atomic unit.
#[derive(Debug, Clone)]
pub enum ExecutionAction {
    /// Pending|Queued → Running.
    Start,
    /// Running → Paused.
    Pause,
    /// Paused → Running.
    Resume,
    /// Running|Paused → Completed (terminal), preserving an optional JSON
    /// output in the same durable transition.
    Complete { output: Option<Value> },
    /// Running|Paused → Failed (terminal), classified.
    Fail { failure: TransitionFailure },
}

impl ExecutionAction {
    /// The audit/log verb for this move.
    #[must_use]
    pub const fn verb(&self) -> &'static str {
        match self {
            Self::Start => "execution.start",
            Self::Pause => "execution.pause",
            Self::Resume => "execution.resume",
            Self::Complete { .. } => "execution.complete",
            Self::Fail { .. } => "execution.fail",
        }
    }
}

/// A lifecycle move on one execution.
#[derive(Debug, Clone)]
pub struct ExecutionTransitionRequest {
    pub tenant_id: TenantId,
    pub execution_id: ExecutionId,
    pub action: ExecutionAction,
    pub actor: TransitionActor,
    pub correlation_id: String,
    /// Injected clock reading.
    pub now: Timestamp,
}

/// What one atomic lifecycle move produced.
#[derive(Debug, Clone)]
pub struct ExecutionTransitionReceipt {
    pub execution: Execution,
    pub transition: StateTransition,
    pub events: Vec<OutboxRecord>,
}

// ---------------------------------------------------------------------------
// Read models
// ---------------------------------------------------------------------------

/// Counters the worker and the relay need, read from durable state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PipelineCounters {
    /// Attempts currently owned and unfinished by `worker`.
    pub active_attempts: u64,
    /// Outbox rows awaiting publication.
    pub outbox_backlog: u64,
    /// Tasks currently claimable across all tenants.
    pub claimable_tasks: u64,
}

// ---------------------------------------------------------------------------
// The port
// ---------------------------------------------------------------------------

/// Every atomic durable operation the execution pipeline performs.
///
/// Implementations **must** run each method as exactly one database
/// transaction. There is no partial-success vocabulary in this trait: a method
/// either committed the whole unit or it returns `Err` and nothing changed.
#[async_trait::async_trait]
pub trait DurableExecutionPort: std::fmt::Debug + Send + Sync {
    /// Whether callers must hold the control plane's local submission gate.
    ///
    /// Only the in-memory reference implementation returns `true`. Durable
    /// PostgreSQL submission instead requires an authoritative transaction,
    /// which is the cross-replica atomicity boundary.
    fn requires_local_submission_gate(&self) -> bool {
        false
    }

    /// Classifies a same-key request without creating work.
    ///
    /// This optional probe is used only together with
    /// [`Self::requires_local_submission_gate`]. Returning `Unknown` is the
    /// safe default for stores whose idempotency decision belongs inside their
    /// transaction.
    async fn submission_admission(
        &self,
        _intent: &SubmissionIntent,
    ) -> Result<SubmissionAdmission> {
        Ok(SubmissionAdmission::Unknown)
    }

    /// Atomically submits one execution.
    ///
    /// Returns [`AppError::Conflict`] when the idempotency key was already
    /// used with a **different** fingerprint. Returns a receipt with
    /// [`SubmissionKind::Replayed`] when it was used with the same one.
    async fn submit(&self, intent: SubmissionIntent) -> Result<SubmissionReceipt>;

    /// Atomically claims up to [`ClaimQuery::capacity`] tasks, creating one
    /// durable attempt per claim and installing the ownership fence.
    async fn claim(&self, query: ClaimQuery) -> Result<Vec<TaskClaim>>;

    /// Extends a live lease. Returns `false` when the caller no longer owns it
    /// (the task moved on or another worker took over).
    async fn renew_lease(&self, task_id: TaskId, worker: &str, until: Timestamp) -> Result<bool>;

    /// Atomically records a terminal outcome: attempt, task, execution (when
    /// requested), usage, outbox events.
    ///
    /// Returns [`AppError::Conflict`] when `expected_version` no longer
    /// matches or the caller no longer owns the task — a stale worker must not
    /// be able to overwrite newer state.
    async fn settle(&self, settlement: TaskSettlement) -> Result<SettlementReceipt>;

    /// Atomically cancels an execution (and its live task, if any).
    ///
    /// Returns [`AppError::Conflict`] for a completed or failed execution:
    /// cancellation must never reopen a finished run.
    async fn cancel(&self, request: CancellationRequest) -> Result<CancellationReceipt>;

    /// Atomically applies one lifecycle move to an execution.
    ///
    /// Illegal moves fail with [`AppError::Conflict`] and change nothing.
    async fn transition(
        &self,
        request: ExecutionTransitionRequest,
    ) -> Result<ExecutionTransitionReceipt>;

    /// Durable counters for capacity control and backlog telemetry, evaluated
    /// **at `at`** — never at whatever the server clock happens to read. A
    /// capacity decision made against an implicit clock is not reproducible
    /// and cannot be tested.
    async fn counters(&self, worker: &str, at: &Timestamp) -> Result<PipelineCounters>;

    /// Lists a tenant's executions (reads hide other tenants entirely).
    async fn list_executions(&self, tenant: TenantId, limit: usize) -> Result<Vec<Execution>>;

    /// Loads one task with its owning execution, scoped to `tenant`.
    async fn load_task(
        &self,
        tenant: TenantId,
        task_id: TaskId,
    ) -> Result<Option<(Task, Option<Execution>)>>;

    /// Loads an execution, scoped to `tenant`. A foreign tenant's execution
    /// reads as `None` — existence is hidden, not merely forbidden.
    async fn load_execution(
        &self,
        tenant: TenantId,
        execution_id: ExecutionId,
    ) -> Result<Option<Execution>>;
}

// ---------------------------------------------------------------------------
// Small shared helpers used by every implementation
// ---------------------------------------------------------------------------

/// Renders the "you lost ownership" conflict a stale worker receives.
#[must_use]
pub fn stale_worker_conflict(task_id: TaskId, expected: u64, actual: u64) -> AppError {
    AppError::conflict(format!(
        "stale worker write rejected for task {task_id}: observed version {expected}, durable version {actual}"
    ))
}

/// Renders the "someone else owns this" conflict.
#[must_use]
pub fn ownership_conflict(task_id: TaskId, worker: &str, owner: Option<&str>) -> AppError {
    match owner {
        Some(owner) => AppError::conflict(format!(
            "worker '{worker}' does not own task {task_id}; '{owner}' does"
        )),
        None => AppError::conflict(format!(
            "worker '{worker}' does not own task {task_id}; it is unowned"
        )),
    }
}

/// Renders the "cannot reopen a finished execution" conflict.
#[must_use]
pub fn terminal_execution_conflict(execution_id: ExecutionId, status: ExecutionStatus) -> AppError {
    AppError::conflict(format!(
        "execution {execution_id} is already {status}; cancellation must not reopen a finished run"
    ))
}

/// Renders the "attempt is already finished" conflict.
#[must_use]
pub fn immutable_attempt_conflict(attempt_id: TaskAttemptId, status: AttemptStatus) -> AppError {
    AppError::conflict(format!(
        "attempt {attempt_id} is already {status}; attempts are immutable once terminal"
    ))
}

/// Renders the idempotency fingerprint conflict.
#[must_use]
pub fn idempotency_conflict(key: &str) -> AppError {
    AppError::conflict(format!(
        "idempotency key '{key}' was already used with a different request; \
         reuse the original request or choose a new key"
    ))
}

/// Whether a task status permits another attempt to be scheduled.
#[must_use]
pub const fn task_can_retry(status: TaskStatus) -> bool {
    matches!(status, TaskStatus::Failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent() -> SubmissionIntent {
        SubmissionIntent {
            tenant_id: TenantId::new(),
            organization_id: OrganizationId::new(),
            project_id: ProjectId::new(),
            workflow_id: Some(WorkflowId::new()),
            agent_id: None,
            agent_version_id: None,
            input: serde_json::json!({"prompt": "hi"}),
            idempotency_key: Some("k".to_owned()),
            fingerprint: "ab".repeat(32),
            operation: "execution.run".to_owned(),
            priority: TaskPriority::Normal,
            max_attempts: 3,
            deadline: None,
            correlation_id: "corr".to_owned(),
            request_id: uuid::Uuid::now_v7(),
            created_by: "user-1".to_owned(),
            actor: TransitionActor::worker("w-1").expect("actor"),
            create_initial_attempt: false,
            now: Timestamp::from_unix_seconds(1_700_000_000).expect("ts"),
        }
    }

    #[test]
    fn intents_reject_structurally_invalid_submissions() {
        let mut both = intent();
        both.agent_id = Some(AgentId::new());
        assert!(both.validate().is_err(), "a submission targets exactly one");

        let mut neither = intent();
        neither.workflow_id = None;
        assert!(neither.validate().is_err());

        let mut unpinned_agent = intent();
        unpinned_agent.workflow_id = None;
        unpinned_agent.agent_id = Some(AgentId::new());
        assert!(unpinned_agent.validate().is_err());

        let mut workflow_with_agent_pin = intent();
        workflow_with_agent_pin.agent_version_id = Some(AgentVersionId::new());
        assert!(workflow_with_agent_pin.validate().is_err());

        let mut no_attempts = intent();
        no_attempts.max_attempts = 0;
        assert!(no_attempts.validate().is_err());

        let mut no_fingerprint = intent();
        no_fingerprint.fingerprint = String::new();
        assert!(no_fingerprint.validate().is_err());

        let mut no_operation = intent();
        no_operation.operation = "  ".to_owned();
        assert!(no_operation.validate().is_err());

        assert!(intent().validate().is_ok());
    }

    #[test]
    fn usage_expands_only_into_non_zero_observations() {
        let empty = ExecutionUsage::default();
        assert!(empty.is_empty());
        assert!(empty.observations().is_empty(), "no synthetic zero meters");

        let usage = ExecutionUsage {
            provider: Some("openai".to_owned()),
            model: Some("gpt-4o-mini".to_owned()),
            tokens_input: 100,
            tokens_output: 50,
            tool_calls: 2,
            steps: 3,
            cost_micros: Some(900),
        };
        let observations = usage.observations();
        assert_eq!(observations.len(), 3);
        assert_eq!(observations[0].dimension, QuotaDimension::Tokens);
        assert_eq!(observations[0].quantity, 150, "tokens are the sum in + out");
        assert_eq!(
            observations[0]
                .metadata
                .get("model")
                .and_then(Value::as_str),
            Some("gpt-4o-mini")
        );
        assert_eq!(observations[1].dimension, QuotaDimension::ToolInvocations);
        assert_eq!(observations[2].dimension, QuotaDimension::Executions);

        let tokens_only = ExecutionUsage {
            tokens_input: 5,
            ..ExecutionUsage::default()
        };
        assert_eq!(tokens_only.observations().len(), 1);
    }

    #[test]
    fn conflict_messages_name_the_specific_problem() {
        let task_id = TaskId::new();
        let stale = stale_worker_conflict(task_id, 3, 7);
        let rendered = format!("{stale}");
        assert!(rendered.contains("stale worker"), "{rendered}");
        assert!(rendered.contains('3') && rendered.contains('7'));
        assert!(matches!(stale, AppError::Conflict(_)));

        let ownership = ownership_conflict(task_id, "w-2", Some("w-1"));
        assert!(format!("{ownership}").contains("'w-1'"));

        let unowned = ownership_conflict(task_id, "w-2", None);
        assert!(format!("{unowned}").contains("unowned"));

        let execution_id = ExecutionId::new();
        let terminal = terminal_execution_conflict(execution_id, ExecutionStatus::Completed);
        assert!(format!("{terminal}").contains("completed"));

        let attempted = immutable_attempt_conflict(TaskAttemptId::new(), AttemptStatus::Succeeded);
        assert!(format!("{attempted}").contains("immutable"));

        let idem = idempotency_conflict("key-1");
        assert!(format!("{idem}").contains("key-1"));
        assert!(matches!(idem, AppError::Conflict(_)));
    }

    #[test]
    fn only_failed_tasks_can_be_retried() {
        assert!(task_can_retry(TaskStatus::Failed));
        assert!(!task_can_retry(TaskStatus::Queued));
        assert!(!task_can_retry(TaskStatus::Running));
        assert!(!task_can_retry(TaskStatus::Completed));
        assert!(!task_can_retry(TaskStatus::DeadLettered));
    }

    #[test]
    fn submission_kind_tokens_are_stable() {
        assert_eq!(SubmissionKind::Created.metric_token(), "created");
        assert_eq!(SubmissionKind::Replayed.metric_token(), "replayed");
    }
}
