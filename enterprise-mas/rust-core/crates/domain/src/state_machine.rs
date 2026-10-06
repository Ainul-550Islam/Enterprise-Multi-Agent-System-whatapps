//! PURPOSE:
//! The **single canonical state machine** for the durable execution pipeline.
//!
//! This module owns the one definition of "what may become what" for the
//! execution, task and task-attempt aggregates, and it owns the one shape of
//! the record a transition leaves behind. It exists because the platform
//! previously grew several ad-hoc lifecycle lanes (an orchestration engine
//! lane, a worker lane, a service lane) that each invented their own idea of a
//! legal move; this module is the authority they must all defer to.
//!
//! Layer / ownership
//! -----------------
//! `mas-domain` owns this file. It is pure: no database, no broker, no clock
//! beyond the [`Timestamp`] value it is handed, no I/O of any kind. The
//! application, worker and persistence layers *call* it; they never
//! re-implement it.
//!
//! Production responsibility
//! -------------------------
//! * Every durable state change — and only a durable state change — produces a
//!   [`StateTransition`] carrying source state, target state, acting actor,
//!   occurrence timestamp, the post-transition version, and the classified
//!   failure information when the move was a failure.
//! * Illegal transitions **fail safely**: they return an
//!   [`AppError::Conflict`] naming the aggregate, the action and the current
//!   state. They never silently coerce, never skip a state, and never
//!   resurrect a terminal aggregate.
//! * Versioning is monotonic and owned here: each applied transition bumps the
//!   aggregate version by exactly one, which is what makes stale-worker writes
//!   detectable downstream.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT acquire locks, write to a database or publish events. It
//!   decides *whether* a move is legal and *what record* it leaves; someone
//!   else persists that decision.
//! * It must NOT invent new lifecycle states. The state sets are the ones the
//!   domain already requires; adding a state here without a domain need is a
//!   defect, not a feature.
//! * It must NOT hold secrets. Failure messages are truncated, caller-supplied
//!   and already-redacted summaries; this module never sees a credential.

use mas_common::enums::{ExecutionStatus, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Bounds
// ---------------------------------------------------------------------------

/// Hard cap on a stable failure code (machine-readable, short).
pub const MAX_FAILURE_CODE_LENGTH: usize = 128;
/// Hard cap on a human-readable failure message (safe summaries only).
pub const MAX_FAILURE_MESSAGE_LENGTH: usize = 2048;
/// Hard cap on an actor identifier.
pub const MAX_ACTOR_ID_LENGTH: usize = 128;
/// Transitions retained per aggregate before the oldest are dropped. The log
/// is a diagnostic + outbox source, not an audit store: the authoritative
/// history is the `audit_events` table.
pub const MAX_RECORDED_TRANSITIONS: usize = 64;

// ---------------------------------------------------------------------------
// Aggregate & actor identity
// ---------------------------------------------------------------------------

/// Which durable aggregate a transition belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateKind {
    /// One complete workflow/agent run.
    Execution,
    /// One unit of dispatched work.
    Task,
    /// One execution try of a task.
    TaskAttempt,
}

impl AggregateKind {
    /// Canonical wire/database token.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Execution => "execution",
            Self::Task => "task",
            Self::TaskAttempt => "task_attempt",
        }
    }
}

impl std::fmt::Display for AggregateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Who (or what) caused a transition. Recorded so an operator can tell a user
/// cancellation from a worker crash-recovery move without guesswork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    /// An authenticated human principal (user or API key acting for one).
    Principal,
    /// A worker process performing durable queue work.
    Worker,
    /// An internal platform service (scheduler, recovery sweep, relay).
    System,
    /// The outbox relay, when a transition is a consequence of publication.
    Relay,
}

impl ActorKind {
    /// Canonical wire/database token.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Principal => "principal",
            Self::Worker => "worker",
            Self::System => "system",
            Self::Relay => "relay",
        }
    }
}

impl std::fmt::Display for ActorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The acting identity behind one transition.
///
/// The identifier is a stable, non-secret label: a principal id, a worker name,
/// or a service name. It is deliberately *not* a credential, a token or an
/// email address, because transitions are logged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionActor {
    /// Class of actor.
    pub kind: ActorKind,
    /// Stable identifier within that class.
    pub id: String,
    /// Tenant the actor was scoped to, when the actor is tenant-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
}

impl TransitionActor {
    /// A tenant-scoped human/API-key principal.
    pub fn principal(id: impl Into<String>, tenant_id: TenantId) -> Result<Self> {
        Ok(Self {
            kind: ActorKind::Principal,
            id: Self::checked_id(id)?,
            tenant_id: Some(tenant_id),
        })
    }

    /// A worker process. Workers are not tenant-scoped: one worker drains rows
    /// for many tenants, and the row — not the worker — carries the tenancy.
    pub fn worker(id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            kind: ActorKind::Worker,
            id: Self::checked_id(id)?,
            tenant_id: None,
        })
    }

    /// An internal platform service.
    pub fn system(id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            kind: ActorKind::System,
            id: Self::checked_id(id)?,
            tenant_id: None,
        })
    }

    /// The outbox relay.
    pub fn relay(id: impl Into<String>) -> Result<Self> {
        Ok(Self {
            kind: ActorKind::Relay,
            id: Self::checked_id(id)?,
            tenant_id: None,
        })
    }

    /// Attaches a tenant scope (used when a system actor adopts a tenant for
    /// the duration of one unit of work, e.g. a scheduler tick).
    #[must_use]
    pub fn with_tenant(mut self, tenant_id: TenantId) -> Self {
        self.tenant_id = Some(tenant_id);
        self
    }

    /// `worker:w-1` — the form used in log lines and audit detail.
    #[must_use]
    pub fn label(&self) -> String {
        format!("{}:{}", self.kind, self.id)
    }

    fn checked_id(id: impl Into<String>) -> Result<String> {
        let id = id.into();
        validation::validate_non_empty("actor.id", &id)?;
        validation::validate_length("actor.id", &id, 1, MAX_ACTOR_ID_LENGTH)?;
        Ok(id)
    }
}

// ---------------------------------------------------------------------------
// Failure classification carried by a transition
// ---------------------------------------------------------------------------

/// The classified failure behind a transition into `Failed` (or the terminal
/// outcome of a task attempt).
///
/// `code` is a stable, machine-readable token — never a free-form sentence —
/// so retry logic, dashboards and alert routing can key off it. `message` is a
/// safe, already-redacted summary intended for humans.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionFailure {
    /// Stable machine-readable error code.
    pub code: String,
    /// Safe human-readable summary (redacted upstream, truncated here).
    pub message: String,
    /// Whether retrying the unit of work could plausibly succeed.
    pub retryable: bool,
}

impl TransitionFailure {
    /// Builds a failure, validating and truncating both fields.
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Result<Self> {
        let mut code = code.into();
        let mut message = message.into();
        validation::validate_non_empty("failure.code", &code)?;
        code.truncate(MAX_FAILURE_CODE_LENGTH);
        message.truncate(MAX_FAILURE_MESSAGE_LENGTH);
        Ok(Self {
            code,
            message,
            retryable,
        })
    }

    /// A failure that retrying cannot fix (authn/authz, malformed input, …).
    pub fn terminal(code: impl Into<String>, message: impl Into<String>) -> Result<Self> {
        Self::new(code, message, false)
    }

    /// A failure a later attempt may plausibly clear (broker, rate limit, …).
    pub fn transient(code: impl Into<String>, message: impl Into<String>) -> Result<Self> {
        Self::new(code, message, true)
    }

    /// Canonical wire token for the retry class.
    #[must_use]
    pub const fn class(&self) -> &'static str {
        if self.retryable {
            "transient"
        } else {
            "terminal"
        }
    }
}

// ---------------------------------------------------------------------------
// The transition record
// ---------------------------------------------------------------------------

/// One durable state change, fully described.
///
/// Every field here exists because an operator or a downstream consumer needs
/// it: `from`/`to` to reconstruct the timeline, `actor` to attribute the move,
/// `occurred_at` to order it, `version` to fence stale writers, and `failure`
/// to classify the reason without parsing prose.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateTransition {
    /// Aggregate the transition applied to.
    pub aggregate: AggregateKind,
    /// Aggregate identifier (as a string so the record stays transport-agnostic).
    pub aggregate_id: String,
    /// State before the transition.
    pub from: String,
    /// State after the transition.
    pub to: String,
    /// Who caused it.
    pub actor: TransitionActor,
    /// When it happened.
    pub occurred_at: Timestamp,
    /// Aggregate version *after* the transition was applied (1-based).
    pub version: u64,
    /// Classified failure, when the transition was a failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<TransitionFailure>,
    /// End-to-end correlation key, when the caller had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

impl StateTransition {
    /// `true` when the transition moved the aggregate into a failure state.
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        self.failure.is_some()
    }

    /// `true` when a retry could plausibly succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.failure
            .as_ref()
            .is_some_and(|failure| failure.retryable)
    }
}

// ---------------------------------------------------------------------------
// Transition log (in-aggregate, drained by the persistence layer)
// ---------------------------------------------------------------------------

/// Bounded, append-only log of the transitions an aggregate has taken.
///
/// It exists so the application layer can hand the audit sink and the outbox
/// exactly the transitions a unit of work produced, without re-deriving them
/// from a before/after diff. Bounded because it lives in memory on the
/// aggregate: the authoritative history is the audit ledger.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionLog {
    entries: Vec<StateTransition>,
}

impl TransitionLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Appends a transition, dropping the oldest when the bound is reached.
    pub fn record(&mut self, transition: StateTransition) {
        if self.entries.len() >= MAX_RECORDED_TRANSITIONS {
            self.entries.remove(0);
        }
        self.entries.push(transition);
    }

    /// Removes and returns every recorded transition (drain-then-persist).
    pub fn drain(&mut self) -> Vec<StateTransition> {
        std::mem::take(&mut self.entries)
    }

    /// Borrows the recorded transitions.
    #[must_use]
    pub fn entries(&self) -> &[StateTransition] {
        &self.entries
    }

    /// The most recent transition, if any.
    #[must_use]
    pub fn last(&self) -> Option<&StateTransition> {
        self.entries.last()
    }

    /// Number of retained transitions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing has been recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Execution state machine
// ---------------------------------------------------------------------------

/// The canonical execution lifecycle.
///
/// ```text
///   pending ──► queued ──► running ──► completed   (terminal)
///      │           │          │  ╲
///      │           │          │   ╲──► failed      (terminal)
///      │           │          │
///      │           │          ├────► paused ──► running
///      │           │          │
///      └───────────┴──────────┴────► cancelled    (terminal)
/// ```
///
/// `paused` is kept because the runtime genuinely suspends runs awaiting
/// approval; removing it would delete working behavior, and the domain still
/// requires it. No other state may be added without a domain need.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionStateMachine;

impl ExecutionStateMachine {
    /// Whether `from → to` is a legal execution transition.
    ///
    /// Self-transitions are `false` except for the idempotent
    /// `cancelled → cancelled` case, which the aggregate handles before
    /// consulting this table (a repeated cancellation is a no-op, not an
    /// error).
    #[must_use]
    pub const fn can_transition(from: ExecutionStatus, to: ExecutionStatus) -> bool {
        use ExecutionStatus as S;
        match (from, to) {
            // Intake: created, then made visible to the dispatcher.
            (S::Pending, S::Queued) => true,
            // A pending execution may also be started directly (the durable
            // task is created and claimed in the same breath) or cancelled
            // before it ever reaches a worker.
            (S::Pending, S::Running) => true,
            (S::Pending, S::Cancelled) => true,
            (S::Pending, S::Failed) => true,
            // Dispatch: a worker claimed the durable task.
            (S::Queued, S::Running) => true,
            (S::Queued, S::Cancelled) => true,
            (S::Queued, S::Failed) => true,
            // Running may complete, fail, be cancelled, or pause for approval.
            (S::Running, S::Completed) => true,
            (S::Running, S::Failed) => true,
            (S::Running, S::Cancelled) => true,
            (S::Running, S::Paused) => true,
            // Paused resumes, or ends.
            (S::Paused, S::Running) => true,
            (S::Paused, S::Completed) => true,
            (S::Paused, S::Failed) => true,
            (S::Paused, S::Cancelled) => true,
            // Everything else — including every move out of a terminal state —
            // is illegal.
            _ => false,
        }
    }

    /// The legal targets of `from`, in declaration order.
    #[must_use]
    pub fn legal_targets(from: ExecutionStatus) -> Vec<ExecutionStatus> {
        ExecutionStatus::ALL
            .iter()
            .copied()
            .filter(|to| Self::can_transition(from, *to))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Task state machine
// ---------------------------------------------------------------------------

/// The canonical task lifecycle.
///
/// ```text
///   pending ──► queued ──► running ──► completed   (terminal)
///      │           │          │
///      │           │          ├──────► failed ──► queued     (retry)
///      │           │          │                  ╲
///      │           │          │                   ╲──► dead_lettered (terminal)
///      │           │          │
///      └───────────┴──────────┴──────────────────► cancelled (terminal)
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TaskStateMachine;

impl TaskStateMachine {
    /// Whether `from → to` is a legal task transition.
    #[must_use]
    pub const fn can_transition(from: TaskStatus, to: TaskStatus) -> bool {
        use TaskStatus as S;
        match (from, to) {
            // Intake: created, then made claimable.
            (S::Pending, S::Queued) => true,
            (S::Pending, S::Cancelled) => true,
            (S::Pending, S::Failed) => true,
            // Claim: exactly one worker moves the row to running.
            (S::Queued, S::Running) => true,
            (S::Queued, S::Cancelled) => true,
            (S::Queued, S::Failed) => true,
            // Settlement.
            (S::Running, S::Completed) => true,
            (S::Running, S::Failed) => true,
            (S::Running, S::Cancelled) => true,
            // Retry or dead-letter after a failure.
            (S::Failed, S::Queued) => true,
            (S::Failed, S::DeadLettered) => true,
            // Everything else is illegal: a terminal task is never reopened.
            _ => false,
        }
    }

    /// The legal targets of `from`, in declaration order.
    #[must_use]
    pub fn legal_targets(from: TaskStatus) -> Vec<TaskStatus> {
        TaskStatus::ALL
            .iter()
            .copied()
            .filter(|to| Self::can_transition(from, *to))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Shared guard helpers
// ---------------------------------------------------------------------------

/// Renders the "illegal transition" error every aggregate raises.
///
/// Centralised so the message shape is identical across aggregates: an
/// operator grepping for one phrasing finds every rejection.
#[must_use]
pub fn illegal_transition(
    aggregate: AggregateKind,
    aggregate_id: &str,
    action: &str,
    from: &str,
    to: &str,
) -> AppError {
    AppError::conflict(format!(
        "illegal {aggregate} transition: cannot {action} {aggregate_id} from '{from}' to '{to}'"
    ))
}

/// Renders the "already terminal, refusing to reopen" error.
#[must_use]
pub fn terminal_conflict(
    aggregate: AggregateKind,
    aggregate_id: &str,
    action: &str,
    status: &str,
) -> AppError {
    AppError::conflict(format!(
        "{aggregate} {aggregate_id} is already terminal in state '{status}'; refusing to {action}"
    ))
}

/// A cooperative cancellation signal that did **not** change the aggregate's
/// status (the run is still finishing a safe point).
///
/// Returned by cancellation requests so the caller can persist the flag and
/// emit a signal event without pretending the status moved. `already_requested`
/// makes the operation idempotent: a second request is a no-op, not a conflict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancellationSignal {
    /// Aggregate the signal applies to.
    pub aggregate: AggregateKind,
    /// Aggregate identifier.
    pub aggregate_id: String,
    /// Who requested it.
    pub actor: TransitionActor,
    /// When this request was recorded.
    pub requested_at: Timestamp,
    /// Aggregate version after the flag was set.
    pub version: u64,
    /// `true` when the flag was already set (idempotent no-op).
    pub already_requested: bool,
}

/// Result of asking an aggregate to cancel: either a real terminal transition,
/// or an acknowledgement that it was already cancelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancellationOutcome {
    /// The aggregate moved into `cancelled`.
    Cancelled(StateTransition),
    /// It was already cancelled; nothing changed.
    AlreadyCancelled,
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;

    fn actor() -> TransitionActor {
        TransitionActor::worker("w-1").expect("worker actor")
    }

    #[test]
    fn actor_ids_are_validated() {
        assert!(TransitionActor::worker("").is_err());
        assert!(TransitionActor::worker("x".repeat(MAX_ACTOR_ID_LENGTH + 1)).is_err());
        assert!(TransitionActor::worker("w-1").is_ok());
    }

    #[test]
    fn actor_label_is_stable_and_secret_free() {
        let worker = TransitionActor::worker("w-7").expect("actor");
        assert_eq!(worker.label(), "worker:w-7");
        let tenant = TenantId::new();
        let principal = TransitionActor::principal("user-9", tenant).expect("actor");
        assert_eq!(principal.label(), "principal:user-9");
        assert_eq!(principal.tenant_id, Some(tenant));
    }

    #[test]
    fn failure_fields_are_truncated_and_classified() {
        let failure =
            TransitionFailure::terminal("x".repeat(500), "y".repeat(9_000)).expect("failure");
        assert_eq!(failure.code.len(), MAX_FAILURE_CODE_LENGTH);
        assert_eq!(failure.message.len(), MAX_FAILURE_MESSAGE_LENGTH);
        assert_eq!(failure.class(), "terminal");
        assert!(!failure.retryable);

        let transient =
            TransitionFailure::transient("upstream_unavailable", "try again").expect("failure");
        assert_eq!(transient.class(), "transient");
        assert!(transient.retryable);
        assert!(TransitionFailure::new("", "m", true).is_err());
    }

    #[test]
    fn execution_forbids_reopening_terminal_states() {
        for terminal in [
            ExecutionStatus::Completed,
            ExecutionStatus::Failed,
            ExecutionStatus::Cancelled,
        ] {
            for target in ExecutionStatus::ALL {
                assert!(
                    !ExecutionStateMachine::can_transition(terminal, *target),
                    "{terminal} must not transition to {target}"
                );
            }
        }
    }

    #[test]
    fn execution_happy_path_and_pause_are_legal() {
        assert!(ExecutionStateMachine::can_transition(
            ExecutionStatus::Pending,
            ExecutionStatus::Queued
        ));
        assert!(ExecutionStateMachine::can_transition(
            ExecutionStatus::Queued,
            ExecutionStatus::Running
        ));
        assert!(ExecutionStateMachine::can_transition(
            ExecutionStatus::Running,
            ExecutionStatus::Paused
        ));
        assert!(ExecutionStateMachine::can_transition(
            ExecutionStatus::Paused,
            ExecutionStatus::Running
        ));
        assert!(ExecutionStateMachine::can_transition(
            ExecutionStatus::Running,
            ExecutionStatus::Completed
        ));
        assert!(ExecutionStateMachine::can_transition(
            ExecutionStatus::Running,
            ExecutionStatus::Failed
        ));
        assert!(ExecutionStateMachine::can_transition(
            ExecutionStatus::Running,
            ExecutionStatus::Cancelled
        ));
    }

    #[test]
    fn execution_forbids_nonsense_moves() {
        assert!(!ExecutionStateMachine::can_transition(
            ExecutionStatus::Pending,
            ExecutionStatus::Completed
        ));
        assert!(!ExecutionStateMachine::can_transition(
            ExecutionStatus::Pending,
            ExecutionStatus::Paused
        ));
        assert!(!ExecutionStateMachine::can_transition(
            ExecutionStatus::Queued,
            ExecutionStatus::Completed
        ));
        assert!(!ExecutionStateMachine::can_transition(
            ExecutionStatus::Running,
            ExecutionStatus::Queued
        ));
    }

    #[test]
    fn task_forbids_reopening_terminal_states() {
        for terminal in [
            TaskStatus::Completed,
            TaskStatus::Cancelled,
            TaskStatus::DeadLettered,
        ] {
            for target in TaskStatus::ALL {
                assert!(
                    !TaskStateMachine::can_transition(terminal, *target),
                    "{terminal} must not transition to {target}"
                );
            }
        }
    }

    #[test]
    fn task_retry_and_dead_letter_are_legal_only_from_failed() {
        assert!(TaskStateMachine::can_transition(
            TaskStatus::Failed,
            TaskStatus::Queued
        ));
        assert!(TaskStateMachine::can_transition(
            TaskStatus::Failed,
            TaskStatus::DeadLettered
        ));
        assert!(!TaskStateMachine::can_transition(
            TaskStatus::Running,
            TaskStatus::Queued
        ));
        assert!(!TaskStateMachine::can_transition(
            TaskStatus::Pending,
            TaskStatus::DeadLettered
        ));
    }

    #[test]
    fn legal_targets_match_the_table() {
        let targets = ExecutionStateMachine::legal_targets(ExecutionStatus::Running);
        assert_eq!(
            targets,
            vec![
                ExecutionStatus::Paused,
                ExecutionStatus::Completed,
                ExecutionStatus::Failed,
                ExecutionStatus::Cancelled,
            ]
        );
        let task_targets = TaskStateMachine::legal_targets(TaskStatus::Queued);
        assert_eq!(
            task_targets,
            vec![
                TaskStatus::Running,
                TaskStatus::Failed,
                TaskStatus::Cancelled
            ]
        );
    }

    #[test]
    fn transition_log_bounded_and_drainable() {
        let mut log = TransitionLog::new();
        for index in 0..(MAX_RECORDED_TRANSITIONS + 10) {
            log.record(StateTransition {
                aggregate: AggregateKind::Task,
                aggregate_id: format!("task-{index}"),
                from: "queued".to_owned(),
                to: "running".to_owned(),
                actor: actor(),
                occurred_at: Timestamp::now(),
                version: u64::try_from(index).expect("fits") + 1,
                failure: None,
                correlation_id: None,
            });
        }
        assert_eq!(log.len(), MAX_RECORDED_TRANSITIONS);
        let first = log.entries().first().expect("first");
        assert_eq!(
            first.aggregate_id, "task-10",
            "oldest entries dropped first"
        );
        assert_eq!(log.drain().len(), MAX_RECORDED_TRANSITIONS);
        assert!(log.is_empty());
    }

    #[test]
    fn transition_records_expose_failure_semantics() {
        let failed = StateTransition {
            aggregate: AggregateKind::Execution,
            aggregate_id: "exec-1".to_owned(),
            from: "running".to_owned(),
            to: "failed".to_owned(),
            actor: actor(),
            occurred_at: Timestamp::now(),
            version: 3,
            failure: Some(TransitionFailure::transient("rate_limited", "slow down").expect("f")),
            correlation_id: Some("corr-1".to_owned()),
        };
        assert!(failed.is_failure());
        assert!(failed.is_retryable());

        let completed = StateTransition {
            failure: None,
            to: "completed".to_owned(),
            ..failed.clone()
        };
        assert!(!completed.is_failure());
        assert!(!completed.is_retryable());
    }

    #[test]
    fn illegal_transition_errors_name_every_part() {
        let error = illegal_transition(
            AggregateKind::Task,
            "task-42",
            "complete",
            "queued",
            "completed",
        );
        let rendered = format!("{error}");
        assert!(rendered.contains("task"), "names the aggregate: {rendered}");
        assert!(rendered.contains("task-42"), "names the id: {rendered}");
        assert!(
            rendered.contains("complete"),
            "names the action: {rendered}"
        );
        assert!(rendered.contains("queued"), "names the source: {rendered}");
        assert!(
            rendered.contains("completed"),
            "names the target: {rendered}"
        );
        assert!(matches!(error, AppError::Conflict(_)));

        let terminal = terminal_conflict(AggregateKind::Execution, "exec-9", "start", "completed");
        assert!(matches!(terminal, AppError::Conflict(_)));
        assert!(format!("{terminal}").contains("already terminal"));
    }
}
