//! PURPOSE:
//! A faithful in-memory reference implementation of
//! [`crate::durable::DurableExecutionPort`].
//!
//! This exists so the pipeline's concurrency and durability rules — capacity
//! ceilings, ownership fencing, stale-write rejection, idempotency conflicts,
//! immutable attempts — can be proven without a database. It is **not** a
//! production fallback: it is process-local and vanishes on restart, and the
//! composition roots wire the Postgres implementation instead. Every rule
//! implemented here is implemented because the SQL implementation must enforce
//! the same rule; where the database would enforce it with a constraint or a
//! `WHERE` clause, this implementation enforces it with an explicit check, and
//! the test suite asserts both.
//!
//! Layer / ownership
//! -----------------
//! `mas-application`, alongside the port it implements.
//!
//! Production responsibility
//! -------------------------
//! * Reproduce, exactly, the semantics the Postgres implementation must have:
//!   one `UNIQUE (tenant_id, idempotency_key)` ledger, compare-and-swap on the
//!   task version, ownership-gated settlement, terminal immutability.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT be wired as the production implementation.
//! * It must NOT relax a rule "because it's only a test double" — the tests
//!   assert against this implementation, so relaxing it makes the suite lie.

use mas_common::enums::{ExecutionStatus, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{EventId, ExecutionId, TaskAttemptId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{
    CancellationOutcome, Execution, IdempotencyRecord, Task, TaskAttempt, TransitionActor,
    TransitionFailure,
};
use mas_events::outbox::OutboxRecord;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::durable::{
    idempotency_conflict, immutable_attempt_conflict, ownership_conflict, stale_worker_conflict,
    terminal_execution_conflict, CancellationReceipt, CancellationRequest, CancellationResult,
    ClaimQuery, DurableExecutionPort, ExecutionFinalization, ExecutionOutcome, PipelineCounters,
    SettlementReceipt, SubmissionIntent, SubmissionKind, SubmissionReceipt, TaskClaim, TaskOutcome,
    TaskSettlement, UsageObservation,
};
use crate::durable_events as events;
use crate::durable_events::EventContext;
use crate::pipeline_metrics::{Outcome, PipelineMetrics};

/// Default backoff applied when a failed task is requeued.
const DEFAULT_RETRY_DELAY: Duration = Duration::from_millis(250);

/// All durable state, behind one lock so a unit of work is atomic with respect
/// to concurrent callers — the in-memory equivalent of one transaction.
#[derive(Debug, Default)]
struct MemoryState {
    executions: BTreeMap<ExecutionId, Execution>,
    tasks: BTreeMap<TaskId, Task>,
    attempts: BTreeMap<TaskAttemptId, TaskAttempt>,
    /// The `(tenant, key)` uniqueness the schema enforces with a constraint.
    idempotency: BTreeMap<(TenantId, String), IdempotencyRecord>,
    outbox: BTreeMap<EventId, OutboxRecord>,
    usage: Vec<UsageObservation>,
    /// Monotonic sequence used to keep claim ordering deterministic when two
    /// tasks share a priority and an availability time.
    sequence: u64,
    /// Enqueue order, so claim ordering can fall back to FIFO.
    enqueue_order: BTreeMap<TaskId, u64>,
}

/// The reference implementation.
#[derive(Debug, Default)]
pub struct InMemoryDurableExecution {
    state: Mutex<MemoryState>,
    /// Named pipeline metrics. Emitted from the same code path as the durable
    /// writes, so the counters cannot drift from the state they describe.
    metrics: PipelineMetrics,
}

impl InMemoryDurableExecution {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The metrics emitter this store writes to.
    ///
    /// Exposed so a test can assert that a submission, a replay and a conflict
    /// each move the counter they are supposed to move.
    #[must_use]
    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    /// Test/SRE introspection: every outbox row, in event-id order.
    #[must_use]
    pub fn outbox(&self) -> Vec<OutboxRecord> {
        self.lock().outbox.values().cloned().collect()
    }

    /// Test introspection: every committed usage observation.
    #[must_use]
    pub fn usage(&self) -> Vec<UsageObservation> {
        self.lock().usage.clone()
    }

    /// Test introspection: every attempt ever created.
    #[must_use]
    pub fn attempts(&self) -> Vec<TaskAttempt> {
        self.lock().attempts.values().cloned().collect()
    }

    /// Test introspection: one execution by id (ignores tenant scoping).
    #[must_use]
    pub fn execution(&self, id: ExecutionId) -> Option<Execution> {
        self.lock().executions.get(&id).cloned()
    }

    /// Test introspection: one task by id (ignores tenant scoping).
    #[must_use]
    pub fn task(&self, id: TaskId) -> Option<Task> {
        self.lock().tasks.get(&id).cloned()
    }

    /// Test introspection: the idempotency ledger.
    #[must_use]
    pub fn idempotency(&self) -> Vec<IdempotencyRecord> {
        self.lock().idempotency.values().cloned().collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn event_context(intent: &SubmissionIntent, execution_id: ExecutionId) -> EventContext {
        EventContext {
            tenant_id: intent.tenant_id,
            organization_id: intent.organization_id,
            project_id: intent.project_id,
            execution_id,
            correlation_id: intent.correlation_id.clone(),
            actor: intent.actor.label(),
            occurred_at: intent.now,
        }
    }

    /// Deterministic ordering: urgent priority first, then earliest
    /// availability, then enqueue order (FIFO) so no task starves.
    fn ordered(state: &MemoryState, ids: Vec<TaskId>) -> Vec<TaskId> {
        let mut ranked: Vec<(u8, i64, u64, TaskId)> = ids
            .into_iter()
            .map(|id| {
                let task = state.tasks.get(&id);
                (
                    task.map_or(0, |task| task.priority.rank()),
                    task.map_or(i64::MAX, |task| task.available_at().to_unix_ms()),
                    state.enqueue_order.get(&id).copied().unwrap_or(u64::MAX),
                    id,
                )
            })
            .collect();
        ranked.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then(left.1.cmp(&right.1))
                .then(left.2.cmp(&right.2))
        });
        ranked.into_iter().map(|row| row.3).collect()
    }

    /// Tasks that are queued, available and unowned right now.
    fn ready_candidates(state: &MemoryState, query: &ClaimQuery) -> Vec<TaskId> {
        let ids: Vec<TaskId> = state
            .tasks
            .values()
            .filter(|task| Self::in_scope(task, query))
            .filter(|task| task.is_claimable_at(&query.now))
            .map(|task| task.id)
            .collect();
        Self::ordered(state, ids)
    }

    /// Tasks left `running` by a worker that stopped renewing its lease.
    ///
    /// These are the crash-recovery surface. Without reclaiming them, a
    /// worker that dies mid-task leaves the task permanently unclaimable and
    /// permanently "running" — a silent, unrecoverable loss.
    fn abandoned_candidates(state: &MemoryState, query: &ClaimQuery) -> Vec<TaskId> {
        let ids: Vec<TaskId> = state
            .tasks
            .values()
            .filter(|task| Self::in_scope(task, query))
            .filter(|task| task.status == TaskStatus::Running && !task.is_owned_at(&query.now))
            .map(|task| task.id)
            .collect();
        Self::ordered(state, ids)
    }

    /// Restricts a candidate list to the one task a query named, if it did.
    fn narrowed(ids: Vec<TaskId>, query: &ClaimQuery) -> Vec<TaskId> {
        match query.task_id {
            Some(task_id) => ids.into_iter().filter(|id| *id == task_id).collect(),
            None => ids,
        }
    }

    fn in_scope(task: &Task, query: &ClaimQuery) -> bool {
        if let Some(tenant) = query.tenant_id {
            if task.tenant_id != tenant {
                return false;
            }
        }
        match task.deadline {
            Some(deadline) => deadline.is_after(&query.now),
            None => true,
        }
    }

    /// Closes out an abandoned attempt and puts the task back on the queue.
    ///
    /// Returns `false` when the task could not be requeued — either because it
    /// was already reclaimed by someone else, or because its attempt budget
    /// was spent and it was dead-lettered instead.
    fn reclaim_abandoned(
        state: &mut MemoryState,
        task_id: TaskId,
        actor: &TransitionActor,
        query: &ClaimQuery,
    ) -> Result<bool> {
        let Some(task) = state.tasks.get_mut(&task_id) else {
            return Ok(false);
        };
        if task.status != TaskStatus::Running || task.is_owned_at(&query.now) {
            return Ok(false);
        }
        if !task.has_attempts_remaining() {
            task.dead_letter(actor, "lease_expired_attempts_exhausted")?;
            Self::close_attempt(state, task_id, actor, query)?;
            return Ok(false);
        }
        let failure = TransitionFailure::transient(
            "lease_expired",
            "the owning worker stopped renewing its lease",
        )?;
        task.fail(actor, failure.clone())?;
        Self::close_attempt(state, task_id, actor, query)?;
        let task = state
            .tasks
            .get_mut(&task_id)
            .ok_or_else(|| AppError::not_found("task", task_id.to_string()))?;
        task.retry(actor, Duration::ZERO)?;
        task.not_before = Some(query.now);
        let task_snapshot = task.clone();
        let context = EventContext {
            tenant_id: task_snapshot.tenant_id,
            organization_id: task_snapshot.organization_id,
            project_id: task_snapshot.project_id,
            execution_id: task_snapshot.execution_id.unwrap_or_else(ExecutionId::new),
            correlation_id: task_snapshot.correlation_id.clone(),
            actor: actor.label(),
            occurred_at: query.now,
        };
        let record = events::task_failed(
            &context,
            task_id,
            task_snapshot.last_attempt_id,
            "lease_expired",
            true,
            task_snapshot.version(),
            true,
        )?;
        state.outbox.insert(record.event_id, record);
        if let Some(task) = state.tasks.get_mut(&task_id) {
            task.take_transitions();
        }
        Ok(true)
    }

    /// Marks the still-open attempt of an abandoned task as failed.
    ///
    /// An attempt that is never closed would keep `active_attempts` inflated
    /// forever, which in turn would pin the worker at its capacity ceiling.
    fn close_attempt(
        state: &mut MemoryState,
        task_id: TaskId,
        actor: &TransitionActor,
        query: &ClaimQuery,
    ) -> Result<()> {
        let attempt_id = state
            .tasks
            .get(&task_id)
            .and_then(|task| task.last_attempt_id);
        let Some(attempt_id) = attempt_id else {
            return Ok(());
        };
        let Some(attempt) = state.attempts.get_mut(&attempt_id) else {
            return Ok(());
        };
        if attempt.status.is_terminal() {
            return Ok(());
        }
        let _ = actor;
        attempt.fail(
            "lease_expired",
            "worker lease lapsed mid-run",
            true,
            &query.now,
        )?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl DurableExecutionPort for InMemoryDurableExecution {
    async fn submit(&self, intent: SubmissionIntent) -> Result<SubmissionReceipt> {
        intent.validate()?;
        let mut state = self.lock();

        // ── Idempotency: DB-enforced uniqueness, then classify. ───────────
        if let Some(key) = intent.idempotency_key.as_deref() {
            // A BTreeMap keyed by (tenant, key) is the in-memory equivalent of
            // `UNIQUE (tenant_id, idempotency_key)`: the second insert cannot
            // succeed, so there is no read-then-write race window.
            if let Some(existing) = state.idempotency.get(&(intent.tenant_id, key.to_owned())) {
                match IdempotencyRecord::decide(Some(existing), &intent.fingerprint) {
                    mas_domain::IdempotencyDecision::New => {
                        unreachable!("a stored record is never new")
                    },
                    mas_domain::IdempotencyDecision::Conflict { .. } => {
                        // A conflict is a permanent rejection, not a retry
                        // candidate: the same key can never mean two things.
                        self.metrics.idempotency_conflict();
                        self.metrics.submission(Outcome::Conflict);
                        return Err(idempotency_conflict(key));
                    },
                    mas_domain::IdempotencyDecision::Replay { record } => {
                        let execution = state
                            .executions
                            .get(&record.execution_id.ok_or_else(|| {
                                AppError::internal("idempotency record without an execution")
                            })?)
                            .cloned()
                            .ok_or_else(|| {
                                AppError::internal("idempotency reference to vanished execution")
                            })?;
                        let task = record
                            .task_id
                            .and_then(|id| state.tasks.get(&id).cloned())
                            .ok_or_else(|| {
                                AppError::internal("idempotency reference to vanished task")
                            })?;
                        let context = EventContext {
                            tenant_id: intent.tenant_id,
                            organization_id: intent.organization_id,
                            project_id: intent.project_id,
                            execution_id: execution.id,
                            correlation_id: intent.correlation_id.clone(),
                            actor: intent.actor.label(),
                            occurred_at: intent.now,
                        };
                        // A replay emits a replay event so operators can see
                        // the duplicate, but creates no new work and no new
                        // attempt — that is what makes it safe.
                        let replayed = events::execution_submitted(
                            &context,
                            task.id,
                            execution.status,
                            execution.version(),
                            true,
                        )?;
                        state.outbox.insert(replayed.event_id, replayed.clone());
                        self.metrics.idempotency_replay();
                        self.metrics.submission(Outcome::Replay);
                        return Ok(SubmissionReceipt {
                            kind: SubmissionKind::Replayed,
                            execution,
                            task,
                            attempt: None,
                            transitions: Vec::new(),
                            events: vec![replayed],
                        });
                    },
                }
            }
        }

        // ── Create the execution. ────────────────────────────────────────
        let mut execution = Execution::new(
            intent.tenant_id,
            intent.organization_id,
            intent.project_id,
            intent.workflow_id,
            intent.agent_id,
            intent.input.clone(),
            intent.correlation_id.clone(),
            intent.created_by.clone(),
        )?;
        if let Some(deadline) = intent.deadline {
            execution.set_deadline(deadline)?;
        }
        let mut task = Task::new(
            intent.tenant_id,
            intent.organization_id,
            intent.project_id,
            intent.agent_id,
            intent.workflow_id,
            intent.operation.clone(),
            intent.input.clone(),
            intent.priority,
            intent.idempotency_key.clone().unwrap_or_else(|| {
                // No client key: derive a stable per-execution guard so the
                // task's own uniqueness constraint still protects it.
                format!("exec:{}", execution.id)
            }),
            intent.created_by.clone(),
        )?
        .with_request_id(intent.request_id)
        .with_correlation_id(intent.correlation_id.clone())?;
        if let Some(version_id) = intent.agent_version_id {
            task = task.with_agent_version_id(version_id)?;
        }
        task.max_attempts = intent.max_attempts;
        if let Some(deadline) = intent.deadline {
            task.deadline = Some(deadline);
        }
        task.attach_execution(execution.id)?;

        // Pending → Queued: the execution and its task become dispatchable in
        // the same transaction that creates them.
        let enqueued = execution.enqueue(&intent.actor)?;
        let queued = task.queue(&intent.actor)?;

        // The aggregates stamp themselves with the wall clock; the unit of
        // work was given an injected reading, so re-stamp them onto it. Without
        // this, an injected-clock submission would be "created" in the future
        // relative to its own transaction and never become claimable.
        execution.created_at = intent.now;
        execution.updated_at = intent.now;
        task.created_at = intent.now;
        task.updated_at = intent.now;
        task.queued_at = Some(intent.now);

        let context = Self::event_context(&intent, execution.id);
        let submitted = events::execution_submitted(
            &context,
            task.id,
            execution.status,
            execution.version(),
            false,
        )?;
        let created = events::task_created(&context, task.id, task.status, task.version())?;

        // ── Optional initial attempt. ────────────────────────────────────
        let mut attempt = None;
        let mut attempt_event = None;
        if intent.create_initial_attempt {
            let started = TaskAttempt::start(
                intent.tenant_id,
                task.id,
                Some(execution.id),
                task.attempt_count + 1,
                &intent.actor.id,
                None,
                Some("initial".to_owned()),
                &intent.now,
            )?;
            attempt_event = Some(events::task_claimed(
                &context,
                task.id,
                started.id,
                started.attempt_number,
                &intent.actor.id,
                task.version(),
            )?);
            attempt = Some(started);
        }

        // ── Commit (one lock scope = one transaction). ───────────────────
        // Drained from the aggregates, never re-listed by hand: a second,
        // hand-written list is how a transition ends up audited twice.
        let _ = (enqueued, queued);
        let mut transitions = execution.take_transitions();
        transitions.extend(task.take_transitions());

        if let Some(key) = intent.idempotency_key.clone() {
            let record = IdempotencyRecord::new(
                intent.tenant_id,
                key,
                intent.fingerprint.clone(),
                None,
                Some(execution.id),
                Some(task.id),
                &intent.now,
            )?;
            state
                .idempotency
                .insert((intent.tenant_id, record.idempotency_key.clone()), record);
        }
        if let Some(started) = attempt.clone() {
            state.attempts.insert(started.id, started);
        }
        state.executions.insert(execution.id, execution.clone());
        state.tasks.insert(task.id, task.clone());
        state.sequence += 1;
        let sequence = state.sequence;
        state.enqueue_order.insert(task.id, sequence);

        let mut committed = vec![submitted, created];
        if let Some(record) = attempt_event {
            committed.push(record);
        }
        for record in &committed {
            state.outbox.insert(record.event_id, record.clone());
        }

        self.metrics.submission(Outcome::Accepted);
        self.metrics.task_created();

        Ok(SubmissionReceipt {
            kind: SubmissionKind::Created,
            execution,
            task,
            attempt,
            transitions,
            events: committed,
        })
    }

    async fn claim(&self, query: ClaimQuery) -> Result<Vec<TaskClaim>> {
        if query.capacity == 0 {
            return Ok(Vec::new());
        }
        if query.worker.trim().is_empty() {
            return Err(AppError::invalid_field(
                "worker",
                "required",
                "a claim must name the worker that will own the work",
            ));
        }
        let mut state = self.lock();

        // Ready work first, then abandoned work: a task whose lease lapsed is
        // reclaimed only when there is spare capacity, so recovery never
        // starves the normal queue.
        let ready = Self::narrowed(Self::ready_candidates(&state, &query), &query);
        let abandoned = Self::narrowed(Self::abandoned_candidates(&state, &query), &query);
        let actor = TransitionActor::worker(&query.worker)?;
        let mut claims = Vec::new();

        let mut ordered: Vec<(TaskId, bool)> = ready.into_iter().map(|id| (id, false)).collect();
        ordered.extend(abandoned.into_iter().map(|id| (id, true)));

        for (task_id, needs_reclaim) in ordered {
            if claims.len() >= query.capacity {
                break;
            }
            if needs_reclaim && !Self::reclaim_abandoned(&mut state, task_id, &actor, &query)? {
                // The abandoned task was dead-lettered instead (its attempt
                // budget was already spent), so it consumed no capacity.
                continue;
            }
            let Some(task) = state.tasks.get(&task_id) else {
                continue;
            };
            if task.status != TaskStatus::Queued || !task.is_claimable_at(&query.now) {
                continue;
            }

            let attempt_number = task.attempt_count + 1;
            let tenant_id = task.tenant_id;
            let organization_id = task.organization_id;
            let project_id = task.project_id;
            let execution_id = task.execution_id;
            let correlation_id = task.correlation_id.clone();

            let attempt = TaskAttempt::start(
                tenant_id,
                task_id,
                execution_id,
                attempt_number,
                &query.worker,
                Some(query.lease_until),
                if attempt_number > 1 {
                    Some(format!("retry:{attempt_number}"))
                } else {
                    Some("initial".to_owned())
                },
                &query.now,
            )?;
            let attempt_id = attempt.id;

            // Mutate the task in its own scope so the execution can be mutated
            // afterwards (the maps are disjoint, but the lock is not).
            {
                let task = state
                    .tasks
                    .get_mut(&task_id)
                    .ok_or_else(|| AppError::not_found("task", task_id.to_string()))?;
                task.start(&actor, &query.worker, query.lease_until, attempt_id)?;
            }

            // The execution moves Queued → Running in the *same* unit as the
            // claim: an execution whose task is running but which still reads
            // "queued" is a lie the rest of the platform would act on.
            let mut execution_transition = None;
            if let Some(execution_id) = execution_id {
                if let Some(execution) = state.executions.get_mut(&execution_id) {
                    if execution.status == ExecutionStatus::Queued {
                        let transition = execution.start(&actor)?;
                        execution.updated_at = query.now;
                        execution_transition = Some((execution_id, transition));
                    }
                }
            }

            let context = EventContext {
                tenant_id,
                organization_id,
                project_id,
                execution_id: execution_id.unwrap_or_else(ExecutionId::new),
                correlation_id,
                actor: format!("worker:{}", query.worker),
                occurred_at: query.now,
            };
            let task_snapshot = state
                .tasks
                .get(&task_id)
                .cloned()
                .ok_or_else(|| AppError::not_found("task", task_id.to_string()))?;
            let mut committed = vec![events::task_claimed(
                &context,
                task_id,
                attempt_id,
                attempt_number,
                &query.worker,
                task_snapshot.version(),
            )?];
            if let Some((execution_id, _)) = execution_transition.as_ref() {
                if let Some(execution) = state.executions.get(execution_id) {
                    committed.push(events::execution_started(&context, execution.version())?);
                }
            }
            for record in &committed {
                state.outbox.insert(record.event_id, record.clone());
            }
            state.attempts.insert(attempt_id, attempt.clone());
            if let Some((execution_id, _)) = execution_transition {
                if let Some(execution) = state.executions.get_mut(&execution_id) {
                    execution.take_transitions();
                }
            }
            if let Some(task) = state.tasks.get_mut(&task_id) {
                task.take_transitions();
            }

            claims.push(TaskClaim {
                task: task_snapshot,
                attempt,
                lease_until: query.lease_until,
            });
        }
        Ok(claims)
    }

    async fn renew_lease(&self, task_id: TaskId, worker: &str, until: Timestamp) -> Result<bool> {
        let mut state = self.lock();
        let Some(task) = state.tasks.get_mut(&task_id) else {
            return Ok(false);
        };
        match task.renew_ownership(worker, until) {
            Ok(()) => Ok(true),
            Err(AppError::Conflict(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn settle(&self, settlement: TaskSettlement) -> Result<SettlementReceipt> {
        // The whole unit reads, mutates and re-inserts cloned aggregates, then
        // commits every row at the end — the in-memory equivalent of one
        // transaction, so a failure part-way through leaves nothing behind.
        let mut state = self.lock();

        // ── Stale-worker fences: version, then ownership. ───────────────
        let observed = state
            .tasks
            .get(&settlement.task_id)
            .cloned()
            .ok_or_else(|| AppError::not_found("task", settlement.task_id.to_string()))?;
        if observed.version() != settlement.expected_version {
            return Err(stale_worker_conflict(
                settlement.task_id,
                settlement.expected_version,
                observed.version(),
            ));
        }
        if !observed.is_owned_by_at(&settlement.worker, &settlement.now) {
            return Err(ownership_conflict(
                settlement.task_id,
                &settlement.worker,
                observed.locked_by.as_deref(),
            ));
        }
        let observed_attempt = state
            .attempts
            .get(&settlement.attempt_id)
            .cloned()
            .ok_or_else(|| {
                AppError::not_found("task attempt", settlement.attempt_id.to_string())
            })?;
        if observed_attempt.status.is_terminal() {
            return Err(immutable_attempt_conflict(
                observed_attempt.id,
                observed_attempt.status,
            ));
        }

        let mut task = observed.clone();
        let mut attempt = observed_attempt.clone();
        let task_id = task.id;
        let tenant_id = task.tenant_id;
        let organization_id = task.organization_id;
        let project_id = task.project_id;
        let execution_id = task.execution_id;
        let attempt_id = attempt.id;

        let mut usage = Vec::new();
        let mut requeued = false;
        let mut dead_lettered = false;

        match &settlement.outcome {
            TaskOutcome::Succeeded {
                usage: observed_usage,
            } => {
                attempt.started_at = attempt.started_at.min(settlement.now);
                attempt.succeed(&settlement.now)?;
                task.complete(&settlement.actor)?;
                // Usage is metered only on success, and only for what was
                // actually consumed — never a synthetic counter.
                usage = observed_usage.observations();
            },
            TaskOutcome::Cancelled => {
                attempt.cancel(&settlement.now)?;
                match task.cancel(&settlement.actor)? {
                    CancellationOutcome::Cancelled(_) | CancellationOutcome::AlreadyCancelled => {},
                }
            },
            TaskOutcome::Failed { failure } => {
                attempt.fail(
                    failure.code.clone(),
                    failure.message.clone(),
                    failure.retryable,
                    &settlement.now,
                )?;
                task.fail(&settlement.actor, failure.clone())?;
                if failure.retryable && task.has_attempts_remaining() {
                    task.retry(&settlement.actor, DEFAULT_RETRY_DELAY)?;
                    // Same re-stamping rule as submission: the backoff must be
                    // measured from the unit's clock, not the wall clock.
                    task.not_before = settlement.now.checked_add(DEFAULT_RETRY_DELAY);
                    requeued = true;
                } else {
                    let reason = if failure.retryable {
                        "attempts_exhausted"
                    } else {
                        "terminal_failure"
                    };
                    task.dead_letter(&settlement.actor, reason)?;
                    dead_lettered = true;
                }
            },
        }

        // ── Execution finalization, in the same unit. ────────────────────
        //
        // Only when the task is genuinely finished. A task being requeued
        // means its execution is still running: finalizing it as failed here
        // would make the eventual success an illegal transition and doom the
        // retry.
        let mut execution_snapshot: Option<Execution> = None;
        if !requeued {
            if let Some(finalization) = settlement.execution.clone() {
                let mut execution = state
                    .executions
                    .get(&finalization.execution_id)
                    .cloned()
                    .ok_or_else(|| {
                        AppError::not_found("execution", finalization.execution_id.to_string())
                    })?;
                match &finalization.outcome {
                    ExecutionOutcome::Completed => {
                        execution.complete(&settlement.actor, None)?;
                    },
                    ExecutionOutcome::Failed { failure } => {
                        execution.fail(&settlement.actor, failure.clone())?;
                    },
                    ExecutionOutcome::Cancelled => match execution.cancel(&settlement.actor)? {
                        CancellationOutcome::Cancelled(_)
                        | CancellationOutcome::AlreadyCancelled => {},
                    },
                }
                execution_snapshot = Some(execution);
            }
        }

        // ── Events. ─────────────────────────────────────────────────────
        let execution_for_context = execution_id.unwrap_or_else(ExecutionId::new);
        let context = EventContext {
            tenant_id,
            organization_id,
            project_id,
            execution_id: execution_for_context,
            correlation_id: task.correlation_id.clone(),
            actor: settlement.actor.label(),
            occurred_at: settlement.now,
        };
        let mut committed: Vec<OutboxRecord> = Vec::new();
        match &settlement.outcome {
            TaskOutcome::Succeeded {
                usage: observed_usage,
            } => {
                committed.push(events::task_succeeded(
                    &context,
                    task_id,
                    Some(attempt_id),
                    task.version(),
                )?);
                if !observed_usage.is_empty() {
                    committed.push(events::usage_recorded(
                        &context,
                        task_id,
                        attempt_id,
                        observed_usage,
                    )?);
                }
            },
            TaskOutcome::Cancelled => {
                committed.push(events::task_cancelled(&context, task_id, task.version())?);
            },
            TaskOutcome::Failed { failure } => {
                committed.push(events::task_failed(
                    &context,
                    task_id,
                    Some(attempt_id),
                    &failure.code,
                    failure.retryable,
                    task.version(),
                    requeued,
                )?);
                if dead_lettered {
                    committed.push(events::task_dead_lettered(
                        &context,
                        task_id,
                        task.version(),
                    )?);
                }
            },
        }
        if let Some(execution) = execution_snapshot.as_ref() {
            let event = match &settlement.execution {
                Some(ExecutionFinalization {
                    outcome: ExecutionOutcome::Completed,
                    ..
                }) => events::execution_completed(&context, execution.version())?,
                Some(ExecutionFinalization {
                    outcome: ExecutionOutcome::Failed { failure },
                    ..
                }) => events::execution_failed(
                    &context,
                    &failure.code,
                    failure.retryable,
                    execution.version(),
                )?,
                _ => events::execution_cancelled(&context, execution.version())?,
            };
            committed.push(event);
        }

        // ── Commit: every row lands here or nothing lands at all. ────────
        let mut transitions = Vec::new();
        if requeued {
            state.sequence += 1;
            let sequence = state.sequence;
            state.enqueue_order.insert(task_id, sequence);
        }
        transitions.extend(task.take_transitions());
        state.tasks.insert(task_id, task.clone());
        state.attempts.insert(attempt_id, attempt.clone());
        if let Some(execution) = execution_snapshot.as_ref() {
            transitions.extend(execution.transitions().entries().to_vec());
            let mut committed_execution = execution.clone();
            transitions.extend(committed_execution.take_transitions());
            state.executions.insert(execution.id, committed_execution);
        }
        for observation in &usage {
            state.usage.push(observation.clone());
        }
        for record in &committed {
            state.outbox.insert(record.event_id, record.clone());
        }

        Ok(SettlementReceipt {
            task,
            attempt,
            execution: execution_snapshot,
            requeued,
            dead_lettered,
            transitions,
            events: committed,
            usage,
        })
    }

    async fn cancel(&self, request: CancellationRequest) -> Result<CancellationReceipt> {
        let mut state = self.lock();
        let execution = state
            .executions
            .get_mut(&request.execution_id)
            .cloned()
            .ok_or_else(|| AppError::not_found("execution", request.execution_id.to_string()))?;
        if execution.tenant_id != request.tenant_id {
            // Existence is hidden across tenants, not merely forbidden.
            return Err(AppError::not_found(
                "execution",
                request.execution_id.to_string(),
            ));
        }

        let context = EventContext {
            tenant_id: execution.tenant_id,
            organization_id: execution.organization_id,
            project_id: execution.project_id,
            execution_id: execution.id,
            correlation_id: request.correlation_id.clone(),
            actor: request.actor.label(),
            occurred_at: request.now,
        };

        // A completed or failed execution is never reopened.
        if matches!(
            execution.status,
            ExecutionStatus::Completed | ExecutionStatus::Failed
        ) {
            return Err(terminal_execution_conflict(execution.id, execution.status));
        }
        if execution.status == ExecutionStatus::Cancelled {
            return Ok(CancellationReceipt {
                result: CancellationResult::AlreadyCancelled,
                execution,
                task: None,
                signal: None,
                transitions: Vec::new(),
                events: Vec::new(),
            });
        }

        let execution = state
            .executions
            .get_mut(&request.execution_id)
            .ok_or_else(|| AppError::not_found("execution", request.execution_id.to_string()))?;

        // Running/paused: set the cooperative flag; the runtime settles it as
        // cancelled at the next safe point. Pending/queued: cancel outright.
        let mut committed = Vec::new();
        let mut signal = None;
        let result;
        match execution.status {
            ExecutionStatus::Running | ExecutionStatus::Paused => {
                signal = Some(execution.request_cancellation(&request.actor)?);
                let already = signal.as_ref().is_some_and(|s| s.already_requested);
                committed.push(events::execution_cancellation_requested(
                    &context,
                    execution.status,
                    execution.version(),
                    already,
                )?);
                result = CancellationResult::SignalSet;
            },
            _ => {
                match execution.cancel(&request.actor)? {
                    CancellationOutcome::Cancelled(_) => {
                        result = CancellationResult::Cancelled;
                    },
                    CancellationOutcome::AlreadyCancelled => {
                        result = CancellationResult::AlreadyCancelled;
                    },
                }
                committed.push(events::execution_cancelled(&context, execution.version())?);
            },
        }

        // Cancel the live task too, so a queued task is not picked up later.
        let mut task_snapshot = None;
        let live_task_id = state
            .tasks
            .values()
            .find(|task| {
                task.execution_id == Some(request.execution_id)
                    && matches!(
                        task.status,
                        TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running
                    )
            })
            .map(|task| task.id);
        if let Some(task_id) = live_task_id {
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| AppError::not_found("task", task_id.to_string()))?;
            match task.cancel(&request.actor)? {
                CancellationOutcome::Cancelled(_) => {
                    committed.push(events::task_cancelled(&context, task_id, task.version())?);
                },
                CancellationOutcome::AlreadyCancelled => {},
            }
            task_snapshot = Some(task.clone());
        }

        let execution_snapshot = state
            .executions
            .get(&request.execution_id)
            .cloned()
            .ok_or_else(|| AppError::not_found("execution", request.execution_id.to_string()))?;
        let mut transitions = execution_snapshot.transitions().entries().to_vec();
        if let Some(task) = task_snapshot.as_ref() {
            transitions.extend(task.transitions().entries().to_vec());
        }
        if let Some(execution) = state.executions.get_mut(&request.execution_id) {
            transitions.extend(execution.take_transitions());
        }
        if let Some(task_id) = live_task_id {
            if let Some(task) = state.tasks.get_mut(&task_id) {
                transitions.extend(task.take_transitions());
            }
        }
        for record in &committed {
            state.outbox.insert(record.event_id, record.clone());
        }

        Ok(CancellationReceipt {
            result,
            execution: execution_snapshot,
            task: task_snapshot,
            signal,
            transitions,
            events: committed,
        })
    }

    async fn transition(
        &self,
        request: crate::durable::ExecutionTransitionRequest,
    ) -> Result<crate::durable::ExecutionTransitionReceipt> {
        use crate::durable::{ExecutionAction, ExecutionTransitionReceipt};

        let mut state = self.lock();
        let execution = state
            .executions
            .get(&request.execution_id)
            .cloned()
            .ok_or_else(|| AppError::not_found("execution", request.execution_id.to_string()))?;
        if execution.tenant_id != request.tenant_id {
            return Err(AppError::not_found(
                "execution",
                request.execution_id.to_string(),
            ));
        }

        let live = state
            .executions
            .get_mut(&request.execution_id)
            .ok_or_else(|| AppError::not_found("execution", request.execution_id.to_string()))?;

        // A cancellation request outranks completion: a run that was asked to
        // stop must end as cancelled, never as completed.
        if matches!(request.action, ExecutionAction::Complete) && live.cancellation_requested() {
            return Err(AppError::conflict(
                "cancellation was requested; execution must end as cancelled",
            ));
        }

        let transition = match &request.action {
            ExecutionAction::Start => live.start(&request.actor)?,
            ExecutionAction::Pause => live.pause(&request.actor)?,
            ExecutionAction::Resume => live.resume(&request.actor)?,
            ExecutionAction::Complete => live.complete(&request.actor, None)?,
            ExecutionAction::Fail { failure } => live.fail(&request.actor, failure.clone())?,
        };

        live.updated_at = request.now;
        let snapshot = live.clone();
        let context = EventContext {
            tenant_id: snapshot.tenant_id,
            organization_id: snapshot.organization_id,
            project_id: snapshot.project_id,
            execution_id: snapshot.id,
            correlation_id: request.correlation_id.clone(),
            actor: request.actor.label(),
            occurred_at: request.now,
        };
        let mut committed = Vec::new();
        match &request.action {
            ExecutionAction::Complete => {
                committed.push(events::execution_completed(&context, snapshot.version())?)
            },
            ExecutionAction::Fail { failure } => committed.push(events::execution_failed(
                &context,
                &failure.code,
                failure.retryable,
                snapshot.version(),
            )?),
            ExecutionAction::Start | ExecutionAction::Pause | ExecutionAction::Resume => {},
        }
        for record in &committed {
            state.outbox.insert(record.event_id, record.clone());
        }
        let mut transitions = vec![transition.clone()];
        if let Some(live) = state.executions.get_mut(&request.execution_id) {
            transitions.extend(live.take_transitions());
        }

        Ok(ExecutionTransitionReceipt {
            execution: snapshot,
            transition,
            events: committed,
        })
    }

    async fn list_executions(&self, tenant: TenantId, limit: usize) -> Result<Vec<Execution>> {
        let state = self.lock();
        Ok(state
            .executions
            .values()
            .filter(|execution| execution.tenant_id == tenant)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn counters(&self, worker: &str, at: &Timestamp) -> Result<PipelineCounters> {
        let state = self.lock();
        let now = *at;
        let active_attempts = state
            .attempts
            .values()
            .filter(|attempt| {
                !attempt.status.is_terminal()
                    && attempt.worker_id.as_deref() == Some(worker)
                    && !attempt.is_lease_expired_at(&now)
            })
            .count();
        let claimable_tasks = state
            .tasks
            .values()
            .filter(|task| task.is_claimable_at(&now))
            .count();
        Ok(PipelineCounters {
            active_attempts: u64::try_from(active_attempts).unwrap_or(u64::MAX),
            outbox_backlog: u64::try_from(
                state
                    .outbox
                    .values()
                    .filter(|record| {
                        !matches!(
                            record.status,
                            mas_common::enums::EventStatus::Published
                                | mas_common::enums::EventStatus::DeadLettered
                        )
                    })
                    .count(),
            )
            .unwrap_or(u64::MAX),
            claimable_tasks: u64::try_from(claimable_tasks).unwrap_or(u64::MAX),
        })
    }

    async fn load_task(
        &self,
        tenant: TenantId,
        task_id: TaskId,
    ) -> Result<Option<(Task, Option<Execution>)>> {
        let state = self.lock();
        let Some(task) = state.tasks.get(&task_id) else {
            return Ok(None);
        };
        if task.tenant_id != tenant {
            return Ok(None);
        }
        let execution = task
            .execution_id
            .and_then(|id| state.executions.get(&id).cloned());
        Ok(Some((task.clone(), execution)))
    }

    async fn load_execution(
        &self,
        tenant: TenantId,
        execution_id: ExecutionId,
    ) -> Result<Option<Execution>> {
        let state = self.lock();
        Ok(state
            .executions
            .get(&execution_id)
            .filter(|execution| execution.tenant_id == tenant)
            .cloned())
    }
}

/// Convenience for tests and dev loops: a redacted, retryable failure.
///
/// Deliberately lives here rather than in the port so production code builds
/// its failures from real classification, not from a helper.
#[must_use]
pub fn transient_failure(code: &str) -> TransitionFailure {
    TransitionFailure::transient(code, code.replace('_', " ")).unwrap_or_else(|_| {
        TransitionFailure::transient("transient", "transient failure")
            .expect("literal failure is valid")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable::{ExecutionUsage, SubmissionIntent};
    use crate::pipeline_metrics;
    use mas_common::enums::TaskPriority;
    use mas_common::ids::{OrganizationId, ProjectId};

    fn now() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    /// One fixed tenant for the whole test: a helper that minted a fresh
    /// tenant per call would make every submission land in its own scope, so
    /// no idempotency key could ever collide.
    fn test_tenant() -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(0xF00D))
    }

    fn intent(key: &str, fingerprint: &str) -> SubmissionIntent {
        SubmissionIntent {
            tenant_id: test_tenant(),
            organization_id: OrganizationId::new(),
            project_id: ProjectId::new(),
            workflow_id: Some(mas_common::ids::WorkflowId::new()),
            agent_id: None,
            agent_version_id: None,
            input: serde_json::json!({"prompt": "hi"}),
            idempotency_key: Some(key.to_owned()),
            fingerprint: fingerprint.to_owned(),
            operation: "execution.run".to_owned(),
            priority: TaskPriority::Normal,
            max_attempts: 3,
            deadline: None,
            correlation_id: "corr-1".to_owned(),
            request_id: uuid::Uuid::now_v7(),
            created_by: "user-1".to_owned(),
            actor: mas_domain::TransitionActor::principal("user-1", TenantId::new())
                .expect("actor"),
            create_initial_attempt: false,
            now: now(),
        }
    }

    const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn claim_query(worker: &str, capacity: usize) -> ClaimQuery {
        ClaimQuery {
            worker: worker.to_owned(),
            capacity,
            lease_until: now().checked_add(Duration::from_secs(30)).expect("ts"),
            now: now(),
            tenant_id: None,
            task_id: None,
        }
    }

    // -- named pipeline metrics --------------------------------------------

    fn counter(store: &InMemoryDurableExecution, family: &str, outcome: Outcome) -> u64 {
        let labels = mas_observability::metrics::LabelSet::build([(
            pipeline_metrics::OUTCOME_LABEL,
            outcome.as_str(),
        )])
        .expect("label");
        store.metrics().registry().counter_value(family, &labels)
    }

    fn plain_counter(store: &InMemoryDurableExecution, family: &str) -> u64 {
        store
            .metrics()
            .registry()
            .counter_value(family, &mas_observability::metrics::LabelSet::empty())
    }

    #[tokio::test]
    async fn durable_task_retains_its_originating_request_id() {
        let store = InMemoryDurableExecution::new();
        let submission = intent("request-id-task", A);
        let request_id = submission.request_id;
        let receipt = store.submit(submission).await.expect("submit");
        let (task, _) = store
            .load_task(receipt.task.tenant_id, receipt.task.id)
            .await
            .expect("load task")
            .expect("persisted task");

        assert_eq!(receipt.task.request_id, Some(request_id));
        assert_eq!(task.request_id, Some(request_id));
    }

    #[tokio::test]
    async fn a_new_submission_counts_as_accepted_and_creates_one_task() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");

        assert_eq!(
            counter(
                &store,
                pipeline_metrics::EXECUTION_SUBMISSIONS,
                Outcome::Accepted
            ),
            1
        );
        assert_eq!(plain_counter(&store, pipeline_metrics::TASK_CREATED), 1);
    }

    #[tokio::test]
    async fn a_safe_replay_counts_as_a_replay_and_creates_nothing() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("first");
        store.submit(intent("k1", A)).await.expect("replay");

        assert_eq!(
            counter(
                &store,
                pipeline_metrics::EXECUTION_SUBMISSIONS,
                Outcome::Accepted
            ),
            1
        );
        assert_eq!(
            counter(
                &store,
                pipeline_metrics::EXECUTION_SUBMISSIONS,
                Outcome::Replay
            ),
            1
        );
        assert_eq!(
            plain_counter(&store, pipeline_metrics::IDEMPOTENCY_DUPLICATES),
            1,
            "the duplicate is counted so a client retry-loop is visible"
        );
        // A replay creates no second task: the point of idempotency.
        assert_eq!(plain_counter(&store, pipeline_metrics::TASK_CREATED), 1);
    }

    #[tokio::test]
    async fn a_conflicting_fingerprint_counts_as_a_conflict_not_a_retry() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("first");
        let error = store
            .submit(intent("k1", B))
            .await
            .expect_err("same key, different request");

        assert!(
            matches!(error, AppError::Conflict(_)),
            "a fingerprint conflict is deterministic: {error:?}"
        );
        assert_eq!(
            counter(
                &store,
                pipeline_metrics::EXECUTION_SUBMISSIONS,
                Outcome::Conflict
            ),
            1
        );
        assert_eq!(
            plain_counter(&store, pipeline_metrics::IDEMPOTENCY_CONFLICTS),
            1
        );
        assert_eq!(
            counter(
                &store,
                pipeline_metrics::EXECUTION_SUBMISSIONS,
                Outcome::Accepted
            ),
            1,
            "the rejected request created no work"
        );
    }

    #[tokio::test]
    async fn submission_creates_execution_task_and_events_atomically() {
        let store = InMemoryDurableExecution::new();
        let receipt = store.submit(intent("k1", A)).await.expect("submit");

        assert_eq!(receipt.kind, SubmissionKind::Created);
        assert_eq!(receipt.execution.status, ExecutionStatus::Queued);
        assert_eq!(receipt.task.status, TaskStatus::Queued);
        assert_eq!(receipt.task.execution_id, Some(receipt.execution.id));
        assert_eq!(
            receipt.events.len(),
            2,
            "execution.submitted + task.created"
        );
        assert!(store.execution(receipt.execution.id).is_some());
        assert!(store.task(receipt.task.id).is_some());
        assert_eq!(store.idempotency().len(), 1);
    }

    #[tokio::test]
    async fn same_key_same_fingerprint_replays_without_new_work() {
        let store = InMemoryDurableExecution::new();
        let first = store.submit(intent("k1", A)).await.expect("first");
        let second = store.submit(intent("k1", A)).await.expect("second");

        assert_eq!(second.kind, SubmissionKind::Replayed);
        assert_eq!(second.execution.id, first.execution.id);
        assert_eq!(second.task.id, first.task.id);
        assert!(second.attempt.is_none(), "a replay creates no attempt");
        assert!(store.execution(first.execution.id).is_some());
        assert_eq!(
            store
                .outbox()
                .iter()
                .filter(|record| record.envelope.event_type == events::EXECUTION_REPLAYED)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn same_key_different_fingerprint_is_a_conflict() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("first");
        let error = store
            .submit(intent("k1", B))
            .await
            .expect_err("a conflicting fingerprint must be rejected");
        assert!(matches!(error, AppError::Conflict(_)));
        assert!(
            format!("{error}").contains("k1"),
            "names the offending key: {error}"
        );
    }

    #[tokio::test]
    async fn task_claim_and_settlement_events_keep_submission_correlation() {
        let store = InMemoryDurableExecution::new();
        let correlation_id = "corr-submission-e2e";
        let mut submission = intent("idem-correlation-roundtrip", A);
        submission.correlation_id = correlation_id.to_owned();
        let receipt = store.submit(submission).await.expect("submit");

        let worker = "worker-correlation";
        let claim = store
            .claim(claim_query(worker, 1))
            .await
            .expect("claim")
            .pop()
            .expect("one claim");
        assert_eq!(claim.task.id, receipt.task.id);

        let claimed_event = store
            .outbox()
            .into_iter()
            .find(|record| record.envelope.event_type == events::TASK_CLAIMED)
            .expect("claim event");
        assert_eq!(claimed_event.envelope.correlation_id, correlation_id);

        store
            .settle(TaskSettlement {
                tenant_id: claim.task.tenant_id,
                task_id: claim.task.id,
                attempt_id: claim.attempt.id,
                worker: worker.to_owned(),
                expected_version: claim.task.version(),
                outcome: TaskOutcome::Succeeded {
                    usage: ExecutionUsage::default(),
                },
                execution: None,
                actor: TransitionActor::worker(worker).expect("worker actor"),
                now: now().checked_add(Duration::from_secs(1)).expect("time"),
            })
            .await
            .expect("settle");

        let succeeded_event = store
            .outbox()
            .into_iter()
            .find(|record| record.envelope.event_type == events::TASK_SUCCEEDED)
            .expect("success event");
        assert_eq!(succeeded_event.envelope.correlation_id, correlation_id);
    }

    #[tokio::test]
    async fn claim_never_exceeds_capacity_and_never_double_claims() {
        let store = InMemoryDurableExecution::new();
        for index in 0..5 {
            store
                .submit(intent(&format!("k{index}"), A))
                .await
                .expect("submit");
        }

        let first = store.claim(claim_query("w-1", 3)).await.expect("claim");
        assert_eq!(first.len(), 3, "capacity is a hard ceiling");
        let second = store.claim(claim_query("w-2", 3)).await.expect("claim");
        assert_eq!(second.len(), 2, "only the remainder is claimable");

        let overlap = first
            .iter()
            .any(|claim| second.iter().any(|other| other.task.id == claim.task.id));
        assert!(!overlap, "a task claimed by w-1 must never reach w-2");
        assert!(store
            .claim(claim_query("w-3", 10))
            .await
            .expect("claim")
            .is_empty());
    }

    #[tokio::test]
    async fn zero_capacity_claims_nothing() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        assert!(store
            .claim(claim_query("w-1", 0))
            .await
            .expect("claim")
            .is_empty());
    }

    #[tokio::test]
    async fn claim_orders_by_priority_then_availability() {
        let store = InMemoryDurableExecution::new();
        let mut low = intent("low", A);
        low.priority = TaskPriority::Low;
        store.submit(low).await.expect("submit");
        let mut critical = intent("critical", A);
        critical.priority = TaskPriority::Critical;
        store.submit(critical).await.expect("submit");

        let claims = store.claim(claim_query("w-1", 2)).await.expect("claim");
        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].task.priority, TaskPriority::Critical);
        assert_eq!(claims[1].task.priority, TaskPriority::Low);
    }

    #[tokio::test]
    async fn settlement_persists_terminal_state_and_usage_only_on_success() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .remove(0);

        let receipt = store
            .settle(TaskSettlement {
                tenant_id: claim.task.tenant_id,
                task_id: claim.task.id,
                attempt_id: claim.attempt.id,
                worker: "w-1".to_owned(),
                expected_version: claim.task.version(),
                outcome: TaskOutcome::Succeeded {
                    usage: ExecutionUsage {
                        tokens_input: 10,
                        tokens_output: 20,
                        tool_calls: 1,
                        steps: 1,
                        ..ExecutionUsage::default()
                    },
                },
                execution: Some(ExecutionFinalization {
                    execution_id: claim.task.execution_id.expect("execution"),
                    outcome: ExecutionOutcome::Completed,
                }),
                actor: mas_domain::TransitionActor::worker("w-1").expect("actor"),
                now: now(),
            })
            .await
            .expect("settle");

        assert_eq!(receipt.task.status, TaskStatus::Completed);
        assert_eq!(receipt.attempt.status, mas_domain::AttemptStatus::Succeeded);
        assert!(receipt.execution.is_some());
        assert_eq!(
            receipt.execution.as_ref().map(|e| e.status),
            Some(ExecutionStatus::Completed)
        );
        assert_eq!(receipt.usage.len(), 3, "tokens + tool calls + steps");
        assert_eq!(store.usage().len(), 3);
        assert!(!receipt.events.is_empty());
    }

    #[tokio::test]
    async fn a_stale_worker_cannot_overwrite_newer_state() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .remove(0);
        let observed_version = claim.task.version();

        // Ownership advances to w-2 (the lease lapsed and another worker took
        // the task), so w-1's version is now stale.
        let later = now().checked_add(Duration::from_secs(60)).expect("ts");
        let stolen = store
            .claim(ClaimQuery {
                worker: "w-2".to_owned(),
                capacity: 1,
                lease_until: later,
                now: later,
                tenant_id: None,
                task_id: None,
            })
            .await
            .expect("claim");
        assert_eq!(stolen.len(), 1, "an expired lease frees the task");

        let error = store
            .settle(TaskSettlement {
                tenant_id: claim.task.tenant_id,
                task_id: claim.task.id,
                attempt_id: claim.attempt.id,
                worker: "w-1".to_owned(),
                expected_version: observed_version,
                outcome: TaskOutcome::Succeeded {
                    usage: ExecutionUsage::default(),
                },
                execution: None,
                actor: mas_domain::TransitionActor::worker("w-1").expect("actor"),
                now: later,
            })
            .await
            .expect_err("a stale worker must be rejected");
        assert!(matches!(error, AppError::Conflict(_)));
        assert!(format!("{error}").contains("stale worker"), "{error}");
        assert_eq!(
            store.task(claim.task.id).expect("task").status,
            TaskStatus::Running,
            "w-2 still owns it; w-1 changed nothing"
        );
    }

    #[tokio::test]
    async fn a_worker_that_never_owned_the_task_cannot_settle_it() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .remove(0);
        let error = store
            .settle(TaskSettlement {
                tenant_id: claim.task.tenant_id,
                task_id: claim.task.id,
                attempt_id: claim.attempt.id,
                worker: "w-9".to_owned(),
                expected_version: claim.task.version(),
                outcome: TaskOutcome::Cancelled,
                execution: None,
                actor: mas_domain::TransitionActor::worker("w-9").expect("actor"),
                now: now(),
            })
            .await
            .expect_err("a non-owner must not settle");
        assert!(matches!(error, AppError::Conflict(_)));
    }

    #[tokio::test]
    async fn transient_failure_requeues_terminal_failure_dead_letters() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .remove(0);

        let receipt = store
            .settle(TaskSettlement {
                tenant_id: claim.task.tenant_id,
                task_id: claim.task.id,
                attempt_id: claim.attempt.id,
                worker: "w-1".to_owned(),
                expected_version: claim.task.version(),
                outcome: TaskOutcome::Failed {
                    failure: TransitionFailure::transient("rate_limited", "429").expect("f"),
                },
                execution: None,
                actor: mas_domain::TransitionActor::worker("w-1").expect("actor"),
                now: now(),
            })
            .await
            .expect("settle");
        assert!(receipt.requeued, "a transient failure is retried");
        assert!(!receipt.dead_lettered);
        assert_eq!(receipt.task.status, TaskStatus::Queued);
        assert!(receipt.usage.is_empty(), "a failure meters nothing");

        // Terminal failure: straight to the dead-letter queue.
        // The requeue carried a backoff, so the next claim is only valid once
        // that backoff has elapsed on the injected clock.
        let after_backoff = now().checked_add(Duration::from_secs(1)).expect("ts");
        let claim2 = store
            .claim(ClaimQuery {
                worker: "w-1".to_owned(),
                capacity: 1,
                lease_until: after_backoff
                    .checked_add(Duration::from_secs(30))
                    .expect("ts"),
                now: after_backoff,
                tenant_id: None,
                task_id: None,
            })
            .await
            .expect("claim")
            .remove(0);
        let receipt2 = store
            .settle(TaskSettlement {
                tenant_id: claim2.task.tenant_id,
                task_id: claim2.task.id,
                attempt_id: claim2.attempt.id,
                worker: "w-1".to_owned(),
                expected_version: claim2.task.version(),
                outcome: TaskOutcome::Failed {
                    failure: TransitionFailure::terminal("unauthorized", "bad token").expect("f"),
                },
                execution: None,
                actor: mas_domain::TransitionActor::worker("w-1").expect("actor"),
                now: now(),
            })
            .await
            .expect("settle");
        assert!(!receipt2.requeued);
        assert!(receipt2.dead_lettered);
        assert_eq!(receipt2.task.status, TaskStatus::DeadLettered);
    }

    #[tokio::test]
    async fn terminal_attempts_cannot_be_settled_twice() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .remove(0);
        let settlement = TaskSettlement {
            tenant_id: claim.task.tenant_id,
            task_id: claim.task.id,
            attempt_id: claim.attempt.id,
            worker: "w-1".to_owned(),
            expected_version: claim.task.version(),
            outcome: TaskOutcome::Succeeded {
                usage: ExecutionUsage::default(),
            },
            execution: None,
            actor: mas_domain::TransitionActor::worker("w-1").expect("actor"),
            now: now(),
        };
        store
            .settle(settlement.clone())
            .await
            .expect("first settle");
        let error = store
            .settle(settlement)
            .await
            .expect_err("a finished attempt is immutable");
        assert!(matches!(error, AppError::Conflict(_)));
    }

    #[tokio::test]
    async fn cancellation_handles_every_lifecycle_state() {
        let store = InMemoryDurableExecution::new();

        // Queued → cancelled outright, and the task goes with it.
        let queued = store.submit(intent("queued", A)).await.expect("submit");
        let receipt = store
            .cancel(CancellationRequest {
                tenant_id: queued.execution.tenant_id,
                execution_id: queued.execution.id,
                actor: mas_domain::TransitionActor::principal("user-1", queued.execution.tenant_id)
                    .expect("actor"),
                correlation_id: "corr".to_owned(),
                now: now(),
            })
            .await
            .expect("cancel");
        assert_eq!(receipt.result, CancellationResult::Cancelled);
        assert_eq!(receipt.execution.status, ExecutionStatus::Cancelled);
        assert_eq!(
            receipt.task.as_ref().map(|task| task.status),
            Some(TaskStatus::Cancelled)
        );

        // Cancelling again is an idempotent no-op, not an error.
        let again = store
            .cancel(CancellationRequest {
                tenant_id: queued.execution.tenant_id,
                execution_id: queued.execution.id,
                actor: mas_domain::TransitionActor::principal("user-1", queued.execution.tenant_id)
                    .expect("actor"),
                correlation_id: "corr".to_owned(),
                now: now(),
            })
            .await
            .expect("cancel again");
        assert_eq!(again.result, CancellationResult::AlreadyCancelled);

        // Running → the cooperative flag is set; the run is not torn down.
        let running = store.submit(intent("running", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .into_iter()
            .find(|claim| claim.task.execution_id == Some(running.execution.id))
            .expect("the running task");
        assert_eq!(claim.task.status, TaskStatus::Running);
        let signalled = store
            .cancel(CancellationRequest {
                tenant_id: running.execution.tenant_id,
                execution_id: running.execution.id,
                actor: mas_domain::TransitionActor::principal(
                    "user-1",
                    running.execution.tenant_id,
                )
                .expect("actor"),
                correlation_id: "corr".to_owned(),
                now: now(),
            })
            .await
            .expect("cancel");
        assert_eq!(signalled.result, CancellationResult::SignalSet);
        assert!(signalled.execution.cancellation_requested());
        assert_eq!(
            signalled.execution.status,
            ExecutionStatus::Running,
            "a running execution is stopped cooperatively, not erased"
        );
    }

    #[tokio::test]
    async fn cancellation_never_reopens_a_completed_execution() {
        let store = InMemoryDurableExecution::new();
        let receipt = store.submit(intent("k1", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .remove(0);
        store
            .settle(TaskSettlement {
                tenant_id: claim.task.tenant_id,
                task_id: claim.task.id,
                attempt_id: claim.attempt.id,
                worker: "w-1".to_owned(),
                expected_version: claim.task.version(),
                outcome: TaskOutcome::Succeeded {
                    usage: ExecutionUsage::default(),
                },
                execution: Some(ExecutionFinalization {
                    execution_id: claim.task.execution_id.expect("execution"),
                    outcome: ExecutionOutcome::Completed,
                }),
                actor: mas_domain::TransitionActor::worker("w-1").expect("actor"),
                now: now(),
            })
            .await
            .expect("settle");

        let error = store
            .cancel(CancellationRequest {
                tenant_id: receipt.execution.tenant_id,
                execution_id: receipt.execution.id,
                actor: mas_domain::TransitionActor::principal(
                    "user-1",
                    receipt.execution.tenant_id,
                )
                .expect("actor"),
                correlation_id: "corr".to_owned(),
                now: now(),
            })
            .await
            .expect_err("a completed execution must not be reopened");
        assert!(matches!(error, AppError::Conflict(_)));
        assert_eq!(
            store.execution(receipt.execution.id).expect("exec").status,
            ExecutionStatus::Completed
        );
    }

    #[tokio::test]
    async fn tenants_are_invisible_to_each_other() {
        let store = InMemoryDurableExecution::new();
        let receipt = store.submit(intent("k1", A)).await.expect("submit");
        let foreign = TenantId::new();

        assert!(store
            .load_execution(foreign, receipt.execution.id)
            .await
            .expect("load")
            .is_none());
        assert!(store
            .load_task(foreign, receipt.task.id)
            .await
            .expect("load")
            .is_none());
        let error = store
            .cancel(CancellationRequest {
                tenant_id: foreign,
                execution_id: receipt.execution.id,
                actor: mas_domain::TransitionActor::principal("user-1", foreign).expect("actor"),
                correlation_id: "corr".to_owned(),
                now: now(),
            })
            .await
            .expect_err("a foreign tenant must not see the execution");
        assert!(
            matches!(error, AppError::NotFound { .. }),
            "existence is hidden, not forbidden: {error:?}"
        );
        assert!(store
            .load_execution(receipt.execution.tenant_id, receipt.execution.id)
            .await
            .expect("load")
            .is_some());
    }

    #[tokio::test]
    async fn counters_reflect_durable_state() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        store.submit(intent("k2", A)).await.expect("submit");

        let before = store.counters("w-1", &now()).await.expect("counters");
        assert_eq!(before.active_attempts, 0);
        assert_eq!(before.claimable_tasks, 2);
        assert!(before.outbox_backlog >= 4);

        store.claim(claim_query("w-1", 1)).await.expect("claim");
        let after = store.counters("w-1", &now()).await.expect("counters");
        assert_eq!(after.active_attempts, 1);
        assert_eq!(
            store
                .counters("w-2", &now())
                .await
                .expect("counters")
                .active_attempts,
            0
        );
        assert_eq!(after.claimable_tasks, 1);
    }

    #[tokio::test]
    async fn lease_renewal_is_owner_only() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        let claim = store
            .claim(claim_query("w-1", 1))
            .await
            .expect("claim")
            .remove(0);
        let until = now().checked_add(Duration::from_secs(90)).expect("ts");
        assert!(store
            .renew_lease(claim.task.id, "w-1", until)
            .await
            .expect("renew"));
        assert!(!store
            .renew_lease(claim.task.id, "w-2", until)
            .await
            .expect("renew"));
        assert!(!store
            .renew_lease(TaskId::new(), "w-1", until)
            .await
            .expect("renew"));
    }

    #[tokio::test]
    async fn an_expired_lease_frees_the_task_for_another_worker() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        let short = ClaimQuery {
            worker: "w-1".to_owned(),
            capacity: 1,
            lease_until: now().checked_add(Duration::from_secs(1)).expect("ts"),
            now: now(),
            tenant_id: None,
            task_id: None,
        };
        assert_eq!(store.claim(short).await.expect("claim").len(), 1);

        let after_expiry = now().checked_add(Duration::from_secs(10)).expect("ts");
        let stolen = store
            .claim(ClaimQuery {
                worker: "w-2".to_owned(),
                capacity: 1,
                lease_until: after_expiry
                    .checked_add(Duration::from_secs(30))
                    .expect("ts"),
                now: after_expiry,
                tenant_id: None,
                task_id: None,
            })
            .await
            .expect("claim");
        assert_eq!(stolen.len(), 1, "w-2 reclaims the abandoned task");
        assert_eq!(store.attempts().len(), 2, "each claim is its own attempt");
    }

    #[tokio::test]
    async fn attempts_are_durable_and_numbered_not_counted_from_deliveries() {
        let store = InMemoryDurableExecution::new();
        store.submit(intent("k1", A)).await.expect("submit");
        for index in 0..3u32 {
            // Each failure requeues with a backoff, so every subsequent claim
            // is made at a later point on the injected clock.
            let at = now()
                .checked_add(Duration::from_secs(u64::from(index) * 10))
                .expect("ts");
            let claim = store
                .claim(ClaimQuery {
                    worker: "w-1".to_owned(),
                    capacity: 1,
                    lease_until: at.checked_add(Duration::from_secs(5)).expect("ts"),
                    now: at,
                    tenant_id: None,
                    task_id: None,
                })
                .await
                .expect("claim")
                .remove(0);
            assert_eq!(claim.attempt.attempt_number, index + 1);
            store
                .settle(TaskSettlement {
                    tenant_id: claim.task.tenant_id,
                    task_id: claim.task.id,
                    attempt_id: claim.attempt.id,
                    worker: "w-1".to_owned(),
                    expected_version: claim.task.version(),
                    outcome: TaskOutcome::Failed {
                        failure: TransitionFailure::transient("upstream_unavailable", "down")
                            .expect("f"),
                    },
                    execution: None,
                    actor: mas_domain::TransitionActor::worker("w-1").expect("actor"),
                    now: at,
                })
                .await
                .expect("settle");
        }
        let attempts = store.attempts();
        assert_eq!(attempts.len(), 3, "one durable attempt per real try");
        let numbers: Vec<u32> = attempts.iter().map(|a| a.attempt_number).collect();
        assert_eq!(numbers, vec![1, 2, 3]);
        assert!(attempts.iter().all(|a| a.status.is_terminal()));
    }

    #[tokio::test]
    async fn transient_failure_helper_is_retryable() {
        let failure = transient_failure("broker_unavailable");
        assert!(failure.retryable);
        assert_eq!(failure.code, "broker_unavailable");
    }
}
