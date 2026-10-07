//! PURPOSE:
//! Durable attempt lifecycle: claim, own, heartbeat, settle.
//!
//! This module is the worker's only route to durable execution state. It owns
//! the ordering rule that makes the pipeline correct:
//!
//! ```text
//!   capacity from durable attempts → claim (creates the attempt row and the
//!   ownership fence in one unit) → run → settle (attempt + task + execution
//!   + usage + outbox in one unit) → only then ack the broker
//! ```
//!
//! Layer / ownership
//! -----------------
//! `mas-worker`, sitting on [`DurableExecutionPort`]. The worker never touches
//! a task row directly, so it cannot accidentally invent a second lifecycle.
//!
//! Production responsibility
//! -------------------------
//! * **Attempts are durable records, not broker metadata.** The attempt number
//!   and count come from `task_attempts`, never from a delivery counter.
//! * **Ownership is a fence, not a hint.** Every settlement carries the worker
//!   identity and the task version it observed; a worker that lost ownership
//!   is rejected rather than allowed to overwrite newer state.
//! * **Terminal state is persisted before the caller acks.** `settle` commits
//!   first; the caller acks afterwards. There is no "settle and ack" helper
//!   that could get the order wrong.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT derive how many times work ran from broker redeliveries.
//! * It must NOT hold capacity in process memory: the durable attempt count is
//!   the only correct input after a restart.
//! * It must NOT swallow a stale-write rejection — a `Conflict` from `settle`
//!   is a finding, not a retry opportunity.

use mas_application::durable::{
    CancellationRequest, ClaimQuery, DurableExecutionPort, ExecutionAction, ExecutionFinalization,
    ExecutionOutcome, ExecutionTransitionRequest, SettlementReceipt, TaskClaim, TaskOutcome,
    TaskSettlement,
};
use mas_application::pipeline_log::PipelineLogContext;
use mas_application::pipeline_metrics::{Outcome, PipelineMetrics};
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, OrganizationId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{Execution, TaskAttempt, TransitionActor, TransitionFailure};
use std::sync::Arc;
use std::time::Duration;

use crate::capacity::CapacityGate;
use crate::classify::{cancelled, ClassifiedFailure, FailureClass};
use crate::run_slot::{NoRunSlots, RunSlot, RunSlotPort};

/// The outcome of claiming one named task.
#[derive(Debug)]
pub enum ClaimOneOutcome {
    /// Ownership was granted; an attempt row exists for this run.
    ///
    /// Boxed because a `TaskClaim` carries the whole task plus its attempt
    /// fence — several hundred bytes against the other variants' few bytes.
    /// Every claim path returns exactly one of these per delivery, so the
    /// allocation is not on a hot loop.
    Claimed(Box<TaskClaim>),
    /// The task cannot be claimed.
    ///
    /// `defer` distinguishes the two cases that must be handled differently:
    ///
    /// * `defer: true` — the task is real, unfinished and unowned, but not
    ///   available yet (retry backoff). Dropping the wake-up would lose the
    ///   retry, so the caller must defer until `available_at`. Deferring for
    ///   less than that burns broker redeliveries without ever reaching a
    ///   state where the task can run.
    /// * `defer: false` — the task is settled, owned by another worker, or
    ///   gone. There is nothing to wait for; the caller must drop it.
    NotClaimable {
        defer: bool,
        /// When the task may next be claimed (only meaningful when deferring).
        available_at: Option<Timestamp>,
    },
    /// This worker is at its durable capacity ceiling.
    AtCapacity,
}

/// The worker-side durable attempt coordinator.
#[derive(Debug)]
pub struct AttemptCoordinator {
    durable: Arc<dyn DurableExecutionPort>,
    worker: String,
    capacity: CapacityGate,
    lease_ttl: Duration,
    /// The concurrency slot a run holds for its duration.
    ///
    /// Defaults to [`NoRunSlots`] so an existing composition root keeps
    /// working unchanged; a root that has a runtime pool and a control plane
    /// upgrades it with [`AttemptCoordinator::with_run_slots`].
    run_slots: Arc<dyn RunSlotPort>,
    /// Named pipeline metrics, emitted where the durable state actually
    /// changes so the counters cannot drift from it.
    metrics: PipelineMetrics,
}

impl AttemptCoordinator {
    /// Binds the coordinator to a durable port, a worker identity and a ceiling.
    pub fn new(
        durable: Arc<dyn DurableExecutionPort>,
        worker: impl Into<String>,
        max_in_flight: usize,
        lease_ttl: Duration,
    ) -> Result<Self> {
        let worker = worker.into();
        if worker.trim().is_empty() {
            return Err(AppError::invalid_field(
                "worker",
                "required",
                "a coordinator must have a worker identity to fence tasks with",
            ));
        }
        Ok(Self {
            durable,
            worker,
            capacity: CapacityGate::new(max_in_flight)?,
            lease_ttl,
            run_slots: Arc::new(NoRunSlots),
            metrics: PipelineMetrics::default(),
        })
    }

    /// Attaches a real concurrency gate.
    ///
    /// Builder-shaped on purpose: the existing four-argument constructor is
    /// used by composition roots and tests that have no control plane, and
    /// changing its arity would break every one of them for no gain.
    #[must_use]
    pub fn with_run_slots(mut self, run_slots: Arc<dyn RunSlotPort>) -> Self {
        self.run_slots = run_slots;
        self
    }

    /// The concurrency gate in use (exposed so the consumer can ask it
    /// directly rather than reaching past the coordinator).
    #[must_use]
    pub fn run_slots(&self) -> &Arc<dyn RunSlotPort> {
        &self.run_slots
    }

    /// Takes the concurrency slot for one run, before the work starts.
    ///
    /// Delegates rather than decides: the worker knows *which* run, the
    /// control plane knows *whether* it fits.
    pub async fn acquire_run_slot(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        at: &Timestamp,
    ) -> Result<RunSlot> {
        self.run_slots
            .acquire(tenant, organization, execution, task, at)
            .await
    }

    /// Extends a run's slot lease from the heartbeat.
    pub async fn renew_run_slot(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        at: &Timestamp,
    ) -> Result<()> {
        let until = self.lease_until(at)?;
        self.run_slots
            .renew(tenant, organization, execution, task, &until, at)
            .await
    }

    /// Returns a run's slot. Safe to call whether or not one was taken.
    pub async fn release_run_slot(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        at: &Timestamp,
    ) -> Result<()> {
        self.run_slots
            .release(tenant, organization, execution, task, at)
            .await
    }

    /// The metrics emitter this coordinator writes to.
    #[must_use]
    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    /// The worker identity this coordinator fences tasks with.
    #[must_use]
    pub fn worker(&self) -> &str {
        &self.worker
    }

    /// The lease window handed to every claim.
    #[must_use]
    pub const fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }

    /// The configured ceiling.
    #[must_use]
    pub const fn max_in_flight(&self) -> usize {
        self.capacity.max_in_flight()
    }

    /// The durable port (exposed for composition and tests).
    #[must_use]
    pub fn durable(&self) -> &Arc<dyn DurableExecutionPort> {
        &self.durable
    }

    /// Lease expiry for a claim made at `now`.
    pub fn lease_until(&self, now: &Timestamp) -> Result<Timestamp> {
        now.checked_add(self.lease_ttl)
            .ok_or_else(|| AppError::internal("lease expiry overflowed the clock"))
    }

    /// Durable, unfinished attempts owned by this worker, at `at`.
    pub async fn active_attempts(&self, at: &Timestamp) -> Result<u64> {
        Ok(self
            .durable
            .counters(&self.worker, at)
            .await?
            .active_attempts)
    }

    /// How many more tasks this worker may claim, at `at`.
    pub async fn remaining_capacity(&self, at: &Timestamp) -> Result<usize> {
        let active = self.active_attempts(at).await?;
        Ok(self.capacity.remaining(active))
    }

    /// Claims work, clamped to the durable remaining capacity.
    ///
    /// `batch_size` is a *hint*; the durable port enforces the real ceiling.
    /// Passing a batch larger than the remaining room is clamped here as well,
    /// so an over-claim is impossible from either direction.
    pub async fn claim(&self, batch_size: usize, at: &Timestamp) -> Result<Vec<TaskClaim>> {
        let active = self.active_attempts(at).await?;
        let capacity = self.capacity.clamp_batch(active, batch_size);
        if capacity == 0 {
            return Ok(Vec::new());
        }
        self.durable
            .claim(ClaimQuery {
                worker: self.worker.clone(),
                capacity,
                lease_until: self.lease_until(at)?,
                now: *at,
                tenant_id: None,
                task_id: None,
            })
            .await
    }

    /// Claims one named task, scoped to the tenant the message named.
    ///
    /// The broker message names a task; this is the durable counterpart to
    /// receiving it. Passing the tenant through is deliberate: a worker that
    /// somehow received another tenant's message cannot claim that tenant's
    /// work, even if the message was misrouted.
    pub async fn claim_one(
        &self,
        task_id: TaskId,
        tenant_id: TenantId,
        at: &Timestamp,
    ) -> Result<ClaimOneOutcome> {
        let started_at = std::time::Instant::now();
        let active = self.active_attempts(at).await?;
        self.metrics
            .set_active_attempts(i64::try_from(active).unwrap_or(i64::MAX));
        if !self.capacity.has_room(active) {
            return Ok(ClaimOneOutcome::AtCapacity);
        }
        let claims = self
            .durable
            .claim(ClaimQuery {
                worker: self.worker.clone(),
                capacity: 1,
                lease_until: self.lease_until(at)?,
                now: *at,
                tenant_id: Some(tenant_id),
                task_id: Some(task_id),
            })
            .await?;
        if let Some(claim) = claims.into_iter().next() {
            // Measured from the moment the claim was asked for to the moment
            // ownership existed — the queue-time operators actually feel.
            self.metrics
                .claim_latency_ms(started_at.elapsed().as_secs_f64() * 1_000.0);
            return Ok(ClaimOneOutcome::Claimed(Box::new(claim)));
        }

        // Nothing was claimed. Whether that is final depends on *why*.
        let Some((task, _)) = self.durable.load_task(tenant_id, task_id).await? else {
            return Ok(ClaimOneOutcome::NotClaimable {
                defer: false,
                available_at: None,
            });
        };
        if task.is_terminal() || task.locked_by.is_some() {
            // Settled, or someone else owns it: nothing will change by waiting.
            return Ok(ClaimOneOutcome::NotClaimable {
                defer: false,
                available_at: None,
            });
        }
        // Real, unfinished, unowned — just not available yet. Deferring keeps
        // the retry alive; dropping it would silently lose the wake-up.
        Ok(ClaimOneOutcome::NotClaimable {
            defer: true,
            available_at: Some(task.available_at()),
        })
    }

    /// Extends the lease on a task this worker still owns.
    ///
    /// `false` means ownership moved on — the caller must stop working on the
    /// task rather than keep heartbeating into the void.
    pub async fn renew(&self, task_id: TaskId, at: &Timestamp) -> Result<bool> {
        self.durable
            .renew_lease(task_id, &self.worker, self.lease_until(at)?)
            .await
    }

    /// Whether this worker still owns `task` at `at`.
    pub fn still_owns(&self, task: &mas_domain::Task, at: &Timestamp) -> bool {
        task.is_owned_by_at(&self.worker, at)
    }

    // -- settlement ---------------------------------------------------------

    /// Settles a successful attempt.
    pub async fn succeed(
        &self,
        claim: &TaskClaim,
        usage: mas_application::durable::ExecutionUsage,
        finalize_execution: bool,
        at: &Timestamp,
    ) -> Result<SettlementReceipt> {
        self.settle(
            claim,
            TaskOutcome::Succeeded { usage },
            finalize_execution,
            at,
        )
        .await
    }

    /// Settles a failed attempt from a classified failure.
    pub async fn fail(
        &self,
        claim: &TaskClaim,
        failure: &ClassifiedFailure,
        finalize_execution: bool,
        at: &Timestamp,
    ) -> Result<SettlementReceipt> {
        let outcome = match failure.class {
            FailureClass::Cancelled => TaskOutcome::Cancelled,
            FailureClass::Retryable | FailureClass::Terminal => TaskOutcome::Failed {
                failure: failure.transition_failure()?,
            },
        };
        self.settle(claim, outcome, finalize_execution, at).await
    }

    /// Settles an attempt that observed a cancellation.
    pub async fn cancel(
        &self,
        claim: &TaskClaim,
        detail: &str,
        finalize_execution: bool,
        at: &Timestamp,
    ) -> Result<SettlementReceipt> {
        let failure = cancelled(detail);
        self.fail(claim, &failure, finalize_execution, at).await
    }

    /// The single settlement entry point.
    ///
    /// Carries the worker identity and the version the claim observed, so the
    /// durable port can reject a stale write. Returns `Err(Conflict)` when this
    /// worker no longer owns the task — the caller must **not** retry that
    /// settlement.
    pub async fn settle(
        &self,
        claim: &TaskClaim,
        outcome: TaskOutcome,
        finalize_execution: bool,
        at: &Timestamp,
    ) -> Result<SettlementReceipt> {
        let actor = TransitionActor::worker(&self.worker)?;
        self.metrics.observe_outcome(&outcome);

        // One log context for the whole settlement, so every line written
        // during it — success, retry, dead letter, rejection — identifies the
        // same run and can be found from any one of its ids. The correlation
        // id is the client's idempotency key: it is the causal identifier that
        // survives every hop from the original request to this settlement, and
        // it is safe to log because it is caller-chosen metadata, not a secret
        // and not key material.
        let log = PipelineLogContext::new(claim.task.tenant_id, claim.task.idempotency_key.clone())
            .with_execution_opt(claim.task.execution_id)
            .with_task(claim.task.id)
            .with_attempt(claim.attempt.attempt_number)
            .with_worker(&self.worker);
        let execution = if finalize_execution {
            claim
                .task
                .execution_id
                .map(|execution_id| ExecutionFinalization {
                    execution_id,
                    outcome: execution_outcome_for(&outcome),
                })
        } else {
            None
        };
        let settled = self
            .durable
            .settle(TaskSettlement {
                tenant_id: claim.task.tenant_id,
                task_id: claim.task.id,
                attempt_id: claim.attempt.id,
                worker: self.worker.clone(),
                expected_version: claim.task.version(),
                outcome,
                execution,
                actor,
                now: *at,
            })
            .await;

        match &settled {
            Ok(receipt) => {
                // Retry vs dead-letter is the durable port's decision, and the
                // receipt is where it says so. Counting it *after* the write
                // means the counters describe what actually happened.
                if receipt.dead_lettered {
                    self.metrics.dead_letter();
                    self.metrics.execution_terminal(Outcome::DeadLettered, 0.0);
                    log.warn(
                        module_path!(),
                        "task dead-lettered: this run will not be retried",
                        at,
                    );
                } else if receipt.requeued {
                    self.metrics.task_retry();
                    log.safe_field("retry.scheduled", true).info(
                        module_path!(),
                        "task failed transiently and was requeued",
                        at,
                    );
                } else {
                    log.info(module_path!(), "task settled", at);
                }
            },
            // A conflict here is the ownership fence doing its job: another
            // worker owns this task now. Counting it is what makes a
            // misconfigured lease visible before it becomes data loss.
            Err(AppError::Conflict(_)) => {
                self.metrics.stale_worker_rejection();
                log.warn(
                    module_path!(),
                    "settlement rejected: this worker no longer owns the task",
                    at,
                );
            },
            Err(error) => {
                log.error(
                    module_path!(),
                    "settlement failed and the task is still owned by us",
                    at,
                );
                let _ = error;
            },
        }

        settled
    }

    /// Durable cancellation of an execution, from the worker's perspective.
    pub async fn cancel_execution(
        &self,
        tenant_id: mas_common::ids::TenantId,
        execution_id: ExecutionId,
        correlation_id: &str,
        at: &Timestamp,
    ) -> Result<mas_application::durable::CancellationReceipt> {
        self.durable
            .cancel(CancellationRequest {
                tenant_id,
                execution_id,
                actor: TransitionActor::worker(&self.worker)?,
                correlation_id: correlation_id.to_owned(),
                now: *at,
            })
            .await
    }

    /// Marks a claimed execution as running, for tasks claimed before the
    /// execution-start transition existed in the claim path.
    ///
    /// Idempotent: an execution that is already running is left alone.
    pub async fn ensure_execution_running(
        &self,
        tenant_id: mas_common::ids::TenantId,
        execution_id: ExecutionId,
        correlation_id: &str,
        at: &Timestamp,
    ) -> Result<Option<Execution>> {
        let Some(execution) = self.durable.load_execution(tenant_id, execution_id).await? else {
            return Ok(None);
        };
        if execution.status != mas_common::enums::ExecutionStatus::Queued {
            return Ok(Some(execution));
        }
        let receipt = self
            .durable
            .transition(ExecutionTransitionRequest {
                tenant_id,
                execution_id,
                action: ExecutionAction::Start,
                actor: TransitionActor::worker(&self.worker)?,
                correlation_id: correlation_id.to_owned(),
                now: *at,
            })
            .await?;
        Ok(Some(receipt.execution))
    }
}

/// Maps a task outcome onto the execution outcome to record in the same unit.
fn execution_outcome_for(outcome: &TaskOutcome) -> ExecutionOutcome {
    match outcome {
        TaskOutcome::Succeeded { .. } => ExecutionOutcome::Completed,
        TaskOutcome::Failed { failure } => ExecutionOutcome::Failed {
            failure: failure.clone(),
        },
        TaskOutcome::Cancelled => ExecutionOutcome::Cancelled,
    }
}

/// Re-exports so callers need only this module.
pub type OwnedAttempt = TaskAttempt;

/// Convenience: the failure record for a terminal, non-retryable cause.
pub fn terminal_failure(code: &str, message: &str) -> Result<TransitionFailure> {
    TransitionFailure::terminal(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_application::durable::ExecutionUsage;
    use mas_application::durable_memory::InMemoryDurableExecution;
    use mas_application::fingerprint::{FingerprintInput, RequestFingerprint};
    use mas_common::enums::{ExecutionStatus, TaskPriority, TaskStatus};
    use mas_common::ids::{OrganizationId, ProjectId, TenantId, WorkflowId};
    use std::time::Duration;

    const TENANT: u128 = 0xF00D;
    fn tenant() -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(TENANT))
    }

    async fn seed(
        store: &InMemoryDurableExecution,
        key: &str,
    ) -> mas_application::durable::SubmissionReceipt {
        let input = serde_json::json!({"prompt": key});
        let fingerprint = RequestFingerprint::submission(&FingerprintInput {
            tenant_id: tenant(),
            organization_id: OrganizationId::from_uuid(uuid::Uuid::from_u128(1)),
            project_id: ProjectId::from_uuid(uuid::Uuid::from_u128(2)),
            workflow_id: Some(WorkflowId::from_uuid(uuid::Uuid::from_u128(3))),
            agent_id: None,
            operation: "execution.run",
            input: &input,
            priority: TaskPriority::Normal,
            deadline: None,
        })
        .expect("fingerprint");
        store
            .submit(mas_application::durable::SubmissionIntent {
                tenant_id: tenant(),
                organization_id: OrganizationId::from_uuid(uuid::Uuid::from_u128(1)),
                project_id: ProjectId::from_uuid(uuid::Uuid::from_u128(2)),
                workflow_id: Some(WorkflowId::from_uuid(uuid::Uuid::from_u128(3))),
                agent_id: None,
                agent_version_id: None,
                input,
                idempotency_key: Some(key.to_owned()),
                fingerprint,
                operation: "execution.run".to_owned(),
                priority: TaskPriority::Normal,
                max_attempts: 3,
                deadline: None,
                correlation_id: "corr".to_owned(),
                request_id: uuid::Uuid::now_v7(),
                created_by: "user".to_owned(),
                actor: TransitionActor::system("test").expect("actor"),
                create_initial_attempt: false,
                now: now(),
            })
            .await
            .expect("submit")
    }

    fn now() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    fn later(secs: u64) -> Timestamp {
        now().checked_add(Duration::from_secs(secs)).expect("ts")
    }

    fn build(store: Arc<InMemoryDurableExecution>, worker: &str, max: usize) -> AttemptCoordinator {
        AttemptCoordinator::new(store, worker, max, Duration::from_secs(15)).expect("coordinator")
    }

    #[tokio::test]
    async fn claim_creates_a_durable_attempt_and_starts_the_execution() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let coordinator = build(store.clone(), "w-1", 4);

        let claims = coordinator.claim(8, &now()).await.expect("claim");
        assert_eq!(claims.len(), 1);
        let claim = &claims[0];
        assert_eq!(claim.attempt.attempt_number, 1, "attempts are 1-based");
        assert_eq!(claim.attempt.worker_id.as_deref(), Some("w-1"));
        assert_eq!(claim.task.status, TaskStatus::Running);
        assert_eq!(claim.task.attempt_count, 1);
        assert!(claim.task.is_owned_by_at("w-1", &now()));

        // The execution started in the same unit as the claim.
        let execution = store.execution(claim.task.execution_id.expect("execution"));
        assert_eq!(
            execution.expect("execution").status,
            ExecutionStatus::Running
        );
        assert_eq!(store.attempts().len(), 1);
    }

    #[tokio::test]
    async fn capacity_is_measured_against_durable_attempts() {
        let store = Arc::new(InMemoryDurableExecution::new());
        for index in 0..4 {
            seed(&store, &format!("k{index}")).await;
        }
        let coordinator = build(store.clone(), "w-1", 2);

        let first = coordinator.claim(16, &now()).await.expect("claim");
        assert_eq!(
            first.len(),
            2,
            "the ceiling is honoured, not the batch size"
        );

        assert_eq!(
            coordinator
                .remaining_capacity(&now())
                .await
                .expect("capacity"),
            0
        );
        assert!(
            coordinator
                .claim(16, &now())
                .await
                .expect("claim")
                .is_empty(),
            "at capacity the worker claims nothing at all"
        );

        // Settling one attempt frees exactly one slot.
        coordinator
            .succeed(&first[0], ExecutionUsage::default(), true, &now())
            .await
            .expect("settle");
        assert_eq!(
            coordinator
                .remaining_capacity(&now())
                .await
                .expect("capacity"),
            1
        );
        assert_eq!(coordinator.claim(16, &now()).await.expect("claim").len(), 1);
    }

    #[tokio::test]
    async fn a_stale_worker_cannot_settle_after_ownership_advances() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let worker_a = build(store.clone(), "w-1", 4);

        // Worker A claims the task.
        let claim = worker_a.claim(4, &now()).await.expect("claim").remove(0);

        // A's lease lapses; worker B takes ownership of the same task.
        let worker_b = build(store.clone(), "w-2", 4);
        let after_lapse = later(120);
        let stolen = worker_b.claim(4, &after_lapse).await.expect("claim");
        assert_eq!(stolen.len(), 1, "the lapsed lease was reclaimed");
        assert_eq!(stolen[0].task.id, claim.task.id);

        // A finishes late and tries to record its success. It must be rejected.
        let error = worker_a
            .succeed(&claim, ExecutionUsage::default(), true, &after_lapse)
            .await
            .expect_err("a stale worker must not overwrite newer state");
        assert!(matches!(error, AppError::Conflict(_)), "{error:?}");

        // B's attempt is still the live one, and the task is still running.
        let task = store.task(claim.task.id).expect("task");
        assert_eq!(task.locked_by.as_deref(), Some("w-2"));
        assert_eq!(task.status, TaskStatus::Running);
    }

    #[tokio::test]
    async fn a_worker_that_never_owned_the_task_cannot_settle_it() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let owner = build(store.clone(), "w-1", 4);
        let stranger = build(store.clone(), "w-9", 4);

        let claim = owner.claim(4, &now()).await.expect("claim").remove(0);
        let error = stranger
            .succeed(&claim, ExecutionUsage::default(), false, &now())
            .await
            .expect_err("a non-owner must be rejected");
        assert!(matches!(error, AppError::Conflict(_)));
    }

    #[tokio::test]
    async fn settlement_persists_everything_before_the_caller_acks() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let coordinator = build(store.clone(), "w-1", 4);
        let claim = coordinator.claim(4, &now()).await.expect("claim").remove(0);

        let receipt = coordinator
            .succeed(
                &claim,
                ExecutionUsage {
                    provider: Some("openai".to_owned()),
                    model: Some("gpt-4o-mini".to_owned()),
                    tokens_input: 10,
                    tokens_output: 5,
                    tool_calls: 0,
                    steps: 1,
                    cost_micros: None,
                },
                true,
                &now(),
            )
            .await
            .expect("settle");

        // Task, attempt and execution are all terminal in one unit.
        assert_eq!(receipt.task.status, TaskStatus::Completed);
        assert_eq!(receipt.attempt.status, mas_domain::AttemptStatus::Succeeded);
        assert_eq!(
            receipt.execution.as_ref().map(|e| e.status),
            Some(ExecutionStatus::Completed)
        );
        // Usage was metered, and the events were written in the same unit.
        assert_eq!(
            receipt.usage.len(),
            2,
            "tokens + steps, no synthetic zeroes"
        );
        assert!(receipt
            .events
            .iter()
            .any(|record| record.envelope.event_type == "usage.recorded"));
        assert!(receipt
            .events
            .iter()
            .any(|record| record.envelope.event_type == "execution.completed"));
        // And the durable store agrees, which is what "before ack" means.
        assert_eq!(
            store.task(claim.task.id).expect("task").status,
            TaskStatus::Completed
        );
        assert_eq!(store.usage().len(), 2);
    }

    #[tokio::test]
    async fn a_failed_execution_meters_nothing() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let coordinator = build(store.clone(), "w-1", 4);
        let claim = coordinator.claim(4, &now()).await.expect("claim").remove(0);

        let receipt = coordinator
            .fail(
                &claim,
                &crate::classify::classify(&AppError::rate_limited("429")),
                true,
                &now(),
            )
            .await
            .expect("settle");
        assert!(receipt.requeued, "a transient failure is retried");
        assert!(
            receipt.usage.is_empty(),
            "a failure must never meter success"
        );
        assert!(store.usage().is_empty());
        assert!(
            receipt.execution.is_none(),
            "an execution is NOT finalized while its task may still be retried: \
             marking it failed here would make the eventual success an illegal \
             transition and doom the retry"
        );
        let execution = store.execution(claim.task.execution_id.expect("execution"));
        assert_eq!(
            execution.expect("execution").status,
            ExecutionStatus::Running,
            "the execution lives on through a retryable failure"
        );
    }

    #[tokio::test]
    async fn a_dead_lettered_failure_finalizes_the_execution_and_meters_nothing() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let coordinator = build(store.clone(), "w-1", 4);
        let claim = coordinator.claim(4, &now()).await.expect("claim").remove(0);

        // A terminal error: no retry is possible, so this time the execution
        // really is finished — and it must say so rather than hang as
        // `running` forever.
        let receipt = coordinator
            .fail(
                &claim,
                &crate::classify::classify(&AppError::forbidden("tool not permitted")),
                true,
                &now(),
            )
            .await
            .expect("settle");
        assert!(receipt.dead_lettered, "a terminal failure ends the task");
        assert!(!receipt.requeued);
        assert!(
            receipt.usage.is_empty(),
            "a failure must never meter success"
        );
        assert!(store.usage().is_empty());
        assert_eq!(
            receipt.execution.as_ref().map(|e| e.status),
            Some(ExecutionStatus::Failed),
            "a dead-lettered task finalizes its execution in the same unit"
        );
        assert_eq!(
            store.task(claim.task.id).expect("task").status,
            TaskStatus::DeadLettered
        );
    }

    #[tokio::test]
    async fn renewing_reports_when_ownership_is_lost() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let coordinator = build(store.clone(), "w-1", 4);
        let claim = coordinator.claim(4, &now()).await.expect("claim").remove(0);

        assert!(coordinator
            .renew(claim.task.id, &now())
            .await
            .expect("renew"));

        let other = build(store.clone(), "w-2", 4);
        assert!(!other.renew(claim.task.id, &now()).await.expect("renew"));
        assert!(!coordinator
            .renew(mas_common::ids::TaskId::new(), &now())
            .await
            .expect("renew"));
    }

    #[tokio::test]
    async fn coordinators_require_an_identity_and_a_sane_ceiling() {
        let store = Arc::new(InMemoryDurableExecution::new());
        assert!(AttemptCoordinator::new(store.clone(), "", 4, Duration::from_secs(1)).is_err());
        assert!(
            AttemptCoordinator::new(store.clone(), "w-1", usize::MAX, Duration::from_secs(1))
                .is_err()
        );
        let coordinator =
            AttemptCoordinator::new(store.clone(), "w-1", 4, Duration::from_secs(30)).expect("ok");
        assert_eq!(coordinator.worker(), "w-1");
        assert_eq!(coordinator.max_in_flight(), 4);
        assert_eq!(coordinator.lease_ttl(), Duration::from_secs(30));
        assert!(coordinator.lease_until(&now()).is_ok());
    }

    #[tokio::test]
    async fn still_owns_reflects_the_durable_fence() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let owner = build(store.clone(), "w-1", 4);
        let other = build(store.clone(), "w-2", 4);
        let claim = owner.claim(4, &now()).await.expect("claim").remove(0);

        assert!(owner.still_owns(&claim.task, &now()));
        assert!(!other.still_owns(&claim.task, &now()));
        assert!(
            !owner.still_owns(&claim.task, &later(120)),
            "an expired lease is not ownership"
        );
    }

    #[tokio::test]
    async fn aborting_a_claimed_execution_is_durable_and_idempotent() {
        let store = Arc::new(InMemoryDurableExecution::new());
        let receipt = seed(&store, "k1").await;
        let coordinator = build(store.clone(), "w-1", 4);

        let first = coordinator
            .cancel_execution(tenant(), receipt.execution.id, "corr", &now())
            .await
            .expect("cancel");
        assert_eq!(
            first.result,
            mas_application::durable::CancellationResult::Cancelled
        );
        let second = coordinator
            .cancel_execution(tenant(), receipt.execution.id, "corr", &now())
            .await
            .expect("cancel again");
        assert_eq!(
            second.result,
            mas_application::durable::CancellationResult::AlreadyCancelled
        );
    }

    #[tokio::test]
    async fn terminal_failure_helper_marks_a_cause_non_retryable() {
        let failure = terminal_failure("policy_denied", "tool not permitted").expect("failure");
        assert!(!failure.retryable);
        assert_eq!(failure.code, "policy_denied");
    }

    #[tokio::test]
    async fn cancelling_a_claim_settles_as_cancelled_not_failed() {
        let store = Arc::new(InMemoryDurableExecution::new());
        seed(&store, "k1").await;
        let coordinator = build(store.clone(), "w-1", 4);
        let claim = coordinator.claim(4, &now()).await.expect("claim").remove(0);

        let receipt = coordinator
            .cancel(&claim, "operator stopped the run", true, &now())
            .await
            .expect("settle");
        assert_eq!(receipt.task.status, TaskStatus::Cancelled);
        assert_eq!(receipt.attempt.status, mas_domain::AttemptStatus::Cancelled);
        assert_eq!(
            receipt.execution.as_ref().map(|e| e.status),
            Some(ExecutionStatus::Cancelled)
        );
        assert!(!receipt.dead_lettered);
    }
}
