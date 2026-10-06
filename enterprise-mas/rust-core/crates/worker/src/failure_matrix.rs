// PURPOSE: The canonical durable execution pipeline's **failure and recovery
// matrix**: one executed test per way this pipeline can fail, and one assertion
// about what the durable state looks like afterwards.
//
// Owning layer: `mas-worker` (the consumer/attempt plane), exercising the
// `mas-application` durable port.
//
// Production responsibility: this file is the executable answer to "what if it
// fails here?". It exists because the pipeline makes a promise — a run is never
// silently successful and never permanently unrecoverable — and a promise that
// only lives in prose is not a promise. Every case here pins one row of that
// matrix: the injected fault, the state the system must be left in, and the
// recovery that must still be possible.
//
// It must NOT:
//   • assert on in-process bookkeeping (a claim map, a counter, a channel) as
//     if it were durable truth — every assertion reads through the durable port
//     or the broker, which is what a restarted process would see;
//   • weaken a failure into a success anywhere to make a test pass;
//   • mock the durability away — the faults are injected at the durable write
//     boundary and at the broker, which is where real failures happen.
//
// The seventeen cases, in the order the spec enumerates them:
//    1  DB failure before commit
//    2  DB failure after the execution insert but before the task insert
//    3  DB failure during the outbox insert
//    4  broker publish failure
//    5  worker crash before attempt creation
//    6  worker crash after attempt creation
//    7  worker crash after the runtime produced its result
//    8  worker crash before the terminal state commit
//    9  duplicate broker delivery
//   10  duplicate client idempotency request
//   11  stale worker completion
//   12  retryable provider error
//   13  terminal provider error
//   14  cancellation during execution
//   15  process restart
//   16  partial workflow failure
//   17  duplicate outbox relay delivery  → `crates/events/src/outbox.rs`
//
// Case 17 lives with the relay because that is where the ownership fence it
// depends on is implemented; it is listed here so the matrix reads as one list.

#![cfg(test)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mas_application::durable::{
    CancellationReceipt, CancellationRequest, CancellationResult, ClaimQuery, DurableExecutionPort,
    ExecutionTransitionReceipt, ExecutionTransitionRequest, ExecutionUsage, PipelineCounters,
    SettlementReceipt, SubmissionIntent, SubmissionKind, SubmissionReceipt, TaskClaim,
    TaskSettlement,
};
use mas_application::durable_memory::InMemoryDurableExecution;
use mas_application::fingerprint::{FingerprintInput, RequestFingerprint};
use mas_common::enums::{ExecutionStatus, TaskPriority, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, OrganizationId, ProjectId, TaskId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_contracts::task::TaskQueueMessage;
use mas_domain::TransitionActor;
use mas_messaging::broker::{BrokerPort, InMemoryBroker, PublishRequest};
use mas_messaging::codec::{CodecConfig, JsonEventCodec};
use mas_messaging::headers::HeaderSet;
use mas_messaging::subjects::Subject;
use tokio::sync::Mutex;

use crate::attempts::{AttemptCoordinator, ClaimOneOutcome};
use crate::backoff::RetryPolicy;
use crate::classify::{classify, FailureClass};
use crate::consumer::{ConsumerConfig, ConsumerOutcome, TaskConsumer};
use crate::handler::{canned::CannedHandler, OperationDispatcher};

// ---------------------------------------------------------------------------
// Fault injection at the durable write boundary
// ---------------------------------------------------------------------------

/// A durable port that fails one named method on demand.
///
/// The fault is armed per method and consumed by the next call to that method,
/// so a test can say "the *next* settle fails" without perturbing every other
/// write. Faults are armed as `AppError::Database`, which is exactly what a
/// connection loss, a serialisation failure or a statement timeout produces in
/// production — not an invented error shape.
///
/// This models the boundary a real transaction crosses: a caller cannot tell
/// (and must not need to tell) whether a failed write committed nothing or
/// committed and rolled back. What it *can* rely on is that `Err` means "no
/// success was reported", and this wrapper enforces that no inner write is
/// reachable once the fault fires, so the observable state is the
/// pre-write state.
#[derive(Debug)]
struct FaultDurable {
    inner: Arc<dyn DurableExecutionPort>,
    faults: Mutex<HashMap<&'static str, VecDeque<AppError>>>,
}

impl FaultDurable {
    fn new(inner: Arc<dyn DurableExecutionPort>) -> Self {
        Self {
            inner,
            faults: Mutex::new(HashMap::new()),
        }
    }

    /// Arms one failure for the next call to `method`.
    async fn arm(&self, method: &'static str, message: &str) {
        self.faults
            .lock()
            .await
            .entry(method)
            .or_default()
            .push_back(AppError::Database(message.to_owned()));
    }

    /// Consumes the next armed failure for `method`, if any.
    async fn take(&self, method: &'static str) -> Option<AppError> {
        self.faults.lock().await.get_mut(method)?.pop_front()
    }
}

#[async_trait]
impl DurableExecutionPort for FaultDurable {
    async fn submit(&self, intent: SubmissionIntent) -> Result<SubmissionReceipt> {
        if let Some(error) = self.take("submit").await {
            return Err(error);
        }
        self.inner.submit(intent).await
    }

    async fn claim(&self, query: ClaimQuery) -> Result<Vec<TaskClaim>> {
        if let Some(error) = self.take("claim").await {
            return Err(error);
        }
        self.inner.claim(query).await
    }

    async fn renew_lease(&self, task_id: TaskId, worker: &str, until: Timestamp) -> Result<bool> {
        if let Some(error) = self.take("renew_lease").await {
            return Err(error);
        }
        self.inner.renew_lease(task_id, worker, until).await
    }

    async fn settle(&self, settlement: TaskSettlement) -> Result<SettlementReceipt> {
        if let Some(error) = self.take("settle").await {
            return Err(error);
        }
        self.inner.settle(settlement).await
    }

    async fn cancel(&self, request: CancellationRequest) -> Result<CancellationReceipt> {
        if let Some(error) = self.take("cancel").await {
            return Err(error);
        }
        self.inner.cancel(request).await
    }

    async fn transition(
        &self,
        request: ExecutionTransitionRequest,
    ) -> Result<ExecutionTransitionReceipt> {
        if let Some(error) = self.take("transition").await {
            return Err(error);
        }
        self.inner.transition(request).await
    }

    async fn counters(&self, worker: &str, at: &Timestamp) -> Result<PipelineCounters> {
        if let Some(error) = self.take("counters").await {
            return Err(error);
        }
        self.inner.counters(worker, at).await
    }

    async fn list_executions(
        &self,
        tenant: TenantId,
        limit: usize,
    ) -> Result<Vec<mas_domain::Execution>> {
        if let Some(error) = self.take("list_executions").await {
            return Err(error);
        }
        self.inner.list_executions(tenant, limit).await
    }

    async fn load_task(
        &self,
        tenant: TenantId,
        task_id: TaskId,
    ) -> Result<Option<(mas_domain::Task, Option<mas_domain::Execution>)>> {
        if let Some(error) = self.take("load_task").await {
            return Err(error);
        }
        self.inner.load_task(tenant, task_id).await
    }

    async fn load_execution(
        &self,
        tenant: TenantId,
        execution_id: ExecutionId,
    ) -> Result<Option<mas_domain::Execution>> {
        if let Some(error) = self.take("load_execution").await {
            return Err(error);
        }
        self.inner.load_execution(tenant, execution_id).await
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// One pipeline under test: a broker the test can break, a durable store the
/// test can break, and a handler the test can script.
struct Rig {
    broker: Arc<InMemoryBroker>,
    durable: Arc<InMemoryDurableExecution>,
    handler: Arc<CannedHandler>,
}

fn rig() -> Rig {
    let broker = InMemoryBroker::new();
    broker.set_available(true);
    Rig {
        broker: Arc::new(broker),
        durable: Arc::new(InMemoryDurableExecution::new()),
        handler: Arc::new(CannedHandler::default()),
    }
}

const TENANT: u128 = 0xF00D;
const ORG: u128 = 1;
const PROJECT: u128 = 2;
const WORKFLOW: u128 = 3;

fn tenant() -> TenantId {
    TenantId::from_uuid(uuid::Uuid::from_u128(TENANT))
}

fn organization() -> OrganizationId {
    OrganizationId::from_uuid(uuid::Uuid::from_u128(ORG))
}

fn project() -> ProjectId {
    ProjectId::from_uuid(uuid::Uuid::from_u128(PROJECT))
}

fn workflow() -> WorkflowId {
    WorkflowId::from_uuid(uuid::Uuid::from_u128(WORKFLOW))
}

/// The fixed base instant every timestamp in this file is derived from, plus
/// an offset in seconds. Time is injected, never read, so "an hour later" is
/// reproducible.
fn at(offset_secs: u64) -> Timestamp {
    Timestamp::from_unix_seconds(1_700_000_000 + i64::try_from(offset_secs).expect("fits"))
        .expect("fixed timestamp")
}

/// The payload a key names. Deterministic, because the fingerprint that
/// identifies a *duplicate* request must be reproducible from the request.
fn payload(key: &str) -> serde_json::Value {
    serde_json::json!({"job": key})
}

/// The canonical fingerprint for a key: same key ⇒ same bytes, so a client
/// retry is recognised as a replay and a changed body as a conflict.
fn fingerprint(key: &str) -> String {
    RequestFingerprint::submission(&FingerprintInput {
        tenant_id: tenant(),
        organization_id: organization(),
        project_id: project(),
        workflow_id: Some(workflow()),
        agent_id: None,
        operation: "execution.run",
        input: &payload(key),
        priority: TaskPriority::High,
        deadline: None,
    })
    .expect("fingerprint")
}

/// One submission request, keyed by `key`, with the canonical fingerprint.
fn submission_intent(key: &str) -> SubmissionIntent {
    SubmissionIntent {
        tenant_id: tenant(),
        organization_id: organization(),
        project_id: project(),
        workflow_id: Some(workflow()),
        agent_id: None,
        input: payload(key),
        idempotency_key: Some(key.to_owned()),
        fingerprint: fingerprint(key),
        operation: "execution.run".to_owned(),
        priority: TaskPriority::High,
        max_attempts: 3,
        deadline: None,
        correlation_id: format!("corr-{key}"),
        created_by: "user-1".to_owned(),
        actor: TransitionActor::principal("user-1", tenant()).expect("actor"),
        create_initial_attempt: false,
        now: at(0),
    }
}

/// Submits one execution the way production does and returns its task.
async fn seed(rig: &Rig, key: &str) -> mas_domain::Task {
    let receipt = rig
        .durable
        .submit(submission_intent(key))
        .await
        .expect("submit");
    receipt.task
}

fn queue_message(task: &mas_domain::Task) -> TaskQueueMessage {
    TaskQueueMessage::staged(
        task.id,
        task.tenant_id,
        task.organization_id,
        task.project_id,
        task.operation.clone(),
        task.input.clone(),
        task.execution_id,
        task.agent_id,
        task.workflow_id,
        task.idempotency_key.clone(),
        task.priority,
        1,
        task.max_attempts,
        task.deadline,
        "corr-matrix",
    )
    .expect("message")
}

async fn publish(rig: &Rig, payload: &TaskQueueMessage) -> u64 {
    let codec = JsonEventCodec::new(CodecConfig::default());
    rig.broker
        .publish(PublishRequest {
            subject: Subject::parse("mas.tasks.execution.run").expect("subject"),
            headers: HeaderSet::default(),
            payload: codec.encode_value(payload).expect("frame"),
            msg_id: None,
        })
        .await
        .expect("publish")
}

fn coordinator(
    durable: Arc<dyn DurableExecutionPort>,
    worker: &str,
    lease_secs: u64,
) -> AttemptCoordinator {
    AttemptCoordinator::new(durable, worker, 4, Duration::from_secs(lease_secs))
        .expect("coordinator")
}

fn dispatcher(handler: Arc<CannedHandler>) -> OperationDispatcher {
    OperationDispatcher::new().with_handler("execution.run", handler)
}

fn config(worker: &str) -> ConsumerConfig {
    ConsumerConfig {
        consumer_name: "c-matrix".to_owned(),
        filter: mas_messaging::subjects::SubjectFilter::parse("mas.tasks.>").expect("filter"),
        batch_size: 4,
        ack_wait: Duration::from_millis(200),
        poll_interval: Duration::from_millis(5),
        lease_ttl: Duration::from_millis(150),
        max_in_flight: 4,
        drain_timeout: Duration::from_secs(3),
        worker_name: worker.to_owned(),
        retry: RetryPolicy {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(40),
            max_deliver: 10,
        },
    }
}

/// Runs a consumer for `millis` and then asks it to drain, which is the shape
/// of a graceful stop.
async fn run_consumer(
    rig: &Rig,
    dispatcher: OperationDispatcher,
    worker: &str,
    millis: u64,
) -> ConsumerOutcome {
    run_consumer_on(rig, rig.durable.clone(), dispatcher, worker, millis).await
}

/// As [`run_consumer`], but over a specific durable port so a test can inject
/// faults at the write boundary the worker actually uses.
async fn run_consumer_on(
    rig: &Rig,
    durable: Arc<dyn DurableExecutionPort>,
    dispatcher: OperationDispatcher,
    worker: &str,
    millis: u64,
) -> ConsumerOutcome {
    let attempts = Arc::new(coordinator(durable, worker, 150));
    let consumer = TaskConsumer::new(
        rig.broker.clone(),
        attempts,
        Arc::new(dispatcher),
        config(worker),
    );
    let (handle, watch) = crate::grace::channel();
    let run = tokio::spawn(async move { consumer.run(watch).await });
    tokio::time::sleep(Duration::from_millis(millis)).await;
    handle.trigger();
    run.await.expect("join").expect("run")
}

async fn task_row(rig: &Rig, id: TaskId) -> mas_domain::Task {
    rig.durable
        .load_task(tenant(), id)
        .await
        .expect("load")
        .expect("row")
        .0
}

async fn execution_row(rig: &Rig, id: ExecutionId) -> mas_domain::Execution {
    rig.durable
        .load_execution(tenant(), id)
        .await
        .expect("load")
        .expect("row")
}

/// Claims `task` durably, or panics with a readable reason.
async fn claim_owned(
    coord: &AttemptCoordinator,
    task: &mas_domain::Task,
    when: Timestamp,
) -> TaskClaim {
    match coord
        .claim_one(task.id, task.tenant_id, &when)
        .await
        .expect("claim_one")
    {
        ClaimOneOutcome::Claimed(claim) => *claim,
        other => panic!("expected a claim, got {other:?}"),
    }
}

/// One metered run fans `ExecutionUsage` out into one usage observation per
/// quota dimension (input tokens, output tokens, tool calls). "How many runs
/// were metered" is therefore a delta, not a row count.
const OBSERVATIONS_PER_METERED_RUN: usize = 3;

/// The current number of durable usage observations.
fn metered(rig: &Rig) -> usize {
    rig.durable.usage().len()
}

/// Asserts that exactly one more run was metered since `before`.
fn assert_metered_once(rig: &Rig, before: usize) {
    let after = metered(rig);
    assert!(
        after > before,
        "a successful run must be metered: {before} observations before, {after} after"
    );
    assert_eq!(
        after - before,
        OBSERVATIONS_PER_METERED_RUN,
        "exactly one run's worth of usage, not a duplicate meter"
    );
}

fn usage() -> ExecutionUsage {
    ExecutionUsage {
        provider: Some("openai".to_owned()),
        model: Some("gpt-4o-mini".to_owned()),
        tokens_input: 100,
        tokens_output: 20,
        tool_calls: 1,
        steps: 2,
        cost_micros: Some(1_500),
    }
}

// ---------------------------------------------------------------------------
// 1. DB failure before commit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_01_db_failure_before_commit_leaves_nothing_behind() {
    let rig = rig();
    let faulted = Arc::new(FaultDurable::new(rig.durable.clone()));
    faulted.arm("submit", "connection reset by peer").await;

    let intent = submission_intent("idem-db-1");

    let error = faulted.submit(intent.clone()).await.expect_err("must fail");
    assert!(
        matches!(error, AppError::Database(_)),
        "the failure must surface as a database error, not be swallowed: {error:?}"
    );

    // Nothing was reported successful, and nothing exists: no execution, no
    // task, no outbox event, no idempotency reservation. A caller that saw
    // `Err` can retry the same key and get a clean first submission.
    assert!(
        rig.durable
            .list_executions(tenant(), 100)
            .await
            .expect("list")
            .is_empty(),
        "a failed commit must not leave an execution"
    );
    assert!(
        rig.durable.outbox().is_empty(),
        "a failed commit must not leave outbox rows"
    );
    assert!(
        rig.durable.idempotency().is_empty(),
        "a failed commit must not consume the idempotency key"
    );

    // And recovery is possible: the same request submitted again succeeds.
    let receipt = rig
        .durable
        .submit(intent)
        .await
        .expect("retry after failure");
    assert_eq!(
        receipt.kind,
        SubmissionKind::Created,
        "a failure before commit must not make the key permanently unusable"
    );
}

// ---------------------------------------------------------------------------
// 2. DB failure after the execution insert but before the task insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_02_db_failure_mid_transaction_is_reported_as_failure_not_success() {
    // The submission unit is ONE transaction: execution + idempotency + task +
    // attempt + audit + outbox. There is no "after the execution insert but
    // before the task insert" state to observe, because a transaction has no
    // partially-visible state. What this case pins is the two consequences:
    //
    //   (a) a failure anywhere inside the unit is reported as a failure, and
    //   (b) the unit is retryable — the key is not burned and the store is not
    //       left holding a queued execution with no task to run.
    let rig = rig();
    let faulted = Arc::new(FaultDurable::new(rig.durable.clone()));

    // Submit once, cleanly, so there is a baseline.
    let first = seed(&rig, "idem-mid-1").await;
    assert_eq!(first.status, TaskStatus::Queued);

    // Now the *next* submission fails. The point of the arm-then-call ordering
    // is that the fault fires at the durable write, i.e. inside the unit.
    faulted.arm("submit", "deadlock detected").await;
    let intent = submission_intent("idem-mid-2");
    assert!(
        faulted.submit(intent).await.is_err(),
        "a failure inside the atomic unit must be reported as a failure"
    );

    // The store is unchanged by the failed unit: still exactly one execution
    // and one task, and the failed key can still be used.
    assert_eq!(
        rig.durable
            .list_executions(tenant(), 100)
            .await
            .expect("list")
            .len(),
        1
    );
    assert_eq!(
        rig.durable.outbox().len(),
        2,
        "only the successful submission emitted events"
    );

    // Every queued execution has a task to run — the invariant that a partial
    // commit would have violated.
    for execution in rig
        .durable
        .list_executions(tenant(), 100)
        .await
        .expect("list")
    {
        let counters = rig
            .durable
            .counters("probe", &at(1))
            .await
            .expect("counters");
        assert!(
            counters.claimable_tasks >= 1 || execution.status != ExecutionStatus::Queued,
            "a queued execution must always have runnable work"
        );
    }
}

// ---------------------------------------------------------------------------
// 3. DB failure during the outbox insert
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_03_db_failure_during_outbox_insert_does_not_orphan_an_execution() {
    // The outbox row is written in the SAME transaction as the execution. If
    // that write fails, the execution must not exist: an execution with no
    // event describing it is invisible to everything downstream and is the
    // classic "committed but nobody was told" bug.
    let rig = rig();
    let faulted = Arc::new(FaultDurable::new(rig.durable.clone()));
    faulted.arm("submit", "could not serialize access").await;

    let intent = submission_intent("idem-outbox");

    faulted
        .submit(intent)
        .await
        .expect_err("the outbox write fails");

    // No execution without its events, and no events without an execution:
    // event rows are always paired with the state they describe.
    assert!(
        rig.durable.outbox().is_empty()
            && rig
                .durable
                .list_executions(tenant(), 100)
                .await
                .expect("l")
                .is_empty(),
        "execution and outbox are one unit; one cannot exist without the other"
    );

    // Recovery: retrying the identical request produces a complete unit.
    let retry = submission_intent("idem-outbox");
    let receipt = rig.durable.submit(retry).await.expect("retry");
    assert_eq!(receipt.kind, SubmissionKind::Created);
    assert_eq!(receipt.events.len(), 2, "the retried unit emits its events");
    assert_eq!(rig.durable.outbox().len(), 2);
}

// ---------------------------------------------------------------------------
// 4. Broker publish failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_04_broker_publish_failure_leaves_the_task_claimable() {
    // Work is durable before anyone tries to tell the broker about it. If the
    // broker is down, the task must still be sitting there claimable, and the
    // relay (not the submission path) is what eventually publishes the event.
    let rig = rig();

    // The submission itself never depends on the broker: it writes an outbox
    // row in the transaction. Break the broker first and prove that.
    rig.broker.set_available(false);
    let task = seed(&rig, "idem-nobroker").await;
    assert_eq!(
        task.status,
        TaskStatus::Queued,
        "submission is durable without a broker"
    );
    assert_eq!(
        rig.durable.outbox().len(),
        2,
        "the events are durable, not yet published"
    );

    // The outbox rows are unpublished — that is the backlog an operator sees.
    let backlog = rig
        .durable
        .outbox()
        .iter()
        .filter(|row| row.published_at.is_none())
        .count();
    assert_eq!(
        backlog, 2,
        "a broker outage converts into backlog, never into loss"
    );

    // With the broker back, the work is still runnable: nothing was lost.
    rig.broker.set_available(true);
    let coord = coordinator(rig.durable.clone(), "w-1", 30);
    let claim = claim_owned(&coord, &task, at(1)).await;
    assert_eq!(claim.task.status, TaskStatus::Running);
}

// ---------------------------------------------------------------------------
// 5. Worker crash before attempt creation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_05_worker_crash_before_attempt_creation_leaves_the_task_claimable() {
    // The delivery arrived and the worker died before it ever took ownership.
    // The durable port was never successfully called, so the store must be
    // byte-for-byte what it was: no attempt row, no owner, still claimable.
    let rig = rig();
    let task = seed(&rig, "idem-crash-early").await;
    assert_eq!(rig.durable.attempts().len(), 0);

    let faulted = Arc::new(FaultDurable::new(rig.durable.clone()));
    faulted.arm("claim", "connection reset by peer").await;
    let crashed = coordinator(faulted.clone(), "w-crashed", 30);
    let dead = crashed
        .claim_one(task.id, task.tenant_id, &at(1))
        .await
        .expect_err("the worker never reached the durable write");
    assert!(
        matches!(dead, AppError::Database(_)),
        "the caller sees the failure; it is not turned into a silent success: {dead:?}"
    );
    drop(crashed); // the process is gone, along with everything it held in memory

    let row = task_row(&rig, task.id).await;
    assert_eq!(row.status, TaskStatus::Queued, "the task never moved");
    assert_eq!(row.locked_by, None, "no ownership was taken");
    assert_eq!(rig.durable.attempts().len(), 0, "no attempt row exists");

    // Recovery: the database comes back and any worker takes the work. The
    // crash cost one retry, not the request.
    let healthy = coordinator(rig.durable.clone(), "w-after-crash", 30);
    let reclaimed = claim_owned(&healthy, &task, at(2)).await;
    assert_eq!(reclaimed.task.status, TaskStatus::Running);
    assert_eq!(rig.durable.attempts().len(), 1);

    let receipt = healthy
        .succeed(&reclaimed, usage(), true, &at(3))
        .await
        .expect("settle");
    assert_eq!(receipt.task.status, TaskStatus::Completed);
}

// ---------------------------------------------------------------------------
// 6. Worker crash after attempt creation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_06_worker_crash_after_attempt_creation_is_reclaimable_after_the_lease() {
    // The attempt row exists, so the task is Running and owned. A crashed
    // worker holds that ownership until its lease expires — that is the point
    // of the lease — after which another worker takes over cleanly.
    let rig = rig();
    let task = seed(&rig, "idem-crash-claim").await;

    let crashed = coordinator(rig.durable.clone(), "w-crashed", 30);
    let _claim = claim_owned(&crashed, &task, at(0)).await;
    assert_eq!(task_row(&rig, task.id).await.status, TaskStatus::Running);
    assert_eq!(rig.durable.attempts().len(), 1, "the attempt is durable");
    drop(crashed); // crash

    // While the lease is live, another worker must NOT take the task: two
    // parallel machines for one piece of work is the bug this prevents.
    let other = coordinator(rig.durable.clone(), "w-other", 30);
    let contested = other
        .claim_one(task.id, task.tenant_id, &at(10))
        .await
        .expect("claim_one");
    assert!(
        !matches!(contested, ClaimOneOutcome::Claimed(_)),
        "a live lease must not be stealable"
    );

    // After the lease expires, ownership is reclaimed and the task runs once.
    let recovered = coordinator(rig.durable.clone(), "w-recovered", 30);
    let reclaimed = claim_owned(&recovered, &task, at(31)).await;
    assert_eq!(reclaimed.task.status, TaskStatus::Running);
    assert_eq!(
        task_row(&rig, task.id).await.locked_by.as_deref(),
        Some("w-recovered"),
        "ownership advanced to the worker that is actually running it"
    );
    assert_eq!(
        rig.durable.attempts().len(),
        2,
        "attempt count comes from task_attempts, never from broker metadata"
    );

    // And it can still finish: not permanently unrecoverable.
    let receipt = recovered
        .succeed(&reclaimed, usage(), true, &at(32))
        .await
        .expect("settle");
    assert_eq!(receipt.task.status, TaskStatus::Completed);
}

// ---------------------------------------------------------------------------
// 7. Worker crash after the runtime produced its result
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_07_worker_crash_after_the_runtime_result_is_not_lost_and_reruns() {
    // The runtime did the work and then the process died before the durable
    // write. The result is lost — that is unavoidable without a durable
    // in-runtime journal — but the *task* is not: it is reclaimed and rerun,
    // and the final state is a real success, not a silent one.
    let rig = rig();
    let task = seed(&rig, "idem-crash-result").await;

    let crashed = coordinator(rig.durable.clone(), "w-crashed", 30);
    // The claim is deliberately dropped on the floor: the worker owned the
    // task and then died holding it, which is the state being modelled.
    let _claim = claim_owned(&crashed, &task, at(0)).await;
    rig.handler
        .push_failure(AppError::Internal("boom".to_owned()));

    // The handler ran and produced a result; then the process died.
    let outcome = dispatch(&rig.handler, &queue_message(&task)).await;
    assert!(outcome.is_err(), "the runtime result was a failure here");
    drop(crashed); // crash before settle

    let row = task_row(&rig, task.id).await;
    assert_eq!(
        row.status,
        TaskStatus::Running,
        "the crash left no terminal state; the task is not silently successful"
    );
    assert!(
        rig.durable.usage().is_empty(),
        "the lost result was not metered: nothing was reported successful"
    );

    // After the lease expires a healthy worker reruns it and settles for real.
    let before = metered(&rig);
    let healthy = coordinator(rig.durable.clone(), "w-healthy", 30);
    let reclaimed = claim_owned(&healthy, &task, at(31)).await;
    let receipt = healthy
        .succeed(&reclaimed, usage(), true, &at(32))
        .await
        .expect("settle");

    assert_eq!(receipt.task.status, TaskStatus::Completed);
    assert_eq!(
        execution_row(&rig, task.execution_id.expect("execution"))
            .await
            .status,
        ExecutionStatus::Completed
    );
    assert_metered_once(&rig, before);
}

// ---------------------------------------------------------------------------
// 8. Worker crash before the terminal state commit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_08_failure_to_commit_the_terminal_state_is_reported_and_retryable() {
    // The durable write of the terminal outcome fails. The worker must see an
    // error (not treat "I ran it, so it must be fine" as success) and the task
    // must remain recoverable.
    let rig = rig();
    let task = seed(&rig, "idem-commit-fail").await;
    let faulted = Arc::new(FaultDurable::new(rig.durable.clone()));

    let coord = coordinator(rig.durable.clone(), "w-1", 30);
    let claim = claim_owned(&coord, &task, at(0)).await;

    // Swap the port under the coordinator's feet to the failing one.
    faulted.arm("settle", "statement timeout").await;
    let failing = coordinator(faulted.clone(), "w-1", 30);
    let error = failing
        .succeed(&claim, usage(), true, &at(1))
        .await
        .expect_err("the terminal write fails");
    assert!(
        matches!(error, AppError::Database(_)),
        "a failed terminal commit must surface as an error: {error:?}"
    );

    // The task is still Running and still owned: not terminal, not lost.
    let row = task_row(&rig, task.id).await;
    assert_eq!(
        row.status,
        TaskStatus::Running,
        "no terminal state was faked"
    );
    assert!(
        rig.durable.usage().is_empty(),
        "usage is committed with the terminal state, so a failed commit meters nothing"
    );

    // Recovery: the same worker settles again once the database is healthy.
    let before = metered(&rig);
    let receipt = coord
        .succeed(&claim, usage(), true, &at(2))
        .await
        .expect("retry the settle");
    assert_eq!(receipt.task.status, TaskStatus::Completed);
    assert_metered_once(&rig, before);
}

// ---------------------------------------------------------------------------
// 9. Duplicate broker delivery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_09_duplicate_broker_delivery_runs_the_work_once() {
    // At-least-once delivery means the same message arrives twice. The durable
    // attempt fence, not the broker, is what decides whether work runs.
    let rig = rig();
    let task = seed(&rig, "idem-dup-delivery").await;
    let message = queue_message(&task);
    publish(&rig, &message).await;
    publish(&rig, &message).await;

    let outcome = run_consumer(&rig, dispatcher(rig.handler.clone()), "w-1", 250).await;

    assert_eq!(outcome.acked, 2, "both deliveries are settled");
    assert_eq!(
        rig.handler.invocations(),
        1,
        "the work ran once: ownership, not delivery count, is the authority"
    );
    let row = task_row(&rig, task.id).await;
    assert_eq!(row.status, TaskStatus::Completed);
    assert_eq!(
        rig.durable.attempts().len(),
        1,
        "one attempt for one run: a duplicate delivery must not double-run"
    );
}

// ---------------------------------------------------------------------------
// 10. Duplicate client idempotency request
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_10_duplicate_client_idempotency_request_is_a_safe_replay() {
    // The client retried because it did not see the answer. Same key, same
    // fingerprint ⇒ the original receipt comes back and no second run starts.
    let rig = rig();
    let first = seed(&rig, "idem-replay").await;

    // The identical request, re-sent: same key, same body, same fingerprint.
    let intent = submission_intent("idem-replay");
    let replayed = rig.durable.submit(intent).await.expect("replay");

    assert_eq!(
        replayed.kind,
        SubmissionKind::Replayed,
        "a safe replay, not a conflict (got {:?})",
        replayed.kind
    );
    assert_eq!(
        replayed.execution.id,
        first.execution_id.expect("execution")
    );
    assert_eq!(
        replayed.task.id, first.id,
        "the same task, not a second one"
    );

    // A replay emits exactly one event and it is the *replay marker*, not a
    // second `execution.submitted`/`task.created` pair: operators can see the
    // duplicate without any consumer being able to mistake it for new work.
    assert_eq!(
        replayed.events.len(),
        1,
        "one replay marker, not a duplicate of the original events"
    );
    assert_eq!(
        rig.durable.outbox().len(),
        3,
        "the outbox grew by exactly the replay marker"
    );
    assert_eq!(
        rig.durable
            .list_executions(tenant(), 100)
            .await
            .expect("l")
            .len(),
        1,
        "no second execution"
    );
    assert_eq!(
        rig.durable.idempotency().len(),
        1,
        "one idempotency record for one key"
    );
}

// ---------------------------------------------------------------------------
// 11. Stale worker completion
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_11_a_stale_worker_completion_is_rejected() {
    // A loses its lease, B takes ownership, A finishes late and tries to write.
    // A must be rejected: a stale worker must never transition a task it no
    // longer owns.
    let rig = rig();
    let task = seed(&rig, "idem-stale").await;

    let a = coordinator(rig.durable.clone(), "w-a", 30);
    let claim_a = claim_owned(&a, &task, at(0)).await;
    assert_eq!(
        task_row(&rig, task.id).await.locked_by.as_deref(),
        Some("w-a")
    );

    // The lease expires and B takes over.
    let b = coordinator(rig.durable.clone(), "w-b", 30);
    let claim_b = claim_owned(&b, &task, at(31)).await;
    assert_eq!(
        task_row(&rig, task.id).await.locked_by.as_deref(),
        Some("w-b"),
        "ownership advanced to B"
    );

    // A completes late.
    let late = a.succeed(&claim_a, usage(), true, &at(32)).await;
    assert!(
        matches!(late, Err(AppError::Conflict(_))),
        "a stale worker must not be able to write: {late:?}"
    );

    // B's write is the one that counts, and the state reflects B's outcome.
    let receipt = b
        .succeed(&claim_b, usage(), true, &at(33))
        .await
        .expect("B settles");
    assert_eq!(receipt.task.status, TaskStatus::Completed);
    assert_eq!(
        task_row(&rig, task.id).await.status,
        TaskStatus::Completed,
        "A's rejected write changed nothing"
    );
    assert_eq!(
        metered(&rig),
        OBSERVATIONS_PER_METERED_RUN,
        "A's late success must not meter: one run, one meter"
    );
}

// ---------------------------------------------------------------------------
// 12. Retryable provider error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_12_a_retryable_provider_error_requeues_and_preserves_the_class() {
    // A transient upstream failure must be classified retryable, keep the
    // error code stable, leave the task runnable, and not be mistaken for
    // success or flattened into a generic internal error.
    let rig = rig();
    let task = seed(&rig, "idem-retryable").await;

    let coord = coordinator(rig.durable.clone(), "w-1", 30);
    let claim = claim_owned(&coord, &task, at(0)).await;

    let error = AppError::RateLimited("provider returned 429".to_owned());
    let classified = classify(&error);
    assert_eq!(
        classified.class,
        FailureClass::Retryable,
        "a rate limit is retryable"
    );
    assert!(
        !classified.code.is_empty(),
        "the code is stable and machine-readable"
    );

    let receipt = coord
        .fail(&claim, &classified, false, &at(1))
        .await
        .expect("settle");

    assert!(
        receipt.requeued,
        "a retryable failure schedules another attempt"
    );
    assert!(!receipt.dead_lettered);
    let row = task_row(&rig, task.id).await;
    assert!(
        row.status == TaskStatus::Queued || row.status == TaskStatus::Pending,
        "the task is runnable again, not terminal: {:?}",
        row.status
    );
    assert_eq!(
        row.last_error_code.as_deref(),
        Some(classified.code.as_str())
    );
    assert!(
        rig.durable.usage().is_empty(),
        "a failed run meters nothing"
    );
}

// ---------------------------------------------------------------------------
// 13. Terminal provider error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_13_a_terminal_provider_error_is_final_and_never_retried() {
    // An authentication failure is terminal: it will not fix itself, so
    // retrying it only burns capacity and delays the operator's signal.
    let rig = rig();
    let task = seed(&rig, "idem-terminal").await;

    let coord = coordinator(rig.durable.clone(), "w-1", 30);
    let claim = claim_owned(&coord, &task, at(0)).await;

    let error = AppError::Unauthorized("provider rejected the credential".to_owned());
    let classified = classify(&error);
    assert_eq!(classified.class, FailureClass::Terminal);
    assert!(!classified.is_retryable());

    let receipt = coord
        .fail(&claim, &classified, true, &at(1))
        .await
        .expect("settle");

    assert!(!receipt.requeued, "a terminal failure schedules no retry");
    assert!(
        receipt.dead_lettered,
        "a failure that can never succeed on retry is dead-lettered, not retried forever"
    );
    let row = task_row(&rig, task.id).await;
    assert_eq!(
        row.status,
        TaskStatus::DeadLettered,
        "the task is finished and will not be picked up again"
    );
    assert_eq!(
        row.last_error_code.as_deref(),
        Some(classified.code.as_str()),
        "the stable code survives, not a generic internal error"
    );
    assert_eq!(
        execution_row(&rig, task.execution_id.expect("execution"))
            .await
            .status,
        ExecutionStatus::Failed,
        "an authentication failure is not mapped to success or to a generic internal error"
    );
    assert!(
        rig.durable.usage().is_empty(),
        "a failed run meters nothing"
    );
}

// ---------------------------------------------------------------------------
// 14. Cancellation during execution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_14_cancellation_during_execution_is_durable_and_idempotent() {
    // Two shapes, both durable, both idempotent:
    //
    //   * cancel while queued  ⇒ cancelled outright, the task can never run;
    //   * cancel while running ⇒ the cooperative flag is set on the *execution*
    //     (not in worker memory), the runtime observes it and settles Cancelled.
    //
    // Neither may reopen a finished run, and neither may meter the work.
    let rig = rig();

    // ── (a) cancel before the work starts ──────────────────────────────────
    let queued = seed(&rig, "idem-cancel-queued").await;
    let cancel = rig
        .durable
        .cancel(CancellationRequest {
            tenant_id: tenant(),
            execution_id: queued.execution_id.expect("execution"),
            actor: TransitionActor::principal("user-1", tenant()).expect("actor"),
            correlation_id: "corr-cancel-queued".to_owned(),
            now: at(1),
        })
        .await
        .expect("cancel");
    assert_eq!(
        cancel.result,
        CancellationResult::Cancelled,
        "a queued execution is cancelled outright"
    );
    assert_eq!(cancel.execution.status, ExecutionStatus::Cancelled);

    // A cancelled execution's task is not claimable, so it can never run late.
    let coord = coordinator(rig.durable.clone(), "w-1", 30);
    let contested = coord
        .claim_one(queued.id, queued.tenant_id, &at(2))
        .await
        .expect("claim_one");
    assert!(
        !matches!(contested, ClaimOneOutcome::Claimed(_)),
        "a cancelled task must not be claimable"
    );

    // ── (b) cancel while running ───────────────────────────────────────────
    let running = seed(&rig, "idem-cancel-running").await;
    let execution_id = running.execution_id.expect("execution");
    let claim = claim_owned(&coord, &running, at(0)).await;

    let signalled = rig
        .durable
        .cancel(CancellationRequest {
            tenant_id: tenant(),
            execution_id,
            actor: TransitionActor::principal("user-1", tenant()).expect("actor"),
            correlation_id: "corr-cancel".to_owned(),
            now: at(1),
        })
        .await
        .expect("cancel");

    assert_eq!(
        signalled.result,
        CancellationResult::SignalSet,
        "a running execution is stopped cooperatively, not deleted underneath the worker"
    );
    assert!(
        signalled.execution.cancellation_requested(),
        "the request is durable state the restarted runtime would also see"
    );

    // A second cancel is a no-op: no second transition, no second history.
    let again = rig
        .durable
        .cancel(CancellationRequest {
            tenant_id: tenant(),
            execution_id,
            actor: TransitionActor::principal("user-1", tenant()).expect("actor"),
            correlation_id: "corr-cancel-2".to_owned(),
            now: at(2),
        })
        .await
        .expect("cancelling twice is legal");
    assert_eq!(again.result, CancellationResult::SignalSet);
    assert_eq!(
        again.execution.version(),
        signalled.execution.version(),
        "an idempotent cancel applies no second transition: the version is unchanged"
    );
    assert!(
        again.execution.cancellation_requested(),
        "the flag is still set; the second request did not clear it"
    );

    // Cancelling the execution also cancels its live task, which is what
    // actually stops the work: the task row becomes terminal, so no worker —
    // including the one holding the lease — can settle it any more.
    let stopped = task_row(&rig, running.id).await;
    assert_eq!(
        stopped.status,
        TaskStatus::Cancelled,
        "the live task is cancelled, not left running"
    );

    // The worker that was running it arrives late with a result. Both a
    // success and a failure are refused: the version fence is what makes
    // cancellation final.
    let late_success = coord.succeed(&claim, usage(), true, &at(4)).await;
    assert!(
        matches!(late_success, Err(AppError::Conflict(_))),
        "a late success must not reopen a cancelled run: {late_success:?}"
    );

    let late_failure = coord
        .fail(
            &claim,
            &classify(&AppError::Internal("worker died mid-step".to_owned())),
            true,
            &at(5),
        )
        .await;
    assert!(
        matches!(late_failure, Err(AppError::Conflict(_))),
        "a late failure must not overwrite a cancellation: {late_failure:?}"
    );

    assert_eq!(
        task_row(&rig, running.id).await.status,
        TaskStatus::Cancelled,
        "the cancellation survived both late writes"
    );
    assert!(
        rig.durable.usage().is_empty(),
        "a cancelled run meters nothing"
    );

    // The execution carries the cancellation: a restarted process reads it
    // from the row, not from a flag the dead worker was holding.
    let execution = execution_row(&rig, execution_id).await;
    assert!(
        execution.cancellation_requested(),
        "the cooperative flag is durable state"
    );
}

// ---------------------------------------------------------------------------
// 15. Process restart
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_15_process_restart_recovers_from_durable_state_alone() {
    // The recovery story is the whole point of durable attempts: a brand-new
    // coordinator object, in a brand-new process, reconstructs exactly what it
    // needs from the database. Nothing load-bearing lives in memory.
    let rig = rig();
    let task = seed(&rig, "idem-restart").await;

    // Process 1: claim, then "die" (dropped, including all in-memory state).
    let before = coordinator(rig.durable.clone(), "w-1", 30);
    let claim = claim_owned(&before, &task, at(0)).await;
    let observed_active = before.active_attempts(&at(1)).await.expect("active");
    assert_eq!(observed_active, 1, "in-flight work is counted");
    drop(before);

    // Process 2: a fresh coordinator with no memory of the first.
    let after = coordinator(rig.durable.clone(), "w-1", 30);
    assert_eq!(
        after.active_attempts(&at(2)).await.expect("active"),
        1,
        "the restarted process sees the same in-flight work from the database"
    );

    // And the restarted process settles it: no permanent wedge.
    let before = metered(&rig);
    let receipt = after
        .succeed(&claim, usage(), true, &at(3))
        .await
        .expect("settle after restart");
    assert_eq!(receipt.task.status, TaskStatus::Completed);
    assert_eq!(after.active_attempts(&at(4)).await.expect("active"), 0);

    // A restart *after* completion does not re-run anything.
    let restarted = coordinator(rig.durable.clone(), "w-2", 30);
    let contested = restarted
        .claim_one(task.id, task.tenant_id, &at(100))
        .await
        .expect("claim_one");
    assert!(
        !matches!(contested, ClaimOneOutcome::Claimed(_)),
        "a finished task is not claimable after a restart"
    );
    assert_metered_once(&rig, before);
}

// ---------------------------------------------------------------------------
// 16. Partial workflow failure
// ---------------------------------------------------------------------------

#[tokio::test]
async fn case_16_a_partial_workflow_failure_is_a_failure_not_a_partial_success() {
    // Some steps produced output and then the run failed. The durable record
    // must say *failed*: a partially-completed workflow is not a success, and
    // the work it did get through must not be metered as if it were.
    let rig = rig();
    let task = seed(&rig, "idem-partial").await;
    let execution_id = task.execution_id.expect("execution");

    let coord = coordinator(rig.durable.clone(), "w-1", 30);
    let claim = claim_owned(&coord, &task, at(0)).await;

    // The runtime completed two steps and then hit a terminal error.
    let classified = classify(&AppError::Validation {
        message: "step 3 rejected the step-2 output".to_owned(),
        issues: vec![],
    });
    assert_eq!(classified.class, FailureClass::Terminal);

    let receipt = coord
        .fail(&claim, &classified, true, &at(1))
        .await
        .expect("settle");

    assert_eq!(receipt.task.status, TaskStatus::DeadLettered);
    assert!(
        receipt.dead_lettered,
        "a failure that cannot succeed on retry is finished deliberately, not retried"
    );
    assert!(
        receipt.usage.is_empty(),
        "partial work is not metered: no synthetic counters, no half-success meters"
    );
    assert!(
        rig.durable.usage().is_empty(),
        "usage is authoritative: a failed execution produces no meter"
    );

    let execution = execution_row(&rig, execution_id).await;
    assert_eq!(
        execution.status,
        ExecutionStatus::Failed,
        "the execution is failed even though some steps completed"
    );
    assert!(
        rig.durable
            .outbox()
            .iter()
            .any(|row| row.envelope.event_type.contains("failed")),
        "the failure is announced on the same event path as a success"
    );
}

// ---------------------------------------------------------------------------
// Small helpers the cases above rely on
// ---------------------------------------------------------------------------

/// Runs the handler once, the way the consumer does — used to model "the
/// runtime produced a result and then the process died".
async fn dispatch(
    handler: &CannedHandler,
    message: &TaskQueueMessage,
) -> Result<serde_json::Value> {
    use crate::handler::TaskHandlerPort;
    handler.execute(message).await
}
