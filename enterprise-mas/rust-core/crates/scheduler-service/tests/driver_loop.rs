//! End-to-end: wall-clock driver + deterministic runtime over the real
//! in-memory store/leases, plus request-level coverage of the HTTP
//! dispatch adapter and completion listener.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ScheduleId, TenantId};
use mas_common::result::Result as MResult;
use mas_common::timestamps::Timestamp;
use mas_domain::{Schedule, ScheduleKind};
use mas_scheduler_service::dispatch::{
    CompletionNotice, HttpDispatcher, InFlightLedger, LoopbackDispatcher,
};
use mas_scheduler_service::driver::{DriverConfig, TickDriver};
use mas_scheduler_service::server::completion_router;
use mas_scheduling::due::DueWork;
use mas_scheduling::lease::InMemoryLeaseStore;
use mas_scheduling::runner::{DispatchAck, DispatchPort, SchedulerConfig, SchedulerRuntime};
use mas_scheduling::store::{InMemoryScheduleStore, ScheduleStorePort};
use mas_worker::grace;

/// An enabled every-second cron schedule that comes due ≤1s after seeding
/// (domain floor for `Interval` is 10s; six-field cron has seconds).
fn due_now_schedule(name: &str, tenant: TenantId) -> Schedule {
    let mut schedule = Schedule::new(
        tenant,
        None,
        name,
        ScheduleKind::Cron {
            expression: "*/1 * * * * *".to_owned(),
            timezone: "UTC".to_owned(),
        },
        serde_json::json!({"operation": "heartbeat", "input": {}}),
    )
    .expect("schedule");
    schedule.enable().expect("enable");
    schedule
}

fn driver(interval_ms: u64) -> TickDriver {
    TickDriver::new(DriverConfig {
        tick_interval: Duration::from_millis(interval_ms),
        recover_on_start: false,
        error_backoff_cap: Duration::from_millis(500),
    })
    .expect("driver")
}

/// Dispatcher that defers while `defer` is set, then starts runs.
#[derive(Debug)]
struct FlipDispatcher {
    defer: Arc<AtomicBool>,
    started: Arc<AtomicU64>,
    ledger: InFlightLedger,
}

#[async_trait::async_trait]
impl DispatchPort for FlipDispatcher {
    async fn dispatch(&self, work: &DueWork) -> MResult<DispatchAck> {
        if self.defer.load(Ordering::SeqCst) {
            return Ok(DispatchAck::Deferred);
        }
        self.ledger.start(work.schedule_id);
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(DispatchAck::Started {
            execution: ExecutionId::new(),
        })
    }

    async fn is_running(&self, schedule: ScheduleId) -> MResult<bool> {
        Ok(self.ledger.is_running(schedule))
    }

    async fn mark_finished(&self, schedule: ScheduleId) -> MResult<()> {
        self.ledger.finish(schedule);
        Ok(())
    }
}

/// Store wrapper that fails `list_due` while the gate is closed.
#[derive(Debug)]
struct FlakyStore {
    inner: InMemoryScheduleStore,
    gate_open: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl ScheduleStorePort for FlakyStore {
    async fn list_due(&self, now: &Timestamp, limit: usize) -> MResult<Vec<Schedule>> {
        if !self.gate_open.load(Ordering::SeqCst) {
            return Err(AppError::database("store unreachable"));
        }
        self.inner.list_due(now, limit).await
    }

    async fn get(&self, id: ScheduleId) -> MResult<Option<Schedule>> {
        self.inner.get(id).await
    }

    async fn save(&self, schedule: &Schedule) -> MResult<()> {
        self.inner.save(schedule).await
    }

    async fn record_run(&self, schedule_id: ScheduleId, planned_at: Timestamp) -> MResult<bool> {
        self.inner.record_run(schedule_id, planned_at).await
    }
}

/// Waits (bounded) until `sentinel` fires; panics after 5s.
async fn wait_until(sentinel: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !sentinel() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sentinel never fired"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn driver_dispatches_due_schedule_then_drains_cleanly() {
    let store = InMemoryScheduleStore::new();
    store.seed(due_now_schedule("driver-e2e", TenantId::new()));
    let dispatcher = LoopbackDispatcher::new();
    let counter = dispatcher.dispatched.clone();
    let mut runtime = SchedulerRuntime::new(
        store,
        InMemoryLeaseStore::new(),
        dispatcher,
        SchedulerConfig::local("driver-e2e").expect("cfg"),
    )
    .expect("runtime");

    let (handle, watch) = grace::channel();
    let run = tokio::spawn(async move {
        let mut driver = driver(25);
        driver.run(&mut runtime, watch).await
    });

    wait_until(|| counter.load(Ordering::SeqCst) >= 1).await;
    handle.trigger();
    let stats = run.await.expect("join").expect("stats");
    assert!(stats.ticks >= 1);
    assert!(stats.dispatched >= 1);
    assert!(stats.stopped_by_shutdown);
    assert_eq!(stats.consecutive_errors, 0);
}

#[tokio::test]
async fn deferred_runs_requeue_and_later_dispatch() {
    let store = InMemoryScheduleStore::new();
    store.seed(due_now_schedule("defer-e2e", TenantId::new()));
    let defer = Arc::new(AtomicBool::new(true));
    let started = Arc::new(AtomicU64::new(0));
    let dispatcher = FlipDispatcher {
        defer: defer.clone(),
        started: started.clone(),
        ledger: InFlightLedger::default(),
    };
    let mut runtime = SchedulerRuntime::new(
        store,
        InMemoryLeaseStore::new(),
        dispatcher,
        SchedulerConfig::local("defer-e2e").expect("cfg"),
    )
    .expect("runtime");

    let (handle, watch) = grace::channel();
    let run = tokio::spawn(async move {
        let mut driver = driver(25);
        driver.run(&mut runtime, watch).await
    });

    // A few deferral rounds pass (bounded by wall clock); nothing starts.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        started.load(Ordering::SeqCst),
        0,
        "deferred work is held, not started"
    );
    defer.store(false, Ordering::SeqCst);

    let sentinel = started.clone();
    wait_until(move || sentinel.load(Ordering::SeqCst) >= 1).await;
    handle.trigger();
    let stats = run.await.expect("join").expect("stats");
    assert!(stats.dispatched >= 1, "deferred runs converge: {stats:?}");
    assert_eq!(stats.dispatch_errors, 0, "deferred ≠ errored");
    assert!(stats.stopped_by_shutdown);
}

#[tokio::test]
async fn failing_store_backs_off_and_recovers_on_repair() {
    let store = InMemoryScheduleStore::new();
    store.seed(due_now_schedule("flaky-e2e", TenantId::new()));
    let gate = Arc::new(AtomicBool::new(false));
    let flaky = FlakyStore {
        inner: store,
        gate_open: gate.clone(),
    };
    let dispatcher = LoopbackDispatcher::new();
    let counter = dispatcher.dispatched.clone();
    let mut runtime = SchedulerRuntime::new(
        flaky,
        InMemoryLeaseStore::new(),
        dispatcher,
        SchedulerConfig::local("flaky-e2e").expect("cfg"),
    )
    .expect("runtime");

    let (handle, watch) = grace::channel();
    let run = tokio::spawn(async move {
        let mut driver = driver(25);
        driver.run(&mut runtime, watch).await
    });

    // Errors accumulate while the gate is closed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    gate.store(true, Ordering::SeqCst);

    let sentinel = counter.clone();
    wait_until(move || sentinel.load(Ordering::SeqCst) >= 1).await;
    handle.trigger();
    let stats = run.await.expect("join").expect("stats");
    assert!(
        stats.dispatched >= 1,
        "after repair the schedule fires: {stats:?}",
    );
    assert_eq!(stats.consecutive_errors, 0, "errors reset once healthy");
    assert!(stats.stopped_by_shutdown);
}

#[tokio::test]
async fn http_dispatcher_round_trip_with_completion_listener() {
    // ── Fake API downstream: accepts runs, returns an execution id ──
    let execution_id = ExecutionId::new();
    let api = axum::Router::new().route(
        "/v1/internal/schedule-runs",
        axum::routing::post(move || async move {
            axum::Json(serde_json::json!({ "execution_id": execution_id }))
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let api_addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, api).await });

    // ── Scheduler side: dispatcher + completion listener on the loopback ──
    let ledger = InFlightLedger::default();
    let dispatcher = HttpDispatcher::new(ledger.clone(), &format!("http://{api_addr}"), None)
        .expect("dispatcher");
    let work = DueWork {
        schedule_id: ScheduleId::new(),
        tenant_id: TenantId::new(),
        planned_at: Timestamp::now(),
        task_template: serde_json::json!({"operation": "heartbeat"}),
    };
    let ack = DispatchPort::dispatch(&dispatcher, &work)
        .await
        .expect("dispatched");
    assert!(matches!(ack, DispatchAck::Started { .. }));
    assert!(
        ledger.is_running(work.schedule_id),
        "acceptance inserts into the ledger",
    );

    // Completion listener bound for real (exercises the public route).
    let completion_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let completion_addr = completion_listener.local_addr().expect("addr");
    let ledger_for_listener = ledger.clone();
    tokio::spawn(async move {
        axum::serve(completion_listener, completion_router(ledger_for_listener)).await
    });

    let notice = CompletionNotice {
        schedule_id: work.schedule_id,
        execution_id,
        outcome: "succeeded".to_owned(),
    };
    let response = reqwest::Client::new()
        .post(format!("http://{completion_addr}/v1/scheduler/completions"))
        .json(&notice)
        .send()
        .await
        .expect("posted completion");
    assert_eq!(response.status().as_u16(), 204);
    assert!(
        !ledger.is_running(work.schedule_id),
        "completion drops the entry"
    );
}
