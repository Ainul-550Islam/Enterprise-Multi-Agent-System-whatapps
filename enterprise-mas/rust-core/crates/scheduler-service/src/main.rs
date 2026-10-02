//! `mas-scheduler` process bootstrap.
//!
//! Bring-up modes:
//! * `--dev-inmemory` — in-memory schedule store + leases + loopback
//!   dispatcher + completion listener. Seeds one repeating schedule so the
//!   tick loop has visible work. Never production: no state survives.
//! * (default) — production composition: `mas_persistence::ScheduleStore`
//!   (due-scan + compare id) + `DurableDispatcher` (due runs become pending
//!   executions via the application pipeline) + `pg` schedule_runs journal
//!   as the cross-replica duplicate-fire guard.
//!
//! Environment (dev mode):
//! * `MAS_SCHEDULER_ADDR` — completion listener bind (default `127.0.0.1:8090`).
//! * `MAS_SCHEDULER_INTERVAL_MS` — tick interval (default `1000`).

use mas_common::ids::TenantId;
use mas_domain::{Schedule, ScheduleKind};
use mas_scheduler_service::dispatch::LoopbackDispatcher;
use mas_scheduler_service::driver::{DriverConfig, TickDriver};
use mas_scheduler_service::server::completion_router;
use mas_scheduling::lease::InMemoryLeaseStore;
use mas_scheduling::runner::{SchedulerConfig, SchedulerRuntime};
use mas_scheduling::store::InMemoryScheduleStore;
use mas_worker::grace;

#[tokio::main]
async fn main() {
    std::process::exit(match run().await {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("mas-scheduler: {err}");
            2
        },
    });
}

async fn run() -> Result<(), String> {
    let dev_inmemory = std::env::args().skip(1).any(|arg| arg == "--dev-inmemory");
    if !dev_inmemory {
        return run_production().await;
    }

    // ── Ports ──────────────────────────────────────────────────────────
    let store = InMemoryScheduleStore::new();
    let leases = InMemoryLeaseStore::new();

    // ── Seed: one heartbeat schedule, firing every 3 seconds ───────────
    let mut schedule = Schedule::new(
        TenantId::new(),
        None,
        "dev-heartbeat",
        ScheduleKind::Cron {
            expression: "*/2 * * * * *".to_owned(),
            timezone: "UTC".to_owned(),
        },
        serde_json::json!({"operation": "execution.run", "input": {"beat": true}}),
    )
    .map_err(|e| format!("seed schedule: {e}"))?;
    schedule.enable().map_err(|e| format!("enable: {e}"))?;
    store.seed(schedule.clone());

    // ── Layered configuration (files + env) ────────────────────────────
    let layered = mas_common::config::load_layered().map_err(|e| format!("config: {e}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let addr = merger.string(
        "MAS_SCHEDULER_ADDR",
        "scheduler.completion_addr",
        "127.0.0.1:8090",
    );
    let replica_id = merger.string(
        "MAS_SCHEDULER_REPLICA_ID",
        "scheduler.replica_id",
        "scheduler-1",
    );
    let interval_ms = merger
        .u64(
            "MAS_SCHEDULER_INTERVAL_MS",
            "scheduler.tick_interval_ms",
            1_000,
        )
        .map_err(|e| format!("config: {e}"))?;
    let recover = merger
        .bool("MAS_SCHEDULER_RECOVER", "scheduler.recover_on_start", true)
        .map_err(|e| format!("config: {e}"))?;
    let backoff_cap_ms = merger
        .u64(
            "MAS_SCHEDULER_BACKOFF_CAP_MS",
            "scheduler.error_backoff_cap_ms",
            30_000,
        )
        .map_err(|e| format!("config: {e}"))?;

    // ── Dispatcher + completion listener ───────────────────────────────
    let dispatcher = LoopbackDispatcher::new();
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(|e| format!("bind {addr}: {e}"))?;
    let app = completion_router(dispatcher.ledger().clone());
    let (handle, watch) = grace::channel();
    let mut http_watch = watch.clone();
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { http_watch.changed().await })
            .await
    });
    eprintln!("mas-scheduler dev: completion listener on {addr}");

    // ── Runtime + driver ───────────────────────────────────────────────
    let mut runtime = SchedulerRuntime::new(
        store,
        leases,
        dispatcher.clone(),
        SchedulerConfig::local(&replica_id).map_err(|e| format!("scheduler cfg: {e}"))?,
    )
    .map_err(|e| format!("runtime: {e}"))?;
    let mut driver = TickDriver::new(DriverConfig {
        tick_interval: std::time::Duration::from_millis(interval_ms),
        recover_on_start: recover,
        error_backoff_cap: std::time::Duration::from_millis(backoff_cap_ms),
    })
    .map_err(|e| format!("driver cfg: {e}"))?;

    tokio::spawn(grace::watch_signals(handle));
    eprintln!("mas-scheduler dev: ticking every {interval_ms}ms (ctrl-c to stop)");
    let stats = driver
        .run(&mut runtime, watch)
        .await
        .map_err(|e| format!("driver: {e}"))?;
    eprintln!(
        "stopped: ticks={} dispatched={} errors={} lease_lost={} clean={}",
        stats.ticks,
        stats.dispatched,
        stats.consecutive_errors,
        stats.lease_lost,
        stats.stopped_by_shutdown,
    );
    Ok(())
}

/// Production path: durable store + durable dispatcher + journal-divided
/// fire protection (unique schedule_runs (schedule_id, planned_at)).
async fn run_production() -> Result<(), String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
    use mas_persistence::repositories::schedules::ScheduleStore;
    use mas_scheduler_service::durable::DurableDispatcher;

    let layered = mas_common::config::load_layered().map_err(|e| format!("config: {e}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let replica_id = merger.string(
        "MAS_SCHEDULER_REPLICA_ID",
        "scheduler.replica_id",
        "scheduler-1",
    );
    let interval_ms = merger
        .u64(
            "MAS_SCHEDULER_INTERVAL_MS",
            "scheduler.tick_interval_ms",
            1_000,
        )
        .map_err(|e| format!("config: {e}"))?;
    let recover = merger
        .bool("MAS_SCHEDULER_RECOVER", "scheduler.recover_on_start", true)
        .map_err(|e| format!("config: {e}"))?;
    let backoff_cap_ms = merger
        .u64(
            "MAS_SCHEDULER_BACKOFF_CAP_MS",
            "scheduler.error_backoff_cap_ms",
            30_000,
        )
        .map_err(|e| format!("config: {e}"))?;

    let url = merger.string("MAS_DATABASE_URL", "database.url", "");
    if url.is_empty() {
        return Err("mas-scheduler production requires database.url / MAS_DATABASE_URL".to_owned());
    }
    let url = DatabaseUrl::new(url).map_err(|e| format!("database url: {e}"))?;
    let mut config =
        PoolConfig::for_url(url, "mas-scheduler").map_err(|e| format!("database config: {e}"))?;
    config.max_connections = 8;
    let database = Database::connect(&config)
        .await
        .map_err(|e| format!("database: {e}"))?;
    let pool = database.pool().clone();

    // Migrations must be current: schedulers crash-loop promptly otherwise.
    let runner = mas_persistence::migrations::MigrationRunner::new(pool.clone());
    runner
        .ensure_table()
        .await
        .map_err(|e| format!("migrations: {e}"))?;
    runner
        .verify()
        .await
        .map_err(|e| format!("migration ledger drift — apply scripts/migrate.sh up first: {e}"))?;

    let store = ScheduleStore::new(pool.clone());
    let leases = InMemoryLeaseStore::new();
    let dispatcher = DurableDispatcher::new(pool, &replica_id);

    let mut runtime = SchedulerRuntime::new(
        store,
        leases,
        dispatcher,
        SchedulerConfig::local(&replica_id).map_err(|e| format!("scheduler cfg: {e}"))?,
    )
    .map_err(|e| format!("runtime: {e}"))?;
    let mut driver = TickDriver::new(DriverConfig {
        tick_interval: std::time::Duration::from_millis(interval_ms),
        recover_on_start: recover,
        error_backoff_cap: std::time::Duration::from_millis(backoff_cap_ms),
    })
    .map_err(|e| format!("driver cfg: {e}"))?;

    let (handle, watch) = grace::channel();
    tokio::spawn(mas_worker::grace::watch_signals(handle));
    eprintln!(
        "mas-scheduler production: replica={replica_id} tick={interval_ms}ms (journal-guarded fires)"
    );
    let stats = driver
        .run(&mut runtime, watch)
        .await
        .map_err(|e| format!("driver: {e}"))?;
    eprintln!(
        "stopped: ticks={} dispatched={} errors={} clean={}",
        stats.ticks, stats.dispatched, stats.consecutive_errors, stats.stopped_by_shutdown,
    );
    Ok(())
}
