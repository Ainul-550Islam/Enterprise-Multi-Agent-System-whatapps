//! PURPOSE: `mas-scheduler` process bootstrap: the schedule loop, the
//! outbox relay and graceful shutdown.
//!
//! Owning layer: `mas-scheduler-service` (the process edge).
//!
//! Production responsibility: bring up exactly one production path — due
//! schedules in, durable executions out, outbox events published — and refuse
//! to start when a production dependency (broker URL, database) is missing
//! rather than degrading into a silent no-op.
//!
//! It must NOT:
//!   • run the in-memory dev lane without the explicit `--dev-inmemory` flag;
//!   • start the relay without a broker, or the loop without a durable store;
//!   • hold schedule state in process memory as if it were durable.

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
//!
//! Production also runs the **transactional outbox relay** alongside the tick
//! driver: every event the platform writes inside a domain transaction is
//! claimed and published exactly once, with ownership fenced by a row lease so
//! several replicas may run it and a killed replica's rows come back.

use std::sync::Arc;
use std::time::Duration;

use mas_common::ids::TenantId;
use mas_domain::{Schedule, ScheduleKind};
use mas_scheduler_service::dispatch::LoopbackDispatcher;
use mas_scheduler_service::driver::{DriverConfig, TickDriver};
use mas_scheduler_service::server::{completion_router, InternalAuth};
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
    // Internal calls are authenticated even in the dev lane. With no secret
    // configured every completion is refused, which is loud and correct: a
    // dev lane that accepts unauthenticated completions is a dev lane that
    // cannot reproduce a production authorization bug.
    let internal_auth = load_internal_auth(&merger)?;
    if !internal_auth.is_enabled() {
        eprintln!(
            "mas-scheduler dev: MAS_INTERNAL_SERVICE_SECRET is not set — \
             completion and stats routes will reject every request"
        );
    }
    let app = completion_router(dispatcher.ledger().clone(), internal_auth);
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
/// Loads the shared secret used to authenticate internal service calls.
///
/// The secret is a credential: it comes from the environment or the secret
/// manager, is wrapped so `Debug` cannot print it, and is zeroized on drop. It
/// is never read from a command-line argument, which would put it in the
/// process table.
fn load_internal_auth(
    merger: &mas_common::config::Merger<'_>,
) -> Result<mas_scheduler_service::server::InternalAuth, String> {
    let raw = merger.string(
        "MAS_INTERNAL_SERVICE_SECRET",
        "scheduler.internal_service_secret",
        "",
    );
    if raw.trim().is_empty() {
        return Ok(mas_scheduler_service::server::InternalAuth::new(None));
    }
    Ok(mas_scheduler_service::server::InternalAuth::new(Some(
        mas_security::SecretString::new(raw),
    )))
}

/// Loads production authentication and binds nonce receipt consumption to the
/// scheduler plane database role. A production completion listener without a
/// durable receipt authority would accept the same valid v2 request after a
/// restart, so missing secret or pool is a startup failure.
fn load_production_internal_auth(
    merger: &mas_common::config::Merger<'_>,
    receipt_pool: sqlx::PgPool,
) -> Result<InternalAuth, String> {
    let raw = merger.string(
        "MAS_INTERNAL_SERVICE_SECRET",
        "scheduler.internal_service_secret",
        "",
    );
    if raw.trim().is_empty() {
        return Err(
            "mas-scheduler production requires MAS_INTERNAL_SERVICE_SECRET / \
             scheduler.internal_service_secret for authenticated completion requests"
                .to_owned(),
        );
    }
    Ok(InternalAuth::with_postgres(
        Some(mas_security::SecretString::new(raw)),
        receipt_pool,
    ))
}

/// Builds one machine-plane pool from that plane's own URL.
///
/// Every plane is a separate database role with a separate credential, because
/// a plane is not a request: it has no principal and no tenant when its work
/// begins, so it cannot be expressed as an RLS policy. Confining the
/// exemption to a dedicated role per plane is what keeps `mas_app` — the role
/// request traffic uses — free of BYPASSRLS.
///
/// There is deliberately no fallback to the runtime pool. Under `mas_app` a
/// plane's query returns zero rows, and a scheduler that silently idles or an
/// audit writer that silently drops events is far worse than a process that
/// refuses to start and names the variable that has to be set.
async fn plane_pool(
    merger: &mas_common::config::Merger<'_>,
    env_var: &str,
    config_key: &str,
    application_name: &str,
    max_connections: u32,
) -> Result<sqlx::PgPool, String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};

    let raw = merger.string(env_var, config_key, "");
    if raw.is_empty() {
        return Err(format!(
            "mas-scheduler production requires {env_var} (config {config_key}): \
             the plane has no caller and therefore no row-level-security scope, \
             so it runs as its own database role — see migrations/0017_plane_roles \
             and migrations/0019_scheduler_plane_role"
        ));
    }
    let url = DatabaseUrl::new(raw).map_err(|e| format!("{env_var}: {e}"))?;
    let mut config =
        PoolConfig::for_url(url, application_name).map_err(|e| format!("{env_var} config: {e}"))?;
    config.max_connections = max_connections;
    let config = config
        .validated()
        .map_err(|e| format!("{env_var} config: {e}"))?;
    let database = Database::connect(&config)
        .await
        .map_err(|e| format!("{env_var} database: {e}"))?;
    Ok(database.pool().clone())
}

async fn run_production() -> Result<(), String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
    use mas_scheduler_service::dispatch::{HttpDispatcher, InFlightLedger};
    use mas_scheduler_service::durable::DurableDispatcher;
    use mas_scheduler_service::plane_store::SchedulerPlaneStore;

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
    let completion_addr = merger.string(
        "MAS_SCHEDULER_COMPLETION_ADDR",
        "scheduler.completion_addr",
        "",
    );
    if completion_addr.trim().is_empty() {
        return Err(
            "mas-scheduler production requires MAS_SCHEDULER_COMPLETION_ADDR / \
             scheduler.completion_addr; completion state must have an explicit listener"
                .to_owned(),
        );
    }

    // Outbox relay: the scheduler drains the transactional event outbox
    // alongside its tick driver, because it is already the process running a
    // durable loop against the same database with a graceful-drain path.
    let relay_enabled = merger
        .bool("MAS_OUTBOX_RELAY", "outbox.relay_enabled", true)
        .map_err(|e| format!("config: {e}"))?;
    let relay_batch = merger
        .u64("MAS_OUTBOX_BATCH", "outbox.relay_batch", 64)
        .map_err(|e| format!("config: {e}"))?;
    let relay_interval_ms = merger
        .u64("MAS_OUTBOX_INTERVAL_MS", "outbox.relay_interval_ms", 250)
        .map_err(|e| format!("config: {e}"))?;
    let relay_lease_ms = merger
        .u64("MAS_OUTBOX_LEASE_MS", "outbox.relay_lease_ms", 30_000)
        .map_err(|e| format!("config: {e}"))?;
    let nats_url = merger.string("MAS_NATS_URL", "broker.nats_url", "");

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

    // ── Machine-plane pools ─────────────────────────────────────────────
    //
    // `pool` above is the runtime pool (`mas_app`, no BYPASSRLS). These three
    // are the planes this process hosts. Each is a separate role with a
    // separate credential, and each is required: a missing one is a
    // configuration error, not a reason to fall back to `mas_app`.
    //
    //   * `mas_scheduler` — the cross-tenant due-scan. SELECT on `schedules`
    //     and `schedule_runs` and nothing else (migration 0019).
    //   * `mas_audit`     — append-only audit lane (migration 0017). Audit
    //     events are written even when the tick that raised them rolls back;
    //     a denial that is not recorded did not happen, as far as anyone can
    //     prove later.
    //   * `mas_outbox`    — the relay reads rows committed by transactions
    //     that have already ended, so it cannot carry their GUCs (0017).
    let scheduler_plane = plane_pool(
        &merger,
        "MAS_DATABASE_SCHEDULER_URL",
        "database.scheduler_url",
        "mas-scheduler-plane",
        4,
    )
    .await?;
    let audit_plane = plane_pool(
        &merger,
        "MAS_DATABASE_AUDIT_URL",
        "database.audit_url",
        "mas-scheduler-audit",
        4,
    )
    .await?;
    let outbox_plane = plane_pool(
        &merger,
        "MAS_DATABASE_OUTBOX_URL",
        "database.outbox_url",
        "mas-scheduler-outbox",
        4,
    )
    .await?;

    // The schedule store is the two-pool adapter: plane connection for the
    // due-scan, runtime connection inside the schedule's own scope for
    // everything after it.
    let store = SchedulerPlaneStore::new(scheduler_plane.clone(), pool.clone());
    let leases = InMemoryLeaseStore::new();

    // Completion state is separate from scheduling storage but must use the
    // same durable nonce authority across scheduler replicas. The ledger is a
    // local overlap optimization; the signature receipt is the actual replay
    // security boundary.
    let completion_ledger = InFlightLedger::default();
    let completion_auth = load_production_internal_auth(&merger, scheduler_plane.clone())?;

    // ── Dispatcher selection ────────────────────────────────────────────
    //
    // `durable` (the default, and the only lane that uses the canonical
    // pipeline) submits each fired tick through `DurableExecutionPort`, so a
    // due run becomes a real execution with DB-enforced idempotency.
    //
    // `http` posts to the API's internal schedule-run endpoint instead. It
    // exists for deployments that keep submission behind the API's
    // authorization surface, and it still proves itself: the request is signed
    // with the internal service secret and passes through the SSRF guard.
    //
    // The choice is explicit configuration rather than a compile-time
    // accident, and anything other than these two values is a boot failure.
    let dispatcher_kind = merger.string(
        "MAS_SCHEDULER_DISPATCHER",
        "scheduler.dispatcher",
        "durable",
    );
    let dispatcher: Arc<dyn mas_scheduling::runner::DispatchPort> = match dispatcher_kind.as_str() {
        "durable" => Arc::new(DurableDispatcher::new(
            scheduler_plane,
            pool.clone(),
            audit_plane,
            &replica_id,
        )),
        "http" => {
            let api_base_url = merger.string("MAS_SCHEDULER_API_URL", "scheduler.api_base_url", "");
            if api_base_url.is_empty() {
                return Err(
                    "scheduler.dispatcher=http requires scheduler.api_base_url / \
                     MAS_SCHEDULER_API_URL: the destination cannot be guessed, and \
                     a dispatcher with nowhere to send a run is a scheduler that \
                     silently drops ticks"
                        .to_owned(),
                );
            }
            // The host the API lives on, and only that host. An internal
            // endpoint policy with no allowlist would be an open permit to
            // reach any private address, which is the vulnerability the guard
            // exists to close — so the host is derived from the configured URL
            // rather than left empty.
            let host = api_base_url
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .split('/')
                .next()
                .unwrap_or_default()
                .split(':')
                .next()
                .unwrap_or_default()
                .to_owned();
            if host.is_empty() {
                return Err(format!(
                    "scheduler.api_base_url has no host component: {api_base_url}"
                ));
            }
            let policy =
                mas_integrations::http_guard::EgressPolicy::for_internal_endpoints([host], None);
            let internal_secret = {
                let raw = merger.string(
                    "MAS_INTERNAL_SERVICE_SECRET",
                    "scheduler.internal_service_secret",
                    "",
                );
                if raw.trim().is_empty() {
                    None
                } else {
                    Some(mas_security::SecretString::new(raw))
                }
            };
            if internal_secret.is_none() {
                eprintln!(
                    "mas-scheduler: MAS_INTERNAL_SERVICE_SECRET is not set — internal \
                     schedule-run calls will be unsigned and the API will refuse them"
                );
            }
            let http = HttpDispatcher::new(
                completion_ledger.clone(),
                &api_base_url,
                internal_secret,
                "mas-scheduler",
                policy,
            )
            .map_err(|e| format!("http dispatcher: {e}"))?;
            Arc::new(http)
        },
        other => {
            return Err(format!(
                "scheduler.dispatcher must be one of durable|http (got {other:?})"
            ))
        },
    };

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
    tokio::spawn(mas_worker::grace::watch_signals(handle.clone()));

    // Production previously never started this listener even though the
    // endpoint was implemented. Bind before the driver starts so a replica
    // cannot dispatch work that has no authenticated completion path.
    let completion_listener = tokio::net::TcpListener::bind(&completion_addr)
        .await
        .map_err(|error| format!("bind completion listener {completion_addr}: {error}"))?;
    let completion_app = completion_router(completion_ledger, completion_auth);
    let mut completion_shutdown = watch.clone();
    let completion_failure_shutdown = handle.clone();
    let completion_server = tokio::spawn(async move {
        if let Err(error) = axum::serve(completion_listener, completion_app)
            .with_graceful_shutdown(async move { completion_shutdown.changed().await })
            .await
        {
            tracing::error!(%error, "scheduler completion listener stopped unexpectedly");
            // This listener is part of the authenticated scheduling surface.
            // Do not keep dispatching while its completion path is unhealthy.
            completion_failure_shutdown.trigger();
        }
    });
    eprintln!(
        "mas-scheduler production: replica={replica_id} tick={interval_ms}ms \
         completion_listener={completion_addr} (journal-guarded fires)"
    );

    // ── Outbox relay ───────────────────────────────────────────────────
    // Runs concurrently with the tick driver. Every event the platform writes
    // in a domain transaction lands here and is published exactly once; the
    // relay's only state is the row claim, so this replica can be killed at
    // any point and the rows come back.
    let relay = if relay_enabled {
        Some(
            spawn_outbox_relay(
                outbox_plane.clone(),
                &replica_id,
                &nats_url,
                relay_batch,
                Duration::from_millis(relay_interval_ms),
                Duration::from_millis(relay_lease_ms),
                watch.clone(),
            )
            .await?,
        )
    } else {
        eprintln!("mas-scheduler: outbox relay disabled by configuration");
        None
    };

    let stats = driver
        .run(&mut runtime, watch)
        .await
        .map_err(|e| format!("driver: {e}"))?;
    handle.trigger();
    completion_server
        .await
        .map_err(|error| format!("completion listener task: {error}"))?;
    eprintln!(
        "stopped: ticks={} dispatched={} errors={} clean={}",
        stats.ticks, stats.dispatched, stats.consecutive_errors, stats.stopped_by_shutdown,
    );

    if let Some(relay) = relay {
        relay.abort();
    }
    Ok(())
}

/// Starts the outbox relay as a background task.
///
/// The relay drains the transactional outbox to the event bus. It owns no
/// state beyond the row claim, so it is safe to run several replicas and safe
/// to kill: rows whose claimant dies become claimable again after the lease.
///
/// # Errors
///
/// Returns a boot failure when the bus cannot be reached. A relay that cannot
/// publish would claim rows and then fail every one of them, so refusing to
/// start is the useful behaviour.
async fn spawn_outbox_relay(
    pool: sqlx::PgPool,
    replica_id: &str,
    nats_url: &str,
    batch: u64,
    interval: Duration,
    lease: Duration,
    mut shutdown: mas_worker::grace::ShutdownWatch,
) -> Result<tokio::task::JoinHandle<()>, String> {
    use mas_application::pipeline_metrics::PipelineMetrics;
    use mas_events::outbox::{OutboxRelay, OutboxStorePort, RetryPolicy};
    use mas_persistence::repositories::outbox::PostgresOutboxStore;

    if nats_url.is_empty() {
        return Err(
            "outbox relay requires broker.nats_url / MAS_NATS_URL: the event plane \
             is the only transport that publishes durable outbox rows, and \
             starting without it would dead-letter every event"
                .to_owned(),
        );
    }

    #[cfg(feature = "nats")]
    let bus: Arc<dyn mas_events::bus::EventBusPort> = Arc::new(
        mas_messaging::nats::NatsEventBus::connect(nats_url)
            .await
            .map_err(|e| format!("outbox relay: event bus: {e}"))?,
    );

    // Built without the `nats` feature there is no production event transport,
    // and refusing to start is the only honest option: a bus that goes
    // nowhere would let the relay mark events published that nobody received.
    #[cfg(not(feature = "nats"))]
    let bus: Arc<dyn mas_events::bus::EventBusPort> = {
        let _ = nats_url;
        return Err(
            "outbox relay requires mas-scheduler built with the `nats` feature; \
             there is no in-process fallback for production event publication"
                .to_owned(),
        );
    };

    let store: Arc<dyn OutboxStorePort> = Arc::new(PostgresOutboxStore::new(pool));

    // The worker identity is unique per replica: two replicas sharing a claim
    // name could settle each other's rows.
    let worker = format!("{replica_id}-relay");
    let metrics = PipelineMetrics::default();
    let relay =
        OutboxRelay::new(store, bus, RetryPolicy::default(), worker.clone()).with_lease(lease);
    let batch = usize::try_from(batch.clamp(1, 1_000)).unwrap_or(64);

    eprintln!("mas-scheduler: outbox relay {worker} batch={batch} every {interval:?}");

    Ok(tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    eprintln!("mas-scheduler: outbox relay draining");
                    return;
                },
                _ = tokio::time::sleep(interval) => {},
            }

            match relay.relay_once(batch).await {
                Ok(cycle) if cycle.total() == 0 => {
                    // An empty cycle still says something worth recording: the
                    // backlog is drained. A gauge left at its last non-zero
                    // value would read as a permanent backlog.
                    metrics.set_outbox_backlog(0);
                },
                Ok(cycle) => {
                    // Emitted from the cycle the relay already reports, so the
                    // counters and the log line can never disagree.
                    metrics.set_outbox_backlog(i64::try_from(cycle.total()).unwrap_or(i64::MAX));
                    for _ in 0..cycle.dead_lettered {
                        metrics.dead_letter();
                    }
                    for _ in 0..cycle.lost {
                        metrics.stale_worker_rejection();
                    }
                    tracing::info!(
                        worker = %worker,
                        published = cycle.published,
                        dead_lettered = cycle.dead_lettered,
                        requeued = cycle.requeued,
                        released = cycle.released,
                        lost = cycle.lost,
                        "outbox relay cycle"
                    );
                },
                Err(error) => tracing::warn!(
                    worker = %worker,
                    %error,
                    "outbox relay cycle failed; the batch stays claimed until its lease expires"
                ),
            }
        }
    }))
}
