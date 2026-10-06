// PURPOSE:
// The production composition root for the worker process.
//
// There is exactly one bring-up path. It selects the broker and the durable
// store, binds `execution.run` to `AgentRunHandler` over a connected,
// health-probed gRPC bridge, and aborts the boot when bridge configuration is
// missing or the bridge is unreachable. A worker that cannot reach the AI
// orchestrator must not start: starting anyway would let it claim tasks and
// then report failures for work it could never have performed.
//
// There is deliberately no `--dev-inmemory` lane and no echo handler. A second
// bring-up path that answers tasks with canned output is a second execution
// lifecycle, and the two cannot be told apart from the outside: both report
// success. The queue plumbing is exercised by `mas-worker`'s own tests against
// a real in-process broker, which is where that coverage belongs.

//! `mas-worker` process bootstrap.
//!
//! Bring-up: durable handler registry + durable attempt coordination in
//! PostgreSQL. The **broker lane** selects at boot via `broker.kind`:
//! `pg` (default, SKIP-LOCKED claims over the durable task tables) or `nats`
//! (feature `nats`, JetStream stream + explicit-ack pull consumers over
//! `MAS_NATS_URL` / `broker.nats_url`).
//!
//! Every claim, every attempt and every settlement goes through
//! `DurableExecutionPort`, so the worker holds no authoritative state of its
//! own: a restart re-derives everything from the database.
//!
//! Consumer provisioning, the AI bridge health probe, and fast-fail on missing
//! configuration all happen before the loop starts.
//!
//! Environment: `MAS_DATABASE_URL`, the AI bridge settings (see
//! `agent_execution`), plus `MAS_NATS_URL` for the nats lane.

mod agent_execution;

use std::sync::Arc;

// `AgentBridge` is imported for its `health()` method, which the boot-time
// readiness probe calls on the concrete bridge.
use agent_execution::{AgentBridgeSettings, AgentRunHandler};
use mas_execution::AgentBridge;
use mas_messaging::broker::BrokerPort;
use mas_worker::consumer::{ConsumerConfig, TaskConsumer};
use mas_worker::handler::OperationDispatcher;
use mas_worker::AttemptCoordinator;
use mas_worker::ConsumerOutcome;
use mas_worker_service::run_slots::ControlPlaneRunSlots;

#[tokio::main]
async fn main() {
    std::process::exit(match run().await {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("mas-worker: {err}");
            2
        },
    });
}

/// The single bring-up path.
///
/// Order matters and is deliberate:
///
/// 1. **Config** — every value the worker needs is resolved before anything is
///    bound.
/// 2. **Database** — the durable store is the only authority the worker has, so
///    it is connected first.
/// 3. **Broker lane** — selected and health-probed, so a worker never starts
///    against a broker it cannot see.
/// 4. **Durable attempt coordination** — claims, attempts and settlements all
///    go through `DurableExecutionPort`; the worker keeps no authoritative
///    state of its own, so a restart re-derives everything.
/// 5. **AI bridge** — connected and health-probed. A missing or unreachable
///    bridge aborts the boot rather than starting a worker that would claim
///    tasks it cannot run.
/// 6. **Consume** — `execution.run` dispatches through
///    `AgentRunHandler` → canonical runtime → gRPC bridge → Python.
///
/// # Errors
///
/// Returns a human-readable boot failure. Every failure path names the knob
/// that has to change, because a worker that refuses to start is only useful
/// if the operator can tell why.
async fn run() -> Result<(), String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
    use mas_persistence::repositories::PgDurableExecution;

    let layered = mas_common::config::load_layered().map_err(|e| format!("config: {e}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let worker_name = merger.string("MAS_WORKER_NAME", "worker.worker_name", "worker-1");
    let poll_ms = merger
        .u64("MAS_WORKER_POLL_MS", "worker.poll_interval_ms", 100)
        .map_err(|e| format!("config: {e}"))?;
    let consumer_name = merger.string("MAS_WORKER_CONSUMER", "worker.consumer_name", "tasks-main");
    let max_in_flight = merger
        .u64("MAS_WORKER_MAX_IN_FLIGHT", "worker.max_in_flight", 8)
        .map_err(|e| format!("config: {e}"))?
        .max(1) as usize;
    let lease_ttl_ms = merger
        .u64("MAS_WORKER_LEASE_TTL_MS", "worker.lease_ttl_ms", 15_000)
        .map_err(|e| format!("config: {e}"))?
        .max(1_000);

    let url = merger.string("MAS_DATABASE_URL", "database.url", "");
    if url.is_empty() {
        return Err(
            "mas-worker requires database.url / MAS_DATABASE_URL: the durable \
                    pipeline has no in-memory mode, because a claim this process cannot \
                    lose on restart is not a claim"
                .to_owned(),
        );
    }
    // ── Worker plane pool ──────────────────────────────────────────────
    //
    // A worker claim is `SELECT … FROM tasks FOR UPDATE SKIP LOCKED`, and it
    // scans every tenant's queue on purpose: the tenant authority is the row
    // the lease is won on, which by definition is unknown until the query
    // returns. That makes the worker one of the planes that cannot be
    // expressed as an RLS policy, so it connects as `mas_worker` — a role
    // whose BYPASSRLS is confined to the durable execution tables (migration
    // 0017). It has no grant on credentials, api_keys or sessions, so a
    // compromised worker cannot read tenant secrets.
    //
    // `MAS_DATABASE_URL` is still required: it is the pool the migration
    // ledger is verified against and the pool any non-tenant work uses. There
    // is no fallback from the worker plane to the runtime pool — under
    // `mas_app` the claim would return zero rows and the worker would idle
    // while looking healthy.
    let runtime_url = DatabaseUrl::new(url).map_err(|e| format!("database url: {e}"))?;
    let mut runtime_config =
        PoolConfig::for_url(runtime_url, "mas-worker").map_err(|e| format!("database: {e}"))?;
    runtime_config.max_connections = merger
        .u64("MAS_DATABASE_MAX_CONNS", "database.max_connections", 16)
        .map_err(|e| format!("config: {e}"))?
        .try_into()
        .unwrap_or(16);
    let runtime_database = Database::connect(&runtime_config)
        .await
        .map_err(|e| format!("database: {e}"))?;
    let pool = runtime_database.pool().clone();

    // Migrations must be current. A worker that claims tasks against a stale
    // schema would fence and settle them against columns that no longer
    // exist, which is a corruption and not an outage.
    let runner = mas_persistence::migrations::MigrationRunner::new(pool.clone());
    runner
        .ensure_table()
        .await
        .map_err(|e| format!("migrations: {e}"))?;
    runner
        .verify()
        .await
        .map_err(|e| format!("migration ledger drift — apply scripts/migrate.sh up first: {e}"))?;

    // The worker plane is a *different database role*, so it needs its own
    // URL — the whole point is that the credential differs.
    let worker_url = merger.string("MAS_DATABASE_WORKER_URL", "database.worker_url", "");
    if worker_url.is_empty() {
        return Err(
            "mas-worker requires database.worker_url / MAS_DATABASE_WORKER_URL: the \
             worker plane claims tasks across tenants and therefore runs as the \
             mas_worker role (migrations/0017_plane_roles); falling back to the \
             runtime role would make every claim return zero rows"
                .to_owned(),
        );
    }
    let worker_url =
        DatabaseUrl::new(worker_url).map_err(|e| format!("worker database url: {e}"))?;
    let mut worker_config = PoolConfig::for_url(worker_url, "mas-worker-plane")
        .map_err(|e| format!("worker database: {e}"))?;
    worker_config.max_connections = merger
        .u64("MAS_DATABASE_MAX_CONNS", "database.max_connections", 16)
        .map_err(|e| format!("config: {e}"))?
        .try_into()
        .unwrap_or(16);
    let worker_database = Database::connect(&worker_config)
        .await
        .map_err(|e| format!("worker database: {e}"))?;
    let worker_pool = worker_database.pool().clone();

    let (broker, broker_lane) = select_broker(&merger, &worker_pool).await?;

    // The durable execution port is the worker's only authority. Attempts are
    // claimed, fenced and settled through it, so the worker holds nothing that
    // a restart would lose.
    // Claims, attempts and settlements all run on the worker plane. Once a
    // lease is won, the row itself names the tenant, and the worker derives
    // everything else from the database rather than from anything the broker
    // told it.
    let durable: Arc<dyn mas_application::durable::DurableExecutionPort> =
        Arc::new(PgDurableExecution::new(worker_pool));
    // ── The concurrency slot a run holds for its duration ──────────────
    //
    // Built over the *runtime* pool, not the worker-plane pool. The slot is a
    // `quota_reservations` row and it is written inside the tenant's own
    // row-level-security scope, which is only meaningful on a role that RLS
    // applies to. `mas_worker` bypasses RLS — that is what lets it claim tasks
    // across tenants — and pointing that role at the reservation table would
    // let one worker release another tenant's slot.
    //
    // Without this gate, `max_in_flight` is the only concurrency limit in the
    // system and it is a per-process constant: N workers would run N × 8 runs
    // for a tenant whose plan allows four.
    let control_plane = std::sync::Arc::new(
        mas_persistence::repositories::control_plane::adapters::build_postgres_control_plane(
            pool.clone(),
        ),
    );
    let attempts = Arc::new(
        AttemptCoordinator::new(
            durable,
            worker_name.clone(),
            max_in_flight,
            std::time::Duration::from_millis(lease_ttl_ms),
        )
        .map_err(|e| format!("attempt coordinator: {e}"))?
        .with_run_slots(Arc::new(ControlPlaneRunSlots::new(
            pool.clone(),
            control_plane,
            worker_name.clone(),
        ))),
    );

    let handlers_env = merger.string("MAS_WORKER_HANDLERS", "worker.handlers", "");
    let bound = handlers_env
        .split(',')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .collect::<Vec<_>>();

    // The AI bridge is resolved BEFORE the consumer starts, and a missing
    // configuration aborts the boot. Binding an echo handler here instead would
    // turn every execution into a silent no-op that reports success.
    let mut dispatcher = OperationDispatcher::new();
    if bound.contains(&"execution.run") {
        let settings =
            AgentBridgeSettings::from_merger(&merger).map_err(|e| format!("ai bridge: {e}"))?;
        eprintln!(
            "mas-worker: connecting ai bridge at {}",
            settings.endpoint()
        );
        let bridge = settings
            .connect()
            .await
            .map_err(|e| format!("ai bridge connect: {e}"))?;

        match bridge.health().await {
            Ok(()) => eprintln!("mas-worker: ai orchestrator healthy"),
            Err(error) => {
                return Err(format!(
                    "ai orchestrator health probe failed: {error}. \
                     refusing to start a worker whose AI runtime is unreachable"
                ))
            },
        }

        // The cancellation token the graceful-drain path owns: cancelling it
        // stops in-flight AI work instead of abandoning it mid-run.
        let cancellation = mas_orchestration::cancellation::CancellationToken::new();
        dispatcher = dispatcher.with_handler(
            "execution.run",
            Arc::new(AgentRunHandler::new(Arc::new(bridge), cancellation)),
        );
    }
    let ops: Vec<&str> = dispatcher.operations();
    eprintln!("mas-worker: handlers bound = {ops:?} (unknown ops ⇒ permanent-fail pipeline)");

    let consumer = TaskConsumer::new(
        broker,
        attempts,
        Arc::new(dispatcher),
        ConsumerConfig {
            consumer_name,
            poll_interval: std::time::Duration::from_millis(poll_ms),
            worker_name,
            max_in_flight,
            lease_ttl: std::time::Duration::from_millis(lease_ttl_ms),
            ..ConsumerConfig::default()
        },
    );
    let (handle, watch) = mas_worker::grace::channel();
    tokio::spawn(mas_worker::grace::watch_signals(handle));
    eprintln!("mas-worker: claiming mas.tasks.* via {broker_lane} lane");
    let outcome: ConsumerOutcome = consumer
        .run(watch)
        .await
        .map_err(|e| format!("consumer: {e}"))?;
    eprintln!(
        "drained: acked={} naks={} terms={} completed={} failed={} abandoned={}",
        outcome.acked,
        outcome.naks,
        outcome.terms,
        outcome.completed,
        outcome.failed,
        outcome.abandoned,
    );
    Ok(())
}

/// Broker selection: `broker.kind` picks the durable task lane.
///
/// * `pg` (default) — `PgTaskBroker` over the claims/outbox tables (SKIP
///   LOCKED); no extra infrastructure beyond PostgreSQL.
/// * `nats` (feature `nats`) — JetStream stream `broker.stream`,
///   explicit-ack pull consumers, DLQ lane via the adapter; requires
///   `MAS_NATS_URL` / `broker.nats_url`. The lifecycle store remains
///   PostgreSQL either way.
///
/// Invalid kinds fail fast at boot; the lane is otherwise invisible to the
/// consumer loop above (both satisfy `BrokerPort`).
#[derive(Debug)]
enum LaneDecision {
    Pg,
    #[cfg(feature = "nats")]
    Nats {
        url: String,
    },
}

/// Pure boot-fail rules, unit-testable without a server.
fn resolve_lane(kind: &str, nats_url: &str) -> Result<LaneDecision, String> {
    match kind {
        "pg" => Ok(LaneDecision::Pg),
        "nats" => {
            #[cfg(feature = "nats")]
            {
                if nats_url.is_empty() {
                    return Err(
                        "broker.kind=nats requires MAS_NATS_URL / broker.nats_url".to_owned()
                    );
                }
                Ok(LaneDecision::Nats {
                    url: nats_url.to_owned(),
                })
            }
            #[cfg(not(feature = "nats"))]
            {
                let _ = nats_url;
                Err(
                    "broker.kind=nats requires building mas-worker with `--features nats`"
                        .to_owned(),
                )
            }
        },
        other => Err(format!(
            "broker.kind must be one of pg|nats (got {other:?})"
        )),
    }
}

async fn select_broker(
    merger: &mas_common::config::Merger<'_>,
    pool: &sqlx::PgPool,
) -> Result<(Arc<dyn BrokerPort>, String), String> {
    use mas_persistence::repositories::broker::PgTaskBroker;

    let kind = merger.string("MAS_BROKER_KIND", "broker.kind", "pg");
    let nats_url = merger.string("MAS_NATS_URL", "broker.nats_url", "");
    match resolve_lane(&kind, &nats_url)? {
        LaneDecision::Pg => Ok((
            Arc::new(PgTaskBroker::new(pool.clone())) as Arc<dyn BrokerPort>,
            "pg (SKIP LOCKED)".to_owned(),
        )),
        #[cfg(feature = "nats")]
        LaneDecision::Nats { url } => {
            let stream = merger.string("MAS_BROKER_STREAM", "broker.stream", "MAS_TASKS");
            let ack_wait_ms = merger
                .u64("MAS_BROKER_ACK_WAIT_MS", "broker.ack_wait_ms", 30_000)
                .map_err(|e| format!("config: {e}"))?;
            let config = mas_messaging::nats::NatsBrokerConfig::for_url(url)
                .with_stream(stream)
                .with_ack_wait(std::time::Duration::from_millis(ack_wait_ms));
            let broker = mas_messaging::nats::NatsBroker::connect(&config)
                .await
                .map_err(|e| format!("nats broker: {e}"))?;
            // Boot-time liveness probe: refuse to start a worker that
            // cannot see its task lane.
            broker
                .ping()
                .await
                .map_err(|e| format!("nats health probe: {e}"))?;
            Ok((
                Arc::new(broker) as Arc<dyn BrokerPort>,
                "nats (JetStream)".to_owned(),
            ))
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_lane_defaults_to_pg_and_rejects_unknown_kinds() {
        assert!(matches!(resolve_lane("pg", ""), Ok(LaneDecision::Pg)));
        assert!(matches!(
            resolve_lane("pg", "anything"),
            Ok(LaneDecision::Pg)
        ));
        let err = resolve_lane("redis", "")
            .expect_err("unknown broker kinds must fail at boot, not silently degrade");
        assert!(err.contains("broker.kind"), "names the knob: {err}");
        assert!(err.contains("redis"), "echoes the invalid value: {err}");
    }

    #[cfg(feature = "nats")]
    #[test]
    fn resolve_lane_nats_requires_a_url() {
        let err =
            resolve_lane("nats", "").expect_err("nats lane without a URL must refuse to boot");
        assert!(err.contains("MAS_NATS_URL"), "names the missing key: {err}");
        let ok = resolve_lane("nats", "nats://127.0.0.1:4222")
            .expect("nats with URL selects the nats lane");
        match ok {
            LaneDecision::Nats { url } => assert_eq!(url, "nats://127.0.0.1:4222"),
            _ => panic!("expected Nats decision"),
        }
    }

    #[cfg(not(feature = "nats"))]
    #[test]
    fn resolve_lane_nats_without_feature_gives_build_guidance() {
        let err = resolve_lane("nats", "nats://127.0.0.1:4222")
            .expect_err("nats lane requires the nats feature build");
        assert!(
            err.contains("--features nats"),
            "message tells the operator what to rebuild with: {err}"
        );
    }

    // -- production composition ---------------------------------------------
    //
    // These read this file and the manifest as data. The alternative --
    // booting a worker against a real database to observe which handler it
    // registered -- is not available in a unit test, and the property being
    // guaranteed is structural anyway: which symbols the production path is
    // allowed to name.

    /// This file's own source, and the manifest that gates dev-only bridges.
    const MAIN_SOURCE: &str = include_str!("main.rs");
    const MANIFEST: &str = include_str!("../Cargo.toml");

    /// The production composition root: from its start to the test module.
    ///
    /// Stopping at `mod tests` is not cosmetic. `include_str!` embeds this
    /// whole file, tests included, and the tests below necessarily *name* the
    /// symbols they forbid. Scanning to the end of the file would match the
    /// assertions themselves and fail for no real reason.
    fn production_source() -> &'static str {
        let start = MAIN_SOURCE
            .find("async fn run(")
            .expect("run() is the one and only production composition root");
        let end = MAIN_SOURCE
            .find("\nmod tests")
            .expect("the test module ends the file's production region");
        &MAIN_SOURCE[start..end]
    }

    /// The `mas-execution` dependency line, with any comment removed.
    ///
    /// The manifest *discusses* `dev-bridges` in a comment explaining why it is
    /// absent, so a naive `contains` matches the explanation rather than the
    /// declaration. Only the dependency line itself is evidence.
    fn execution_dependency_line() -> String {
        MANIFEST
            .lines()
            .find(|line| line.trim_start().starts_with("mas-execution"))
            .map(|line| line.split("//").next().unwrap_or("").to_owned())
            .expect("the worker depends on mas-execution")
    }

    #[test]
    fn echo_handler_never_reaches_the_production_path() {
        // `EchoHandler` exists so the demo can prove queue plumbing works. If it
        // is ever referenced from the production path, every execution becomes a
        // no-op that reports success -- the single most dangerous outcome in
        // this system, because it is indistinguishable from working.
        let production = production_source();
        assert!(
            !production.contains("EchoHandler"),
            "EchoHandler must stay confined to the demo path; \
             a worker that echoes reports successes it never produced"
        );
    }

    #[test]
    fn the_production_path_never_names_the_in_memory_bridge() {
        let production = production_source();
        assert!(
            !production.contains("InMemoryAgentBridge"),
            "production must dispatch through the real gRPC bridge, \
             not an in-memory stand-in"
        );
    }

    #[test]
    fn the_production_path_binds_the_grpc_bridge() {
        // Positive counterpart to the two above: it is not enough that the
        // forbidden symbols are absent, the real adapter must actually be wired
        // in, or a future refactor could satisfy both by binding nothing.
        let production = production_source();
        assert!(
            production.contains("AgentBridgeSettings"),
            "the production path must resolve the bridge from settings"
        );
        assert!(
            production.contains("settings\n            .connect()")
                || production.contains("settings.connect()")
                || production.contains(".connect()"),
            "the production path must connect the bridge before consuming"
        );
    }

    #[test]
    fn the_worker_never_enables_the_dev_bridges_feature() {
        // The strongest guarantee available: `InMemoryAgentBridge` only exists
        // when `mas-execution/dev-bridges` is on. Keeping it out of this
        // manifest makes an in-memory production bridge a compile error rather
        // than a review-time question.
        assert!(
            !execution_dependency_line().contains("dev-bridges"),
            "the worker must depend on mas-execution without dev-bridges; \
             enabling it would make an in-memory bridge nameable in production"
        );
    }
}
