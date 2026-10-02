//! `mas-worker` process bootstrap.
//!
//! Bring-up modes:
//! * `--dev-inmemory` — in-process `InMemoryBroker` + in-memory leases +
//!   in-memory task rows + an echo handler over a seeded demo task. Local
//!   smoke run, killed by anything else (never production).
//! * (default) — production: durable handler registry + lifecycle store in
//!   PostgreSQL; the task **broker lane** selects at boot via `broker.kind`:
//!   `pg` (default, SKIP-LOCKED claims over the outbox tables) or `nats`
//!   (feature `nats`, JetStream stream + explicit-ack pull consumers over
//!   `MAS_NATS_URL` / `broker.nats_url`). Consumer provisioning, health
//!   probe, and fast-fail on missing config all happen before the loop
//!   starts.
//!
//! Environment: nothing required in dev mode; production needs
//! `MAS_DATABASE_URL` (plus `MAS_NATS_URL` for the nats lane).

use std::sync::Arc;

use mas_common::ids::{AgentId, OrganizationId, ProjectId, TenantId};
use mas_domain::Task;
use mas_messaging::broker::{BrokerPort, InMemoryBroker, PublishRequest};
use mas_messaging::codec::{CodecConfig, JsonEventCodec};
use mas_messaging::headers::HeaderSet;
use mas_messaging::subjects::Subject;
use mas_scheduling::lease::InMemoryLeaseStore;
use mas_worker::consumer::{ConsumerConfig, TaskConsumer};
use mas_worker::handler::OperationDispatcher;
use mas_worker::lifecycle::InMemoryTaskStore;
use mas_worker::ConsumerOutcome;

/// Minimal handler for the bring-up run: echoes back its input.
#[derive(Debug)]
struct EchoHandler;

#[async_trait::async_trait]
impl mas_worker::TaskHandlerPort for EchoHandler {
    async fn execute(
        &self,
        message: &mas_contracts::task::TaskQueueMessage,
    ) -> mas_common::result::Result<serde_json::Value> {
        eprintln!(
            "demo task {} ran (operation {})
",
            message.task_id, message.operation
        );
        Ok(serde_json::json!({"echo": message.input.clone()}))
    }
}

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

async fn run() -> Result<(), String> {
    let dev_inmemory = std::env::args().skip(1).any(|arg| arg == "--dev-inmemory");
    if !dev_inmemory {
        return run_production().await;
    }

    let layered = mas_common::config::load_layered().map_err(|e| format!("config: {e}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let worker_name = merger.string("MAS_WORKER_NAME", "worker.worker_name", "worker-1");
    let poll_ms = merger
        .u64("MAS_WORKER_POLL_MS", "worker.poll_interval_ms", 100)
        .map_err(|e| format!("config: {e}"))?;
    let consumer_name = merger.string("MAS_WORKER_CONSUMER", "worker.consumer_name", "tasks-main");

    let broker_inner = InMemoryBroker::new();
    broker_inner.set_available(true);
    let broker = Arc::new(broker_inner);
    let leases = Arc::new(InMemoryLeaseStore::new());
    let lifecycle = Arc::new(InMemoryTaskStore::new());
    let dispatcher =
        OperationDispatcher::new().with_handler("execution.run", Arc::new(EchoHandler));

    // Seed one demo task envelope (the publisher side of the contract).
    let seed = demo_task();
    let mut row = seed.clone();
    row.queue().map_err(|e| format!("seed queue: {e}"))?;
    lifecycle.seed(row);
    let message = mas_contracts::task::TaskQueueMessage::staged(
        seed.id,
        seed.tenant_id,
        seed.organization_id,
        seed.project_id,
        seed.operation.clone(),
        seed.input.clone(),
        seed.execution_id,
        seed.agent_id,
        seed.workflow_id,
        seed.idempotency_key.clone(),
        seed.priority,
        1,
        seed.max_attempts,
        seed.deadline,
        "dev-seed-1",
    )
    .map_err(|e| format!("seed message: {e}"))?;
    let codec = JsonEventCodec::new(CodecConfig::default());
    let frame = codec
        .encode_value(&message)
        .map_err(|e| format!("frame: {e}"))?;
    broker
        .publish(PublishRequest {
            subject: Subject::parse("mas.tasks.execution.run")
                .map_err(|e| format!("subject: {e}"))?,
            headers: HeaderSet::default(),
            payload: frame,
            msg_id: Some("dev-seed-1".to_owned()),
        })
        .await
        .map_err(|e| format!("publish: {e}"))?;

    let consumer = TaskConsumer::new(
        broker,
        leases,
        lifecycle,
        Arc::new(dispatcher),
        ConsumerConfig {
            consumer_name,
            poll_interval: std::time::Duration::from_millis(poll_ms),
            worker_name,
            ..ConsumerConfig::default()
        },
    );
    let (handle, watch) = mas_worker::grace::channel();
    tokio::spawn(mas_worker::grace::watch_signals(handle));

    eprintln!("mas-worker dev loop: consuming mas.tasks.> (ctrl-c to drain)");
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
    let _ = run_demo_hint();
    Ok(())
}

fn demo_task() -> Task {
    Task::new(
        TenantId::new(),
        OrganizationId::new(),
        ProjectId::new(),
        Some(AgentId::new()),
        None,
        "execution.run",
        serde_json::json!({"seed": true}),
        mas_common::enums::TaskPriority::Normal,
        "dev-task-1",
        "mas-worker-demo",
    )
    .expect("demo task")
}

fn run_demo_hint() -> &'static str {
    "demo ran against the in-memory broker; no state persists"
}

/// Production consumption: durable pg lane + durable lifecycle; handler
/// registry driven from MAS_WORKER_HANDLERS ("execution.run" binds the
/// echo handler ONLY for lab smoke — a production runtime binding phase
/// replaces it; absent operations classify permanent → failed+republish).
async fn run_production() -> Result<(), String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
    use mas_persistence::repositories::tasks::TaskStore;
    use mas_worker::lifecycle::TaskLifecyclePort;

    let layered = mas_common::config::load_layered().map_err(|e| format!("config: {e}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let worker_name = merger.string("MAS_WORKER_NAME", "worker.worker_name", "worker-1");
    let poll_ms = merger
        .u64("MAS_WORKER_POLL_MS", "worker.poll_interval_ms", 100)
        .map_err(|e| format!("config: {e}"))?;
    let consumer_name = merger.string("MAS_WORKER_CONSUMER", "worker.consumer_name", "tasks-main");

    let url = merger.string("MAS_DATABASE_URL", "database.url", "");
    if url.is_empty() {
        return Err("mas-worker production requires database.url / MAS_DATABASE_URL".to_owned());
    }
    let url = DatabaseUrl::new(url).map_err(|e| format!("database url: {e}"))?;
    let mut config =
        PoolConfig::for_url(url, "mas-worker").map_err(|e| format!("database: {e}"))?;
    config.max_connections = merger
        .u64("MAS_DATABASE_MAX_CONNS", "database.max_connections", 16)
        .map_err(|e| format!("config: {e}"))?
        .try_into()
        .unwrap_or(16);
    let database = Database::connect(&config)
        .await
        .map_err(|e| format!("database: {e}"))?;
    let pool = database.pool().clone();

    let (broker, broker_lane) = select_broker(&merger, &pool).await?;
    let lifecycle: Arc<dyn TaskLifecyclePort> = Arc::new(TaskStore::new(pool));
    let leases: Arc<dyn mas_scheduling::lease::LeaseStorePort> =
        Arc::new(InMemoryLeaseStore::new());

    let handlers_env = merger.string("MAS_WORKER_HANDLERS", "worker.handlers", "");
    let bound = handlers_env
        .split(',')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .collect::<Vec<_>>();
    let mut dispatcher = OperationDispatcher::new();
    if bound.contains(&"execution.run") {
        dispatcher = dispatcher.with_handler("execution.run", Arc::new(EchoHandler));
    }
    let ops: Vec<&str> = dispatcher.operations();
    eprintln!(
        "mas-worker production: handlers bound = {ops:?} (unknown ops ⇒ permanent-fail pipeline)"
    );

    let consumer = TaskConsumer::new(
        broker,
        leases,
        lifecycle,
        Arc::new(dispatcher),
        ConsumerConfig {
            consumer_name,
            poll_interval: std::time::Duration::from_millis(poll_ms),
            worker_name,
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
}
