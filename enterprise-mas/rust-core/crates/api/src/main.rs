//! `mas-api` process bootstrap: HTTP + gRPC servers over the application
//! layer with graceful shutdown.
//!
//! Two bring-up modes:
//! * `--dev-inmemory` — everything over `InMemoryServices` (local bring-up,
//!   NO persistence). Requires explicit flag so production never starts up
//!   stateless by accident.
//! * (default) — full production composition: PostgreSQL-backed stores via
//!   `mas_persistence::PgServices`, `engine` audit on `PostgresAuditLog`,
//!   bearer auth via `PostgresApiKeyVerifier`, migration verification at
//!   boot (deterministic fail-fast on drift; `MAS_DATABASE_MIGRATE_ON_BOOT`
//!   enables the deliberate apply path), and the DB dependency probe on the
//!   readiness surface.
//!
//! Environment:
//! * `MAS_API_HTTP_ADDR` (default `0.0.0.0:8080`)
//! * `MAS_API_GRPC_ADDR` (default `0.0.0.0:50051`)
//! * `MAS_DEV_TOKENS` (`token:subject:kind,...`; only with `--dev-inmemory`)

use std::net::SocketAddr;
use std::sync::Arc;

use mas_api::auth::{PrincipalKind, StaticTokenVerifier};
use mas_api::server::ServerConfig;
use mas_api::state::AppState;
use mas_application::agent_service::AgentService;
use mas_application::audit::InMemoryAuditSink;
use mas_application::execution_service::ExecutionService;
use mas_application::schedule_service::ScheduleService;
use mas_application::stores::InMemoryServices;
use mas_application::tenancy_service::TenancyService;
use mas_application::workflow_service::WorkflowService;
use mas_observability::health::HealthRegistry;

#[tokio::main]
async fn main() {
    std::process::exit(match run().await {
        Ok(0) => 0,
        Ok(_) => 0,
        Err(err) => {
            eprintln!("mas-api: {err}");
            2
        },
    });
}

async fn run() -> Result<i32, String> {
    let dev_inmemory = std::env::args().skip(1).any(|arg| arg == "--dev-inmemory");

    let layered = mas_common::config::load_layered().map_err(|err| format!("config: {err}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let http_addr = parse_addr_value(
        merger.string(
            "MAS_API_HTTP_ADDR",
            "server.http_addr",
            &ServerConfig::default().http_addr.to_string(),
        ),
        "MAS_API_HTTP_ADDR",
    )?;
    let grpc_addr = parse_addr_value(
        merger.string(
            "MAS_API_GRPC_ADDR",
            "server.grpc_addr",
            &ServerConfig::default().grpc_addr.to_string(),
        ),
        "MAS_API_GRPC_ADDR",
    )?;
    let _ = (&http_addr, &grpc_addr); // listener bind-up ships with compose path below

    // Production guardrails (dev flags are exactly the ones documented in
    // config/production.toml's header).
    if environment_string(&merger) == "production" {
        enforce_production_guardrails(&merger.string("MAS_DEV_TOKENS", "server.dev_tokens", ""))?;
    }

    let (tenancy, agents, workflows, executions, schedules, verifier, health) = if dev_inmemory {
        compose_inmemory(&merger)?
    } else {
        compose_production(&merger).await?
    };

    // ── Production-mode guardrails: dev affordances are forbidden even
    //    outside `environment == "production"` — non-dev wiring is always
    //    durable and must never accept static tokens. ──────────────────
    if !dev_inmemory {
        enforce_production_guardrails(&merger.string("MAS_DEV_TOKENS", "server.dev_tokens", ""))?;
    }

    let state = AppState::new(
        "mas-api",
        env!("CARGO_PKG_VERSION"),
        verifier,
        tenancy,
        agents,
        workflows,
        executions,
        schedules,
        health,
    );

    let (http_addr, grpc_addr) = (
        parse_addr_value(
            merger.string(
                "MAS_API_HTTP_ADDR",
                "server.http_addr",
                &ServerConfig::default().http_addr.to_string(),
            ),
            "MAS_API_HTTP_ADDR",
        )?,
        parse_addr_value(
            merger.string(
                "MAS_API_GRPC_ADDR",
                "server.grpc_addr",
                &ServerConfig::default().grpc_addr.to_string(),
            ),
            "MAS_API_GRPC_ADDR",
        )?,
    );

    eprintln!(
        "mas-api starting mode={} http={http_addr} grpc={grpc_addr}",
        if dev_inmemory {
            "--dev-inmemory"
        } else {
            "production"
        }
    );
    mas_api::server::serve(
        state,
        ServerConfig {
            http_addr,
            grpc_addr,
        },
    )
    .await
    .map_err(|err| err.to_string())?;
    Ok(0)
}

type ServiceSet = (
    Arc<TenancyService>,
    Arc<AgentService>,
    Arc<WorkflowService>,
    Arc<ExecutionService>,
    Arc<ScheduleService>,
    Arc<dyn mas_api::auth::TokenVerifierPort>,
    Arc<HealthRegistry>,
);

fn compose_inmemory(merger: &mas_common::config::Merger<'_>) -> Result<ServiceSet, String> {
    let store = Arc::new(InMemoryServices::new());
    let audit = Arc::new(InMemoryAuditSink::new());
    let tenancy = Arc::new(TenancyService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
    ));
    let agents = Arc::new(AgentService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
    ));
    let workflows = Arc::new(WorkflowService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
    ));
    let executions = Arc::new(ExecutionService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
    ));
    let schedules = Arc::new(ScheduleService::new(Box::new(store), Box::new(audit)));
    let _ = merger;

    let verifier: Arc<dyn mas_api::auth::TokenVerifierPort> = Arc::new(dev_verifier()?);
    Ok((
        tenancy,
        agents,
        workflows,
        executions,
        schedules,
        verifier,
        Arc::new(HealthRegistry::new()),
    ))
}

/// Production composition: durable everything, dev affordances rejected.
async fn compose_production(merger: &mas_common::config::Merger<'_>) -> Result<ServiceSet, String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
    use mas_persistence::repositories::api_keys::ApiKeyStore;
    use mas_persistence::repositories::audit::PostgresAuditLog;
    use mas_persistence::repositories::services::PgServices;

    let url = merger.string("MAS_DATABASE_URL", "database.url", "");
    if url.is_empty() {
        return Err(
            "production mode requires a database URL (config database.url or MAS_DATABASE_URL); \
             local bring-up uses --dev-inmemory"
                .to_owned(),
        );
    }
    let max_connections = merger
        .u64("MAS_DATABASE_MAX_CONNS", "database.max_connections", 32)
        .map_err(|err| format!("config: {err}"))?;
    let url_holder = DatabaseUrl::new(url).map_err(|err| format!("database url: {err}"))?;
    let mut config = PoolConfig::for_url(url_holder, "mas-api")
        .map_err(|err| format!("database config: {err}"))?;
    config.max_connections = u32::try_from(max_connections).unwrap_or(32);
    let config = config
        .validated()
        .map_err(|err| format!("database config: {err}"))?;
    let database = Database::connect(&config)
        .await
        .map_err(|err| format!("database: {err}"))?;

    // ── Migration ledger verification (migrate-on-boot is opt-in) ─────
    let pool = database.pool().clone();
    let runner = mas_persistence::migrations::MigrationRunner::new(pool.clone());
    runner
        .ensure_table()
        .await
        .map_err(|err| format!("migrations: {err}"))?;
    let migrate_on_boot = merger
        .bool(
            "MAS_DATABASE_MIGRATE_ON_BOOT",
            "database.migrate_on_boot",
            false,
        )
        .map_err(|err| format!("config: {err}"))?;
    if migrate_on_boot {
        let applied = runner
            .migrate()
            .await
            .map_err(|err| format!("migrate: {err}"))?;
        eprintln!("mas-api: {} migration(s) applied on boot", applied.len());
    } else {
        runner.verify().await.map_err(|err| {
            format!(
                "migration ledger drift — apply migrations first (scripts/migrate.sh up): {err}"
            )
        })?;
    }

    // ── Durable ports (each Box owns a cheap pool handle clone) ───────
    let tenancy = Arc::new(TenancyService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(pool.clone())),
    ));
    let agents = Arc::new(AgentService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(pool.clone())),
    ));
    let workflows = Arc::new(WorkflowService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(pool.clone())),
    ));
    let executions = Arc::new(ExecutionService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(pool.clone())),
    ));
    let schedules = Arc::new(ScheduleService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(pool.clone())),
    ));

    let verifier: Arc<dyn mas_api::auth::TokenVerifierPort> = Arc::new(
        mas_api::prod_auth::PostgresApiKeyVerifier::new(ApiKeyStore::new(pool.clone())),
    );

    // ── Dependency probe over the same pool ───────────────────────────
    let health = Arc::new(HealthRegistry::new());
    health
        .register(
            mas_observability::health::ProbeKind::Readiness,
            "postgres",
            PgHealthCheck {
                database: database.clone(),
            },
        )
        .map_err(|err| format!("health: {err}"))?;

    Ok((
        tenancy, agents, workflows, executions, schedules, verifier, health,
    ))
}

/// Readiness adapter over the pool's `SELECT 1` probe.
struct PgHealthCheck {
    database: mas_persistence::pool::Database,
}

#[async_trait::async_trait]
impl mas_observability::health::HealthCheckPort for PgHealthCheck {
    async fn check(
        &self,
        at: &mas_common::timestamps::Timestamp,
    ) -> mas_observability::health::ComponentHealth {
        let probe = self.database.probe().await;
        if probe.healthy {
            mas_observability::health::ComponentHealth::up("postgres", at)
                .unwrap_or_else(|_| unreachable!("static name"))
        } else {
            mas_observability::health::ComponentHealth::down(
                "postgres",
                at,
                probe.message.as_deref().unwrap_or("database probe failed"),
            )
            .unwrap_or_else(|_| unreachable!("static name"))
        }
    }
}

/// The production-mode dev-token guardrail, isolated as a pure rule so the
/// binary's test suite pins the exact failure contract: any non-empty
/// `server.dev_tokens` value is refused whenever the process is not
/// explicitly running with `--dev-inmemory`.
fn enforce_production_guardrails(dev_tokens_raw: &str) -> Result<(), String> {
    mas_common::config::refuse_if(
        !dev_tokens_raw.trim().is_empty(),
        "production refuses static dev tokens (config server.dev_tokens must stay empty)",
    )
    .map_err(|err| format!("config guardrail: {err}"))
}

fn parse_addr_value(raw: String, source: &str) -> Result<SocketAddr, String> {
    raw.parse()
        .map_err(|err| format!("{source} invalid ('{raw}'): {err}"))
}

/// Effective `environment` key (MAS_ENV wins via the merger's env layer).
fn environment_string(merger: &mas_common::config::Merger<'_>) -> String {
    merger
        .string("MAS_ENV", "environment", "development")
        .to_ascii_lowercase()
}

fn dev_verifier() -> Result<StaticTokenVerifier, String> {
    let layered = mas_common::config::load_layered().map_err(|err| format!("config: {err}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let mut verifier = StaticTokenVerifier::new().with_token(
        "dev-token",
        uuid::Uuid::now_v7().to_string().as_str(),
        PrincipalKind::User,
    );
    let table = merger.string("MAS_DEV_TOKENS", "server.dev_tokens", "");
    if !table.is_empty() {
        for entry in table.split(',').filter(|e| !e.trim().is_empty()) {
            let mut parts = entry.splitn(3, ':');
            let (Some(token), Some(subject), Some(kind)) =
                (parts.next(), parts.next(), parts.next())
            else {
                return Err(format!(
                    "MAS_DEV_TOKENS entry '{entry}' malformed (token:subject:kind)"
                ));
            };
            let kind = match kind {
                "user" => PrincipalKind::User,
                "service" => PrincipalKind::Service,
                "api_key" => PrincipalKind::ApiKey,
                other => return Err(format!("unknown principal kind '{other}'")),
            };
            verifier = verifier.with_token(token, subject, kind);
        }
    }
    Ok(verifier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_guardrail_accepts_empty_dev_tokens() {
        assert!(enforce_production_guardrails("").is_ok());
        assert!(enforce_production_guardrails("   ").is_ok());
    }

    #[test]
    fn production_guardrail_refuses_configured_dev_tokens() {
        let err = enforce_production_guardrails("dev-token:sub-a:user")
            .expect_err("static tokens must not survive production mode");
        assert!(err.contains("dev tokens"), "message names the cause: {err}");
        assert!(err.contains("guardrail"), "message names the gate: {err}");
    }
}
