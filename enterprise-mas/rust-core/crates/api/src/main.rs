//! PURPOSE: `mas-api` process bootstrap: HTTP + gRPC servers over the
//! application layer with graceful shutdown.
//!
//! Owning layer: `mas-api` (the process edge).
//!
//! Production responsibility: compose the real object graph once, bind the
//! listeners, install health/readiness probes, and shut down without dropping
//! in-flight requests. The `--dev-inmemory` lane exists for local bring-up and
//! is unreachable unless the flag is passed explicitly — production must never
//! start stateless by accident.
//!
//! It must NOT:
//!   • contain request handling, business rules or SQL;
//!   • construct stores directly — composition comes from `state.rs`;
//!   • treat a missing production dependency as "start with a fallback".

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

    let (
        tenancy,
        agents,
        workflows,
        executions,
        schedules,
        control_plane,
        audit,
        control_plane_stores,
        verifier,
        scope_resolver,
        runtime_pool,
        internal_secret,
        health,
    ) = if dev_inmemory {
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
        scope_resolver,
        tenancy,
        agents,
        workflows,
        executions,
        schedules,
        control_plane,
        audit,
        control_plane_stores,
        health,
        // One transaction per request, with row-level security armed inside
        // it. `None` in the in-memory lane only.
        runtime_pool,
        // Internal-call authentication. `None` ⇒ every internal route
        // refuses, rather than trusting the network.
        internal_secret,
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
    // The canonical control plane, and the Postgres control-plane stores
    // (`None` in the in-memory lane, which has no database).
    Arc<mas_application::control_plane::ControlPlane>,
    Arc<dyn mas_application::audit::AuditSinkPort>,
    Option<Arc<mas_persistence::repositories::control_plane::ControlPlaneStores>>,
    Arc<dyn mas_api::auth::TokenVerifierPort>,
    Option<mas_api::scope_resolution::RequestScopeResolver>,
    // The runtime PostgreSQL pool (`None` in the in-memory lane).
    Option<sqlx::PgPool>,
    // The shared secret internal services authenticate with (`None` means
    // every internal route refuses; see `handlers::internal`).
    Option<mas_security::SecretString>,
    Arc<HealthRegistry>,
);

/// Reads the shared secret used to authenticate internal service calls.
///
/// The value is a credential, so it comes only from configuration or the
/// environment — never a command-line argument, which would place it in the
/// process table and in shell history. An absent secret is not an error at
/// boot: it is reported, and every internal route then refuses, which is the
/// fail-closed behaviour.
fn load_internal_secret(
    merger: &mas_common::config::Merger<'_>,
) -> Result<Option<mas_security::SecretString>, String> {
    let raw = merger.string(
        "MAS_INTERNAL_SERVICE_SECRET",
        "security.internal_service_secret",
        "",
    );
    if raw.trim().is_empty() {
        eprintln!(
            "mas-api: MAS_INTERNAL_SERVICE_SECRET is not set — \
             /v1/internal/* will reject every request"
        );
        return Ok(None);
    }
    Ok(Some(mas_security::SecretString::new(raw)))
}

fn compose_inmemory(merger: &mas_common::config::Merger<'_>) -> Result<ServiceSet, String> {
    let store = Arc::new(InMemoryServices::new());
    let audit = Arc::new(InMemoryAuditSink::new());
    // The control plane is built first and shared: tenant provisioning writes
    // the subscription, the gate reads it, and they must be one store.
    let control_plane = Arc::new(mas_application::control_plane::build_in_memory_control_plane());
    let tenancy = Arc::new(TenancyService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(audit.clone()),
        Box::new(control_plane.entitlement_store()),
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
        Box::new(audit.clone()),
        Arc::new(mas_application::durable_memory::InMemoryDurableExecution::new()),
        // The same control plane the tenancy service provisions through. One
        // process, one gate: a second instance would not see the subscriptions
        // the first one's provisioning wrote.
        Arc::clone(&control_plane),
    ));
    let schedules = Arc::new(ScheduleService::new(
        Box::new(store),
        Box::new(audit.clone()) as Box<dyn mas_application::audit::AuditSinkPort>,
    ));
    let _ = merger;

    let verifier: Arc<dyn mas_api::auth::TokenVerifierPort> = Arc::new(dev_verifier()?);
    // The in-memory lane still resolves scope from identity — over an
    // in-memory directory rather than PostgreSQL. Resolving from a header here
    // would make the development lane behave differently from production in
    // exactly the way that hides bugs until deploy.
    let scope_resolver = Some(mas_api::scope_resolution::RequestScopeResolver::new(
        mas_tenancy::TenantResolver::new(
            Arc::new(mas_tenancy::InMemoryDirectory::new()),
            Arc::new(mas_tenancy::InMemoryMemberships::new()),
        ),
    ));
    Ok((
        tenancy,
        agents,
        workflows,
        executions,
        schedules,
        // The ExecutionService owns one handle; this is a second handle to
        // the same control plane, not a second control plane.
        control_plane,
        audit.clone() as Arc<dyn mas_application::audit::AuditSinkPort>,
        None,
        verifier,
        scope_resolver,
        None,
        load_internal_secret(merger)?,
        Arc::new(HealthRegistry::new()),
    ))
}

/// Production composition: durable everything, dev affordances rejected.
async fn compose_production(merger: &mas_common::config::Merger<'_>) -> Result<ServiceSet, String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};
    use mas_persistence::repositories::api_keys::ApiKeyStore;
    use mas_persistence::repositories::audit::PostgresAuditLog;
    use mas_persistence::repositories::services::PgServices;
    use mas_persistence::repositories::PgDurableExecution;

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

    // ── Audit plane ────────────────────────────────────────────────────
    //
    // Audit events are appended on their own pool, as `mas_audit` — a role
    // with INSERT on `audit_events` and nothing else (migration 0017). Two
    // reasons, both load-bearing:
    //
    //   1. A denial must be recordable for a tenant the caller was never
    //      scoped to. That is exactly what row-level security forbids on the
    //      runtime role, and rightly so: the audit row is the evidence the
    //      denial happened, so it cannot be subject to the check that caused
    //      it.
    //   2. An audit event must survive the rollback of the mutation it
    //      describes. Writing it inside the request transaction would erase
    //      the record of an attempt that failed.
    //
    // There is no fallback to the runtime pool: under `mas_app` the INSERT
    // would be refused by the table's own policy, and dropping audit events
    // silently is not a failure mode worth having.
    let audit_url = merger.string("MAS_DATABASE_AUDIT_URL", "database.audit_url", "");
    if audit_url.is_empty() {
        return Err("production mode requires an audit database URL \
             (config database.audit_url or MAS_DATABASE_AUDIT_URL) \
             for the mas_audit role; see migrations/0017_plane_roles"
            .to_owned());
    }
    let audit_url_holder =
        DatabaseUrl::new(audit_url).map_err(|err| format!("audit database url: {err}"))?;
    let mut audit_config = PoolConfig::for_url(audit_url_holder, "mas-api-audit")
        .map_err(|err| format!("audit database config: {err}"))?;
    audit_config.max_connections = u32::try_from(max_connections).unwrap_or(32).min(8);
    let audit_config = audit_config
        .validated()
        .map_err(|err| format!("audit database config: {err}"))?;
    let audit_database = Database::connect(&audit_config)
        .await
        .map_err(|err| format!("audit database: {err}"))?;
    let audit_pool = audit_database.pool().clone();

    // ── Durable ports (each Box owns a cheap pool handle clone) ───────
    // The control plane runs on the same pool as the durable port, so a quota
    // reservation and the execution it authorises commit against one
    // consistent view of the tenant's counters. It is built before the tenancy
    // service because provisioning writes through it.
    let control_plane = Arc::new(
        mas_persistence::repositories::control_plane::build_postgres_control_plane(pool.clone()),
    );
    let tenancy = Arc::new(TenancyService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
        Box::new(control_plane.entitlement_store()),
    ));
    let agents = Arc::new(AgentService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));
    let workflows = Arc::new(WorkflowService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));
    // The durable port is Postgres: submission, claiming, settlement and
    // cancellation all run as one transaction each against the same pool the
    // rest of the service uses.
    // One audit log for the whole lane, shared with the control-plane
    // handlers: a request's audit trail is one sequence, not one sequence per
    // service that happened to write a line.
    let audit_log = Arc::new(PostgresAuditLog::new(audit_pool.clone()));
    let audit_sink: Arc<dyn mas_application::audit::AuditSinkPort> = audit_log.clone();
    let executions = Arc::new(ExecutionService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(audit_log.clone()),
        Arc::new(PgDurableExecution::new(pool.clone())),
        Arc::clone(&control_plane),
    ));
    let schedules = Arc::new(ScheduleService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));

    let verifier: Arc<dyn mas_api::auth::TokenVerifierPort> = Arc::new(
        mas_api::prod_auth::PostgresApiKeyVerifier::new(ApiKeyStore::new(pool.clone())),
    );

    // ── Tenancy directory (the one unscoped read path) ────────────────
    //
    // Resolving which tenant a principal may reach has to happen before a
    // scope exists, and every directory table is RLS-protected. So the
    // directory runs on its own pool, as `mas_directory` — a role with SELECT
    // on four catalog tables and nothing else. There is deliberately no
    // fallback to the runtime pool: under `mas_app` those reads would return
    // zero rows, and "tenant not found" is a far worse failure than a loud
    // configuration error.
    let directory_url = merger.string("MAS_DATABASE_DIRECTORY_URL", "database.directory_url", "");
    if directory_url.is_empty() {
        return Err("production mode requires a directory database URL \
             (config database.directory_url or MAS_DATABASE_DIRECTORY_URL) \
             for the mas_directory role; see migrations/0015_directory_role"
            .to_owned());
    }
    let directory_url_holder =
        DatabaseUrl::new(directory_url).map_err(|err| format!("directory database url: {err}"))?;
    let mut directory_config = PoolConfig::for_url(directory_url_holder, "mas-api-directory")
        .map_err(|err| format!("directory database config: {err}"))?;
    directory_config.max_connections = u32::try_from(max_connections).unwrap_or(32).min(8);
    let directory_config = directory_config
        .validated()
        .map_err(|err| format!("directory database config: {err}"))?;
    let directory_database = Database::connect(&directory_config)
        .await
        .map_err(|err| format!("directory database: {err}"))?;
    let directory_pool = directory_database.pool().clone();
    let scope_resolver = Some(mas_api::scope_resolution::RequestScopeResolver::new(
        mas_tenancy::TenantResolver::new(
            Arc::new(mas_persistence::directory::PgTenantDirectory::new(
                directory_pool.clone(),
            )),
            Arc::new(mas_persistence::directory::PgMembershipDirectory::new(
                directory_pool,
            )),
        ),
    ));

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
        tenancy,
        agents,
        workflows,
        executions,
        schedules,
        // A second handle to the control plane the ExecutionService owns —
        // not a second control plane.
        control_plane,
        audit_sink,
        Some(Arc::new(
            mas_persistence::repositories::control_plane::ControlPlaneStores::new(pool.clone()),
        )),
        verifier,
        scope_resolver,
        Some(pool.clone()),
        load_internal_secret(merger)?,
        health,
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

/// Builds the development verifier.
///
/// A dev token now declares its tenancy explicitly — `token:subject:kind` for
/// an unbound identity, and the scope resolver then requires such a principal
/// to name a tenant. A dev lane that granted "any tenant" would be a dev lane
/// that cannot reproduce a production authorization bug.
fn dev_verifier() -> Result<StaticTokenVerifier, String> {
    use mas_security::{ScopeSet, SecurityPrincipal};

    let layered = mas_common::config::load_layered().map_err(|err| format!("config: {err}"))?;
    let merger = mas_common::config::Merger::new(&layered);
    let mut verifier = StaticTokenVerifier::new().with_token(
        "dev-token",
        SecurityPrincipal::new(
            uuid::Uuid::now_v7().to_string(),
            PrincipalKind::User,
            None,
            ScopeSet::all(),
        ),
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
                "service" => PrincipalKind::ServiceAccount,
                "api_key" => PrincipalKind::ApiKey,
                other => return Err(format!("unknown principal kind '{other}'")),
            };
            verifier = verifier.with_token(
                token,
                SecurityPrincipal::new(subject, kind, None, ScopeSet::all()),
            );
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
