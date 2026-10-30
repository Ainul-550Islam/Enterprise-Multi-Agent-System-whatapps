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
use mas_application::analytics_service::AnalyticsService;
use mas_application::api_key_service::ApiKeyApplicationService;
use mas_application::approval_service::ApprovalApplicationService;
use mas_application::audit::InMemoryAuditSink;
use mas_application::data_hub_service::{DataHubApplicationService, DataHubLimits};
use mas_application::execution_service::ExecutionService;
use mas_application::notification_service::NotificationService;
use mas_application::oauth_authorization_service::OAuthAuthorizationService;
use mas_application::rollback_service::RollbackApplicationService;
use mas_application::schedule_service::ScheduleService;
use mas_application::search_service::SearchService;
use mas_application::stores::InMemoryServices;
use mas_application::tenancy_service::TenancyService;
use mas_application::workflow_service::WorkflowService;
use mas_observability::health::HealthRegistry;

mod inbound_webhook_composition;
mod integration_composition;
mod oauth_composition;

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
        rollbacks,
        executions,
        schedules,
        notifications,
        inbound_webhook_endpoints,
        inbound_webhook_ingress,
        integration_plane,
        oauth_authorization,
        oauth_callbacks,
        oauth_refresh_worker,
        oauth_cleanup_worker,
        oauth_materialization_reconciler,
        analytics,
        api_keys,
        approvals,
        data_hub,
        search,
        audit_query,
        policies,
        usage,
        tools,
        connector_admin,
        webhook_admin,
        stream_hub,
        assistant,
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
    )
    .with_rollbacks(rollbacks)
    .with_notifications(notifications)
    .with_analytics(analytics)
    .with_api_keys(api_keys)
    .with_approvals(approvals)
    .with_data_hub(data_hub)
    .with_search(search);
    let state = install_governance_services(state, audit_query, policies, usage);
    let state = install_tooling_services(
        state,
        tools,
        connector_admin,
        webhook_admin,
        stream_hub,
        assistant,
    );
    let state = match (inbound_webhook_endpoints, inbound_webhook_ingress) {
        (Some(endpoints), Some(ingress)) => state.with_inbound_webhooks(endpoints, ingress),
        (None, None) if dev_inmemory => state,
        (None, None) => {
            return Err(
                "production composition omitted the canonical inbound webhook plane; refusing to start"
                    .to_owned(),
            );
        },
        _ => {
            return Err(
                "inbound webhook endpoint and ingress composition must be installed together"
                    .to_owned(),
            );
        },
    };

    let state = match integration_plane {
        Some(integration_plane) => {
            spawn_outbound_webhook_worker(
                integration_plane.outbound_webhook_worker,
                integration_plane.outbound_webhook_poll,
            );
            spawn_notification_delivery_worker(
                integration_plane.notification_delivery_worker,
                integration_plane.notification_delivery_poll,
            );
            state.with_integrations(integration_plane.service)
        },
        None if dev_inmemory => state,
        None => {
            return Err(
                "production composition omitted the canonical connector/outbound-webhook service or durable delivery workers; refusing to start"
                    .to_owned(),
            );
        },
    };

    let state = match (oauth_authorization, oauth_callbacks) {
        (Some(authorization), Some(callbacks)) => state.with_oauth(authorization, callbacks),
        (None, None) if dev_inmemory => state,
        (None, None) => {
            return Err(
                "production composition omitted the canonical OAuth authorization/callback plane; refusing to start"
                    .to_owned(),
            );
        },
        _ => {
            return Err(
                "OAuth authorization and callback composition must be installed together"
                    .to_owned(),
            );
        },
    };

    match (
        oauth_refresh_worker,
        oauth_cleanup_worker,
        oauth_materialization_reconciler,
    ) {
        (Some(refresh_worker), Some(cleanup_worker), Some(materialization_reconciler)) => {
            spawn_oauth_refresh_worker(&merger, refresh_worker)?;
            spawn_oauth_token_cleanup_worker(&merger, cleanup_worker)?;
            spawn_oauth_materialization_reconciler(&merger, materialization_reconciler)?;
        },
        (None, None, None) if dev_inmemory => {},
        (None, None, None) => {
            return Err(
                "production composition omitted the durable OAuth lifecycle workers; refusing to start"
                    .to_owned(),
            );
        },
        _ => {
            return Err(
                "OAuth refresh, cleanup, and materialization reconciliation workers must be composed together"
                    .to_owned(),
            );
        },
    }

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
    Arc<RollbackApplicationService>,
    Arc<ExecutionService>,
    Arc<ScheduleService>,
    Arc<NotificationService>,
    Option<Arc<mas_application::inbound_webhook_service::InboundWebhookEndpointService>>,
    Option<Arc<dyn mas_integrations::inbound_webhooks::InboundWebhookIngressPort>>,
    Option<integration_composition::IntegrationComposition>,
    Option<Arc<OAuthAuthorizationService>>,
    Option<Arc<mas_integrations::durable_oauth::DurableOAuthAuthorizationService>>,
    Option<Arc<mas_integrations::oauth_refresh::OAuthTokenRefreshWorker>>,
    Option<Arc<mas_integrations::oauth_cleanup::OAuthTokenCleanupWorker>>,
    Option<Arc<mas_integrations::oauth_materialization::OAuthMaterializationReconciler>>,
    Arc<AnalyticsService>,
    Arc<ApiKeyApplicationService>,
    Arc<ApprovalApplicationService>,
    Arc<DataHubApplicationService>,
    Arc<SearchService>,
    // BATCH-01 governance reads. `None` (the in-memory lane, or production
    // without an audit cursor key) means the route answers a typed 503
    // instead of fabricating data.
    Option<Arc<mas_application::audit_query_service::AuditQueryService>>,
    Option<Arc<mas_application::policy_service::PolicyService>>,
    Option<Arc<mas_application::usage_service::UsageService>>,
    // BATCH-02 tooling reads/actions. `None` ⇒ typed 503 per surface.
    Option<Arc<mas_application::tool_service::ToolService>>,
    Option<Arc<mas_application::connector_service::ConnectorService>>,
    Option<Arc<mas_application::webhook_service::WebhookService>>,
    Option<Arc<mas_api::handlers::stream::StreamHub>>,
    Option<Arc<mas_application::assistant_service::AssistantService>>,
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

/// Opens a deliberately separate role-scoped database pool. Each dedicated
/// callback/relay role needs its own credential/SET ROLE URL: reusing `mas_app`
/// would either fail under RLS or silently widen request-plane authority.
async fn connect_role_pool(
    merger: &mas_common::config::Merger<'_>,
    environment_key: &str,
    config_key: &str,
    application_name: &str,
    max_connections: u64,
) -> Result<sqlx::PgPool, String> {
    use mas_persistence::pool::{Database, DatabaseUrl, PoolConfig};

    let raw_url = merger.string(environment_key, config_key, "");
    if raw_url.is_empty() {
        return Err(format!(
            "production mode requires {environment_key} ({config_key}) for {application_name}"
        ));
    }
    let url = DatabaseUrl::new(raw_url)
        .map_err(|error| format!("{application_name} database url: {error}"))?;
    let mut config = PoolConfig::for_url(url, application_name)
        .map_err(|error| format!("{application_name} database config: {error}"))?;
    config.max_connections = u32::try_from(max_connections).unwrap_or(32).min(8);
    let config = config
        .validated()
        .map_err(|error| format!("{application_name} database config: {error}"))?;
    Database::connect(&config)
        .await
        .map(|database| database.pool().clone())
        .map_err(|error| format!("{application_name} database: {error}"))
}

/// Installs whichever governance services this lane composed. A lane that
/// omitted one (in-memory bring-up, or production without an audit cursor
/// key) leaves the field `None`, and the dependent route answers a typed 503
/// instead of serving fabricated data.
fn install_governance_services(
    state: AppState,
    audit_query: Option<Arc<mas_application::audit_query_service::AuditQueryService>>,
    policies: Option<Arc<mas_application::policy_service::PolicyService>>,
    usage: Option<Arc<mas_application::usage_service::UsageService>>,
) -> AppState {
    let state = match audit_query {
        Some(service) => state.with_audit_query(service),
        None => state,
    };
    let state = match policies {
        Some(service) => state.with_policies(service),
        None => state,
    };
    match usage {
        Some(service) => state.with_usage(service),
        None => state,
    }
}

/// Installs whichever tooling services this lane composed. Every field is
/// optional for the same reason as the governance services: a lane that
/// omitted one answers a typed 503 on the dependent routes instead of
/// fabricating catalogues, deliveries, or stream events.
fn install_tooling_services(
    state: AppState,
    tools: Option<Arc<mas_application::tool_service::ToolService>>,
    connector_admin: Option<Arc<mas_application::connector_service::ConnectorService>>,
    webhook_admin: Option<Arc<mas_application::webhook_service::WebhookService>>,
    stream_hub: Option<Arc<mas_api::handlers::stream::StreamHub>>,
    assistant: Option<Arc<mas_application::assistant_service::AssistantService>>,
) -> AppState {
    let state = match tools {
        Some(service) => state.with_tools(service),
        None => state,
    };
    let state = match connector_admin {
        Some(service) => state.with_connector_admin(service),
        None => state,
    };
    let state = match webhook_admin {
        Some(service) => state.with_webhook_admin(service),
        None => state,
    };
    let state = match stream_hub {
        Some(hub) => state.with_stream_hub(hub),
        None => state,
    };
    match assistant {
        Some(service) => state.with_assistant(service),
        None => state,
    }
}

/// Typed `[stream]` bounds for the SSE hub from the layered configuration.
///
/// # Errors
/// A missing/invalid key or an out-of-range value — configuration must fail
/// at boot, never at the first connected client.
fn stream_config(
    merger: &mas_common::config::Merger<'_>,
) -> Result<mas_common::config::StreamConfig, String> {
    let heartbeat_secs = merger
        .u64("MAS_STREAM_HEARTBEAT_SECS", "stream.heartbeat_secs", 15)
        .map_err(|error| format!("stream.heartbeat_secs: {error}"))?;
    let channel_buffer = merger
        .u64("MAS_STREAM_CHANNEL_BUFFER", "stream.channel_buffer", 256)
        .map_err(|error| format!("stream.channel_buffer: {error}"))?;
    let max_connections = merger
        .u64(
            "MAS_STREAM_MAX_CONNECTIONS_PER_PRINCIPAL",
            "stream.max_connections_per_principal",
            8,
        )
        .map_err(|error| format!("stream.max_connections_per_principal: {error}"))?;
    let config = mas_common::config::StreamConfig {
        heartbeat_secs,
        channel_buffer: usize::try_from(channel_buffer)
            .map_err(|_| "stream.channel_buffer does not fit this platform".to_owned())?,
        max_connections_per_principal: usize::try_from(max_connections).map_err(|_| {
            "stream.max_connections_per_principal does not fit this platform".to_owned()
        })?,
    };
    config
        .validate()
        .map_err(|error| format!("stream configuration: {error}"))?;
    Ok(config)
}

/// Starts the lease-fenced outbound webhook relay. The loop owns no queue
/// state: PostgreSQL claims/fences make concurrent API replicas safe, while a
/// shutdown during a bounded pass abandons its lease rather than risking a
/// stale settlement. The next replica may reclaim it after expiry.
fn spawn_outbound_webhook_worker(
    worker: Arc<integration_composition::OutboundWebhookWorker>,
    poll_interval: std::time::Duration,
) {
    tokio::spawn(async move {
        let shutdown = mas_api::server::shutdown_signal();
        tokio::pin!(shutdown);
        loop {
            let now = mas_common::timestamps::Timestamp::now();
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!(worker = "outbound_webhook", "outbound webhook relay stopping on shutdown");
                    break;
                },
                result = worker.run_once(&now) => {
                    match result {
                        Ok(cycle) => tracing::debug!(
                            attempted = cycle.attempted,
                            delivered = cycle.delivered,
                            requeued = cycle.requeued,
                            dead_lettered = cycle.dead_lettered,
                            stale = cycle.stale,
                            released = cycle.released,
                            "outbound webhook relay pass completed"
                        ),
                        Err(error) => tracing::warn!(
                            error = %error.public_message(),
                            "outbound webhook relay pass failed"
                        ),
                    }
                },
            }
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!(worker = "outbound_webhook", "outbound webhook relay stopped before next poll");
                    break;
                },
                () = tokio::time::sleep(poll_interval) => {},
            }
        }
    });
}

/// Starts the lease-fenced notification delivery loop. Only represented
/// adapters can settle a row successfully; unsupported channels remain durable
/// failure/dead-letter evidence rather than becoming a successful no-op.
fn spawn_notification_delivery_worker(
    worker: Arc<integration_composition::NotificationDeliveryWorker>,
    poll_interval: std::time::Duration,
) {
    tokio::spawn(async move {
        let shutdown = mas_api::server::shutdown_signal();
        tokio::pin!(shutdown);
        loop {
            let now = mas_common::timestamps::Timestamp::now();
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!(worker = "notification_delivery", "notification delivery worker stopping on shutdown");
                    break;
                },
                result = worker.run_once(&now) => {
                    match result {
                        Ok(cycle) => tracing::debug!(
                            attempted = cycle.attempted,
                            delivered = cycle.delivered,
                            requeued = cycle.requeued,
                            dead_lettered = cycle.dead_lettered,
                            stale = cycle.stale,
                            released = cycle.released,
                            "notification delivery pass completed"
                        ),
                        Err(error) => tracing::warn!(
                            error = %error.public_message(),
                            "notification delivery pass failed"
                        ),
                    }
                },
            }
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!(worker = "notification_delivery", "notification delivery worker stopped before next poll");
                    break;
                },
                () = tokio::time::sleep(poll_interval) => {},
            }
        }
    });
}

/// Starts the independently fenced OAuth refresh loop. Every API replica may
/// run this loop: PostgreSQL SKIP LOCKED leases and monotonic fences prevent
/// duplicate durable settlement, and no process-local schedule is authoritative.
fn spawn_oauth_refresh_worker(
    merger: &mas_common::config::Merger<'_>,
    worker: Arc<mas_integrations::oauth_refresh::OAuthTokenRefreshWorker>,
) -> Result<(), String> {
    let poll_seconds = merger
        .u64(
            "MAS_OAUTH_REFRESH_POLL_SECONDS",
            "integrations.oauth.refresh_poll_seconds",
            30,
        )
        .map_err(|error| format!("OAuth refresh poll configuration: {error}"))?;
    if !(1..=3_600).contains(&poll_seconds) {
        return Err("OAuth refresh poll interval must be between 1 and 3600 seconds".to_owned());
    }
    let batch = merger
        .u64(
            "MAS_OAUTH_REFRESH_BATCH_SIZE",
            "integrations.oauth.refresh_batch_size",
            32,
        )
        .map_err(|error| format!("OAuth refresh batch configuration: {error}"))?;
    let batch = u16::try_from(batch)
        .ok()
        .filter(|value| {
            (1..=mas_integrations::oauth_refresh::MAX_OAUTH_REFRESH_BATCH).contains(value)
        })
        .ok_or_else(|| "OAuth refresh batch size must be between 1 and 100".to_owned())?;
    let configured_name = merger.string(
        "MAS_OAUTH_REFRESH_WORKER_ID",
        "integrations.oauth.refresh_worker_id",
        "api-oauth-refresh",
    );
    if configured_name.is_empty()
        || configured_name.len() > 100
        || !configured_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("OAuth refresh worker id must be a bounded safe token".to_owned());
    }
    let worker_id = format!("{configured_name}-{}", std::process::id());
    tokio::spawn(async move {
        loop {
            match worker
                .run_once(&worker_id, &mas_common::timestamps::Timestamp::now(), batch)
                .await
            {
                Ok(stats) => {
                    tracing::debug!(
                        claimed = stats.claimed,
                        refreshed = stats.refreshed,
                        retry_scheduled = stats.retry_scheduled,
                        revoked_or_dead_lettered = stats.revoked_or_dead_lettered,
                        "OAuth token refresh pass completed"
                    );
                },
                Err(error) => {
                    // The job row/lease remains durable for retry. Do not log
                    // any provider/Vault response or material; AppError's
                    // public message is intentionally bounded/redacted.
                    tracing::warn!(error = %error.public_message(), "OAuth token refresh pass failed");
                },
            }
            tokio::time::sleep(std::time::Duration::from_secs(poll_seconds)).await;
        }
    });
    Ok(())
}

/// Starts the independently fenced OAuth token-cleanup loop. It deliberately
/// shares the refresh poll/batch configuration so operations has one bounded
/// OAuth lifecycle cadence; queues remain independent and each has its own
/// PostgreSQL lease/fence namespace.
fn spawn_oauth_token_cleanup_worker(
    merger: &mas_common::config::Merger<'_>,
    worker: Arc<mas_integrations::oauth_cleanup::OAuthTokenCleanupWorker>,
) -> Result<(), String> {
    let poll_seconds = merger
        .u64(
            "MAS_OAUTH_REFRESH_POLL_SECONDS",
            "integrations.oauth.refresh_poll_seconds",
            30,
        )
        .map_err(|error| format!("OAuth token cleanup poll configuration: {error}"))?;
    if !(1..=3_600).contains(&poll_seconds) {
        return Err(
            "OAuth token cleanup poll interval must be between 1 and 3600 seconds".to_owned(),
        );
    }
    let batch = merger
        .u64(
            "MAS_OAUTH_REFRESH_BATCH_SIZE",
            "integrations.oauth.refresh_batch_size",
            32,
        )
        .map_err(|error| format!("OAuth token cleanup batch configuration: {error}"))?;
    let batch = u16::try_from(batch)
        .ok()
        .filter(|value| {
            (1..=mas_integrations::oauth_cleanup::MAX_OAUTH_TOKEN_CLEANUP_BATCH).contains(value)
        })
        .ok_or_else(|| "OAuth token cleanup batch size must be between 1 and 100".to_owned())?;
    let configured_name = merger.string(
        "MAS_OAUTH_REFRESH_WORKER_ID",
        "integrations.oauth.refresh_worker_id",
        "api-oauth-refresh",
    );
    if configured_name.is_empty()
        || configured_name.len() > 100
        || !configured_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("OAuth token cleanup worker id must be a bounded safe token".to_owned());
    }
    let worker_id = format!("{configured_name}-cleanup-{}", std::process::id());
    tokio::spawn(async move {
        loop {
            match worker
                .run_once(&worker_id, &mas_common::timestamps::Timestamp::now(), batch)
                .await
            {
                Ok(stats) => {
                    tracing::debug!(
                        claimed = stats.claimed,
                        cleaned = stats.cleaned,
                        retry_scheduled = stats.retry_scheduled,
                        dead_lettered = stats.dead_lettered,
                        "OAuth token cleanup pass completed"
                    );
                },
                Err(error) => {
                    tracing::warn!(
                        error = %error.public_message(),
                        "OAuth token cleanup pass failed"
                    );
                },
            }
            tokio::time::sleep(std::time::Duration::from_secs(poll_seconds)).await;
        }
    });
    Ok(())
}

/// Starts the fenced reconciler for uncertain callback materialization outcomes.
/// It shares the bounded OAuth lifecycle cadence but owns a separate durable
/// queue, so a failed secret-manager cleanup cannot stall token refreshes.
fn spawn_oauth_materialization_reconciler(
    merger: &mas_common::config::Merger<'_>,
    worker: Arc<mas_integrations::oauth_materialization::OAuthMaterializationReconciler>,
) -> Result<(), String> {
    let poll_seconds = merger
        .u64(
            "MAS_OAUTH_REFRESH_POLL_SECONDS",
            "integrations.oauth.refresh_poll_seconds",
            30,
        )
        .map_err(|error| format!("OAuth materialization poll configuration: {error}"))?;
    if !(1..=3_600).contains(&poll_seconds) {
        return Err(
            "OAuth materialization poll interval must be between 1 and 3600 seconds".to_owned(),
        );
    }
    let batch = merger
        .u64(
            "MAS_OAUTH_REFRESH_BATCH_SIZE",
            "integrations.oauth.refresh_batch_size",
            32,
        )
        .map_err(|error| format!("OAuth materialization batch configuration: {error}"))?;
    let batch = u16::try_from(batch)
        .ok()
        .filter(|value| {
            (1..=mas_integrations::oauth_materialization::MAX_OAUTH_MATERIALIZATION_BATCH)
                .contains(value)
        })
        .ok_or_else(|| "OAuth materialization batch size must be between 1 and 100".to_owned())?;
    let configured_name = merger.string(
        "MAS_OAUTH_REFRESH_WORKER_ID",
        "integrations.oauth.refresh_worker_id",
        "api-oauth-refresh",
    );
    if configured_name.is_empty()
        || configured_name.len() > 100
        || !configured_name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("OAuth materialization worker id must be a bounded safe token".to_owned());
    }
    let worker_id = format!("{configured_name}-materialization-{}", std::process::id());
    tokio::spawn(async move {
        loop {
            match worker
                .run_once(&worker_id, &mas_common::timestamps::Timestamp::now(), batch)
                .await
            {
                Ok(stats) => {
                    tracing::debug!(
                        claimed = stats.claimed,
                        bindings_confirmed = stats.bindings_confirmed,
                        orphan_material_deleted = stats.orphan_material_deleted,
                        retry_scheduled = stats.retry_scheduled,
                        dead_lettered = stats.dead_lettered,
                        "OAuth materialization reconciliation pass completed"
                    );
                },
                Err(error) => {
                    tracing::warn!(
                        error = %error.public_message(),
                        "OAuth materialization reconciliation pass failed"
                    );
                },
            }
            tokio::time::sleep(std::time::Duration::from_secs(poll_seconds)).await;
        }
    });
    Ok(())
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
    let agents = Arc::new(
        AgentService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        )
        .with_rollback_store(Box::new(store.clone())),
    );
    let workflows = Arc::new(
        WorkflowService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        )
        .with_rollback_store(Box::new(store.clone())),
    );
    let rollbacks = Arc::new(RollbackApplicationService::new(
        Arc::clone(&agents),
        Arc::clone(&workflows),
        Box::new(store.clone()),
    ));
    let executions = Arc::new(
        ExecutionService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
            Arc::new(mas_application::durable_memory::InMemoryDurableExecution::new()),
            // The same control plane the tenancy service provisions through. One
            // process, one gate: a second instance would not see the subscriptions
            // the first one's provisioning wrote.
            Arc::clone(&control_plane),
        )
        .with_step_store(Box::new(store.clone())),
    );
    let schedules = Arc::new(ScheduleService::new(
        Box::new(store.clone()),
        Box::new(audit.clone()) as Box<dyn mas_application::audit::AuditSinkPort>,
    ));
    let notifications = Arc::new(NotificationService::new(
        Box::new(store.clone()),
        Box::new(audit.clone()) as Box<dyn mas_application::audit::AuditSinkPort>,
    ));
    let analytics = Arc::new(AnalyticsService::new(Box::new(store.clone())));
    let api_key_store = Arc::new(mas_security::InMemoryApiKeyStore::new());
    let security_api_keys = Arc::new(mas_security::ApiKeyService::new(
        api_key_store.clone(),
        audit.clone(),
    ));
    let api_keys = Arc::new(ApiKeyApplicationService::new(
        security_api_keys,
        api_key_store,
    ));
    let approvals = Arc::new(ApprovalApplicationService::new(
        store.clone(),
        audit.clone(),
    ));
    let data_hub = Arc::new(
        DataHubApplicationService::new(
            store.clone(),
            store.clone(),
            store.clone(),
            Arc::new(mas_execution::InMemoryDataHubBridge::new()),
            audit.clone(),
            data_hub_limits(merger)?,
        )
        .map_err(|error| format!("Data Hub configuration: {error}"))?,
    );

    let search = Arc::new(SearchService::new(
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
        Box::new(store.clone()),
    ));
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
        rollbacks,
        executions,
        schedules,
        notifications,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        analytics,
        api_keys,
        approvals,
        data_hub,
        search,
        // The in-memory lane has no database plane for governance reads; the
        // routes answer a typed 503 rather than a plausible empty result.
        None,
        None,
        None,
        // Likewise the tooling plane: no durable catalogue, deliveries, or
        // event transport in the dev lane — typed 503, never fake data.
        None,
        None,
        None,
        None,
        None,
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

    // Public callbacks cannot carry a user session/RLS GUC. They therefore
    // receive separate minimal pools: one writes verified receipt/outbox facts
    // as `mas_inbound`; one reads only secret-reference metadata through the
    // pre-existing integrations relay boundary for audited Vault resolution.
    let inbound_pool = connect_role_pool(
        merger,
        "MAS_DATABASE_INBOUND_URL",
        "database.inbound_url",
        "mas-api-inbound",
        max_connections,
    )
    .await?;
    let integrations_pool = connect_role_pool(
        merger,
        "MAS_DATABASE_INTEGRATIONS_URL",
        "database.integrations_url",
        "mas-api-integrations",
        max_connections,
    )
    .await?;
    // Durable recipient delivery reads only notification rows across tenants
    // and therefore gets its own narrow mas_notifications role/pool. It must
    // never borrow mas_app or the integrations relay role.
    let notifications_pool = connect_role_pool(
        merger,
        "MAS_DATABASE_NOTIFICATIONS_URL",
        "database.notifications_url",
        "mas-api-notifications",
        max_connections,
    )
    .await?;
    // Retain probe handles before workers consume their role pools. Readiness
    // must report loss of a delivery role rather than only the mas_app pool.
    let integrations_health_pool = integrations_pool.clone();
    let notifications_health_pool = notifications_pool.clone();
    // OAuth state consumption intentionally precedes tenant resolution, so it
    // uses its own narrowly granted mas_oauth callback pool rather than the
    // request/RLS pool or broad integrations relay role.
    let oauth_pool = connect_role_pool(
        merger,
        "MAS_DATABASE_OAUTH_URL",
        "database.oauth_url",
        "mas-api-oauth",
        max_connections,
    )
    .await?;
    let oauth_refresh_pool = connect_role_pool(
        merger,
        "MAS_DATABASE_OAUTH_REFRESH_URL",
        "database.oauth_refresh_url",
        "mas-api-oauth-refresh",
        max_connections,
    )
    .await?;

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
    let agents = Arc::new(
        AgentService::new(
            Box::new(PgServices::new(pool.clone())),
            Box::new(PgServices::new(pool.clone())),
            Box::new(PgServices::new(pool.clone())),
            Box::new(PostgresAuditLog::new(audit_pool.clone())),
        )
        .with_rollback_store(Box::new(PgServices::new(pool.clone()))),
    );
    let workflows = Arc::new(
        WorkflowService::new(
            Box::new(PgServices::new(pool.clone())),
            Box::new(PgServices::new(pool.clone())),
            Box::new(PgServices::new(pool.clone())),
            Box::new(PostgresAuditLog::new(audit_pool.clone())),
        )
        .with_rollback_store(Box::new(PgServices::new(pool.clone()))),
    );
    // The durable port is Postgres: submission, claiming, settlement and
    // cancellation all run as one transaction each against the same pool the
    // rest of the service uses.
    // One audit log for the whole lane, shared with the control-plane
    // handlers: a request's audit trail is one sequence, not one sequence per
    // service that happened to write a line.
    let audit_log = Arc::new(PostgresAuditLog::new(audit_pool.clone()));
    let audit_sink: Arc<dyn mas_application::audit::AuditSinkPort> = audit_log.clone();
    let (inbound_webhook_endpoints, inbound_webhook_ingress) =
        inbound_webhook_composition::compose(
            merger,
            pool.clone(),
            inbound_pool,
            integrations_pool.clone(),
            audit_log.clone(),
        )?;
    let (
        oauth_authorization,
        oauth_callbacks,
        oauth_refresh_worker,
        oauth_cleanup_worker,
        oauth_materialization_reconciler,
    ) = oauth_composition::compose(
        merger,
        pool.clone(),
        oauth_pool,
        oauth_refresh_pool,
        audit_log.clone(),
    )?;
    let integration_plane = integration_composition::compose(
        merger,
        pool.clone(),
        integrations_pool,
        notifications_pool,
        audit_log.clone(),
    )?;
    let rollbacks = Arc::new(RollbackApplicationService::new(
        Arc::clone(&agents),
        Arc::clone(&workflows),
        Box::new(PgServices::new(pool.clone())),
    ));
    let executions = Arc::new(
        ExecutionService::new(
            Box::new(PgServices::new(pool.clone())),
            Box::new(PgServices::new(pool.clone())),
            Box::new(PgServices::new(pool.clone())),
            Box::new(PgServices::new(pool.clone())),
            Box::new(audit_log.clone()),
            Arc::new(PgDurableExecution::new(pool.clone())),
            Arc::clone(&control_plane),
        )
        .with_step_store(Box::new(PgServices::new(pool.clone()))),
    );
    let schedules = Arc::new(ScheduleService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));
    let notifications = Arc::new(NotificationService::new(
        Box::new(mas_persistence::repositories::PostgresNotificationStore::new(pool.clone())),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));

    let analytics = Arc::new(AnalyticsService::new(Box::new(
        mas_persistence::repositories::analytics::PostgresAnalyticsStore::new(pool.clone()),
    )));
    let api_key_store = Arc::new(ApiKeyStore::new(pool.clone()));
    let security_api_keys = Arc::new(mas_security::ApiKeyService::new(
        api_key_store.clone(),
        audit_log.clone(),
    ));
    let api_keys = Arc::new(ApiKeyApplicationService::new(
        security_api_keys,
        api_key_store,
    ));
    let approvals = Arc::new(ApprovalApplicationService::new(
        Arc::new(mas_persistence::repositories::PostgresApprovalStore::new(
            pool.clone(),
        )),
        audit_log.clone(),
    ));
    let data_hub = compose_production_data_hub(merger, pool.clone(), audit_log.clone()).await?;
    let search = Arc::new(SearchService::with_catalog(Box::new(
        mas_persistence::repositories::PostgresSearchCatalog::new(pool.clone()),
    )));

    // ── Governance surface: audit reads, policy lifecycle, usage/quota ──
    //
    // The policy service deliberately shares the control plane's canonical
    // engine and store: the same compiled policy set is both enforced at
    // execution time and administered through /v1/policies, so an edit can
    // never drift from what is evaluated.
    let policy_service = Arc::new(mas_application::policy_service::PolicyService::new(
        Arc::clone(control_plane.policy().store()),
        Arc::clone(control_plane.policy()),
        Arc::clone(&audit_sink),
    ));
    // Plan-card and usage reads come from the append-only usage ledger and
    // the tenant's quota definitions; utilization is computed here, never
    // supplied by a client.
    let usage_service = Arc::new(mas_application::usage_service::UsageService::new(Box::new(
        mas_persistence::PostgresUsageStore::new(pool.clone()),
    )));
    // Filtered audit keyset cursors must be opaque and tamper-checked, so
    // they are HMAC-signed with a deployment-owned >=32-byte key. Prefer the
    // dedicated key, fall back to the internal service secret; without either
    // the read plane stays uncomposed and /v1/audit-events answers a typed
    // 503 — minting unverifiable cursors would be worse than refusing.
    let audit_cursor_key = {
        let dedicated = merger.string("MAS_AUDIT_CURSOR_KEY", "audit.cursor_key", "");
        let fallback = load_internal_secret(merger)?.map(|secret| secret.expose().to_owned());
        if dedicated.len() >= 32 {
            Some(dedicated.into_bytes())
        } else if fallback.as_deref().is_some_and(|value| value.len() >= 32) {
            fallback.map(String::into_bytes)
        } else {
            eprintln!(
                "mas-api: no >=32-byte MAS_AUDIT_CURSOR_KEY or internal service secret; \
                 /v1/audit-events stays disabled (typed 503)"
            );
            None
        }
    };
    let audit_query = audit_cursor_key.and_then(|key| {
        match mas_application::audit_query_service::AuditQueryService::new(
            Box::new(PostgresAuditLog::new(audit_pool.clone())),
            key,
        ) {
            Ok(service) => Some(Arc::new(service)),
            Err(error) => {
                eprintln!("mas-api: audit query composition failed: {error}");
                None
            },
        }
    });

    // ── Tooling surface: catalogue, connector admin, webhook ops, SSE, assistant
    //
    // Tool permissions are evaluated against the SAME canonical policy engine
    // the control plane enforces with, so a grant can never disagree with the
    // policy an invocation is checked against.
    let tool_service = Arc::new(mas_application::tool_service::ToolService::new(
        Box::new(mas_persistence::PostgresToolStore::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Arc::clone(control_plane.policy()),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));
    // Provider manifests are an operator-owned built-in registry (no tenant
    // state); disable+revoke is the durable lifecycle port written for this
    // batch, and probes reuse the already-composed guarded integration plane.
    let connector_admin = Arc::new(mas_application::connector_service::ConnectorService::new(
        Box::new(mas_integrations::registry::InMemoryProviderRegistry::with_builtin()),
        Arc::clone(&integration_plane.service),
        Box::new(
            mas_persistence::repositories::connector_lifecycle::PostgresConnectorLifecycle::new(
                pool.clone(),
            ),
        ),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));
    // Delivery history and replay over the dead-letter evidence tables; the
    // relay (not the API) owns sending, so nothing here touches leases.
    let webhook_admin = Arc::new(mas_application::webhook_service::WebhookService::new(
        Box::new(
            mas_persistence::repositories::webhook_admin::PostgresWebhookAdmin::new(pool.clone()),
        ),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));
    // The assistant is an ordinary execution submission (same stores, same
    // policy/quota/audit path) — no second execution path exists.
    let assistant = Arc::new(mas_application::assistant_service::AssistantService::new(
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Box::new(PgServices::new(pool.clone())),
        Arc::clone(&executions),
        Box::new(PostgresAuditLog::new(audit_pool.clone())),
    ));
    // SSE fan-out tails the canonical event plane through the read-only
    // `mas.events.>` tail (the relay's `NatsEventBus` stays publish-only by
    // contract). No NATS URL means no live event source: production boots
    // must fail loudly here rather than serve an `/v1/stream` that silently
    // never delivers.
    let stream_hub = {
        let config = stream_config(merger)?;
        let nats_url = merger.string("MAS_NATS_URL", "broker.nats_url", "");
        if nats_url.is_empty() {
            return Err("the production event stream requires an event bus address \
                 (MAS_NATS_URL or config broker.nats_url); refusing to boot \
                 an SSE plane with no live event source"
                .to_owned());
        }
        let tail = mas_messaging::event_tail::NatsEventTailBus::connect(&nats_url)
            .await
            .map_err(|error| format!("nats event tail: {error}"))?;
        let hub =
            mas_api::handlers::stream::StreamHub::new(config).map_err(|error| error.to_string())?;
        hub.attach(Arc::new(tail))
            .await
            .map_err(|error| format!("event stream hub subscription: {error}"))?;
        hub
    };

    let oidc = compose_oidc_verifier(merger)?;
    let verifier: Arc<dyn mas_api::auth::TokenVerifierPort> =
        Arc::new(mas_api::oidc_auth::ProductionTokenVerifier::new(
            mas_api::prod_auth::PostgresApiKeyVerifier::new(ApiKeyStore::new(pool.clone())),
            oidc,
        ));

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
    health
        .register(
            mas_observability::health::ProbeKind::Readiness,
            "postgres-integrations-relay",
            PgPoolHealthCheck {
                pool: integrations_health_pool,
                component: "postgres-integrations-relay",
            },
        )
        .map_err(|err| format!("integrations relay health: {err}"))?;
    health
        .register(
            mas_observability::health::ProbeKind::Readiness,
            "postgres-notifications-delivery",
            PgPoolHealthCheck {
                pool: notifications_health_pool,
                component: "postgres-notifications-delivery",
            },
        )
        .map_err(|err| format!("notification delivery health: {err}"))?;

    Ok((
        tenancy,
        agents,
        workflows,
        rollbacks,
        executions,
        schedules,
        notifications,
        Some(inbound_webhook_endpoints),
        Some(inbound_webhook_ingress),
        Some(integration_plane),
        Some(oauth_authorization),
        Some(oauth_callbacks),
        Some(oauth_refresh_worker),
        Some(oauth_cleanup_worker),
        Some(oauth_materialization_reconciler),
        analytics,
        api_keys,
        approvals,
        data_hub,
        search,
        audit_query,
        Some(policy_service),
        Some(usage_service),
        Some(tool_service),
        Some(connector_admin),
        Some(webhook_admin),
        Some(Arc::new(stream_hub)),
        Some(assistant),
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

/// Resolves bounded Data Hub request limits from the layered configuration.
fn data_hub_limits(merger: &mas_common::config::Merger<'_>) -> Result<DataHubLimits, String> {
    let max_upload_bytes = merger
        .u64(
            "MAS_DATA_HUB_MAX_UPLOAD_BYTES",
            "data_hub.max_upload_bytes",
            mas_application::data_hub_service::DEFAULT_MAX_UPLOAD_BYTES,
        )
        .map_err(|error| format!("Data Hub upload limit: {error}"))?;
    let bridge_timeout_ms = merger
        .u64(
            "MAS_DATA_HUB_BRIDGE_TIMEOUT_MS",
            "data_hub.bridge_timeout_ms",
            300_000,
        )
        .map_err(|error| format!("Data Hub bridge timeout: {error}"))?;
    DataHubLimits {
        max_upload_bytes,
        bridge_timeout: std::time::Duration::from_millis(bridge_timeout_ms),
    }
    .validate()
    .map_err(|error| format!("Data Hub limits: {error}"))
}

/// Composes the real Rust-to-Python Data Hub path. A durable API process
/// refuses startup when this authenticated dependency is absent instead of
/// exposing catalogue mutations whose sync/upload operations can never work.
async fn compose_production_data_hub(
    merger: &mas_common::config::Merger<'_>,
    pool: sqlx::PgPool,
    audit: Arc<mas_persistence::repositories::PostgresAuditLog>,
) -> Result<Arc<DataHubApplicationService>, String> {
    let endpoint = merger.string("MAS_AI_BRIDGE_ENDPOINT", "ai_bridge.endpoint", "");
    if endpoint.trim().is_empty() {
        return Err(
            "Data Hub requires ai_bridge.endpoint / MAS_AI_BRIDGE_ENDPOINT; \
             production does not substitute an in-memory ingestion plane"
                .to_owned(),
        );
    }
    let production = environment_string(merger) == "production";
    if production && !endpoint.starts_with("https://") {
        return Err("production Data Hub bridge endpoint must use https://".to_owned());
    }
    let token = merger.string("MAS_AI_BRIDGE_TOKEN", "ai_bridge.token", "");
    if token.trim().is_empty() {
        return Err("Data Hub requires MAS_AI_BRIDGE_TOKEN / ai_bridge.token; \
             the Python ingestion plane refuses unauthenticated requests"
            .to_owned());
    }
    let signing_secret = merger.string(
        "MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET",
        "ai_bridge.internal_signing_secret",
        "",
    );
    if production && signing_secret.trim().is_empty() {
        return Err(
            "production Data Hub requires MAS_AI_BRIDGE_INTERNAL_SIGNING_SECRET / \
             ai_bridge.internal_signing_secret for body-bound RPC signatures"
                .to_owned(),
        );
    }
    let timeout_ms = merger
        .u64("MAS_AI_BRIDGE_TIMEOUT_MS", "ai_bridge.timeout_ms", 120_000)
        .map_err(|error| format!("Data Hub bridge timeout: {error}"))?;
    let max_response_bytes = merger
        .u64(
            "MAS_AI_BRIDGE_MAX_RESPONSE_BYTES",
            "ai_bridge.max_response_bytes",
            8 * 1024 * 1024,
        )
        .map_err(|error| format!("Data Hub bridge response limit: {error}"))?;
    let ca = optional_pem(
        merger.string(
            "MAS_AI_BRIDGE_TLS_CA_CERTIFICATE_PATH",
            "ai_bridge.tls_ca_certificate_path",
            "",
        ),
        "TLS CA certificate",
    )?;
    let certificate = optional_pem(
        merger.string(
            "MAS_AI_BRIDGE_TLS_CLIENT_CERTIFICATE_PATH",
            "ai_bridge.tls_client_certificate_path",
            "",
        ),
        "TLS client certificate",
    )?;
    let private_key_path = merger.string(
        "MAS_AI_BRIDGE_TLS_CLIENT_PRIVATE_KEY_PATH",
        "ai_bridge.tls_client_private_key_path",
        "",
    );
    let private_key = if private_key_path.trim().is_empty() {
        None
    } else {
        let contents = std::fs::read_to_string(&private_key_path).map_err(|error| {
            format!("cannot read Data Hub TLS client private key at {private_key_path}: {error}")
        })?;
        if contents.trim().is_empty() {
            return Err("configured Data Hub TLS client private key is empty".to_owned());
        }
        Some(mas_security::SecretString::new(contents))
    };
    if certificate.is_some() != private_key.is_some() {
        return Err(
            "Data Hub mutual TLS requires both ai_bridge.tls_client_certificate_path \
             and ai_bridge.tls_client_private_key_path"
                .to_owned(),
        );
    }
    if !endpoint.starts_with("https://") && (ca.is_some() || certificate.is_some()) {
        return Err("Data Hub TLS material requires an https:// endpoint".to_owned());
    }
    let limits = data_hub_limits(merger)?;
    let agent_bridge = mas_ai_bridge::GrpcAgentBridgeConfig {
        endpoint,
        default_timeout: std::time::Duration::from_millis(timeout_ms.max(1)),
        max_response_bytes: usize::try_from(max_response_bytes.max(1024))
            .map_err(|_| "Data Hub bridge response limit does not fit usize".to_owned())?,
        bearer_token: Some(mas_security::SecretString::new(token)),
        service_subject: "mas-api-data-hub".to_owned(),
        service_issuer: "platform-internal".to_owned(),
        service_scopes: vec![
            "data_hub_sync".to_owned(),
            "data_hub_ingest".to_owned(),
            "data_hub_purge".to_owned(),
        ],
        internal_signing_secret: (!signing_secret.trim().is_empty())
            .then(|| mas_security::SecretString::new(signing_secret)),
        internal_service_id: "mas-api".to_owned(),
        tls_ca_certificate: ca,
        tls_client_certificate: certificate,
        tls_client_private_key: private_key,
    };
    let bridge = Arc::new(
        mas_ai_bridge::GrpcDataHubBridge::connect(mas_ai_bridge::GrpcDataHubBridgeConfig {
            agent_bridge,
            max_request_bytes: usize::try_from(limits.max_upload_bytes)
                .map_err(|_| "Data Hub upload limit does not fit usize".to_owned())?,
        })
        .await
        .map_err(|error| format!("Data Hub bridge: {error}"))?,
    );
    let service = DataHubApplicationService::new(
        Arc::new(mas_persistence::repositories::PostgresDataSourceStore::new(
            pool.clone(),
        )),
        Arc::new(mas_persistence::repositories::PostgresDocumentStore::new(
            pool.clone(),
        )),
        Arc::new(mas_persistence::repositories::services::PgServices::new(
            pool,
        )),
        bridge,
        audit,
        limits,
    )
    .map_err(|error| format!("Data Hub composition: {error}"))?;
    Ok(Arc::new(service))
}

/// Reads an optional nonempty PEM bundle without ever printing its contents.
fn optional_pem(path: String, label: &str) -> Result<Option<Vec<u8>>, String> {
    if path.trim().is_empty() {
        return Ok(None);
    }
    let contents = std::fs::read(&path)
        .map_err(|error| format!("cannot read Data Hub {label} at {path}: {error}"))?;
    if contents.iter().all(u8::is_ascii_whitespace) {
        return Err(format!("configured Data Hub {label} at {path} is empty"));
    }
    Ok(Some(contents))
}

/// Builds the production OIDC verifier from the explicit deployment pins.
///
/// In-memory development deliberately keeps a static fixture verifier; any
/// durable API process must use TLS-pinned asymmetric JWT verification and
/// cannot silently downgrade to static tokens or symmetric IdP secrets.
fn compose_oidc_verifier(
    merger: &mas_common::config::Merger<'_>,
) -> Result<mas_api::oidc_auth::OidcJwksTokenVerifier, String> {
    use std::str::FromStr;
    use std::time::Duration;

    let issuer = merger.string("MAS_OIDC_ISSUER", "oidc.issuer", "");
    let jwks_url = merger.string("MAS_OIDC_JWKS_URL", "oidc.jwks_url", "");
    validate_production_oidc_url("OIDC issuer", &issuer)?;
    validate_production_oidc_url("OIDC JWKS URL", &jwks_url)?;

    let audience = merger.string("MAS_OIDC_AUDIENCE", "oidc.audience", "");
    let audience = (!audience.trim().is_empty()).then_some(audience);
    let algorithms = merger
        .string_list("MAS_OIDC_ALGS", "oidc.algs", &["RS256", "ES256"])
        .map_err(|error| format!("OIDC algorithms: {error}"))?
        .into_iter()
        .map(|raw| {
            mas_security::JwsAlgorithm::from_str(&raw)
                .map_err(|_| format!("OIDC algorithm '{raw}' is not supported"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let clock_skew_seconds = merger
        .u64("MAS_OIDC_CLOCK_SKEW_SECONDS", "oidc.clock_skew_seconds", 60)
        .map_err(|error| format!("OIDC clock skew: {error}"))?;
    let cache_ttl_seconds = merger
        .u64(
            "MAS_OIDC_JWKS_CACHE_TTL_SECONDS",
            "oidc.jwks_cache_ttl_seconds",
            300,
        )
        .map_err(|error| format!("JWKS cache TTL: {error}"))?;
    let timeout_ms = merger
        .u64(
            "MAS_OIDC_JWKS_TIMEOUT_MS",
            "oidc.jwks_request_timeout_ms",
            5_000,
        )
        .map_err(|error| format!("JWKS timeout: {error}"))?;
    if clock_skew_seconds > 300 {
        return Err("OIDC clock skew must not exceed 300 seconds".to_owned());
    }
    if !(1..=86_400).contains(&cache_ttl_seconds) {
        return Err("JWKS cache TTL must be between 1 and 86400 seconds".to_owned());
    }
    if !(1..=60_000).contains(&timeout_ms) {
        return Err("JWKS request timeout must be between 1 and 60000 milliseconds".to_owned());
    }
    let config = mas_security::JwksConfig::new(
        issuer,
        audience,
        algorithms,
        Duration::from_secs(clock_skew_seconds),
        Duration::from_secs(cache_ttl_seconds),
    )
    .map_err(|error| format!("OIDC configuration: {error}"))?;
    let fetcher = mas_api::oidc_auth::HttpJwksFetcher::new(Duration::from_millis(timeout_ms))
        .map_err(|error| format!("JWKS HTTP configuration: {error}"))?;
    let validator = mas_security::RemoteJwksValidator::new(jwks_url, config, Arc::new(fetcher))
        .map_err(|error| format!("JWKS verifier configuration: {error}"))?;
    Ok(mas_api::oidc_auth::OidcJwksTokenVerifier::new(validator))
}

/// Refuses insecure or ambiguous OIDC endpoints in the durable production
/// lane. Development's local HTTP IdP remains available only with the
/// explicit `--dev-inmemory` fixture mode, which never composes this verifier.
fn validate_production_oidc_url(label: &str, raw: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(raw)
        .map_err(|_| format!("{label} must be a valid absolute HTTPS URL"))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(format!(
            "{label} must be an HTTPS URL with a host and no credentials or fragment"
        ));
    }
    Ok(())
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

/// Readiness adapter for narrowly privileged worker pools. The check exposes
/// neither DSN nor database error detail: role/connection failures are useful
/// as a binary readiness signal but may contain deployment metadata.
struct PgPoolHealthCheck {
    pool: sqlx::PgPool,
    component: &'static str,
}

#[async_trait::async_trait]
impl mas_observability::health::HealthCheckPort for PgPoolHealthCheck {
    async fn check(
        &self,
        at: &mas_common::timestamps::Timestamp,
    ) -> mas_observability::health::ComponentHealth {
        let probe = sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await;
        match probe {
            Ok(1) => mas_observability::health::ComponentHealth::up(self.component, at)
                .unwrap_or_else(|_| unreachable!("static component name")),
            Ok(_) | Err(_) => mas_observability::health::ComponentHealth::down(
                self.component,
                at,
                "dedicated database role pool probe failed",
            )
            .unwrap_or_else(|_| unreachable!("static component name")),
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
