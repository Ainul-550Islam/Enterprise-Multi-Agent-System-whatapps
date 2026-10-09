//! PURPOSE: Authorized HTTP handlers for the canonical tenant connector and
//! outbound-webhook management plane.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: translate stable public DTOs into the single
//! `IntegrationService`, preserve resolved tenant/organization authority, and
//! expose no secret/reference material in responses. Connectivity tests invoke
//! real guarded provider probes through the application service; they never
//! synthesize success.
//!
//! Security boundary: every route requires authenticated resolved scope and a
//! canonical resource/action decision before a service context is created.
//! Path IDs are identifiers only, never tenant authority. The service and
//! request-scoped adapters hide foreign resources as not found.
//!
//! Data ownership: integration application/persistence layers own connector,
//! endpoint, delivery and audit facts. This file owns only DTO validation and
//! response conversion.
//!
//! Prohibited responsibilities: direct SQL, secret access, credential/OAuth
//! lifecycle, egress-policy construction, HTTP dispatch, pagination by unsafe
//! offset, or fallback/in-memory connector behavior.

use std::collections::BTreeSet;

use axum::extract::{Path, Query, State};
use axum::Json;
use mas_application::integration_service::{RegisterConnector, RegisterWebhookEndpoint};
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, WebhookId};
use mas_contracts::connector::{
    ConnectorListQuery, ConnectorPageResponse, ConnectorResponse, ConnectorStatusResponse,
    RegisterConnectorRequest, TestConnectorRequest,
};
use mas_contracts::webhook::{
    RegisterWebhookRequest, WebhookDto, WebhookListQuery, WebhookPageDto,
};

use crate::handlers::Ctx;
use crate::response::{created, ok, ApiError, ApiResult};
use crate::state::AppState;

/// `POST /v1/connectors`.
pub async fn register_connector(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(request): Json<RegisterConnectorRequest>,
) -> ApiResult<ConnectorResponse> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorized_connector(&ctx, mas_security::Action::Create)?;
    let auth_mode = request.auth_mode.parse().map_err(|_| {
        ApiError::new(
            AppError::invalid_field(
                "auth_mode",
                "invalid_enum_value",
                "connector auth mode is invalid",
            ),
            &ctx,
        )
    })?;
    state
        .integration_service(&ctx)?
        .register_connector(
            &service_context,
            RegisterConnector {
                provider: request.provider,
                name: request.name,
                display_name: request.display_name,
                auth_mode,
                config: request.config,
            },
        )
        .await
        .map(|connector| created(&ctx, connector_dto(&connector)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/connectors`.
pub async fn list_connectors(
    State(state): State<AppState>,
    Query(query): Query<ConnectorListQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<ConnectorPageResponse> {
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorized_connector(&ctx, mas_security::Action::Read)?;
    state
        .integration_service(&ctx)?
        .list_connectors_page(
            &service_context,
            query.cursor.as_deref(),
            query.page_limit(),
        )
        .await
        .map(|page| {
            ok(
                &ctx,
                ConnectorPageResponse {
                    items: page.items.iter().map(connector_dto).collect(),
                    next_cursor: page.next_cursor,
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/connectors/{connector_id}`.
pub async fn get_connector(
    State(state): State<AppState>,
    Path(connector_id): Path<ConnectorId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ConnectorResponse> {
    let service_context = authorized_connector(&ctx, mas_security::Action::Read)?;
    state
        .integration_service(&ctx)?
        .get_connector(&service_context, connector_id)
        .await
        .map(|connector| ok(&ctx, connector_dto(&connector)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/connectors/test`.
pub async fn test_connector(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(request): Json<TestConnectorRequest>,
) -> ApiResult<ConnectorStatusResponse> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorized_connector(&ctx, mas_security::Action::Update)?;
    let connector = state
        .integration_service(&ctx)?
        .test_connector(&service_context, request.connector_id)
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    let tested_at = connector.last_tested_at.ok_or_else(|| {
        ApiError::new(
            AppError::internal("connector test completed without a test timestamp"),
            &ctx,
        )
    })?;
    Ok(ok(
        &ctx,
        ConnectorStatusResponse {
            connector_id: connector.id,
            status: connector.status,
            ok: connector.is_usable(),
            message: connector.last_error,
            latency_ms: None,
            tested_at,
        },
    ))
}

/// `POST /v1/webhooks`.
pub async fn register_webhook(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(request): Json<RegisterWebhookRequest>,
) -> ApiResult<WebhookDto> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorized_webhook(&ctx, mas_security::Action::Create)?;
    state
        .integration_service(&ctx)?
        .register_webhook(
            &service_context,
            RegisterWebhookEndpoint {
                url: request.url,
                subscribed_events: request
                    .subscribed_events
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                secret_reference_id: request.secret_reference_id,
                // The endpoint must capture only the canonical version stored
                // on the tenant-owned reference; public callers never choose a
                // signing version independently.
                signing_key_version: None,
                description: request.description,
            },
        )
        .await
        .map(|endpoint| created(&ctx, webhook_dto(&endpoint)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/webhooks`.
pub async fn list_webhooks(
    State(state): State<AppState>,
    Query(query): Query<WebhookListQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<WebhookPageDto> {
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorized_webhook(&ctx, mas_security::Action::Read)?;
    state
        .integration_service(&ctx)?
        .list_webhooks_page(
            &service_context,
            query.cursor.as_deref(),
            query.page_limit(),
        )
        .await
        .map(|page| {
            ok(
                &ctx,
                WebhookPageDto {
                    items: page.items.iter().map(webhook_dto).collect(),
                    next_cursor: page.next_cursor,
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/webhooks/{webhook_id}/enable`.
pub async fn enable_webhook(
    State(state): State<AppState>,
    Path(webhook_id): Path<WebhookId>,
    Ctx(ctx): Ctx,
) -> ApiResult<WebhookDto> {
    set_webhook_enabled(state, ctx, webhook_id, true).await
}

/// `POST /v1/webhooks/{webhook_id}/disable`.
pub async fn disable_webhook(
    State(state): State<AppState>,
    Path(webhook_id): Path<WebhookId>,
    Ctx(ctx): Ctx,
) -> ApiResult<WebhookDto> {
    set_webhook_enabled(state, ctx, webhook_id, false).await
}

async fn set_webhook_enabled(
    state: AppState,
    ctx: crate::context::RequestContext,
    webhook_id: WebhookId,
    enabled: bool,
) -> ApiResult<WebhookDto> {
    let service_context = authorized_webhook(&ctx, mas_security::Action::Update)?;
    state
        .integration_service(&ctx)?
        .set_webhook_enabled(&service_context, webhook_id, enabled)
        .await
        .map(|endpoint| ok(&ctx, webhook_dto(&endpoint)))
        .map_err(|error| ApiError::new(error, &ctx))
}

fn connector_dto(connector: &mas_domain::Connector) -> ConnectorResponse {
    ConnectorResponse {
        id: connector.id,
        tenant_id: connector.tenant_id,
        provider: connector.provider.clone(),
        name: connector.name.clone(),
        display_name: connector.display_name.clone(),
        auth_mode: connector.auth_mode.to_string(),
        status: connector.status,
        has_credential: connector.credential_id.is_some(),
        last_tested_at: connector.last_tested_at,
        last_error: connector.last_error.clone(),
        created_at: connector.created_at,
        updated_at: connector.updated_at,
    }
}

fn webhook_dto(endpoint: &mas_domain::WebhookEndpoint) -> WebhookDto {
    WebhookDto {
        id: endpoint.id,
        url: endpoint.url.as_str().to_owned(),
        subscribed_events: endpoint.subscribed_events.iter().cloned().collect(),
        enabled: endpoint.enabled,
        has_signing_secret: true,
        consecutive_failures: endpoint.consecutive_failures,
        created_at: endpoint.created_at,
        updated_at: endpoint.updated_at,
    }
}

fn authorized_connector(
    ctx: &crate::context::RequestContext,
    action: mas_security::Action,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::Connector,
        action,
        None,
    )
    .map_err(|error| ApiError::new(error, ctx))
}

fn authorized_webhook(
    ctx: &crate::context::RequestContext,
    action: mas_security::Action,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::Webhook,
        action,
        None,
    )
    .map_err(|error| ApiError::new(error, ctx))
}
