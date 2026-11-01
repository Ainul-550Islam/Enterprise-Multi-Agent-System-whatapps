//! PURPOSE: Authenticated endpoint-management and public signature-first HTTP
//! handlers for canonical inbound webhooks.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: expose tenant-authorized endpoint configuration as stable
//! DTOs and accept provider callbacks only through the composed ingress
//! verifier, which authenticates bytes before durable receipt/outbox processing.
//!
//! Security boundary: management routes require resolved authorization on the
//! webhook resource; the public callback route accepts no tenant identifier,
//! secret reference, or payload-derived authority. It extracts only bounded
//! headers/body and delegates all cryptographic and receipt decisions.
//!
//! Data ownership: application services own authorized mutations; the ingress
//! owns authentication; PostgreSQL owns endpoint/receipt/outbox state. This
//! handler owns only HTTP decoding and stable DTO conversion.
//!
//! Prohibited responsibilities: direct SQL, secret lookup, HMAC computation,
//! accepting unsigned input, returning payloads or tenant data, or treating a
//! callback as successful before the durable ingress result is returned.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use mas_common::error::AppError;
use mas_common::ids::InboundWebhookId;
use mas_common::timestamps::Timestamp;
use mas_contracts::inbound_webhook::{
    InboundWebhookDto, InboundWebhookIngestResponse, InboundWebhookPageDto, InboundWebhookQuery,
    RegisterInboundWebhookRequest,
};
use mas_integrations::inbound_webhooks::{
    InboundWebhookDisposition, InboundWebhookRequest, MAX_INBOUND_WEBHOOK_BYTES,
};
use mas_integrations::webhooks::SIGNATURE_HEADER;

use crate::handlers::Ctx;
use crate::response::{accepted, created, ok, ApiError, ApiResult};
use crate::state::AppState;

/// Header naming the provider's opaque idempotency/event identifier.
pub const INBOUND_EVENT_ID_HEADER: &str = "x-mas-event-id";
/// Optional non-negative provider sequence for deterministic ordered sources.
pub const INBOUND_SEQUENCE_HEADER: &str = "x-mas-sequence";

/// `POST /v1/inbound-webhooks`.
pub async fn register(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(request): Json<RegisterInboundWebhookRequest>,
) -> ApiResult<InboundWebhookDto> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_ctx = authorized(&ctx, mas_security::Action::Create)?;
    state
        .inbound_webhook_endpoint_service(&ctx)?
        .register(
            &service_ctx,
            mas_application::inbound_webhook_service::RegisterInboundWebhookEndpoint {
                source: request.source,
                signing_secret_reference_id: request.signing_secret_reference_id,
                required_key_version: request.required_key_version,
                accepted_key_versions: request.accepted_key_versions,
            },
        )
        .await
        .map(|endpoint| created(&ctx, endpoint_dto(&endpoint)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/inbound-webhooks`.
pub async fn list(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<InboundWebhookQuery>,
) -> ApiResult<InboundWebhookPageDto> {
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_ctx = authorized(&ctx, mas_security::Action::Read)?;
    state
        .inbound_webhook_endpoint_service(&ctx)?
        .list(&service_ctx, query.cursor.as_deref(), query.page_limit())
        .await
        .map(|page| {
            ok(
                &ctx,
                InboundWebhookPageDto {
                    items: page.items.iter().map(endpoint_dto).collect(),
                    next_cursor: page.next_cursor,
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/inbound-webhooks/{endpoint_id}`.
pub async fn get(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(endpoint_id): Path<InboundWebhookId>,
) -> ApiResult<InboundWebhookDto> {
    let service_ctx = authorized(&ctx, mas_security::Action::Read)?;
    state
        .inbound_webhook_endpoint_service(&ctx)?
        .get(&service_ctx, endpoint_id)
        .await
        .map(|endpoint| ok(&ctx, endpoint_dto(&endpoint)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/inbound-webhooks/{endpoint_id}/enable`.
pub async fn enable(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(endpoint_id): Path<InboundWebhookId>,
) -> ApiResult<InboundWebhookDto> {
    set_enabled(state, ctx, endpoint_id, true).await
}

/// `POST /v1/inbound-webhooks/{endpoint_id}/disable`.
pub async fn disable(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(endpoint_id): Path<InboundWebhookId>,
) -> ApiResult<InboundWebhookDto> {
    set_enabled(state, ctx, endpoint_id, false).await
}

async fn set_enabled(
    state: AppState,
    ctx: crate::context::RequestContext,
    endpoint_id: InboundWebhookId,
    enabled: bool,
) -> ApiResult<InboundWebhookDto> {
    let service_ctx = authorized(&ctx, mas_security::Action::Update)?;
    state
        .inbound_webhook_endpoint_service(&ctx)?
        .set_enabled(&service_ctx, endpoint_id, enabled)
        .await
        .map(|endpoint| ok(&ctx, endpoint_dto(&endpoint)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/public/inbound-webhooks/{route_id}`.
///
/// The path id selects only server-owned endpoint configuration. The public
/// caller cannot select a tenant, organization, source, secret, or callback
/// processing mode; cryptographic verification happens inside the ingress
/// before it reaches the receipt store.
pub async fn ingest(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(route_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<InboundWebhookIngestResponse> {
    if body.len() > MAX_INBOUND_WEBHOOK_BYTES {
        return Err(ApiError::new(
            AppError::invalid_field("body", "invalid_size", "inbound webhook body is too large"),
            &ctx,
        ));
    }
    let signature_header = required_header(&headers, SIGNATURE_HEADER, &ctx)?;
    let external_event_id = required_header(&headers, INBOUND_EVENT_ID_HEADER, &ctx)?;
    let sequence_no = optional_sequence_header(&headers, &ctx)?;
    let disposition = state
        .inbound_webhook_ingress(&ctx)?
        .ingest(
            InboundWebhookRequest {
                route_id,
                external_event_id,
                sequence_no,
                signature_header,
                body: body.to_vec(),
            },
            &Timestamp::now(),
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(accepted(
        &ctx,
        InboundWebhookIngestResponse {
            disposition: disposition_name(disposition).to_owned(),
        },
    ))
}

fn required_header(
    headers: &HeaderMap,
    name: &str,
    ctx: &crate::context::RequestContext,
) -> Result<String, ApiError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            ApiError::new(
                AppError::unauthorized("invalid inbound webhook authentication"),
                ctx,
            )
        })
}

fn optional_sequence_header(
    headers: &HeaderMap,
    ctx: &crate::context::RequestContext,
) -> Result<Option<i64>, ApiError> {
    let Some(value) = headers.get(INBOUND_SEQUENCE_HEADER) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .ok()
        .filter(|value| !value.is_empty() && value.len() <= 20)
        .ok_or_else(|| {
            ApiError::new(
                AppError::invalid_field(
                    "x-mas-sequence",
                    "invalid_format",
                    "inbound webhook sequence is invalid",
                ),
                ctx,
            )
        })?;
    value.parse::<i64>().map(Some).map_err(|_| {
        ApiError::new(
            AppError::invalid_field(
                "x-mas-sequence",
                "invalid_format",
                "inbound webhook sequence is invalid",
            ),
            ctx,
        )
    })
}

fn endpoint_dto(
    endpoint: &mas_integrations::inbound_webhooks::InboundWebhookEndpoint,
) -> InboundWebhookDto {
    InboundWebhookDto {
        id: endpoint.id,
        source: endpoint.source.clone(),
        has_signing_secret: true,
        required_key_version: endpoint.required_key_version.clone(),
        accepted_key_versions: endpoint.accepted_key_versions.clone(),
        enabled: endpoint.enabled,
        created_at: endpoint.created_at,
        updated_at: endpoint.updated_at,
    }
}

fn disposition_name(disposition: InboundWebhookDisposition) -> &'static str {
    match disposition {
        InboundWebhookDisposition::Accepted => "accepted",
        InboundWebhookDisposition::Deferred => "deferred",
        InboundWebhookDisposition::Duplicate => "duplicate",
    }
}

fn authorized(
    ctx: &crate::context::RequestContext,
    action: mas_security::Action,
) -> std::result::Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::Webhook,
        action,
        None,
    )
    .map_err(|error| ApiError::new(error, ctx))
}
