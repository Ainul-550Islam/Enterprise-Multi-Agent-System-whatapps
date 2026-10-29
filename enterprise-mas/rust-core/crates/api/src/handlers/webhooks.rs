//! PURPOSE: Thin tenant-authorized HTTP handlers for webhook delivery
//! inspection and dead-letter replay.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production responsibility: authenticate and authorize the resolved webhook
//! scope, call one application use case, and expose only redacted delivery
//! metadata.
//!
//! Data ownership: `WebhookService` and its persistence port own all delivery
//! state; this module owns no state.
//!
//! MUST NOT: read payload bodies, resolve signing keys, sign or dispatch HTTP,
//! or reveal another tenant's endpoint/delivery existence.

use axum::extract::{Path, Query, State};
use mas_common::ids::WebhookId;
use mas_contracts::webhook::{WebhookDeliveryDto, WebhookDeliveryPageDto, WebhookDeliveryQuery};

use crate::handlers::Ctx;
use crate::response::{ok, ApiError, ApiResult};
use crate::state::AppState;

/// `GET /v1/webhooks/{id}/deliveries`.
pub async fn deliveries(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(endpoint): Path<WebhookId>,
    Query(query): Query<WebhookDeliveryQuery>,
) -> ApiResult<WebhookDeliveryPageDto> {
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Webhook,
        mas_security::Action::Read,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .webhook_admin_service(&ctx)?
        .deliveries(
            &service_context,
            endpoint,
            query.cursor.as_deref(),
            query.page_limit(),
        )
        .await
        .map(|page| {
            ok(
                &ctx,
                WebhookDeliveryPageDto {
                    items: page
                        .items
                        .into_iter()
                        .map(|delivery| WebhookDeliveryDto {
                            id: delivery.id,
                            endpoint_id: delivery.endpoint_id,
                            event_id: delivery.event_id,
                            state: delivery.state,
                            attempt_count: delivery.attempt_count,
                            last_status_code: delivery.last_status_code,
                            next_attempt_at: delivery.next_attempt_at,
                            payload_hash: delivery.payload_hash,
                            last_error: delivery.last_error,
                        })
                        .collect(),
                    next_cursor: page.next_cursor,
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/webhooks/{id}/deliveries/{delivery_id}/replay`.
pub async fn replay(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path((endpoint, delivery_id)): Path<(WebhookId, uuid::Uuid)>,
) -> ApiResult<WebhookDeliveryDto> {
    let service_context = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Webhook,
        mas_security::Action::Publish,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .webhook_admin_service(&ctx)?
        .replay(&service_context, endpoint, delivery_id)
        .await
        .map(|delivery| {
            ok(
                &ctx,
                WebhookDeliveryDto {
                    id: delivery.id,
                    endpoint_id: delivery.endpoint_id,
                    event_id: delivery.event_id,
                    state: delivery.state,
                    attempt_count: delivery.attempt_count,
                    last_status_code: delivery.last_status_code,
                    next_attempt_at: delivery.next_attempt_at,
                    payload_hash: delivery.payload_hash,
                    last_error: delivery.last_error,
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}
