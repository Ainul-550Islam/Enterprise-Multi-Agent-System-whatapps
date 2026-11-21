//! PURPOSE: The public WhatsApp webhook handler (verification handshake and
//! inbound message deliveries).
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: answer the provider's subscription check and process
//! inbound deliveries in the two phases the WhatsApp service defines --
//! verify the signature over the raw body and resolve the tenant pre-scope,
//! then record inside the resolved tenant's authoritative scope.
//!
//! Security boundary: this route is public because the provider calls it;
//! it is authenticated by the `X-Hub-Signature-256` over the raw body, and
//! the verify token is compared in constant time. The tenant is resolved
//! from the channel's phone-number handle through platform truth, never
//! from the body.
//!
//! Data ownership: the WhatsApp service owns the message log.
//!
//! Prohibited responsibilities: parsing decisions, SQL, secret logging.

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::handlers::Ctx;
use crate::state::AppState;

/// `GET /webhooks/whatsapp` query (the provider's handshake).
#[derive(Debug, Deserialize)]
pub struct VerifyQuery {
    /// `hub.mode` -- must be `subscribe`.
    #[serde(rename = "hub.mode")]
    pub hub_mode: Option<String>,
    /// `hub.verify_token` -- compared in constant time.
    #[serde(rename = "hub.verify_token")]
    pub hub_verify_token: Option<String>,
    /// `hub.challenge` -- echoed back on success.
    #[serde(rename = "hub.challenge")]
    pub hub_challenge: Option<String>,
}

/// `GET /webhooks/whatsapp`.
pub async fn verify(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<VerifyQuery>,
) -> Response {
    let Some(service) = state.whatsapp.clone() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "whatsapp not configured").into_response();
    };
    match service.verify_subscription(
        query.hub_mode.as_deref(),
        query.hub_verify_token.as_deref(),
        query.hub_challenge.as_deref(),
    ) {
        Ok(challenge) => (StatusCode::OK, challenge).into_response(),
        Err(error) => crate::response::ApiError::new(error, &ctx).into_response(),
    }
}

/// `POST /webhooks/whatsapp`.
pub async fn inbound(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let signature = headers
        .get("x-hub-signature-256")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let Some(service) = state.whatsapp.clone() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "whatsapp not configured").into_response();
    };

    let delivery = match service.receive_inbound(&body, &signature).await {
        Ok(delivery) => delivery,
        Err(error) => {
            return crate::response::ApiError::new(error, &ctx).into_response();
        },
    };

    use mas_application::whatsapp_service::WhatsAppDelivery;
    match delivery {
        WhatsAppDelivery::Rejected { reason } => {
            // A delivery that does not verify, or names no channel this
            // platform owns, is refused. An unverifiable delivery must not
            // be acknowledged at all.
            if reason.contains("signature") {
                return (
                    StatusCode::FORBIDDEN,
                    axum::Json(json!({ "status": "rejected", "reason": reason })),
                )
                    .into_response();
            }
            (
                StatusCode::OK,
                axum::Json(json!({ "status": "ignored", "reason": reason })),
            )
                .into_response()
        },
        WhatsAppDelivery::Resolved(resolved) => {
            let Some(pool) = state.runtime_pool.as_ref() else {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "whatsapp scope not configured",
                )
                    .into_response();
            };
            let scope = mas_security::scope::AuthoritativeScope::resolved(
                resolved.owner.0,
                resolved.owner.1,
                "webhook:whatsapp",
            );
            let outcome = mas_persistence::scoped_request::with_authoritative_scope(
                pool,
                scope,
                move || async move { service.apply_inbound(&resolved).await },
            )
            .await;
            match outcome {
                Ok(recorded) => (
                    StatusCode::OK,
                    axum::Json(json!({ "status": "applied", "messages": recorded.len() })),
                )
                    .into_response(),
                Err(error) => crate::response::ApiError::new(error, &ctx).into_response(),
            }
        },
    }
}
