//! PURPOSE: The public billing provider webhook (Stripe) handler.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: accept one provider delivery, process it in the two
//! phases the billing service defines -- verify/claim/resolve pre-scope,
//! then apply inside the resolved tenant's authoritative scope -- and
//! acknowledge exactly what happened.
//!
//! Security boundary: this route is public because the provider calls it;
//! it is authenticated by the *signature over the raw body*, not by any
//! platform credential. The tenant is resolved from platform truth (the
//! customer mapping), never from the body.
//!
//! Data ownership: the billing service owns receipts and effects.
//!
//! Prohibited responsibilities: parsing decisions, SQL, secret logging.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::handlers::Ctx;
use crate::state::AppState;

/// `POST /webhooks/billing/stripe`.
pub async fn stripe_inbound(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let signature = headers
        .get("stripe-signature")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let Some(billing) = state.billing.clone() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "billing not configured").into_response();
    };
    let now = mas_common::timestamps::Timestamp::now()
        .to_unix_seconds()
        .max(0) as u64;

    let delivery = match billing.receive_stripe_event(&body, &signature, now).await {
        Ok(delivery) => delivery,
        Err(error) => {
            return crate::response::ApiError::new(error, &ctx).into_response();
        },
    };

    use mas_application::billing_service::StripeDelivery;
    match delivery {
        StripeDelivery::Rejected { reason } => {
            // An unauthentic or unprocessable delivery is refused, not
            // acknowledged: 403 tells the provider the signature must be
            // fixed (it will retry, then park the event for operator eyes).
            (
                StatusCode::FORBIDDEN,
                axum::Json(json!({ "status": "rejected", "reason": reason })),
            )
                .into_response()
        },
        StripeDelivery::Duplicate { event_id } => (
            StatusCode::OK,
            axum::Json(json!({ "status": "duplicate", "event_id": event_id })),
        )
            .into_response(),
        StripeDelivery::Resolved(resolved) => {
            let Some(pool) = state.runtime_pool.as_ref() else {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "billing scope not configured",
                )
                    .into_response();
            };
            let scope = mas_security::scope::AuthoritativeScope::resolved(
                resolved.owner.0,
                resolved.owner.1,
                "webhook:stripe",
            );
            let outcome = mas_persistence::scoped_request::with_authoritative_scope(
                pool,
                scope,
                move || async move { billing.apply_stripe_event(&resolved).await },
            )
            .await;
            match outcome {
                Ok(applied) => {
                    use mas_application::billing_service::StripeApplyOutcome;
                    let (status, effect) = match &applied {
                        StripeApplyOutcome::Applied { effect } => (StatusCode::OK, effect.clone()),
                        StripeApplyOutcome::AlreadyApplied => {
                            (StatusCode::OK, "already_applied".to_owned())
                        },
                        StripeApplyOutcome::Ignored { reason } => {
                            (StatusCode::OK, format!("ignored: {reason}"))
                        },
                    };
                    (
                        status,
                        axum::Json(json!({ "status": "applied", "effect": effect })),
                    )
                        .into_response()
                },
                Err(error) => crate::response::ApiError::new(error, &ctx).into_response(),
            }
        },
    }
}
