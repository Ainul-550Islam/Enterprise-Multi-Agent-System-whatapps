//! PURPOSE: Authenticated tenant-facing billing checkout handlers.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: open a checkout for a plan exactly once per idempotency
//! key, over the canonical billing service.
//!
//! Security boundary: behind resolved-scope authentication; the tenant comes
//! from the resolved scope, never from the body. Provider handles in the
//! response are the tenant's own.
//!
//! Data ownership: the billing service/PostgreSQL own checkout rows and
//! provider sessions. Handlers own only DTO conversion.
//!
//! Prohibited responsibilities: provider calls, SQL, secret handling.

use axum::extract::State;
use serde::{Deserialize, Serialize};

use crate::handlers::Ctx;
use crate::response::{ok, ApiError, ApiResult};
use crate::state::AppState;

/// `POST /v1/billing/checkout` body.
#[derive(Debug, Deserialize)]
pub struct CreateCheckoutRequest {
    /// The plan to buy.
    pub plan_id: uuid::Uuid,
    /// `one_time` or `subscription`.
    pub interval: String,
    /// Browser return URL on success.
    pub success_url: String,
    /// Browser return URL on cancellation.
    pub cancel_url: String,
    /// Caller-chosen idempotency key.
    pub idempotency_key: String,
    /// Contact email, when known.
    #[serde(default)]
    pub email: Option<String>,
}

/// `POST /v1/billing/checkout` response.
#[derive(Debug, Serialize)]
pub struct CheckoutCreatedDto {
    /// Platform checkout session id.
    pub id: uuid::Uuid,
    /// Provider checkout session handle.
    pub provider_session_id: Option<String>,
    /// Where to send the browser.
    pub checkout_url: String,
    /// Lifecycle state.
    pub state: String,
}

/// `POST /v1/billing/checkout`.
pub async fn create(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    axum::Json(request): axum::Json<CreateCheckoutRequest>,
) -> ApiResult<CheckoutCreatedDto> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let interval: mas_domain::billing::BillingInterval = request
        .interval
        .parse()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let billing = state.billing_service(&ctx)?;
    let created = billing
        .create_checkout_session(
            scope.tenant_id(),
            mas_common::ids::PlanId::from_uuid(request.plan_id),
            interval,
            request.email.as_deref(),
            &request.success_url,
            &request.cancel_url,
            &request.idempotency_key,
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(
        &ctx,
        CheckoutCreatedDto {
            id: created.record.id.into_uuid(),
            provider_session_id: created.record.provider_session_id,
            checkout_url: created.checkout_url,
            state: created.record.state.as_str().to_owned(),
        },
    ))
}

/// `POST /v1/billing/portal-session` body.
#[derive(Debug, Deserialize)]
pub struct CreatePortalRequest {
    /// Browser return URL when they leave the portal.
    pub return_url: String,
    /// Caller-chosen idempotency key.
    pub idempotency_key: String,
}

/// `POST /v1/billing/portal-session` response.
#[derive(Debug, Serialize)]
pub struct PortalCreatedDto {
    /// Where to send the browser.
    pub portal_url: String,
}

/// `POST /v1/billing/portal-session`.
pub async fn create_portal(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    axum::Json(request): axum::Json<CreatePortalRequest>,
) -> ApiResult<PortalCreatedDto> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let billing = state.billing_service(&ctx)?;
    let created = billing
        .create_portal_session(
            scope.tenant_id(),
            &request.return_url,
            &request.idempotency_key,
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(
        &ctx,
        PortalCreatedDto {
            portal_url: created.portal_url,
        },
    ))
}
