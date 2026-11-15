/// Protected billing checkout session handler.
///
/// POST /v1/billing/checkout-session
/// Body: { plan_code, success_url, cancel_url }
/// Requires billing-admin role + authentication.
use crate::errors::ChannelError;
use crate::application::billing_service::BillingService;
use crate::config::StripeConfig;
use serde::Deserialize;
use tracing::instrument;

/// Request body for creating a checkout session.
#[derive(Debug, Deserialize)]
pub struct CheckoutSessionRequest {
    /// Internal plan code (e.g. "free", "pro", "business")
    pub plan_code: String,
    /// URL to redirect after successful payment
    pub success_url: String,
    /// URL to redirect after cancellation
    pub cancel_url: String,
}

/// Response from creating a checkout session.
#[derive(Debug, Serialize)]
pub struct CheckoutSessionResponse {
    /// Stripe session ID
    pub session_id: String,
    /// URL to redirect the customer to
    pub session_url: String,
}

/// Create a checkout session for a tenant.
#[instrument(skip(service, config))]
pub async fn create_checkout_session(
    service: &mut BillingService,
    config: &StripeConfig,
    _req: web::Json<CheckoutSessionRequest>,
) -> Result<web::Json<CheckoutSessionResponse>, warp::reject::Rejection> {
    // In a full implementation, we'd extract the tenant_id from the auth context
    // and the customer ID from the customer record.
    // For now, we use placeholders and log the intent.

    // Create a placeholder tenant UUID
    let tenant_id = uuid::Uuid::new_v4();

    // Create the checkout session
    let (session_id, session_url) = service.create_checkout_session(
        // pool would be injected here; simplified for protocol-only crate
        &tenant_id,
        &_req.plan_code,
        &_req.success_url,
        &_req.cancel_url,
    )
    .await
    .map_err(|e| {
        warp::reject::reject::custom(warp::reject::custom::custom(format!(
            "checkout session error: {:?}",
            e
        )))
    })?;

    Ok(web::Json(CheckoutSessionResponse {
        session_id,
        session_url,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkout_request_deserialization() {
        let json = r#"{
            "plan_code": "pro",
            "success_url": "https://example.com/success",
            "cancel_url": "https://example.com/cancel"
        }"#;

        let req = serde_json::from_str::<CheckoutSessionRequest>(json);
        assert!(req.is_ok());
        let req = req.unwrap();
        assert_eq!(req.plan_code, "pro");
        assert_eq!(req.success_url, "https://example.com/success");
        assert_eq!(req.cancel_url, "https://example.com/cancel");
    }
}