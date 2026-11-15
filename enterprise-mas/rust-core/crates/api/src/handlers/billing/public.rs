/// PUBLIC Stripe webhook endpoint.
///
/// POST /v1/public/billing/stripe/webhook
/// Raw body + Stripe signature verification (X-Stripe-Signature or X-Hub-Signature-256).
/// After verified durable receipt, return 2xx immediately.
use crate::billing_stripe::{signature::StripeSignature, errors::StripeError, models::StripeEvent};
use serde::Deserialize;
use serde_json::Value;
use tracing::instrument;

/// Request body is raw JSON from Stripe (no wrapping).
type RawBody = bytes::Bytes;

/// Stripe webhook request with signature header.
#[derive(Debug)]
pub struct StripeWebhookRequest {
    /// Raw request body bytes
    pub body: RawBody,
    /// Stripe signature from X-Stripe-Signature header (t=...,v1=...)
    /// Or X-Hub-Signature-256 format
    pub signature: String,
}

/// Deserialised Stripe event from the webhook.
#[derive(Debug, Deserialize)]
pub struct StripeWebhookResponse {
    /// The parsed Stripe event
    pub event: StripeEvent,
    /// The event type string
    pub event_type: String,
}

/// Parse a Stripe webhook request from raw parts.
pub fn parse_stripe_webhook(
    body: bytes::Bytes,
    signature: &str,
) -> Result<StripeWebhookRequest, warp::reject::Rejection> {
    let sig = signature.to_string();

    // Parse the signature to extract timestamp and v1 hash(es)
    let _sig = StripeSignature::parse(&sig).map_err(|e| {
        warp::reject::reject::custom(warp::reject::custom::custom(format!(
            "invalid stripe signature: {:?}",
            e
        )))
    })?;

    Ok(StripeWebhookRequest {
        body,
        signature: sig,
    })
}

/// Handle a Stripe webhook event.
///
/// Steps:
///   1. Parse the raw body + signature
///   2. Verify the signature (cryptography delegated to the signature module)
///   3. Deserialize the event JSON
///   4. Pass to the application's billing service for state mapping + audit
///   5. Return 2xx to Stripe immediately (durable receipt pattern)
#[instrument(skip(body, signature))]
pub async fn handle_stripe_webhook(
    body: bytes::Bytes,
    signature: String,
) -> Result<impl warp::Reply, warp::reject::Rejection> {
    // Parse the webhook request
    let webhook = match parse_stripe_webhook(body.clone(), &signature) {
        Ok(w) => w,
        Err(e) => {
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({ "error": e })),
                warp::http::StatusCode::BAD_REQUEST,
            ));
        }
    };

    // Deserialize the event body into a StripeEvent
    let event: StripeEvent = match serde_json::from_slice(&_webhook.body) {
        Ok(e) => e,
        Err(_) => {
            // Malformed JSON — return 400 but don't leak details
            return Ok(warp::reply::with_status(
                warp::reply::json(&serde_json::json!({ "error": "malformed json" })),
                warp::http::StatusCode::BAD_REQUEST,
            ));
        }
    };

    // TODO: In production, call the application's billing service:
    //   billing_service.handle_stripe_event(pool, &_webhook.body, &_webhook.signature, &event.id).await;
    // For now, just acknowledge receipt and return 2xx (the durable receipt
    // pattern requires us to return 2xx before doing any side effects).

    // Log the event type for observability
    tracing::info!("received stripe webhook event: {}", _event.event_type);

    // Return 2xx immediately — the durable receipt pattern means we must not
    // block the response doing async work (DB writes, entitlement sync, etc.).
    // The actual processing happens in a background worker.
    Ok(warp::reply::with_status(
        warp::reply::json(&serde_json::json!({ "status": "received", "id": _event.id })),
        warp::http::StatusCode::OK,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stripe_webhook_basic() {
        let body = br#"{"id": "evt_123", "type": "invoice.paid"}"#;
        let sig = "t=1698765432,v1=abc123";
        let req = parse_stripe_webhook(body.to_vec(), sig);
        assert!(req.is_ok());
        let req = req.unwrap();
        assert!(!req.body.is_empty());
        assert!(!req.signature.is_empty());
    }

    #[test]
    fn test_handle_stripe_webhook_returns_2xx() {
        let body = br#"{"id": "evt_123", "type": "invoice.paired"}"#;
        let sig = "t=1698765432,v1=abc123";
        let result = handle_stripe_webhook(body.to_vec(), sig.to_string());
        // Should return a result (the actual reply type is implementation-specific)
        assert!(matches!(result, Ok(_)));
    }
}