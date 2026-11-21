//! Stripe webhook route contract: unsigned deliveries are refused, the
//! public lane is signature-authenticated (not bearer), and the in-memory
//! test composition answers a deterministic 503 when billing is unwired.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use mas_api::{router::router, state::test_support::inmemory_state};
use serde_json::Value;
use tower::ServiceExt;

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
    body: Body,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(builder.body(body).expect("request"))
        .await
        .expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn billing_checkout_rejects_unauthenticated_requests() {
    let app = router(inmemory_state());
    for path in [
        "/v1/billing/checkout",
        "/v1/billing/checkout-session",
        "/v1/billing/portal-session",
    ] {
        let (status, _) = call(&app, "POST", path, &[], Body::empty()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}");
    }
}

#[tokio::test]
async fn stripe_webhook_without_a_wired_service_is_unavailable() {
    let app = router(inmemory_state());
    let (status, _) = call(
        &app,
        "POST",
        "/webhooks/billing/stripe",
        &[("stripe-signature", "t=1,v1=00")],
        Body::from("{}"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn stripe_webhook_is_not_a_bearer_route() {
    let app = router(inmemory_state());
    let (status, _) = call(
        &app,
        "POST",
        "/webhooks/billing/stripe",
        &[("authorization", "Bearer dev-token")],
        Body::from("{}"),
    )
    .await;
    // Unwired → 503; a bearer token must not turn this into 401/200.
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
