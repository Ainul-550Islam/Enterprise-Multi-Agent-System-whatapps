//! WhatsApp route contract at the HTTP boundary: authentication, the scope
//! requirement of the channel roundtrip, and the separation between the
//! public signature-authenticated webhook lane and the bearer lane.
//!
//! The in-memory test lane has no WhatsApp service configured, so the public
//! lane answers a deterministic 503 — that answer is asserted (it is the
//! honest fail-closed behaviour), not papered over.

use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use http_body_util::BodyExt;
use mas_api::{router::router, state::test_support::inmemory_state};
use serde_json::Value;
use tower::ServiceExt;

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Body,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
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
async fn whatsapp_surfaces_reject_unauthenticated_requests() {
    let app = router(inmemory_state());
    let paths = [
        ("POST", "/v1/whatsapp/channels"),
        ("GET", "/v1/whatsapp/channels"),
        (
            "POST",
            "/v1/whatsapp/channels/00000000-0000-0000-0000-000000000000/messages",
        ),
        ("GET", "/v1/whatsapp/conversations"),
        (
            "GET",
            "/v1/whatsapp/conversations/00000000-0000-0000-0000-000000000000/messages",
        ),
        (
            "POST",
            "/v1/whatsapp/conversations/00000000-0000-0000-0000-000000000000/messages",
        ),
        ("GET", "/v1/whatsapp/templates"),
        ("POST", "/v1/whatsapp/templates"),
        ("GET", "/v1/whatsapp/stats"),
        ("PUT", "/v1/whatsapp/messages/wamid.TEST/status"),
    ];
    for (method, path) in paths {
        let (status, _) = call(&app, method, path, None, Body::empty()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
    }
}

#[tokio::test]
async fn channel_roundtrip_requires_scope() {
    let app = router(inmemory_state());
    // authenticated, but the test principal has no resolved tenant scope:
    // the channel roundtrip must refuse before touching any store
    let body = Body::from(
        serde_json::to_vec(&serde_json::json!({
            "display_name": "support line",
            "phone_number_id": "PN-1",
            "waba_id": "WABA-1",
            "api_version": "v21.0",
            "verify_token_ref": "ref://whatsapp/verify",
        }))
        .expect("body"),
    );
    let (status, envelope) = call(
        &app,
        "POST",
        "/v1/whatsapp/channels",
        Some("test-token"),
        body,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unscoped register: {envelope}"
    );
    let code = envelope
        .pointer("/error/code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert_eq!(code, "VALIDATION_FAILED", "envelope: {envelope}");
    // the refusal names the missing scope header — actionable, local, fail-closed
    let issues = envelope
        .pointer("/error/details/issues")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert!(
        issues
            .iter()
            .any(|i| i.get("field").and_then(Value::as_str) == Some("x-tenant-id")),
        "refusal must name x-tenant-id: {envelope}"
    );

    // the list side of the roundtrip is guarded identically
    let (status, _) = call(
        &app,
        "GET",
        "/v1/whatsapp/channels",
        Some("test-token"),
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn public_webhook_lane_fails_closed_without_a_configured_service() {
    let app = router(inmemory_state());
    // GET handshake: unconfigured deployment answers 503, never a fake 200
    let (status, _) = call(
        &app,
        "GET",
        "/webhooks/whatsapp?hub.mode=subscribe&hub.verify_token=t&hub.challenge=42",
        None,
        Body::empty(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    // POST inbound: same fail-closed answer without a configured service
    let (status, _) = call(&app, "POST", "/webhooks/whatsapp", None, Body::from("{}")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
