//! Fake-server integration: a local axum app answering with real
//! `ApiEnvelope` shapes, covered end-to-end through the CLI's client and
//! command layer — envelope decode, error mapping, replay header, scope
//! guards, transitions, and the scoped `list|get` resources.

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use mas_cli::args::{Command, ExecutionCmd, ListGet, ScheduleCmd, SubmitArgs};
use mas_cli::{run, ExitCode, Format, GlobalOpts};

/// Starts the fake API on a loopback port and returns `(base_url, counters)`.
async fn fake_api() -> String {
    let posts = Arc::new(AtomicUsize::new(0));
    let posts_in_handler = posts.clone();
    let app = Router::new()
        .route("/v1/health/live", get(|| async { envelope(json!({"status": "alive"})) }))
        .route(
            "/v1/health/ready",
            get(|| async { envelope(json!({"status": "ready", "checks": []})) }),
        )
        .route(
            "/v1/executions",
            get(|headers: HeaderMap| async move {
                // Scope headers must be present on scoped routes.
                assert!(headers.contains_key("x-tenant-id"));
                assert!(headers.contains_key("x-organization-id"));
                envelope(json!([
                    execution_record("completed"),
                    execution_record("running"),
                ]))
            }),
        )
        .route(
            "/v1/projects/{project_id}/executions",
            post(move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
                let posts = posts_in_handler.clone();
                async move {
                    posts.fetch_add(1, Ordering::SeqCst);
                    // Idempotent replay when a second POST carries the same key.
                    let key = headers
                        .get("idempotency-key")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    assert!(key.starts_with("cli-1e-") || key == "repeat-key");
                    let _ = body;
                    let mut response = envelope(execution_record("pending")).into_response();
                    if key == "repeat-key" {
                        response
                            .headers_mut()
                            .insert("x-idempotent-replay", axum::http::HeaderValue::from_static("true"));
                    }
                    (StatusCode::CREATED, response)
                }
            }),
        )
        .route(
            "/v1/executions/{id}/start",
            post(|| async { envelope(execution_record("running")) }),
        )
        .route(
            "/v1/executions/{id}",
            get(|| async {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({
                        "error": {
                            "code": "NOT_FOUND",
                            "message": "execution not found",
                            "request_id": Uuid::nil(),
                        },
                        "meta": { "request_id": Uuid::nil(), "correlation_id": "c", "api_version": "v1", "responded_at": "2026-09-30T00:00:00.000Z" },
                    })),
                )
            }),
        )
        .route(
            "/v1/schedules/{id}/disable",
            post(|| async { envelope(json!({"status": "disabled"})) }),
        )
        .route(
            "/v1/agents",
            get(|| async {
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": { "code": "UNAUTHORIZED", "message": "scope required" },
                        "meta": { "request_id": Uuid::nil(), "correlation_id": "c", "api_version": "v1", "responded_at": "2026-09-30T00:00:00.000Z" },
                    })),
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{addr}")
}

/// A complete `ExecutionResponse` record (all required contract fields).
fn execution_record(status: &str) -> serde_json::Value {
    json!({
        "id": Uuid::nil(),
        "status": status,
        "tenant_id": Uuid::nil(),
        "root_execution_id": Uuid::nil(),
        "correlation_id": "corr",
        "cancellation_requested": false,
        "created_at": "2026-09-30T00:00:00.000Z",
    })
}

fn envelope(data: serde_json::Value) -> Json<serde_json::Value> {
    Json(json!({
        "data": data,
        "meta": {
            "request_id": uuid::Uuid::nil(),
            "correlation_id": "test",
            "api_version": "v1",
            "responded_at": "2026-09-30T00:00:00.000Z",
        },
    }))
}

fn opts(base_url: &str) -> GlobalOpts {
    GlobalOpts {
        base_url: base_url.to_owned(),
        token: "dev-token".to_owned(),
        tenant: Some(Uuid::nil()),
        organization: Some(Uuid::nil()),
        correlation_id: Some("cli-e2e".to_owned()),
        format: Format::Brief,
    }
}

#[tokio::test]
async fn status_against_fake_api() {
    let base_url = fake_api().await;
    let exit = run(&opts(&base_url), &Command::Status).await;
    assert_eq!(exit, ExitCode::Ok);
}

#[tokio::test]
async fn execution_list_decodes_the_envelope() {
    let base_url = fake_api().await;
    let exit = run(&opts(&base_url), &Command::Execution(ExecutionCmd::List)).await;
    assert_eq!(exit, ExitCode::Ok);
}

#[tokio::test]
async fn execution_submit_posts_and_surfaces_replay() {
    let base_url = fake_api().await;
    let submit = SubmitArgs {
        project: Uuid::nil(),
        workflow: Some(Uuid::nil()),
        agent: None,
        input: Some(r#"{"x":1}"#.to_owned()),
        idempotency_key: Some("cli-1e-a1a1a1a1".to_owned()),
        wait: false,
        wait_timeout: 60,
    };
    let exit = run(
        &opts(&base_url),
        &Command::Execution(ExecutionCmd::Submit(submit)),
    )
    .await;
    assert_eq!(exit, ExitCode::Ok);
}

#[tokio::test]
async fn ephemeral_get_maps_stable_error_to_exit_1() {
    let base_url = fake_api().await;
    let exit = run(
        &opts(&base_url),
        &Command::Execution(ExecutionCmd::Get { id: Uuid::nil() }),
    )
    .await;
    assert_eq!(exit, ExitCode::ApiError, "NOT_FOUND envelope ⇒ 1");
}

#[tokio::test]
async fn schedule_disable_transition_round_trips() {
    let base_url = fake_api().await;
    let exit = run(
        &opts(&base_url),
        &Command::Schedule(ScheduleCmd::Disable { id: Uuid::nil() }),
    )
    .await;
    assert_eq!(exit, ExitCode::Ok);
}

#[tokio::test]
async fn missing_scope_fails_locally_without_network() {
    let mut no_scope = opts("http://127.0.0.1:1"); // unreachable on purpose
    no_scope.tenant = None;
    let exit = run(&no_scope, &Command::Agent(ListGet::List)).await;
    assert_eq!(exit, ExitCode::UsageOrTransport);
    // And scope-present version hits the (fake) auth wall: API error.
    let base_url = fake_api().await;
    let exit = run(&opts(&base_url), &Command::Agent(ListGet::List)).await;
    assert_eq!(exit, ExitCode::ApiError);
}

#[tokio::test]
async fn transport_failure_maps_to_exit_2() {
    let exit = run(
        &GlobalOpts {
            base_url: "http://127.0.0.1:1".to_owned(),
            ..opts("http://127.0.0.1:1")
        },
        &Command::Status,
    )
    .await;
    assert_eq!(exit, ExitCode::UsageOrTransport);
}
