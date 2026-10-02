//! The small HTTP surface this service owns:
//!
//! ```text
//! GET  /healthz                       liveness (always 200 "ok")
//! GET  /v1/scheduler/stats            in-flight ledger depth
//! POST /v1/scheduler/completions      downstream run finished → drop ledger entry
//! ```
//!
//! The completion route is the second half of the overlap protocol (see
//! [`crate::dispatch`]): executors (or the API relaying their lifecycle
//! events) POST a [`CompletionNotice`] here so `is_running` flips to false
//! and the next tick may fire the schedule again.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::dispatch::{CompletionNotice, InFlightLedger};

/// Liveness probe. No state — anything holding this router is alive.
pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { "ok" }))
}

/// Stateful routes (ledger-backed): stats + completions.
pub fn completion_router(ledger: InFlightLedger) -> Router {
    Router::new()
        .route("/v1/scheduler/completions", post(post_completion))
        .route("/v1/scheduler/stats", get(get_stats))
        .with_state(ledger)
}

async fn post_completion(
    State(ledger): State<InFlightLedger>,
    Json(notice): Json<CompletionNotice>,
) -> StatusCode {
    let _was_running = ledger.is_running(notice.schedule_id);
    ledger.finish(notice.schedule_id);
    tracing::info!(
        schedule = %notice.schedule_id,
        execution = %notice.execution_id,
        outcome = %notice.outcome,
        "run completion acknowledged",
    );
    // Idempotent: completing an unknown or already-finished schedule is
    // still a success (at-least-once delivery downstream).
    StatusCode::NO_CONTENT
}

async fn get_stats(State(ledger): State<InFlightLedger>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "in_flight": ledger.len(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::InFlightLedger;
    use mas_common::ids::{ExecutionId, ScheduleId};
    use tower::ServiceExt; // oneshot

    #[tokio::test]
    async fn completion_clears_the_ledger_and_is_idempotent() {
        let ledger = InFlightLedger::default();
        let schedule = ScheduleId::new();
        ledger.start(schedule);
        assert!(ledger.is_running(schedule));

        let router = completion_router(ledger.clone());
        let notice = CompletionNotice {
            schedule_id: schedule,
            execution_id: ExecutionId::new(),
            outcome: "succeeded".to_owned(),
        };
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::post("/v1/scheduler/completions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&notice).expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(!ledger.is_running(schedule), "ledger cleared by the route");

        // Replay → still 204.
        let response = router
            .oneshot(
                axum::http::Request::post("/v1/scheduler/completions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&notice).expect("json"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn health_and_stats_answer() {
        let ledger = InFlightLedger::default();
        let schedule = ScheduleId::new();
        ledger.start(schedule);
        let router = completion_router(ledger);
        let response = router
            .oneshot(
                axum::http::Request::get("/v1/scheduler/stats")
                    .body(axum::body::Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(response.status().is_success());
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .expect("body");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(json["in_flight"], 1);
    }
}
