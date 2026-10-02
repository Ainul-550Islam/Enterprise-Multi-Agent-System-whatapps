//! Health routes (public — no auth, caller of last resort for k8s probes).

use axum::extract::State;
use axum::Json;
use mas_common::timestamps::Timestamp;
use mas_contracts::health::{DependencyHealth, LivenessResponse, ReadinessResponse};

use crate::state::AppState;

/// `GET /v1/health/live` — always 200 when the process answers.
pub async fn live(State(state): State<AppState>) -> Json<LivenessResponse> {
    let _ = &state.health;
    Json(LivenessResponse::ok(
        state.service_name.as_ref(),
        state.service_version.as_ref(),
    ))
}

/// `GET /v1/health/ready` — aggregates registered readiness checks
/// (empty registry ⇒ ready; any `Down` component ⇒ 503).
pub async fn ready(
    State(state): State<AppState>,
) -> (axum::http::StatusCode, Json<ReadinessResponse>) {
    let aggregate = state
        .health
        .evaluate(
            mas_observability::health::ProbeKind::Readiness,
            &Timestamp::now(),
        )
        .await;
    let ready = aggregate.is_operational();
    let dependencies = aggregate
        .components
        .iter()
        .map(|component| DependencyHealth {
            name: component.name.clone(),
            healthy: component.status.severity() == 0,
            latency_ms: component.latency_ms,
            message: component.detail.clone(),
        })
        .collect::<Vec<_>>();
    let status = if ready {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(ReadinessResponse {
            ready,
            service: state.service_name.to_string(),
            version: state.service_version.to_string(),
            dependencies,
            timestamp: Timestamp::now(),
        }),
    )
}
