//! Schedule routes.

use axum::extract::{Path, State};
use axum::Json;
use mas_common::ids::ScheduleId;
use mas_domain::ScheduleKind;
use serde::{Deserialize, Serialize};

use crate::handlers::Ctx;
use crate::response::{created, ApiResult, IntoApiResult};
use crate::state::AppState;

/// Compact schedule wire shape.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleResponse {
    /// Id.
    pub id: String,
    /// Tenant id.
    pub tenant_id: String,
    /// Project id (tenant-wide schedules omit it).
    pub project_id: Option<String>,
    /// Name.
    pub name: String,
    /// Status (`active | paused | disabled | completed`).
    pub status: String,
    /// Next computed run (None for paused/disabled/completed).
    pub next_run_at: Option<String>,
}

fn schedule_response(schedule: &mas_domain::Schedule) -> ScheduleResponse {
    ScheduleResponse {
        id: schedule.id.to_string(),
        tenant_id: schedule.tenant_id.to_string(),
        project_id: schedule.project_id.map(|p| p.to_string()),
        name: schedule.name.clone(),
        status: schedule.status.to_string(),
        next_run_at: schedule.next_run_at.map(|t| t.to_string()),
    }
}

/// `POST /v1/schedules` body.
#[derive(Debug, Deserialize)]
pub struct RegisterScheduleBody {
    /// Project when the schedule is project-bound.
    #[serde(default)]
    pub project_id: Option<mas_common::ids::ProjectId>,
    /// Human name.
    pub name: String,
    /// `interval | one_time` (cron registrations go through the operator
    /// CLI — they carry an expression, not seconds, and need validation).
    pub kind: String,
    /// Interval seconds (`kind=interval`).
    #[serde(default)]
    pub every_seconds: Option<u64>,
    /// Fire-at timestamp, RFC 3339 (`kind=one_time`).
    #[serde(default)]
    pub run_at: Option<String>,
    /// JSON object template for the task created on each firing.
    pub task_template: serde_json::Value,
}

impl RegisterScheduleBody {
    fn kind(&self) -> mas_common::result::Result<ScheduleKind> {
        match self.kind.as_str() {
            "interval" => Ok(ScheduleKind::Interval {
                every_seconds: self.every_seconds.ok_or_else(|| {
                    mas_common::error::AppError::invalid_field(
                        "every_seconds",
                        "invalid",
                        "required for interval schedules",
                    )
                })?,
            }),
            "one_time" => {
                let run_at = self.run_at.as_deref().ok_or_else(|| {
                    mas_common::error::AppError::invalid_field(
                        "run_at",
                        "invalid",
                        "required (RFC 3339) for one_time schedules",
                    )
                })?;
                let timestamp = mas_common::timestamps::Timestamp::parse_rfc3339(run_at)?;
                Ok(ScheduleKind::OneTime { run_at: timestamp })
            },
            other => Err(mas_common::error::AppError::invalid_field(
                "kind",
                "invalid",
                format!("unsupported schedule kind '{other}' (interval | one_time)"),
            )),
        }
    }
}

/// `POST /v1/schedules` — registers ACTIVE (register == fire-when-due).
pub async fn register(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<RegisterScheduleBody>,
) -> ApiResult<ScheduleResponse> {
    scoped(&ctx)?;
    let kind = body
        .kind()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .schedules
        .register(
            &service_ctx,
            body.project_id,
            &body.name,
            kind,
            body.task_template,
        )
        .await
        .map(|schedule| created(&ctx, schedule_response(&schedule)))
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `POST /v1/schedules/{schedule_id}/pause`.
pub async fn pause(
    State(state): State<AppState>,
    Path(schedule_id): Path<ScheduleId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ScheduleResponse> {
    apply(state, ctx, schedule_id, StateChange::Pause).await
}

/// `POST /v1/schedules/{schedule_id}/resume` (idempotent).
pub async fn resume(
    State(state): State<AppState>,
    Path(schedule_id): Path<ScheduleId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ScheduleResponse> {
    apply(state, ctx, schedule_id, StateChange::Resume).await
}

/// `POST /v1/schedules/{schedule_id}/disable`.
pub async fn disable(
    State(state): State<AppState>,
    Path(schedule_id): Path<ScheduleId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ScheduleResponse> {
    apply(state, ctx, schedule_id, StateChange::Disable).await
}

/// `GET /v1/schedules/{schedule_id}`.
pub async fn get(
    State(state): State<AppState>,
    Path(schedule_id): Path<ScheduleId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ScheduleResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .schedules
        .get(&service_ctx, schedule_id)
        .await
        .map(|schedule| schedule_response(&schedule))
        .into_api(&ctx)
}

/// `GET /v1/schedules` — tenant-scoped list.
pub async fn list(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ScheduleResponse>> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .schedules
        .list(&service_ctx)
        .await
        .map(|schedules| schedules.iter().map(schedule_response).collect::<Vec<_>>())
        .into_api(&ctx)
}

#[derive(Debug, Clone, Copy)]
enum StateChange {
    Pause,
    Resume,
    Disable,
}

async fn apply(
    state: AppState,
    ctx: crate::context::RequestContext,
    schedule_id: ScheduleId,
    change: StateChange,
) -> ApiResult<ScheduleResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let result = match change {
        StateChange::Pause => state.schedules.pause(&service_ctx, schedule_id).await,
        StateChange::Resume => state.schedules.resume(&service_ctx, schedule_id).await,
        StateChange::Disable => state.schedules.disable(&service_ctx, schedule_id).await,
    };
    result
        .map(|schedule| schedule_response(&schedule))
        .into_api(&ctx)
}

fn scoped(
    ctx: &crate::context::RequestContext,
) -> std::result::Result<(), crate::response::ApiError> {
    ctx.require_scope()
        .map(|_| ())
        .map_err(|e| crate::response::ApiError::new(e, ctx))
}
