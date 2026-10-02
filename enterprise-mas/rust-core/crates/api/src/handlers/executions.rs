//! Execution routes. Submission lives under the project (`POST
//! /v1/projects/{project_id}/executions`) because the project is part of
//! the scope chain; transitions and reads are id-addressed.

use axum::extract::{Path, State};
use axum::Json;
use mas_common::ids::{ExecutionId, ProjectId};
use mas_contracts::execution::{ExecutionResponse, StartExecutionRequest};

use crate::handlers::Ctx;
use crate::response::{created, replayed, ApiResult, IntoApiResult};
use crate::state::AppState;

/// `POST /v1/projects/{project_id}/executions` — idempotent submission.
/// `Idempotency-Key` header or `idempotency_key` body field (header wins).
/// Replays answer `X-Idempotent-Replay: true` with the original execution.
pub async fn submit(
    State(state): State<AppState>,
    Path(project_id): Path<ProjectId>,
    Ctx(ctx): Ctx,
    headers: axum::http::HeaderMap,
    Json(body): Json<StartExecutionRequest>,
) -> ApiResult<ExecutionResponse> {
    scoped(&ctx)?;
    body.validate()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let header_key = headers
        .get(crate::context::IDEMPOTENCY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned);
    let idempotency_key = header_key.clone().or_else(|| {
        body.idempotency_key.as_ref().map(|key| {
            let trimmed = key.trim().to_owned();
            trimmed
        })
    });
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let outcome = state
        .executions
        .submit(
            &service_ctx,
            project_id,
            body.workflow_id,
            body.agent_id,
            body.input,
            idempotency_key.as_deref(),
        )
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let envelope = mas_application::dto::execution_response(&outcome.execution);
    let response = created(&ctx, envelope);
    Ok(if outcome.replayed {
        replayed(response)
    } else {
        response
    })
}

/// `POST /v1/executions/{execution_id}/start`.
pub async fn start(
    State(state): State<AppState>,
    Path(execution_id): Path<ExecutionId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ExecutionResponse> {
    transition(state, ctx, execution_id, Transition::Start).await
}

/// `POST /v1/executions/{execution_id}/pause`.
pub async fn pause(
    State(state): State<AppState>,
    Path(execution_id): Path<ExecutionId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ExecutionResponse> {
    transition(state, ctx, execution_id, Transition::Pause).await
}

/// `POST /v1/executions/{execution_id}/resume`.
pub async fn resume(
    State(state): State<AppState>,
    Path(execution_id): Path<ExecutionId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ExecutionResponse> {
    transition(state, ctx, execution_id, Transition::Resume).await
}

/// `POST /v1/executions/{execution_id}/cancel` — flags cancellation first
/// (the loop observes, drains, and calls `cancel` itself), then attempts
/// the domain transition immediately for already-idle executions.
pub async fn cancel(
    State(state): State<AppState>,
    Path(execution_id): Path<ExecutionId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ExecutionResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let execution = state
        .executions
        .request_cancellation(&service_ctx, execution_id)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let response = mas_application::dto::execution_response(&execution);
    Ok(crate::response::ok(&ctx, response))
}

/// `GET /v1/executions/{execution_id}`.
pub async fn get(
    State(state): State<AppState>,
    Path(execution_id): Path<ExecutionId>,
    Ctx(ctx): Ctx,
) -> ApiResult<ExecutionResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .executions
        .get_response(&service_ctx, execution_id)
        .await
        .into_api(&ctx)
}

/// `GET /v1/executions` — tenant-scoped list.
pub async fn list(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ExecutionResponse>> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let executions = state
        .executions
        .list(&service_ctx)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let mapped = executions
        .iter()
        .map(mas_application::dto::execution_response)
        .collect::<Vec<_>>();
    Ok(crate::response::ok(&ctx, mapped))
}

#[derive(Debug, Clone, Copy)]
enum Transition {
    Start,
    Pause,
    Resume,
}

async fn transition(
    state: AppState,
    ctx: crate::context::RequestContext,
    execution_id: ExecutionId,
    action: Transition,
) -> ApiResult<ExecutionResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let result = match action {
        Transition::Start => state.executions.start(&service_ctx, execution_id).await,
        Transition::Pause => state.executions.pause(&service_ctx, execution_id).await,
        Transition::Resume => state.executions.resume(&service_ctx, execution_id).await,
    };
    result
        .map(|execution| mas_application::dto::execution_response(&execution))
        .into_api(&ctx)
}

fn scoped(
    ctx: &crate::context::RequestContext,
) -> std::result::Result<(), crate::response::ApiError> {
    ctx.require_scope()
        .map(|_| ())
        .map_err(|e| crate::response::ApiError::new(e, ctx))
}
