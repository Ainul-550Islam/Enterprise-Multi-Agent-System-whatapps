//! Governance rollback handlers: thin HTTP adapters over the application
//! rollback facade. The path version and request target must agree so a URL
//! cannot ambiguously name a rollback target.

use axum::extract::{Path, Query, State};
use axum::Json;
use mas_common::ids::{AgentId, WorkflowId};
use mas_contracts::rollback::{RollbackHistoryQuery, RollbackRequest, RollbackResponseDto};

use crate::handlers::Ctx;
use crate::response::{created, ApiResult};
use crate::state::AppState;

/// `POST /v1/agents/{agent_id}/versions/{version}/rollback`.
pub async fn rollback_agent(
    State(state): State<AppState>,
    Path((agent_id, path_version)): Path<(AgentId, u64)>,
    Ctx(ctx): Ctx,
    Json(body): Json<RollbackRequest>,
) -> ApiResult<RollbackResponseDto> {
    let service_ctx = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Agent,
        mas_security::Action::Publish,
        None,
    )
    .map_err(|error| crate::response::ApiError::new(error, &ctx))?;
    body.validate()
        .map_err(|error| crate::response::ApiError::new(error, &ctx))?;
    if body.target_version_number != path_version {
        return Err(crate::response::ApiError::new(
            mas_common::error::AppError::invalid_field(
                "target_version_number",
                "path_mismatch",
                "request target_version_number must match the version in the route",
            ),
            &ctx,
        ));
    }
    let service = state.rollbacks.as_ref().ok_or_else(|| {
        crate::response::ApiError::new(
            mas_common::error::AppError::internal("rollback service is not configured"),
            &ctx,
        )
    })?;
    service
        .rollback_agent(&service_ctx, agent_id, &body)
        .await
        .map(|receipt| created(&ctx, receipt))
        .map_err(|error| crate::response::ApiError::new(error, &ctx))
}

/// `POST /v1/workflows/{workflow_id}/versions/{version}/rollback`.
pub async fn rollback_workflow(
    State(state): State<AppState>,
    Path((workflow_id, path_version)): Path<(WorkflowId, u64)>,
    Ctx(ctx): Ctx,
    Json(body): Json<RollbackRequest>,
) -> ApiResult<RollbackResponseDto> {
    let service_ctx = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Workflow,
        mas_security::Action::Publish,
        None,
    )
    .map_err(|error| crate::response::ApiError::new(error, &ctx))?;
    body.validate()
        .map_err(|error| crate::response::ApiError::new(error, &ctx))?;
    if body.target_version_number != path_version {
        return Err(crate::response::ApiError::new(
            mas_common::error::AppError::invalid_field(
                "target_version_number",
                "path_mismatch",
                "request target_version_number must match the version in the route",
            ),
            &ctx,
        ));
    }
    let service = state.rollbacks.as_ref().ok_or_else(|| {
        crate::response::ApiError::new(
            mas_common::error::AppError::internal("rollback service is not configured"),
            &ctx,
        )
    })?;
    service
        .rollback_workflow(&service_ctx, workflow_id, &body)
        .await
        .map(|receipt| created(&ctx, receipt))
        .map_err(|error| crate::response::ApiError::new(error, &ctx))
}

/// `GET /v1/rollbacks?limit=` — newest-first, bounded history for the
/// authenticated tenant. Idempotency keys are never returned.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<RollbackHistoryQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<mas_contracts::rollback::RollbackHistoryItemDto>> {
    let service_ctx = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Audit,
        mas_security::Action::ViewAudit,
        None,
    )
    .map_err(|error| crate::response::ApiError::new(error, &ctx))?;
    query
        .validate()
        .map_err(|error| crate::response::ApiError::new(error, &ctx))?;
    let service = state.rollbacks.as_ref().ok_or_else(|| {
        crate::response::ApiError::new(
            mas_common::error::AppError::internal("rollback service is not configured"),
            &ctx,
        )
    })?;
    service
        .list(&service_ctx, &query)
        .await
        .map(|items| crate::response::ok(&ctx, items))
        .map_err(|error| crate::response::ApiError::new(error, &ctx))
}
