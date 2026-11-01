//! Human-in-the-loop approval queue HTTP adapters.
//!
//! Approval decisions are governance actions. The policy aggregate enforces
//! quorum, no self-approval, role allow-lists, one vote per actor and expiry;
//! this layer supplies only authenticated tenant context and the canonical RBAC
//! decision.

use axum::extract::{Path, Query, State};
use axum::Json;
use mas_common::ids::ApprovalRequestId;
use mas_contracts::approval::{ApprovalDecisionRequest, ApprovalQuery, ApprovalResponse};

use crate::handlers::Ctx;
use crate::response::{ApiError, ApiResult};
use crate::state::AppState;

/// `GET /v1/approvals` — tenant-scoped pending/history queue.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<ApprovalQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ApprovalResponse>> {
    let service_ctx = policy_context(&ctx, mas_security::Action::Read)?;
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .approval_service(&ctx)?
        .list(&service_ctx, &query)
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/approvals/{id}/approve` — records one governed approval vote.
pub async fn approve(
    State(state): State<AppState>,
    Path(id): Path<ApprovalRequestId>,
    Ctx(ctx): Ctx,
    Json(body): Json<ApprovalDecisionRequest>,
) -> ApiResult<ApprovalResponse> {
    decide(state, ctx, id, body, true).await
}

/// `POST /v1/approvals/{id}/deny` — records one governed veto vote.
pub async fn deny(
    State(state): State<AppState>,
    Path(id): Path<ApprovalRequestId>,
    Ctx(ctx): Ctx,
    Json(body): Json<ApprovalDecisionRequest>,
) -> ApiResult<ApprovalResponse> {
    decide(state, ctx, id, body, false).await
}

async fn decide(
    state: AppState,
    ctx: crate::context::RequestContext,
    id: ApprovalRequestId,
    body: ApprovalDecisionRequest,
    approve: bool,
) -> ApiResult<ApprovalResponse> {
    let service_ctx = policy_context(&ctx, mas_security::Action::ManagePolicies)?;
    body.validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .approval_service(&ctx)?
        .decide(&service_ctx, id, approve, &body.reason)
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

fn policy_context(
    ctx: &crate::context::RequestContext,
    action: mas_security::Action,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::Policy,
        action,
        None,
    )
    .map_err(|error| ApiError::new(error, ctx))
}
