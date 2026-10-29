//! PURPOSE: Thin HTTP handlers for tenant policy CRUD, lifecycle, rollback and dry-run evaluation.
//!
//! Owning layer: `mas-api` transport edge.
//! Production responsibility: authenticate from middleware, authorize each action through the canonical matrix, make one PolicyService call, and envelope the result.
//! Data ownership: all policy state and audit side effects belong to `PolicyService` and its ports.
//! MUST NOT: implement policy business rules, accept tenant scope from the request, persist dry-run decisions, or expose cross-tenant existence.

use axum::{
    extract::{Path, State},
    Json,
};
use mas_common::ids::PolicyId;
use mas_contracts::policy::{
    CreatePolicyRequest, EvaluatePolicyRequest, PolicyDecisionResponse, PolicyResponse,
    UpdatePolicyRequest,
};
use mas_security::{Action, ResourceKind};
use serde::Deserialize;

use crate::{
    handlers::{authorized_service_context, Ctx},
    response::{created, ok, ApiError, ApiResult},
    state::AppState,
};

/// `GET /v1/policies`.
pub async fn list(State(state): State<AppState>, Ctx(ctx): Ctx) -> ApiResult<Vec<PolicyResponse>> {
    let service_ctx = authorized_service_context(&ctx, ResourceKind::Policy, Action::Read, None)
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .policy_service(&ctx)?
        .list(&service_ctx)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/policies` creates a draft under the server-resolved tenant.
pub async fn create(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(request): Json<CreatePolicyRequest>,
) -> ApiResult<PolicyResponse> {
    let service_ctx = authorized_service_context(
        &ctx,
        ResourceKind::Policy,
        Action::ManagePolicies,
        request.project_id,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .policy_service(&ctx)?
        .create(&service_ctx, request)
        .await
        .map(|data| created(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/policies/{id}`.
pub async fn get(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<PolicyId>,
) -> ApiResult<PolicyResponse> {
    let service_ctx = authorized_service_context(&ctx, ResourceKind::Policy, Action::Read, None)
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .policy_service(&ctx)?
        .get(&service_ctx, id)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `PATCH /v1/policies/{id}` updates an inactive draft; publication is a separate explicit operation.
pub async fn update(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<PolicyId>,
    Json(request): Json<UpdatePolicyRequest>,
) -> ApiResult<PolicyResponse> {
    let service_ctx =
        authorized_service_context(&ctx, ResourceKind::Policy, Action::ManagePolicies, None)
            .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .policy_service(&ctx)?
        .update(&service_ctx, id, request)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/policies/{id}/publish` compile-gates and activates a draft.
pub async fn publish(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<PolicyId>,
) -> ApiResult<PolicyResponse> {
    let service_ctx =
        authorized_service_context(&ctx, ResourceKind::Policy, Action::ManagePolicies, None)
            .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .policy_service(&ctx)?
        .publish(&service_ctx, id)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// Input to append a rollback revision. The service deactivates the changed policy and stores it as a new version.
#[derive(Debug, Deserialize)]
pub struct RollbackPolicyBody {
    pub target_definition: serde_json::Value,
}

/// `POST /v1/policies/{id}/rollback`.
pub async fn rollback(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<PolicyId>,
    Json(body): Json<RollbackPolicyBody>,
) -> ApiResult<PolicyResponse> {
    let service_ctx =
        authorized_service_context(&ctx, ResourceKind::Policy, Action::ManagePolicies, None)
            .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .policy_service(&ctx)?
        .rollback(&service_ctx, id, body.target_definition)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/policies/{id}/evaluate` — a dry run, never a decision write and never quota consumption.
pub async fn evaluate(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<PolicyId>,
    Json(request): Json<EvaluatePolicyRequest>,
) -> ApiResult<PolicyDecisionResponse> {
    let service_ctx = authorized_service_context(&ctx, ResourceKind::Policy, Action::Read, None)
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .policy_service(&ctx)?
        .evaluate(&service_ctx, id, request)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_payload_requires_a_complete_definition_field() {
        assert!(serde_json::from_value::<RollbackPolicyBody>(
            serde_json::json!({"target_definition":{"rules":[]}})
        )
        .is_ok());
        assert!(serde_json::from_value::<RollbackPolicyBody>(serde_json::json!({})).is_err());
    }
}
