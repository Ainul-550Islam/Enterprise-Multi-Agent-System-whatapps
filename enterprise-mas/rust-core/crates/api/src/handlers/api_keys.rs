//! Tenant-scoped API-key lifecycle endpoints.
//!
//! Raw key material is returned by create/rotate only. Listing and revocation
//! project metadata exclusively, and all calls go through the canonical
//! security service that hashes material before persistence and writes its
//! mandatory audit evidence.

use axum::extract::{Path, State};
use axum::Json;
use mas_common::ids::ApiKeyId;
use mas_contracts::api_key::{
    ApiKeyMetadataDto, CreateApiKeyRequest, CreatedApiKeyDto, RevokeApiKeyRequest,
    RotateApiKeyRequest,
};

use crate::handlers::Ctx;
use crate::response::{created, ApiError, ApiResult};
use crate::state::AppState;

/// `POST /v1/api-keys` — issues a plaintext key exactly once.
pub async fn create(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<CreateApiKeyRequest>,
) -> ApiResult<CreatedApiKeyDto> {
    let service_ctx = credential_context(&ctx)?;
    body.validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .api_key_service(&ctx)?
        .create(&service_ctx, &body.name, &body.scopes, body.expires_at)
        .await
        .map(|payload| created(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/api-keys` — safe metadata only; never raw key material or hashes.
pub async fn list(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ApiKeyMetadataDto>> {
    let service_ctx = credential_context(&ctx)?;
    state
        .api_key_service(&ctx)?
        .list(&service_ctx)
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/api-keys/{id}/revoke` — irreversible lifecycle transition.
pub async fn revoke(
    State(state): State<AppState>,
    Path(id): Path<ApiKeyId>,
    Ctx(ctx): Ctx,
    body: Option<Json<RevokeApiKeyRequest>>,
) -> ApiResult<serde_json::Value> {
    let service_ctx = credential_context(&ctx)?;
    if let Some(Json(body)) = body {
        if let Some(reason) = &body.reason {
            mas_common::validation::validate_length("reason", reason, 1, 1024)
                .map_err(|error| ApiError::new(error, &ctx))?;
        }
    }
    state
        .api_key_service(&ctx)?
        .revoke(&service_ctx, id)
        .await
        .map(|_| crate::response::ok(&ctx, serde_json::json!({"id": id, "revoked": true})))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/api-keys/{id}/rotate` — returns one new plaintext secret.
pub async fn rotate(
    State(state): State<AppState>,
    Path(id): Path<ApiKeyId>,
    Ctx(ctx): Ctx,
    Json(body): Json<RotateApiKeyRequest>,
) -> ApiResult<CreatedApiKeyDto> {
    let service_ctx = credential_context(&ctx)?;
    body.validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .api_key_service(&ctx)?
        .rotate(&service_ctx, id, body.grace_seconds)
        .await
        .map(|payload| created(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

fn credential_context(
    ctx: &crate::context::RequestContext,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::ApiKey,
        mas_security::Action::ManageCredentials,
        None,
    )
    .map_err(|error| ApiError::new(error, ctx))
}
