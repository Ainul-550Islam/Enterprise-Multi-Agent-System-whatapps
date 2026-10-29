//! PURPOSE: Thin authenticated HTTP adapter for the built-in assistant message submission.
//!
//! Owning layer: `mas-api`.
//!
//! Production responsibility: validate the request contract and delegate to the tenant assistant use case.
//!
//! Data ownership: resolved request context and the existing execution-pipeline response DTO.
//!
//! MUST NOT: execute AI directly, choose tenant scope from request data, or re-register assistant insights.

use axum::extract::State;
use axum::http::HeaderMap;
use axum::Json;
use mas_contracts::assistant::{AssistantExecutionDto, AssistantMessageRequest};

use crate::handlers::Ctx;
use crate::response::{accepted, ApiError, ApiResult};
use crate::state::AppState;

/// POST /v1/assistant/messages.
pub async fn submit(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    headers: HeaderMap,
    Json(request): Json<AssistantMessageRequest>,
) -> ApiResult<AssistantExecutionDto> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let header_key = headers
        .get(crate::context::IDEMPOTENCY_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let service_context = ctx
        .service_context()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let outcome = state
        .assistant_service(&ctx)?
        .submit(&service_context, &request.message, header_key.as_deref())
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(accepted(
        &ctx,
        AssistantExecutionDto {
            execution_id: outcome.execution.id,
        },
    ))
}
