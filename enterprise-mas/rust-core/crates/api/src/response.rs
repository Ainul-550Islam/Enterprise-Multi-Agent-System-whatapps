//! Uniform response shapes: every route answers `ApiEnvelope<T>` — data on
//! success, `StableApiError` on failure, `EnvelopeMeta` (request id +
//! correlation echo) on both.

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use mas_common::error::AppError;
use mas_common::timestamps::Timestamp;
use mas_contracts::api::ApiVersion;
use mas_contracts::api::{ApiEnvelope, EnvelopeMeta};
use mas_contracts::errors::StableApiError;

use crate::context::{RequestContext, CORRELATION_HEADER};

/// Builds the meta block for a request (`api_version: v1`).
pub fn meta_for(ctx: &RequestContext) -> EnvelopeMeta {
    EnvelopeMeta {
        request_id: ctx.request_id,
        correlation_id: ctx.correlation_id.clone(),
        api_version: ApiVersion::V1,
        responded_at: Timestamp::now(),
    }
}

/// Success envelope (`200 OK` with `data`).
pub fn ok<T: serde::Serialize>(ctx: &RequestContext, data: T) -> ApiResponse<T> {
    ApiResponse {
        status: StatusCode::OK,
        envelope: ApiEnvelope {
            data: Some(data),
            error: None,
            meta: meta_for(ctx),
        },
        correlation_id: ctx.correlation_id.clone(),
        idempotent_replay: false,
    }
}

/// Created-envelope (`201 Created`) for registration routes.
pub fn created<T: serde::Serialize>(ctx: &RequestContext, data: T) -> ApiResponse<T> {
    ApiResponse {
        status: StatusCode::CREATED,
        correlation_id: String::new(),
        ..ok(ctx, data)
    }
}

/// Marks a response as an idempotent replay (`X-Idempotent-Replay: true`).
#[must_use]
pub fn replayed<T: serde::Serialize>(mut response: ApiResponse<T>) -> ApiResponse<T> {
    response.idempotent_replay = true;
    response
}

/// The wire response: status + envelope + correlation headers.
#[derive(Debug)]
pub struct ApiResponse<T: serde::Serialize> {
    status: StatusCode,
    envelope: ApiEnvelope<T>,
    correlation_id: String,
    idempotent_replay: bool,
}

impl<T: serde::Serialize> IntoResponse for ApiResponse<T> {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        if let Ok(value) = HeaderValue::from_str(&self.correlation_id) {
            headers.insert(CORRELATION_HEADER, value);
        }
        if self.idempotent_replay {
            headers.insert("x-idempotent-replay", HeaderValue::from_static("true"));
        }
        (self.status, headers, Json(self.envelope)).into_response()
    }
}

/// Converts an [`AppError`] into the stable error contract. Only the
/// public surface (code + safe message) crosses the wire; internal context
/// stays in logs/spans.
pub fn stable_error(err: &AppError, ctx: &RequestContext) -> StableApiError {
    let mut error = StableApiError::new(err.error_code(), err.public_message());
    error.request_id = Some(ctx.request_id);
    if let AppError::Validation { issues, .. } = err {
        error.details = Some(serde_json::json!({
            "issues": issues
        }));
    }
    error
}

#[derive(Debug)]
struct ApiErrorInner {
    source: AppError,
    ctx: RequestContext,
}

/// The handler error type — internally boxed so handler `Result`s stay
/// one-word wide (clippy's `result_large_err` boundary).
#[derive(Debug)]
pub struct ApiError {
    inner: Box<ApiErrorInner>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let inner = *self.inner;
        tracing::warn!(
            code = %inner.source.error_code(),
            correlation = %inner.ctx.correlation_id,
            "request failed"
        );
        ApiResponse::<serde_json::Value> {
            status: inner.source.http_status(),
            envelope: ApiEnvelope {
                data: None,
                error: Some(stable_error(&inner.source, &inner.ctx)),
                meta: meta_for(&inner.ctx),
            },
            correlation_id: inner.ctx.correlation_id.clone(),
            idempotent_replay: false,
        }
        .into_response()
    }
}

impl ApiError {
    /// Wraps a service error using the request's context.
    pub fn new(source: AppError, ctx: &RequestContext) -> Self {
        Self {
            inner: Box::new(ApiErrorInner {
                source,
                ctx: ctx.clone(),
            }),
        }
    }
}

/// Handlers return this alias.
pub type ApiResult<T> = std::result::Result<ApiResponse<T>, ApiError>;

/// Convenience extension: map a `Result<T>` into an `ApiResult` body.
pub trait IntoApiResult<T> {
    /// Convert into an ApiResponse-wrapped success, or a boxed ApiError.
    fn into_api(self, ctx: &RequestContext) -> ApiResult<T>
    where
        T: serde::Serialize;
}

impl<T> IntoApiResult<T> for std::result::Result<T, AppError> {
    fn into_api(self, ctx: &RequestContext) -> ApiResult<T>
    where
        T: serde::Serialize,
    {
        self.map(|data| ok(ctx, data))
            .map_err(|err| ApiError::new(err, ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Principal, PrincipalKind};

    fn ctx() -> RequestContext {
        RequestContext {
            principal: Principal {
                subject: "jane".to_owned(),
                kind: PrincipalKind::User,
            },
            tenant_id: None,
            organization_id: None,
            request_id: uuid::Uuid::now_v7(),
            correlation_id: "corr-42".to_owned(),
        }
    }

    #[test]
    fn envelopes_carry_meta_and_public_errors_only() {
        let context = ctx();
        let response = ok(&context, serde_json::json!({"n": 1}));
        assert_eq!(response.envelope.meta.correlation_id, "corr-42");
        assert!(response.envelope.error.is_none());

        let err = AppError::validation("title must not be empty");
        let stable = stable_error(&err, &context);
        assert_eq!(stable.code, "VALIDATION_FAILED");
        assert_eq!(stable.request_id, Some(context.request_id));

        let internal = AppError::internal("sqlx pool exploded at driver.rs:42");
        let stable = stable_error(&internal, &context);
        assert_eq!(stable.code, "INTERNAL_ERROR");
        assert!(
            !stable.message.contains("sqlx") && !stable.message.contains("driver"),
            "internal detail stays server-side"
        );
    }
}
