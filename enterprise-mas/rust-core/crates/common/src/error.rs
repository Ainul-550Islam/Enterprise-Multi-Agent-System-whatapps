//! Unified, transport-agnostic error model.
//!
//! Every crate converges on [`AppError`]. The API/gRPC boundary converts it
//! into the stable external contract (`StableApiError` in `mas-contracts`)
//! using only [`AppError::error_code`], [`AppError::public_message`] and
//! [`AppError::http_status`], so internal details never leak.

use http::StatusCode;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Machine-readable detail about a single invalid field or rule violation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationIssue {
    /// Name or dotted path of the offending field.
    pub field: String,
    /// Stable machine-readable rule code (e.g. `required`, `too_long`, `invalid_format`).
    pub code: String,
    /// Safe, human-readable explanation.
    pub message: String,
}

impl ValidationIssue {
    #[must_use]
    pub fn new(
        field: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            field: field.into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

/// The single error type used across `rust-core`.
///
/// Variants are intentionally coarse: fine-grained context travels in the
/// payload strings/structures, never in new variants (keeps error codes stable).
#[derive(Debug, Clone, thiserror::Error)]
pub enum AppError {
    /// Input failed validation. `issues` carries field-level detail (may be empty).
    #[error("validation failed: {message}")]
    Validation {
        message: String,
        issues: Vec<ValidationIssue>,
    },

    /// Looked-up resource does not exist (or is invisible to the caller).
    #[error("{resource} not found (lookup: {lookup})")]
    NotFound {
        resource: &'static str,
        lookup: String,
    },

    /// Missing/invalid/expired credentials.
    #[error("unauthenticated: {0}")]
    Unauthorized(String),

    /// Authenticated, but not allowed to perform the action.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// State conflict (duplicate key, illegal transition, optimistic-lock failure).
    #[error("conflict: {0}")]
    Conflict(String),

    /// Quota/rate limit exceeded; retry after the hinted delay.
    #[error("rate limited: {0}")]
    RateLimited(String),

    /// Operation exceeded its deadline.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// Operation was cancelled by the caller/system before completion.
    #[error("operation cancelled: {0}")]
    Cancelled(String),

    /// Database-layer failure (connection, query, migration, integrity).
    #[error("database error: {0}")]
    Database(String),

    /// Message-broker / queue failure.
    #[error("messaging error: {0}")]
    Messaging(String),

    /// (De)serialization failure of internal payloads.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// An outbound call to an external dependency failed.
    #[error("external service '{service}' failed: {message}")]
    ExternalService { service: String, message: String },

    /// Unexpected internal failure; the payload is never sent to clients.
    #[error("internal error: {0}")]
    Internal(String),
}

impl AppError {
    // ------------------------------------------------------------------
    // Constructors
    // ------------------------------------------------------------------

    /// Simple validation failure without field detail.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation {
            message: message.into(),
            issues: Vec::new(),
        }
    }

    /// Validation failure with structured field detail.
    pub fn validation_with_issues(
        message: impl Into<String>,
        issues: Vec<ValidationIssue>,
    ) -> Self {
        Self::Validation {
            message: message.into(),
            issues,
        }
    }

    /// Single-field validation failure convenience.
    pub fn invalid_field(
        field: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let issue = ValidationIssue::new(field, code, message);
        Self::Validation {
            message: format!("invalid value for field '{}'", issue.field),
            issues: vec![issue],
        }
    }

    pub fn not_found(resource: &'static str, lookup: impl Into<String>) -> Self {
        Self::NotFound {
            resource,
            lookup: lookup.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::Unauthorized(message.into())
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    pub fn rate_limited(message: impl Into<String>) -> Self {
        Self::RateLimited(message.into())
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Timeout(message.into())
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::Cancelled(message.into())
    }

    pub fn database(message: impl Into<String>) -> Self {
        Self::Database(message.into())
    }

    pub fn messaging(message: impl Into<String>) -> Self {
        Self::Messaging(message.into())
    }

    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization(message.into())
    }

    pub fn external_service(service: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ExternalService {
            service: service.into(),
            message: message.into(),
        }
    }

    /// Internal errors wrap debugging context that must never reach clients.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// Wrap an internal error with additional context, preserving retryability class.
    #[must_use]
    pub fn with_context(self, context: impl Into<String>) -> Self {
        let context = context.into();
        match self {
            Self::Database(m) => Self::Database(format!("{context}: {m}")),
            Self::Messaging(m) => Self::Messaging(format!("{context}: {m}")),
            Self::ExternalService { service, message } => Self::ExternalService {
                service,
                message: format!("{context}: {message}"),
            },
            Self::Internal(m) => Self::Internal(format!("{context}: {m}")),
            Self::Serialization(m) => Self::Serialization(format!("{context}: {m}")),
            other => other,
        }
    }

    // ------------------------------------------------------------------
    // Classification
    // ------------------------------------------------------------------

    /// Stable, externally visible machine code. Part of the API contract —
    /// never rename or repurpose a code once shipped.
    #[must_use]
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Validation { .. } => "VALIDATION_FAILED",
            Self::NotFound { .. } => "RESOURCE_NOT_FOUND",
            Self::Unauthorized(_) => "UNAUTHENTICATED",
            Self::Forbidden(_) => "FORBIDDEN",
            Self::Conflict(_) => "CONFLICT",
            Self::RateLimited(_) => "RATE_LIMITED",
            Self::Timeout(_) => "TIMEOUT",
            Self::Cancelled(_) => "CANCELLED",
            Self::Database(_) => "DATABASE_ERROR",
            Self::Messaging(_) => "MESSAGING_ERROR",
            Self::Serialization(_) => "SERIALIZATION_ERROR",
            Self::ExternalService { .. } => "EXTERNAL_SERVICE_ERROR",
            Self::Internal(_) => "INTERNAL_ERROR",
        }
    }

    /// Whether the operation may be retried by a caller/worker with backoff.
    /// This classifies *error categories*; per-operation rules (e.g. max
    /// attempts, non-idempotent writes) live in the retry policy layer.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::RateLimited(_) | Self::Timeout(_) | Self::Messaging(_) | Self::Database(_) => {
                true
            },
            Self::ExternalService { .. } => true,
            Self::Validation { .. }
            | Self::NotFound { .. }
            | Self::Unauthorized(_)
            | Self::Forbidden(_)
            | Self::Conflict(_)
            | Self::Cancelled(_)
            | Self::Serialization(_)
            | Self::Internal(_) => false,
        }
    }

    /// Suggested delay before retrying, when the error itself carries such a hint.
    #[must_use]
    pub fn retry_after_hint(&self) -> Option<Duration> {
        match self {
            Self::RateLimited(_) => Some(Duration::from_secs(1)),
            Self::Timeout(_) => Some(Duration::from_millis(250)),
            _ => None,
        }
    }

    /// HTTP status used by the API boundary when mapping this error.
    #[must_use]
    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::Validation { .. } => StatusCode::BAD_REQUEST,
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            // 499 (nginx-style "Client Closed Request") is the de-facto
            // cancellation status; fall back to 400 if a stack rejects it.
            Self::Cancelled(_) => StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            Self::Database(_) | Self::Messaging(_) | Self::Serialization(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            },
            Self::ExternalService { .. } => StatusCode::BAD_GATEWAY,
        }
    }

    /// Message safe to expose to API callers. Internal-class errors are
    /// replaced with a generic phrase; validation/auth-style messages are
    /// authored to be safe and are passed through.
    #[must_use]
    pub fn public_message(&self) -> String {
        match self {
            Self::Validation { message, .. } => message.clone(),
            Self::NotFound { resource, lookup } => {
                format!("{resource} not found (lookup: {lookup})")
            },
            Self::Unauthorized(m) => format!("unauthenticated: {m}"),
            Self::Forbidden(m) => format!("forbidden: {m}"),
            Self::Conflict(m) => format!("conflict: {m}"),
            Self::RateLimited(m) => format!("rate limit exceeded: {m}"),
            Self::Timeout(_) => "the operation timed out; it may be retried".to_owned(),
            Self::Cancelled(_) => "the operation was cancelled".to_owned(),
            Self::ExternalService { service, .. } => {
                format!("upstream dependency '{service}' is temporarily unavailable")
            },
            Self::Database(_) | Self::Messaging(_) | Self::Serialization(_) | Self::Internal(_) => {
                "an internal error occurred".to_owned()
            },
        }
    }

    /// Field-level issues for `Validation` errors; empty otherwise.
    #[must_use]
    pub fn validation_issues(&self) -> &[ValidationIssue] {
        match self {
            Self::Validation { issues, .. } => issues,
            _ => &[],
        }
    }
}

// ---------------------------------------------------------------------------
// Conversions from common third-party error types.
// ---------------------------------------------------------------------------

impl From<uuid::Error> for AppError {
    fn from(err: uuid::Error) -> Self {
        Self::invalid_field("id", "invalid_format", format!("invalid UUID: {err}"))
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<url::ParseError> for AppError {
    fn from(err: url::ParseError) -> Self {
        Self::invalid_field("url", "invalid_format", format!("invalid URL: {err}"))
    }
}

impl From<chrono::ParseError> for AppError {
    fn from(err: chrono::ParseError) -> Self {
        Self::invalid_field(
            "timestamp",
            "invalid_format",
            format!("invalid timestamp: {err}"),
        )
    }
}

impl From<base64::DecodeError> for AppError {
    fn from(err: base64::DecodeError) -> Self {
        Self::validation(format!("invalid base64 payload: {err}"))
    }
}

impl From<std::num::ParseIntError> for AppError {
    fn from(err: std::num::ParseIntError) -> Self {
        Self::validation(format!("invalid integer: {err}"))
    }
}

impl From<std::num::TryFromIntError> for AppError {
    fn from(err: std::num::TryFromIntError) -> Self {
        Self::validation(format!("integer out of range: {err}"))
    }
}

impl From<std::string::FromUtf8Error> for AppError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self::Serialization(format!("invalid UTF-8: {err}"))
    }
}

impl From<std::str::Utf8Error> for AppError {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::Serialization(format!("invalid UTF-8: {err}"))
    }
}
