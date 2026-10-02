//! The stable error contract.
//!
//! Every failure crossing an API/gRPC boundary is a [`StableApiError`].
//! Codes and message-safety invariants come from `AppError`: internal
//! details never leak beyond the boundary.

use mas_common::error::{AppError, ValidationIssue};
use serde::{Deserialize, Serialize};

/// The externally visible error structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StableApiError {
    /// Stable machine code (see `AppError::error_code`).
    pub code: String,
    /// Safe, human-readable message.
    pub message: String,
    /// Correlates with server logs/traces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<uuid::Uuid>,
    /// Optional structured detail (validation issues etc.). Never secrets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl StableApiError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            request_id: None,
            details: None,
        }
    }

    pub fn internal(context: &str) -> Self {
        let _ = context; // never leaked; use tracing for internal context
        Self::new("INTERNAL_ERROR", "an internal error occurred")
    }

    #[must_use]
    pub fn with_request_id(mut self, request_id: uuid::Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }

    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// HTTP status of this error.
    #[must_use]
    pub fn http_status(&self) -> u16 {
        match self.code.as_str() {
            "VALIDATION_FAILED" => 400,
            "UNAUTHENTICATED" => 401,
            "FORBIDDEN" => 403,
            "RESOURCE_NOT_FOUND" => 404,
            "CONFLICT" => 409,
            "RATE_LIMITED" => 429,
            "CANCELLED" => 499,
            "TIMEOUT" => 504,
            "EXTERNAL_SERVICE_ERROR" => 502,
            _ => 500,
        }
    }
}

impl From<&AppError> for StableApiError {
    /// Safe boundary mapping: code from `error_code`, message from
    /// `public_message`, details only from validation issues.
    fn from(err: &AppError) -> Self {
        let details = match err {
            AppError::Validation { issues, .. } if !issues.is_empty() => {
                serde_json::to_value(issues).ok()
            },
            _ => None,
        };
        Self {
            code: err.error_code().to_owned(),
            message: err.public_message(),
            request_id: None, // attached by the edge layer
            details,
        }
    }
}

impl From<AppError> for StableApiError {
    fn from(err: AppError) -> Self {
        Self::from(&err)
    }
}

/// Detailed validation error payload for `details`.
impl From<Vec<ValidationIssue>> for StableApiError {
    fn from(issues: Vec<ValidationIssue>) -> Self {
        let message = format!("validation failed with {} issue(s)", issues.len());
        let details = serde_json::to_value(&issues).ok();
        Self {
            code: "VALIDATION_FAILED".to_owned(),
            message,
            request_id: None,
            details,
        }
    }
}
