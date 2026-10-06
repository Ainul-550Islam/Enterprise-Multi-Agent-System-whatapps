//! PURPOSE: The unified, transport-agnostic error model every crate converges
//! on.
//!
//! Owning layer: `mas-common` (the base layer).
//!
//! Production responsibility: one taxonomy that carries a stable machine
//! code, a *safe* public message and an HTTP mapping, so the API boundary can
//! translate without leaking internals, and the worker can classify retry
//! vs terminal from the variant itself.
//!
//! It must NOT:
//!   • put a credential, token, payload or connection string in a message —
//!     `public_message()` must stay safe to return to a caller;
//!   • add a variant without adding it to the exhaustive classifier in
//!     `mas-worker::classify`, which would inherit the wrong retry class;
//!   • collapse distinct failures into one generic internal error.

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

    /// A quota ceiling refused the request.
    ///
    /// Deliberately **not** [`Self::RateLimited`]. Rate limiting is a
    /// transient back-pressure signal ("come back in a moment"), quota
    /// exhaustion is an entitlement fact about the tenant's plan ("you have
    /// used your 10,000 executions this month"). Conflating them made every
    /// quota denial retryable, so a worker would burn its whole attempt
    /// budget re-submitting work that could not possibly succeed until the
    /// billing period rolled over.
    ///
    /// `retry_after` is populated only when the engine can name the window
    /// reset; it is a *hint for a human or a scheduler*, never a promise that
    /// retrying sooner will help, and it never flips `is_retryable()`.
    #[error("quota exceeded: {message}")]
    QuotaExceeded {
        /// Metered dimension that was exhausted (`executions`, `tokens`, …).
        dimension: String,
        /// Units consumed in the current window.
        consumed: u64,
        /// The configured ceiling.
        limit: u64,
        /// When the window resets, if the engine can compute it.
        retry_after: Option<Duration>,
        /// Safe, human-readable explanation.
        message: String,
    },

    /// A policy evaluation returned `Deny` (or `Escalate` where the caller
    /// cannot escalate).
    ///
    /// Separate from [`Self::Forbidden`] because a policy denial is
    /// *explainable*: it carries the identity of the rule that denied and the
    /// version of the policy that contained it. Collapsing it into a generic
    /// `FORBIDDEN` made denials unauditable — an operator seeing
    /// `FORBIDDEN` at 3am could not answer "which rule?".
    #[error("policy denied: {message}")]
    PolicyDenied {
        /// Stable machine code of the denial reason.
        reason_code: String,
        /// Policy that produced the decision, when one matched.
        policy_id: Option<String>,
        /// Version of that policy, so a stale cache can be ruled out.
        policy_version: Option<u64>,
        /// Safe, human-readable explanation.
        message: String,
    },

    /// The tenant's subscription does not include the requested feature, or
    /// the subscription is not in a state that permits starting work.
    ///
    /// Separate from [`Self::Forbidden`] so a client can distinguish "your
    /// role does not permit this" (403, retrying will not help, nothing to
    /// buy) from "your plan does not include this" (402, retrying will not
    /// help, but upgrading will). Same HTTP family, completely different
    /// remediation.
    #[error("entitlement required: {message}")]
    EntitlementRequired {
        /// The feature that was gated.
        feature: String,
        /// Plan code currently in effect, when known.
        plan_code: Option<String>,
        /// Subscription state, when known.
        subscription_state: Option<String>,
        /// Safe, human-readable explanation.
        message: String,
    },

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

    /// A structured failure an external agent runtime reported.
    ///
    /// This exists because the AI orchestrator has its own error vocabulary
    /// (`rate_limited`, `invalid_tool_permission`, `context_length_exceeded`,
    /// ...) and its own opinion about whether a retry is worth it. Flattening
    /// that into `ExternalService` would discard both: the code would become
    /// `EXTERNAL_SERVICE_ERROR` and every run would look transient, so a
    /// permanently invalid request would burn the whole attempt budget before
    /// it dead-letters.
    ///
    /// The variant carries the external code and message verbatim (already
    /// redacted by the bridge) so the durable failure record and the audit
    /// trail name what actually went wrong.
    #[error("external failure [{code}]: {message}")]
    ExternalFailure {
        /// Stable code the external runtime reported.
        code: String,
        /// Safe, already-redacted message from the external runtime.
        message: String,
        /// Whether the external runtime believes another attempt may succeed.
        retryable: bool,
    },
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

    /// Quota refusal for one metered dimension.
    #[must_use]
    pub fn quota_exceeded(
        dimension: impl Into<String>,
        consumed: u64,
        limit: u64,
        retry_after: Option<Duration>,
        message: impl Into<String>,
    ) -> Self {
        Self::QuotaExceeded {
            dimension: dimension.into(),
            consumed,
            limit,
            retry_after,
            message: message.into(),
        }
    }

    /// Policy refusal carrying the rule's provenance.
    #[must_use]
    pub fn policy_denied(
        reason_code: impl Into<String>,
        policy_id: Option<String>,
        policy_version: Option<u64>,
        message: impl Into<String>,
    ) -> Self {
        Self::PolicyDenied {
            reason_code: reason_code.into(),
            policy_id,
            policy_version,
            message: message.into(),
        }
    }

    /// Entitlement refusal (plan/subscription does not cover the feature).
    #[must_use]
    pub fn entitlement_required(
        feature: impl Into<String>,
        plan_code: Option<String>,
        subscription_state: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::EntitlementRequired {
            feature: feature.into(),
            plan_code,
            subscription_state,
            message: message.into(),
        }
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

    /// Constructs a structured failure an external agent runtime reported.
    ///
    /// `retryable` is the external runtime's own verdict and is honoured as
    /// given: it produced the failure, so it classified it, and it has more
    /// information about it than this process does.
    #[must_use]
    pub fn external_failure(
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self::ExternalFailure {
            code: code.into(),
            message: message.into(),
            retryable,
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
            // The control-plane refusals keep their structured payload and
            // gain the context in the message. Re-wrapping them as
            // `Internal` would lose the code, the dimension and the rule
            // provenance that makes the denial auditable.
            Self::QuotaExceeded {
                dimension,
                consumed,
                limit,
                retry_after,
                message,
            } => Self::QuotaExceeded {
                dimension,
                consumed,
                limit,
                retry_after,
                message: format!("{context}: {message}"),
            },
            Self::PolicyDenied {
                reason_code,
                policy_id,
                policy_version,
                message,
            } => Self::PolicyDenied {
                reason_code,
                policy_id,
                policy_version,
                message: format!("{context}: {message}"),
            },
            Self::EntitlementRequired {
                feature,
                plan_code,
                subscription_state,
                message,
            } => Self::EntitlementRequired {
                feature,
                plan_code,
                subscription_state,
                message: format!("{context}: {message}"),
            },
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
            Self::QuotaExceeded { .. } => "QUOTA_EXCEEDED",
            Self::PolicyDenied { .. } => "POLICY_DENIED",
            Self::EntitlementRequired { .. } => "ENTITLEMENT_REQUIRED",
            Self::Timeout(_) => "TIMEOUT",
            Self::Cancelled(_) => "CANCELLED",
            Self::Database(_) => "DATABASE_ERROR",
            Self::Messaging(_) => "MESSAGING_ERROR",
            Self::Serialization(_) => "SERIALIZATION_ERROR",
            Self::ExternalService { .. } => "EXTERNAL_SERVICE_ERROR",
            Self::Internal(_) => "INTERNAL_ERROR",
            Self::ExternalFailure { .. } => "EXTERNAL_FAILURE",
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
            // A quota may carry a `retry_after` window reset, but that is not
            // a licence for the worker to retry: the worker's retry budget
            // spans seconds, quota windows span hours. Retrying a
            // QUOTA_EXCEEDED inside one window turns one clean denial into
            // `max_attempts` identical denials and an eventual dead letter
            // that says nothing about why. Rescheduling at window reset is a
            // scheduler decision, not a retry decision.
            Self::QuotaExceeded { .. } => false,
            // The external runtime's own verdict decides; it produced the
            // failure and therefore knows whether a retry can help.
            Self::ExternalFailure { retryable, .. } => *retryable,
            Self::Validation { .. }
            | Self::NotFound { .. }
            | Self::Unauthorized(_)
            | Self::Forbidden(_)
            | Self::Conflict(_)
            | Self::Cancelled(_)
            | Self::Serialization(_)
            | Self::Internal(_)
            | Self::PolicyDenied { .. }
            | Self::EntitlementRequired { .. } => false,
        }
    }

    /// Suggested delay before retrying, when the error itself carries such a hint.
    #[must_use]
    pub fn retry_after_hint(&self) -> Option<Duration> {
        match self {
            Self::RateLimited(_) => Some(Duration::from_secs(1)),
            Self::Timeout(_) => Some(Duration::from_millis(250)),
            Self::QuotaExceeded { retry_after, .. } => *retry_after,
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
            Self::QuotaExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::PolicyDenied { .. } => StatusCode::FORBIDDEN,
            Self::EntitlementRequired { .. } => StatusCode::PAYMENT_REQUIRED,
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            // 499 (nginx-style "Client Closed Request") is the de-facto
            // cancellation status; fall back to 400 if a stack rejects it.
            Self::Cancelled(_) => StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            Self::Database(_) | Self::Messaging(_) | Self::Serialization(_) | Self::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            },
            Self::ExternalService { .. } => StatusCode::BAD_GATEWAY,
            Self::ExternalFailure { retryable, .. } => {
                if *retryable {
                    StatusCode::BAD_GATEWAY
                } else {
                    StatusCode::UNPROCESSABLE_ENTITY
                }
            },
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
            Self::QuotaExceeded {
                dimension,
                consumed,
                limit,
                retry_after,
                message,
            } => {
                // The dimension and the arithmetic are the caller's own
                // numbers, so they are safe to publish; `message` is
                // authored at the call site and must stay secret-free.
                match retry_after {
                    Some(delay) => format!(
                        "quota exceeded for '{dimension}' ({consumed}/{limit}); \
                         the window resets in {}s",
                        delay.as_secs()
                    ),
                    None => {
                        format!("quota exceeded for '{dimension}' ({consumed}/{limit}): {message}")
                    },
                }
            },
            Self::PolicyDenied {
                reason_code,
                policy_id,
                policy_version,
                message,
            } => {
                let provenance = match (policy_id, policy_version) {
                    (Some(id), Some(version)) => format!(" [policy {id} v{version}]"),
                    (Some(id), None) => format!(" [policy {id}]"),
                    _ => String::new(),
                };
                // The rule identity is published on purpose: a denial the
                // operator cannot trace to a rule is a denial they will
                // "fix" by deleting the policy.
                format!("policy denied ({reason_code}){provenance}: {message}")
            },
            Self::EntitlementRequired {
                feature,
                plan_code,
                subscription_state,
                message,
            } => {
                let shape = match (plan_code, subscription_state) {
                    (Some(plan), Some(state)) => format!(" (plan '{plan}', subscription {state})"),
                    (Some(plan), None) => format!(" (plan '{plan}')"),
                    (None, Some(state)) => format!(" (subscription {state})"),
                    _ => String::new(),
                };
                format!("feature '{feature}' is not included{shape}: {message}")
            },
            Self::Timeout(_) => "the operation timed out; it may be retried".to_owned(),
            Self::Cancelled(_) => "the operation was cancelled".to_owned(),
            Self::ExternalService { service, .. } => {
                format!("upstream dependency '{service}' is temporarily unavailable")
            },
            Self::Database(_) | Self::Messaging(_) | Self::Serialization(_) | Self::Internal(_) => {
                "an internal error occurred".to_owned()
            },
            Self::ExternalFailure { message, .. } => message.clone(),
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
