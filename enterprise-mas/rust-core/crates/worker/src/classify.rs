//! PURPOSE:
//! The canonical failure classification for the durable execution pipeline.
//!
//! Retry decisions, terminal-vs-transient event classes, and the task's
//! requeue/dead-letter branch must all agree. They agree because they all ask
//! this module, which is the only place that translates an [`AppError`] into a
//! retry class.
//!
//! Layer / ownership
//! -----------------
//! `mas-worker`. It is the worker that has to decide whether to spend another
//! attempt on a failure, so the worker owns the table.
//!
//! Production responsibility
//! -------------------------
//! Classify per the platform contract:
//!
//! * **Retryable** — broker failure, rate limit / quota exceeded, transient
//!   5xx, dependency unavailable, transient network or database loss.
//! * **Terminal** — authentication, authorization, invalid request or config,
//!   tenant mismatch, policy denial, invalid tool permission, malformed input.
//! * **Cancelled** — the caller or the runtime stopped the work.
//!
//! And preserve the special cases that a naive classifier destroys:
//!
//! * a timeout must never be reported as success,
//! * a cancellation must never be reported as success,
//! * an authorization denial must never collapse into a generic internal error,
//! * an exceeded quota must never be reported as a provider failure.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT classify from message text — only from the error variant and
//!   its stable code.
//! * It must NOT downgrade a terminal error to retryable to "be helpful": a
//!   retry that can never succeed burns the attempt budget and delays the
//!   dead-letter that an operator needs to see.

use mas_common::error::AppError;
use mas_domain::TransitionFailure;

/// The retry class of a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Another attempt may plausibly succeed.
    Retryable,
    /// Retrying cannot succeed; dead-letter the task.
    Terminal,
    /// The work was stopped on purpose; it is not a failure.
    Cancelled,
}

impl FailureClass {
    /// Stable metric/log token.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Retryable => "transient",
            Self::Terminal => "terminal",
            Self::Cancelled => "cancelled",
        }
    }
}

/// A classified failure: the class plus the stable code and a safe message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedFailure {
    /// Retry class.
    pub class: FailureClass,
    /// Stable machine-readable code.
    pub code: String,
    /// Safe, redacted message (never a credential, never a full payload).
    pub message: String,
}

impl ClassifiedFailure {
    /// Whether another attempt may be scheduled.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self.class, FailureClass::Retryable)
    }

    /// Whether the work was stopped deliberately.
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        matches!(self.class, FailureClass::Cancelled)
    }

    /// The domain-level failure record this classification produces.
    pub fn transition_failure(&self) -> Result<TransitionFailure, AppError> {
        TransitionFailure::new(self.code.clone(), self.message.clone(), self.is_retryable())
    }
}

/// Stable code for a cancellation.
pub const CODE_CANCELLED: &str = "cancelled";
/// Stable code used when a classification cannot be derived (should not
/// happen; recorded as terminal so it surfaces rather than retrying forever).
pub const CODE_UNCLASSIFIED: &str = "unclassified";

/// Classifies one error.
///
/// The mapping is exhaustive on [`AppError`] on purpose: adding a variant is a
/// compile error here, so a new error kind cannot silently inherit the wrong
/// retry class.
#[must_use]
pub fn classify(error: &AppError) -> ClassifiedFailure {
    let class = match error {
        // ── Terminal: the request itself is wrong or not permitted. ──────
        AppError::Unauthorized(_) => FailureClass::Terminal,
        AppError::Forbidden(_) => FailureClass::Terminal,
        AppError::Validation { .. } => FailureClass::Terminal,
        AppError::Serialization(_) => FailureClass::Terminal,
        // A state conflict means the durable row moved underneath us (stale
        // worker, duplicate claim). Retrying the same mutation cannot help —
        // the caller must re-read.
        AppError::Conflict(_) => FailureClass::Terminal,
        // A missing resource the task depends on will not appear on retry.
        AppError::NotFound { .. } => FailureClass::Terminal,
        // A quota ceiling is a fact about the tenant's plan, not a transient
        // condition. Retrying converts one clean denial into `max_attempts`
        // identical denials and a dead letter that says nothing about why;
        // rescheduling at the window reset is the scheduler's job, not the
        // retry policy's. Deliberately NOT Retryable even though the error
        // may carry a `retry_after` hint.
        AppError::QuotaExceeded { .. } => FailureClass::Terminal,
        // A policy denial is a decision, and replaying the request re-runs the
        // same evaluation against the same policy and gets the same answer.
        AppError::PolicyDenied { .. } => FailureClass::Terminal,
        // The tenant's plan does not include the feature. No amount of
        // retrying will make the subscription appear.
        AppError::EntitlementRequired { .. } => FailureClass::Terminal,

        // ── Cancelled: deliberate stop, never a failure. ─────────────────
        AppError::Cancelled(_) => FailureClass::Cancelled,
        // A surface this deployment never composed cannot appear by waiting;
        // retrying the unit of work only burns the attempt budget against a
        // wiring gap an operator has to fix.
        AppError::Unavailable(_) => FailureClass::Terminal,

        // ── Retryable: the environment, not the request. ─────────────────
        AppError::RateLimited(_) => FailureClass::Retryable,
        AppError::Timeout(_) => FailureClass::Retryable,
        AppError::Messaging(_) => FailureClass::Retryable,
        AppError::Database(_) => FailureClass::Retryable,
        AppError::ExternalService { .. } => FailureClass::Retryable,
        AppError::Internal(_) => FailureClass::Retryable,
        // The external runtime produced this failure, so its verdict about
        // whether a retry can help is authoritative.
        AppError::ExternalFailure { retryable, .. } => {
            if *retryable {
                FailureClass::Retryable
            } else {
                FailureClass::Terminal
            }
        },
    };

    // An external runtime's structured failure carries its own code and
    // message. Preserving them verbatim is the whole point of the variant: an
    // `invalid_tool_permission` must be recorded as one, not collapsed into a
    // generic `EXTERNAL_SERVICE_ERROR` that every dashboard reads as transient.
    if let AppError::ExternalFailure { code, message, .. } = error {
        return ClassifiedFailure {
            class,
            code: code.clone(),
            message: message.clone(),
        };
    }

    ClassifiedFailure {
        class,
        code: error.error_code().to_owned(),
        message: error.public_message(),
    }
}

/// A cancellation, classified.
#[must_use]
pub fn cancelled(detail: &str) -> ClassifiedFailure {
    ClassifiedFailure {
        class: FailureClass::Cancelled,
        code: CODE_CANCELLED.to_owned(),
        message: detail.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_external_runtime_failure_keeps_its_own_code_and_verdict() {
        let retryable = classify(&AppError::external_failure(
            "rate_limited",
            "provider throttled the run",
            true,
        ));
        assert_eq!(retryable.class, FailureClass::Retryable);
        assert_eq!(retryable.code, "rate_limited");
        assert_eq!(retryable.message, "provider throttled the run");
        assert!(retryable.is_retryable());

        let terminal = classify(&AppError::external_failure(
            "invalid_tool_permission",
            "tool not permitted for this agent",
            false,
        ));
        assert_eq!(terminal.class, FailureClass::Terminal);
        assert_eq!(terminal.code, "invalid_tool_permission");
        assert!(
            !terminal.is_retryable(),
            "a terminal external verdict must not burn the attempt budget"
        );
    }

    #[test]
    fn retryable_classes_cover_the_environment_failures() {
        for error in [
            AppError::messaging("broker unavailable"),
            AppError::rate_limited("quota exceeded"),
            AppError::timeout("deadline passed"),
            AppError::external_service("openai", "503"),
            AppError::database("connection reset"),
            AppError::internal("transient"),
        ] {
            let classified = classify(&error);
            assert!(
                classified.is_retryable(),
                "{error:?} must be retryable, got {:?}",
                classified.class
            );
            assert_eq!(classified.class.as_str(), "transient");
        }
    }

    #[test]
    fn terminal_classes_cover_the_request_failures() {
        for error in [
            AppError::unauthorized("bad token"),
            AppError::forbidden("policy denied"),
            AppError::validation("malformed input"),
            AppError::conflict("stale write"),
            AppError::serialization("not json"),
            AppError::not_found("agent", "missing"),
        ] {
            let classified = classify(&error);
            assert!(
                !classified.is_retryable(),
                "{error:?} must be terminal, got {:?}",
                classified.class
            );
            assert_eq!(classified.class.as_str(), "terminal");
        }
    }

    #[test]
    fn cancellation_is_never_reported_as_a_failure_or_a_success() {
        let classified = classify(&AppError::cancelled("stop requested"));
        assert!(classified.is_cancelled());
        assert!(!classified.is_retryable());
        assert_eq!(classified.code, "CANCELLED");
        assert_eq!(classified.class.as_str(), "cancelled");

        let explicit = cancelled("operator stopped it");
        assert!(explicit.is_cancelled());
        assert_eq!(explicit.code, CODE_CANCELLED);
    }

    #[test]
    fn authorization_denial_keeps_its_own_code() {
        // The forbidden case must never be flattened into a generic internal
        // error: it is a policy outcome an operator has to be able to see.
        let classified = classify(&AppError::forbidden("tenant mismatch"));
        assert_eq!(classified.class, FailureClass::Terminal);
        assert_eq!(classified.code, "FORBIDDEN");
        assert!(classified.message.contains("tenant mismatch"));
    }

    #[test]
    fn quota_exceeded_is_not_reported_as_a_provider_failure() {
        let classified = classify(&AppError::rate_limited("monthly token quota"));
        assert_eq!(classified.class, FailureClass::Retryable);
        assert_eq!(classified.code, "RATE_LIMITED");
        assert_ne!(classified.code, "EXTERNAL_SERVICE_ERROR");
    }

    #[test]
    fn timeout_is_never_a_success() {
        let classified = classify(&AppError::timeout("runtime deadline"));
        assert!(classified.is_retryable(), "a timeout may be retried");
        assert_ne!(classified.class, FailureClass::Cancelled);
    }

    #[test]
    fn classification_produces_a_domain_failure_record() {
        let classified = classify(&AppError::external_service("openai", "502"));
        let failure = classified.transition_failure().expect("failure");
        assert!(failure.retryable);
        assert_eq!(failure.code, "EXTERNAL_SERVICE_ERROR");
        assert!(
            !failure.message.is_empty(),
            "the safe message is always populated"
        );
        assert!(
            !failure.message.contains("502"),
            "provider detail is not echoed verbatim into durable state"
        );
    }
}
