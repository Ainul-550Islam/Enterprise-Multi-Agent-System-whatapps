//! Result processing + failure taxonomy.
//!
//! Raw runner output is never trusted as-is: [`ResultProcessor`] truncates
//! oversize payloads, redacts secrets/PII before anything lands in step
//! records or events, and extracts failure structure. [`classify_failure`]
//! maps an [`AppError`] to an execution-level [`FailureClass`] that the retry
//! machinery and DLQ routing decisions consume — one taxonomy, everywhere.

use mas_common::constants;
use mas_common::error::AppError;
use mas_common::redaction::SecretRedactor;
use mas_common::result::Result;
use mas_orchestration::node_executor::NodeFailure;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Execution-level failure classes (stable wire values — used by the API,
/// metrics labels and DLQ routing; do not rename).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    /// Node/attempt budget exhausted — next stop is the DLQ.
    RetryExhausted,
    /// Error kind can never succeed on retry (validation, forbidden, …).
    NonRetryable,
    /// Wall-clock or per-attempt timeout.
    Timeout,
    /// Downstream service misbehaving (agent runtime, tool endpoint).
    Upstream,
    /// Breached a platform invariant (bug); page the operator.
    Platform,
    /// Cooperative cancellation landed.
    Cancelled,
    /// Policy denied the action (or requires approval at a place that
    /// cannot provide it).
    PolicyDenied,
    /// A declared budget was spent (steps, tool calls, tokens, wall time).
    ResourceExhausted,
    /// Payload violated platform size/shape rules.
    PayloadRejected,
}

impl FailureClass {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RetryExhausted => "retry_exhausted",
            Self::NonRetryable => "non_retryable",
            Self::Timeout => "timeout",
            Self::Upstream => "upstream",
            Self::Platform => "platform",
            Self::Cancelled => "cancelled",
            Self::PolicyDenied => "policy_denied",
            Self::ResourceExhausted => "resource_exhausted",
            Self::PayloadRejected => "payload_rejected",
        }
    }

    /// Whether another attempt with a different backoff could help.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Upstream | Self::ResourceExhausted
        )
    }

    /// DLQ routing hint for the event/task layer.
    #[must_use]
    pub const fn dlq_topic_suffix(&self) -> &'static str {
        match self {
            Self::RetryExhausted => "exhausted",
            Self::NonRetryable => "poison",
            Self::Timeout => "timeout",
            Self::Upstream => "upstream",
            Self::Platform => "platform",
            Self::Cancelled => "cancelled",
            Self::PolicyDenied => "policy",
            Self::ResourceExhausted => "quota",
            Self::PayloadRejected => "payload",
        }
    }
}

/// Maps an [`AppError`] (or a synthetic failure) to a failure class.
#[must_use]
pub fn classify_failure(error: &AppError) -> FailureClass {
    match error {
        AppError::Cancelled(_) => FailureClass::Cancelled,
        AppError::Timeout(_) => FailureClass::Timeout,
        AppError::RateLimited(_) => FailureClass::ResourceExhausted,
        AppError::ExternalService { .. } | AppError::Messaging(_) => FailureClass::Upstream,
        AppError::Forbidden(_) | AppError::Unauthorized(_) => FailureClass::PolicyDenied,
        AppError::Validation { .. } => FailureClass::NonRetryable,
        AppError::Serialization(_) => FailureClass::PayloadRejected,
        AppError::Database(_) | AppError::Internal(_) | AppError::NotFound { .. } => {
            FailureClass::Platform
        },
        AppError::Conflict(_) => FailureClass::NonRetryable,
    }
}

/// Normalizes raw runner results into storable, safe forms.
///
/// Everything passing through here becomes safe to persist/emit: size-capped,
/// redacted, with failure structure extracted.
#[derive(Debug, Clone)]
pub struct ResultProcessor {
    redactor: SecretRedactor,
    /// Hard cap for stored payloads (kept below the platform maximum).
    max_payload_bytes: usize,
}

impl Default for ResultProcessor {
    fn default() -> Self {
        Self::new(constants::MAX_PAYLOAD_BYTES)
    }
}

impl ResultProcessor {
    pub fn new(max_payload_bytes: usize) -> Self {
        Self {
            redactor: SecretRedactor::default(),
            max_payload_bytes: max_payload_bytes.min(constants::MAX_PAYLOAD_BYTES),
        }
    }

    /// Additional secret patterns (connector-specific token formats) can be
    /// registered without touching the built-ins.
    #[must_use]
    pub fn with_redactor(mut self, redactor: SecretRedactor) -> Self {
        self.redactor = redactor;
        self
    }

    #[must_use]
    pub const fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }

    /// Redacts and (when necessary) replaces an oversized output with a
    /// `{"truncated": true, "original_bytes": n}` marker — storing raw
    /// oversized output is never allowed.
    pub fn prepare_output(&self, output: &Value) -> Value {
        let mut redacted = output.clone();
        self.redactor.redact_value(&mut redacted);
        let size = serde_json::to_vec(&redacted).map_or(usize::MAX, |v| v.len());
        if size <= self.max_payload_bytes {
            return redacted;
        }
        // Find a smaller representation: scalar → shortened string; structured
        // → marker object with metadata only. Depth-first shrink is
        // deliberately *lossy* rather than subtle.
        match &redacted {
            Value::String(s) => {
                let budget = self.max_payload_bytes.saturating_sub(64).max(64);
                let mut shortened: String = s.chars().take(budget / 2).collect();
                shortened.push('…');
                Value::from(shortened)
            },
            _ => serde_json::json!({
                "truncated": true,
                "original_bytes": size,
                "note": "output exceeded the per-step payload cap and was not stored",
            }),
        }
    }

    /// Prepares the *input snapshot* stored alongside a step (`input_ref`
    /// content when inline). Same rules as [`Self::prepare_output`].
    pub fn prepare_input(&self, input: &Value) -> Value {
        self.prepare_output(input)
    }

    /// A redacted, truncated human-safe one-line summary for logs/messages.
    pub fn summarize(&self, value: &Value) -> String {
        let mut redacted = value.clone();
        self.redactor.redact_value(&mut redacted);
        let mut text = match &redacted {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        text.truncate(512);
        text
    }

    /// Serialized size guard with a clean error (callers preferring `Err`
    /// over silent truncation should use this before persisting).
    pub fn checked_serialized_len(&self, value: &Value) -> Result<usize> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| AppError::serialization(format!("payload serialization failed: {e}")))?;
        if bytes.len() > self.max_payload_bytes {
            return Err(AppError::invalid_field(
                "payload",
                "too_large",
                format!(
                    "payload is {} bytes; limit is {}",
                    bytes.len(),
                    self.max_payload_bytes
                ),
            ));
        }
        Ok(bytes.len())
    }

    /// Converts an [`AppError`] into a [`NodeFailure`] for the orchestration
    /// layer, carrying the classifier verdict in the code namespace.
    pub fn failure_for(&self, class: FailureClass, error: &AppError) -> NodeFailure {
        NodeFailure {
            code: format!("{}:{}", class.as_str(), error.error_code().to_lowercase()),
            message: {
                let mut message = error.public_message();
                message.truncate(2048);
                message
            },
            retryable: error.is_retryable(),
            retry_after_hint: error.retry_after_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taxonomy_is_stable_and_consistent() {
        assert_eq!(
            classify_failure(&AppError::timeout("t")),
            FailureClass::Timeout
        );
        assert_eq!(
            classify_failure(&AppError::rate_limited("rl")),
            FailureClass::ResourceExhausted
        );
        assert_eq!(
            classify_failure(&AppError::external_service("svc", "down")),
            FailureClass::Upstream
        );
        assert_eq!(
            classify_failure(&AppError::forbidden("nope")),
            FailureClass::PolicyDenied
        );
        assert_eq!(
            classify_failure(&AppError::internal("bug")),
            FailureClass::Platform
        );
        assert_eq!(
            classify_failure(&AppError::validation("bad input")),
            FailureClass::NonRetryable
        );
        assert_eq!(
            classify_failure(&AppError::cancelled("stop")),
            FailureClass::Cancelled
        );
        assert!(FailureClass::Timeout.is_retryable());
        assert!(!FailureClass::NonRetryable.is_retryable());
        // Wire values are part of the platform contract.
        assert_eq!(FailureClass::RetryExhausted.as_str(), "retry_exhausted");
        assert_eq!(FailureClass::PolicyDenied.dlq_topic_suffix(), "policy");
    }

    #[test]
    fn outputs_are_redacted_and_capped() {
        let processor = ResultProcessor::new(256);
        let output = serde_json::json!({
            "user_email": "admin@example.com",
            "note": "fine",
        });
        let prepared = processor.prepare_output(&output);
        let text = prepared.to_string();
        assert!(
            !text.contains("admin@example.com"),
            "email must be redacted: {text}"
        );
        assert!(text.contains("fine"));

        let oversized = serde_json::json!({"blob": "x".repeat(1024)});
        let prepared = processor.prepare_output(&oversized);
        assert!(prepared.to_string().len() <= 256 + 32);
        let big_object = serde_json::json!({"a": ["y".repeat(1024)]});
        let prepared = processor.prepare_output(&big_object);
        if let Value::Object(map) = &prepared {
            assert_eq!(map.get("truncated"), Some(&Value::from(true)));
        }
    }

    #[test]
    fn summaries_and_failure_mapping() {
        let processor = ResultProcessor::default();
        let summary = processor.summarize(&serde_json::json!({"k": "v".repeat(600)}));
        assert!(summary.len() <= 512);
        let failure = processor.failure_for(FailureClass::Timeout, &AppError::timeout("slow"));
        assert!(failure.code.starts_with("timeout:"));
        assert!(failure.retryable);
        let len = processor
            .checked_serialized_len(&serde_json::json!({"ok": true}))
            .expect("small payload");
        assert!(len > 0);
    }
}
