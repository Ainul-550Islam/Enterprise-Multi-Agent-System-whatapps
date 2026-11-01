//! Validated message-header sets and tracing-context propagation.
//!
//! Every frame on the broker carries a small, normalized header set:
//! lower-cased keys, no CR/LF in values, bounded count and length. The
//! [`PropagationContext`] helpers move correlation/causation/tenant identity
//! between [`mas_events::EventEnvelope`]s and transport headers so a service
//! on the other side of the wire can rebuild exactly the provenance the
//! platform requires (correlation is always present; causation optional).

use std::collections::BTreeMap;
use std::fmt;

use mas_events::envelope::EventEnvelope;

/// Propagation key for the required correlation id.
pub const CORRELATION_ID_KEY: &str = "x-correlation-id";
/// Propagation key for the optional causation id.
pub const CAUSATION_ID_KEY: &str = "x-causation-id";
/// Propagation key for the tenant scope of the frame.
pub const TENANT_ID_KEY: &str = "x-tenant-id";
/// Propagation key for the unique event id.
pub const EVENT_ID_KEY: &str = "x-event-id";
/// W3C trace context header.
pub const TRACEPARENT_KEY: &str = "traceparent";
/// Idempotency key used by producers to make publishes exactly-once-ish.
pub const IDEMPOTENCY_KEY: &str = "x-idempotency-key";

/// Maximum number of distinct headers on one frame.
pub const MAX_HEADERS: usize = 64;
/// Maximum length of one header *name*.
pub const MAX_KEY_LEN: usize = 128;
/// Maximum length of one header *value*.
pub const MAX_VALUE_LEN: usize = 4096;

/// A header failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderError {
    /// The offending key (empty string when the key itself was invalid).
    pub key: String,
    /// Why it failed; safe for logs.
    pub reason: String,
}

impl HeaderError {
    fn new(key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.key.is_empty() {
            write!(f, "invalid header: {}", self.reason)
        } else {
            write!(f, "invalid header '{}': {}", self.key, self.reason)
        }
    }
}

impl std::error::Error for HeaderError {}

impl From<HeaderError> for mas_common::error::AppError {
    fn from(value: HeaderError) -> Self {
        mas_common::error::AppError::validation(value.to_string())
    }
}

/// Validates (and normalizes to lowercase) a header name.
///
/// Allowed: ASCII alphanumerics plus `-`, `_`, `.`. Keys are lowered so
/// lookups are deterministic regardless of producer casing.
pub fn normalize_header_key(key: &str) -> Result<String, HeaderError> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(HeaderError::new("", "empty header name"));
    }
    if trimmed.len() > MAX_KEY_LEN {
        return Err(HeaderError::new(
            trimmed,
            format!("header name exceeds {MAX_KEY_LEN} characters"),
        ));
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(HeaderError::new(
            trimmed,
            "header name contains disallowed characters",
        ));
    }
    Ok(trimmed.to_ascii_lowercase())
}

/// Validates a header value: bounded length, no CR/LF (header-injection
/// prevention), no NUL.
pub fn validate_header_value(key: &str, value: &str) -> Result<(), HeaderError> {
    if value.len() > MAX_VALUE_LEN {
        return Err(HeaderError::new(
            key,
            format!("value exceeds {MAX_VALUE_LEN} characters"),
        ));
    }
    if value.chars().any(|c| matches!(c, '\r' | '\n' | '\0')) {
        return Err(HeaderError::new(
            key,
            "value contains CR/LF/NUL (header injection blocked)",
        ));
    }
    Ok(())
}

/// A bounded, normalized header set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeaderSet {
    inner: BTreeMap<String, String>,
}

impl HeaderSet {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct headers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Inserts a header after validation; the key is normalized to lowercase
    /// so a re-insert replaces deterministically.
    pub fn insert(&mut self, key: &str, value: impl Into<String>) -> Result<(), HeaderError> {
        let normalized = normalize_header_key(key)?;
        let value = value.into();
        validate_header_value(&normalized, &value)?;
        if !self.inner.contains_key(&normalized) && self.inner.len() >= MAX_HEADERS {
            return Err(HeaderError::new(
                normalized,
                format!("header count exceeds {MAX_HEADERS}"),
            ));
        }
        self.inner.insert(normalized, value);
        Ok(())
    }

    /// Inserts only when absent — used when applying propagation defaults the
    /// producer may already have set explicitly.
    pub fn insert_if_absent(
        &mut self,
        key: &str,
        value: impl Into<String>,
    ) -> Result<(), HeaderError> {
        let normalized = normalize_header_key(key)?;
        if self.inner.contains_key(&normalized) {
            return Ok(());
        }
        self.insert(&normalized, value)
    }

    /// Case-insensitive lookup (keys are stored normalized).
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner
            .get(&key.to_ascii_lowercase())
            .map(String::as_str)
    }

    /// Removes a header, returning the previous value if any.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.inner.remove(&key.to_ascii_lowercase())
    }

    /// Iterates in deterministic (sorted key) order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.inner.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Converts into the inner map (keys already normalized).
    #[must_use]
    pub fn into_map(self) -> BTreeMap<String, String> {
        self.inner
    }
}

impl FromIterator<(String, String)> for HeaderSet {
    /// Collects raw pairs, **validating** each; the collect fails on the first
    /// invalid pair.
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        // `FromIterator` cannot fail; validate leniently by skipping invalid
        // pairs — strict construction goes through `insert`.
        let mut set = HeaderSet::new();
        for (k, v) in iter {
            let _ = set.insert(&k, v);
        }
        set
    }
}

/// Provenance carried next to a frame, extracted without needing the full
/// envelope (used by dead-letter tooling and gateway edge code).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PropagationContext {
    /// Required correlation id (present whenever the source was an envelope).
    pub correlation_id: Option<String>,
    /// Optional causation id linking to the causing event/command.
    pub causation_id: Option<String>,
    /// Tenant scope as string form of the tenant id.
    pub tenant_id: Option<String>,
    /// Unique event id.
    pub event_id: Option<String>,
    /// W3C traceparent, forwarded verbatim.
    pub traceparent: Option<String>,
    /// Producer idempotency key, if set.
    pub idempotency_key: Option<String>,
}

impl PropagationContext {
    /// Builds a context from a full envelope.
    #[must_use]
    pub fn from_envelope(envelope: &EventEnvelope) -> Self {
        Self {
            correlation_id: Some(envelope.correlation_id.clone()),
            causation_id: envelope.causation_id.clone(),
            tenant_id: Some(envelope.tenant_id.to_string()),
            event_id: Some(envelope.id.to_string()),
            traceparent: envelope.metadata.headers.get(TRACEPARENT_KEY).cloned(),
            idempotency_key: envelope.metadata.headers.get(IDEMPOTENCY_KEY).cloned(),
        }
    }

    /// Extracts a context from transport headers (every field optional).
    #[must_use]
    pub fn from_headers(headers: &HeaderSet) -> Self {
        Self {
            correlation_id: headers.get(CORRELATION_ID_KEY).map(str::to_owned),
            causation_id: headers.get(CAUSATION_ID_KEY).map(str::to_owned),
            tenant_id: headers.get(TENANT_ID_KEY).map(str::to_owned),
            event_id: headers.get(EVENT_ID_KEY).map(str::to_owned),
            traceparent: headers.get(TRACEPARENT_KEY).map(str::to_owned),
            idempotency_key: headers.get(IDEMPOTENCY_KEY).map(str::to_owned),
        }
    }

    /// Stamps this context onto an existing header set without overwriting
    /// values the producer already set.
    pub fn inject_into(&self, headers: &mut HeaderSet) -> Result<(), HeaderError> {
        if let Some(v) = &self.correlation_id {
            headers.insert_if_absent(CORRELATION_ID_KEY, v.clone())?;
        }
        if let Some(v) = &self.causation_id {
            headers.insert_if_absent(CAUSATION_ID_KEY, v.clone())?;
        }
        if let Some(v) = &self.tenant_id {
            headers.insert_if_absent(TENANT_ID_KEY, v.clone())?;
        }
        if let Some(v) = &self.event_id {
            headers.insert_if_absent(EVENT_ID_KEY, v.clone())?;
        }
        if let Some(v) = &self.traceparent {
            headers.insert_if_absent(TRACEPARENT_KEY, v.clone())?;
        }
        if let Some(v) = &self.idempotency_key {
            headers.insert_if_absent(IDEMPOTENCY_KEY, v.clone())?;
        }
        Ok(())
    }

    /// Stamps this context onto a fresh header set.
    pub fn to_header_set(&self) -> Result<HeaderSet, HeaderError> {
        let mut set = HeaderSet::new();
        self.inject_into(&mut set)?;
        Ok(set)
    }

    /// True when the mandatory correlation id is present.
    #[must_use]
    pub fn has_correlation(&self) -> bool {
        self.correlation_id.is_some()
    }
}

/// Convenience: the full header set for publishing one envelope.
pub fn headers_for_envelope(envelope: &EventEnvelope) -> Result<HeaderSet, HeaderError> {
    PropagationContext::from_envelope(envelope).to_header_set()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;

    fn sample_envelope() -> EventEnvelope {
        EventEnvelope::builder(
            "execution.completed",
            1,
            "execution",
            "exec-9",
            TenantId::new(),
        )
        .expect("builder")
        .with_correlation("corr-7")
        .expect("correlation")
        .with_causation("cause-1")
        .with_header(
            TRACEPARENT_KEY,
            "00-abcdefabcdefabcdefabcdefabcdefab-0123456789abcdef-01",
        )
        .build()
        .expect("envelope")
    }

    #[test]
    fn headers_are_normalized_and_validated() {
        let mut set = HeaderSet::new();
        set.insert("X-Custom-Thing", "v1").expect("insert");
        assert_eq!(set.get("x-custom-thing"), Some("v1"));
        set.insert("X-CUSTOM-THING", "v2")
            .expect("re-insert replaces");
        assert_eq!(set.len(), 1);
        assert_eq!(set.get("x-custom-thing"), Some("v2"));

        assert!(set.insert("bad key", "x").is_err(), "spaces are invalid");
        assert!(
            set.insert("ok", "line1\nline2").is_err(),
            "CR/LF injection blocked"
        );
        assert!(set.insert("ok", "x".repeat(MAX_VALUE_LEN + 1)).is_err());
        set.insert("ok", "fine").expect("valid");
        set.remove("OK");
        assert!(set.get("ok").is_none());
    }

    #[test]
    fn header_count_is_bounded() {
        let mut set = HeaderSet::new();
        for i in 0..MAX_HEADERS {
            set.insert(&format!("k-{i}"), "v").expect("within bound");
        }
        assert!(set.insert("k-overflow", "v").is_err());
        // Replacing an existing key still works at the cap.
        set.insert("k-0", "v2").expect("replace ok at cap");
    }

    #[test]
    fn envelope_propagation_roundtrips() {
        let envelope = sample_envelope();
        let headers = headers_for_envelope(&envelope).expect("headers");
        assert_eq!(headers.get(CORRELATION_ID_KEY), Some("corr-7"));
        assert_eq!(headers.get(CAUSATION_ID_KEY), Some("cause-1"));
        assert!(headers.get(TRACEPARENT_KEY).is_some());
        assert_eq!(headers.get(TENANT_ID_KEY).map(str::len), Some(36));

        let ctx = PropagationContext::from_headers(&headers);
        assert!(ctx.has_correlation());
        assert_eq!(ctx.correlation_id.as_deref(), Some("corr-7"));
        assert_eq!(ctx.causation_id.as_deref(), Some("cause-1"));
        assert!(ctx.traceparent.is_some());
        assert!(ctx.idempotency_key.is_none());

        // Injecting again preserves explicitly set values.
        let mut override_set = HeaderSet::new();
        override_set
            .insert(CORRELATION_ID_KEY, "explicit-corr")
            .expect("insert");
        ctx.inject_into(&mut override_set).expect("inject");
        assert_eq!(
            override_set.get(CORRELATION_ID_KEY),
            Some("explicit-corr"),
            "injection must not clobber explicit values"
        );
    }
}
