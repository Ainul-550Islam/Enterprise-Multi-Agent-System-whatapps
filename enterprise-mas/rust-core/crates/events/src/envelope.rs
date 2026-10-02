//! The immutable event envelope: ids, type + version, aggregate, tenant,
//! causation/correlation, payload + metadata.

use mas_common::error::AppError;
use mas_common::ids::{EventId, OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;

/// Event type envelope rules: `kind` is `lowercase.dot.separated` segments,
/// 4..=128 chars, at most 6 segments, and no ending colon/dot.
pub fn validate_event_type(kind: &str) -> Result<()> {
    validation::validate_length("event_type", kind, 4, 128)?;
    let valid = kind.split('.').take(7).enumerate().all(|(i, segment)| {
        i < 6
            && !segment.is_empty()
            && segment
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
    });
    if !valid || kind.matches('.').count() < 1 {
        return Err(AppError::invalid_field(
            "event_type",
            "invalid_format",
            format!("'{kind}' is not a valid event type (expect e.g. 'execution.started')"),
        ));
    }
    Ok(())
}

/// Type error reason — the exact rejection path in `validate_event_type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventTypeError {
    TooShort,
    TooLong,
    InvalidCharacters,
    TooManySegments,
    MissingDot,
}

/// Structural metadata riders (tracing, replay marks).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMetadata {
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Replay marker (event originally published, then re-sent by tooling).
    #[serde(default)]
    pub replay: bool,
}

/// Full event envelope. Payloads carry ids/verdicts, not mutable state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: EventId,
    /// Event type (e.g. `execution.started`).
    pub event_type: String,
    /// Schema version of this event type (payload-stable contract).
    pub event_version: u32,
    /// Aggregate kind (`execution`, `task`, `policy`, …).
    pub aggregate_type: String,
    /// Aggregate id as string (uuid etc.).
    pub aggregate_id: String,
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    pub occurred_at: Timestamp,
    /// Trace linkage of the triggering action.
    pub correlation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    #[serde(default)]
    pub metadata: EventMetadata,
    pub payload: Value,
}

impl EventEnvelope {
    /// Builder starter — required identity comes first.
    pub fn builder(
        event_type: impl Into<String>,
        event_version: u32,
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        tenant_id: TenantId,
    ) -> Result<EventEnvelopeBuilder> {
        EventEnvelopeBuilder::new(
            event_type,
            event_version,
            aggregate_type,
            aggregate_id,
            tenant_id,
        )
    }

    /// Deterministic JSON rendering (for hashing, DLQ lookups).
    pub fn to_json_string(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(|e| AppError::serialization(format!("event envelope: {e}")))
    }

    /// Approximate byte size (queue-depth accounting).
    pub fn size_hint(&self) -> usize {
        self.payload.to_string().len() + self.event_type.len() + self.aggregate_id.len() + 128
    }
}

impl fmt::Display for EventEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[v{}] {}::{} (tenant {})",
            self.event_type,
            self.event_version,
            self.aggregate_type,
            self.aggregate_id,
            self.tenant_id
        )
    }
}

/// Builder for [`EventEnvelope`].
#[derive(Debug, Clone)]
pub struct EventEnvelopeBuilder {
    envelope: EventEnvelope,
}

impl EventEnvelopeBuilder {
    fn new(
        event_type: impl Into<String>,
        event_version: u32,
        aggregate_type: impl Into<String>,
        aggregate_id: impl Into<String>,
        tenant_id: TenantId,
    ) -> Result<Self> {
        let event_type = event_type.into();
        let aggregate_type = aggregate_type.into();
        let aggregate_id = aggregate_id.into();
        validate_event_type(&event_type)?;
        validation::validate_non_empty("aggregate_type", &aggregate_type)?;
        validation::validate_non_empty("aggregate_id", &aggregate_id)?;
        if event_version == 0 {
            return Err(AppError::invalid_field(
                "event_version",
                "out_of_range",
                "event versions start at 1",
            ));
        }
        Ok(Self {
            envelope: EventEnvelope {
                id: EventId::new(),
                event_type,
                event_version,
                aggregate_type,
                aggregate_id,
                tenant_id,
                organization_id: None,
                occurred_at: Timestamp::now(),
                correlation_id: String::new(),
                causation_id: None,
                actor: None,
                metadata: EventMetadata::default(),
                payload: Value::Null,
            },
        })
    }

    /// The pre-assigned event id (before [`Self::build`]).
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.envelope.id
    }

    #[must_use]
    pub const fn with_organization(mut self, organization_id: OrganizationId) -> Self {
        self.envelope.organization_id = Some(organization_id);
        self
    }

    #[must_use]
    pub const fn with_id(mut self, id: EventId) -> Self {
        self.envelope.id = id;
        self
    }

    #[must_use]
    pub const fn occurred_at(mut self, at: Timestamp) -> Self {
        self.envelope.occurred_at = at;
        self
    }

    pub fn with_correlation(mut self, correlation_id: impl Into<String>) -> Result<Self> {
        let correlation_id = correlation_id.into();
        validation::validate_non_empty("correlation_id", &correlation_id)?;
        self.envelope.correlation_id = correlation_id;
        Ok(self)
    }

    #[must_use]
    pub fn with_causation(mut self, causation_id: impl Into<String>) -> Self {
        self.envelope.causation_id = Some(causation_id.into());
        self
    }

    #[must_use]
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.envelope.actor = Some(actor.into());
        self
    }

    #[must_use]
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.envelope.payload = payload;
        self
    }

    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.envelope
            .metadata
            .headers
            .insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub const fn replay(mut self) -> Self {
        self.envelope.metadata.replay = true;
        self
    }

    pub fn build(self) -> Result<EventEnvelope> {
        if self.envelope.correlation_id.is_empty() {
            return Err(AppError::invalid_field(
                "correlation_id",
                "required",
                "events must carry a correlation id",
            ));
        }
        Ok(self.envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> EventEnvelopeBuilder {
        EventEnvelope::builder(
            "execution.started",
            1,
            "execution",
            "01HZ-EXEC",
            TenantId::new(),
        )
        .expect("builder")
    }

    #[test]
    fn builds_a_complete_envelope() {
        let envelope = base()
            .with_actor("alice")
            .with_correlation("corr-1")
            .expect("correlation")
            .with_causation("cause-9")
            .with_organization(OrganizationId::new())
            .with_payload(serde_json::json!({"status": "running"}))
            .with_header("trace", "abc=1")
            .build()
            .expect("build");
        assert_eq!(envelope.event_type, "execution.started");
        assert_eq!(envelope.aggregate_type, "execution");
        assert_eq!(envelope.correlation_id, "corr-1");
        assert_eq!(envelope.causation_id.as_deref(), Some("cause-9"));
        assert!(envelope.organization_id.is_some());
        assert!(envelope.actor.is_some());
        assert_eq!(
            envelope.metadata.headers.get("trace").map(String::as_str),
            Some("abc=1")
        );
        assert!(!envelope.metadata.replay);
        assert!(envelope.size_hint() > 0);
        assert!(envelope
            .to_json_string()
            .expect("json")
            .contains("execution.started"));
        let display = format!("{envelope}");
        assert!(display.contains("execution.started"));
        assert!(display.contains("01HZ-EXEC"));
    }

    #[test]
    fn builder_validates_type_and_required_fields() {
        for bad in [
            "a",
            "X.y",
            "execution",
            "execution..started",
            "execution.started.x.z.w.q.r",
            &"a.b".repeat(80),
        ] {
            assert!(
                EventEnvelope::builder(bad, 1, "agg", "id-1", TenantId::new()).is_err(),
                "reject {bad:?}"
            );
        }
        assert!(
            EventEnvelope::builder("execution.started", 0, "agg", "id", TenantId::new()).is_err()
        );
        assert!(base().build().is_err(), "correlation is required");
        assert!(base().with_correlation(" ").is_err());
    }

    #[test]
    fn replay_marker_and_version_survive() {
        let envelope = base()
            .with_correlation("c")
            .expect("c")
            .replay()
            .build()
            .expect("build");
        assert!(envelope.metadata.replay);
        assert_eq!(envelope.event_version, 1);
        // Re-serialization keeps semantics
        let json = envelope.to_json_string().expect("json");
        let back: EventEnvelope = serde_json::from_str(&json).expect("parse");
        assert_eq!(back, envelope);
    }
}
