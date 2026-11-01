//! Framed payload codecs with hard size caps.
//!
//! Policy (see the platform docs): payloads are size-capped and validated by
//! `messaging::codec`; malformed frames are NAK'ed and **never retried
//! blindly**. All errors surfaced here are therefore non-retriable by
//! construction — [`CodecError::is_retriable`] returns `false` so dispatcher
//! code can route straight to dead-letter.

use std::fmt;

use mas_events::envelope::EventEnvelope;
use serde::{de::DeserializeOwned, Serialize};

/// Default wire payload ceiling: 1 MiB (matches the JetStream default
/// `max_payload` of 1 MiB for typical event traffic).
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

/// Smallest accepted ceiling: 1 KiB — anything lower cannot carry even a
/// minimal envelope.
pub const MIN_MAX_PAYLOAD_BYTES: usize = 1024;

/// Largest accepted ceiling: 64 MiB — above this, payloads belong in object
/// storage with a claim check, not on the bus.
pub const MAX_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Codec configuration. `max_payload_bytes` is a *hard* cap enforced on both
/// encode and decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodecConfig {
    max_payload_bytes: usize,
}

impl Default for CodecConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        }
    }
}

impl CodecConfig {
    /// Builds a config after validating the ceiling against sane bounds.
    pub fn new(max_payload_bytes: usize) -> Result<Self, CodecError> {
        if !(MIN_MAX_PAYLOAD_BYTES..=MAX_MAX_PAYLOAD_BYTES).contains(&max_payload_bytes) {
            return Err(CodecError::malformed(format!(
                "max_payload_bytes {max_payload_bytes} out of bounds \
                 ({MIN_MAX_PAYLOAD_BYTES}..={MAX_MAX_PAYLOAD_BYTES})"
            )));
        }
        Ok(Self { max_payload_bytes })
    }

    /// The configured hard cap.
    #[must_use]
    pub fn max_payload_bytes(&self) -> usize {
        self.max_payload_bytes
    }
}

/// A codec fault. Both variants denote programmer/data errors that retrying
/// will not fix: the caller should NAK/term and dead-letter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Serialized payload exceeds the configured hard cap.
    Oversize {
        /// Actual payload length in bytes.
        size: usize,
        /// Configured cap in bytes.
        limit: usize,
    },
    /// Payload is not decodable (truncated, wrong content type, schema drift).
    Malformed {
        /// Human-readable reason, safe for logs (never echoes raw payload).
        reason: String,
    },
}

impl CodecError {
    /// Convenience constructor for [`CodecError::Malformed`].
    pub fn malformed(reason: impl Into<String>) -> Self {
        Self::Malformed {
            reason: reason.into(),
        }
    }

    /// Codec errors never benefit from retrying the same bytes.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        false
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Oversize { size, limit } => {
                write!(f, "payload of {size} bytes exceeds limit of {limit} bytes")
            },
            Self::Malformed { reason } => write!(f, "malformed payload: {reason}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<CodecError> for mas_common::error::AppError {
    fn from(value: CodecError) -> Self {
        mas_common::error::AppError::validation(value.to_string())
    }
}

/// Enforces a size ceiling on a byte slice.
pub fn enforce_size_limit(bytes: &[u8], limit: usize) -> Result<(), CodecError> {
    if bytes.len() > limit {
        return Err(CodecError::Oversize {
            size: bytes.len(),
            limit,
        });
    }
    Ok(())
}

/// A bidirectional codec for [`EventEnvelope`] frames.
pub trait EventFrameCodec: Send + Sync + fmt::Debug {
    /// The wire content type (`application/json`, `application/protobuf`, ...).
    fn content_type(&self) -> &'static str;

    /// Serializes an envelope, enforcing the size cap on the result.
    fn encode(&self, envelope: &EventEnvelope) -> Result<Vec<u8>, CodecError>;

    /// Deserializes an envelope, enforcing the size cap before parsing.
    fn decode(&self, frame: &[u8]) -> Result<EventEnvelope, CodecError>;
}

/// Canonical JSON codec (`application/json`) — the default wire format.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonEventCodec {
    config: CodecConfig,
}

impl JsonEventCodec {
    /// Builds a codec with an explicit configuration.
    #[must_use]
    pub fn new(config: CodecConfig) -> Self {
        Self { config }
    }

    /// Access to the validated configuration.
    #[must_use]
    pub fn config(&self) -> CodecConfig {
        self.config
    }

    /// Encodes any serializable value, enforcing the size cap on the output.
    pub fn encode_value<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|err| CodecError::malformed(format!("json encode failed: {err}")))?;
        enforce_size_limit(&bytes, self.config.max_payload_bytes())?;
        Ok(bytes)
    }

    /// Decodes any deserializable value, enforcing the size cap first so a
    /// hostile `Content-Length` cannot force a large allocation.
    pub fn decode_value<T: DeserializeOwned>(&self, frame: &[u8]) -> Result<T, CodecError> {
        enforce_size_limit(frame, self.config.max_payload_bytes())?;
        serde_json::from_slice(frame).map_err(|err| {
            CodecError::malformed(format!(
                "json decode failed at line {} column {}: {}",
                err.line(),
                err.column(),
                err.classify_ref()
            ))
        })
    }
}

/// Small helper that classifies `serde_json` errors with a stable word so log
/// lines stay greppable without leaking raw payload text.
trait ClassifyRef {
    fn classify_ref(&self) -> &'static str;
}

impl ClassifyRef for serde_json::Error {
    fn classify_ref(&self) -> &'static str {
        match self.classify() {
            serde_json::error::Category::Io => "io",
            serde_json::error::Category::Syntax => "syntax",
            serde_json::error::Category::Data => "data",
            serde_json::error::Category::Eof => "eof",
        }
    }
}

impl EventFrameCodec for JsonEventCodec {
    fn content_type(&self) -> &'static str {
        "application/json"
    }

    fn encode(&self, envelope: &EventEnvelope) -> Result<Vec<u8>, CodecError> {
        self.encode_value(envelope)
    }

    fn decode(&self, frame: &[u8]) -> Result<EventEnvelope, CodecError> {
        self.decode_value(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;
    use mas_events::envelope::EventEnvelope;

    fn sample_envelope() -> EventEnvelope {
        EventEnvelope::builder("task.submitted", 1, "task", "task-42", TenantId::new())
            .expect("builder")
            .with_correlation("corr-1")
            .expect("correlation")
            .with_payload(serde_json::json!({"title": "hello"}))
            .build()
            .expect("envelope")
    }

    #[test]
    fn config_bounds_are_enforced() {
        assert!(CodecConfig::new(10).is_err());
        assert!(CodecConfig::new(MAX_MAX_PAYLOAD_BYTES + 1).is_err());
        let cfg = CodecConfig::new(2048).expect("in bounds");
        assert_eq!(cfg.max_payload_bytes(), 2048);
    }

    #[test]
    fn json_roundtrip_preserves_envelope() {
        let codec = JsonEventCodec::default();
        assert_eq!(codec.content_type(), "application/json");
        let envelope = sample_envelope();
        let frame = codec.encode(&envelope).expect("encode");
        let decoded = codec.decode(&frame).expect("decode");
        assert_eq!(decoded.id, envelope.id);
        assert_eq!(decoded.event_type, "task.submitted");
        assert_eq!(decoded.correlation_id, "corr-1");
        assert_eq!(decoded.payload, envelope.payload);
    }

    #[test]
    fn oversize_and_malformed_are_non_retriable() {
        let codec = JsonEventCodec::new(CodecConfig::new(MIN_MAX_PAYLOAD_BYTES).expect("cfg"));
        let envelope = EventEnvelope::builder("task.submitted", 1, "task", "t-9", TenantId::new())
            .expect("builder")
            .with_correlation("c")
            .expect("correlation")
            .with_payload(serde_json::json!({"blob": "x".repeat(4096)}))
            .build()
            .expect("envelope");
        let err = codec.encode(&envelope).expect_err("must exceed 1 KiB");
        assert!(matches!(err, CodecError::Oversize { .. }));
        assert!(!err.is_retriable());

        let err = codec
            .decode(b"{\"not_an_envelope\":")
            .expect_err("truncated json");
        assert!(matches!(err, CodecError::Malformed { .. }));
        assert!(!err.is_retriable());

        let low = CodecConfig::new(MIN_MAX_PAYLOAD_BYTES).expect("cfg");
        let junk = vec![b'x'; MIN_MAX_PAYLOAD_BYTES + 1];
        let err = JsonEventCodec::new(low)
            .decode(&junk)
            .expect_err("oversize in");
        assert!(
            matches!(err, CodecError::Oversize { size, .. } if size == MIN_MAX_PAYLOAD_BYTES + 1)
        );
    }
}
