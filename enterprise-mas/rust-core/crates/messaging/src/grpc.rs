//! Dependency-free gRPC helpers.
//!
//! The platform's gRPC surfaces (worker dispatch, scheduling, CLI control
//! plane) share conventions that do not require a `tonic` dependency to
//! specify or test:
//!
//! * [`GrpcCode`] — the 17 canonical codes and the one blessed mapping from
//!   [`mas_common::error::AppError`];
//! * [`GrpcMetadata`] — gRPC metadata rules (lowercase keys; `-bin` keys carry
//!   raw bytes, everything else printable ASCII);
//! * [`encode_timeout`] / [`parse_timeout`] — the `grpc-timeout` header wire
//!   grammar (`<1-8 digits><unit>` with units `H M S m u n`);
//! * [`TraceContext`] — W3C `traceparent` parsing/formatting for cross-service
//!   spans;
//! * [`inject_standard`] — the whitelist of headers propagated onto outbound
//!   RPC metadata. **Credentials (`authorization`, cookies, API keys) are
//!   deliberately excluded**: internal calls authenticate with workload
//!   identity, not by replaying end-user tokens.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use mas_common::error::AppError;

use crate::headers::{normalize_header_key, HeaderSet};

/// Canonical gRPC status codes (numbers per the gRPC spec).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GrpcCode {
    /// Success.
    Ok,
    /// Caller cancelled the call.
    Cancelled,
    /// Unclassified error.
    Unknown,
    /// Client request failed validation.
    InvalidArgument,
    /// Deadline expired before completion.
    DeadlineExceeded,
    /// Entity not found (existence deliberately concealed → also used for
    /// cross-tenant hits).
    NotFound,
    /// Entity already exists.
    AlreadyExists,
    /// Caller lacks permission for the resource.
    PermissionDenied,
    /// Some resource is exhausted (quota, rate limit).
    ResourceExhausted,
    /// System not in the required state for the operation.
    FailedPrecondition,
    /// Operation aborted (concurrency conflict).
    Aborted,
    /// Operation applied out of range.
    OutOfRange,
    /// Not implemented by this server.
    Unimplemented,
    /// Internal invariant broken.
    Internal,
    /// Service currently unavailable; safe to retry with backoff.
    Unavailable,
    /// Unrecoverable data loss/corruption.
    DataLoss,
    /// Missing/invalid authentication.
    Unauthenticated,
}

impl GrpcCode {
    /// The numeric code on the wire.
    #[must_use]
    pub fn number(self) -> u32 {
        match self {
            Self::Ok => 0,
            Self::Cancelled => 1,
            Self::Unknown => 2,
            Self::InvalidArgument => 3,
            Self::DeadlineExceeded => 4,
            Self::NotFound => 5,
            Self::AlreadyExists => 6,
            Self::PermissionDenied => 7,
            Self::ResourceExhausted => 8,
            Self::FailedPrecondition => 9,
            Self::Aborted => 10,
            Self::OutOfRange => 11,
            Self::Unimplemented => 12,
            Self::Internal => 13,
            Self::Unavailable => 14,
            Self::DataLoss => 15,
            Self::Unauthenticated => 16,
        }
    }

    /// Reverse of [`GrpcCode::number`].
    #[must_use]
    pub fn from_number(number: u32) -> Option<Self> {
        Some(match number {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => return None,
        })
    }

    /// Whether clients may retry (same rules as gRPC libraries use for
    /// transparent retries).
    #[must_use]
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::Cancelled
                | Self::DeadlineExceeded
                | Self::ResourceExhausted
                | Self::Aborted
                | Self::Unavailable
        )
    }

    /// The one blessed mapping from platform errors to gRPC codes.
    #[must_use]
    pub fn from_app_error(error: &AppError) -> Self {
        match error.error_code() {
            "VALIDATION_FAILED" | "SERIALIZATION_ERROR" => Self::InvalidArgument,
            "RESOURCE_NOT_FOUND" => Self::NotFound,
            "UNAUTHENTICATED" => Self::Unauthenticated,
            "FORBIDDEN" => Self::PermissionDenied,
            "CONFLICT" => Self::Aborted,
            "RATE_LIMITED" => Self::ResourceExhausted,
            "TIMEOUT" => Self::DeadlineExceeded,
            "CANCELLED" => Self::Cancelled,
            "DATABASE_ERROR" | "MESSAGING_ERROR" | "EXTERNAL_SERVICE_ERROR" => Self::Unavailable,
            _ => Self::Internal,
        }
    }
}

impl fmt::Display for GrpcCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}({})", self, self.number())
    }
}

/// Metadata carries ASCII or binary (`-bin` suffix) values.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MetadataValue {
    /// Printable ASCII value.
    Ascii(String),
    /// Raw bytes; encoded as base64 only when crossing the wire.
    Binary(Vec<u8>),
}

/// gRPC metadata with enforced key/value rules.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrpcMetadata {
    inner: BTreeMap<String, MetadataValue>,
}

/// Errors raised while building metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataError {
    /// Offending key.
    pub key: String,
    /// Why it was rejected.
    pub reason: String,
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid metadata '{}': {}", self.key, self.reason)
    }
}

impl std::error::Error for MetadataError {}

impl From<MetadataError> for AppError {
    fn from(value: MetadataError) -> Self {
        AppError::validation(value.to_string())
    }
}

impl GrpcMetadata {
    /// Empty metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates a metadata key: lowercase ASCII (alnum + `-_ .`), non-empty.
    /// `-bin` keys are binary carriers.
    fn normalize_key(key: &str) -> Result<String, MetadataError> {
        // `normalize_header_key` applies gRPC-valid characters *and*
        // enforces lowercase storage, which is exactly the metadata rule.
        normalize_header_key(key).map_err(|e| MetadataError {
            key: key.to_owned(),
            reason: e.reason,
        })
    }

    /// Inserts an ASCII value (printable ASCII only, no CR/LF/NUL).
    pub fn insert_ascii(
        &mut self,
        key: &str,
        value: impl Into<String>,
    ) -> Result<(), MetadataError> {
        let key = Self::normalize_key(key)?;
        let value = value.into();
        if key.ends_with("-bin") {
            return Err(MetadataError {
                key,
                reason: "use insert_binary for -bin keys".to_owned(),
            });
        }
        if !value.chars().all(|c| (0x20..=0x7e).contains(&(c as u32))) {
            return Err(MetadataError {
                key,
                reason: "ASCII metadata values must be printable (0x20..=0x7e)".to_owned(),
            });
        }
        self.inner.insert(key, MetadataValue::Ascii(value));
        Ok(())
    }

    /// Inserts raw bytes; the key must end in `-bin`.
    pub fn insert_binary(&mut self, key: &str, value: &[u8]) -> Result<(), MetadataError> {
        let key = Self::normalize_key(key)?;
        if !key.ends_with("-bin") {
            return Err(MetadataError {
                key,
                reason: "binary values require a -bin key suffix".to_owned(),
            });
        }
        self.inner
            .insert(key, MetadataValue::Binary(value.to_vec()));
        Ok(())
    }

    /// Returns the ASCII value, if present and ASCII.
    #[must_use]
    pub fn get_ascii(&self, key: &str) -> Option<&str> {
        match self.inner.get(&key.to_ascii_lowercase()) {
            Some(MetadataValue::Ascii(v)) => Some(v),
            _ => None,
        }
    }

    /// Returns binary bytes, if present and binary.
    #[must_use]
    pub fn get_binary(&self, key: &str) -> Option<&[u8]> {
        match self.inner.get(&key.to_ascii_lowercase()) {
            Some(MetadataValue::Binary(v)) => Some(v),
            _ => None,
        }
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Wire view: ASCII values verbatim, binaries base64-encoded.
    #[must_use]
    pub fn to_wire(&self) -> BTreeMap<String, String> {
        self.inner
            .iter()
            .map(|(k, v)| {
                let wire = match v {
                    MetadataValue::Ascii(s) => s.clone(),
                    MetadataValue::Binary(b) => base64_encode(b),
                };
                (k.clone(), wire)
            })
            .collect()
    }
}

/// The headers forwarded onto outbound internal RPC metadata.
const FORWARDED_HEADER_KEYS: [&str; 6] = [
    crate::headers::CORRELATION_ID_KEY,
    crate::headers::CAUSATION_ID_KEY,
    crate::headers::TENANT_ID_KEY,
    crate::headers::EVENT_ID_KEY,
    crate::headers::TRACEPARENT_KEY,
    crate::headers::IDEMPOTENCY_KEY,
];

/// Copies the propagation whitelist from transport headers into RPC metadata.
/// User credentials are intentionally *not* forwarded (see module docs).
pub fn inject_standard(
    metadata: &mut GrpcMetadata,
    headers: &HeaderSet,
) -> Result<(), MetadataError> {
    for key in FORWARDED_HEADER_KEYS {
        if let Some(value) = headers.get(key) {
            if let Some(existing) = metadata.get_ascii(key) {
                if existing != value {
                    // Explicit per-call metadata wins; never silently downgrade.
                    continue;
                }
                continue;
            }
            metadata.insert_ascii(key, value)?;
        }
    }
    Ok(())
}

/// Encodes a timeout into `grpc-timeout` form (`<1-8 digits><H|M|S|m|u|n>`),
/// using the largest unit that divides the duration exactly (spec behavior).
#[must_use]
pub fn encode_timeout(timeout: Duration) -> String {
    let hours = timeout.as_secs() / 3600;
    if hours > 0
        && hours < 100_000_000
        && timeout.as_secs() % 3600 == 0
        && timeout.subsec_nanos() == 0
    {
        return format!("{hours}H");
    }
    let minutes = timeout.as_secs() / 60;
    if minutes > 0
        && minutes < 100_000_000
        && timeout.as_secs() % 60 == 0
        && timeout.subsec_nanos() == 0
    {
        return format!("{minutes}M");
    }
    let seconds = timeout.as_secs();
    if seconds > 0 && seconds < 100_000_000 && timeout.subsec_nanos() == 0 {
        return format!("{seconds}S");
    }
    let millis = timeout.as_millis();
    if millis > 0 && millis < 100_000_000 && timeout.subsec_nanos() % 1_000_000 == 0 {
        return format!("{millis}m");
    }
    let micros = timeout.as_micros();
    if micros > 0 && micros < 100_000_000 && timeout.subsec_nanos() % 1_000 == 0 {
        return format!("{micros}u");
    }
    format!("{}n", timeout.as_nanos())
}

/// Parses the `grpc-timeout` wire form back into a duration.
pub fn parse_timeout(raw: &str) -> Result<Duration, MetadataError> {
    let bad = || MetadataError {
        key: "grpc-timeout".to_owned(),
        reason: format!("malformed timeout {raw:?}"),
    };
    if raw.is_empty() || raw.len() > 9 {
        return Err(bad());
    }
    let (digits, unit) = raw.split_at(raw.len() - 1);
    if digits.is_empty() || digits.len() > 8 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let value: u64 = digits.parse().map_err(|_| bad())?;
    Ok(match unit {
        "H" => Duration::from_secs(value.saturating_mul(3600)),
        "M" => Duration::from_secs(value.saturating_mul(60)),
        "S" => Duration::from_secs(value),
        "m" => Duration::from_millis(value),
        "u" => Duration::from_micros(value),
        "n" => Duration::from_nanos(value),
        _ => return Err(bad()),
    })
}

/// The effective call timeout: the tighter of the remaining caller budget and
/// the configured service timeout. Never `None` when both are set — servers
/// must always bound work.
#[must_use]
pub fn effective_timeout(remaining_budget: Option<Duration>, configured: Duration) -> Duration {
    match remaining_budget {
        Some(remaining) if remaining < configured => remaining,
        _ => configured,
    }
}

/// A parsed W3C `traceparent` (`version-traceid-spanid-flags`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    /// 128-bit trace id.
    pub trace_id: [u8; 16],
    /// 64-bit span id.
    pub span_id: [u8; 8],
    /// Whether the trace is sampled (`flags & 0x01`).
    pub sampled: bool,
}

impl TraceContext {
    /// Parses `traceparent`. Rejects future versions with extra fields beyond
    /// the tolerated trailing data, all-zero ids, and bad hex.
    pub fn parse(raw: &str) -> Result<Self, MetadataError> {
        let bad = || MetadataError {
            key: "traceparent".to_owned(),
            reason: format!("malformed traceparent {raw:?}"),
        };
        let parts: Vec<&str> = raw.split('-').collect();
        if parts.len() != 4 {
            return Err(bad());
        }
        let (version, trace_hex, span_hex, flags) = (parts[0], parts[1], parts[2], parts[3]);
        if version.len() != 2 || trace_hex.len() != 32 || span_hex.len() != 16 || flags.len() != 2 {
            return Err(bad());
        }
        let hex = |s: &str| -> Result<Vec<u8>, MetadataError> {
            if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(bad());
            }
            (0..s.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| bad()))
                .collect()
        };
        let trace_bytes = hex(trace_hex)?;
        let span_bytes = hex(span_hex)?;
        let flag_bytes = hex(flags)?;
        let mut trace_id = [0u8; 16];
        trace_id.copy_from_slice(&trace_bytes);
        let mut span_id = [0u8; 8];
        span_id.copy_from_slice(&span_bytes);
        if trace_id.iter().all(|b| *b == 0) || span_id.iter().all(|b| *b == 0) {
            return Err(MetadataError {
                key: "traceparent".to_owned(),
                reason: "all-zero trace/span ids are forbidden by the W3C spec".to_owned(),
            });
        }
        Ok(Self {
            trace_id,
            span_id,
            sampled: flag_bytes[0] & 0x01 == 0x01,
        })
    }

    /// Formats back to the `traceparent` wire form (version `00`).
    #[must_use]
    pub fn format(&self) -> String {
        let trace: String = self.trace_id.iter().map(|b| format!("{b:02x}")).collect();
        let span: String = self.span_id.iter().map(|b| format!("{b:02x}")).collect();
        format!(
            "00-{trace}-{span}-{}",
            if self.sampled { "01" } else { "00" }
        )
    }
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Minimal base64 (standard alphabet, with padding) for `-bin` metadata.
fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        let take = chunk.len() + 1; // chars before padding
        for i in 0..4 {
            if i < take {
                let shift = 18 - (i * 6) as u32;
                out.push(BASE64_ALPHABET[((n >> shift) & 0x3f) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::error::AppError;

    #[test]
    fn codes_map_from_app_errors() {
        let cases = [
            (AppError::validation("x"), GrpcCode::InvalidArgument),
            (AppError::not_found("task", "1"), GrpcCode::NotFound),
            (AppError::unauthorized("x"), GrpcCode::Unauthenticated),
            (AppError::forbidden("x"), GrpcCode::PermissionDenied),
            (AppError::conflict("x"), GrpcCode::Aborted),
            (AppError::rate_limited("x"), GrpcCode::ResourceExhausted),
            (AppError::timeout("x"), GrpcCode::DeadlineExceeded),
            (AppError::cancelled("x"), GrpcCode::Cancelled),
            (AppError::database("x"), GrpcCode::Unavailable),
            (AppError::messaging("x"), GrpcCode::Unavailable),
            (
                AppError::external_service("nats", "down"),
                GrpcCode::Unavailable,
            ),
            (AppError::internal("x"), GrpcCode::Internal),
        ];
        for (error, expected) in cases {
            assert_eq!(GrpcCode::from_app_error(&error), expected, "{error}");
        }
        assert!(GrpcCode::Unavailable.is_retryable());
        assert!(!GrpcCode::InvalidArgument.is_retryable());
        assert_eq!(GrpcCode::from_number(14), Some(GrpcCode::Unavailable));
        assert_eq!(GrpcCode::from_number(17), None);
    }

    #[test]
    fn metadata_rules_are_enforced() {
        let mut metadata = GrpcMetadata::new();
        metadata
            .insert_ascii("x-correlation-id", "corr-1")
            .expect("ascii");
        metadata
            .insert_binary("trace-ctx-bin", &[0xde, 0xad, 0xbe, 0xef])
            .expect("bin");
        assert!(metadata.insert_ascii("trace-ctx-bin", "nope").is_err());
        assert!(metadata.insert_binary("plain", &[1]).is_err());
        assert!(metadata.insert_ascii("bad key", "v").is_err());
        assert!(metadata.insert_ascii("ok", "nul\0byte").is_err());

        let wire = metadata.to_wire();
        assert_eq!(
            wire.get("x-correlation-id").map(String::as_str),
            Some("corr-1")
        );
        assert_eq!(
            wire.get("trace-ctx-bin").map(String::as_str),
            Some("3q2+7w==")
        );
    }

    #[test]
    fn inject_standard_forwards_only_the_whitelist() {
        let mut headers = HeaderSet::new();
        headers
            .insert(crate::headers::CORRELATION_ID_KEY, "corr-9")
            .expect("corr");
        headers
            .insert(crate::headers::TENANT_ID_KEY, "tenant-1")
            .expect("tenant");
        headers
            .insert("authorization", "Bearer secret")
            .expect("auth");

        let mut metadata = GrpcMetadata::new();
        inject_standard(&mut metadata, &headers).expect("inject");
        assert_eq!(metadata.get_ascii("x-correlation-id"), Some("corr-9"));
        assert_eq!(metadata.get_ascii("x-tenant-id"), Some("tenant-1"));
        assert!(
            metadata.get_ascii("authorization").is_none(),
            "credentials must never be forwarded"
        );

        // Explicit metadata wins over injected values.
        let mut explicit = GrpcMetadata::new();
        explicit
            .insert_ascii("x-correlation-id", "override")
            .expect("set");
        inject_standard(&mut explicit, &headers).expect("inject");
        assert_eq!(explicit.get_ascii("x-correlation-id"), Some("override"));
    }

    #[test]
    fn timeout_wire_format_roundtrips() {
        for (duration, expected) in [
            (Duration::from_secs(7200), "2H"),
            (Duration::from_secs(180), "3M"),
            (Duration::from_secs(42), "42S"),
            (Duration::from_millis(1500), "1500m"),
            (Duration::from_micros(7), "7u"),
            (Duration::from_nanos(999), "999n"),
        ] {
            assert_eq!(encode_timeout(duration), expected);
            assert_eq!(parse_timeout(expected).expect("parse"), duration);
        }
        assert!(parse_timeout("9S3").is_err());
        assert!(parse_timeout("").is_err());
        assert!(parse_timeout("12x").is_err());
        assert_eq!(
            effective_timeout(Some(Duration::from_secs(2)), Duration::from_secs(5)),
            Duration::from_secs(2),
            "tighter budget wins"
        );
        assert_eq!(
            effective_timeout(None, Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn traceparent_roundtrips_and_rejects_garbage() {
        let raw = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = TraceContext::parse(raw).expect("parse");
        assert!(ctx.sampled);
        assert_eq!(ctx.format(), raw);

        assert!(TraceContext::parse("00-00-00-00").is_err());
        assert!(
            TraceContext::parse("00-00000000000000000000000000000000-00f067aa0ba902b7-01").is_err(),
            "all-zero trace id forbidden"
        );
        assert!(
            TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-zz").is_err()
        );
    }
}
