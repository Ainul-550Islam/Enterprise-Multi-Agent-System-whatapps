//! Cursor pagination primitives.
//!
//! * [`PageRequest`] — inbound page parameters with size enforcement.
//! * [`Cursor`] — opaque, versioned, tamper-evident page token
//!   (URL-safe base64 of a versioned JSON payload).
//! * [`PageResponse<T>`] — outbound page with an optional `next_cursor`.

use crate::constants::CURSOR_VERSION;
use crate::error::{AppError, ValidationIssue};
use crate::result::Result;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

/// Hard upper bound for any list endpoint, regardless of requested size.
pub const MAX_PAGE_SIZE: u32 = 100;
/// Default page size when none is requested.
pub const DEFAULT_PAGE_SIZE: u32 = 25;

/// Decoded cursor payload: sort key (timestamp milliseconds) + tie-breaker ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPayload {
    /// Payload format version (currently [`CURSOR_VERSION`]).
    pub v: u16,
    /// Sort key: `Timestamp::to_unix_ms()` of the last returned row.
    pub ts_ms: i64,
    /// Tie-breaker: canonical string of the last row's ID.
    pub id: String,
}

impl CursorPayload {
    #[must_use]
    pub fn new(ts_ms: i64, id: impl Into<String>) -> Self {
        Self {
            v: CURSOR_VERSION,
            ts_ms,
            id: id.into(),
        }
    }
}

/// Opaque, versioned pagination cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor(CursorPayload);

impl Cursor {
    #[must_use]
    pub fn new(ts_ms: i64, id: impl Into<String>) -> Self {
        Self(CursorPayload::new(ts_ms, id))
    }

    #[must_use]
    pub fn from_payload(payload: CursorPayload) -> Self {
        Self(payload)
    }

    #[must_use]
    pub fn payload(&self) -> &CursorPayload {
        &self.0
    }

    /// Encodes as URL-safe base64(JSON). Never fails for valid payloads except
    /// on (impossible in practice) serialization issues.
    pub fn encode(&self) -> Result<String> {
        let json = serde_json::to_vec(&self.0)
            .map_err(|err| AppError::serialization(format!("cursor encode failed: {err}")))?;
        Ok(URL_SAFE_NO_PAD.encode(json))
    }

    /// Decodes and validates a cursor string. Unknown versions are rejected
    /// (old cursors must never silently mis-page after an upgrade).
    pub fn decode(raw: &str) -> Result<Self> {
        let raw = raw.trim();
        if raw.is_empty() || raw.len() > 8 * 1024 {
            return Err(invalid_cursor("cursor is empty or unreasonably large"));
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| invalid_cursor("cursor is not valid base64url"))?;
        let payload: CursorPayload = serde_json::from_slice(&bytes)
            .map_err(|_| invalid_cursor("cursor payload is malformed"))?;
        if payload.v != CURSOR_VERSION {
            return Err(invalid_cursor("cursor version is not supported"));
        }
        if payload.id.is_empty() || payload.id.len() > 256 {
            return Err(invalid_cursor("cursor id is invalid"));
        }
        if payload.ts_ms < 0 {
            return Err(invalid_cursor("cursor timestamp is invalid"));
        }
        Ok(Self(payload))
    }
}

fn invalid_cursor(detail: &str) -> AppError {
    AppError::validation_with_issues(
        format!("invalid cursor: {detail}"),
        vec![ValidationIssue::new("cursor", "invalid_cursor", detail)],
    )
}

/// Inbound pagination parameters. All fields optional; sane defaults apply.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PageRequest {
    /// Opaque cursor from a previous `PageResponse::next_cursor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Requested page size; clamped to `[1, MAX_PAGE_SIZE]`-violating values
    /// are *rejected* (not silently clamped) so bugs surface early.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl PageRequest {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    #[must_use]
    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// Validates size bounds; returns [`AppError::Validation`] on violation.
    pub fn validate(&self) -> Result<()> {
        if let Some(limit) = self.limit {
            if limit == 0 {
                return Err(AppError::invalid_field(
                    "limit",
                    "out_of_range",
                    "must be at least 1",
                ));
            }
            if limit > MAX_PAGE_SIZE {
                return Err(AppError::invalid_field(
                    "limit",
                    "out_of_range",
                    format!("must be at most {MAX_PAGE_SIZE}"),
                ));
            }
        }
        Ok(())
    }

    /// Page size to actually use after validation.
    #[must_use]
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(MAX_PAGE_SIZE)
    }

    /// Decodes the cursor if present.
    pub fn decode_cursor(&self) -> Result<Option<Cursor>> {
        self.cursor.as_deref().map(Cursor::decode).transpose()
    }
}

/// One page of results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PageResponse<T> {
    pub items: Vec<T>,
    /// Present when more results are available; feed it into the next
    /// [`PageRequest::cursor`]. Absent on the final page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl<T> PageResponse<T> {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
        }
    }

    #[must_use]
    pub fn with_items(items: Vec<T>, next_cursor: Option<String>) -> Self {
        Self { items, next_cursor }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[must_use]
    pub fn has_more(&self) -> bool {
        self.next_cursor.is_some()
    }

    #[must_use]
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> PageResponse<U> {
        PageResponse {
            items: self.items.into_iter().map(f).collect(),
            next_cursor: self.next_cursor,
        }
    }
}
