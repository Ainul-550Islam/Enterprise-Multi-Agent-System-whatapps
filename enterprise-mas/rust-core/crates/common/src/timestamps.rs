//! The single timestamp type of the platform.
//!
//! Policy: **all persisted, transmitted and compared timestamps are UTC
//! [`Timestamp`] values**. Naive datetime types and local time are forbidden
//! outside presentation code.

use crate::error::AppError;
use crate::result::Result;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// UTC timestamp with millisecond-precision wire/storage behavior.
///
/// Serializes as RFC 3339 (e.g. `2026-09-29T04:12:33.123Z`). For epoch-millis
/// payloads use the [`unix_ms_serde`] module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// Current UTC time.
    #[must_use]
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// The Unix epoch.
    #[must_use]
    pub const fn epoch() -> Self {
        Self(DateTime::UNIX_EPOCH)
    }

    /// Builds a timestamp from milliseconds since the Unix epoch.
    pub fn from_unix_ms(ms: i64) -> Result<Self> {
        DateTime::<Utc>::from_timestamp_millis(ms)
            .map(Self)
            .ok_or_else(|| {
                AppError::invalid_field(
                    "timestamp",
                    "out_of_range",
                    "milliseconds since epoch out of representable range",
                )
            })
    }

    /// Builds a timestamp from whole seconds since the Unix epoch.
    pub fn from_unix_seconds(secs: i64) -> Result<Self> {
        DateTime::<Utc>::from_timestamp(secs, 0)
            .map(Self)
            .ok_or_else(|| {
                AppError::invalid_field(
                    "timestamp",
                    "out_of_range",
                    "seconds since epoch out of representable range",
                )
            })
    }

    /// Wraps an existing `DateTime<Utc>`.
    #[must_use]
    pub const fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }

    /// Parses an RFC 3339 string (any offset; normalized to UTC).
    pub fn parse_rfc3339(s: &str) -> Result<Self> {
        let parsed = DateTime::parse_from_rfc3339(s.trim()).map_err(|err| {
            AppError::invalid_field(
                "timestamp",
                "invalid_format",
                format!("expected RFC 3339 timestamp: {err}"),
            )
        })?;
        Ok(Self(parsed.with_timezone(&Utc)))
    }

    /// Borrows the inner `DateTime<Utc>` (read-only interop).
    #[must_use]
    pub const fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }

    /// Converts into the inner `DateTime<Utc>`.
    #[must_use]
    pub fn into_datetime(self) -> DateTime<Utc> {
        self.0
    }

    /// Milliseconds since the Unix epoch (ordering-safe).
    #[must_use]
    pub fn to_unix_ms(&self) -> i64 {
        self.0.timestamp_millis()
    }

    /// Whole seconds since the Unix epoch.
    #[must_use]
    pub fn to_unix_seconds(&self) -> i64 {
        self.0.timestamp()
    }

    /// RFC 3339 with exactly millisecond precision and a `Z` designator.
    /// This is the canonical string form used across logs and payloads.
    #[must_use]
    pub fn to_rfc3339_millis(&self) -> String {
        self.0.to_rfc3339_opts(SecondsFormat::Millis, true)
    }

    /// `self + duration`; `None` on overflow instead of panicking.
    #[must_use]
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        let chrono_duration = chrono::Duration::from_std(duration).ok()?;
        self.0.checked_add_signed(chrono_duration).map(Self)
    }

    /// `self - duration`; `None` on overflow instead of panicking.
    #[must_use]
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        let chrono_duration = chrono::Duration::from_std(duration).ok()?;
        self.0.checked_sub_signed(chrono_duration).map(Self)
    }

    /// Positive difference `self - earlier`, `None` when `earlier` is later.
    #[must_use]
    pub fn duration_since(&self, earlier: &Timestamp) -> Option<Duration> {
        self.0.signed_duration_since(earlier.0).to_std().ok()
    }

    /// Elapsed time since this timestamp until now (`None` for future timestamps).
    #[must_use]
    pub fn elapsed(&self) -> Option<Duration> {
        Self::now().duration_since(self)
    }

    /// Strictly in the future relative to now.
    #[must_use]
    pub fn is_future(&self) -> bool {
        self.0 > Utc::now()
    }

    /// In the past or exactly now.
    #[must_use]
    pub fn is_past(&self) -> bool {
        !self.is_future()
    }

    /// `self` strictly after `other`.
    #[must_use]
    pub fn is_after(&self, other: &Timestamp) -> bool {
        self > other
    }

    /// `self` strictly before `other`.
    #[must_use]
    pub fn is_before(&self, other: &Timestamp) -> bool {
        self < other
    }
}

/// Convenience free function mirroring [`Timestamp::now`].
#[must_use]
pub fn utc_now() -> Timestamp {
    Timestamp::now()
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::now()
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_rfc3339_millis())
    }
}

impl FromStr for Timestamp {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse_rfc3339(s)
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

/// Serde adapter encoding a [`Timestamp`] as integer epoch milliseconds.
/// Usage: `#[serde(with = "mas_common::timestamps::unix_ms_serde")]`.
pub mod unix_ms_serde {
    use super::Timestamp;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(ts: &Timestamp, serializer: S) -> Result<S::Ok, S::Error> {
        ts.to_unix_ms().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Timestamp, D::Error> {
        let ms = i64::deserialize(deserializer)?;
        Timestamp::from_unix_ms(ms).map_err(serde::de::Error::custom)
    }
}

/// Serde adapter for `Option<Timestamp>` as integer epoch milliseconds.
pub mod optional_unix_ms_serde {
    use super::Timestamp;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        ts: &Option<Timestamp>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        ts.map(|t| t.to_unix_ms()).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Timestamp>, D::Error> {
        let ms = Option::<i64>::deserialize(deserializer)?;
        match ms {
            Some(ms) => Timestamp::from_unix_ms(ms)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}
