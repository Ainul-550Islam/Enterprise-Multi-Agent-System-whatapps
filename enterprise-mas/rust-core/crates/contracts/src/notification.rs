//! PURPOSE: Stable tenant-notification HTTP/gRPC DTOs.
//!
//! Owning layer: `mas-contracts` public contract boundary.
//!
//! Production role: represent only safe recipient-visible notification fields,
//! strict keyset query inputs, and a stable lifecycle response shape shared by
//! API clients and non-Rust consumers.
//!
//! Security boundary: destination addresses, retry/error diagnostics,
//! deduplication keys, secret references, and delivery transport metadata are
//! intentionally absent. Authorization and recipient checks remain server-side.
//!
//! Data ownership: these types own no state; PostgreSQL/application services
//! own lifecycle, ordering, and audit evidence.
//!
//! Prohibited responsibilities: accepting arbitrary delivery channels, raw
//! notification creation, offset pagination, authorization decisions, or
//! material-bearing credential fields.

use mas_common::ids::UserId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Query for an authorized tenant recipient's notification feed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationQuery {
    /// Return archived records as well as the default active feed.
    #[serde(default)]
    pub include_archived: bool,
    /// Return only notifications which have not been marked read.
    #[serde(default)]
    pub unread_only: bool,
    /// Maximum number of records in a keyset page.
    pub limit: Option<u16>,
    /// Opaque cursor returned by the preceding page.
    pub cursor: Option<String>,
}

impl NotificationQuery {
    /// Validates page bounds before the cursor reaches a storage query.
    pub fn validate(&self) -> Result<()> {
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "notification page limit must be between 1 and 200",
            ));
        }
        if let Some(cursor) = &self.cursor {
            validation::validate_length("cursor", cursor, 1, 512)?;
            if !cursor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(mas_common::error::AppError::invalid_field(
                    "cursor",
                    "invalid_format",
                    "notification cursor must be URL-safe base64 without padding",
                ));
            }
        }
        Ok(())
    }

    /// Applies the product default while preserving the protocol's upper bound.
    #[must_use]
    pub const fn page_limit(&self) -> u16 {
        match self.limit {
            Some(limit) => limit,
            None => 50,
        }
    }
}

/// Mark one notification read. Kept as a request type for gRPC/legacy clients;
/// the HTTP route carries the identifier in its path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkNotificationReadRequest {
    /// Notification identifier.
    pub notification_id: uuid::Uuid,
}

/// Recipient-visible notification record. It intentionally excludes delivery
/// destination, retry diagnostics, and producer deduplication metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationDto {
    /// Notification identifier.
    pub id: uuid::Uuid,
    /// Recipient identity; only returned through recipient/admin-authorized routes.
    pub recipient_id: UserId,
    /// Represented delivery channel (`email`, `webhook`, `slack`, `in_app`).
    pub channel: String,
    /// Safe pre-rendered subject.
    pub subject: String,
    /// Safe pre-rendered body.
    pub body: String,
    /// Durable delivery state.
    pub delivery_state: String,
    /// Recipient read transition time.
    pub read_at: Option<Timestamp>,
    /// Recipient archive transition time.
    pub archived_at: Option<Timestamp>,
    /// Creation timestamp used as the first component of the keyset cursor.
    pub created_at: Timestamp,
}

/// Stable descending keyset page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPageDto {
    /// Current page records, newest first.
    pub items: Vec<NotificationDto>,
    /// Opaque cursor for the next page, or `None` at the end.
    pub next_cursor: Option<String>,
}

/// User-selected notification channels. Preference persistence is deliberately
/// not inferred from delivery rows and is not implemented by this DTO alone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPreferencesDto {
    /// Channels enabled for product notifications.
    pub channels: Vec<String>,
    /// Update timestamp.
    pub updated_at: Option<Timestamp>,
}
