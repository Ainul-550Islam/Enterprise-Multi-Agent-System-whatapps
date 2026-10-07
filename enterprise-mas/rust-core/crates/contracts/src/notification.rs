//! In-app notification list and preference DTOs.

use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Query for the authenticated user's tenant notification feed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationQuery {
    /// Return only unread items when true.
    pub unread_only: Option<bool>,
    /// Maximum number of items.
    pub limit: Option<u16>,
    /// Keyset cursor.
    pub cursor: Option<String>,
}

impl NotificationQuery {
    /// Validates page bounds.
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
        }
        Ok(())
    }
}

/// Mark one notification read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkNotificationReadRequest {
    /// Notification identifier.
    pub notification_id: uuid::Uuid,
}

/// Safe notification view.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationDto {
    /// Notification id.
    pub id: uuid::Uuid,
    /// Channel.
    pub channel: String,
    /// Subject.
    pub subject: String,
    /// Safe, pre-rendered body.
    pub body: String,
    /// Delivery/read state.
    pub state: String,
    /// Creation timestamp.
    pub created_at: Timestamp,
}

/// User-selected notification channels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPreferencesDto {
    /// Channels enabled for product notifications.
    pub channels: Vec<String>,
    /// Update timestamp.
    pub updated_at: Option<Timestamp>,
}
