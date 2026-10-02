//! Notifications and their delivery lifecycle.

use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Delivery channel.
    NotificationChannel {
        Email => "email",
        Webhook => "webhook",
        Slack => "slack",
        InApp => "in_app",
    }
}

string_enum! {
    /// Delivery lifecycle.
    DeliveryState {
        Pending => "pending",
        Sending => "sending",
        Delivered => "delivered",
        Failed => "failed",
        Cancelled => "cancelled",
    }
}

impl DeliveryState {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Delivered | Self::Cancelled)
    }
}

/// An outbound notification with retry state.
///
/// Note: no dedicated `NotificationId` exists in the common ID set; the
/// delivery pipeline identifies notifications by the event that produced
/// them plus `destination`, so the row PK is a plain UUID owned here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub id: uuid::Uuid,
    pub tenant_id: TenantId,
    pub channel: NotificationChannel,
    /// Channel-specific destination (email address, webhook id, channel ref).
    pub destination: String,
    pub subject: String,
    /// Pre-rendered, pre-redacted body.
    pub body: String,
    pub state: DeliveryState,
    pub attempts: u32,
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Notification {
    pub fn new(
        tenant_id: TenantId,
        channel: NotificationChannel,
        destination: impl Into<String>,
        subject: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<Self> {
        let destination = destination.into();
        let subject = subject.into();
        let body = body.into();
        mas_common::validation::validate_non_empty("destination", &destination)?;
        mas_common::validation::validate_length("subject", &subject, 1, 512)?;
        mas_common::validation::validate_length("body", &body, 1, 64 * 1024)?;
        let now = Timestamp::now();
        Ok(Self {
            id: uuid::Uuid::now_v7(),
            tenant_id,
            channel,
            destination,
            subject,
            body,
            state: DeliveryState::Pending,
            attempts: 0,
            max_attempts: mas_common::constants::DEFAULT_MAX_RETRIES + 1,
            next_attempt_at: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Pending → Sending (a worker claimed it).
    pub fn mark_sending(&mut self) -> Result<()> {
        match self.state {
            DeliveryState::Pending | DeliveryState::Failed => {
                if self.attempts >= self.max_attempts {
                    return Err(mas_common::error::AppError::conflict(
                        "notification exhausted delivery attempts",
                    ));
                }
                self.state = DeliveryState::Sending;
                self.attempts += 1;
                self.touch();
                Ok(())
            },
            other => Err(mas_common::error::AppError::conflict(format!(
                "notification in state '{other}' cannot start sending"
            ))),
        }
    }

    /// Sending → Delivered (terminal).
    pub fn mark_delivered(&mut self) -> Result<()> {
        match self.state {
            DeliveryState::Sending => {
                self.state = DeliveryState::Delivered;
                self.last_error = None;
                self.next_attempt_at = None;
                self.touch();
                Ok(())
            },
            other => Err(mas_common::error::AppError::conflict(format!(
                "notification in state '{other}' cannot complete delivery"
            ))),
        }
    }

    /// Sending → Failed with the next retry scheduled (exponential backoff in
    /// seconds, capped).
    pub fn schedule_retry(&mut self, error: impl Into<String>) -> Result<()> {
        match self.state {
            DeliveryState::Sending => {
                let mut error = error.into();
                error.truncate(1024);
                self.state = DeliveryState::Failed;
                self.last_error = Some(error);
                // backoff: 30s, 60s, 120s, … capped at 15 min.
                let backoff_seconds = 30_u64 << (self.attempts.saturating_sub(1).min(5));
                let backoff_seconds = backoff_seconds.min(900);
                self.next_attempt_at =
                    Timestamp::now().checked_add(std::time::Duration::from_secs(backoff_seconds));
                self.touch();
                Ok(())
            },
            other => Err(mas_common::error::AppError::conflict(format!(
                "notification in state '{other}' cannot be retried"
            ))),
        }
    }

    /// Pending|Sending|Failed → Cancelled (terminal).
    pub fn cancel(&mut self) -> Result<()> {
        match self.state {
            DeliveryState::Pending | DeliveryState::Sending | DeliveryState::Failed => {
                self.state = DeliveryState::Cancelled;
                self.touch();
                Ok(())
            },
            DeliveryState::Cancelled => Ok(()),
            DeliveryState::Delivered => Err(mas_common::error::AppError::conflict(
                "delivered notifications cannot be cancelled",
            )),
        }
    }

    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    #[must_use]
    pub fn is_due(&self, now: &Timestamp) -> bool {
        match self.state {
            DeliveryState::Pending => true,
            DeliveryState::Failed => self.next_attempt_at.is_some_and(|at| !at.is_after(now)),
            _ => false,
        }
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
