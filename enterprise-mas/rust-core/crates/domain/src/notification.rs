//! Notifications and their delivery lifecycle.

use mas_common::ids::{TenantId, UserId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Delivery channel represented by the durable notification plane.
    ///
    /// The enum is deliberately closed: a channel cannot be silently invented
    /// by request JSON and must have an explicit delivery adapter and policy.
    NotificationChannel {
        Email => "email",
        Webhook => "webhook",
        Slack => "slack",
        InApp => "in_app",
    }
}

string_enum! {
    /// Delivery lifecycle independent of a recipient's read/archive lifecycle.
    DeliveryState {
        Pending => "pending",
        Sending => "sending",
        Delivered => "delivered",
        Failed => "failed",
        /// Retry budget is exhausted; retained as a durable dead letter.
        DeadLettered => "dead_lettered",
        Cancelled => "cancelled",
    }
}

impl DeliveryState {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(self, Self::Delivered | Self::DeadLettered | Self::Cancelled)
    }
}

/// An outbound notification with delivery, deduplication, and recipient
/// lifecycle state. The pre-rendered body must already be redacted before the
/// aggregate is created; notification persistence never owns raw secrets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub id: uuid::Uuid,
    pub tenant_id: TenantId,
    /// The only user allowed to read/archive this notification unless a
    /// separately authorized tenant administrator view is introduced.
    pub recipient_id: UserId,
    pub channel: NotificationChannel,
    /// Stable producer-supplied key used for tenant/recipient/channel dedupe.
    pub deduplication_key: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Notification {
    /// Creates a pending notification. `deduplication_key` is intentionally a
    /// caller-provided durable fact rather than a process-local hash: producer
    /// retries must converge across process restarts and replicas.
    pub fn new(
        tenant_id: TenantId,
        recipient_id: UserId,
        channel: NotificationChannel,
        deduplication_key: impl Into<String>,
        destination: impl Into<String>,
        subject: impl Into<String>,
        body: impl Into<String>,
    ) -> Result<Self> {
        let deduplication_key = deduplication_key.into();
        let destination = destination.into();
        let subject = subject.into();
        let body = body.into();
        mas_common::validation::validate_length("deduplication_key", &deduplication_key, 1, 256)?;
        if deduplication_key.chars().any(char::is_control) {
            return Err(mas_common::error::AppError::invalid_field(
                "deduplication_key",
                "invalid_format",
                "notification deduplication keys cannot contain control characters",
            ));
        }
        mas_common::validation::validate_non_empty("destination", &destination)?;
        mas_common::validation::validate_length("subject", &subject, 1, 512)?;
        mas_common::validation::validate_length("body", &body, 1, 64 * 1024)?;
        let now = Timestamp::now();
        Ok(Self {
            id: uuid::Uuid::now_v7(),
            tenant_id,
            recipient_id,
            channel,
            deduplication_key,
            destination,
            subject,
            body,
            state: DeliveryState::Pending,
            attempts: 0,
            max_attempts: mas_common::constants::DEFAULT_MAX_RETRIES + 1,
            next_attempt_at: None,
            last_error: None,
            read_at: None,
            archived_at: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Pending|Failed → Sending (a worker claimed it).
    pub fn mark_sending(&mut self) -> Result<()> {
        match self.state {
            DeliveryState::Pending | DeliveryState::Failed => {
                if self.attempts >= self.max_attempts {
                    return Err(mas_common::error::AppError::conflict(
                        "notification exhausted delivery attempts",
                    ));
                }
                self.state = DeliveryState::Sending;
                self.attempts = self.attempts.saturating_add(1);
                self.touch();
                Ok(())
            },
            other => Err(mas_common::error::AppError::conflict(format!(
                "notification in state '{other}' cannot start sending"
            ))),
        }
    }

    /// Sending → Delivered (terminal delivery; receipt remains readable).
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

    /// Sending → Failed or DeadLettered with a bounded exponential retry.
    pub fn schedule_retry(&mut self, error: impl Into<String>) -> Result<()> {
        match self.state {
            DeliveryState::Sending => {
                let mut error = error.into();
                error.truncate(1024);
                self.last_error = Some(error);
                if self.attempts >= self.max_attempts {
                    self.state = DeliveryState::DeadLettered;
                    self.next_attempt_at = None;
                } else {
                    self.state = DeliveryState::Failed;
                    // backoff: 30s, 60s, 120s, … capped at 15 min.
                    let backoff_seconds = 30_u64 << (self.attempts.saturating_sub(1).min(5));
                    let backoff_seconds = backoff_seconds.min(900);
                    self.next_attempt_at = Timestamp::now()
                        .checked_add(std::time::Duration::from_secs(backoff_seconds));
                }
                self.touch();
                Ok(())
            },
            other => Err(mas_common::error::AppError::conflict(format!(
                "notification in state '{other}' cannot be retried"
            ))),
        }
    }

    /// Marks the notification read idempotently. Archive state does not erase
    /// the evidence that it was read.
    pub fn mark_read(&mut self) {
        if self.read_at.is_none() {
            self.read_at = Some(Timestamp::now());
            self.touch();
        }
    }

    /// Archives a recipient-visible notification idempotently.
    pub fn archive(&mut self) {
        if self.archived_at.is_none() {
            self.archived_at = Some(Timestamp::now());
            self.touch();
        }
    }

    /// Pending|Sending|Failed → Cancelled (terminal delivery).
    pub fn cancel(&mut self) -> Result<()> {
        match self.state {
            DeliveryState::Pending | DeliveryState::Sending | DeliveryState::Failed => {
                self.state = DeliveryState::Cancelled;
                self.next_attempt_at = None;
                self.touch();
                Ok(())
            },
            DeliveryState::Cancelled => Ok(()),
            DeliveryState::Delivered | DeliveryState::DeadLettered => Err(
                mas_common::error::AppError::conflict("terminal notifications cannot be cancelled"),
            ),
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

    #[must_use]
    pub const fn is_archived(&self) -> bool {
        self.archived_at.is_some()
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_exhaustion_is_a_durable_dead_letter_not_an_ambiguous_failure() {
        let mut notification = Notification::new(
            TenantId::new(),
            UserId::new(),
            NotificationChannel::InApp,
            "execution:42:completed",
            "in-app",
            "Execution completed",
            "The execution completed successfully.",
        )
        .expect("notification");
        notification.max_attempts = 1;
        notification.mark_sending().expect("claim");
        notification
            .schedule_retry("channel unavailable")
            .expect("settle");
        assert_eq!(notification.state, DeliveryState::DeadLettered);
        assert!(notification.is_terminal());
        assert!(notification.next_attempt_at.is_none());
    }

    #[test]
    fn recipient_read_and_archive_lifecycle_is_idempotent() {
        let mut notification = Notification::new(
            TenantId::new(),
            UserId::new(),
            NotificationChannel::InApp,
            "execution:43:failed",
            "in-app",
            "Execution failed",
            "The execution failed.",
        )
        .expect("notification");
        notification.mark_read();
        let first_read = notification.read_at;
        notification.mark_read();
        assert_eq!(notification.read_at, first_read);
        notification.archive();
        let first_archive = notification.archived_at;
        notification.archive();
        assert_eq!(notification.archived_at, first_archive);
    }
}
