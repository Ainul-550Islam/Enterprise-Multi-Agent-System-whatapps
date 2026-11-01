//! PURPOSE: Lease-fenced durable notification delivery worker ports.
//!
//! Owning layer: `mas-integrations` delivery infrastructure boundary.
//!
//! Production role: claim tenant notification work from a durable store, invoke
//! only represented channel adapters, and atomically settle delivery state with
//! a fence. It never converts an adapter error into a fake success.
//!
//! Security boundary: recipient content/destinations are retrieved only from a
//! claimed durable row; sender adapters must enforce their own transport and
//! credential boundaries. Stale workers cannot overwrite newer settlement due
//! to monotonically incremented fences.
//!
//! Data ownership: the store owns notification rows/leases; channel adapters
//! own no durable state. This worker owns no in-memory queue across restarts.
//!
//! Prohibited responsibilities: user authorization, notification creation,
//! template rendering, arbitrary channel dispatch, raw secret storage, or
//! direct unguarded HTTP sending.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{DeliveryState, Notification, NotificationChannel};

/// One notification row claimed under a monotonically increasing fence.
#[derive(Debug, Clone)]
pub struct ClaimedNotification {
    pub notification: Notification,
    pub fence: i64,
}

/// Durable notification claim/settle port.
#[async_trait]
pub trait NotificationDeliveryLeasePort: std::fmt::Debug + Send + Sync {
    /// Atomically claims due or expired-lease jobs using `SKIP LOCKED`.
    async fn claim_due(
        &self,
        worker: &str,
        now: &Timestamp,
        lease: Duration,
        limit: u32,
    ) -> Result<Vec<ClaimedNotification>>;
    /// Settles only while the exact fence and lease remain current.
    async fn settle_claim(
        &self,
        claim: &ClaimedNotification,
        notification: &Notification,
    ) -> Result<bool>;
    /// Releases an undecided claim only while its fence remains current.
    async fn release_claim(&self, claim: &ClaimedNotification) -> Result<bool>;
}

/// A represented notification channel adapter.
#[async_trait]
pub trait NotificationChannelPort: std::fmt::Debug + Send + Sync {
    /// Delivers one claimed notification. Implementations must reject unknown
    /// channels and must not log body/destination or material-bearing headers.
    async fn deliver(&self, notification: &Notification) -> Result<()>;
}

/// Bounded worker configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableNotificationWorkerConfig {
    pub worker_id: String,
    pub batch_size: u32,
    pub lease: Duration,
}

impl Default for DurableNotificationWorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: "notification-worker".to_owned(),
            batch_size: 100,
            lease: Duration::from_secs(30),
        }
    }
}

impl DurableNotificationWorkerConfig {
    /// Validates worker values before they become persisted lease authority.
    pub fn validate(&self) -> Result<()> {
        if self.worker_id.trim().is_empty()
            || self.worker_id.len() > 128
            || self.worker_id.chars().any(char::is_control)
        {
            return Err(AppError::invalid_field(
                "worker_id",
                "invalid_format",
                "notification worker id must be 1..=128 printable characters",
            ));
        }
        if !(1..=1_000).contains(&self.batch_size) {
            return Err(AppError::invalid_field(
                "batch_size",
                "out_of_range",
                "notification worker batch size must be between 1 and 1000",
            ));
        }
        if self.lease < Duration::from_secs(1) || self.lease > Duration::from_secs(300) {
            return Err(AppError::invalid_field(
                "lease",
                "out_of_range",
                "notification worker lease must be between 1 and 300 seconds",
            ));
        }
        Ok(())
    }
}

/// Low-cardinality outcomes of one delivery cycle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotificationDeliveryCycle {
    pub attempted: u64,
    pub delivered: u64,
    pub requeued: u64,
    pub dead_lettered: u64,
    pub stale: u64,
    pub released: u64,
}

/// Production-safe notification delivery worker.
#[derive(Debug)]
pub struct DurableNotificationWorker<S, C>
where
    S: NotificationDeliveryLeasePort,
    C: NotificationChannelPort,
{
    store: S,
    channel: C,
    config: DurableNotificationWorkerConfig,
}

impl<S, C> DurableNotificationWorker<S, C>
where
    S: NotificationDeliveryLeasePort,
    C: NotificationChannelPort,
{
    /// Builds a worker with bounded claim settings.
    pub fn new(store: S, channel: C, config: DurableNotificationWorkerConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            store,
            channel,
            config,
        })
    }

    /// Runs one bounded claim/deliver/fenced-settlement pass.
    pub async fn run_once(&self, now: &Timestamp) -> Result<NotificationDeliveryCycle> {
        let claims = self
            .store
            .claim_due(
                &self.config.worker_id,
                now,
                self.config.lease,
                self.config.batch_size,
            )
            .await?;
        let mut cycle = NotificationDeliveryCycle::default();
        for claim in claims {
            self.deliver_one(claim, &mut cycle).await;
        }
        Ok(cycle)
    }

    async fn deliver_one(&self, claim: ClaimedNotification, cycle: &mut NotificationDeliveryCycle) {
        let mut notification = claim.notification.clone();
        if notification.mark_sending().is_err() {
            tracing::warn!(
                outcome = "invalid_lifecycle",
                "notification claim had invalid lifecycle state"
            );
            self.release(&claim, cycle).await;
            return;
        }
        cycle.attempted = cycle.attempted.saturating_add(1);
        match self.channel.deliver(&notification).await {
            Ok(()) => match notification.mark_delivered() {
                Ok(()) => self.settle(&claim, &notification, cycle).await,
                Err(_) => {
                    tracing::warn!(
                        outcome = "invalid_transition",
                        "notification delivery transition failed"
                    );
                    self.release(&claim, cycle).await;
                },
            },
            Err(_) => {
                // Channel adapters may receive provider response text. Persist
                // a stable classification only; raw remote errors can contain
                // recipient data, headers, or material and never belong in a
                // durable notification row or log field.
                if notification
                    .schedule_retry("notification channel delivery failed")
                    .is_err()
                {
                    self.release(&claim, cycle).await;
                    return;
                }
                self.settle(&claim, &notification, cycle).await;
            },
        }
    }

    async fn settle(
        &self,
        claim: &ClaimedNotification,
        notification: &Notification,
        cycle: &mut NotificationDeliveryCycle,
    ) {
        match self.store.settle_claim(claim, notification).await {
            Ok(true) => match notification.state {
                DeliveryState::Delivered => cycle.delivered = cycle.delivered.saturating_add(1),
                DeliveryState::Failed => cycle.requeued = cycle.requeued.saturating_add(1),
                DeliveryState::DeadLettered => {
                    cycle.dead_lettered = cycle.dead_lettered.saturating_add(1)
                },
                _ => cycle.stale = cycle.stale.saturating_add(1),
            },
            Ok(false) => {
                cycle.stale = cycle.stale.saturating_add(1);
                tracing::warn!(
                    outcome = "stale_fence",
                    "notification settlement lost ownership"
                );
            },
            Err(_) => {
                tracing::warn!(
                    outcome = "settlement_error",
                    "notification settlement failed"
                );
                self.release(claim, cycle).await;
            },
        }
    }

    async fn release(&self, claim: &ClaimedNotification, cycle: &mut NotificationDeliveryCycle) {
        match self.store.release_claim(claim).await {
            Ok(true) => cycle.released = cycle.released.saturating_add(1),
            Ok(false) | Err(_) => cycle.stale = cycle.stale.saturating_add(1),
        }
    }
}

/// Adapter for exactly one represented delivery channel.
#[async_trait]
pub trait RepresentedNotificationChannelAdapter: std::fmt::Debug + Send + Sync {
    /// The closed channel vocabulary this adapter owns.
    fn channel(&self) -> NotificationChannel;
    /// Delivers a notification for that channel. Implementations must not
    /// reinterpret a destination as an arbitrary URL or protocol request.
    async fn deliver_channel(&self, notification: &Notification) -> Result<()>;
}

/// Closed router over explicitly registered channel adapters.
///
/// A missing adapter is a delivery failure, not a successful no-op. This makes
/// unsupported Email/Slack/Webhook integration visible as durable retry/dead
/// letter evidence instead of silently losing a user-visible message.
pub struct RepresentedNotificationRouter {
    adapters: BTreeMap<NotificationChannel, Arc<dyn RepresentedNotificationChannelAdapter>>,
}

impl std::fmt::Debug for RepresentedNotificationRouter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RepresentedNotificationRouter")
            .field("channels", &self.adapters.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl RepresentedNotificationRouter {
    /// Builds a router and rejects duplicate channel ownership at boot.
    pub fn new(
        adapters: impl IntoIterator<Item = Arc<dyn RepresentedNotificationChannelAdapter>>,
    ) -> Result<Self> {
        let mut registered = BTreeMap::new();
        for adapter in adapters {
            let channel = adapter.channel();
            if registered.insert(channel, adapter).is_some() {
                return Err(AppError::conflict(format!(
                    "duplicate notification delivery adapter for channel '{channel}'"
                )));
            }
        }
        Ok(Self {
            adapters: registered,
        })
    }
}

#[async_trait]
impl NotificationChannelPort for RepresentedNotificationRouter {
    async fn deliver(&self, notification: &Notification) -> Result<()> {
        let adapter = self.adapters.get(&notification.channel).ok_or_else(|| {
            AppError::external_service(
                "notification_delivery",
                format!(
                    "no configured delivery adapter for represented channel '{}'",
                    notification.channel
                ),
            )
        })?;
        adapter.deliver_channel(notification).await
    }
}

/// In-app delivery is complete once the durable notification row is visible to
/// its authorized recipient. It performs no network send and refuses every
/// other channel rather than pretending a remote delivery occurred.
#[derive(Debug, Default)]
pub struct InAppNotificationAdapter;

#[async_trait]
impl RepresentedNotificationChannelAdapter for InAppNotificationAdapter {
    fn channel(&self) -> NotificationChannel {
        NotificationChannel::InApp
    }

    async fn deliver_channel(&self, notification: &Notification) -> Result<()> {
        if notification.channel != NotificationChannel::InApp {
            return Err(AppError::invalid_field(
                "channel",
                "adapter_mismatch",
                "in-app delivery adapter received another notification channel",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_config_rejects_unbounded_claim_authority() {
        assert!(DurableNotificationWorkerConfig::default()
            .validate()
            .is_ok());
        assert!(DurableNotificationWorkerConfig {
            worker_id: "".to_owned(),
            ..DurableNotificationWorkerConfig::default()
        }
        .validate()
        .is_err());
    }
}
