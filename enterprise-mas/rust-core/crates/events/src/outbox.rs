//! Transactional outbox with bounded retries and dead-lettering.
//!
//! State-changing services enqueue into the outbox *within the same
//! transaction* as the state write; the [`OutboxDispatcher`] then publishes
//! asynchronously to any [`EventBusPort`]. Events therefore survive process
//! crashes without the service knowing the bus exists.

use crate::bus::EventBusPort;
use crate::envelope::EventEnvelope;
use mas_common::enums::EventStatus;
use mas_common::error::AppError;
use mas_common::ids::EventId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Retry policy placeholder that doubles as the retry configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts (delivery attempts before DLQ); ≥ 1.
    pub max_attempts: u32,
    /// Base backoff between retries, doubled per attempt (capped by the
    /// engine's retry ceiling).
    pub backoff_base: Duration,
    /// Hard cap on the backoff.
    pub backoff_ceiling: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            backoff_base: Duration::from_millis(500),
            backoff_ceiling: Duration::from_secs(60),
        }
    }
}

impl RetryPolicy {
    /// Delay *before* attempt `n` (1-based).
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let shift = attempt.saturating_sub(1).min(20);
        let scaled = self
            .backoff_base
            .checked_mul(1u32 << shift)
            .unwrap_or(self.backoff_ceiling);
        scaled.min(self.backoff_ceiling)
    }
}

/// One buffered event (published by the dispatcher).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxRecord {
    pub event_id: EventId,
    pub envelope: EventEnvelope,
    pub status: EventStatus,
    /// Attempts consumed so far.
    pub attempts: u32,
    /// When the record is next eligible for dispatch.
    pub next_attempt_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub enqueued_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<Timestamp>,
}

impl OutboxRecord {
    pub fn new(envelope: EventEnvelope) -> Self {
        Self {
            event_id: envelope.id,
            envelope,
            status: EventStatus::Pending,
            attempts: 0,
            next_attempt_at: Timestamp::now(),
            last_error: None,
            enqueued_at: Timestamp::now(),
            published_at: None,
        }
    }
}

/// Outbox persistence port (Postgres in production).
#[async_trait::async_trait]
pub trait OutboxStorePort: Send + Sync + fmt::Debug {
    /// Enqueues (idempotent on event_id).
    async fn enqueue(&self, record: &OutboxRecord) -> Result<bool>;
    /// Due records, ordered by `next_attempt_at`, oldest first.
    async fn fetch_due(&self, limit: usize) -> Result<Vec<OutboxRecord>>;
    /// Transitions Pending→Published (only when still pending).
    async fn mark_published(&self, event_id: EventId) -> Result<()>;
    /// Transitions Pending→Failed with a backoff + message.
    async fn mark_failed(
        &self,
        event_id: EventId,
        error: &str,
        next_attempt_at: Timestamp,
    ) -> Result<()>;
    /// Transitions Failed→DeadLettered.
    async fn mark_dead_lettered(&self, event_id: EventId) -> Result<()>;
}

/// In-memory outbox (dev/tests).
#[derive(Debug, Default)]
pub struct InMemoryOutbox {
    records: Mutex<BTreeMap<EventId, OutboxRecord>>,
}

impl InMemoryOutbox {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test introspection: the enqueued records in id order.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> Vec<OutboxRecord> {
        self.records
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl OutboxStorePort for InMemoryOutbox {
    async fn enqueue(&self, record: &OutboxRecord) -> Result<bool> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        if store.contains_key(&record.event_id) {
            return Ok(false); // idempotent: already enqueued
        }
        store.insert(record.event_id, record.clone());
        Ok(true)
    }

    async fn fetch_due(&self, limit: usize) -> Result<Vec<OutboxRecord>> {
        let store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let now = Timestamp::now();
        let mut due: Vec<OutboxRecord> = store
            .values()
            .filter(|record| {
                matches!(record.status, EventStatus::Pending | EventStatus::Failed)
                    && !record.next_attempt_at.is_future()
                    && now.duration_since(&record.next_attempt_at).is_some()
            })
            .cloned()
            .collect();
        due.sort_by_key(|record| record.next_attempt_at.to_unix_ms());
        due.truncate(limit.max(1));
        Ok(due)
    }

    async fn mark_published(&self, event_id: EventId) -> Result<()> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return Err(AppError::not_found("outbox record", event_id.to_string()));
        };
        if record.status != EventStatus::Pending && record.status != EventStatus::Failed {
            return Err(AppError::conflict(format!(
                "outbox record is {} and cannot be published",
                record.status
            )));
        }
        record.status = EventStatus::Published;
        record.published_at = Some(Timestamp::now());
        Ok(())
    }

    async fn mark_failed(
        &self,
        event_id: EventId,
        error: &str,
        next_attempt_at: Timestamp,
    ) -> Result<()> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return Err(AppError::not_found("outbox record", event_id.to_string()));
        };
        record.status = EventStatus::Failed;
        record.attempts += 1;
        record.last_error = Some(error.to_owned());
        record.next_attempt_at = next_attempt_at;
        Ok(())
    }

    async fn mark_dead_lettered(&self, event_id: EventId) -> Result<()> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return Err(AppError::not_found("outbox record", event_id.to_string()));
        };
        record.status = EventStatus::DeadLettered;
        Ok(())
    }
}

/// Bridges outbox → bus: dispatch_due publishes eligible records with the
/// retry policy, dead-lettering exhausted ones.
#[derive(Debug)]
pub struct OutboxDispatcher {
    store: Arc<dyn OutboxStorePort>,
    bus: Arc<dyn EventBusPort>,
    policy: RetryPolicy,
}

impl OutboxDispatcher {
    pub fn new(
        store: Arc<dyn OutboxStorePort>,
        bus: Arc<dyn EventBusPort>,
        policy: RetryPolicy,
    ) -> Self {
        Self { store, bus, policy }
    }

    /// One dispatch cycle: publish up to `limit` due records.
    /// Returns (published, dead_lettered, still_pending).
    pub async fn dispatch_due(&self, limit: usize) -> Result<(usize, usize, usize)> {
        let due = self.store.fetch_due(limit).await?;
        let mut published = 0usize;
        let mut dead_lettered = 0usize;
        let mut pending = 0usize;
        for record in due {
            if record.status == EventStatus::Failed && record.attempts >= self.policy.max_attempts {
                self.store.mark_dead_lettered(record.event_id).await?;
                dead_lettered += 1;
                continue;
            }
            match self.bus.publish(&record.envelope).await {
                Ok(()) => {
                    self.store.mark_published(record.event_id).await?;
                    published += 1;
                },
                Err(error) => {
                    let attempt = record.attempts + 1;
                    if attempt >= self.policy.max_attempts {
                        self.store.mark_dead_lettered(record.event_id).await?;
                        dead_lettered += 1;
                    } else {
                        let next_at = Timestamp::now()
                            .checked_add(self.policy.delay_for(attempt))
                            .ok_or_else(|| AppError::internal("retry time overflow"))?;
                        self.store
                            .mark_failed(record.event_id, &error.to_string(), next_at)
                            .await?;
                        pending += 1;
                    }
                },
            }
        }
        // Count everything left that is not terminal for caller reporting.
        let in_flight = self
            .store
            .fetch_due(limit.saturating_sub(published + pending + dead_lettered))
            .await?
            .len();
        Ok((published, dead_lettered, pending + in_flight))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::InMemoryEventBus;
    use mas_common::ids::TenantId;

    fn envelope(id_seed: u8) -> EventEnvelope {
        EventEnvelope::builder(
            "execution.started",
            1,
            "execution",
            format!("exec-{id_seed}"),
            TenantId::new(),
        )
        .expect("builder")
        .with_correlation(format!("corr-{id_seed}"))
        .expect("correlation")
        .build()
        .expect("envelope")
    }

    #[tokio::test]
    async fn enqueue_once_publish_then_no_redeliver() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(InMemoryEventBus::new());
        let dispatcher = OutboxDispatcher::new(store.clone(), bus.clone(), RetryPolicy::default());
        let record = OutboxRecord::new(envelope(1));
        assert!(store.enqueue(&record).await.expect("enqueue"));
        assert!(
            !store.enqueue(&record).await.expect("enqueue again"),
            "idempotent"
        );
        let (published, dead, pending) = dispatcher.dispatch_due(10).await.expect("dispatch");
        assert_eq!((published, dead, pending), (1, 0, 0));
        assert_eq!(bus.remembered().len(), 1);
        let (published2, _, _) = dispatcher.dispatch_due(10).await.expect("dispatch 2");
        assert_eq!(published2, 0, "published records are not re-dispatched");
    }

    #[tokio::test]
    async fn failures_retry_with_backoff_then_dead_letter() {
        let store = Arc::new(InMemoryOutbox::new());
        // A bus that always fails publication.
        struct FailingBus;
        impl fmt::Debug for FailingBus {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("FailingBus").finish()
            }
        }
        #[async_trait::async_trait]
        impl EventBusPort for FailingBus {
            async fn publish(&self, _e: &EventEnvelope) -> Result<()> {
                Err(AppError::external_service("event-bus", "bus is down"))
            }
            async fn subscribe(
                &self,
                _c: crate::bus::ConsumerId,
                _f: crate::bus::SubscriptionFilter,
                _h: crate::bus::EventHandler,
            ) -> Result<()> {
                Ok(())
            }
            async fn unsubscribe(&self, _c: &crate::bus::ConsumerId) -> Result<()> {
                Ok(())
            }
            async fn consumers(&self) -> Vec<crate::bus::ConsumerId> {
                Vec::new()
            }
        }
        let policy = RetryPolicy {
            max_attempts: 3,
            backoff_base: Duration::from_millis(1),
            backoff_ceiling: Duration::from_millis(10),
        };
        let dispatcher = OutboxDispatcher::new(store.clone(), Arc::new(FailingBus), policy);
        store
            .enqueue(&OutboxRecord::new(envelope(2)))
            .await
            .expect("enqueue");

        tokio::time::sleep(Duration::from_millis(2)).await;
        let (_, dead1, pending1) = dispatcher.dispatch_due(5).await.expect("d1");
        assert_eq!((dead1, pending1), (0, 1));

        tokio::time::sleep(Duration::from_millis(5)).await;
        let (_, dead2, pending2) = dispatcher.dispatch_due(5).await.expect("d2");
        assert_eq!((dead2, pending2), (0, 1));

        tokio::time::sleep(Duration::from_millis(10)).await;
        let (_, dead3, pending3) = dispatcher.dispatch_due(5).await.expect("d3");
        assert_eq!((dead3, pending3), (1, 0));
        let snapshot = store.snapshot();
        let rec = &snapshot[0];
        assert_eq!(rec.status, EventStatus::DeadLettered);
        assert_eq!(rec.attempts, 2);
        assert!(rec.last_error.as_deref().is_some());
    }

    #[test]
    fn retry_policy_scales_and_caps() {
        let policy = RetryPolicy {
            max_attempts: 5,
            backoff_base: Duration::from_millis(100),
            backoff_ceiling: Duration::from_millis(400),
        };
        assert_eq!(policy.delay_for(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for(20), Duration::from_millis(400));
        assert!(RetryPolicy::default().max_attempts > 0);
    }
}
