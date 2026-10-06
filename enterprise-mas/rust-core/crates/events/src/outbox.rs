//! PURPOSE: The transactional outbox: the ONLY path by which durable state
//! changes become events on the bus.
//!
//! Owning layer: `mas-events`.
//!
//! Production responsibility: publish at-least-once, never lose a row, never
//! publish twice by accident and never discard silently. Rows are claimed with
//! an ownership fence (`FOR UPDATE SKIP LOCKED` + `claimed_by`), marked
//! published strictly after the broker acknowledges, retried with bounded
//! backoff, and dead-lettered with the failure retained rather than dropped.
//!
//! It must NOT:
//!   • mark a row published before the broker acknowledges it;
//!   • treat process-local memory as authoritative — every cycle reconstructs
//!     its state from the store;
//!   • delete a failed row, or stop reporting a backlog because it is large.

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
    /// Who currently owns the row (`None` when unclaimed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    /// When the current claim was taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<Timestamp>,
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
            claimed_by: None,
            claimed_at: None,
        }
    }

    /// Whether this row has already consumed its whole attempt budget.
    #[must_use]
    pub const fn is_exhausted(&self, policy: &RetryPolicy) -> bool {
        self.attempts >= policy.max_attempts
    }
}

/// Outbox persistence port (Postgres in production).
///
/// # Ownership
///
/// Every settlement method takes the claiming `worker` and refuses to write a
/// row that worker no longer owns. That is what makes the relay safe to run as
/// more than one replica, and safe to kill mid-publish: a replica that lost
/// its lease (or crashed and had the lease reclaimed) cannot overwrite the
/// settlement of the replica that took over from it.
#[async_trait::async_trait]
pub trait OutboxStorePort: Send + Sync + fmt::Debug {
    /// Enqueues (idempotent on event_id).
    async fn enqueue(&self, record: &OutboxRecord) -> Result<bool>;
    /// Due records, ordered by `next_attempt_at`, oldest first.
    ///
    /// **Read-only.** It must not be used to drive publication: rows it
    /// returns are not owned, so two replicas would both publish them. Use
    /// [`OutboxStorePort::claim_due`] for the publish path.
    async fn fetch_due(&self, limit: usize) -> Result<Vec<OutboxRecord>>;

    /// Atomically takes ownership of up to `limit` due records for `worker`.
    ///
    /// One statement, so it is race-safe under any number of replicas:
    /// unclaimed rows plus rows whose lease has expired are selected
    /// `FOR UPDATE SKIP LOCKED`, then stamped with the claimant in the same
    /// write. A row therefore has exactly one owner at a time, and a crashed
    /// relay's rows become claimable again after `lease`.
    ///
    /// The returned rows carry the `attempts` count as recorded, so the relay
    /// can decide retry-vs-dead-letter without a second round trip.
    async fn claim_due(
        &self,
        limit: usize,
        worker: &str,
        lease: Duration,
    ) -> Result<Vec<OutboxRecord>>;

    /// Hands a claimed row back without publishing it.
    ///
    /// Used when the relay cannot decide an outcome (for example a shutdown
    /// mid-batch): releasing is always better than holding a lease the process
    /// is about to lose anyway.
    async fn release_claim(&self, event_id: EventId, worker: &str) -> Result<()>;

    /// Transitions Pending/Failed→Published, **only while `worker` owns it**,
    /// and only after the caller has the broker's acknowledgement.
    async fn mark_published(&self, event_id: EventId, worker: &str) -> Result<()>;
    /// Transitions Pending/Failed→Failed with a backoff + message, only while
    /// `worker` owns it.
    async fn mark_failed(
        &self,
        event_id: EventId,
        worker: &str,
        error: &str,
        next_attempt_at: Timestamp,
    ) -> Result<()>;
    /// Transitions Failed→DeadLettered, only while `worker` owns it.
    async fn mark_dead_lettered(&self, event_id: EventId, worker: &str) -> Result<()>;
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

    /// Test introspection: backdates a row's claim, so a test can model "the
    /// claimant died and its lease has now expired" without sleeping.
    ///
    /// Sleeping would make the suite depend on wall-clock scheduling; moving
    /// the timestamp is exact and instant.
    #[cfg(test)]
    pub(crate) fn backdate_claim(&self, event_id: EventId, by: Duration) -> bool {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return false;
        };
        let Some(at) = record.claimed_at else {
            return false;
        };
        match at.checked_sub(by) {
            Some(earlier) => {
                record.claimed_at = Some(earlier);
                true
            },
            None => false,
        }
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

    async fn claim_due(
        &self,
        limit: usize,
        worker: &str,
        lease: Duration,
    ) -> Result<Vec<OutboxRecord>> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let now = Timestamp::now();
        let lease_ms = lease.as_millis();
        let mut due: Vec<OutboxRecord> = store
            .values_mut()
            .filter(|record| {
                if !matches!(record.status, EventStatus::Pending | EventStatus::Failed) {
                    return false;
                }
                if record.next_attempt_at.is_future() {
                    return false;
                }
                match (&record.claimed_by, record.claimed_at) {
                    // Unclaimed: anyone may take it.
                    (None, _) => true,
                    // Held by this same worker: re-claimable (a restarted
                    // batch, or a lease that is being renewed).
                    (Some(owner), _) if owner == worker => true,
                    // Held by someone else: only after the lease expires.
                    (Some(_), Some(at)) => now
                        .duration_since(&at)
                        .map(|elapsed| elapsed.as_millis() > lease_ms)
                        .unwrap_or(false),
                    (Some(_), None) => false,
                }
            })
            .map(|record| {
                record.claimed_by = Some(worker.to_owned());
                record.claimed_at = Some(now);
                record.clone()
            })
            .collect();
        due.sort_by_key(|record| record.next_attempt_at.to_unix_ms());
        due.truncate(limit.max(1));
        Ok(due)
    }

    async fn release_claim(&self, event_id: EventId, worker: &str) -> Result<()> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return Err(AppError::not_found("outbox record", event_id.to_string()));
        };
        if record.claimed_by.as_deref() != Some(worker) {
            return Err(AppError::conflict(format!(
                "outbox event {event_id} is claimed by {:?}, not {worker}",
                record.claimed_by
            )));
        }
        record.claimed_by = None;
        record.claimed_at = None;
        Ok(())
    }

    async fn mark_published(&self, event_id: EventId, worker: &str) -> Result<()> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return Err(AppError::not_found("outbox record", event_id.to_string()));
        };
        if record.claimed_by.as_deref() != Some(worker) {
            return Err(AppError::conflict(format!(
                "outbox event {event_id} is not owned by {worker}; a lost claim must not publish"
            )));
        }
        if record.status != EventStatus::Pending && record.status != EventStatus::Failed {
            return Err(AppError::conflict(format!(
                "outbox record is {} and cannot be published",
                record.status
            )));
        }
        record.status = EventStatus::Published;
        record.published_at = Some(Timestamp::now());
        record.claimed_by = None;
        record.claimed_at = None;
        Ok(())
    }

    async fn mark_failed(
        &self,
        event_id: EventId,
        worker: &str,
        error: &str,
        next_attempt_at: Timestamp,
    ) -> Result<()> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return Err(AppError::not_found("outbox record", event_id.to_string()));
        };
        if record.claimed_by.as_deref() != Some(worker) {
            return Err(AppError::conflict(format!(
                "outbox event {event_id} is not owned by {worker}; a lost claim must not settle"
            )));
        }
        record.status = EventStatus::Failed;
        record.attempts = record.attempts.saturating_add(1);
        record.last_error = Some(error.to_owned());
        record.next_attempt_at = next_attempt_at;
        // The row goes back to the pool on its backoff clock; the claim is
        // dropped so another replica may take it when it comes due.
        record.claimed_by = None;
        record.claimed_at = None;
        Ok(())
    }

    async fn mark_dead_lettered(&self, event_id: EventId, worker: &str) -> Result<()> {
        let mut store = self.records.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = store.get_mut(&event_id) else {
            return Err(AppError::not_found("outbox record", event_id.to_string()));
        };
        if record.claimed_by.as_deref() != Some(worker) {
            return Err(AppError::conflict(format!(
                "outbox event {event_id} is not owned by {worker}; a lost claim must not settle"
            )));
        }
        record.status = EventStatus::DeadLettered;
        record.claimed_by = None;
        record.claimed_at = None;
        Ok(())
    }
}

/// One relay cycle, itemised.
///
/// Every field is a count the operator needs: `published` and `dead_lettered`
/// are terminal outcomes, `requeued` is a transient failure that will be
/// retried, `released` is work handed back, and `lost` is the count of
/// settlements this replica was no longer allowed to make. `lost` is expected
/// to be zero and is an alarm condition when it is not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelayCycle {
    /// Records the broker acknowledged.
    pub published: usize,
    /// Records that exhausted the attempt budget and were dead-lettered.
    pub dead_lettered: usize,
    /// Records that failed transiently and were put back with a backoff.
    pub requeued: usize,
    /// Records handed back un-published (shutdown, or an undecidable outcome).
    pub released: usize,
    /// Settlements rejected because this replica no longer owned the row.
    pub lost: usize,
}

impl RelayCycle {
    /// Total records the cycle looked at.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.published + self.dead_lettered + self.requeued + self.released + self.lost
    }
}

/// Outbox → broker relay.
///
/// The relay is the **only** component allowed to move an outbox row towards
/// the broker, and it owns exactly one thing: the claim. Everything about
/// publication is derived from durable row state, so the relay holds no
/// authoritative state of its own and any number of replicas may run it.
///
/// Order within a cycle, which is the whole contract:
///
/// 1. **Claim** a batch atomically (`claim_due`). A claimed row has one owner.
/// 2. **Publish** to the broker.
/// 3. **Settle from the broker's answer only.** Success → `mark_published`
///    after the acknowledgement, never before. Transient failure →
///    `mark_failed` with a backoff. Budget exhausted → `mark_dead_lettered`.
///    A record is never dropped and never silently discarded.
/// 4. **Hand back what cannot be decided** (`release_claim`) rather than
///    leaving a lease this process may be about to lose.
///
/// Duplicate publication is prevented twice over: by the claim (one owner at a
/// time) and by the ownership check inside every settlement (a stale owner
/// cannot write).
#[derive(Debug)]
pub struct OutboxRelay {
    store: Arc<dyn OutboxStorePort>,
    bus: Arc<dyn EventBusPort>,
    policy: RetryPolicy,
    worker: String,
    lease: Duration,
}

impl OutboxRelay {
    /// Default claim lease: long enough to publish a batch, short enough that
    /// a crashed relay's rows come back quickly.
    pub const DEFAULT_LEASE: Duration = Duration::from_secs(30);

    /// Builds a relay with an explicit worker identity.
    ///
    /// The identity is what rows are claimed under, so it must be unique per
    /// running replica: two replicas sharing a name would share a claim
    /// window and could settle each other's rows.
    #[must_use]
    pub fn new(
        store: Arc<dyn OutboxStorePort>,
        bus: Arc<dyn EventBusPort>,
        policy: RetryPolicy,
        worker: impl Into<String>,
    ) -> Self {
        Self {
            store,
            bus,
            policy,
            worker: worker.into(),
            lease: Self::DEFAULT_LEASE,
        }
    }

    #[must_use]
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease.max(Duration::from_secs(1));
        self
    }

    /// The identity rows are claimed under.
    #[must_use]
    pub fn worker(&self) -> &str {
        &self.worker
    }

    /// One relay cycle: claim up to `limit` rows, publish, settle.
    ///
    /// # Errors
    ///
    /// Returns the claim error when the batch could not be taken at all. A
    /// failure to publish or settle an individual row does **not** fail the
    /// cycle: it is recorded in the returned [`RelayCycle`], because one
    /// poisoned row must not stop the rest of the backlog from draining.
    pub async fn relay_once(&self, limit: usize) -> Result<RelayCycle> {
        let claimed = self
            .store
            .claim_due(limit, &self.worker, self.lease)
            .await?;
        let mut cycle = RelayCycle::default();

        for record in claimed {
            // A row that has already spent its budget is not published: it is
            // dead-lettered so an operator sees it instead of it retrying
            // forever on a clock nobody is watching.
            if record.is_exhausted(&self.policy) {
                self.settle_dead_letter(&record, &mut cycle).await;
                continue;
            }

            match self.bus.publish(&record.envelope).await {
                // The broker acknowledged. Only now is the row published.
                Ok(()) => match self
                    .store
                    .mark_published(record.event_id, &self.worker)
                    .await
                {
                    Ok(()) => cycle.published += 1,
                    Err(error) => {
                        self.settle_lost(&record, &mut cycle, &error).await;
                    },
                },
                Err(error) => {
                    let attempt = record.attempts.saturating_add(1);
                    if attempt >= self.policy.max_attempts {
                        self.settle_dead_letter(&record, &mut cycle).await;
                    } else {
                        self.schedule_retry(&record, &mut cycle, attempt, &error.to_string())
                            .await;
                    }
                },
            }
        }

        Ok(cycle)
    }

    /// Dead-letters a row whose attempt budget is spent.
    async fn settle_dead_letter(&self, record: &OutboxRecord, cycle: &mut RelayCycle) {
        match self
            .store
            .mark_dead_lettered(record.event_id, &self.worker)
            .await
        {
            Ok(()) => cycle.dead_lettered += 1,
            Err(error) => self.settle_lost(record, cycle, &error).await,
        }
    }

    /// Puts a transiently-failed row back with a backoff.
    ///
    /// The backoff is derived from the attempt count stored on the row, not
    /// from anything the relay remembers between cycles: a restarted relay
    /// must compute the same delay as the one that failed the row.
    async fn schedule_retry(
        &self,
        record: &OutboxRecord,
        cycle: &mut RelayCycle,
        attempt: u32,
        error: &str,
    ) {
        let Some(next_at) = Timestamp::now().checked_add(self.policy.delay_for(attempt)) else {
            // The clock cannot express the backoff. Dead-lettering is wrong
            // here — the row is not exhausted — so the claim is released and
            // the row is retried on the next cycle.
            tracing::warn!(
                event_id = %record.event_id,
                attempt,
                "outbox backoff overflowed; releasing the claim instead of losing the event"
            );
            self.release(record, cycle).await;
            return;
        };

        match self
            .store
            .mark_failed(record.event_id, &self.worker, error, next_at)
            .await
        {
            Ok(()) => cycle.requeued += 1,
            Err(failure) => self.settle_lost(record, cycle, &failure).await,
        }
    }

    /// A settlement the store rejected.
    ///
    /// Ownership was lost, so the row now belongs to another replica (or has
    /// been settled by one). The correct response is to record it and move on:
    /// re-settling would be the stale-write this whole design prevents.
    async fn settle_lost(&self, record: &OutboxRecord, cycle: &mut RelayCycle, error: &AppError) {
        cycle.lost += 1;
        tracing::warn!(
            event_id = %record.event_id,
            worker = %self.worker,
            %error,
            "outbox settlement rejected: this replica no longer owns the row"
        );
    }

    /// Hands a row back without publishing or failing it.
    async fn release(&self, record: &OutboxRecord, cycle: &mut RelayCycle) {
        match self
            .store
            .release_claim(record.event_id, &self.worker)
            .await
        {
            Ok(()) => cycle.released += 1,
            // A release that fails because ownership moved is not an error
            // worth surfacing: someone else has the row and will settle it.
            Err(_) => cycle.lost += 1,
        }
    }
}

/// Backwards-compatible alias for the pre-claim name.
///
/// The type is a relay now, but call sites that only ever ran one cycle keep
/// their name.
pub type OutboxDispatcher = OutboxRelay;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{
        ConsumerId, EventBusPort, EventHandler, InMemoryEventBus, SubscriptionFilter,
    };
    use mas_common::ids::TenantId;
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    /// A bus whose behaviour is decided per call, so a test can express
    /// "the broker acked" separately from "the broker is down".
    struct ScriptedBus {
        publish_ok: bool,
        calls: AtomicUsize,
    }

    impl fmt::Debug for ScriptedBus {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("ScriptedBus")
                .field("publish_ok", &self.publish_ok)
                .field("calls", &self.calls.load(Ordering::SeqCst))
                .finish()
        }
    }

    #[async_trait::async_trait]
    impl EventBusPort for ScriptedBus {
        async fn publish(&self, _e: &EventEnvelope) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.publish_ok {
                Ok(())
            } else {
                Err(AppError::external_service("event-bus", "bus is down"))
            }
        }
        async fn subscribe(
            &self,
            _c: ConsumerId,
            _f: SubscriptionFilter,
            _h: EventHandler,
        ) -> Result<()> {
            Ok(())
        }
        async fn unsubscribe(&self, _c: &ConsumerId) -> Result<()> {
            Ok(())
        }
        async fn consumers(&self) -> Vec<ConsumerId> {
            Vec::new()
        }
    }

    fn relay(
        store: Arc<dyn OutboxStorePort>,
        bus: Arc<dyn EventBusPort>,
        worker: &str,
    ) -> OutboxRelay {
        OutboxRelay::new(store, bus, RetryPolicy::default(), worker)
    }

    // -- the claim ---------------------------------------------------------

    #[tokio::test]
    async fn enqueue_is_idempotent_on_event_id() {
        let store = Arc::new(InMemoryOutbox::new());
        let record = OutboxRecord::new(envelope(1));
        assert!(store.enqueue(&record).await.expect("enqueue"));
        assert!(
            !store.enqueue(&record).await.expect("enqueue again"),
            "the same event id must not produce a second row"
        );
    }

    #[tokio::test]
    async fn a_claimed_row_cannot_be_claimed_by_a_second_relay() {
        let store = Arc::new(InMemoryOutbox::new());
        let record = OutboxRecord::new(envelope(1));
        store.enqueue(&record).await.expect("enqueue");

        let first = store
            .claim_due(10, "relay-a", Duration::from_secs(30))
            .await
            .expect("claim");
        assert_eq!(first.len(), 1, "relay-a takes the row");
        assert_eq!(first[0].claimed_by.as_deref(), Some("relay-a"));

        let second = store
            .claim_due(10, "relay-b", Duration::from_secs(30))
            .await
            .expect("claim");
        assert!(
            second.is_empty(),
            "a row held under a live lease has exactly one owner, so duplicate \
             publication is impossible by construction"
        );
    }

    #[tokio::test]
    async fn an_expired_lease_is_reclaimable_after_a_crash() {
        let store = Arc::new(InMemoryOutbox::new());
        let record = OutboxRecord::new(envelope(1));
        store.enqueue(&record).await.expect("enqueue");

        let lease = Duration::from_secs(30);
        store.claim_due(10, "relay-a", lease).await.expect("claim");
        // The claimant dies: its lease expires without it ever settling.
        assert!(store.backdate_claim(record.event_id, lease + Duration::from_secs(1)));

        let reclaimed = store.claim_due(10, "relay-b", lease).await.expect("claim");
        assert_eq!(
            reclaimed.len(),
            1,
            "a crashed relay's rows come back, so no event is stranded by a crash"
        );
        assert_eq!(reclaimed[0].claimed_by.as_deref(), Some("relay-b"));
    }

    #[tokio::test]
    async fn a_stale_owner_cannot_settle_a_row_it_lost() {
        let store = Arc::new(InMemoryOutbox::new());
        let record = OutboxRecord::new(envelope(1));
        let event_id = record.event_id;
        store.enqueue(&record).await.expect("enqueue");

        store
            .claim_due(10, "relay-a", Duration::from_secs(30))
            .await
            .expect("claim");

        // Ownership has moved; relay-a's write must be refused by the store.
        let error = store
            .mark_published(event_id, "relay-b")
            .await
            .expect_err("a worker that never claimed the row cannot publish it");
        assert!(matches!(error, AppError::Conflict(_)), "{error:?}");
    }

    #[tokio::test]
    async fn releasing_a_claim_returns_the_row_to_the_pool() {
        let store = Arc::new(InMemoryOutbox::new());
        let record = OutboxRecord::new(envelope(1));
        let event_id = record.event_id;
        store.enqueue(&record).await.expect("enqueue");

        store
            .claim_due(10, "relay-a", Duration::from_secs(30))
            .await
            .expect("claim");
        store
            .release_claim(event_id, "relay-a")
            .await
            .expect("release");

        let reclaimed = store
            .claim_due(10, "relay-b", Duration::from_secs(30))
            .await
            .expect("claim");
        assert_eq!(reclaimed.len(), 1, "a released row is claimable again");
    }

    // -- publication -------------------------------------------------------

    #[tokio::test]
    async fn publish_only_settles_after_the_broker_acknowledges() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(InMemoryEventBus::new());
        let record = OutboxRecord::new(envelope(1));
        store.enqueue(&record).await.expect("enqueue");

        let cycle = relay(store.clone(), bus.clone(), "relay-a")
            .relay_once(10)
            .await
            .expect("relay");

        assert_eq!(cycle.published, 1, "the broker acked");
        assert_eq!(bus.remembered().len(), 1, "exactly one copy on the bus");
        assert_eq!(
            store.snapshot()[0].status,
            EventStatus::Published,
            "the row is published only after the acknowledgement"
        );
    }

    #[tokio::test]
    async fn a_published_row_is_never_republished() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(InMemoryEventBus::new());
        store
            .enqueue(&OutboxRecord::new(envelope(1)))
            .await
            .expect("enqueue");

        let relay_worker = relay(store.clone(), bus.clone(), "relay-a");
        relay_worker.relay_once(10).await.expect("first cycle");
        let second = relay_worker.relay_once(10).await.expect("second cycle");

        assert_eq!(second.total(), 0, "nothing is due after publication");
        assert_eq!(
            bus.remembered().len(),
            1,
            "the bus saw the event exactly once"
        );
    }

    #[tokio::test]
    async fn two_relay_replicas_publish_each_event_once() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(InMemoryEventBus::new());
        for seed in 1..=5u8 {
            store
                .enqueue(&OutboxRecord::new(envelope(seed)))
                .await
                .expect("enqueue");
        }

        // Two replicas race over the same backlog.
        let a = relay(store.clone(), bus.clone(), "relay-a");
        let b = relay(store.clone(), bus.clone(), "relay-b");
        let (first, second) = tokio::join!(a.relay_once(10), b.relay_once(10));
        let first = first.expect("relay a");
        let second = second.expect("relay b");

        assert_eq!(
            first.published + second.published,
            5,
            "every event was published exactly once across both replicas: {first:?} {second:?}"
        );
        assert_eq!(bus.remembered().len(), 5, "no duplicate publication");
    }

    /// An outbox store that fails `mark_published` on demand, so a test can
    /// model the crash window that matters: the broker has acknowledged the
    /// event, and then the process dies before the row records it.
    #[derive(Debug)]
    struct MarkPublishedFails {
        inner: InMemoryOutbox,
        fail_next: AtomicUsize,
    }

    impl MarkPublishedFails {
        fn new(inner: InMemoryOutbox) -> Self {
            Self {
                inner,
                fail_next: AtomicUsize::new(1),
            }
        }

        fn heal(&self) {
            self.fail_next.store(0, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl OutboxStorePort for MarkPublishedFails {
        async fn enqueue(&self, record: &OutboxRecord) -> Result<bool> {
            self.inner.enqueue(record).await
        }
        async fn fetch_due(&self, limit: usize) -> Result<Vec<OutboxRecord>> {
            self.inner.fetch_due(limit).await
        }
        async fn claim_due(
            &self,
            limit: usize,
            worker: &str,
            lease: Duration,
        ) -> Result<Vec<OutboxRecord>> {
            self.inner.claim_due(limit, worker, lease).await
        }
        async fn release_claim(&self, event_id: EventId, worker: &str) -> Result<()> {
            self.inner.release_claim(event_id, worker).await
        }
        async fn mark_published(&self, event_id: EventId, worker: &str) -> Result<()> {
            if self.fail_next.swap(0, Ordering::SeqCst) == 1 {
                return Err(AppError::Database(
                    "connection reset after the broker acknowledged".to_owned(),
                ));
            }
            self.inner.mark_published(event_id, worker).await
        }
        async fn mark_failed(
            &self,
            event_id: EventId,
            worker: &str,
            error: &str,
            next_attempt_at: Timestamp,
        ) -> Result<()> {
            self.inner
                .mark_failed(event_id, worker, error, next_attempt_at)
                .await
        }
        async fn mark_dead_lettered(&self, event_id: EventId, worker: &str) -> Result<()> {
            self.inner.mark_dead_lettered(event_id, worker).await
        }
    }

    /// Matrix case 17 — duplicate outbox relay delivery.
    ///
    /// The relay crashed in the window between "the broker acknowledged the
    /// event" and "the row recorded that it was published". The row is still
    /// due, so it is published a second time. What this guarantees:
    ///
    ///   * the row is **not lost** — it stays due until it is marked published;
    ///   * the duplicate carries the **same event id**, so every downstream
    ///     consumer can deduplicate it;
    ///   * the row is **not marked published before** the broker acked, so the
    ///     failure was never reported as a success.
    #[tokio::test]
    async fn a_crash_between_broker_ack_and_mark_published_republishes_under_one_event_id() {
        let inner = InMemoryOutbox::new();
        inner
            .enqueue(&OutboxRecord::new(envelope(1)))
            .await
            .expect("enqueue");
        let store = Arc::new(MarkPublishedFails::new(inner));
        let bus = Arc::new(InMemoryEventBus::new());

        // Cycle 1: the broker acks, then the durable write fails.
        let relay_worker = relay(store.clone(), bus.clone(), "relay-a");
        let first = relay_worker.relay_once(10).await;
        match first {
            Ok(cycle) => assert_eq!(
                cycle.published, 0,
                "a failed mark-published is never reported as a published event: {cycle:?}"
            ),
            Err(ref error) => assert!(
                matches!(error, AppError::Database(_)),
                "the failure surfaces as a database error, not a silent success: {error:?}"
            ),
        }
        assert_eq!(
            bus.remembered().len(),
            1,
            "the broker did receive the event"
        );
        assert_eq!(
            store.inner.snapshot()[0].status,
            EventStatus::Pending,
            "the row is still due: nothing was silently discarded, nothing was faked as published"
        );

        // Cycle 2: the same row is claimed again and published again.
        store.heal();
        let second = relay_worker.relay_once(10).await.expect("second cycle");
        assert_eq!(second.published, 1);

        let published = bus.remembered();
        assert_eq!(
            published.len(),
            2,
            "at-least-once: the event was delivered twice"
        );
        assert_eq!(
            published[0].id, published[1].id,
            "the duplicate carries the SAME event id, so consumers can deduplicate"
        );
        assert_eq!(
            published[0].event_type, published[1].event_type,
            "and the same type and payload — it is the same event, not a second fact"
        );
        assert_eq!(
            store.inner.snapshot()[0].status,
            EventStatus::Published,
            "the row is marked published only once the broker has acked"
        );

        // A third cycle finds nothing: the duplicate is bounded, not a loop.
        let third = relay_worker.relay_once(10).await.expect("third cycle");
        assert_eq!(third.total(), 0);
        assert_eq!(bus.remembered().len(), 2);
    }

    #[tokio::test]
    async fn a_transient_broker_failure_is_requeued_not_discarded() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(ScriptedBus {
            publish_ok: false,
            calls: AtomicUsize::new(0),
        });
        store
            .enqueue(&OutboxRecord::new(envelope(1)))
            .await
            .expect("enqueue");

        let cycle = relay(store.clone(), bus.clone(), "relay-a")
            .relay_once(10)
            .await
            .expect("relay");

        assert_eq!(cycle.requeued, 1, "the event is put back with a backoff");
        assert_eq!(cycle.dead_lettered, 0, "one failure is not exhaustion");
        let stored = &store.snapshot()[0];
        assert_eq!(stored.status, EventStatus::Failed);
        assert_eq!(stored.attempts, 1);
        assert!(
            stored.next_attempt_at.is_future(),
            "the retry is scheduled in the future, not retried in a tight loop"
        );
        assert_eq!(
            stored.claimed_by, None,
            "the claim is dropped so another replica may take it when it comes due"
        );
    }

    #[tokio::test]
    async fn an_exhausted_row_is_dead_lettered_and_retained() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(ScriptedBus {
            publish_ok: false,
            calls: AtomicUsize::new(0),
        });
        let policy = RetryPolicy {
            max_attempts: 2,
            // Zero backoff: the point under test is exhaustion, and waiting
            // out a real backoff would make the suite depend on wall clock.
            backoff_base: Duration::from_millis(0),
            ..RetryPolicy::default()
        };
        store
            .enqueue(&OutboxRecord::new(envelope(1)))
            .await
            .expect("enqueue");

        let worker = OutboxRelay::new(store.clone(), bus.clone(), policy, "relay-a");
        worker.relay_once(10).await.expect("attempt 1");
        let cycle = worker.relay_once(10).await.expect("attempt 2");

        // The second cycle sees the row already at its budget.
        let stored = &store.snapshot()[0];
        assert_eq!(
            stored.status,
            EventStatus::DeadLettered,
            "a dead letter is retained, never dropped: {cycle:?}"
        );
        assert!(
            stored.last_error.is_some(),
            "the operator sees why it failed"
        );
    }

    #[tokio::test]
    async fn a_row_whose_lease_was_lost_is_counted_not_overwritten() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(ScriptedBus {
            publish_ok: true,
            calls: AtomicUsize::new(0),
        });
        let record = OutboxRecord::new(envelope(1));
        let event_id = record.event_id;
        store.enqueue(&record).await.expect("enqueue");

        let lease = Duration::from_secs(30);
        // relay-a claims, then loses its lease before it can publish.
        store.claim_due(10, "relay-a", lease).await.expect("claim");
        assert!(store.backdate_claim(event_id, lease + Duration::from_secs(1)));
        store.claim_due(10, "relay-b", lease).await.expect("steal");
        assert_eq!(
            store.snapshot()[0].claimed_by.as_deref(),
            Some("relay-b"),
            "relay-b now owns the row"
        );

        let error = store
            .mark_published(event_id, "relay-a")
            .await
            .expect_err("a stale owner must not settle another replica's row");
        assert!(matches!(error, AppError::Conflict(_)), "{error:?}");
        assert_eq!(
            store.snapshot()[0].status,
            EventStatus::Pending,
            "the losing replica's write changed nothing"
        );
        let _ = bus;
    }

    #[tokio::test]
    async fn an_empty_backlog_is_a_no_op_cycle() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(InMemoryEventBus::new());
        let cycle = relay(store, bus, "relay-a")
            .relay_once(10)
            .await
            .expect("relay");
        assert_eq!(cycle.total(), 0, "nothing due, nothing claimed");
    }

    #[tokio::test]
    async fn a_batch_respects_its_limit_and_the_rest_waits() {
        let store = Arc::new(InMemoryOutbox::new());
        let bus = Arc::new(InMemoryEventBus::new());
        for seed in 1..=5u8 {
            store
                .enqueue(&OutboxRecord::new(envelope(seed)))
                .await
                .expect("enqueue");
        }

        let cycle = relay(store.clone(), bus.clone(), "relay-a")
            .relay_once(2)
            .await
            .expect("relay");
        assert_eq!(cycle.published, 2, "the batch limit is honoured");
        assert_eq!(bus.remembered().len(), 2);

        let rest = relay(store.clone(), bus.clone(), "relay-a")
            .relay_once(10)
            .await
            .expect("relay");
        assert_eq!(rest.published, 3, "the remainder drains on the next cycle");
        assert_eq!(bus.remembered().len(), 5);
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.delay_for(1), Duration::from_millis(500));
        assert_eq!(policy.delay_for(2), Duration::from_millis(1_000));
        assert_eq!(policy.delay_for(3), Duration::from_millis(2_000));
        assert_eq!(
            policy.delay_for(50),
            policy.backoff_ceiling,
            "the backoff is capped so a poisoned row stops hammering the broker"
        );
    }

    #[test]
    fn the_relay_cycle_accounts_for_every_record() {
        let cycle = RelayCycle {
            published: 3,
            dead_lettered: 1,
            requeued: 2,
            released: 1,
            lost: 0,
        };
        assert_eq!(cycle.total(), 7, "no record vanishes from the accounting");
    }
}
