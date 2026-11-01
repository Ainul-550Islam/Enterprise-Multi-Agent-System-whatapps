//! Durable delivery receipts between a broker delivery and worker execution.
//!
//! A broker sequence is transport-local metadata: JetStream owns one sequence
//! space, the PostgreSQL task lane owns another, and neither survives every
//! topology change. This port gives a delivery a database-generated identity
//! and records its lifecycle independently of the task attempt. The consumer
//! reserves one receipt before any claim, renews it with the task heartbeat,
//! and settles it before sending the broker instruction.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use uuid::Uuid;

/// Information needed to reserve one delivery receipt.
#[derive(Debug, Clone)]
pub struct DeliveryReservationRequest {
    /// Durable consumer that received the delivery.
    pub consumer_name: String,
    /// Task named by the decoded, signed/validated broker frame.
    pub task_id: Uuid,
    /// Task tenant, recorded for auditable correlation without trusting a
    /// later broker redelivery.
    pub tenant_id: Uuid,
    /// Worker replica handling the delivery.
    pub worker_id: String,
    /// Broker delivery count (first delivery is one).
    pub delivery_count: u32,
    /// Receipt expiry; a dead worker's outstanding receipt may be superseded
    /// only after this deadline.
    pub lease_expires_at: Timestamp,
}

impl DeliveryReservationRequest {
    /// Rejects malformed operational facts before either receipt adapter
    /// persists them. The PostgreSQL check constraints are a second line of
    /// defence; validating here gives every adapter the same fail-closed
    /// contract and avoids an expired receipt being born live.
    pub fn validate(&self) -> Result<()> {
        if self.consumer_name.trim().is_empty() || self.consumer_name.len() > 128 {
            return Err(AppError::validation(
                "delivery receipt consumer_name must be 1..=128 characters",
            ));
        }
        if self.worker_id.trim().is_empty() || self.worker_id.len() > 128 {
            return Err(AppError::validation(
                "delivery receipt worker_id must be 1..=128 characters",
            ));
        }
        if !(1..=64).contains(&self.delivery_count) {
            return Err(AppError::validation(
                "delivery receipt delivery_count must be in 1..=64",
            ));
        }
        if !self.lease_expires_at.is_after(&Timestamp::now()) {
            return Err(AppError::validation(
                "delivery receipt lease_expires_at must be in the future",
            ));
        }
        Ok(())
    }
}

/// Database-generated delivery identity returned by [`DeliveryReceiptPort`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryReceipt {
    /// Monotonic identity allocated by the durable receipt store.
    pub sequence: i64,
}

/// The terminal/intermediate state written before the matching broker signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryReceiptSettlement {
    /// The downstream task lifecycle committed and the broker is about to ACK.
    Acked,
    /// The worker is asking the broker to redeliver later.
    Nacked,
    /// The delivery is permanently rejected.
    Terminated,
}

impl DeliveryReceiptSettlement {
    /// Stable storage representation constrained by the migration enum check.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acked => "acked",
            Self::Nacked => "nacked",
            Self::Terminated => "terminated",
        }
    }
}

/// Durable source of truth for one broker delivery.
///
/// `reserve` returns `Ok(None)` when a live receipt already owns the task.
/// That is a normal duplicate/concurrent delivery and callers must defer it,
/// never run it. Storage errors are returned so callers can fail closed by
/// leaving the broker delivery unsettled for redelivery.
#[async_trait::async_trait]
pub trait DeliveryReceiptPort: Send + Sync + std::fmt::Debug {
    /// Atomically reserves a live receipt, or reports an existing live owner.
    async fn reserve(&self, request: DeliveryReservationRequest)
        -> Result<Option<DeliveryReceipt>>;

    /// Associates the durable task-attempt id after the lifecycle claim wins.
    async fn attach_attempt(&self, receipt: DeliveryReceipt, attempt_id: Uuid) -> Result<bool>;

    /// Renews a live receipt alongside broker and task leases.
    async fn renew(&self, receipt: DeliveryReceipt, lease_expires_at: Timestamp) -> Result<bool>;

    /// Persists the decision before the broker receives its matching signal.
    async fn settle(
        &self,
        receipt: DeliveryReceipt,
        settlement: DeliveryReceiptSettlement,
    ) -> Result<bool>;
}

#[derive(Debug, Clone)]
struct InMemoryRecord {
    task_id: Uuid,
    lease_expires_at: Timestamp,
    settled: bool,
    attempt_id: Option<Uuid>,
}

/// Volatile receipt store for unit tests only.
///
/// It intentionally has no production constructor wiring. Process-local state
/// cannot prevent replays across replicas or a restart; production uses
/// `mas_persistence::repositories::PostgresDeliveryReceiptStore`.
#[derive(Debug, Default)]
pub struct InMemoryDeliveryReceiptStore {
    next_sequence: AtomicI64,
    records: Mutex<BTreeMap<i64, InMemoryRecord>>,
    active_by_task: Mutex<BTreeMap<Uuid, i64>>,
}

impl InMemoryDeliveryReceiptStore {
    fn next_sequence(&self) -> i64 {
        self.next_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }
}

#[async_trait::async_trait]
impl DeliveryReceiptPort for InMemoryDeliveryReceiptStore {
    async fn reserve(
        &self,
        request: DeliveryReservationRequest,
    ) -> Result<Option<DeliveryReceipt>> {
        request.validate()?;
        let now = Timestamp::now();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut active = self
            .active_by_task
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(sequence) = active.get(&request.task_id).copied() {
            if let Some(record) = records.get(&sequence) {
                if !record.settled && record.lease_expires_at.is_after(&now) {
                    return Ok(None);
                }
            }
            active.remove(&request.task_id);
        }
        let sequence = self.next_sequence();
        records.insert(
            sequence,
            InMemoryRecord {
                task_id: request.task_id,
                lease_expires_at: request.lease_expires_at,
                settled: false,
                attempt_id: None,
            },
        );
        active.insert(request.task_id, sequence);
        Ok(Some(DeliveryReceipt { sequence }))
    }

    async fn attach_attempt(&self, receipt: DeliveryReceipt, attempt_id: Uuid) -> Result<bool> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = records.get_mut(&receipt.sequence) else {
            return Ok(false);
        };
        if record.settled || !record.lease_expires_at.is_after(&Timestamp::now()) {
            return Ok(false);
        }
        if record
            .attempt_id
            .is_some_and(|existing| existing != attempt_id)
        {
            return Ok(false);
        }
        record.attempt_id = Some(attempt_id);
        Ok(true)
    }

    async fn renew(&self, receipt: DeliveryReceipt, lease_expires_at: Timestamp) -> Result<bool> {
        if !lease_expires_at.is_after(&Timestamp::now()) {
            return Ok(false);
        }
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = records.get_mut(&receipt.sequence) else {
            return Ok(false);
        };
        // Match PostgreSQL's `lease_expires_at > now()` fence: once a receipt
        // lapsed, only a new reservation may become the delivery owner.
        if record.settled || !record.lease_expires_at.is_after(&Timestamp::now()) {
            return Ok(false);
        }
        record.lease_expires_at = lease_expires_at;
        Ok(true)
    }

    async fn settle(
        &self,
        receipt: DeliveryReceipt,
        _settlement: DeliveryReceiptSettlement,
    ) -> Result<bool> {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(record) = records.get_mut(&receipt.sequence) else {
            return Ok(false);
        };
        if record.settled || !record.lease_expires_at.is_after(&Timestamp::now()) {
            return Ok(false);
        }
        record.settled = true;
        let task_id = record.task_id;
        drop(records);
        self.active_by_task
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&task_id);
        Ok(true)
    }
}

/// Converts an overflowing duration computation into a validation error rather
/// than wrapping a receipt into the past.
pub fn receipt_expiry(now: Timestamp, window: std::time::Duration) -> Result<Timestamp> {
    now.checked_add(window).ok_or_else(|| {
        AppError::validation(
            "delivery receipt lease expiry is outside the supported timestamp range",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reservation(task_id: Uuid) -> DeliveryReservationRequest {
        DeliveryReservationRequest {
            consumer_name: "tasks-main".to_owned(),
            task_id,
            tenant_id: Uuid::now_v7(),
            worker_id: "worker-a".to_owned(),
            delivery_count: 1,
            lease_expires_at: receipt_expiry(Timestamp::now(), std::time::Duration::from_secs(30))
                .expect("receipt expiry"),
        }
    }

    #[test]
    fn reservation_rejects_non_live_or_out_of_range_delivery_facts() {
        let task_id = Uuid::now_v7();
        let mut invalid = reservation(task_id);
        invalid.delivery_count = 0;
        assert!(invalid.validate().is_err());
        invalid.delivery_count = 1;
        invalid.lease_expires_at = Timestamp::now();
        assert!(invalid.validate().is_err());
    }

    #[tokio::test]
    async fn live_receipt_excludes_duplicate_until_settlement() {
        let store = InMemoryDeliveryReceiptStore::default();
        let task_id = Uuid::now_v7();
        let first = store
            .reserve(reservation(task_id))
            .await
            .expect("first reservation")
            .expect("first receipt");
        assert!(store
            .reserve(reservation(task_id))
            .await
            .expect("duplicate reservation")
            .is_none());
        assert!(store
            .attach_attempt(first, Uuid::now_v7())
            .await
            .expect("attach attempt"));
        assert!(store
            .settle(first, DeliveryReceiptSettlement::Acked)
            .await
            .expect("settle receipt"));
        let next = store
            .reserve(reservation(task_id))
            .await
            .expect("next reservation")
            .expect("next receipt");
        assert!(next.sequence > first.sequence);
    }
}
