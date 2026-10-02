//! The delayed queue: ordered, deduplicated, capacity-bounded, fair.
//!
//! Due work scanned this tick does not always dispatch immediately (dispatch
//! has its own concurrency budget; overlap-skip may want the tick back
//! later). The queue holds runs by `(planned_at, insertion sequence)`, with
//! three invariants:
//!
//! * **Dedup by run key** — the same `(schedule, planned tick)` can never
//!   occupy the queue twice, no matter how many replicas scan;
//! * **Bounded** — past capacity, the *oldest* entry is rejected
//!   (`Conflict`), never silently dropped; operators see backpresssure;
//! * **Fair pop** — `take_due` interleaves per-tenant lanes (scheduler
//!   fairness: no noisy-neighbor starvation inside one tick's batch).

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;

use crate::due::DueWork;

/// Default queue capacity.
pub const DEFAULT_QUEUE_CAPACITY: usize = 10_000;

#[derive(Debug)]
struct Slot {
    sequence: u64,
    work: DueWork,
}

/// The delayed queue (in-memory, single-replica; durable adapters keep the
/// same semantics over the `tasks.not_before` column).
#[derive(Debug)]
pub struct DelayedQueue {
    capacity: usize,
    slots: Vec<Slot>,
    next_sequence: u64,
}

impl Default for DelayedQueue {
    fn default() -> Self {
        Self::new(DEFAULT_QUEUE_CAPACITY)
    }
}

impl DelayedQueue {
    /// Empty queue with explicit capacity (1..=100_000).
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.clamp(1, 100_000),
            slots: Vec::new(),
            next_sequence: 0,
        }
    }

    /// Number of queued runs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Configured capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Enqueues a run in `(planned_at, sequence)` order. Same `run_key` =
    /// dedup no-op (`false`); over capacity = `Conflict` (never silent drop).
    pub fn push(&mut self, work: DueWork) -> Result<bool> {
        if self
            .slots
            .iter()
            .any(|slot| slot.work.run_key() == work.run_key())
        {
            return Ok(false);
        }
        if self.slots.len() >= self.capacity {
            return Err(AppError::conflict(format!(
                "delayed queue is at capacity {} — dispatch is backpressured",
                self.capacity
            )));
        }
        let slot = Slot {
            sequence: self.next_sequence,
            work,
        };
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let position = self
            .slots
            .binary_search_by(|probe| {
                (probe.work.planned_at, probe.sequence).cmp(&(slot.work.planned_at, slot.sequence))
            })
            .unwrap_or_else(|e| e);
        self.slots.insert(position, slot);
        Ok(true)
    }

    /// Removes and returns every run due at `now`, up to `limit`, with
    /// per-tenant fair interleave applied to the taken batch.
    pub fn take_due(&mut self, now: &Timestamp, limit: usize) -> Vec<DueWork> {
        if limit == 0 {
            return Vec::new();
        }
        let cutoff = self
            .slots
            .iter()
            .take_while(|slot| !slot.work.planned_at.is_after(now))
            .take(limit)
            .count();
        let drained: Vec<DueWork> = self.slots.drain(..cutoff).map(|slot| slot.work).collect();
        crate::due::DueScanner::interleave_fair(drained)
    }

    /// Removes all runs of one schedule (pause/disable cleanup). Returns the
    /// number removed.
    pub fn discard_schedule(&mut self, schedule_id: mas_common::ids::ScheduleId) -> usize {
        let before = self.slots.len();
        self.slots
            .retain(|slot| slot.work.schedule_id != schedule_id);
        before - self.slots.len()
    }

    /// Earliest planned tick (the loop's sleep-until hint).
    #[must_use]
    pub fn next_planned(&self) -> Option<Timestamp> {
        self.slots.first().map(|slot| slot.work.planned_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{ScheduleId, TenantId};
    use std::time::Duration;

    fn work(tenant: usize, tick_ms_back: u64, salt: u8) -> DueWork {
        let tenant_id = TenantId::nil();
        // Make distinct tenant ids deterministically: flip one byte.
        let mut bytes = *tenant_id.as_uuid().as_bytes();
        bytes[15] = tenant as u8;
        let tenant_id = TenantId::from_uuid(uuid::Uuid::from_bytes(bytes));
        DueWork {
            schedule_id: {
                let mut b = *ScheduleId::nil().as_uuid().as_bytes();
                b[15] = tenant as u8;
                b[14] = salt;
                ScheduleId::from_uuid(uuid::Uuid::from_bytes(b))
            },
            tenant_id,
            planned_at: Timestamp::now()
                .checked_sub(Duration::from_millis(tick_ms_back))
                .expect("past"),
            task_template: serde_json::json!({"op": salt}),
        }
    }

    #[test]
    fn dedup_and_capacity_bound() {
        let mut queue = DelayedQueue::new(2);
        let w = work(1, 1000, 1);
        assert!(queue.push(w.clone()).expect("push"));
        assert!(
            !queue.push(w.clone()).expect("dedup no-op"),
            "/run key deduplicates"
        );
        queue.push(work(1, 900, 2)).expect("second");
        assert_eq!(queue.len(), 2);
        let err = queue.push(work(2, 800, 3)).expect_err("capacity");
        assert_eq!(err.error_code(), "CONFLICT");
        assert_eq!(queue.len(), 2, "nothing dropped silently");
    }

    #[test]
    fn take_due_is_ordered_fair_and_bounded() {
        let mut queue = DelayedQueue::new(100);
        // Tenant 1: three old runs; tenant 2: one old run; one future run.
        for (back, salt) in [(500, 1), (400, 2), (300, 3)] {
            queue.push(work(1, back, salt)).expect("push");
        }
        queue.push(work(2, 350, 9)).expect("push t2");
        let future = {
            let mut w = work(2, 0, 60);
            w.planned_at = Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .expect("future");
            w
        };
        queue.push(future).expect("push future");

        let now = Timestamp::now();
        let taken = queue.take_due(&now, 100);
        assert_eq!(taken.len(), 4, "future run stays queued");
        assert_eq!(queue.len(), 1);
        assert!(
            queue.next_planned().is_some_and(|p| p.is_future()),
            "only the future run remains queued"
        );

        // Fairness: tenant 2's run cannot be the last one out (that would be
        // lane starvation).
        let t2_pos = taken
            .iter()
            .position(|w| w.tenant_id == work(2, 350, 9).tenant_id)
            .expect("t2 present");
        assert!(t2_pos <= 1, "t2 run within the first round (pos {t2_pos})");

        // Limit is honored.
        let mut queue2 = DelayedQueue::new(10);
        for salt in 0..5u8 {
            queue2
                .push(work(1, 1000 - u64::from(salt), salt))
                .expect("push");
        }
        let taken = queue2.take_due(&now, 2);
        assert_eq!(taken.len(), 2);
        assert_eq!(queue2.len(), 3);
    }

    #[test]
    fn discard_schedule_cleans_lanes() {
        let mut queue = DelayedQueue::new(10);
        let a = work(1, 100, 1);
        // Second tick of the SAME schedule (distinct planned_at).
        let b = DueWork {
            planned_at: Timestamp::now()
                .checked_sub(Duration::from_millis(200))
                .expect("past"),
            ..a.clone()
        };
        let other = work(2, 150, 3);
        queue.push(a.clone()).expect("push a");
        queue.push(b).expect("push b");
        queue.push(other).expect("push other");

        assert_eq!(queue.discard_schedule(a.schedule_id), 2);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.discard_schedule(a.schedule_id), 0);
    }
}
