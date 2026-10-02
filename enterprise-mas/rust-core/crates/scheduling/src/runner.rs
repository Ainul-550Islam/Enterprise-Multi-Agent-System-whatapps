//! The deterministic scheduler runtime: scan → lease → queue → dispatch,
//! plus crash recovery.
//!
//! `tick(now)` takes an explicit timestamp, so the whole lifecycle is
//! unit-testable without sleeping; the production driver
//! (`scheduler-service`) simply calls `tick(Timestamp::now())` on a
//! `tokio::time::interval` and renews its leases.
//!
//! One tick:
//! 1. Scan due schedules through the store (`list_due`).
//! 2. For each due schedule: renew its lease (fencing) — contention loses
//!    the schedule to the other replica, and that's *correct*.
//! 3. Catch-up decision (domain rules + policy) and overlap check.
//! 4. Enqueue the resulting runs (queue dedups exact replays).
//! 5. Pop the fair batch and hand it to the [`DispatchPort`].
//! 6. Record runs + write the advanced schedule state back.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ScheduleId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;

use crate::due::{DueScanner, DueWork, ScanConfig};
use crate::lease::{schedule_resource, Lease, LeaseStorePort};
use crate::policies::{
    decide_catch_up, decide_overlap, CatchUpPolicy, OverlapAction, OverlapPolicy,
};
use crate::queue::DelayedQueue;
use crate::store::ScheduleStorePort;

/// How one dispatched work item was accepted downstream.
#[derive(Debug, Clone, PartialEq)]
pub enum DispatchAck {
    /// Execution created; the run is in flight.
    Started {
        /// The created execution tracking id.
        execution: ExecutionId,
    },
    /// Work accepted but deferred (dispatcher at capacity); scheduler will
    /// keep the run queued for the next tick.
    Deferred,
}

/// What the runtime hands work to (the execution engine adapter).
#[async_trait::async_trait]
pub trait DispatchPort: Send + Sync + std::fmt::Debug {
    /// Dispatches one due run; failing here keeps the run queued next tick.
    async fn dispatch(&self, work: &DueWork) -> Result<DispatchAck>;

    /// Whether a schedule currently has an in-flight run (overlap input).
    async fn is_running(&self, schedule: ScheduleId) -> Result<bool>;

    /// Marks a schedule's run finished (success or failure — overlap bookkeeping).
    async fn mark_finished(&self, schedule: ScheduleId) -> Result<()>;
}

/// Runtime configuration.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// This replica's identity (lease holder).
    pub replica_id: String,
    /// Scan size per tick.
    pub scan: ScanConfig,
    /// Max runs dispatched per tick.
    pub dispatch_batch: usize,
    /// Overlap default when the schedule does not declare one.
    pub default_overlap: OverlapPolicy,
    /// Catch-up default when the schedule has `catch_up = false`.
    pub default_catch_up: CatchUpPolicy,
}

impl SchedulerConfig {
    /// Validates the knobs.
    pub fn validated(self) -> Result<Self> {
        if self.replica_id.trim().is_empty() {
            return Err(AppError::validation("replica_id must not be empty"));
        }
        self.scan.validated()?;
        if self.dispatch_batch == 0 || self.dispatch_batch > 10_000 {
            return Err(AppError::validation("dispatch_batch must be 1..=10_000"));
        }
        Ok(self)
    }

    /// Reasonable single-replica defaults for tests/local dev.
    pub fn local(replica_id: impl Into<String>) -> Result<Self> {
        Self {
            replica_id: replica_id.into(),
            scan: ScanConfig::default(),
            dispatch_batch: 100,
            default_overlap: OverlapPolicy::default(),
            default_catch_up: CatchUpPolicy::Skip,
        }
        .validated()
    }
}

/// What happened in one tick (observability + tests).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TickReport {
    /// Schedules the store returned as candidates.
    pub scanned: usize,
    /// Due schedules after filtering.
    pub due: usize,
    /// Runs dispatched successfully this tick.
    pub dispatched: usize,
    /// Runs skipped by overlap policy.
    pub overlap_skipped: usize,
    /// Runs skipped by catch-up policy.
    pub catch_up_skipped: usize,
    /// Schedules lost to lease contention (another replica owns them).
    pub lease_lost: usize,
    /// Dispatch failures (runs remain queued; retried next tick).
    pub dispatch_errors: usize,
}

/// What startup recovery did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Leases reaped as expired (their holders are presumed dead).
    pub leases_reaped: usize,
    /// Due schedules immediately re-enqueued after restart.
    pub schedules_rescanned: usize,
}

/// The deterministic scheduler runtime.
pub struct SchedulerRuntime<S: ScheduleStorePort, L: LeaseStorePort, D: DispatchPort> {
    store: S,
    leases: L,
    dispatch: D,
    queue: DelayedQueue,
    config: SchedulerConfig,
}

impl<S, L, D> std::fmt::Debug for SchedulerRuntime<S, L, D>
where
    S: ScheduleStorePort,
    L: LeaseStorePort,
    D: DispatchPort,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchedulerRuntime")
            .field("replica", &self.config.replica_id)
            .field("queue_len", &self.queue.len())
            .finish_non_exhaustive()
    }
}

impl<S: ScheduleStorePort, L: LeaseStorePort, D: DispatchPort> SchedulerRuntime<S, L, D> {
    /// Builds a runtime from its ports.
    pub fn new(store: S, leases: L, dispatch: D, config: SchedulerConfig) -> Result<Self> {
        let config = config.validated()?;
        Ok(Self {
            store,
            leases,
            dispatch,
            queue: DelayedQueue::default(),
            config,
        })
    }

    /// Startup recovery: reaps expired leases (so due schedules are
    /// acquirable again) and re-enqueues anything already due.
    pub async fn recover(&mut self, now: &Timestamp) -> Result<RecoveryReport> {
        let reaped = self.leases.reap_expired(now).await?;
        if !reaped.is_empty() {
            tracing::info!(count = reaped.len(), "reaped expired scheduler leases");
        }
        let due = self
            .store
            .list_due(now, self.config.scan.batch_limit)
            .await?;
        let scanned = due.len();
        let batch = DueScanner::scan(&due, now, &self.config.scan)?;
        for work in batch {
            let _ = self.queue.push(work)?;
        }
        Ok(RecoveryReport {
            leases_reaped: reaped.len(),
            schedules_rescanned: scanned,
        })
    }

    /// One scheduler tick. Never fails wholesale: per-schedule failures are
    /// counted in the report and retried next tick.
    pub async fn tick(&mut self, now: &Timestamp) -> Result<TickReport> {
        let mut report = TickReport::default();

        // 1) scan
        let candidates = self
            .store
            .list_due(now, self.config.scan.batch_limit)
            .await?;
        report.scanned = candidates.len();
        let due_work = DueScanner::scan(&candidates, now, &self.config.scan)?;
        report.due = due_work.len();

        // 2-4) per-schedule pipeline
        for work in due_work {
            match self.prepare_run(&work, now).await {
                Prepared::Runs(works) => {
                    for work in works {
                        let _ = self.queue.push(work)?;
                    }
                },
                Prepared::OverlapSkip => report.overlap_skipped += 1,
                Prepared::CatchUpSkip(skipped) => report.catch_up_skipped += skipped,
                Prepared::LeaseLost => report.lease_lost += 1,
            }
        }

        // 5) dispatch the fair batch
        let batch = self.queue.take_due(now, self.config.dispatch_batch);
        for work in batch {
            match self.dispatch.dispatch(&work).await {
                Ok(DispatchAck::Started { .. }) => {
                    report.dispatched += 1;
                    self.complete_run(&work, now).await;
                },
                Ok(DispatchAck::Deferred) => {
                    // Back in the queue for next tick.
                    if let Err(err) = self.queue.push(work.clone()) {
                        tracing::warn!(error = %err, run = %work.run_key(), "re-queue failed");
                    }
                },
                Err(err) => {
                    report.dispatch_errors += 1;
                    tracing::warn!(
                        error = %err,
                        run = %work.run_key(),
                        "dispatch failed; run re-queued"
                    );
                    if let Err(push_err) = self.queue.push(work.clone()) {
                        tracing::warn!(error = %push_err, run = %work.run_key(), "re-queue failed");
                    }
                },
            }
        }
        Ok(report)
    }

    /// Runs one due schedule through lease + catch-up + overlap, returning
    /// the runs to enqueue (0..=1+N: bounded catch-up replays then the due
    /// tick) or the skip classification.
    async fn prepare_run(&self, work: &DueWork, now: &Timestamp) -> Prepared {
        // 1. Lease: fencing against the other replica. Losing is normal
        //    multi-replica operation, not an error.
        let resource = schedule_resource(&work.schedule_id);
        match self
            .leases
            .try_acquire(
                &resource,
                &self.config.replica_id,
                crate::lease::DEFAULT_LEASE_TTL,
                now,
            )
            .await
        {
            Ok(lease) => {
                self.note_lease_held(work.schedule_id, lease);
            },
            Err(err) if err.error_code() == "CONFLICT" => return Prepared::LeaseLost,
            Err(err) => {
                tracing::warn!(error = %err, resource = %resource, "lease acquisition failed");
                return Prepared::LeaseLost;
            },
        }

        // 2. Refresh the schedule (it may have changed since the scan).
        let Some(schedule) = (match self.store.get(work.schedule_id).await {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(error = %err, "schedule refresh failed");
                return Prepared::LeaseLost;
            },
        }) else {
            return Prepared::CatchUpSkip(1);
        };

        // 3. Overlap: previous run still executing?
        let running = self.dispatch.is_running(schedule.id).await.unwrap_or(false);
        if decide_overlap(self.config.default_overlap, running) == OverlapAction::Skip {
            return Prepared::OverlapSkip;
        }

        // 4. Catch-up over ticks *strictly before* the due tick:
        //    enumerate planned ticks after `last_run_at` but before this
        //    tick — those are the ones a pause/downtime made us miss.
        let since = schedule.last_run_at.unwrap_or(work.planned_at);
        let before_due = work
            .planned_at
            .checked_sub(std::time::Duration::from_millis(1))
            .unwrap_or(work.planned_at);
        let policy = CatchUpPolicy::for_schedule(&schedule);
        let decision = match decide_catch_up(&schedule, since, &before_due, policy) {
            Ok(d) => d,
            Err(err) => {
                tracing::warn!(error = %err, schedule = %schedule.id, "catch-up decision failed");
                return Prepared::CatchUpSkip(1);
            },
        };

        let mut runs: Vec<DueWork> = decision
            .fire_at
            .iter()
            .map(|planned_at| DueWork {
                planned_at: *planned_at,
                ..work.clone()
            })
            .collect();
        // The due tick itself always fires.
        runs.push(work.clone());
        Prepared::Runs(runs)
    }

    fn note_lease_held(&self, schedule: ScheduleId, lease: Lease) {
        let _ = (schedule, lease);
        // Held-lease registry lives with the driving service
        // (scheduler-service), which renews/releases on shutdown; the
        // runtime itself treats acquisition as the fencing point.
    }

    /// After a successful dispatch: record the run + advance the schedule.
    async fn complete_run(&self, work: &DueWork, now: &Timestamp) {
        if let Err(err) = self
            .store
            .record_run(work.schedule_id, work.planned_at)
            .await
        {
            tracing::warn!(error = %err, "run record write failed (exactly-once ledger)");
        }
        if let Ok(Some(mut schedule)) = self.store.get(work.schedule_id).await {
            let fired_at = *now;
            if let Err(err) = schedule.mark_fired(fired_at) {
                tracing::warn!(error = %err, "schedule advance failed");
                return;
            }
            if let Err(err) = self.store.save(&schedule).await {
                tracing::warn!(error = %err, "schedule write-back failed");
            }
        }
    }

    /// Queue depth (observability).
    #[must_use]
    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }
}

enum Prepared {
    Runs(Vec<DueWork>),
    OverlapSkip,
    CatchUpSkip(usize),
    LeaseLost,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::InMemoryLeaseStore;
    use crate::store::InMemoryScheduleStore;
    use mas_common::ids::TenantId;
    use mas_domain::schedule::{Schedule, ScheduleKind};
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct FakeDispatch {
        started: AtomicUsize,
        running: Mutex<BTreeSet<ScheduleId>>,
        failing_left: Mutex<u32>,
    }

    #[async_trait::async_trait]
    impl DispatchPort for FakeDispatch {
        async fn dispatch(&self, work: &DueWork) -> Result<DispatchAck> {
            let mut failing = self.failing_left.lock().unwrap_or_else(|e| e.into_inner());
            if *failing > 0 {
                *failing -= 1;
                return Err(AppError::internal("synthetic dispatch failure"));
            }
            drop(failing);
            self.started.fetch_add(1, Ordering::SeqCst);
            self.running
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(work.schedule_id);
            Ok(DispatchAck::Started {
                execution: ExecutionId::new(),
            })
        }

        async fn is_running(&self, schedule: ScheduleId) -> Result<bool> {
            Ok(self
                .running
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&schedule))
        }

        async fn mark_finished(&self, schedule: ScheduleId) -> Result<()> {
            self.running
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&schedule);
            Ok(())
        }
    }

    fn due_schedule(name: &str, every_seconds: u64, lateness: u64) -> Schedule {
        let mut s = Schedule::new(
            TenantId::new(),
            None,
            name,
            ScheduleKind::Interval { every_seconds },
            serde_json::json!({"operation": "noop"}),
        )
        .expect("schedule");
        s.enable().expect("enable");
        s.next_run_at = Some(
            Timestamp::now()
                .checked_sub(std::time::Duration::from_secs(lateness))
                .expect("past"),
        );
        s
    }

    #[tokio::test]
    async fn full_tick_dispatches_once_and_advances_schedule() {
        let store = InMemoryScheduleStore::new();
        let schedule = due_schedule("hourly", 3600, 30);
        let id = schedule.id;
        store.seed(schedule);
        let mut runtime = SchedulerRuntime::new(
            store,
            InMemoryLeaseStore::new(),
            FakeDispatch::default(),
            SchedulerConfig::local("replica-1").expect("config"),
        )
        .expect("runtime");

        let now = Timestamp::now();
        let report = runtime.tick(&now).await.expect("tick");
        assert_eq!(report.scanned, 1);
        assert_eq!(report.due, 1);
        assert_eq!(report.dispatched, 1);
        assert_eq!(report.catch_up_skipped, 0);
        assert_eq!(runtime.queue_len(), 0);

        // Schedule advanced: next_run_at moved past now; not due anymore.
        let stored = runtime.store.get(id).await.expect("get").expect("exists");
        assert!(stored.last_run_at.is_some());
        assert!(stored.next_run_at.is_some_and(|n| n.is_future()));

        // Next tick: nothing due — exactly-once.
        let report = runtime.tick(&now).await.expect("tick2");
        assert_eq!(report.due, 0);
        assert_eq!(report.dispatched, 0);
    }

    #[tokio::test]
    async fn overlap_skip_keeps_the_tick_for_later() {
        let store = InMemoryScheduleStore::new();
        let schedule = due_schedule("long", 60, 30);
        store.seed(schedule.clone());
        let dispatch = FakeDispatch::default();
        dispatch
            .running
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schedule.id); // pretend previous run still going
        let mut runtime = SchedulerRuntime::new(
            store,
            InMemoryLeaseStore::new(),
            dispatch,
            SchedulerConfig::local("replica-1").expect("config"),
        )
        .expect("runtime");

        let now = Timestamp::now();
        let report = runtime.tick(&now).await.expect("tick");
        assert_eq!(report.dispatched, 0);
        assert_eq!(
            report.overlap_skipped, 1,
            "running schedule skips this tick"
        );
    }

    #[tokio::test]
    async fn dispatch_failure_requeues_and_recovers_on_reap() {
        let store = InMemoryScheduleStore::new();
        let schedule = due_schedule("flaky", 3600, 30);
        store.seed(schedule);
        let dispatch = FakeDispatch::default();
        *dispatch
            .failing_left
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = 1; // first call fails
        let mut runtime = SchedulerRuntime::new(
            store,
            InMemoryLeaseStore::new(),
            dispatch,
            SchedulerConfig::local("replica-1").expect("config"),
        )
        .expect("runtime");

        let now = Timestamp::now();
        let report = runtime.tick(&now).await.expect("tick");
        assert_eq!(report.dispatch_errors, 1);
        assert_eq!(report.dispatched, 0);
        assert_eq!(runtime.queue_len(), 1, "failed run awaits next tick");

        // Same tick again would re-dispatch the queued run (now succeeding).
        let report = runtime.tick(&now).await.expect("retry tick");
        assert_eq!(report.dispatched, 1);
        assert_eq!(runtime.queue_len(), 0);

        // Recovery: expired leases reaped + due work re-enqueued.
        let leases = InMemoryLeaseStore::new();
        let stale = leases
            .try_acquire(
                &schedule_resource(&ScheduleId::new()),
                "dead-replica",
                std::time::Duration::from_secs(0),
                &now,
            )
            .await
            .expect("stale lease");
        assert!(!stale.is_active(
            &now.checked_add(std::time::Duration::from_secs(1))
                .unwrap_or(now)
        ));
        let mut runtime2 = SchedulerRuntime::new(
            InMemoryScheduleStore::new(),
            leases,
            FakeDispatch::default(),
            SchedulerConfig::local("replica-2").expect("config"),
        )
        .expect("runtime2");
        let report = runtime2
            .recover(
                &now.checked_add(std::time::Duration::from_secs(5))
                    .unwrap_or(now),
            )
            .await
            .expect("recover");
        assert_eq!(report.leases_reaped, 1);
    }
}
