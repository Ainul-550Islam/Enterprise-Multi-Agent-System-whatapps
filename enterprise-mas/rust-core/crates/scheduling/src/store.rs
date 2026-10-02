//! Schedule storage port + in-memory store.
//!
//! The runner only sees [`ScheduleStorePort`]: candidate scan and post-fire
//! write-back. Production adapters (Postgres behind RLS with the dedicated
//! scheduler role) implement this same contract; the in-memory store here is
//! the semantic reference the integration tests pin against.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use mas_common::ids::ScheduleId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::schedule::Schedule;

/// What the scheduler needs from durable schedule storage.
#[async_trait::async_trait]
pub trait ScheduleStorePort: Send + Sync + fmt::Debug {
    /// Active schedules with `next_run_at <= now`, oldest first, bounded.
    /// (SQL: the `schedules_due_idx` partial index.)
    async fn list_due(&self, now: &Timestamp, limit: usize) -> Result<Vec<Schedule>>;

    /// Loads one schedule (for refresh before write-back).
    async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>>;

    /// Persists the post-fire schedule state (`last_run_at`, `next_run_at`,
    /// status transitions performed by the domain aggregate).
    async fn save(&self, schedule: &Schedule) -> Result<()>;

    /// Persists a run record (`schedule_runs` — one row per planned tick,
    /// enforced by the unique constraint).
    ///
    /// Returns `false` when the tick was already recorded (another replica
    /// beat us; the schedule lease+fencing duplicates this guard).
    async fn record_run(&self, schedule_id: ScheduleId, planned_at: Timestamp) -> Result<bool>;
}

/// In-memory schedule store (tests/dev; mirrors the SQL semantics).
#[derive(Debug, Default)]
pub struct InMemoryScheduleStore {
    schedules: Mutex<BTreeMap<ScheduleId, Schedule>>,
    runs: Mutex<std::collections::BTreeSet<(ScheduleId, i64)>>,
}

impl InMemoryScheduleStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seeds a schedule (test fixture surface).
    pub fn seed(&self, schedule: Schedule) {
        self.schedules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schedule.id, schedule);
    }

    /// Number of stored schedules.
    #[must_use]
    pub fn len(&self) -> usize {
        self.schedules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait::async_trait]
impl ScheduleStorePort for InMemoryScheduleStore {
    async fn list_due(&self, now: &Timestamp, limit: usize) -> Result<Vec<Schedule>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let store = self.schedules.lock().unwrap_or_else(|e| e.into_inner());
        let mut due: Vec<Schedule> = store.values().filter(|s| s.is_due(now)).cloned().collect();
        due.sort_by(|a, b| {
            a.next_run_at
                .cmp(&b.next_run_at)
                .then_with(|| a.id.as_uuid().cmp(b.id.as_uuid()))
        });
        due.truncate(limit);
        Ok(due)
    }

    async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>> {
        Ok(self
            .schedules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }

    async fn save(&self, schedule: &Schedule) -> Result<()> {
        self.schedules
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schedule.id, schedule.clone());
        Ok(())
    }

    async fn record_run(&self, schedule_id: ScheduleId, planned_at: Timestamp) -> Result<bool> {
        let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
        Ok(runs.insert((schedule_id, planned_at.to_unix_ms())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;
    use mas_domain::schedule::ScheduleKind;
    use std::time::Duration;

    fn seeded_schedule(lateness_seconds: u64) -> Schedule {
        let mut s = Schedule::new(
            TenantId::new(),
            None,
            format!("s-{lateness_seconds}"),
            ScheduleKind::Interval {
                every_seconds: 3600,
            },
            serde_json::json!({}),
        )
        .expect("schedule");
        s.enable().expect("enable");
        s.next_run_at = Some(
            Timestamp::now()
                .checked_sub(Duration::from_secs(lateness_seconds))
                .expect("past"),
        );
        s
    }

    #[tokio::test]
    async fn due_listing_is_bounded_and_ordered() {
        let store = InMemoryScheduleStore::new();
        let oldest = seeded_schedule(3600);
        let newest = seeded_schedule(10);
        let not_due = Schedule::new(
            TenantId::new(),
            None,
            "later",
            ScheduleKind::Interval {
                every_seconds: 3600,
            },
            serde_json::json!({}),
        )
        .expect("schedule"); // created disabled → never due
        store.seed(oldest.clone());
        store.seed(newest.clone());
        store.seed(not_due);

        let due = store.list_due(&Timestamp::now(), 10).await.expect("due");
        assert_eq!(due.len(), 2);
        assert_eq!(due[0].id, oldest.id, "oldest planned tick first");
        assert_eq!(due[1].id, newest.id);

        let capped = store.list_due(&Timestamp::now(), 1).await.expect("capped");
        assert_eq!(capped.len(), 1);
        assert!(store
            .list_due(&Timestamp::now(), 0)
            .await
            .expect("zero")
            .is_empty());
    }

    #[tokio::test]
    async fn record_run_is_exactly_once_per_tick() {
        let store = InMemoryScheduleStore::new();
        let id = ScheduleId::new();
        let tick = Timestamp::now();
        assert!(store.record_run(id, tick).await.expect("first"));
        assert!(!store.record_run(id, tick).await.expect("duplicate"));
        let later = tick.checked_add(Duration::from_secs(3600)).expect("later");
        assert!(store.record_run(id, later).await.expect("next tick ok"));
    }
}
