//! Due scanning with per-tenant fairness.
//!
//! The scanner receives the candidate schedules for a tick (already filtered
//! to `status = active` by the store) and produces the work list. Its two
//! invariants:
//!
//! 1. **Determinism** — same inputs, same order, no clocks hidden inside;
//!    `now` is an explicit parameter.
//! 2. **Fairness** — after ordering by planned time, the batch is interleaved
//!    round-robin across tenants so a tenant with thousands of due schedules
//!    cannot push another tenant past the batch limit on every tick.

use mas_common::error::AppError;
use mas_common::ids::{ScheduleId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::schedule::Schedule;
use serde_json::Value;

/// Default scan batch size (per tick, across all tenants).
pub const DEFAULT_SCAN_BATCH: usize = 500;

/// Hard cap on the scan batch — a tick bigger than this indicates operator
/// error and risks a thundering herd on dispatch.
pub const MAX_SCAN_BATCH: usize = 10_000;

/// One due schedule run ready for dispatch.
#[derive(Debug, Clone, PartialEq)]
pub struct DueWork {
    /// The schedule that fired.
    pub schedule_id: ScheduleId,
    /// Owning tenant (RLS scope for dispatch and fairness key).
    pub tenant_id: TenantId,
    /// The planned fire time (stable identity of this run; deduplicated by
    /// the delayed queue together with `schedule_id`).
    pub planned_at: Timestamp,
    /// The schedule's task template (opaque to the scanner; the dispatcher
    /// materializes it).
    pub task_template: Value,
}

impl DueWork {
    /// Identity of one planned run: the schedule plus its planned tick.
    #[must_use]
    pub fn run_key(&self) -> String {
        format!(
            "{}:{}",
            self.schedule_id.as_uuid(),
            self.planned_at.to_unix_ms()
        )
    }
}

/// Configuration for one scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanConfig {
    /// Max schedules dispatched per tick.
    pub batch_limit: usize,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            batch_limit: DEFAULT_SCAN_BATCH,
        }
    }
}

impl ScanConfig {
    /// Validates bounds.
    pub fn validated(self) -> Result<Self> {
        if self.batch_limit == 0 || self.batch_limit > MAX_SCAN_BATCH {
            return Err(AppError::validation(format!(
                "scan batch must be 1..={MAX_SCAN_BATCH}"
            )));
        }
        Ok(self)
    }
}

/// Pure due scanner.
#[derive(Debug, Clone, Copy, Default)]
pub struct DueScanner;

impl DueScanner {
    /// Filters and orders `candidates` into at most `batch_limit` due items.
    ///
    /// Order: ascending `planned_at`, ties broken by schedule id — then
    /// round-robin interleave across tenants (fair), preserving the relative
    /// chronological order inside each tenant's lane.
    pub fn scan(
        candidates: &[Schedule],
        now: &Timestamp,
        config: &ScanConfig,
    ) -> Result<Vec<DueWork>> {
        let config = config.validated()?;
        let mut due: Vec<DueWork> = candidates
            .iter()
            .filter(|schedule| schedule.is_due(now))
            .filter_map(|schedule| {
                schedule.next_run_at.map(|planned_at| DueWork {
                    schedule_id: schedule.id,
                    tenant_id: schedule.tenant_id,
                    planned_at,
                    task_template: schedule.task_template.clone(),
                })
            })
            .collect();
        due.sort_by(|a, b| {
            a.planned_at
                .cmp(&b.planned_at)
                .then_with(|| a.schedule_id.as_uuid().cmp(b.schedule_id.as_uuid()))
        });
        Ok(Self::interleave_fair(due)
            .into_iter()
            .take(config.batch_limit)
            .collect())
    }

    /// Round-robin across tenants while preserving per-lane chronology.
    ///
    /// Implementation: index items by tenant lane, repeatedly pull the head
    /// of each lane in ascending tenant order. A lane's ticks always stay
    /// chronological; lanes interleave.
    #[must_use]
    pub fn interleave_fair(items: Vec<DueWork>) -> Vec<DueWork> {
        use std::collections::{BTreeMap, VecDeque};
        let mut lanes: BTreeMap<TenantId, VecDeque<DueWork>> = BTreeMap::new();
        for item in items {
            lanes.entry(item.tenant_id).or_default().push_back(item);
        }
        let mut out = Vec::new();
        loop {
            let mut progressed = false;
            for lane in lanes.values_mut() {
                if let Some(item) = lane.pop_front() {
                    out.push(item);
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::ScheduleStatus;
    use mas_domain::schedule::{Schedule, ScheduleKind};
    use std::time::Duration;

    fn schedule(tenant: TenantId, name: &str) -> Schedule {
        let mut s = Schedule::new(
            tenant,
            None,
            name,
            ScheduleKind::Interval {
                every_seconds: 3600,
            },
            serde_json::json!({"operation": "noop"}),
        )
        .expect("schedule");
        s.enable().expect("enable");
        assert_eq!(s.status, ScheduleStatus::Active);
        s
    }

    fn backdate(schedule: &mut Schedule, seconds: u64) {
        // Force the cached next run into the past so is_due(now) is true.
        schedule.next_run_at = Some(
            Timestamp::now()
                .checked_sub(Duration::from_secs(seconds))
                .expect("past"),
        );
    }

    #[test]
    fn scan_filters_due_and_orders_chronologically_per_tenant() {
        let tenant = TenantId::new();
        let mut a = schedule(tenant, "a");
        backdate(&mut a, 120);
        let mut b = schedule(tenant, "b");
        backdate(&mut b, 3600);
        let future = schedule(tenant, "future"); // still enabled, not due

        let batch = DueScanner::scan(
            &[a.clone(), b.clone(), future],
            &Timestamp::now(),
            &ScanConfig::default(),
        )
        .expect("scan");
        assert_eq!(batch.len(), 2);
        // b is older → first within the lane.
        assert_eq!(batch[0].schedule_id, b.id);
        assert_eq!(batch[1].schedule_id, a.id);
        assert!(batch.iter().all(|w| w.tenant_id == tenant));
    }

    #[test]
    fn interleave_is_fair_across_tenants() {
        let t1 = TenantId::new();
        let t2 = TenantId::new();
        let now = Timestamp::now();
        let work = |tenant: TenantId, suffix: &str, age: u64| {
            let mut s = schedule(tenant, suffix);
            backdate(&mut s, age);
            DueWork {
                schedule_id: s.id,
                tenant_id: tenant,
                planned_at: s.next_run_at.expect("next"),
                task_template: serde_json::json!({}),
            }
        };
        // t1 has 3 due runs, t2 has 1: without fairness t2's could be pushed
        // past a small batch limit by t1's.
        let items = vec![
            work(t1, "a", 300),
            work(t1, "b", 200),
            work(t1, "c", 100),
            work(t2, "z", 250),
        ];
        let interleaved = DueScanner::interleave_fair(items.clone());
        assert_eq!(interleaved.len(), 4);
        // Round-robin starts with t1 lane (tenant ids sorted) then t2:
        let t2_positions: Vec<usize> = interleaved
            .iter()
            .enumerate()
            .filter(|(_, w)| w.tenant_id == t2)
            .map(|(i, _)| i)
            .collect();
        assert!(
            t2_positions[0] <= 1,
            "t2's lone run must appear within the first round ({t2_positions:?})"
        );
        // Now the same via scan with a tiny limit: t2 must survive.
        let mut schedules: Vec<Schedule> = items
            .iter()
            .map(|w| {
                let mut s = schedule(w.tenant_id, "x");
                s.id = w.schedule_id;
                s.next_run_at = Some(w.planned_at);
                s
            })
            .collect();
        for s in &mut schedules {
            let _ = now;
            s.next_run_at = Some(
                Timestamp::now()
                    .checked_sub(Duration::from_secs(60))
                    .expect("past"),
            );
        }
        let batch = DueScanner::scan(
            &schedules,
            &Timestamp::now(),
            &ScanConfig { batch_limit: 2 },
        )
        .expect("scan");
        let tenants: Vec<TenantId> = batch.iter().map(|w| w.tenant_id).collect();
        assert!(
            tenants.contains(&t2),
            "batch of 2 must not be monopolized by one tenant: {tenants:?}"
        );
    }

    #[test]
    fn due_work_run_keys_are_stable_and_unique() {
        let tenant = TenantId::new();
        let mut s = schedule(tenant, "k");
        backdate(&mut s, 10);
        let w1 = DueWork {
            schedule_id: s.id,
            tenant_id: tenant,
            planned_at: s.next_run_at.expect("next"),
            task_template: serde_json::json!({}),
        };
        let same = DueWork {
            planned_at: w1.planned_at,
            ..w1.clone()
        };
        assert_eq!(w1.run_key(), same.run_key(), "same tick, same key");
        let later = DueWork {
            planned_at: w1
                .planned_at
                .checked_add(Duration::from_secs(3600))
                .expect("later"),
            ..w1.clone()
        };
        assert_ne!(w1.run_key(), later.run_key());

        assert!(DueScanner::scan(&[s], &Timestamp::now(), &ScanConfig { batch_limit: 0 }).is_err());
    }
}
