//! The two judgment calls a scheduler makes every tick, as pure functions:
//!
//! * **Catch-up** — a schedule was paused, or the service was down; `N`
//!   planned ticks went by. Firing all of them is a stampede; firing none
//!   silently drops work. [`CatchUpPolicy`] makes the decision explicit and
//!   bounded.
//! * **Overlap** — a run is due while the previous run of the same schedule
//!   is still executing. [`OverlapPolicy`] decides: parallel, skip, or
//!   replace (long runners must declare which they tolerate).

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::schedule::Schedule;

/// Default catch-up behavior: fire the single latest planned tick.
pub const DEFAULT_MAX_CATCH_UP_RUNS: usize = 1;

/// Absolute cap on catch-up runs ever produced for one schedule per tick.
pub const MAX_CATCH_UP_RUNS: usize = 100;

/// What to do with missed planned ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpPolicy {
    /// Skip every missed tick; resume at the next future tick. Default for
    /// schedules created with `catch_up = false`.
    Skip,
    /// Fire exactly the latest missed tick (catch-up marker), skip the rest.
    Latest,
    /// Fire up to `N` most-recent missed ticks (oldest first).
    Bounded {
        /// Maximum missed ticks to replay per catch-up pass.
        max_runs: usize,
    },
}

impl CatchUpPolicy {
    /// The platform default decided by the schedule's `catch_up` flag.
    #[must_use]
    pub fn for_schedule(schedule: &Schedule) -> Self {
        if schedule.catch_up {
            Self::Bounded {
                max_runs: DEFAULT_MAX_CATCH_UP_RUNS + 2,
            }
        } else {
            Self::Skip
        }
    }
}

/// The decision for one schedule's missed window.
#[derive(Debug, Clone, PartialEq)]
pub struct CatchUpDecision {
    /// Planned ticks to dispatch, oldest first. Empty = skip everything.
    pub fire_at: Vec<Timestamp>,
    /// How many planned ticks were skipped (observability; audit reason).
    pub skipped: usize,
}

impl CatchUpDecision {
    /// Nothing fired — entirely skipped window.
    #[must_use]
    pub fn skip_all(skipped: usize) -> Self {
        Self {
            fire_at: Vec::new(),
            skipped,
        }
    }
}

/// Enumerates the missed planned ticks strictly after `since` up to and
/// including `now`, applying the policy.
///
/// The enumeration uses the schedule's own `next_run_at_after`, so cron,
/// interval and one-time rules behave identically; enumeration is capped at
/// [`MAX_CATCH_UP_RUNS`] x 10 iterations regardless of policy to bound CPU on
/// pathological cases (e.g. every-second cron after a year of downtime).
pub fn decide_catch_up(
    schedule: &Schedule,
    since: Timestamp,
    now: &Timestamp,
    policy: CatchUpPolicy,
) -> Result<CatchUpDecision> {
    let target = match policy {
        CatchUpPolicy::Skip => {
            return Ok(CatchUpDecision::skip_all(count_missed(
                schedule, &since, now,
            )?));
        },
        CatchUpPolicy::Latest => 1_usize,
        CatchUpPolicy::Bounded { max_runs } => {
            if max_runs == 0 || max_runs > MAX_CATCH_UP_RUNS {
                return Err(AppError::validation(format!(
                    "max_runs must be 1..={MAX_CATCH_UP_RUNS}"
                )));
            }
            max_runs
        },
    };
    let missed = enumerate_missed(schedule, &since, now)?;
    let skipped = missed.len().saturating_sub(target);
    let fire_at = missed
        .into_iter()
        .rev()
        .take(target)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    Ok(CatchUpDecision { fire_at, skipped })
}

/// Count missed ticks without materializing them all (used for Skip audit).
fn count_missed(schedule: &Schedule, since: &Timestamp, now: &Timestamp) -> Result<usize> {
    Ok(enumerate_missed(schedule, since, now)?.len())
}

fn enumerate_missed(
    schedule: &Schedule,
    since: &Timestamp,
    now: &Timestamp,
) -> Result<Vec<Timestamp>> {
    let mut out = Vec::new();
    let mut cursor = *since;
    let hard_limit = MAX_CATCH_UP_RUNS * 10;
    while out.len() < hard_limit {
        match schedule.next_run_at_after(&cursor) {
            Some(next) if !next.is_after(now) => {
                out.push(next);
                cursor = next;
            },
            _ => break,
        }
    }
    if out.len() == hard_limit {
        // Pathological backlog: we refuse to guess. Operator fixes the
        // schedule; nothing here should loop near-forever.
        return Err(AppError::conflict(format!(
            "schedule {} accumulated more than {hard_limit} missed ticks; \
             pause it or recreate it instead of catching up",
            schedule.id
        )));
    }
    Ok(out)
}

/// What to do when a run is due but the previous run is still executing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverlapPolicy {
    /// Never run two instances of one schedule concurrently.
    #[default]
    Forbid,
    /// Allow parallel instances (dispatcher enforces its own caps).
    Allow,
}

/// Outcome of the overlap decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapAction {
    /// Dispatch the run.
    Dispatch,
    /// Do not dispatch; the tick is recorded as an overlap skip.
    Skip,
}

/// Pure overlap decision.
#[must_use]
pub fn decide_overlap(policy: OverlapPolicy, currently_running: bool) -> OverlapAction {
    match (policy, currently_running) {
        (_, false) => OverlapAction::Dispatch,
        (OverlapPolicy::Allow, true) => OverlapAction::Dispatch,
        (OverlapPolicy::Forbid, true) => OverlapAction::Skip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;
    use mas_domain::schedule::{Schedule, ScheduleKind};
    use std::time::Duration;

    fn schedule_at(tenant: TenantId, every_seconds: u64) -> Schedule {
        let mut s = Schedule::new(
            tenant,
            None,
            "catch-up",
            ScheduleKind::Interval { every_seconds },
            serde_json::json!({"operation": "noop"}),
        )
        .expect("schedule");
        s.enable().expect("enable");
        s
    }

    #[test]
    fn catch_up_latest_fires_only_the_most_recent() {
        let tenant = TenantId::new();
        let schedule = schedule_at(tenant, 3600);
        let now = Timestamp::now();
        let since = now.checked_sub(Duration::from_secs(11_000)).expect("since"); // >3h ago

        let decision =
            decide_catch_up(&schedule, since, &now, CatchUpPolicy::Latest).expect("decision");
        assert_eq!(decision.fire_at.len(), 1);
        assert_eq!(decision.skipped, 2, "3 ticks missed, 1 fires, 2 skipped");
        // Latest = the most recent planned tick (an hour boundary within window).
        let latest = decision.fire_at[0];
        assert!(latest.is_after(&since));
        assert!(!latest.is_after(&now));
        let earlier_next = latest.checked_add(Duration::from_secs(3600)).expect("next");
        assert!(
            earlier_next.is_after(&now),
            "the fired tick must be the last one in the window"
        );
    }

    #[test]
    fn catch_up_bounded_replays_oldest_first_and_skips_the_rest() {
        let tenant = TenantId::new();
        let schedule = schedule_at(tenant, 900); // every 15 minutes
        let now = Timestamp::now();
        let since = now.checked_sub(Duration::from_secs(3_600)).expect("since");

        let decision = decide_catch_up(
            &schedule,
            since,
            &now,
            CatchUpPolicy::Bounded { max_runs: 2 },
        )
        .expect("decision");
        assert_eq!(decision.fire_at.len(), 2);
        assert!(
            decision.fire_at[0].is_before(&decision.fire_at[1]),
            "oldest replayed first"
        );
        assert_eq!(decision.skipped, 2, "4 missed, 2 replayed, 2 skipped");

        assert!(decide_catch_up(
            &schedule,
            since,
            &now,
            CatchUpPolicy::Bounded { max_runs: 0 }
        )
        .is_err());
    }

    #[test]
    fn catch_up_skip_records_the_window_but_fires_nothing() {
        let tenant = TenantId::new();
        let schedule = schedule_at(tenant, 300);
        let now = Timestamp::now();
        let since = now.checked_sub(Duration::from_secs(1_200)).expect("since");

        let decision =
            decide_catch_up(&schedule, since, &now, CatchUpPolicy::Skip).expect("decision");
        assert!(decision.fire_at.is_empty());
        assert_eq!(decision.skipped, 4);

        let policy = CatchUpPolicy::for_schedule(&schedule);
        assert_eq!(
            policy,
            CatchUpPolicy::Skip,
            "catch_up=false defaults to Skip"
        );
    }

    #[test]
    fn one_time_schedules_catch_up_only_when_still_pending() {
        let tenant = TenantId::new();
        let now = Timestamp::now();
        // run_at must be future at construction (domain validation).
        let run_at = now.checked_add(Duration::from_secs(60)).expect("future");
        let mut s = Schedule::new(
            tenant,
            None,
            "once",
            ScheduleKind::OneTime { run_at },
            serde_json::json!({}),
        )
        .expect("schedule");
        s.enable().expect("enable");

        // A future tick is not "missed": catch-up window (since, now] is empty.
        let since = now.checked_sub(Duration::from_secs(10)).expect("since");
        let decision_now = decide_catch_up(&s, since, &now, CatchUpPolicy::Latest)
            .expect("decision before run time");
        assert!(decision_now.fire_at.is_empty());
        assert_eq!(decision_now.skipped, 0);

        // Once the clock passes run_at without a fire (downtime), catch-up
        // sees exactly that one tick.
        let later = run_at.checked_add(Duration::from_secs(10)).expect("later");
        let decision = decide_catch_up(&s, since, &later, CatchUpPolicy::Latest).expect("decision");
        assert_eq!(decision.fire_at, vec![run_at]);
        assert_eq!(decision.skipped, 0);

        // After the run happened, nothing is schedulable ever again.
        let after_done = run_at.checked_add(Duration::from_secs(1)).expect("after");
        let enumerated = enumerate_missed(&s, &after_done, &later).expect("enumerate");
        assert!(enumerated.is_empty());
    }

    #[test]
    fn overlap_truth_table() {
        assert_eq!(
            decide_overlap(OverlapPolicy::Forbid, true),
            OverlapAction::Skip
        );
        assert_eq!(
            decide_overlap(OverlapPolicy::Forbid, false),
            OverlapAction::Dispatch
        );
        assert_eq!(
            decide_overlap(OverlapPolicy::Allow, true),
            OverlapAction::Dispatch
        );
        assert_eq!(OverlapPolicy::default(), OverlapPolicy::Forbid);
    }
}
