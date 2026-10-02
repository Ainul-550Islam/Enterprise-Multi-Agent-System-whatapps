//! Runtime work scheduler: fair, priority-ordered selection of pending work.
//!
//! Ordering contract (evaluated in order):
//! 1. **Priority first** — `critical` lanes drain before `high`, etc.
//! 2. **Fairness within a priority** — round-robin across tenants, so one
//!    tenant can never starve others.
//! 3. **FIFO within (priority, tenant)** — stable per-tenant ordering.
//!
//! Work may carry `not_before` for scheduled/delayed retries; a lane whose
//! head is not yet due is skipped (not re-ordered) so per-tenant FIFO holds.
//!
//! The scheduler is in-memory and rebuilt from the task store on recovery;
//! persistence is the engine's concern.

use mas_common::enums::TaskPriority;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::Task;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

/// One schedulable unit of runtime work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledWork {
    /// The task being scheduled (when the work is task-shaped).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    /// Execution it belongs to (when spawned by an execution).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    pub tenant_id: TenantId,
    pub priority: TaskPriority,
    /// 1-based attempt (retry attempts re-enqueue with `not_before`).
    pub attempt: u32,
    /// Earliest dispatch time (delayed retries / scheduled work).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<Timestamp>,
    pub queued_at: Timestamp,
}

impl ScheduledWork {
    /// Schedules a task directly from its stored row.
    pub fn for_task(task: &Task, not_before: Option<Timestamp>) -> Self {
        Self {
            task_id: Some(task.id),
            execution_id: task.execution_id,
            tenant_id: task.tenant_id,
            priority: task.priority,
            attempt: task.attempt_count.max(1),
            not_before,
            queued_at: task.queued_at.unwrap_or_else(Timestamp::now),
        }
    }

    /// Schedules synthetic execution-level work (e.g. engine-internal steps).
    pub fn for_execution(
        execution_id: ExecutionId,
        tenant_id: TenantId,
        priority: TaskPriority,
    ) -> Self {
        Self {
            task_id: None,
            execution_id: Some(execution_id),
            tenant_id,
            priority,
            attempt: 1,
            not_before: None,
            queued_at: Timestamp::now(),
        }
    }

    /// Is the work due at `now`?
    #[must_use]
    pub fn is_due(&self, now: Timestamp) -> bool {
        self.not_before.is_none_or(|nb| !now.is_before(&nb))
    }
}

#[derive(Debug, Default)]
struct SchedulerInner {
    /// Lane per (priority rank, tenant), FIFO. Ranks ordered high→low in use.
    lanes: BTreeMap<(u8, TenantId), VecDeque<ScheduledWork>>,
    /// Round-robin tenant order per priority rank.
    rotation: BTreeMap<u8, Vec<TenantId>>,
    /// Round-robin cursor per priority rank.
    cursor: BTreeMap<u8, usize>,
    /// Total queued entries (for fast emptiness checks).
    len: usize,
}

const RANKS: [u8; 4] = [
    TaskPriority::Critical.rank(),
    TaskPriority::High.rank(),
    TaskPriority::Normal.rank(),
    TaskPriority::Low.rank(),
];

/// The fair priority scheduler. Thread-safe; `next_ready` is O(#tenants).
#[derive(Debug, Default)]
pub struct RuntimeScheduler {
    inner: Mutex<SchedulerInner>,
}

impl RuntimeScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds work to its (priority, tenant) lane.
    pub fn enqueue(&self, work: ScheduledWork) -> Result<()> {
        let mut inner = self.lock();
        let rank = work.priority.rank();
        let lane_key = (rank, work.tenant_id);
        // Per-lane depth guard: unbounded queues are a memory-DoS vector.
        let lane = inner.lanes.entry(lane_key).or_default();
        if lane.len() >= mas_common::constants::MAX_STEPS_PER_EXECUTION as usize {
            return Err(AppError::rate_limited(format!(
                "scheduling lane for tenant {} at priority {rank} is full",
                work.tenant_id
            )));
        }
        lane.push_back(work);
        let rotation = inner.rotation.entry(rank).or_default();
        if !rotation.contains(&lane_key.1) {
            rotation.push(lane_key.1);
        }
        inner.len += 1;
        Ok(())
    }

    /// Pops the next due work item under the fairness contract, or `None`.
    pub fn next_ready(&self, now: Timestamp) -> Option<ScheduledWork> {
        let mut inner = self.lock();
        for rank in RANKS {
            let Some(rotation) = inner.rotation.get(&rank).cloned() else {
                continue;
            };
            if rotation.is_empty() {
                continue;
            }
            let tenants = rotation.len();
            let start = *inner.cursor.get(&rank).unwrap_or(&0) % tenants;
            for offset in 0..tenants {
                let index = (start + offset) % tenants;
                let tenant = rotation[index];
                let due = inner
                    .lanes
                    .get(&(rank, tenant))
                    .and_then(|lane| lane.front())
                    .is_some_and(|work| work.is_due(now));
                if !due {
                    continue;
                }
                // Existence was just proven above; `?` here is unreachable.
                let work = inner
                    .lanes
                    .get_mut(&(rank, tenant))
                    .and_then(VecDeque::pop_front)?;
                if inner
                    .lanes
                    .get(&(rank, tenant))
                    .is_some_and(VecDeque::is_empty)
                {
                    inner.lanes.remove(&(rank, tenant));
                    if let Some(rot) = inner.rotation.get_mut(&rank) {
                        rot.retain(|entry| *entry != tenant);
                    }
                    let remaining = inner.rotation.get(&rank).map_or(0, Vec::len);
                    if remaining == 0 {
                        inner.rotation.remove(&rank);
                        inner.cursor.insert(rank, 0);
                    } else {
                        // Next tenant in rotation order after the served one.
                        inner.cursor.insert(rank, index % remaining);
                    }
                } else {
                    // Advance cursor past the tenant we just served.
                    inner.cursor.insert(rank, (index + 1) % tenants);
                }
                inner.len = inner.len.saturating_sub(1);
                return Some(work);
            }
        }
        None
    }

    /// Removes all queued work for a task (cancellation). Returns whether
    /// anything was removed.
    pub fn cancel_task(&self, task_id: TaskId) -> bool {
        self.remove_matching(|work| work.task_id == Some(task_id)) > 0
    }

    /// Removes all queued work of an execution; returns how much was removed.
    pub fn cancel_execution(&self, execution_id: ExecutionId) -> usize {
        self.remove_matching(|work| work.execution_id == Some(execution_id))
    }

    /// Total queued work items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Queued items for one tenant across all priorities (diagnostics).
    #[must_use]
    pub fn tenant_len(&self, tenant_id: TenantId) -> usize {
        let inner = self.lock();
        inner
            .lanes
            .iter()
            .filter(|((_, tenant), _)| *tenant == tenant_id)
            .map(|(_, lane)| lane.len())
            .sum()
    }

    /// Snapshot of per-(rank, tenant) queue depths (observability export).
    #[must_use]
    pub fn lane_depths(&self) -> BTreeMap<(u8, TenantId), usize> {
        self.lock()
            .lanes
            .iter()
            .map(|(key, lane)| (*key, lane.len()))
            .collect()
    }

    // -- internals ------------------------------------------------------------

    fn lock(&self) -> std::sync::MutexGuard<'_, SchedulerInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn remove_matching(&self, predicate: impl Fn(&ScheduledWork) -> bool) -> usize {
        let mut inner = self.lock();
        let mut removed = 0usize;
        let lane_keys: Vec<(u8, TenantId)> = inner.lanes.keys().copied().collect();
        for key in lane_keys {
            if let Some(lane) = inner.lanes.get_mut(&key) {
                let before = lane.len();
                lane.retain(|work| !predicate(work));
                removed += before - lane.len();
                if lane.is_empty() {
                    inner.lanes.remove(&key);
                    if let Some(rot) = inner.rotation.get_mut(&key.0) {
                        rot.retain(|t| *t != key.1);
                    }
                    inner.cursor.insert(key.0, 0);
                }
            }
        }
        inner.len = inner.len.saturating_sub(removed);
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::TaskPriority;
    use mas_common::ids::{OrganizationId, ProjectId};
    use mas_domain::Task;
    use std::time::Duration;

    fn tenant_work(tenant: TenantId, priority: TaskPriority, name: u8) -> ScheduledWork {
        let mut work = ScheduledWork::for_execution(ExecutionId::new(), tenant, priority);
        work.attempt = u32::from(name);
        work
    }

    fn stored_task(tenant: TenantId, priority: TaskPriority, key: &str) -> Task {
        Task::new(
            tenant,
            OrganizationId::new(),
            ProjectId::new(),
            Some(mas_common::ids::AgentId::new()),
            None,
            "agent.run",
            serde_json::json!({}),
            priority,
            key,
            "tester",
        )
        .expect("task")
    }

    #[test]
    fn priority_beats_fairness() {
        let scheduler = RuntimeScheduler::new();
        let t1 = TenantId::new();
        let t2 = TenantId::new();
        scheduler
            .enqueue(tenant_work(t1, TaskPriority::Low, 1))
            .unwrap();
        scheduler
            .enqueue(tenant_work(t2, TaskPriority::Critical, 2))
            .unwrap();
        let first = scheduler
            .next_ready(Timestamp::now())
            .expect("critical first");
        assert_eq!(first.priority, TaskPriority::Critical);
        let second = scheduler.next_ready(Timestamp::now()).expect("then low");
        assert_eq!(second.priority, TaskPriority::Low);
        assert!(scheduler.is_empty());
        assert!(scheduler.next_ready(Timestamp::now()).is_none());
    }

    #[test]
    fn round_robin_within_priority() {
        let scheduler = RuntimeScheduler::new();
        let t1 = TenantId::new();
        let t2 = TenantId::new();
        for name in 1..=3 {
            scheduler
                .enqueue(tenant_work(t1, TaskPriority::Normal, name))
                .unwrap();
        }
        scheduler
            .enqueue(tenant_work(t2, TaskPriority::Normal, 9))
            .unwrap();
        assert_eq!(scheduler.tenant_len(t1), 3);
        assert_eq!(scheduler.tenant_len(t2), 1);
        assert_eq!(scheduler.len(), 4);

        let mut served_t2_at = None;
        for pull in 0..4 {
            let work = scheduler.next_ready(Timestamp::now()).expect("work");
            if work.tenant_id == t2 {
                served_t2_at = Some(pull);
            }
        }
        // t2 must be served before t1's queue is exhausted (fairness).
        assert_eq!(served_t2_at, Some(1));
        assert!(scheduler.is_empty());
    }

    #[test]
    fn not_before_defers_the_lane_head_only() {
        let scheduler = RuntimeScheduler::new();
        let t1 = TenantId::new();
        let future = Timestamp::now()
            .checked_add(Duration::from_secs(3600))
            .expect("future timestamp");
        let mut delayed = tenant_work(t1, TaskPriority::Normal, 1);
        delayed.not_before = Some(future);
        scheduler.enqueue(delayed).unwrap();
        scheduler
            .enqueue(tenant_work(t1, TaskPriority::Normal, 2))
            .unwrap();
        // Head is not due ⇒ nothing served (per-tenant FIFO holds).
        assert!(scheduler.next_ready(Timestamp::now()).is_none());
        assert_eq!(scheduler.len(), 2);
    }

    #[test]
    fn scheduling_from_stored_tasks_and_cancellation() {
        let scheduler = RuntimeScheduler::new();
        let tenant = TenantId::new();
        let mut task = stored_task(tenant, TaskPriority::High, "idem-1");
        task.queue().expect("queued");
        scheduler
            .enqueue(ScheduledWork::for_task(&task, None))
            .unwrap();
        assert_eq!(scheduler.len(), 1);

        assert!(scheduler.cancel_task(task.id));
        assert_eq!(scheduler.len(), 0);
        assert!(!scheduler.cancel_task(TaskId::new()));

        // Execution-scoped cancellation.
        let execution = ExecutionId::new();
        scheduler
            .enqueue(ScheduledWork::for_execution(
                execution,
                tenant,
                TaskPriority::Normal,
            ))
            .unwrap();
        scheduler
            .enqueue(ScheduledWork::for_execution(
                execution,
                tenant,
                TaskPriority::High,
            ))
            .unwrap();
        assert_eq!(scheduler.cancel_execution(execution), 2);
        assert!(scheduler.is_empty());
    }
}
