//! Task-row lifecycle port: the consumer keeps the domain `Task` rows in
//! lockstep with broker settlement. Row updates go through the domain's own
//! transition rules, so illegal states are impossible here by construction.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use mas_common::ids::TaskId;
use mas_common::result::Result;
use mas_domain::Task;

/// Mirror of broker settlement onto persisted task rows.
#[async_trait]
pub trait TaskLifecyclePort: std::fmt::Debug + Send + Sync {
    /// Loads the row (None ⇒ orphan message — the consumer acks those).
    async fn load(&self, task_id: TaskId) -> Result<Option<Task>>;
    /// Persist a mutated row (load → transition → save, always as a unit).
    async fn save(&self, task: &Task) -> Result<()>;
}

/// In-memory reference implementation (tests, dev loop).
#[derive(Debug, Default)]
pub struct InMemoryTaskStore {
    rows: Mutex<BTreeMap<TaskId, Task>>,
}

impl InMemoryTaskStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seeds a row (idempotent replace; fixtures only).
    pub fn seed(&self, task: Task) {
        self.rows.lock().expect("lock").insert(task.id, task);
    }

    /// A snapshot of every row.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Task> {
        self.rows.lock().expect("lock").values().cloned().collect()
    }
}

#[async_trait]
impl TaskLifecyclePort for InMemoryTaskStore {
    async fn load(&self, task_id: TaskId) -> Result<Option<Task>> {
        Ok(self.rows.lock().expect("lock").get(&task_id).cloned())
    }

    async fn save(&self, task: &Task) -> Result<()> {
        self.rows
            .lock()
            .expect("lock")
            .insert(task.id, task.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId, WorkflowId};

    fn task() -> Task {
        Task::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            None,
            Some(WorkflowId::new()),
            "execution.run",
            serde_json::json!({}),
            mas_common::enums::TaskPriority::High,
            "idem-seed-1",
            "svc-test",
        )
        .expect("task")
    }

    #[tokio::test]
    async fn store_roundtrips_rows() {
        let store = InMemoryTaskStore::new();
        let seed = task();
        // Seed the queue-path sequence: new() ⇒ Pending; queue() ⇒ Queued.
        let mut seeded = seed.clone();
        seeded.queue().expect("queue");
        store.seed(seeded.clone());
        let loaded = TaskLifecyclePort::load(&store, seeded.id)
            .await
            .expect("load")
            .expect("present");
        assert_eq!(loaded.idempotency_key, "idem-seed-1");
        assert!(loaded.started_at.is_none());
        TaskLifecyclePort::save(&store, &loaded)
            .await
            .expect("save");
        assert!(TaskLifecyclePort::load(&store, TaskId::new())
            .await
            .expect("none")
            .is_none());
        let _ = seed;
    }
}
