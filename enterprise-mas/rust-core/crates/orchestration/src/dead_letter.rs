//! Dead letters for tasks and events.
//!
//! A dead-lettered unit keeps its *entire original payload* plus full failure
//! metadata so it can be replayed as-is (new attempt lineage) or discarded
//! after inspection. Replaying never mutates history: the record is marked
//! and the caller re-queues using the returned payload.

use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::Timestamp;
use mas_domain::Task;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

/// Replay state of a dead-letter record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeadLetterRecordStatus {
    /// Awaiting operator action.
    #[default]
    Pending,
    /// Re-queued for another attempt (history kept).
    Replayed,
    /// Discarded after inspection (history kept).
    Discarded,
}

/// A task that exhausted its attempts (or hit a non-retryable failure).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterTask {
    /// Dead-letter record id (distinct from the task id).
    pub id: uuid::Uuid,
    /// The full original task as it was at dead-letter time.
    pub task: Task,
    pub tenant_id: TenantId,
    /// Root cause category (`AppError::error_code`).
    pub error_code: String,
    /// Safe failure description of the final attempt.
    pub final_error: String,
    /// Attempts made before dead-lettering.
    pub attempts_made: u32,
    /// Why the unit ended here: `exhausted` | `non_retryable` | `cancelled` etc.
    pub reason: String,
    #[serde(default)]
    pub status: DeadLetterRecordStatus,
    /// When replayed/discarded, by whom (audit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<Timestamp>,
    pub dead_lettered_at: Timestamp,
}

/// An event that could not be delivered/processed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEvent {
    pub id: uuid::Uuid,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    /// Full original payload (already redacted at production time).
    pub payload: serde_json::Value,
    pub error_code: String,
    pub final_error: String,
    pub failure_count: u32,
    #[serde(default)]
    pub status: DeadLetterRecordStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<Timestamp>,
    pub dead_lettered_at: Timestamp,
}

/// Decision returned by `replay*`/`inspect*` for the caller to act upon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DeadLetterAction {
    /// Re-queue this task payload (new attempt lineage).
    RequeueTask { task: Box<Task> },
    /// Re-publish this event payload.
    RepublishEvent {
        event_type: String,
        payload: serde_json::Value,
    },
}

impl DeadLetterTask {
    /// Action required to replay this record.
    #[must_use]
    pub fn replay_action(&self) -> DeadLetterAction {
        DeadLetterAction::RequeueTask {
            task: Box::new(self.task.clone()),
        }
    }
}

impl DeadLetterEvent {
    /// Action required to replay this record.
    #[must_use]
    pub fn replay_action(&self) -> DeadLetterAction {
        DeadLetterAction::RepublishEvent {
            event_type: self.event_type.clone(),
            payload: self.payload.clone(),
        }
    }
}

/// Durable dead-letter storage (production impl: PostgreSQL).
#[async_trait::async_trait]
pub trait DeadLetterStore: Send + Sync + fmt::Debug {
    /// Enqueues a task dead letter; returns the stored record id.
    async fn enqueue_dead_letter(
        &self,
        task: Task,
        error_code: &str,
        final_error: &str,
        reason: &str,
    ) -> Result<DeadLetterTask>;

    /// Enqueues an event dead letter; returns the stored record id.
    async fn enqueue_event(
        &self,
        event_type: &str,
        tenant_id: Option<TenantId>,
        payload: serde_json::Value,
        error_code: &str,
        final_error: &str,
        failure_count: u32,
    ) -> Result<DeadLetterEvent>;

    /// Marks a task record replayed and returns the replay action.
    async fn replay_task(&self, record_id: uuid::Uuid, actor: &str) -> Result<DeadLetterAction>;

    /// Marks an event record replayed and returns the replay action.
    async fn replay_event(&self, record_id: uuid::Uuid, actor: &str) -> Result<DeadLetterAction>;

    /// Discards a record after operator inspection.
    async fn discard(&self, record_id: uuid::Uuid, actor: &str) -> Result<()>;

    /// Reads a task record without changing it.
    async fn inspect_task(&self, record_id: uuid::Uuid) -> Result<DeadLetterTask>;

    /// Reads an event record without changing it.
    async fn inspect_event(&self, record_id: uuid::Uuid) -> Result<DeadLetterEvent>;

    /// Lists task records (optionally filtered by status).
    async fn list_task_records(
        &self,
        status: Option<DeadLetterRecordStatus>,
    ) -> Result<Vec<DeadLetterTask>>;
}

/// In-memory store for development/tests.
#[derive(Debug, Default)]
pub struct InMemoryDeadLetterStore {
    tasks: Mutex<HashMap<uuid::Uuid, DeadLetterTask>>,
    events: Mutex<HashMap<uuid::Uuid, DeadLetterEvent>>,
}

impl InMemoryDeadLetterStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

fn resolve_record(
    status: &mut DeadLetterRecordStatus,
    resolved_by: &mut Option<String>,
    resolved_at: &mut Option<Timestamp>,
    actor: &str,
    to: DeadLetterRecordStatus,
) -> Result<()> {
    if *status != DeadLetterRecordStatus::Pending {
        return Err(AppError::conflict(format!(
            "dead-letter record is already '{status:?}'"
        )));
    }
    *status = to;
    *resolved_by = Some(actor.to_owned());
    *resolved_at = Some(Timestamp::now());
    Ok(())
}

fn make_task_record(
    task: Task,
    error_code: &str,
    final_error: &str,
    reason: &str,
) -> DeadLetterTask {
    DeadLetterTask {
        id: uuid::Uuid::now_v7(),
        tenant_id: task.tenant_id,
        task,
        error_code: error_code.to_owned(),
        final_error: final_error.to_owned(),
        attempts_made: 0,
        reason: reason.to_owned(),
        status: DeadLetterRecordStatus::Pending,
        resolved_by: None,
        resolved_at: None,
        dead_lettered_at: Timestamp::now(),
    }
}

#[async_trait::async_trait]
impl DeadLetterStore for InMemoryDeadLetterStore {
    async fn enqueue_dead_letter(
        &self,
        task: Task,
        error_code: &str,
        final_error: &str,
        reason: &str,
    ) -> Result<DeadLetterTask> {
        let mut record = make_task_record(task, error_code, final_error, reason);
        record.attempts_made = record.task.attempt_count;
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(record.id, record.clone());
        Ok(record)
    }

    async fn enqueue_event(
        &self,
        event_type: &str,
        tenant_id: Option<TenantId>,
        payload: serde_json::Value,
        error_code: &str,
        final_error: &str,
        failure_count: u32,
    ) -> Result<DeadLetterEvent> {
        mas_common::validation::validate_non_empty("event_type", event_type)?;
        let record = DeadLetterEvent {
            id: uuid::Uuid::now_v7(),
            event_type: event_type.to_owned(),
            tenant_id,
            payload,
            error_code: error_code.to_owned(),
            final_error: final_error.to_owned(),
            failure_count,
            status: DeadLetterRecordStatus::Pending,
            resolved_by: None,
            resolved_at: None,
            dead_lettered_at: Timestamp::now(),
        };
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(record.id, record.clone());
        Ok(record)
    }

    async fn replay_task(&self, record_id: uuid::Uuid, actor: &str) -> Result<DeadLetterAction> {
        let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        mas_common::validation::validate_non_empty("actor", actor)?;
        let record = tasks
            .get_mut(&record_id)
            .ok_or_else(|| AppError::not_found("dead_letter_task", record_id.to_string()))?;
        let action = record.replay_action();
        resolve_record(
            &mut record.status,
            &mut record.resolved_by,
            &mut record.resolved_at,
            actor,
            DeadLetterRecordStatus::Replayed,
        )?;
        Ok(action)
    }

    async fn replay_event(&self, record_id: uuid::Uuid, actor: &str) -> Result<DeadLetterAction> {
        mas_common::validation::validate_non_empty("actor", actor)?;
        let mut events = self.events.lock().unwrap_or_else(|e| e.into_inner());
        let record = events
            .get_mut(&record_id)
            .ok_or_else(|| AppError::not_found("dead_letter_event", record_id.to_string()))?;
        let action = record.replay_action();
        resolve_record(
            &mut record.status,
            &mut record.resolved_by,
            &mut record.resolved_at,
            actor,
            DeadLetterRecordStatus::Replayed,
        )?;
        Ok(action)
    }

    async fn discard(&self, record_id: uuid::Uuid, actor: &str) -> Result<()> {
        mas_common::validation::validate_non_empty("actor", actor)?;
        {
            let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(record) = tasks.get_mut(&record_id) {
                return resolve_record(
                    &mut record.status,
                    &mut record.resolved_by,
                    &mut record.resolved_at,
                    actor,
                    DeadLetterRecordStatus::Discarded,
                );
            }
        }
        let mut events = self.events.lock().unwrap_or_else(|e| e.into_inner());
        let record = events
            .get_mut(&record_id)
            .ok_or_else(|| AppError::not_found("dead_letter_record", record_id.to_string()))?;
        resolve_record(
            &mut record.status,
            &mut record.resolved_by,
            &mut record.resolved_at,
            actor,
            DeadLetterRecordStatus::Discarded,
        )
    }

    async fn inspect_task(&self, record_id: uuid::Uuid) -> Result<DeadLetterTask> {
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&record_id)
            .cloned()
            .ok_or_else(|| AppError::not_found("dead_letter_task", record_id.to_string()))
    }

    async fn inspect_event(&self, record_id: uuid::Uuid) -> Result<DeadLetterEvent> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&record_id)
            .cloned()
            .ok_or_else(|| AppError::not_found("dead_letter_event", record_id.to_string()))
    }

    async fn list_task_records(
        &self,
        status: Option<DeadLetterRecordStatus>,
    ) -> Result<Vec<DeadLetterTask>> {
        let tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        let mut records: Vec<DeadLetterTask> = tasks
            .values()
            .filter(|record| status.is_none_or(|wanted| record.status == wanted))
            .cloned()
            .collect();
        records.sort_by_key(|record| record.dead_lettered_at);
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::TaskPriority;
    use mas_common::ids::{AgentId, OrganizationId, ProjectId, TaskId};

    fn sample_task() -> Task {
        Task::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            Some(AgentId::new()),
            None,
            "agent.run",
            serde_json::json!({"hello": "world"}),
            TaskPriority::Normal,
            "idem-1",
            "user:u1",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn task_lifecycle_enqueue_replay() {
        let store = InMemoryDeadLetterStore::new();
        let record = store
            .enqueue_dead_letter(sample_task(), "TIMEOUT", "timed out twice", "exhausted")
            .await
            .unwrap();
        assert_eq!(record.attempts_made, 0);
        assert_eq!(record.status, DeadLetterRecordStatus::Pending);

        let action = store.replay_task(record.id, "ops:alice").await.unwrap();
        match action {
            DeadLetterAction::RequeueTask { task } => {
                assert_eq!(task.operation, "agent.run");
            },
            other => panic!("expected task replay, got {other:?}"),
        }
        let inspected = store.inspect_task(record.id).await.unwrap();
        assert_eq!(inspected.status, DeadLetterRecordStatus::Replayed);
        assert_eq!(inspected.resolved_by.as_deref(), Some("ops:alice"));
        // Second replay conflicts (history is append-only).
        assert!(store.replay_task(record.id, "ops:alice").await.is_err());
    }

    #[tokio::test]
    async fn event_enqueue_inspect_discard() {
        let store = InMemoryDeadLetterStore::new();
        let record = store
            .enqueue_event(
                "execution.completed",
                Some(TenantId::new()),
                serde_json::json!({"id": "e1"}),
                "MESSAGING_ERROR",
                "broker NAK",
                4,
            )
            .await
            .unwrap();
        let action = store.replay_event(record.id, "ops:bob").await.unwrap();
        match action {
            DeadLetterAction::RepublishEvent {
                event_type,
                payload,
            } => {
                assert_eq!(event_type, "execution.completed");
                assert_eq!(payload["id"], "e1");
            },
            other => panic!("expected event replay, got {other:?}"),
        }
        assert!(store.discard(record.id, "ops:bob").await.is_err());
    }

    #[tokio::test]
    async fn list_and_discard_flow() {
        let store = InMemoryDeadLetterStore::new();
        let a = store
            .enqueue_dead_letter(sample_task(), "INTERNAL_ERROR", "boom", "non_retryable")
            .await
            .unwrap();
        let b = store
            .enqueue_dead_letter(sample_task(), "TIMEOUT", "t/o", "exhausted")
            .await
            .unwrap();
        assert_eq!(
            store
                .list_task_records(Some(DeadLetterRecordStatus::Pending))
                .await
                .unwrap()
                .len(),
            2
        );
        store.discard(a.id, "ops:carol").await.unwrap();
        let pending = store
            .list_task_records(Some(DeadLetterRecordStatus::Pending))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, b.id);
        // Quieting the unused-import lint for TaskId in fixtures.
        let _ = TaskId::new();
    }
}
