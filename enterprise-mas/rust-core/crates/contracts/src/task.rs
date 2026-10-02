//! Task DTOs and query filters.

use mas_common::enums::{TaskPriority, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, ExecutionId, OrganizationId, ProjectId, TaskId, TenantId, WorkflowId,
};
use mas_common::pagination::PageRequest;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Request to submit a task. Tenant/project scope comes from the request
/// context, never from the payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitTaskRequest {
    /// Target: an agent…
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// …or a workflow (exactly one required).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    /// Operation key, e.g. `agent.run`, `workflow.run`, `tool.invoke`.
    pub operation: String,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default)]
    pub priority: TaskPriority,
    /// Duplicate-submission guard supplied by the caller.
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
}

impl SubmitTaskRequest {
    pub fn validate(&self) -> Result<()> {
        if self.agent_id.is_none() == self.workflow_id.is_none() {
            return Err(mas_common::error::AppError::invalid_field(
                "target",
                "invalid_target",
                "provide exactly one of agent_id / workflow_id",
            ));
        }
        validation::validate_non_empty("operation", &self.operation)?;
        validation::validate_length("operation", &self.operation, 1, 128)?;
        validation::validate_non_empty("idempotency_key", &self.idempotency_key)?;
        validation::validate_length(
            "idempotency_key",
            &self.idempotency_key,
            1,
            mas_common::constants::MAX_IDEMPOTENCY_KEY_LENGTH,
        )?;
        let payload_size = serde_json::to_vec(&self.input)
            .map_err(|_| mas_common::error::AppError::validation("input is not serializable"))?
            .len();
        if payload_size > mas_common::constants::MAX_PAYLOAD_BYTES {
            return Err(mas_common::error::AppError::invalid_field(
                "input",
                "too_large",
                "task input exceeds the payload limit",
            ));
        }
        if let Some(deadline) = &self.deadline {
            if !deadline.is_future() {
                return Err(mas_common::error::AppError::invalid_field(
                    "deadline",
                    "out_of_range",
                    "deadline must be in the future",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelTaskRequest {
    pub task_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CancelTaskRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(reason) = &self.reason {
            validation::validate_length("reason", reason, 0, 1024)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryTaskRequest {
    pub task_id: TaskId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl RetryTaskRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(reason) = &self.reason {
            validation::validate_length("reason", reason, 0, 1024)?;
        }
        Ok(())
    }
}

/// Filters for listing tasks. All filters are conjunctive.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<TaskStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<TaskPriority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_after: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_before: Option<Timestamp>,
    #[serde(default)]
    pub page: PageRequest,
}

impl TaskQuery {
    pub fn validate(&self) -> Result<()> {
        self.page.validate()?;
        if let (Some(after), Some(before)) = (self.created_after, self.created_before) {
            if !after.is_before(&before) {
                return Err(mas_common::error::AppError::invalid_field(
                    "created_after",
                    "invalid_range",
                    "created_after must be before created_before",
                ));
            }
        }
        Ok(())
    }
}

/// Wire representation of a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResponse {
    pub id: TaskId,
    pub status: TaskStatus,
    pub priority: TaskPriority,
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    pub attempt_count: u32,
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
}

// ---------------------------------------------------------------------------
// Queue wire contract (worker plane)
// ---------------------------------------------------------------------------

/// The canonical queue message produced by the scheduler-service /
/// submission plane and consumed by workers. This is the ONLY task shape
/// allowed on the task subject — consumers never reconstruct Domain tasks
/// from headers alone, and idempotency keys always ride in the payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskQueueMessage {
    pub task_id: TaskId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    /// Dispositional operation, e.g. `execution.run`.
    pub operation: String,
    /// Operation payload (redacted upstream; never secrets).
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    /// Global idempotency key — duplicates collapse to the same `task_id`.
    pub idempotency_key: String,
    pub priority: TaskPriority,
    /// Attempt number this delivery represents (1-based).
    pub attempt_count: u32,
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    pub correlation_id: String,
    pub enqueued_at: Timestamp,
}

impl TaskQueueMessage {
    /// All state needed to construct a queue message (mirrors the domain
    /// `Task` row fields without importing the domain crate into the wire
    /// contract; producers copy from their persisted row).
    #[allow(clippy::too_many_arguments)]
    pub fn staged(
        task_id: TaskId,
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        operation: String,
        input: serde_json::Value,
        execution_id: Option<ExecutionId>,
        agent_id: Option<AgentId>,
        workflow_id: Option<WorkflowId>,
        idempotency_key: String,
        priority: TaskPriority,
        attempt_count: u32,
        max_attempts: u32,
        deadline: Option<Timestamp>,
        correlation_id: &str,
    ) -> Result<Self> {
        validation::validate_non_empty("correlation_id", correlation_id)?;
        validation::validate_length("correlation_id", correlation_id, 1, 256)?;
        let staged = Self {
            task_id,
            tenant_id,
            organization_id,
            project_id,
            operation,
            input,
            execution_id,
            agent_id,
            workflow_id,
            idempotency_key,
            priority,
            attempt_count,
            max_attempts,
            deadline,
            correlation_id: correlation_id.to_owned(),
            enqueued_at: Timestamp::now(),
        };
        staged.validate()?;
        Ok(staged)
    }

    /// Wire-side invariants (checked by producers before framing).
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("operation", &self.operation)?;
        validation::validate_non_empty("idempotency_key", &self.idempotency_key)?;
        validation::validate_length("idempotency_key", &self.idempotency_key, 1, 512)?;
        validation::validate_non_empty("correlation_id", &self.correlation_id)?;
        validation::validate_length("correlation_id", &self.correlation_id, 1, 256)?;
        if self.max_attempts == 0 {
            return Err(AppError::validation("max_attempts must be at least 1"));
        }
        if self.attempt_count == 0 || self.attempt_count > self.max_attempts {
            return Err(AppError::validation(format!(
                "attempt_count {} out of range 1..={}",
                self.attempt_count, self.max_attempts
            )));
        }
        Ok(())
    }
}
