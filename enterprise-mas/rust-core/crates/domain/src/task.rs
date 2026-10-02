//! Task aggregate: a unit of dispatched work.

use mas_common::enums::{TaskPriority, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, ExecutionId, OrganizationId, ProjectId, TaskId, TenantId, WorkflowId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// The task aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    /// Exactly one of `agent_id` / `workflow_id` addresses the target runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    /// Owning execution, when the task was spawned by one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    /// The requested operation (e.g. `agent.run`, `tool.invoke`, `workflow.step`).
    pub operation: String,
    #[serde(default)]
    pub input: serde_json::Value,
    pub priority: TaskPriority,
    pub status: TaskStatus,
    /// Client-supplied duplicate-submission guard (unique per tenant).
    pub idempotency_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    pub max_attempts: u32,
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    /// Safe (redacted, truncated) description of the last failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_by: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Task {
    /// Creates a task in `Pending` (not yet queued).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        agent_id: Option<AgentId>,
        workflow_id: Option<WorkflowId>,
        operation: impl Into<String>,
        input: serde_json::Value,
        priority: TaskPriority,
        idempotency_key: impl Into<String>,
        created_by: impl Into<String>,
    ) -> Result<Self> {
        let operation = operation.into();
        let idempotency_key = idempotency_key.into();
        let created_by = created_by.into();
        validation::validate_non_empty("operation", &operation)?;
        validation::validate_length("operation", &operation, 1, 128)?;
        validation::validate_non_empty("idempotency_key", &idempotency_key)?;
        validation::validate_length(
            "idempotency_key",
            &idempotency_key,
            1,
            mas_common::constants::MAX_IDEMPOTENCY_KEY_LENGTH,
        )?;
        validation::validate_non_empty("created_by", &created_by)?;
        if agent_id.is_none() && workflow_id.is_none() {
            return Err(AppError::invalid_field(
                "task",
                "missing_target",
                "task must target an agent or a workflow",
            ));
        }
        if agent_id.is_some() && workflow_id.is_some() {
            return Err(AppError::invalid_field(
                "task",
                "ambiguous_target",
                "task targets either an agent or a workflow, never both",
            ));
        }
        let now = Timestamp::now();
        Ok(Self {
            id: TaskId::new(),
            tenant_id,
            organization_id,
            project_id,
            agent_id,
            workflow_id,
            execution_id: None,
            operation,
            input,
            priority,
            status: TaskStatus::Pending,
            idempotency_key,
            deadline: None,
            max_attempts: mas_common::constants::DEFAULT_MAX_RETRIES + 1,
            attempt_count: 0,
            queued_at: None,
            started_at: None,
            finished_at: None,
            last_error: None,
            created_by,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether the task blew its deadline.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|deadline| !deadline.is_future())
    }

    /// Remaining attempts (excluding the current one).
    #[must_use]
    pub const fn remaining_attempts(&self) -> u32 {
        self.max_attempts.saturating_sub(self.attempt_count)
    }

    // -- state transitions ---------------------------------------------------

    /// Pending → Queued.
    pub fn queue(&mut self) -> Result<()> {
        match self.status {
            TaskStatus::Pending => {
                if self.is_expired() {
                    return Err(AppError::validation("task deadline has already passed"));
                }
                self.status = TaskStatus::Queued;
                self.queued_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("queue", other)),
        }
    }

    /// Queued → Running (increments attempt count).
    pub fn start(&mut self) -> Result<()> {
        match self.status {
            TaskStatus::Queued => {
                if self.is_expired() {
                    return Err(AppError::timeout("task deadline has passed before start"));
                }
                if self.attempt_count >= self.max_attempts {
                    return Err(AppError::conflict("task has exhausted its attempts"));
                }
                self.status = TaskStatus::Running;
                self.attempt_count += 1;
                self.started_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("start", other)),
        }
    }

    /// Running|Queued|Pending → Cancelled (terminal).
    pub fn cancel(&mut self) -> Result<()> {
        match self.status {
            TaskStatus::Pending | TaskStatus::Queued | TaskStatus::Running => {
                self.status = TaskStatus::Cancelled;
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            TaskStatus::Cancelled => Ok(()),
            other => Err(self.illegal("cancel", other)),
        }
    }

    /// Running → Completed (terminal).
    pub fn complete(&mut self) -> Result<()> {
        match self.status {
            TaskStatus::Running => {
                self.status = TaskStatus::Completed;
                self.finished_at = Some(Timestamp::now());
                self.last_error = None;
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("complete", other)),
        }
    }

    /// Running → Failed. Use [`Task::retry`] or [`Task::dead_letter`] next.
    pub fn fail(&mut self, error: impl Into<String>) -> Result<()> {
        match self.status {
            TaskStatus::Running => {
                let mut error = error.into();
                error.truncate(2048);
                self.status = TaskStatus::Failed;
                self.last_error = Some(error);
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("fail", other)),
        }
    }

    /// Failed → Queued, when attempts remain.
    pub fn retry(&mut self) -> Result<()> {
        match self.status {
            TaskStatus::Failed => {
                if self.remaining_attempts() == 0 {
                    return Err(AppError::conflict(
                        "task has no remaining attempts; dead-letter it instead",
                    ));
                }
                self.status = TaskStatus::Queued;
                self.queued_at = Some(Timestamp::now());
                self.started_at = None;
                self.finished_at = None;
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("retry", other)),
        }
    }

    /// Failed → DeadLettered (terminal; replay happens via new tasks).
    pub fn dead_letter(&mut self, reason: impl Into<String>) -> Result<()> {
        match self.status {
            TaskStatus::Failed => {
                let mut reason = reason.into();
                reason.truncate(2048);
                self.status = TaskStatus::DeadLettered;
                self.last_error = Some(format!("dead-lettered: {reason}"));
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("dead-letter", other)),
        }
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    fn illegal(&self, action: &str, status: TaskStatus) -> AppError {
        AppError::conflict(format!(
            "cannot {action} task {} in status '{status}'",
            self.id
        ))
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
