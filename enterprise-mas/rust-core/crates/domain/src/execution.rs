//! Execution aggregate: one complete workflow/agent run.
//!
//! Child executions (agent delegation) form a tree via
//! `parent_execution_id` / `root_execution_id`; the root is its own root.

use mas_common::constants;
use mas_common::enums::ExecutionStatus;
use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, ExecutionId, OrganizationId, ProjectId, TenantId, WorkflowId, WorkflowVersionId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Accumulated resource consumption of an execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceUsageSummary {
    pub steps_executed: u32,
    pub tool_calls: u32,
    pub tokens_input: u64,
    pub tokens_output: u64,
    /// Wall-clock runtime accumulated while running (ms; excludes pauses).
    pub active_time_ms: u64,
}

impl ResourceUsageSummary {
    #[must_use]
    pub const fn total_tokens(&self) -> u64 {
        self.tokens_input + self.tokens_output
    }
}

/// The execution aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub id: ExecutionId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    /// Definition source: a published workflow version…
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_version_id: Option<WorkflowVersionId>,
    /// …or an agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    /// Delegation tree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<ExecutionId>,
    pub root_execution_id: ExecutionId,
    /// End-to-end tracing key supplied at intake.
    pub correlation_id: String,
    pub status: ExecutionStatus,
    /// Coordinated stop flag: the runtime checks this and transitions to
    /// `Cancelled` at the next safe point.
    cancellation_requested: bool,
    #[serde(default)]
    pub input: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    #[serde(default)]
    pub resource_usage: ResourceUsageSummary,
    pub created_by: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Execution {
    /// Creates a root execution in `Pending`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        workflow_id: Option<WorkflowId>,
        agent_id: Option<AgentId>,
        input: serde_json::Value,
        correlation_id: impl Into<String>,
        created_by: impl Into<String>,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        let created_by = created_by.into();
        validation::validate_non_empty("correlation_id", &correlation_id)?;
        validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        validation::validate_non_empty("created_by", &created_by)?;
        if workflow_id.is_none() && agent_id.is_none() {
            return Err(AppError::invalid_field(
                "execution",
                "missing_target",
                "execution requires a workflow or agent target",
            ));
        }
        let now = Timestamp::now();
        let id = ExecutionId::new();
        Ok(Self {
            id,
            tenant_id,
            organization_id,
            project_id,
            workflow_id,
            workflow_version_id: None,
            agent_id,
            parent_execution_id: None,
            root_execution_id: id, // root is its own root
            correlation_id,
            status: ExecutionStatus::Pending,
            cancellation_requested: false,
            input,
            output: None,
            failure: None,
            started_at: None,
            finished_at: None,
            deadline: None,
            resource_usage: ResourceUsageSummary::default(),
            created_by,
            created_at: now,
            updated_at: now,
        })
    }

    /// Derives a child execution (agent delegation). Links into the same
    /// correlation tree and enforces depth/size limits indirectly via IDs.
    pub fn spawn_child(&self, agent_id: AgentId, input: serde_json::Value) -> Result<Self> {
        if !self.status.is_terminal() {
            // allowed: running/paused parents may delegate
        } else {
            return Err(AppError::conflict(format!(
                "execution {} in status '{}' cannot delegate",
                self.id, self.status
            )));
        }
        let now = Timestamp::now();
        Ok(Self {
            id: ExecutionId::new(),
            tenant_id: self.tenant_id,
            organization_id: self.organization_id,
            project_id: self.project_id,
            workflow_id: None,
            workflow_version_id: None,
            agent_id: Some(agent_id),
            parent_execution_id: Some(self.id),
            root_execution_id: self.root_execution_id,
            correlation_id: self.correlation_id.clone(),
            status: ExecutionStatus::Pending,
            cancellation_requested: false,
            input,
            output: None,
            failure: None,
            started_at: None,
            finished_at: None,
            deadline: self.deadline, // children never outlive the parent's deadline
            resource_usage: ResourceUsageSummary::default(),
            created_by: format!("execution:{}", self.id),
            created_at: now,
            updated_at: now,
        })
    }

    /// `true` when this execution started the tree (no parent).
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.id == self.root_execution_id
    }

    #[must_use]
    pub const fn cancellation_requested(&self) -> bool {
        self.cancellation_requested
    }

    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|deadline| !deadline.is_future())
    }

    pub fn set_deadline(&mut self, deadline: Timestamp) -> Result<()> {
        if !deadline.is_future() {
            return Err(AppError::invalid_field(
                "deadline",
                "out_of_range",
                "deadline must be in the future",
            ));
        }
        self.deadline = Some(deadline);
        self.touch();
        Ok(())
    }

    /// Runtime accounting hook (adds to the running totals, enforcing hard caps).
    pub fn record_usage(&mut self, usage: ResourceUsageSummary) -> Result<()> {
        let next_steps = self
            .resource_usage
            .steps_executed
            .saturating_add(usage.steps_executed);
        let next_tools = self
            .resource_usage
            .tool_calls
            .saturating_add(usage.tool_calls);
        if next_steps > constants::MAX_STEPS_PER_EXECUTION {
            return Err(AppError::rate_limited(
                "execution exceeded maximum step count",
            ));
        }
        if next_tools > constants::MAX_TOOL_CALLS_PER_EXECUTION {
            return Err(AppError::rate_limited(
                "execution exceeded maximum tool calls",
            ));
        }
        self.resource_usage.steps_executed = next_steps;
        self.resource_usage.tool_calls = next_tools;
        self.resource_usage.tokens_input = self
            .resource_usage
            .tokens_input
            .saturating_add(usage.tokens_input);
        self.resource_usage.tokens_output = self
            .resource_usage
            .tokens_output
            .saturating_add(usage.tokens_output);
        self.resource_usage.active_time_ms = self
            .resource_usage
            .active_time_ms
            .saturating_add(usage.active_time_ms);
        self.touch();
        Ok(())
    }

    // -- lifecycle -----------------------------------------------------------

    /// Pending → Running.
    pub fn start(&mut self) -> Result<()> {
        match self.status {
            ExecutionStatus::Pending => {
                if self.is_expired() {
                    return Err(AppError::timeout("execution deadline passed before start"));
                }
                self.status = ExecutionStatus::Running;
                self.started_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("start", other)),
        }
    }

    /// Running → Paused.
    pub fn pause(&mut self) -> Result<()> {
        match self.status {
            ExecutionStatus::Running => {
                self.status = ExecutionStatus::Paused;
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("pause", other)),
        }
    }

    /// Paused → Running.
    pub fn resume(&mut self) -> Result<()> {
        match self.status {
            ExecutionStatus::Paused => {
                if self.is_expired() {
                    return Err(AppError::timeout("execution deadline passed while paused"));
                }
                self.status = ExecutionStatus::Running;
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("resume", other)),
        }
    }

    /// Sets the cooperative cancellation flag (any non-terminal status).
    pub fn request_cancellation(&mut self) -> Result<()> {
        if self.status.is_terminal() {
            return Err(self.illegal("cancel", self.status));
        }
        self.cancellation_requested = true;
        self.touch();
        Ok(())
    }

    /// Immediate transition to `Cancelled` (runtime honored the flag or an
    /// operator forced it). Terminal.
    pub fn cancel(&mut self) -> Result<()> {
        if self.status.is_terminal() {
            if self.status == ExecutionStatus::Cancelled {
                return Ok(()); // idempotent
            }
            return Err(self.illegal("cancel", self.status));
        }
        self.status = ExecutionStatus::Cancelled;
        self.cancellation_requested = true;
        self.finished_at = Some(Timestamp::now());
        self.touch();
        Ok(())
    }

    /// Running|Paused → Completed (terminal). Output must be a JSON value
    /// already validated by the execution layer.
    pub fn complete(&mut self, output: Option<serde_json::Value>) -> Result<()> {
        match self.status {
            ExecutionStatus::Running | ExecutionStatus::Paused => {
                if self.cancellation_requested {
                    return Err(AppError::conflict(
                        "cancellation was requested; execution must end as cancelled",
                    ));
                }
                self.status = ExecutionStatus::Completed;
                self.output = output;
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("complete", other)),
        }
    }

    /// Running|Paused → Failed (terminal).
    pub fn fail(&mut self, error: impl Into<String>) -> Result<()> {
        match self.status {
            ExecutionStatus::Running | ExecutionStatus::Paused => {
                let mut error = error.into();
                error.truncate(4096);
                self.status = ExecutionStatus::Failed;
                self.failure = Some(error);
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("fail", other)),
        }
    }

    /// Wall time since start (None if never started).
    #[must_use]
    pub fn elapsed(&self) -> Option<Duration> {
        let started = self.started_at?;
        let end = self.finished_at.unwrap_or_else(Timestamp::now);
        end.duration_since(&started)
    }

    fn illegal(&self, action: &str, status: ExecutionStatus) -> AppError {
        AppError::conflict(format!(
            "cannot {action} execution {} in status '{status}'",
            self.id
        ))
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
