//! Execution DTOs.

use mas_common::enums::ExecutionStatus;
use mas_common::ids::{AgentId, ExecutionId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Request to start an execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartExecutionRequest {
    /// Target: published workflow…
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    /// …or active agent (exactly one required).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default)]
    pub input: serde_json::Value,
    /// Correlation key (generated when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

impl StartExecutionRequest {
    pub fn validate(&self) -> Result<()> {
        if self.workflow_id.is_none() == self.agent_id.is_none() {
            return Err(mas_common::error::AppError::invalid_field(
                "target",
                "invalid_target",
                "provide exactly one of workflow_id / agent_id",
            ));
        }
        if let Some(correlation_id) = &self.correlation_id {
            validation::validate_length("correlation_id", correlation_id, 1, 256)?;
        }
        if let Some(idempotency_key) = &self.idempotency_key {
            validation::validate_length(
                "idempotency_key",
                idempotency_key,
                1,
                mas_common::constants::MAX_IDEMPOTENCY_KEY_LENGTH,
            )?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelExecutionRequest {
    pub execution_id: ExecutionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PauseExecutionRequest {
    pub execution_id: ExecutionId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeExecutionRequest {
    pub execution_id: ExecutionId,
}

impl CancelExecutionRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(reason) = &self.reason {
            validation::validate_length("reason", reason, 0, 1024)?;
        }
        Ok(())
    }
}

impl PauseExecutionRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

impl ResumeExecutionRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

/// Resource usage projection of an execution.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ExecutionUsageDto {
    pub steps_executed: u32,
    pub tool_calls: u32,
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub active_time_ms: u64,
}

/// Wire representation of an execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResponse {
    pub id: ExecutionId,
    pub status: ExecutionStatus,
    pub tenant_id: mas_common::ids::TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<ExecutionId>,
    pub root_execution_id: ExecutionId,
    pub correlation_id: String,
    pub cancellation_requested: bool,
    #[serde(default)]
    pub usage: ExecutionUsageDto,
    /// Present once completed (large outputs are reference-resolved by the API).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    pub created_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
}

/// One streamed execution event (SSE lines / gRPC stream frames).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEventResponse {
    pub execution_id: ExecutionId,
    /// Monotonic stream position within this execution's event stream.
    pub sequence: u64,
    /// Event type name (`execution.started`, `step.completed`, …).
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<mas_common::ids::ExecutionStepId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_key: Option<String>,
    /// Safe payload (redacted at source).
    #[serde(default)]
    pub payload: serde_json::Value,
    pub occurred_at: Timestamp,
}

impl ExecutionEventResponse {
    pub fn validate(&self) -> Result<()> {
        validation::validate_non_empty("event_type", &self.event_type)?;
        validation::validate_length("event_type", &self.event_type, 1, 128)?;
        Ok(())
    }
}
