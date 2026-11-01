//! Execution steps: individual runtime actions inside an execution.
//!
//! Large inputs/outputs are *referenced* (object-storage keys), never inlined
//! in the row/document.

use mas_common::enums::ExecutionStepStatus;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, ExecutionId, ExecutionStepId, ToolId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Safe, machine-readable error metadata for a failed step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepError {
    /// Stable error code (`AppError::error_code` of the underlying error).
    pub code: String,
    /// Redacted, truncated message.
    pub message: String,
    pub retryable: bool,
}

impl StepError {
    #[must_use]
    pub fn from_app_error(err: &AppError) -> Self {
        let mut message = err.public_message();
        message.truncate(1024);
        Self {
            code: err.error_code().to_owned(),
            message,
            retryable: err.is_retryable(),
        }
    }
}

/// One runtime action of an execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStep {
    pub id: ExecutionStepId,
    pub execution_id: ExecutionId,
    /// Node key of the workflow node this step realizes (if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_key: Option<String>,
    /// Human/system readable step type (`tool.invoke`, `agent.run`, …).
    pub step_type: String,
    /// Monotonic sequence number within the execution (0-based).
    pub sequence: u64,
    /// 1-based attempt number for this step.
    pub attempt: u32,
    pub status: ExecutionStepStatus,
    /// References to stored payloads (object storage keys / step refs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<StepError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_id: Option<ToolId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl ExecutionStep {
    /// Registers a new step in `Pending`.
    pub fn new(
        execution_id: ExecutionId,
        step_type: impl Into<String>,
        sequence: u64,
        node_key: Option<String>,
    ) -> Self {
        let now = Timestamp::now();
        Self {
            id: ExecutionStepId::new(),
            execution_id,
            node_key,
            step_type: step_type.into(),
            sequence,
            attempt: 0,
            status: ExecutionStepStatus::Pending,
            input_ref: None,
            output_ref: None,
            error: None,
            tool_id: None,
            agent_id: None,
            started_at: None,
            finished_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[must_use]
    pub fn with_tool(mut self, tool_id: ToolId) -> Self {
        self.tool_id = Some(tool_id);
        self
    }

    #[must_use]
    pub fn with_agent(mut self, agent_id: AgentId) -> Self {
        self.agent_id = Some(agent_id);
        self
    }

    #[must_use]
    pub fn with_input_ref(mut self, input_ref: impl Into<String>) -> Self {
        self.input_ref = Some(input_ref.into());
        self
    }

    /// Pending → Running. Each `begin` increments the attempt counter,
    /// so re-running a failed step is modeled as `begin` again.
    pub fn begin(&mut self) -> Result<()> {
        match self.status {
            ExecutionStepStatus::Pending | ExecutionStepStatus::Failed => {
                self.status = ExecutionStepStatus::Running;
                self.attempt += 1;
                self.error = None;
                self.started_at = Some(Timestamp::now());
                self.finished_at = None;
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("begin", other)),
        }
    }

    /// Running → Completed.
    pub fn succeed(&mut self, output_ref: Option<String>) -> Result<()> {
        match self.status {
            ExecutionStepStatus::Running => {
                self.status = ExecutionStepStatus::Completed;
                self.output_ref = output_ref;
                self.error = None;
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("complete", other)),
        }
    }

    /// Running → Failed. Whether another `begin` is allowed is the retry
    /// policy's decision (`max step attempts` is enforced by the runtime).
    pub fn fail(&mut self, error: StepError) -> Result<()> {
        match self.status {
            ExecutionStepStatus::Running => {
                self.status = ExecutionStepStatus::Failed;
                self.error = Some(error);
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("fail", other)),
        }
    }

    /// Pending → Skipped (branch not taken / condition false).
    pub fn skip(&mut self, reason: Option<impl Into<String>>) -> Result<()> {
        match self.status {
            ExecutionStepStatus::Pending => {
                self.status = ExecutionStepStatus::Skipped;
                if let Some(reason) = reason {
                    self.error = Some(StepError {
                        code: "SKIPPED".to_owned(),
                        message: {
                            let mut msg: String = reason.into();
                            msg.truncate(1024);
                            msg
                        },
                        retryable: false,
                    });
                }
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("skip", other)),
        }
    }

    /// Pending → WaitingApproval / WaitingApproval → Running.
    pub fn wait_for_approval(&mut self) -> Result<()> {
        match self.status {
            ExecutionStepStatus::Pending => {
                self.status = ExecutionStepStatus::WaitingApproval;
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("wait for approval", other)),
        }
    }

    /// Pending|Running|WaitingApproval → Cancelled.
    pub fn cancel(&mut self) -> Result<()> {
        match self.status {
            ExecutionStepStatus::Pending
            | ExecutionStepStatus::Running
            | ExecutionStepStatus::WaitingApproval => {
                self.status = ExecutionStepStatus::Cancelled;
                self.finished_at = Some(Timestamp::now());
                self.touch();
                Ok(())
            },
            other => Err(self.illegal("cancel", other)),
        }
    }

    #[must_use]
    pub fn duration(&self) -> Option<Duration> {
        let started = self.started_at?;
        let end = self.finished_at.unwrap_or_else(Timestamp::now);
        end.duration_since(&started)
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    fn illegal(&self, action: &str, status: ExecutionStepStatus) -> AppError {
        AppError::conflict(format!(
            "cannot {action} step {} (seq {}) in status '{status}'",
            self.id, self.sequence
        ))
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
