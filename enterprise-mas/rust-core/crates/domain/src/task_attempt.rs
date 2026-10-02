//! Task attempts: one record per execution try of a task.

use mas_common::error::AppError;
use mas_common::ids::{TaskAttemptId, TaskId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::time::Duration;

string_enum! {
    /// Outcome/lifecycle of a single attempt.
    AttemptStatus {
        Running => "running",
        Succeeded => "succeeded",
        Failed => "failed",
        Cancelled => "cancelled",
        TimedOut => "timed_out",
    }
}

impl AttemptStatus {
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }

    /// Whether the attempt outcome permits another try of the task.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Failed | Self::TimedOut)
    }
}

/// One attempt of a task. Terminal states are immutable afterwards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAttempt {
    pub id: TaskAttemptId,
    pub task_id: TaskId,
    /// 1-based attempt number.
    pub attempt_number: u32,
    pub status: AttemptStatus,
    /// Identity of the worker/process running this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Why this attempt was scheduled (e.g. `initial`, `retry:timeout`).
    pub retry_reason: Option<String>,
    pub started_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Timestamp>,
    /// Safe error summary (redacted, truncated) when failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Non-sensitive result metadata (never full payloads).
    #[serde(default)]
    pub result_metadata: serde_json::Map<String, serde_json::Value>,
}

impl TaskAttempt {
    /// Starts a new running attempt.
    pub fn start(
        task_id: TaskId,
        attempt_number: u32,
        worker_id: Option<String>,
        retry_reason: Option<String>,
    ) -> Result<Self> {
        if attempt_number == 0 {
            return Err(AppError::invalid_field(
                "attempt_number",
                "out_of_range",
                "attempts are 1-based",
            ));
        }
        Ok(Self {
            id: TaskAttemptId::new(),
            task_id,
            attempt_number,
            status: AttemptStatus::Running,
            worker_id,
            retry_reason,
            started_at: Timestamp::now(),
            finished_at: None,
            error: None,
            result_metadata: serde_json::Map::new(),
        })
    }

    /// Wall time so far (or final), when measurable.
    #[must_use]
    pub fn duration(&self) -> Option<Duration> {
        let end = self.finished_at.unwrap_or_else(Timestamp::now);
        end.duration_since(&self.started_at)
    }

    fn finalize(&mut self, status: AttemptStatus, error: Option<String>) -> Result<()> {
        if self.status.is_terminal() {
            return Err(AppError::conflict(format!(
                "attempt {} of task {} already finished with status '{}'",
                self.attempt_number, self.task_id, self.status
            )));
        }
        debug_assert!(status.is_terminal());
        let mut error = error;
        if let Some(text) = error.as_mut() {
            text.truncate(2048);
        }
        self.status = status;
        self.error = error;
        self.finished_at = Some(Timestamp::now());
        Ok(())
    }

    /// Running → Succeeded.
    pub fn succeed(&mut self) -> Result<()> {
        self.finalize(AttemptStatus::Succeeded, None)
    }

    /// Running → Failed with a retryable classification.
    pub fn fail(&mut self, error: impl Into<String>) -> Result<()> {
        self.finalize(AttemptStatus::Failed, Some(error.into()))
    }

    /// Running → TimedOut.
    pub fn time_out(&mut self, detail: impl Into<String>) -> Result<()> {
        self.finalize(AttemptStatus::TimedOut, Some(detail.into()))
    }

    /// Running → Cancelled.
    pub fn cancel(&mut self) -> Result<()> {
        self.finalize(AttemptStatus::Cancelled, None)
    }
}
