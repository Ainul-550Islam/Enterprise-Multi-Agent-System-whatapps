//! Platform assistant request and deterministic insight DTOs.

use mas_common::ids::ExecutionId;
use mas_common::result::Result;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::analytics::MetricReferenceDto;

/// A user message passed to the canonical durable platform assistant agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantMessageRequest {
    /// Natural-language question or instruction.
    pub message: String,
}

impl AssistantMessageRequest {
    /// Enforces a bounded, non-empty message.
    pub fn validate(&self) -> Result<()> {
        validation::validate_length("message", &self.message, 1, 16_000)?;
        if self.message.trim().is_empty() {
            return Err(mas_common::error::AppError::invalid_field(
                "message",
                "required",
                "assistant message must not be blank",
            ));
        }
        Ok(())
    }
}

/// Durable execution handle returned after assistant submission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantExecutionDto {
    /// Execution id followed by the existing stream endpoint.
    pub execution_id: ExecutionId,
}

/// Deterministic insight row for the dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantInsightDto {
    /// Stable rule code.
    pub code: String,
    /// Short human-readable title.
    pub title: String,
    /// Supporting explanation.
    pub detail: String,
    /// Evidence used to construct the insight.
    pub metric_refs: Vec<MetricReferenceDto>,
}
