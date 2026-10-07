//! Idempotent, forward-versioned rollback contracts.

use mas_common::ids::{RollbackRecordId, WorkflowId};
use mas_common::result::Result;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Body shared by agent, workflow and policy rollback routes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackRequest {
    /// Version whose immutable snapshot should be re-published.
    pub target_version_number: u64,
    /// Human reason recorded in immutable history.
    pub reason: String,
    /// Client-generated key; retries return the original result.
    pub idempotency_key: String,
}

impl RollbackRequest {
    /// Validates the target and idempotency/audit fields.
    pub fn validate(&self) -> Result<()> {
        if self.target_version_number == 0 {
            return Err(mas_common::error::AppError::invalid_field(
                "target_version_number",
                "out_of_range",
                "version numbers start at one",
            ));
        }
        validation::validate_length("reason", &self.reason, 1, 1024)?;
        validation::validate_length("idempotency_key", &self.idempotency_key, 1, 256)?;
        if self.reason.trim().is_empty()
            || self.reason.chars().any(char::is_control)
            || self.idempotency_key.chars().any(char::is_control)
        {
            return Err(mas_common::error::AppError::invalid_field(
                "rollback",
                "invalid_format",
                "reason and idempotency key must be non-empty and contain no control characters",
            ));
        }
        Ok(())
    }
}

/// Minimal successful rollback receipt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackResponseDto {
    /// Append-only rollback history row.
    pub rollback_record_id: RollbackRecordId,
    /// Resource type.
    pub resource_kind: String,
    /// Resource identifier.
    pub resource_id: String,
    /// Previously active version.
    pub from_version: u64,
    /// Newly created monotonic version.
    pub to_version: u64,
    /// Version snapshot from which content was copied.
    pub source_version: u64,
}

/// Bounded query for tenant-scoped rollback history.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RollbackHistoryQuery {
    /// Maximum rows returned; defaults to 50 and is capped at 200.
    pub limit: Option<u16>,
}

impl RollbackHistoryQuery {
    /// Validates the bounded history page size.
    pub fn validate(&self) -> Result<()> {
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "rollback history limit must be between 1 and 200",
            ));
        }
        Ok(())
    }
}

/// Safe tenant-scoped rollback history row. The idempotency key is intentionally
/// excluded from the read response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackHistoryItemDto {
    /// Append-only history row id.
    pub id: RollbackRecordId,
    /// Resource family.
    pub resource_kind: String,
    /// Resource id within that family.
    pub resource_id: String,
    /// Version active before rollback.
    pub from_version: u64,
    /// Newly published version.
    pub to_version: u64,
    /// Actor kind.
    pub actor_kind: String,
    /// Actor id.
    pub actor_id: String,
    /// Human-provided reason.
    pub reason: String,
    /// Immutable record timestamp.
    pub created_at: mas_common::timestamps::Timestamp,
}

/// Compact workflow reference for rollback history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRollbackTargetDto {
    /// Workflow identifier.
    pub workflow_id: WorkflowId,
    /// Requested immutable snapshot number.
    pub target_version_number: u64,
}
