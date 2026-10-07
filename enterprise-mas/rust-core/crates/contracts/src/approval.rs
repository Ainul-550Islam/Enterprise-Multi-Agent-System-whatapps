//! Human-in-the-loop approval queue DTOs.

use mas_common::ids::ApprovalRequestId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Optional filter for the approvals queue.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApprovalQuery {
    /// `pending`, `approved`, `rejected`, `expired`, or `cancelled`.
    pub status: Option<String>,
    /// Maximum returned records, capped by validation.
    pub limit: Option<u16>,
}

impl ApprovalQuery {
    /// Validates the status filter and result bound.
    pub fn validate(&self) -> Result<()> {
        if let Some(status) = &self.status {
            match status.as_str() {
                "pending" | "approved" | "rejected" | "expired" | "cancelled" => {},
                other => {
                    return Err(mas_common::error::AppError::invalid_field(
                        "status",
                        "invalid_enum_value",
                        format!("unknown approval status '{other}'"),
                    ));
                },
            }
        }
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "approval page limit must be between 1 and 200",
            ));
        }
        Ok(())
    }
}

/// Approval decision request. A reason is mandatory for denials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalDecisionRequest {
    /// Human-readable justification.
    pub reason: String,
}

impl ApprovalDecisionRequest {
    /// Validates the operator-supplied decision reason.
    pub fn validate(&self) -> Result<()> {
        validation::validate_length("reason", &self.reason, 1, 1024)?;
        if self.reason.trim().is_empty() || self.reason.chars().any(char::is_control) {
            return Err(mas_common::error::AppError::invalid_field(
                "reason",
                "invalid_format",
                "reason must be non-empty and contain no control characters",
            ));
        }
        Ok(())
    }
}

/// Safe view of an approval request; excludes internal evaluator state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalResponse {
    /// Approval identifier.
    pub id: ApprovalRequestId,
    /// Tenant scope.
    pub tenant_id: mas_common::ids::TenantId,
    /// Actor whose action is gated.
    pub subject_actor: String,
    /// Action under review.
    pub action: String,
    /// Resource under review.
    pub resource: String,
    /// Optional rule reason.
    pub reason: Option<String>,
    /// Required approval quorum.
    pub quorum: u16,
    /// Number of approvals received.
    pub approvals: u16,
    /// Number of denials received.
    pub rejections: u16,
    /// Lifecycle status.
    pub status: String,
    /// Creation timestamp.
    pub created_at: Timestamp,
    /// Last update timestamp.
    pub updated_at: Timestamp,
    /// Optional expiry instant.
    pub expires_at: Option<Timestamp>,
}
