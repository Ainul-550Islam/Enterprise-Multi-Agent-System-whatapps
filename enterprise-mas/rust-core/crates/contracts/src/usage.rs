//! Usage/quota DTOs.

use mas_common::ids::{ProjectId, UsageRecordId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

/// Bucket size for usage aggregations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    Hour,
    Day,
    Month,
}

impl Granularity {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Month => "month",
        }
    }
}

/// Query for usage records/aggregations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    /// One of the quota dimension strings (`requests`, `tokens`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dimension: Option<String>,
    pub from: Timestamp,
    pub to: Timestamp,
    #[serde(default)]
    pub granularity: Option<Granularity>,
}

impl UsageQuery {
    pub fn validate(&self) -> Result<()> {
        if !self.from.is_before(&self.to) {
            return Err(mas_common::error::AppError::invalid_field(
                "from",
                "invalid_range",
                "'from' must be before 'to'",
            ));
        }
        // Cap query windows at 366 days to protect the read path.
        let window = self
            .to
            .duration_since(&self.from)
            .unwrap_or(std::time::Duration::ZERO);
        if window > std::time::Duration::from_secs(366 * 86_400) {
            return Err(mas_common::error::AppError::invalid_field(
                "from",
                "range_too_large",
                "usage queries are limited to 366 days",
            ));
        }
        if let Some(dimension) = &self.dimension {
            match dimension.as_str() {
                "requests" | "executions" | "concurrent_runs" | "tokens" | "storage_bytes"
                | "tool_invocations" | "api_calls" => {},
                other => {
                    return Err(mas_common::error::AppError::invalid_field(
                        "dimension",
                        "invalid_enum_value",
                        format!("unknown usage dimension '{other}'"),
                    ));
                },
            }
        }
        Ok(())
    }
}

/// Wire representation of one usage record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRecordResponse {
    pub id: UsageRecordId,
    pub dimension: String,
    pub quantity: u64,
    pub actor_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<mas_common::ids::ExecutionId>,
    #[serde(default)]
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub occurred_at: Timestamp,
}

/// Aggregated usage for one dimension over the queried window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageSummaryResponse {
    pub tenant_id: mas_common::ids::TenantId,
    pub dimension: String,
    pub total: u64,
    pub period_start: Timestamp,
    pub period_end: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// 0.0..=1.0+ — present when a limit applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub utilization: Option<f64>,
}

/// Current quota utilization (for dashboards/limits inspection).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaUtilizationDto {
    pub dimension: String,
    pub limit: u64,
    pub used: u64,
    pub remaining: u64,
    /// `enforce` | `warn_only` | `allow_with_overage`
    pub enforcement: String,
    /// `"per_minute" | "hourly" | "daily" | "monthly" | "unbounded"`
    pub period: String,
    pub period_started_at: Timestamp,
}
