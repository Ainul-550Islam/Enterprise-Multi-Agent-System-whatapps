//! Tenant-scoped dashboard and deterministic insight DTOs.

use mas_common::ids::AgentId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Closed time-series bucket vocabulary accepted by analytics endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalyticsGranularity {
    /// One-hour buckets.
    Hour,
    /// One-day buckets.
    Day,
    /// One-week buckets.
    Week,
}

/// A KPI card with comparable-period delta and bounded sparkline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KpiCardDto {
    /// Current aggregate; absent when the metric cannot be derived honestly.
    pub value: Option<f64>,
    /// Percentage change against the immediately preceding equal window.
    pub delta_percent: Option<f64>,
    /// Ordered values for the current range.
    pub sparkline: Vec<f64>,
    /// Display unit, e.g. `count`, `percent`, or `minutes`.
    pub unit: String,
}

/// Four top-line cards for the authenticated tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsOverviewDto {
    /// Requested start (inclusive).
    pub from: Timestamp,
    /// Requested end (exclusive).
    pub to: Timestamp,
    /// Total tenant agents, including draft/disabled resources.
    pub total_agents: KpiCardDto,
    /// Successfully completed executions during the period.
    pub tasks_completed: KpiCardDto,
    /// Completed / (completed + failed) as a percentage.
    pub success_rate: KpiCardDto,
    /// Sum of configured task baselines; absent if any completed agent lacks a baseline.
    pub time_saved_minutes: KpiCardDto,
}

/// One performance bucket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformancePointDto {
    /// Bucket start time.
    pub bucket: Timestamp,
    /// Successful executions in the bucket.
    pub tasks_completed: u64,
    /// Percentage of terminal executions that completed successfully.
    pub success_rate: Option<f64>,
}

/// Bounded time series for a performance chart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformanceSeriesDto {
    /// Bucket size used for the returned points.
    pub granularity: AnalyticsGranularity,
    /// Chronologically ordered buckets.
    pub points: Vec<PerformancePointDto>,
}

/// One agent's contribution to the task-volume chart.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TasksByAgentSliceDto {
    /// Agent identifier; absent for the combined `other` slice.
    pub agent_id: Option<AgentId>,
    /// Stable display label.
    pub label: String,
    /// Completed execution count.
    pub tasks_completed: u64,
}

/// Task-volume distribution, limited to the top slices plus `other`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TasksByAgentDto {
    /// Ranked slices, with `other` last when needed.
    pub slices: Vec<TasksByAgentSliceDto>,
    /// Completed execution count across every slice.
    pub total_completed: u64,
}

/// Summarized measured usage and cost; costs are omitted when pricing metadata is unavailable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageCostDto {
    /// Total cost in the returned currency, if every contributing record agrees on currency.
    pub total_cost: Option<f64>,
    /// ISO-4217 currency for `total_cost`.
    pub currency: Option<String>,
    /// Input-token quantity, if present in the metering data.
    pub tokens_input: u64,
    /// Output-token quantity, if present in the metering data.
    pub tokens_output: u64,
    /// Tool-invocation quantity.
    pub tool_invocations: u64,
    /// Requested time range.
    pub from: Timestamp,
    /// Requested time range end (exclusive).
    pub to: Timestamp,
}

/// Stable metric reference that lets a client explain a deterministic insight.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricReferenceDto {
    /// Metric key, e.g. `success_rate`.
    pub metric: String,
    /// Current value.
    pub current: f64,
    /// Comparison value.
    pub previous: f64,
    /// Optional agent responsible for the metric.
    pub agent_id: Option<AgentId>,
}

/// A deterministic, rule-derived dashboard insight (never LLM-generated).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsightDto {
    /// Stable insight code.
    pub code: String,
    /// Human-readable summary.
    pub title: String,
    /// Supporting explanation.
    pub detail: String,
    /// Machine-readable evidence used to produce the insight.
    pub metric_refs: Vec<MetricReferenceDto>,
}

/// Validated period query shared by analytics endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsQuery {
    /// Start time, inclusive.
    pub from: Timestamp,
    /// End time, exclusive.
    pub to: Timestamp,
    /// Requested bucket size; absent endpoints may select a safe default.
    pub granularity: Option<AnalyticsGranularity>,
}

impl AnalyticsQuery {
    /// Checks ordering and the bounded range.
    pub fn validate(&self) -> Result<()> {
        if !self.from.is_before(&self.to) {
            return Err(mas_common::error::AppError::invalid_field(
                "from",
                "invalid_range",
                "from must be before to",
            ));
        }
        let window = self
            .to
            .duration_since(&self.from)
            .unwrap_or(std::time::Duration::ZERO);
        if window > std::time::Duration::from_secs(366 * 86_400) {
            return Err(mas_common::error::AppError::invalid_field(
                "from",
                "range_too_large",
                "analytics queries are limited to 366 days",
            ));
        }
        Ok(())
    }
}

/// Optional extension fields for future, additive dashboard annotations.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AnalyticsAnnotationsDto {
    /// Named numeric values computed from the same scoped snapshot.
    #[serde(default)]
    pub metrics: BTreeMap<String, f64>,
}
