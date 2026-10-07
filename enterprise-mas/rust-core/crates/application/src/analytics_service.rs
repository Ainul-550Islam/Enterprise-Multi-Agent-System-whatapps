//! Tenant-scoped dashboard read models and deterministic insights.
//!
//! This path is intentionally LLM-free and read-only. It computes a bounded
//! projection from the analytics store port and never fills an unknown agent
//! baseline with a guessed time-saving number.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use mas_common::enums::ExecutionStatus;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_contracts::analytics::{
    AnalyticsGranularity, AnalyticsOverviewDto, AnalyticsQuery, InsightDto, KpiCardDto,
    MetricReferenceDto, PerformancePointDto, PerformanceSeriesDto, TasksByAgentDto,
    TasksByAgentSliceDto, UsageCostDto,
};
use mas_domain::{Agent, Execution, UsageRecord};

use crate::stores::{AnalyticsSnapshot, AnalyticsStorePort};

/// Analytics use cases over a tenant-filtered read port.
#[derive(Debug)]
pub struct AnalyticsService {
    store: Box<dyn AnalyticsStorePort>,
}

impl AnalyticsService {
    /// Composes the service over its read-only store port.
    pub fn new(store: Box<dyn AnalyticsStorePort>) -> Self {
        Self { store }
    }

    /// Four top-line KPI cards with previous-equal-window deltas.
    pub async fn overview(
        &self,
        tenant: TenantId,
        query: &AnalyticsQuery,
    ) -> Result<AnalyticsOverviewDto> {
        query.validate()?;
        let snapshot = self.snapshot(tenant, query).await?;
        let duration = query
            .to
            .duration_since(&query.from)
            .ok_or_else(|| AppError::invalid_field("from", "invalid_range", "invalid range"))?;
        let previous_from = query
            .from
            .checked_sub(duration)
            .unwrap_or(Timestamp::epoch());
        let current = PeriodMetrics::from_snapshot(&snapshot, query.from, query.to);
        let previous = PeriodMetrics::from_snapshot(&snapshot, previous_from, query.from);
        let buckets = bucket_count(query)?;
        let task_sparkline = completed_sparkline(
            &snapshot.executions,
            query.from,
            query.to,
            query.granularity,
            buckets,
        );
        let agent_sparkline = agent_count_sparkline(
            &snapshot.agents,
            query.from,
            query.to,
            query.granularity,
            buckets,
        );
        let success_sparkline = success_rate_sparkline(
            &snapshot.executions,
            query.from,
            query.to,
            query.granularity,
            buckets,
        );
        let saved_sparkline = time_saved_sparkline(
            &snapshot.executions,
            &snapshot.agents,
            query.from,
            query.to,
            query.granularity,
            buckets,
        );

        Ok(AnalyticsOverviewDto {
            from: query.from,
            to: query.to,
            total_agents: KpiCardDto {
                value: Some(current.total_agents as f64),
                delta_percent: percent_delta(
                    current.total_agents as f64,
                    previous.total_agents as f64,
                ),
                sparkline: agent_sparkline,
                unit: "count".to_owned(),
            },
            tasks_completed: KpiCardDto {
                value: Some(current.completed as f64),
                delta_percent: percent_delta(current.completed as f64, previous.completed as f64),
                sparkline: task_sparkline,
                unit: "count".to_owned(),
            },
            success_rate: KpiCardDto {
                value: current.success_rate,
                delta_percent: match (current.success_rate, previous.success_rate) {
                    (Some(now), Some(before)) => percent_delta(now, before),
                    _ => None,
                },
                sparkline: success_sparkline,
                unit: "percent".to_owned(),
            },
            time_saved_minutes: KpiCardDto {
                value: current.time_saved_minutes,
                delta_percent: match (current.time_saved_minutes, previous.time_saved_minutes) {
                    (Some(now), Some(before)) => percent_delta(now, before),
                    _ => None,
                },
                sparkline: saved_sparkline,
                unit: "minutes".to_owned(),
            },
        })
    }

    /// Bounded completed-task and success-rate series.
    pub async fn performance(
        &self,
        tenant: TenantId,
        query: &AnalyticsQuery,
    ) -> Result<PerformanceSeriesDto> {
        query.validate()?;
        let snapshot = self.snapshot(tenant, query).await?;
        let granularity = query.granularity.unwrap_or(AnalyticsGranularity::Day);
        let count = bucket_count(query)?;
        let width = bucket_width(granularity);
        let points = (0..count)
            .map(|index| {
                let start = bucket_start(query.from, index, width);
                let end = start
                    .checked_add(Duration::from_secs(width))
                    .unwrap_or(query.to)
                    .min(query.to);
                let metrics = execution_metrics(&snapshot.executions, start, end, &[], false);
                PerformancePointDto {
                    bucket: start,
                    tasks_completed: metrics.completed,
                    success_rate: metrics.success_rate,
                }
            })
            .collect();
        Ok(PerformanceSeriesDto {
            granularity,
            points,
        })
    }

    /// Top five agents by completed executions and an `other` remainder.
    pub async fn tasks_by_agent(
        &self,
        tenant: TenantId,
        query: &AnalyticsQuery,
    ) -> Result<TasksByAgentDto> {
        query.validate()?;
        let snapshot = self.snapshot(tenant, query).await?;
        let mut counts: BTreeMap<AgentId, u64> = BTreeMap::new();
        for execution in snapshot.executions.iter().filter(|execution| {
            in_range(execution_time(execution), query.from, query.to)
                && execution.status == ExecutionStatus::Completed
        }) {
            if let Some(agent_id) = execution.agent_id {
                *counts.entry(agent_id).or_default() += 1;
            }
        }
        let names: HashMap<AgentId, String> = snapshot
            .agents
            .iter()
            .map(|agent| (agent.id, agent.name.clone()))
            .collect();
        let mut ranked: Vec<(AgentId, u64)> = counts.into_iter().collect();
        ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        let total_completed = ranked.iter().map(|(_, count)| *count).sum();
        let mut slices = Vec::new();
        let mut other = 0_u64;
        for (index, (agent_id, count)) in ranked.into_iter().enumerate() {
            if index < 5 {
                slices.push(TasksByAgentSliceDto {
                    agent_id: Some(agent_id),
                    label: names
                        .get(&agent_id)
                        .cloned()
                        .unwrap_or_else(|| "Unknown agent".to_owned()),
                    tasks_completed: count,
                });
            } else {
                other = other.saturating_add(count);
            }
        }
        if other > 0 {
            slices.push(TasksByAgentSliceDto {
                agent_id: None,
                label: "Other".to_owned(),
                tasks_completed: other,
            });
        }
        Ok(TasksByAgentDto {
            slices,
            total_completed,
        })
    }

    /// Measured usage/cost summary. Unknown or mixed pricing is reported as
    /// unknown instead of combining incompatible currencies.
    pub async fn usage_cost(
        &self,
        tenant: TenantId,
        query: &AnalyticsQuery,
    ) -> Result<UsageCostDto> {
        query.validate()?;
        let snapshot = self.snapshot(tenant, query).await?;
        let records: Vec<&UsageRecord> = snapshot
            .usage_records
            .iter()
            .filter(|record| record.occurred_at >= query.from && record.occurred_at < query.to)
            .collect();
        let mut tokens_input = 0_u64;
        let mut tokens_output = 0_u64;
        let mut tool_invocations = 0_u64;
        let mut total_micros = 0_i128;
        let mut currency: Option<String> = None;
        let mut cost_is_complete = !records.is_empty();
        for record in &records {
            let dimension = record.dimension.as_str();
            if dimension == "tool_invocations" {
                tool_invocations = tool_invocations.saturating_add(record.quantity);
            }
            if dimension == "tokens" {
                match record
                    .metadata
                    .get("token_direction")
                    .and_then(serde_json::Value::as_str)
                {
                    Some("input") => tokens_input = tokens_input.saturating_add(record.quantity),
                    Some("output") => tokens_output = tokens_output.saturating_add(record.quantity),
                    _ => {},
                }
            }
            let cost = record
                .metadata
                .get("cost_micros")
                .and_then(serde_json::Value::as_i64);
            let record_currency = record
                .metadata
                .get("currency")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            match (cost, record_currency) {
                (Some(cost), Some(record_currency)) if cost >= 0 => {
                    if let Some(existing) = &currency {
                        if existing != &record_currency {
                            cost_is_complete = false;
                        }
                    } else {
                        currency = Some(record_currency);
                    }
                    total_micros = total_micros.saturating_add(i128::from(cost));
                },
                _ => cost_is_complete = false,
            }
        }
        let total_cost = if cost_is_complete {
            Some(total_micros as f64 / 1_000_000.0)
        } else {
            None
        };
        let currency = total_cost.and(currency);
        Ok(UsageCostDto {
            total_cost,
            currency,
            tokens_input,
            tokens_output,
            tool_invocations,
            from: query.from,
            to: query.to,
        })
    }

    /// Rule-derived insights based on the largest agent-level week-over-week
    /// completed-task movement. No model call or mutation occurs here.
    pub async fn insights(&self, tenant: TenantId, now: Timestamp) -> Result<Vec<InsightDto>> {
        let current_from = now
            .checked_sub(Duration::from_secs(7 * 86_400))
            .unwrap_or(Timestamp::epoch());
        let previous_from = current_from
            .checked_sub(Duration::from_secs(7 * 86_400))
            .unwrap_or(Timestamp::epoch());
        let snapshot = self.store.snapshot(tenant, previous_from, now).await?;
        let mut movements = Vec::new();
        for agent in snapshot.agents.iter() {
            let current = completed_for_agent(&snapshot.executions, agent.id, current_from, now);
            let previous =
                completed_for_agent(&snapshot.executions, agent.id, previous_from, current_from);
            if current != previous {
                movements.push((agent, current, previous, current.abs_diff(previous)));
            }
        }
        movements.sort_by(|left, right| {
            right
                .3
                .cmp(&left.3)
                .then_with(|| left.0.id.cmp(&right.0.id))
        });
        let Some((agent, current, previous, _)) = movements.first() else {
            return Ok(Vec::new());
        };
        let direction = if current >= previous {
            "increased"
        } else {
            "decreased"
        };
        Ok(vec![InsightDto {
            code: "largest_agent_task_movement".to_owned(),
            title: format!("{} tasks {direction}", agent.name),
            detail: format!(
                "{} completed tasks this week versus {} in the previous week.",
                current, previous
            ),
            metric_refs: vec![MetricReferenceDto {
                metric: "tasks_completed".to_owned(),
                current: *current as f64,
                previous: *previous as f64,
                agent_id: Some(agent.id),
            }],
        }])
    }

    async fn snapshot(
        &self,
        tenant: TenantId,
        query: &AnalyticsQuery,
    ) -> Result<AnalyticsSnapshot> {
        let duration = query
            .to
            .duration_since(&query.from)
            .ok_or_else(|| AppError::invalid_field("from", "invalid_range", "invalid range"))?;
        let previous_from = query
            .from
            .checked_sub(duration)
            .unwrap_or(Timestamp::epoch());
        self.store.snapshot(tenant, previous_from, query.to).await
    }
}

#[derive(Debug, Clone, Copy)]
struct PeriodMetrics {
    total_agents: usize,
    completed: u64,
    success_rate: Option<f64>,
    time_saved_minutes: Option<f64>,
}

impl PeriodMetrics {
    fn from_snapshot(snapshot: &AnalyticsSnapshot, from: Timestamp, to: Timestamp) -> Self {
        let total_agents = snapshot
            .agents
            .iter()
            .filter(|agent| agent.created_at < to)
            .count();
        let execution = execution_metrics(&snapshot.executions, from, to, &snapshot.agents, true);
        Self {
            total_agents,
            completed: execution.completed,
            success_rate: execution.success_rate,
            time_saved_minutes: execution.time_saved_minutes,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ExecutionMetrics {
    completed: u64,
    success_rate: Option<f64>,
    time_saved_minutes: Option<f64>,
}

fn execution_metrics(
    executions: &[Execution],
    from: Timestamp,
    to: Timestamp,
    agents: &[Agent],
    compute_time_saved: bool,
) -> ExecutionMetrics {
    let mut completed = 0_u64;
    let mut failed = 0_u64;
    let mut time_saved = 0.0_f64;
    let mut baseline_known = true;
    let by_id: HashMap<AgentId, &Agent> = agents.iter().map(|agent| (agent.id, agent)).collect();
    for execution in executions
        .iter()
        .filter(|execution| in_range(execution_time(execution), from, to))
    {
        match execution.status {
            ExecutionStatus::Completed => {
                completed = completed.saturating_add(1);
                if compute_time_saved {
                    match execution.agent_id.and_then(|id| by_id.get(&id).copied()) {
                        Some(agent) => match agent.baseline_minutes_per_task {
                            Some(minutes) => time_saved += f64::from(minutes),
                            None => baseline_known = false,
                        },
                        None => baseline_known = false,
                    }
                }
            },
            ExecutionStatus::Failed => failed = failed.saturating_add(1),
            _ => {},
        }
    }
    let terminal = completed.saturating_add(failed);
    let success_rate = (terminal > 0).then(|| completed as f64 / terminal as f64 * 100.0);
    ExecutionMetrics {
        completed,
        success_rate,
        time_saved_minutes: if compute_time_saved && baseline_known {
            Some(time_saved)
        } else {
            None
        },
    }
}

fn completed_sparkline(
    executions: &[Execution],
    from: Timestamp,
    to: Timestamp,
    granularity: Option<AnalyticsGranularity>,
    count: usize,
) -> Vec<f64> {
    let granularity = granularity.unwrap_or(AnalyticsGranularity::Day);
    let width = bucket_width(granularity);
    (0..count)
        .map(|index| {
            let start = bucket_start(from, index, width);
            let end = start
                .checked_add(Duration::from_secs(width))
                .unwrap_or(to)
                .min(to);
            executions
                .iter()
                .filter(|execution| {
                    execution.status == ExecutionStatus::Completed
                        && in_range(execution_time(execution), start, end)
                })
                .count() as f64
        })
        .collect()
}

fn agent_count_sparkline(
    agents: &[Agent],
    from: Timestamp,
    to: Timestamp,
    granularity: Option<AnalyticsGranularity>,
    count: usize,
) -> Vec<f64> {
    let granularity = granularity.unwrap_or(AnalyticsGranularity::Day);
    let width = bucket_width(granularity);
    (0..count)
        .map(|index| {
            let end = bucket_start(from, index + 1, width).min(to);
            agents.iter().filter(|agent| agent.created_at < end).count() as f64
        })
        .collect()
}

fn success_rate_sparkline(
    executions: &[Execution],
    from: Timestamp,
    to: Timestamp,
    granularity: Option<AnalyticsGranularity>,
    count: usize,
) -> Vec<f64> {
    let granularity = granularity.unwrap_or(AnalyticsGranularity::Day);
    let width = bucket_width(granularity);
    (0..count)
        .filter_map(|index| {
            let start = bucket_start(from, index, width);
            let end = start
                .checked_add(Duration::from_secs(width))
                .unwrap_or(to)
                .min(to);
            execution_metrics(executions, start, end, &[], false).success_rate
        })
        .collect()
}

fn time_saved_sparkline(
    executions: &[Execution],
    agents: &[Agent],
    from: Timestamp,
    to: Timestamp,
    granularity: Option<AnalyticsGranularity>,
    count: usize,
) -> Vec<f64> {
    let granularity = granularity.unwrap_or(AnalyticsGranularity::Day);
    let width = bucket_width(granularity);
    (0..count)
        .filter_map(|index| {
            let start = bucket_start(from, index, width);
            let end = start
                .checked_add(Duration::from_secs(width))
                .unwrap_or(to)
                .min(to);
            execution_metrics(executions, start, end, agents, true).time_saved_minutes
        })
        .collect()
}

fn completed_for_agent(
    executions: &[Execution],
    agent: AgentId,
    from: Timestamp,
    to: Timestamp,
) -> u64 {
    executions
        .iter()
        .filter(|execution| {
            execution.agent_id == Some(agent)
                && execution.status == ExecutionStatus::Completed
                && in_range(execution_time(execution), from, to)
        })
        .count() as u64
}

fn execution_time(execution: &Execution) -> Timestamp {
    execution.finished_at.unwrap_or(execution.created_at)
}

fn in_range(at: Timestamp, from: Timestamp, to: Timestamp) -> bool {
    at >= from && at < to
}

fn percent_delta(current: f64, previous: f64) -> Option<f64> {
    if previous == 0.0 {
        (current == 0.0).then_some(0.0)
    } else {
        Some((current - previous) / previous.abs() * 100.0)
    }
}

fn bucket_width(granularity: AnalyticsGranularity) -> u64 {
    match granularity {
        AnalyticsGranularity::Hour => 3_600,
        AnalyticsGranularity::Day => 86_400,
        AnalyticsGranularity::Week => 7 * 86_400,
    }
}

fn bucket_count(query: &AnalyticsQuery) -> Result<usize> {
    let granularity = query.granularity.unwrap_or(AnalyticsGranularity::Day);
    let seconds = query
        .to
        .duration_since(&query.from)
        .ok_or_else(|| AppError::invalid_field("from", "invalid_range", "invalid range"))?
        .as_secs();
    let count = seconds.div_ceil(bucket_width(granularity));
    if count > 500 {
        return Err(AppError::invalid_field(
            "granularity",
            "too_many_buckets",
            "analytics chart is limited to 500 buckets; choose a coarser granularity",
        ));
    }
    Ok(count as usize)
}

fn bucket_start(from: Timestamp, index: usize, width_seconds: u64) -> Timestamp {
    from.checked_add(Duration::from_secs(
        width_seconds.saturating_mul(index as u64),
    ))
    .unwrap_or(from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::{AgentStorePort, ExecutionStorePort, InMemoryServices};
    use mas_common::ids::{OrganizationId, ProjectId};
    use mas_domain::{value_objects::Slug, AgentKind};
    use std::sync::Arc;

    fn query(from: Timestamp, to: Timestamp) -> AnalyticsQuery {
        AnalyticsQuery {
            from,
            to,
            granularity: Some(AnalyticsGranularity::Day),
        }
    }

    #[tokio::test]
    async fn unknown_agent_baseline_is_not_guessed() {
        let store = Arc::new(InMemoryServices::new());
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let project = ProjectId::new();
        let agent = Agent::create(
            tenant,
            organization,
            project,
            "No baseline",
            Slug::new("no-baseline").expect("slug"),
            AgentKind::Standard,
        )
        .expect("agent");
        AgentStorePort::save(&store, &agent)
            .await
            .expect("save agent");
        let mut execution = Execution::new(
            tenant,
            organization,
            project,
            None,
            Some(agent.id),
            serde_json::json!({}),
            "analytics-no-baseline",
            "test",
        )
        .expect("execution");
        execution.status = ExecutionStatus::Completed;
        execution.finished_at = Some(Timestamp::now());
        ExecutionStorePort::save(&store, &execution)
            .await
            .expect("save execution");

        let service = AnalyticsService::new(Box::new(store));
        let from = Timestamp::now()
            .checked_sub(Duration::from_secs(2 * 86_400))
            .expect("from");
        let to = Timestamp::now()
            .checked_add(Duration::from_secs(1))
            .expect("to");
        let response = service
            .overview(tenant, &query(from, to))
            .await
            .expect("overview");
        assert_eq!(response.tasks_completed.value, Some(1.0));
        assert_eq!(response.time_saved_minutes.value, None);
    }
}
