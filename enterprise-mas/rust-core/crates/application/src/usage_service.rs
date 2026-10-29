//! PURPOSE: Tenant-scoped plan-card and usage read use cases with server-computed quota utilization.
//!
//! Owning layer: `mas-application`.
//! Production responsibility: bound usage windows, aggregate durable usage facts, and combine those facts with configured quota definitions/counters.
//! Data ownership: usage records and quota state are owned by persistence/control-plane adapters.
//! MUST NOT: accept tenant identity from client data, invent missing usage, write metering facts, or calculate billing from client-supplied values.

use async_trait::async_trait;
use mas_common::{
    error::AppError,
    ids::{ProjectId, TenantId},
    result::Result,
    timestamps::Timestamp,
};
use mas_contracts::usage::{QuotaUtilizationDto, UsageQuery, UsageSummaryResponse};
use mas_domain::quota::{Quota, QuotaDimension};
use std::fmt;

/// Read-only port for usage facts and configured quota state.
#[async_trait]
pub trait UsageReadPort: Send + Sync + fmt::Debug {
    /// Aggregates tenant records over the half-open interval `[from, to)`; adapter must bound SQL results/work.
    async fn totals(
        &self,
        tenant: TenantId,
        project: Option<ProjectId>,
        from: Timestamp,
        to: Timestamp,
        dimension: Option<&str>,
    ) -> Result<Vec<(String, u64)>>;
    /// Lists currently configured quotas and their authoritative counter values.
    async fn quota_snapshots(&self, tenant: TenantId) -> Result<Vec<(Quota, u64)>>;
}

/// Usage and quota read service.
#[derive(Debug)]
pub struct UsageService {
    port: Box<dyn UsageReadPort>,
}

impl UsageService {
    /// Composes over the production usage/quota read adapter.
    pub fn new(port: Box<dyn UsageReadPort>) -> Self {
        Self { port }
    }

    /// Returns usage totals and the applied quota limit for the requested window.
    pub async fn summary(
        &self,
        tenant: TenantId,
        query: &UsageQuery,
    ) -> Result<Vec<UsageSummaryResponse>> {
        query.validate()?;
        let totals = self
            .port
            .totals(
                tenant,
                query.project_id,
                query.from,
                query.to,
                query.dimension.as_deref(),
            )
            .await?;
        let quotas = self.port.quota_snapshots(tenant).await?;
        let mut output = Vec::with_capacity(totals.len());
        for (dimension, total) in totals {
            let limit = quotas
                .iter()
                .find(|(quota, _)| quota.dimension.as_str() == dimension)
                .map(|(quota, _)| quota.limit);
            let utilization = limit
                .filter(|limit| *limit > 0)
                .map(|limit| total as f64 / limit as f64);
            output.push(UsageSummaryResponse {
                tenant_id: tenant,
                dimension,
                total,
                period_start: query.from,
                period_end: query.to,
                limit,
                utilization,
            });
        }
        Ok(output)
    }

    /// Returns configured quota usage snapshots, deriving percentages from current server-owned values.
    pub async fn quotas(&self, tenant: TenantId) -> Result<Vec<QuotaUtilizationDto>> {
        self.port
            .quota_snapshots(tenant)
            .await?
            .into_iter()
            .map(|(quota, used)| {
                let dim = quota.dimension.as_str().to_owned();
                Ok(QuotaUtilizationDto {
                    dimension: dim,
                    limit: quota.limit,
                    used,
                    remaining: quota.limit.saturating_sub(used),
                    enforcement: quota.enforcement.to_string(),
                    period: quota.period.to_string(),
                    period_started_at: quota.period_started_at,
                })
            })
            .collect()
    }
}

/// Mapping helper used by persistence adapters to reject unknown database dimensions.
pub fn parse_dimension(value: &str) -> Result<QuotaDimension> {
    value.parse::<QuotaDimension>().map_err(|_| {
        AppError::invalid_field(
            "dimension",
            "invalid_enum_value",
            "stored quota dimension is not recognized",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct MemoryPort {
        totals: Mutex<Vec<(String, u64)>>,
        quotas: Mutex<Vec<(Quota, u64)>>,
    }

    #[async_trait]
    impl UsageReadPort for MemoryPort {
        async fn totals(
            &self,
            _tenant: TenantId,
            _project: Option<ProjectId>,
            _from: Timestamp,
            _to: Timestamp,
            _dimension: Option<&str>,
        ) -> Result<Vec<(String, u64)>> {
            Ok(self
                .totals
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone())
        }
        async fn quota_snapshots(&self, _tenant: TenantId) -> Result<Vec<(Quota, u64)>> {
            Ok(self
                .quotas
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone())
        }
    }

    #[tokio::test]
    async fn percentages_are_computed_from_authoritative_snapshots() {
        use mas_domain::quota::{EnforcementMode, QuotaPeriod};
        let tenant = TenantId::new();
        let quota = Quota::new(
            tenant,
            QuotaDimension::Tokens,
            QuotaPeriod::Monthly,
            100,
            EnforcementMode::Enforce,
        )
        .expect("quota");
        let port = MemoryPort {
            totals: Mutex::new(vec![("tokens".to_owned(), 25)]),
            quotas: Mutex::new(vec![(quota, 20)]),
        };
        let service = UsageService::new(Box::new(port));
        let query = UsageQuery {
            project_id: None,
            dimension: None,
            from: Timestamp::from_unix_seconds(1_700_000_000).expect("from"),
            to: Timestamp::from_unix_seconds(1_700_000_060).expect("to"),
            granularity: None,
        };
        let rows = service.summary(tenant, &query).await.expect("summary");
        assert_eq!(rows[0].utilization, Some(0.25));
    }
}
