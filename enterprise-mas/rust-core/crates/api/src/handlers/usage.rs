//! PURPOSE: Tenant-scoped quota and usage summary HTTP reads for the plan card and usage page.
//!
//! Owning layer: `mas-api` transport edge.
//! Production responsibility: authorize read access and delegate aggregation to UsageService using only the resolved tenant.
//! Data ownership: usage facts and quota counters are persistence-owned; utilization is computed by the application service.
//! MUST NOT: register legacy `/v1/usage`, expose other tenants' usage, accept a tenant id, or fabricate zero-valued data when unavailable.

use axum::extract::{Query, State};
use mas_contracts::usage::{QuotaUtilizationDto, UsageQuery, UsageSummaryResponse};
use mas_security::{Action, ResourceKind};

use crate::{
    handlers::{authorized_service_context, Ctx},
    response::{ok, ApiError, ApiResult},
    state::AppState,
};

/// `GET /v1/quotas`.
pub async fn quotas(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<QuotaUtilizationDto>> {
    let service_ctx = authorized_service_context(&ctx, ResourceKind::Usage, Action::Read, None)
        .map_err(|error| ApiError::new(error, &ctx))?;
    let tenant = service_ctx
        .require_tenant()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .usage_service(&ctx)?
        .quotas(tenant)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/usage/summary?from=&to=&dimension=`.
pub async fn summary(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<UsageQuery>,
) -> ApiResult<Vec<UsageSummaryResponse>> {
    let service_ctx =
        authorized_service_context(&ctx, ResourceKind::Usage, Action::Read, query.project_id)
            .map_err(|error| ApiError::new(error, &ctx))?;
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let tenant = service_ctx
        .require_tenant()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .usage_service(&ctx)?
        .summary(tenant, &query)
        .await
        .map(|data| ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_summary_rejects_an_empty_time_window() {
        let instant =
            mas_common::timestamps::Timestamp::from_unix_seconds(1_700_000_000).expect("timestamp");
        let query = UsageQuery {
            project_id: None,
            dimension: None,
            from: instant,
            to: instant,
            granularity: None,
        };
        assert!(query.validate().is_err());
    }
}
