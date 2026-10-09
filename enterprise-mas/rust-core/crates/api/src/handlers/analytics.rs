//! Tenant-scoped, deterministic dashboard read endpoints.
//!
//! These handlers intentionally contain no aggregation logic and no LLM call:
//! they authorize the resolved scope, validate the bounded period query, then
//! delegate to `AnalyticsService`. Unknown metric values remain null rather
//! than being manufactured for a visually complete dashboard.

use axum::extract::{Query, State};
use mas_contracts::analytics::{
    AnalyticsOverviewDto, AnalyticsQuery, InsightDto, PerformanceSeriesDto, TasksByAgentDto,
    UsageCostDto,
};

use crate::handlers::Ctx;
use crate::response::{ApiError, ApiResult};
use crate::state::AppState;

/// `GET /v1/analytics/overview?from=&to=&granularity=`.
pub async fn overview(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<AnalyticsQuery>,
) -> ApiResult<AnalyticsOverviewDto> {
    let service_ctx = read_context(&ctx)?;
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .analytics_service(&ctx)?
        .overview(
            service_ctx
                .require_tenant()
                .map_err(|error| ApiError::new(error, &ctx))?,
            &query,
        )
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/analytics/performance?from=&to=&granularity=`.
pub async fn performance(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<AnalyticsQuery>,
) -> ApiResult<PerformanceSeriesDto> {
    let service_ctx = read_context(&ctx)?;
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .analytics_service(&ctx)?
        .performance(
            service_ctx
                .require_tenant()
                .map_err(|error| ApiError::new(error, &ctx))?,
            &query,
        )
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/analytics/tasks-by-agent?from=&to=&granularity=`.
pub async fn tasks_by_agent(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<AnalyticsQuery>,
) -> ApiResult<TasksByAgentDto> {
    let service_ctx = read_context(&ctx)?;
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .analytics_service(&ctx)?
        .tasks_by_agent(
            service_ctx
                .require_tenant()
                .map_err(|error| ApiError::new(error, &ctx))?,
            &query,
        )
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/analytics/usage-cost?from=&to=&granularity=`.
pub async fn usage_cost(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<AnalyticsQuery>,
) -> ApiResult<UsageCostDto> {
    let service_ctx = read_context(&ctx)?;
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .analytics_service(&ctx)?
        .usage_cost(
            service_ctx
                .require_tenant()
                .map_err(|error| ApiError::new(error, &ctx))?,
            &query,
        )
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/assistant/insights?from=&to=&granularity=` uses the same
/// deterministic service; the assistant endpoint never calls a model merely
/// to render dashboard observations.
pub async fn insights(State(state): State<AppState>, Ctx(ctx): Ctx) -> ApiResult<Vec<InsightDto>> {
    let service_ctx = read_context(&ctx)?;
    state
        .analytics_service(&ctx)?
        .insights(
            service_ctx
                .require_tenant()
                .map_err(|error| ApiError::new(error, &ctx))?,
            mas_common::timestamps::Timestamp::now(),
        )
        .await
        .map(|payload| crate::response::ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

fn read_context(
    ctx: &crate::context::RequestContext,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::Usage,
        mas_security::Action::Read,
        None,
    )
    .map_err(|error| ApiError::new(error, ctx))
}
