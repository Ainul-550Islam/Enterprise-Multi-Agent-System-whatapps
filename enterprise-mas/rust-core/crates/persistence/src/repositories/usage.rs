//! PURPOSE: Tenant-scoped PostgreSQL usage repository: append-only `UsageStorePort` plus bounded usage/quota read queries.
//!
//! Owning layer: `mas-persistence`.
//! Production responsibility: persist immutable usage facts, support bounded tenant-filtered reads, and calculate plan-card totals from authoritative usage rows and quota counters.
//! Data ownership: PostgreSQL is authoritative; request scope is supplied by the middleware-bound transaction and every statement also binds tenant_id.
//! MUST NOT: update/delete usage, perform unscoped pool queries, return unbounded result sets, or accept tenant ids from user payloads as authority.

use async_trait::async_trait;
use mas_application::{stores::UsageStorePort, usage_service::UsageReadPort};
use mas_common::{
    error::AppError,
    ids::{ExecutionId, OrganizationId, ProjectId, TenantId, UsageRecordId},
    result::Result,
    timestamps::Timestamp,
};
use mas_domain::{
    quota::{EnforcementMode, Quota, QuotaDimension, QuotaPeriod},
    usage::{ActorKind, ActorRef, UsageRecord},
};
use sqlx::{PgConnection, Row};

use crate::{error::map_sqlx, scoped_request};

/// Keyset position for descending usage reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageCursor {
    /// Timestamp of the last returned row.
    pub occurred_at: Timestamp,
    /// Identifier of the last returned row.
    pub id: UsageRecordId,
}

/// Bounded, tenant-scoped usage repository.
#[derive(Debug, Clone)]
pub struct PostgresUsageStore {
    _pool: sqlx::PgPool,
}

impl PostgresUsageStore {
    /// Creates an adapter over the shared request-scoped pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { _pool: pool }
    }

    /// Appends one immutable usage fact within a caller-managed transaction.
    pub async fn append_in(connection: &mut PgConnection, record: &UsageRecord) -> Result<()> {
        let mut dimensions = record.metadata.clone();
        dimensions.insert(
            "_actor_kind".to_owned(),
            serde_json::Value::String(record.actor.kind.to_string()),
        );
        sqlx::query("INSERT INTO usage_records (id, tenant_id, organization_id, project_id, resource, quantity, unit, source_id, dimensions, execution_id, occurred_at) VALUES ($1,$2,$3,$4,$5,$6,'units',$7,$8,$9,$10)")
            .bind(record.id.into_uuid()).bind(record.tenant_id.into_uuid()).bind(record.organization_id.into_uuid())
            .bind(record.project_id.map(ProjectId::into_uuid)).bind(record.dimension.as_str())
            .bind(i64::try_from(record.quantity).map_err(|_| AppError::validation("usage quantity exceeds the supported database range"))?)
            .bind(&record.actor.id).bind(serde_json::Value::Object(dimensions))
            .bind(record.execution_id.map(ExecutionId::into_uuid)).bind(record.occurred_at.into_datetime())
            .execute(&mut *connection).await.map_err(map_sqlx)?;
        Ok(())
    }

    /// Reads one fact inside a caller-managed transaction, enforcing ownership in SQL.
    pub async fn get_in(
        connection: &mut PgConnection,
        tenant: TenantId,
        id: UsageRecordId,
    ) -> Result<Option<UsageRecord>> {
        let row = sqlx::query("SELECT id, tenant_id, organization_id, project_id, resource, quantity, source_id, dimensions, execution_id, occurred_at FROM usage_records WHERE tenant_id=$1 AND id=$2")
            .bind(tenant.into_uuid()).bind(id.into_uuid()).fetch_optional(&mut *connection).await.map_err(map_sqlx)?;
        row.as_ref().map(row_to_record).transpose()
    }

    /// Bounded descending page of facts in `[from,to)` inside a caller transaction.
    pub async fn list_in(
        connection: &mut PgConnection,
        tenant: TenantId,
        from: Timestamp,
        to: Timestamp,
        limit: i64,
    ) -> Result<Vec<UsageRecord>> {
        Self::list_page_in(
            connection,
            tenant,
            from,
            to,
            None,
            u16::try_from(limit.clamp(1, 1000)).unwrap_or(1000),
        )
        .await
    }

    /// Keyset page inside a caller-managed transaction. Fetches at most `limit + 1` records so callers can determine whether another page exists.
    pub async fn list_page_in(
        connection: &mut PgConnection,
        tenant: TenantId,
        from: Timestamp,
        to: Timestamp,
        after: Option<UsageCursor>,
        limit: u16,
    ) -> Result<Vec<UsageRecord>> {
        let (cursor_time, cursor_id) = after
            .map(|cursor| {
                (
                    Some(cursor.occurred_at.into_datetime()),
                    Some(cursor.id.into_uuid()),
                )
            })
            .unwrap_or((None, None));
        let query_limit = i64::from(limit.clamp(1, 1000)) + 1;
        let rows = sqlx::query("SELECT id, tenant_id, organization_id, project_id, resource, quantity, source_id, dimensions, execution_id, occurred_at FROM usage_records WHERE tenant_id=$1 AND occurred_at >= $2 AND occurred_at < $3 AND ($4::timestamptz IS NULL OR (occurred_at,id) < ($4,$5)) ORDER BY occurred_at DESC, id DESC LIMIT $6")
            .bind(tenant.into_uuid()).bind(from.into_datetime()).bind(to.into_datetime()).bind(cursor_time).bind(cursor_id).bind(query_limit)
            .fetch_all(&mut *connection).await.map_err(map_sqlx)?;
        rows.iter().map(row_to_record).collect()
    }
}

fn row_to_record(row: &sqlx::postgres::PgRow) -> Result<UsageRecord> {
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let organization: uuid::Uuid = row.try_get("organization_id").map_err(map_sqlx)?;
    let project: Option<uuid::Uuid> = row.try_get("project_id").map_err(map_sqlx)?;
    let resource: String = row.try_get("resource").map_err(map_sqlx)?;
    let quantity: i64 = row.try_get("quantity").map_err(map_sqlx)?;
    let actor_id: Option<String> = row.try_get("source_id").map_err(map_sqlx)?;
    let dimensions: serde_json::Value = row.try_get("dimensions").map_err(map_sqlx)?;
    let execution: Option<uuid::Uuid> = row.try_get("execution_id").map_err(map_sqlx)?;
    let occurred: chrono::DateTime<chrono::Utc> = row.try_get("occurred_at").map_err(map_sqlx)?;
    let mut metadata = dimensions.as_object().cloned().unwrap_or_default();
    let actor_kind = metadata
        .remove("_actor_kind")
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "system".to_owned())
        .parse::<ActorKind>()
        .map_err(|_| AppError::internal("stored usage actor kind is invalid"))?;
    let actor = ActorRef::new(actor_kind, actor_id.unwrap_or_else(|| "system".to_owned()))?;
    let dimension = resource
        .parse::<QuotaDimension>()
        .map_err(|_| AppError::internal("stored usage dimension is invalid"))?;
    Ok(UsageRecord {
        id: UsageRecordId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        organization_id: OrganizationId::from_uuid(organization),
        project_id: project.map(ProjectId::from_uuid),
        actor,
        dimension,
        quantity: u64::try_from(quantity)
            .map_err(|_| AppError::internal("stored usage quantity is invalid"))?,
        execution_id: execution.map(ExecutionId::from_uuid),
        metadata,
        occurred_at: Timestamp::from(occurred),
    })
}

#[async_trait]
impl UsageStorePort for PostgresUsageStore {
    async fn append(&self, record: &UsageRecord) -> Result<()> {
        let mut executor = scoped_request::connection().await?;
        Self::append_in(executor.connection().as_mut(), record).await
    }

    async fn get(&self, id: UsageRecordId) -> Result<Option<UsageRecord>> {
        let tenant = scoped_request::current_scope()?.tenant_id();
        let mut executor = scoped_request::connection().await?;
        Self::get_in(executor.connection().as_mut(), tenant, id).await
    }

    async fn list(
        &self,
        tenant: TenantId,
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<UsageRecord>> {
        let current = scoped_request::current_scope()?;
        if current.tenant_id() != tenant {
            return Err(AppError::not_found("usage", tenant.to_string()));
        }
        let mut executor = scoped_request::connection().await?;
        Self::list_in(executor.connection().as_mut(), tenant, from, to, 1000).await
    }
}

#[async_trait]
impl UsageReadPort for PostgresUsageStore {
    async fn totals(
        &self,
        tenant: TenantId,
        project: Option<ProjectId>,
        from: Timestamp,
        to: Timestamp,
        dimension: Option<&str>,
    ) -> Result<Vec<(String, u64)>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::not_found("usage", tenant.to_string()));
        }
        let mut executor = scoped_request::connection().await?;
        let rows = sqlx::query("SELECT resource, COALESCE(sum(quantity),0)::bigint AS total FROM usage_records WHERE tenant_id=$1 AND occurred_at >= $2 AND occurred_at < $3 AND ($4::text IS NULL OR resource=$4) AND ($5::uuid IS NULL OR project_id=$5) GROUP BY resource ORDER BY resource")
            .bind(tenant.into_uuid()).bind(from.into_datetime()).bind(to.into_datetime()).bind(dimension).bind(project.map(ProjectId::into_uuid))
            .fetch_all(executor.connection().as_mut()).await.map_err(map_sqlx)?;
        rows.iter()
            .map(|row| {
                let name: String = row.try_get("resource").map_err(map_sqlx)?;
                let total: i64 = row.try_get("total").map_err(map_sqlx)?;
                Ok((
                    name,
                    u64::try_from(total)
                        .map_err(|_| AppError::internal("usage total is invalid"))?,
                ))
            })
            .collect()
    }

    async fn quota_snapshots(&self, tenant: TenantId) -> Result<Vec<(Quota, u64)>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::not_found("quota", tenant.to_string()));
        }
        let mut executor = scoped_request::connection().await?;
        let rows = sqlx::query("SELECT id, dimension, period, max_value, enforcement, created_at, updated_at FROM quota_definitions WHERE tenant_id=$1 ORDER BY dimension, period")
            .bind(tenant.into_uuid()).fetch_all(executor.connection().as_mut()).await.map_err(map_sqlx)?;
        let now = Timestamp::now();
        let mut snapshots = Vec::with_capacity(rows.len());
        for row in rows {
            let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
            let dimension: String = row.try_get("dimension").map_err(map_sqlx)?;
            let period: String = row.try_get("period").map_err(map_sqlx)?;
            let limit: i64 = row.try_get("max_value").map_err(map_sqlx)?;
            let enforcement: String = row.try_get("enforcement").map_err(map_sqlx)?;
            let dimension = dimension
                .parse::<QuotaDimension>()
                .map_err(|_| AppError::internal("stored quota dimension is invalid"))?;
            let period = period
                .parse::<QuotaPeriod>()
                .map_err(|_| AppError::internal("stored quota period is invalid"))?;
            let enforcement = enforcement
                .parse::<EnforcementMode>()
                .map_err(|_| AppError::internal("stored quota enforcement mode is invalid"))?;
            let window_seconds = match period {
                QuotaPeriod::PerMinute => Some(60_i64),
                QuotaPeriod::Hourly => Some(3_600_i64),
                QuotaPeriod::Daily => Some(86_400_i64),
                QuotaPeriod::Monthly => Some(2_592_000_i64),
                QuotaPeriod::Unbounded => None,
            };
            let bucket = window_seconds
                .map(|window| now.to_unix_seconds().div_euclid(window.max(1)))
                .unwrap_or(0);
            let period_started_at = window_seconds
                .and_then(|window| Timestamp::from_unix_seconds(bucket.saturating_mul(window)).ok())
                .unwrap_or(now);
            let created_at = Timestamp::from(
                row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")
                    .map_err(map_sqlx)?,
            );
            let updated_at = Timestamp::from(
                row.try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
                    .map_err(map_sqlx)?,
            );
            let quota = Quota {
                id: mas_common::ids::QuotaId::from_uuid(id),
                tenant_id: tenant,
                dimension,
                period,
                limit: u64::try_from(limit)
                    .map_err(|_| AppError::internal("stored quota limit is invalid"))?,
                current_usage: 0,
                enforcement,
                period_started_at,
                created_at,
                updated_at,
            };
            let value: Option<i64> = sqlx::query_scalar("SELECT value FROM quota_counters WHERE tenant_id=$1 AND dimension=$2 AND bucket=$3")
                .bind(tenant.into_uuid()).bind(dimension.as_str()).bind(bucket).fetch_optional(executor.connection().as_mut()).await.map_err(map_sqlx)?;
            let used = value.unwrap_or(0);
            snapshots.push((
                quota,
                u64::try_from(used)
                    .map_err(|_| AppError::internal("stored quota counter is invalid"))?,
            ));
        }
        Ok(snapshots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_hydration_requires_known_dimensions_and_nonnegative_quantity() {
        assert!("tokens".parse::<QuotaDimension>().is_ok());
        assert!("unknown".parse::<QuotaDimension>().is_err());
        assert!(u64::try_from(-1_i64).is_err());
    }
}
