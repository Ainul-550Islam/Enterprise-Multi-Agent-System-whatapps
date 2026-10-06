//! PURPOSE: The one authoritative usage write path and its query surface.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: be the *only* place a usage record is created.
//! Everything that consumes a metered resource — the execution pipeline, the
//! worker, the bridge that reports model usage from Python — funnels through
//! [`PostgresUsageLedger::record`], so there is one shape, one attribution
//! rule and one idempotency dimension.
//!
//! Security boundary: tenant-scoped and RLS-protected. A usage write for a
//! tenant the connection is not scoped to inserts nothing (RLS `WITH CHECK`
//! is not present, but the `tenant_id` is bound from server-side scope, never
//! from a request body, and the SELECT policies confine reads).
//!
//! Data ownership: **Rust-owned platform accounting.** The Python orchestrator
//! reports what the model told it; Rust decides what becomes a usage record.
//! Python never writes this table.
//!
//! It must NOT:
//!   • fabricate usage — a record is written from an observed quantity, and
//!     `quantity = 0` is refused rather than stored as a zero;
//!   • double-meter the same logical work: `(task_id, resource)` is unique,
//!     and `metering_key` is unique per tenant for usage with no task;
//!   • equate "execution failed" with "usage is zero" — a provider that
//!     consumed tokens before failing still produced real usage;
//!   • store cost as a float. Cost is integer micro-units; 0.1 + 0.2 is not
//!     0.3 and an invoice that is wrong in the third decimal is a dispute.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, OrganizationId, ProjectId, TaskAttemptId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::is_unique_violation;
use crate::error::map_sqlx;

/// One usage record as stored.
#[derive(Debug, Clone)]
pub struct UsageRow {
    /// Record id.
    pub id: uuid::Uuid,
    /// Owning tenant.
    pub tenant_id: uuid::Uuid,
    /// Owning organization.
    pub organization_id: Option<uuid::Uuid>,
    /// Project, when attributable.
    pub project_id: Option<uuid::Uuid>,
    /// Metered resource.
    pub resource: String,
    /// Quantity consumed.
    pub quantity: i64,
    /// Unit label.
    pub unit: String,
    /// Legacy source label from 0008.
    pub source_id: Option<String>,
    /// Non-secret detail metadata.
    pub dimensions: serde_json::Value,
    /// Causing execution.
    pub execution_id: Option<uuid::Uuid>,
    /// Causing task.
    pub task_id: Option<uuid::Uuid>,
    /// Causing attempt.
    pub task_attempt_id: Option<uuid::Uuid>,
    /// Model that consumed the resource.
    pub model: Option<String>,
    /// Provider that served it.
    pub provider: Option<String>,
    /// Input-side quantity (prompt tokens, request bytes).
    pub input_quantity: Option<i64>,
    /// Output-side quantity (completion tokens, response bytes).
    pub output_quantity: Option<i64>,
    /// Wall-clock duration.
    pub duration_ms: Option<i64>,
    /// Cost in integer micro-units of `cost_currency`.
    pub cost_micros: Option<i64>,
    /// ISO-4217-ish currency label for `cost_micros`.
    pub cost_currency: Option<String>,
    /// Whether the quantity is model-reported rather than measured.
    pub estimated: bool,
    /// Idempotency key for usage with no task.
    pub metering_key: Option<String>,
    /// When it was consumed.
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    /// When it was recorded.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for UsageRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            tenant_id: row.try_get("tenant_id")?,
            organization_id: row.try_get("organization_id")?,
            project_id: row.try_get("project_id")?,
            resource: row.try_get("resource")?,
            quantity: row.try_get("quantity")?,
            unit: row.try_get("unit")?,
            source_id: row.try_get("source_id")?,
            dimensions: row.try_get("dimensions")?,
            execution_id: row.try_get("execution_id")?,
            task_id: row.try_get("task_id")?,
            task_attempt_id: row.try_get("task_attempt_id")?,
            model: row.try_get("model")?,
            provider: row.try_get("provider")?,
            input_quantity: row.try_get("input_quantity")?,
            output_quantity: row.try_get("output_quantity")?,
            duration_ms: row.try_get("duration_ms")?,
            cost_micros: row.try_get("cost_micros")?,
            cost_currency: row.try_get("cost_currency")?,
            estimated: row.try_get("estimated")?,
            metering_key: row.try_get("metering_key")?,
            occurred_at: row.try_get("occurred_at")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

/// The canonical usage write path.
#[derive(Debug, Clone)]
pub struct PostgresUsageLedger {
    pool: PgPool,
}

impl PostgresUsageLedger {
    /// Builds a ledger over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// One usage observation to record.
///
/// Every field is either an attribution dimension or an observed quantity.
/// There is deliberately no "amount to bill" — that is derived downstream from
/// these facts, so a pricing change cannot retroactively rewrite history.
#[derive(Debug, Clone)]
pub struct UsageWrite {
    /// Owning tenant (server-side scope; never caller-supplied).
    pub tenant_id: TenantId,
    /// Owning organization, when known.
    pub organization_id: Option<OrganizationId>,
    /// Project, when attributable.
    pub project_id: Option<ProjectId>,
    /// Metered dimension.
    pub dimension: QuotaDimension,
    /// Observed quantity (must be positive).
    pub quantity: u64,
    /// Causing execution.
    pub execution_id: Option<ExecutionId>,
    /// Causing task — part of the idempotency dimension.
    pub task_id: Option<TaskId>,
    /// Causing attempt.
    pub task_attempt_id: Option<TaskAttemptId>,
    /// Model identifier.
    pub model: Option<String>,
    /// Provider identifier.
    pub provider: Option<String>,
    /// Input-side quantity.
    pub input_quantity: Option<u64>,
    /// Output-side quantity.
    pub output_quantity: Option<u64>,
    /// Wall-clock duration.
    pub duration_ms: Option<u64>,
    /// Cost in micro-units.
    pub cost_micros: Option<i64>,
    /// Currency for `cost_micros`.
    pub cost_currency: Option<String>,
    /// Whether the quantity is model-reported.
    pub estimated: bool,
    /// Non-secret metadata (never credentials, never prompts).
    pub dimensions: serde_json::Value,
    /// Idempotency key for usage with no task.
    pub metering_key: Option<String>,
    /// When it was consumed.
    pub occurred_at: Timestamp,
}

impl UsageWrite {
    /// Validates the invariants the ledger refuses to guess at.
    pub fn validate(&self) -> Result<()> {
        if self.quantity == 0 {
            return Err(AppError::validation(
                "usage quantity must be positive; zero-quantity work is not a usage record",
            ));
        }
        if self.metering_key.is_none() && self.task_id.is_none() {
            return Err(AppError::validation(concat!(
                "usage must carry a task_id or a metering_key: without one, a ",
                "duplicate delivery cannot be recognised and the same work ",
                "would be metered twice"
            )));
        }
        Ok(())
    }
}

impl PostgresUsageLedger {
    /// Records one usage observation.
    ///
    /// Returns `false` when the record already existed — a duplicate delivery
    /// of the same logical work. That is the exact-once guarantee, and it is
    /// enforced by the unique index rather than by a `SELECT` first: two
    /// concurrent duplicates would both read "absent" and both insert.
    pub async fn record(&self, write: &UsageWrite) -> Result<bool> {
        write.validate()?;
        let result = sqlx::query(
            "INSERT INTO usage_records
                (tenant_id, organization_id, project_id, resource, quantity, unit,
                 source_id, dimensions, execution_id, task_id, task_attempt_id,
                 model, provider, input_quantity, output_quantity, duration_ms,
                 cost_micros, cost_currency, estimated, metering_key, occurred_at)
             VALUES
                ($1,$2,$3,$4,$5,'units',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
             ON CONFLICT DO NOTHING",
        )
        .bind(write.tenant_id.into_uuid())
        .bind(write.organization_id.map(OrganizationId::into_uuid))
        .bind(write.project_id.map(ProjectId::into_uuid))
        .bind(write.dimension.as_str())
        .bind(i64::try_from(write.quantity).unwrap_or(i64::MAX))
        .bind(write.execution_id.map(|id| id.to_string()))
        .bind(write.dimensions.clone())
        .bind(write.execution_id.map(ExecutionId::into_uuid))
        .bind(write.task_id.map(TaskId::into_uuid))
        .bind(write.task_attempt_id.map(TaskAttemptId::into_uuid))
        .bind(&write.model)
        .bind(&write.provider)
        .bind(
            write
                .input_quantity
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
        )
        .bind(
            write
                .output_quantity
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
        )
        .bind(
            write
                .duration_ms
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
        )
        .bind(write.cost_micros)
        .bind(&write.cost_currency)
        .bind(write.estimated)
        .bind(&write.metering_key)
        .bind(write.occurred_at.into_datetime())
        .execute(executor(&self.pool).await?.connection())
        .await;

        match result {
            Ok(done) => Ok(done.rows_affected() > 0),
            Err(error) => {
                // `ON CONFLICT DO NOTHING` covers both partial unique indexes,
                // so a duplicate normally arrives as zero affected rows. A
                // 23505 here means the constraint fired outside that path and
                // is still a duplicate, not a failure.
                if is_unique_violation(&error) {
                    Ok(false)
                } else {
                    Err(map_sqlx(error))
                }
            },
        }
    }

    /// Usage recorded for one execution, oldest first.
    pub async fn for_execution(
        &self,
        tenant: TenantId,
        execution: ExecutionId,
    ) -> Result<Vec<UsageRow>> {
        sqlx::query_as::<_, UsageRow>(
            "SELECT id, tenant_id, organization_id, project_id, resource, quantity, unit,
                    source_id, dimensions, execution_id, task_id, task_attempt_id, model,
                    provider, input_quantity, output_quantity, duration_ms, cost_micros,
                    cost_currency, estimated, metering_key, occurred_at, created_at
               FROM usage_records
              WHERE tenant_id = $1 AND execution_id = $2
              ORDER BY occurred_at, id",
        )
        .bind(tenant.into_uuid())
        .bind(execution.into_uuid())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }

    /// A tenant's usage in a half-open time window, newest first.
    pub async fn for_tenant_between(
        &self,
        tenant: TenantId,
        from: &Timestamp,
        to: &Timestamp,
        limit: i64,
    ) -> Result<Vec<UsageRow>> {
        sqlx::query_as::<_, UsageRow>(
            "SELECT id, tenant_id, organization_id, project_id, resource, quantity, unit,
                    source_id, dimensions, execution_id, task_id, task_attempt_id, model,
                    provider, input_quantity, output_quantity, duration_ms, cost_micros,
                    cost_currency, estimated, metering_key, occurred_at, created_at
               FROM usage_records
              WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
              ORDER BY occurred_at DESC
              LIMIT $4",
        )
        .bind(tenant.into_uuid())
        .bind(from.into_datetime())
        .bind(to.into_datetime())
        .bind(limit.max(1))
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }

    /// Per-dimension totals for one tenant in a window.
    ///
    /// Sums `quantity`, which is what quota is measured in. Cost is summed
    /// separately and only when every contributing row carries a currency —
    /// adding micro-units of two different currencies would produce a number
    /// that looks authoritative and means nothing.
    pub async fn totals_for_tenant_between(
        &self,
        tenant: TenantId,
        from: &Timestamp,
        to: &Timestamp,
    ) -> Result<Vec<UsageTotal>> {
        sqlx::query_as::<_, UsageTotal>(
            "SELECT resource,
                    sum(quantity)::bigint AS quantity,
                    sum(cost_micros) FILTER (WHERE cost_currency IS NOT NULL)::bigint
                        AS cost_micros,
                    min(cost_currency) FILTER (WHERE cost_currency IS NOT NULL) AS currency,
                    count(DISTINCT cost_currency) AS currencies
               FROM usage_records
              WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
              GROUP BY resource
              ORDER BY resource",
        )
        .bind(tenant.into_uuid())
        .bind(from.into_datetime())
        .bind(to.into_datetime())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }
}

/// One dimension's aggregated usage.
#[derive(Debug, Clone)]
pub struct UsageTotal {
    /// Metered resource.
    pub resource: String,
    /// Summed quantity.
    pub quantity: i64,
    /// Summed cost in micro-units, when a single currency is in play.
    pub cost_micros: Option<i64>,
    /// That currency.
    pub currency: Option<String>,
    /// How many distinct currencies contributed.
    pub currencies: i64,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for UsageTotal {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            resource: row.try_get("resource")?,
            quantity: row.try_get("quantity")?,
            cost_micros: row.try_get("cost_micros")?,
            currency: row.try_get("currency")?,
            currencies: row.try_get("currencies")?,
        })
    }
}

impl UsageTotal {
    /// The summed cost, refusing to add mixed currencies.
    ///
    /// `None` means the caller must handle the mixed case explicitly rather
    /// than read a total that silently assumes one currency.
    #[must_use]
    pub fn cost(&self) -> Option<(i64, &str)> {
        match (self.cost_micros, self.currency.as_deref(), self.currencies) {
            (Some(micros), Some(currency), 1) => Some((micros, currency)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write() -> UsageWrite {
        UsageWrite {
            tenant_id: TenantId::new(),
            organization_id: None,
            project_id: None,
            dimension: QuotaDimension::Tokens,
            quantity: 10,
            execution_id: None,
            task_id: None,
            task_attempt_id: None,
            model: None,
            provider: None,
            input_quantity: None,
            output_quantity: None,
            duration_ms: None,
            cost_micros: None,
            cost_currency: None,
            estimated: false,
            dimensions: serde_json::json!({}),
            metering_key: Some("k".to_owned()),
            occurred_at: Timestamp::now(),
        }
    }

    #[test]
    fn zero_quantity_is_refused_rather_than_stored() {
        let mut bad = write();
        bad.quantity = 0;
        assert!(
            bad.validate().is_err(),
            "a zero-quantity record would make 'no usage' and 'usage of zero' indistinguishable"
        );
    }

    #[test]
    fn usage_without_any_idempotency_dimension_is_refused() {
        let mut bad = write();
        bad.metering_key = None;
        bad.task_id = None;
        assert!(
            bad.validate().is_err(),
            "without a task or a metering key a duplicate would be metered twice"
        );
        let mut good = write();
        good.metering_key = None;
        good.task_id = Some(TaskId::new());
        assert!(good.validate().is_ok(), "a task id is a valid dimension");
    }

    #[test]
    fn mixed_currency_totals_are_not_summed() {
        let total = UsageTotal {
            resource: "tokens".to_owned(),
            quantity: 100,
            cost_micros: Some(500),
            currency: Some("USD".to_owned()),
            currencies: 2,
        };
        assert_eq!(
            total.cost(),
            None,
            "adding micro-units of two currencies produces a meaningless number"
        );
        let single = UsageTotal {
            currencies: 1,
            ..total
        };
        assert_eq!(single.cost(), Some((500, "USD")));
    }
}
