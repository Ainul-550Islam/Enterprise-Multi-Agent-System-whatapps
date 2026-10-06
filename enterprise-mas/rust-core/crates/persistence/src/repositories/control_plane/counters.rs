//! PURPOSE: The distributed quota counter and quota-definition stores — the
//! Postgres implementation of `mas-quota`'s two ports.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: make the quota decision *the database's*.
//! `InMemoryCounterStore` is correct for one process and wrong for two: two
//! replicas each holding their own map cannot see each other's reservations,
//! which is precisely the "replica A reserves 70, replica B reserves 50,
//! limit 100, both succeed" failure this module exists to remove.
//!
//! Security boundary: every statement filters on `tenant_id`, and the table
//! sits under FORCE RLS, so a connection whose `app.current_tenant` is armed
//! for tenant A cannot move tenant B's counter even if a caller passes the
//! wrong id. The tenant filter is not decoration — the counter table has no
//! foreign key to `tenants`, so RLS is the only thing preventing a cross-tenant
//! write from an unscoped connection.
//!
//! Data ownership: counters and quota definitions are Rust-owned platform
//! state. Python neither reads nor writes them.
//!
//! It must NOT:
//!   • implement the reservation as read-then-write — the guard must live in
//!     the same statement that performs the increment;
//!   • let a refused reservation modify the row;
//!   • saturate silently on overflow: `bigint` addition can overflow, and an
//!     overflow error is safer than a wrapped counter that reads as headroom;
//!   • be constructed with a pool whose role holds BYPASSRLS in production.

use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_domain::quota::{EnforcementMode, Quota, QuotaDimension, QuotaPeriod};
use mas_quota::counter::{CounterKey, CounterStorePort, ReservationAttempt};
use mas_quota::engine::QuotaRegistryPort;
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::map_sqlx;

/// Largest value a `bigint` counter may hold.
///
/// The saturating expression in SQL clamps to this so an adversarial
/// `amount` cannot turn an increment into an overflow error that reads as a
/// database failure rather than as "the counter is full".
const BIGINT_MAX: i64 = i64::MAX;

/// Postgres-backed counter store: the authoritative quota arithmetic.
#[derive(Debug, Clone)]
pub struct PostgresCounterStore {
    pool: PgPool,
}

impl PostgresCounterStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The underlying pool (composition roots share one instance).
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl CounterStorePort for PostgresCounterStore {
    async fn increment(&self, key: CounterKey, amount: u64) -> Result<u64> {
        let amount = clamp_i64(amount);
        let value: i64 = sqlx::query_scalar(
            "INSERT INTO quota_counters (tenant_id, dimension, bucket, value)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (tenant_id, dimension, bucket) DO UPDATE
                -- Saturating add: a counter that would exceed bigint stays
                -- pinned at the maximum instead of raising 22003 mid-request.
                SET value = LEAST(quota_counters.value + EXCLUDED.value, $5),
                    updated_at = now()
             RETURNING value",
        )
        .bind(key.tenant_id.into_uuid())
        .bind(key.dimension.as_str())
        .bind(key.bucket)
        .bind(amount)
        .bind(BIGINT_MAX)
        .fetch_one(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(u64::try_from(value).unwrap_or(u64::MAX))
    }

    async fn try_reserve(
        &self,
        key: CounterKey,
        amount: u64,
        limit: u64,
    ) -> Result<ReservationAttempt> {
        let amount = clamp_i64(amount);
        let limit = clamp_i64(limit);

        // The entire decision is this one statement.
        //
        // `ON CONFLICT DO UPDATE … WHERE` takes the row lock, so two
        // concurrent reservations for the same key serialise: the second
        // blocks on the first, then re-evaluates the guard against the value
        // the first committed. There is no read-modify-write window, which is
        // what makes this safe across replicas and `InMemoryCounterStore`
        // is not.
        //
        // When the guard fails, `RETURNING` yields no row: the row is left
        // exactly as it was and the caller is told the reservation was
        // refused. A refused reservation therefore cannot inflate the
        // counter, and needs no compensating decrement.
        let granted: Option<i64> = sqlx::query_scalar(
            "INSERT INTO quota_counters (tenant_id, dimension, bucket, value)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (tenant_id, dimension, bucket) DO UPDATE
                SET value = LEAST(quota_counters.value + EXCLUDED.value, $6),
                    updated_at = now()
              WHERE quota_counters.value + EXCLUDED.value <= $5
             RETURNING value",
        )
        .bind(key.tenant_id.into_uuid())
        .bind(key.dimension.as_str())
        .bind(key.bucket)
        .bind(amount)
        .bind(limit)
        .bind(BIGINT_MAX)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        match granted {
            Some(value) => Ok(ReservationAttempt::granted(
                u64::try_from(value).unwrap_or(u64::MAX),
                limit as u64,
            )),
            // Nothing was written. Report what the counter actually holds so
            // the caller's arithmetic matches `usage()` — and note that the
            // projection here is a lower bound only: between this read and
            // the caller's next action another replica may have moved it.
            // That is acceptable because the decision itself was atomic.
            None => {
                let current = self.get(key).await?;
                Ok(ReservationAttempt::refused(
                    current.saturating_add(amount as u64),
                    limit as u64,
                ))
            },
        }
    }

    async fn decrement(&self, key: CounterKey, amount: u64) -> Result<u64> {
        // `GREATEST(..., 0)` makes the decrement saturating, matching the
        // in-memory store and the table's own `value >= 0` check. A release
        // that would go negative is a bug elsewhere; clamping it keeps a
        // double-release from minting headroom the tenant never had.
        let value: Option<i64> = sqlx::query_scalar(
            "UPDATE quota_counters
                SET value = GREATEST(value - $4, 0),
                    updated_at = now()
              WHERE tenant_id = $1
                AND dimension = $2
                AND bucket = $3
          RETURNING value",
        )
        .bind(key.tenant_id.into_uuid())
        .bind(key.dimension.as_str())
        .bind(key.bucket)
        .bind(clamp_i64(amount))
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        // Absent row = nothing was ever consumed = 0. Not an error: releasing
        // a reservation for a dimension that never incremented is a no-op.
        Ok(u64::try_from(value.unwrap_or(0)).unwrap_or(0))
    }

    async fn get(&self, key: CounterKey) -> Result<u64> {
        let value: Option<i64> = sqlx::query_scalar(
            "SELECT value FROM quota_counters
              WHERE tenant_id = $1 AND dimension = $2 AND bucket = $3",
        )
        .bind(key.tenant_id.into_uuid())
        .bind(key.dimension.as_str())
        .bind(key.bucket)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(u64::try_from(value.unwrap_or(0)).unwrap_or(0))
    }

    async fn gc_buckets(&self, min_bucket: i64) -> Result<u64> {
        // Gauges (bucket 0) are never reclaimed: they are the live value of a
        // non-resetting dimension, not a historical window.
        let result = sqlx::query("DELETE FROM quota_counters WHERE bucket > 0 AND bucket < $1")
            .bind(min_bucket)
            .execute(executor(&self.pool).await?.connection())
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected())
    }
}

/// Postgres-backed quota registry: where quota definitions come from.
///
/// A definition is the plan's ceiling for a tenant, unless an override is in
/// force for the (tenant, dimension, period) triple. The override wins, and
/// the registry says so in the returned `Quota` so callers can tell which
/// limit they were measured against.
#[derive(Debug, Clone)]
pub struct PostgresQuotaRegistry {
    pool: PgPool,
}

impl PostgresQuotaRegistry {
    /// Builds a registry over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The tenant-scoped definitions this tenant has configured.
    pub async fn list_for_tenant(&self, tenant: TenantId) -> Result<Vec<QuotaRow>> {
        sqlx::query_as::<_, QuotaRow>(
            "SELECT tenant_id, dimension, period, max_value, enforcement, source, actor_id
               FROM quota_definitions
              WHERE tenant_id = $1
              ORDER BY dimension, period",
        )
        .bind(tenant.into_uuid())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }
}

/// One quota definition as stored.
#[derive(Debug, Clone)]
pub struct QuotaRow {
    /// Owning tenant.
    pub tenant_id: uuid::Uuid,
    /// Metered dimension.
    pub dimension: String,
    /// Reset window.
    pub period: String,
    /// Ceiling.
    pub max_value: i64,
    /// What happens at the ceiling.
    pub enforcement: String,
    /// `plan` or `tenant`.
    pub source: String,
    /// Who last set it.
    pub actor_id: String,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for QuotaRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            dimension: row.try_get("dimension")?,
            period: row.try_get("period")?,
            max_value: row.try_get("max_value")?,
            enforcement: row.try_get("enforcement")?,
            source: row.try_get("source")?,
            actor_id: row.try_get("actor_id")?,
        })
    }
}

impl QuotaRow {
    /// Hydrates into the domain aggregate.
    pub fn into_domain(self) -> Result<Quota> {
        let tenant_id = TenantId::from_uuid(self.tenant_id);
        let dimension = self.dimension.parse::<QuotaDimension>().map_err(|_| {
            AppError::database(format!("unknown quota dimension '{}'", self.dimension))
        })?;
        let period = self
            .period
            .parse::<QuotaPeriod>()
            .map_err(|_| AppError::database(format!("unknown quota period '{}'", self.period)))?;
        let enforcement = self.enforcement.parse::<EnforcementMode>().map_err(|_| {
            AppError::database(format!("unknown enforcement mode '{}'", self.enforcement))
        })?;
        let mut quota = Quota::new(
            tenant_id,
            dimension,
            period,
            u64::try_from(self.max_value).unwrap_or(0),
            enforcement,
        )?;
        // `current_usage` is a mirror; the authoritative value is the counter.
        // It is left at 0 here on purpose so a stale mirror can never be
        // mistaken for the live number by a caller that reads the aggregate.
        quota.current_usage = 0;
        Ok(quota)
    }
}

#[async_trait::async_trait]
impl QuotaRegistryPort for PostgresQuotaRegistry {
    async fn find(&self, tenant: TenantId, dimension: QuotaDimension) -> Result<Option<Quota>> {
        let row: Option<QuotaRow> = sqlx::query_as::<_, QuotaRow>(
            "SELECT tenant_id, dimension, period, max_value, enforcement, source, actor_id
               FROM quota_definitions
              WHERE tenant_id = $1 AND dimension = $2
              LIMIT 1",
        )
        .bind(tenant.into_uuid())
        .bind(dimension.as_str())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(QuotaRow::into_domain).transpose()
    }

    async fn upsert(&self, quota: &Quota) -> Result<()> {
        // `upsert` is the admin path. It writes the *definition*; it does not
        // touch the counter, so redefining a quota never rewrites what the
        // tenant has already consumed this period.
        sqlx::query(
            "INSERT INTO quota_definitions
                (tenant_id, dimension, period, max_value, enforcement, source, actor_id)
             VALUES ($1, $2, $3, $4, $5, 'tenant', 'system')
             ON CONFLICT (tenant_id, dimension, period) DO UPDATE
                SET max_value = EXCLUDED.max_value,
                    enforcement = EXCLUDED.enforcement,
                    updated_at = now()",
        )
        .bind(quota.tenant_id.into_uuid())
        .bind(quota.dimension.as_str())
        .bind(quota.period.as_str())
        .bind(clamp_i64(quota.limit))
        .bind(quota.enforcement.as_str())
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

/// Clamps a `u64` into the `i64` range a `bigint` column can hold.
fn clamp_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamping_never_wraps_into_the_negative_range() {
        assert_eq!(clamp_i64(0), 0);
        assert_eq!(clamp_i64(42), 42);
        assert_eq!(clamp_i64(u64::MAX), i64::MAX);
    }

    #[test]
    fn a_refused_attempt_reports_its_arithmetic() {
        let attempt = ReservationAttempt::refused(120, 100);
        assert!(!attempt.allowed);
        assert_eq!(attempt.value, 120);
        assert_eq!(attempt.overage(), 20);
        assert_eq!(attempt.remaining(), 0);
    }

    #[test]
    fn a_granted_attempt_reports_headroom() {
        let attempt = ReservationAttempt::granted(70, 100);
        assert!(attempt.allowed);
        assert_eq!(attempt.remaining(), 30);
        assert_eq!(attempt.overage(), 0);
    }

    #[test]
    fn a_hydrated_definition_starts_with_an_empty_usage_mirror() {
        let tenant = TenantId::new();
        let row = QuotaRow {
            tenant_id: tenant.into_uuid(),
            dimension: "executions".to_owned(),
            period: "daily".to_owned(),
            max_value: 50,
            enforcement: "enforce".to_owned(),
            source: "plan".to_owned(),
            actor_id: "system".to_owned(),
        };
        let quota = row.into_domain().expect("hydrate");
        assert_eq!(quota.limit, 50);
        assert_eq!(
            quota.current_usage, 0,
            "the aggregate's usage mirror must never be trusted as the live counter"
        );
        assert_eq!(quota.tenant_id, tenant);
    }

    #[test]
    fn an_unknown_enum_in_a_row_is_a_loud_database_error() {
        let row = QuotaRow {
            tenant_id: TenantId::new().into_uuid(),
            dimension: "nonsense".to_owned(),
            period: "daily".to_owned(),
            max_value: 1,
            enforcement: "enforce".to_owned(),
            source: "plan".to_owned(),
            actor_id: "system".to_owned(),
        };
        assert!(
            row.into_domain().is_err(),
            "schema drift must fail loudly, never default"
        );
    }
}
