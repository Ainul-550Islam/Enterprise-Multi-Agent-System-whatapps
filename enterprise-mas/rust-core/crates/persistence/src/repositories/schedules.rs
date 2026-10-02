//! `schedules` + `schedule_runs` repositories.
//!
//! The application's `ScheduleStorePort` (CRUD surface) and the scheduler's
//! own `scheduling::store::ScheduleStorePort` (due-scan + run-journal
//! surface) are implemented on the same store so write-back and journal
//! share one row view.
//!
//! The domain `Schedule` aggregate does NOT carry an organization id (it is
//! tenant-scoped by design), yet the schema materializes `organization_id
//! NOT NULL`. Resolution happens inside SQL (`INSERT … SELECT … FROM
//! tenants`) — the schema's FK keeps the two in lockstep, and an ambiguous
//! tenant surfaces as the FK/insert judgment, never a guess in Rust.

use mas_common::error::AppError;
use mas_common::ids::{ScheduleId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::schedule::Schedule;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::ScheduleRow;

/// Full projected columns for schedule queries.
pub const SCHEDULE_COLUMNS: &str = "id, tenant_id, organization_id, name, target, rule, \
     status, timezone, next_run_at, last_run_at, created_at, updated_at";

/// Due-scan projection: the scheduler's hot path (oldest first, bounded).
const LIST_DUE_SQL: &str = "SELECT id, tenant_id, organization_id, name, target, rule, \
     status, timezone, next_run_at, last_run_at, created_at, updated_at
     FROM schedules
     WHERE status = 'active' AND next_run_at IS NOT NULL AND next_run_at <= $1
     ORDER BY next_run_at ASC
     LIMIT $2";

const RECORD_RUN_SQL: &str = "INSERT INTO schedule_runs (
        id, tenant_id, schedule_id, planned_at, outcome, detail
    )
    SELECT $1, s.tenant_id, s.id, $3, 'dispatched', '{}'
    FROM schedules s WHERE s.id = $2
    ON CONFLICT (schedule_id, planned_at) DO NOTHING";

const SAVE_SQL: &str = "INSERT INTO schedules (
        id, tenant_id, organization_id, name, target, rule, status, timezone,
        next_run_at, last_run_at, created_at, updated_at
    )
    SELECT $1, $2, t.organization_id, $3, $4, $5, $6, $7, $8, $9, $10, $11
    FROM tenants t WHERE t.id = $2
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        target = EXCLUDED.target,
        rule = EXCLUDED.rule,
        status = EXCLUDED.status,
        next_run_at = EXCLUDED.next_run_at,
        last_run_at = EXCLUDED.last_run_at,
        updated_at = EXCLUDED.updated_at";

fn fetch_schedule(pg_row: sqlx::postgres::PgRow) -> Result<ScheduleRow> {
    Ok(ScheduleRow {
        id: pg_row.try_get("id").map_err(map_sqlx)?,
        tenant_id: pg_row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: pg_row.try_get("organization_id").map_err(map_sqlx)?,
        name: pg_row.try_get("name").map_err(map_sqlx)?,
        target: pg_row.try_get("target").map_err(map_sqlx)?,
        rule: pg_row.try_get("rule").map_err(map_sqlx)?,
        status: pg_row.try_get("status").map_err(map_sqlx)?,
        timezone: pg_row.try_get("timezone").map_err(map_sqlx)?,
        next_run_at: pg_row.try_get("next_run_at").map_err(map_sqlx)?,
        last_run_at: pg_row.try_get("last_run_at").map_err(map_sqlx)?,
        created_at: pg_row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: pg_row.try_get("updated_at").map_err(map_sqlx)?,
    })
}

/// PostgreSQL store for `schedules` + `schedule_runs`.
#[derive(Debug, Clone)]
pub struct ScheduleStore {
    pool: PgPool,
}

impl ScheduleStore {
    /// Binds a store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates or rewrites a schedule row. The organization link resolves
    /// via the tenant FK inside the statement (see module docs).
    pub async fn save(&self, schedule: &Schedule) -> Result<()> {
        let row = ScheduleRow::from_domain(schedule, Uuid::nil())?;
        let result = sqlx::query(SAVE_SQL)
            .bind(row.id)
            .bind(row.tenant_id)
            .bind(&row.name)
            .bind(&row.target)
            .bind(&row.rule)
            .bind(&row.status)
            .bind(&row.timezone)
            .bind(row.next_run_at)
            .bind(row.last_run_at)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|error| match error {
                sqlx::Error::Database(db) if db.is_unique_violation() => {
                    AppError::conflict("a schedule with this name already exists in the tenant")
                },
                other => map_sqlx(other),
            })?;
        if result.rows_affected() != 1 {
            // INSERT … SELECT with zero SELECT rows = the tenant id was
            // unknown; say so instead of generic "0 rows affected".
            return Err(AppError::validation(format!(
                "schedule '{}' references an unknown tenant",
                row.name
            )));
        }
        Ok(())
    }

    /// Loads one schedule by primary key.
    pub async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>> {
        let row = sqlx::query(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(fetch_schedule)
            .transpose()?
            .map(ScheduleRow::into_domain)
            .transpose()
    }

    /// Lists schedules in a tenant (name order).
    pub async fn list(&self, tenant: TenantId) -> Result<Vec<Schedule>> {
        let rows = sqlx::query(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE tenant_id = $1 ORDER BY name"
        ))
        .bind(Uuid::from(tenant))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_schedule)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(ScheduleRow::into_domain)
            .collect()
    }

    /// Due-scan with the documented semantics (active rows whose next plan
    /// is at or before `now`, oldest first, bounded).
    pub async fn list_due(&self, now: &Timestamp, limit: usize) -> Result<Vec<Schedule>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(LIST_DUE_SQL)
            .bind(now.into_datetime())
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_schedule)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(ScheduleRow::into_domain)
            .collect()
    }

    /// Journals a planned tick. `false` = the unique constraint reports
    /// another replica already recorded it (duplicate-fire guard).
    pub async fn record_run(&self, schedule_id: ScheduleId, planned_at: Timestamp) -> Result<bool> {
        let result = sqlx::query(RECORD_RUN_SQL)
            .bind(Uuid::now_v7())
            // schedule_runs carries tenant too; deriving it via a subselect
            // keeps replica loops pure (no extra read round-trip).
            .bind(Uuid::from(schedule_id))
            .bind(planned_at.into_datetime())
            .execute(&self.pool)
            .await;
        match result {
            Ok(done) => {
                if done.rows_affected() == 1 {
                    return Ok(true);
                }
                // Zero rows is ambiguous: duplicate tick vs unknown schedule.
                // Resolve it honestly — replicas depend on the distinction.
                let exists: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schedules WHERE id = $1)")
                        .bind(Uuid::from(schedule_id))
                        .fetch_one(&self.pool)
                        .await
                        .map_err(map_sqlx)?;
                if exists {
                    Ok(false)
                } else {
                    Err(AppError::not_found("schedule", schedule_id.to_string()))
                }
            },
            Err(err) => Err(map_sqlx(err)),
        }
    }
}

#[async_trait::async_trait]
impl mas_scheduling::store::ScheduleStorePort for ScheduleStore {
    async fn list_due(&self, now: &Timestamp, limit: usize) -> Result<Vec<Schedule>> {
        ScheduleStore::list_due(self, now, limit).await
    }

    async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>> {
        ScheduleStore::get(self, id).await
    }

    async fn save(&self, schedule: &Schedule) -> Result<()> {
        ScheduleStore::save(self, schedule).await
    }

    async fn record_run(&self, schedule_id: ScheduleId, planned_at: Timestamp) -> Result<bool> {
        ScheduleStore::record_run(self, schedule_id, planned_at).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn due_scan_is_active_only_oldest_first_bounded() {
        for token in [
            "status = 'active'",
            "next_run_at <= $1",
            "ORDER BY next_run_at ASC",
            "LIMIT $2",
        ] {
            assert!(LIST_DUE_SQL.contains(token), "missing {token}");
        }
    }

    #[test]
    fn run_journal_is_uniquely_guarded() {
        assert!(RECORD_RUN_SQL.contains("ON CONFLICT (schedule_id, planned_at) DO NOTHING"));
        assert!(!RECORD_RUN_SQL.contains("DO UPDATE"));
    }

    #[test]
    fn save_resolves_organization_through_the_tenant_fk() {
        assert!(SAVE_SQL.contains("FROM tenants t WHERE t.id = $2"));
        assert!(SAVE_SQL.contains("ON CONFLICT (id) DO UPDATE"));
    }
}
