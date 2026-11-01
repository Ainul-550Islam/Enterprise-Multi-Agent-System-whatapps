//! PURPOSE: The `schedules` + `schedule_runs` repositories.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: persist schedules and the record of each run.
//! Every statement runs on the request's scoped transaction when one exists.
//!
//! Security boundary: the schedule row is the authority for which tenant a
//! fired run belongs to. The internal run endpoint resolves the schedule
//! under the resolved scope and compares the schedule's own `tenant_id`
//! against it, so a caller cannot name a tenant and borrow another tenant's
//! schedule.
//!
//! Two planes reach this store, and they reach different halves of it:
//!
//!   * the **request plane** uses the `&self` methods, which borrow the
//!     request's scoped transaction, so every row it sees is inside
//!     `app.current_tenant`;
//!   * the **scheduler plane** uses the `*_in` methods with an explicit
//!     connection. Its cross-tenant due-scan runs on the `mas_scheduler`
//!     connection (BYPASSRLS, SELECT on `schedules` only); everything after
//!     the scan runs on `mas_app` inside a scoped transaction for the tenant
//!     the due row named.
//!
//! The split is the point: one cross-tenant SELECT on one table is a far
//! smaller exemption than an entire tick loop running RLS-free.
//!
//! It must NOT:
//!   • look a schedule up outside a scope on the request plane;
//!   • expose the task template as caller-controllable — it comes from the
//!     stored row;
//!   • take a second scoped connection while one is already held. The
//!     request transaction sits behind a non-reentrant mutex, so nesting
//!     `connection()` calls inside one method is a deadlock, not a slowdown.

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
use sqlx::{PgConnection, PgPool, Row};
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
    ///
    /// The pool is not used to run statements: every query runs on the
    /// request's transaction, which is the only place row-level security is
    /// armed. It is retained for pool diagnostics and health probes, and
    /// deliberately has no code path that executes a query through it.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool handle, for diagnostics only.
    ///
    /// Never use this to run a tenant-touching statement: it has no scope, and
    /// under `FORCE ROW LEVEL SECURITY` that means it would return nothing.
    /// It exists so pool statistics can be reported.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ── Connection-taking primitives ────────────────────────────────────
    //
    // Every statement the store can run exists once, here, taking an explicit
    // connection. Two callers reach them:
    //
    //   * the request path, through the `&self` methods below, which borrow
    //     the request's scoped transaction;
    //   * the scheduler plane, through `SchedulerPlaneStore` in
    //     `mas-scheduler-service`, which borrows the plane's own connection
    //     for the cross-tenant due-scan and a scoped transaction for
    //     everything after it.
    //
    // Neither caller can widen the other's reach, and there is no third
    // implementation of this SQL to drift out of step.

    /// Creates or rewrites a schedule row on `connection`.
    ///
    /// The organization link resolves via the tenant FK inside the statement
    /// (see module docs), so a tenant that does not exist is rejected by the
    /// database rather than guessed at in Rust.
    ///
    /// # Errors
    /// `conflict` when the tenant already holds a schedule of this name;
    /// `validation` when the tenant id is unknown (the `INSERT … SELECT`
    /// matched no row); the mapped sqlx error otherwise.
    pub async fn save_in(connection: &mut PgConnection, schedule: &Schedule) -> Result<()> {
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
            .execute(&mut *connection)
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

    /// Loads one schedule by primary key on `connection`.
    ///
    /// # Errors
    /// The mapped sqlx error.
    pub async fn get_in(connection: &mut PgConnection, id: ScheduleId) -> Result<Option<Schedule>> {
        let row = sqlx::query(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        row.map(fetch_schedule)
            .transpose()?
            .map(ScheduleRow::into_domain)
            .transpose()
    }

    /// Lists schedules in a tenant (name order) on `connection`.
    ///
    /// # Errors
    /// The mapped sqlx error.
    pub async fn list_in(connection: &mut PgConnection, tenant: TenantId) -> Result<Vec<Schedule>> {
        let rows = sqlx::query(&format!(
            "SELECT {SCHEDULE_COLUMNS} FROM schedules WHERE tenant_id = $1 ORDER BY name"
        ))
        .bind(Uuid::from(tenant))
        .fetch_all(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_schedule)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(ScheduleRow::into_domain)
            .collect()
    }

    /// Due-scan with the documented semantics on `connection`: active rows
    /// whose next plan is at or before `now`, oldest first, bounded.
    ///
    /// This is the platform's one legitimate cross-tenant read. It is only
    /// ever called on the scheduler plane's connection (`mas_scheduler`),
    /// whose BYPASSRLS is granted for SELECT on `schedules` and nothing else.
    /// Called on `mas_app` without a scope it returns zero rows rather than
    /// every tenant's schedules, which is the correct failure direction.
    ///
    /// # Errors
    /// The mapped sqlx error.
    pub async fn list_due_in(
        connection: &mut PgConnection,
        now: &Timestamp,
        limit: usize,
    ) -> Result<Vec<Schedule>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = sqlx::query(LIST_DUE_SQL)
            .bind(now.into_datetime())
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&mut *connection)
            .await
            .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_schedule)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(ScheduleRow::into_domain)
            .collect()
    }

    /// Journals a planned tick on `connection`.
    ///
    /// `false` = the unique constraint reports another replica already
    /// recorded it (duplicate-fire guard).
    ///
    /// # Errors
    /// `not_found` when the schedule does not exist inside the scope the
    /// connection carries — which, under RLS, is also what a schedule owned
    /// by another tenant looks like. The two are deliberately
    /// indistinguishable to the caller: the caller is not entitled to know
    /// that another tenant's schedule exists.
    pub async fn record_run_in(
        connection: &mut PgConnection,
        schedule_id: ScheduleId,
        planned_at: Timestamp,
    ) -> Result<bool> {
        let result = sqlx::query(RECORD_RUN_SQL)
            .bind(Uuid::now_v7())
            // schedule_runs carries tenant too; deriving it via a subselect
            // keeps replica loops pure (no extra read round-trip).
            .bind(Uuid::from(schedule_id))
            .bind(planned_at.into_datetime())
            .execute(&mut *connection)
            .await;
        match result {
            Ok(done) => {
                if done.rows_affected() == 1 {
                    return Ok(true);
                }
                // Zero rows is ambiguous: duplicate tick vs unknown schedule.
                // Resolve it honestly — replicas depend on the distinction.
                //
                // The disambiguating read reuses the connection already in
                // hand. Taking a second scoped connection here deadlocks: the
                // request's transaction sits behind a non-reentrant mutex and
                // this branch is reached while that guard is still alive. The
                // hang was invisible until the suite ran against real
                // PostgreSQL, and it sits on the duplicate-tick path — the
                // path every second replica takes on every tick.
                let exists: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schedules WHERE id = $1)")
                        .bind(Uuid::from(schedule_id))
                        .fetch_one(&mut *connection)
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

    // ── Request-scoped surface ──────────────────────────────────────────

    /// Creates or rewrites a schedule row.
    ///
    /// Every tenant-owned table is FORCE RLS: this statement only sees rows
    /// inside the request's own scope, because the executor IS the request's
    /// transaction.
    ///
    /// # Errors
    /// See [`Self::save_in`].
    pub async fn save(&self, schedule: &Schedule) -> Result<()> {
        let mut scoped = crate::scoped_request::connection().await?;
        Self::save_in(scoped.connection().as_mut(), schedule).await
    }

    /// Loads one schedule by primary key.
    ///
    /// # Errors
    /// See [`Self::get_in`].
    pub async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>> {
        let mut scoped = crate::scoped_request::connection().await?;
        Self::get_in(scoped.connection().as_mut(), id).await
    }

    /// Lists schedules in a tenant (name order).
    ///
    /// # Errors
    /// See [`Self::list_in`].
    pub async fn list(&self, tenant: TenantId) -> Result<Vec<Schedule>> {
        let mut scoped = crate::scoped_request::connection().await?;
        Self::list_in(scoped.connection().as_mut(), tenant).await
    }

    /// Due-scan inside the request's scope.
    ///
    /// This is *not* the scheduler's due-scan: a request sees only its own
    /// tenant's schedules. The scheduler's cross-tenant scan goes through
    /// [`Self::list_due_in`] on the plane connection.
    ///
    /// # Errors
    /// See [`Self::list_due_in`].
    pub async fn list_due(&self, now: &Timestamp, limit: usize) -> Result<Vec<Schedule>> {
        let mut scoped = crate::scoped_request::connection().await?;
        Self::list_due_in(scoped.connection().as_mut(), now, limit).await
    }

    /// Journals a planned tick.
    ///
    /// # Errors
    /// See [`Self::record_run_in`].
    pub async fn record_run(&self, schedule_id: ScheduleId, planned_at: Timestamp) -> Result<bool> {
        let mut scoped = crate::scoped_request::connection().await?;
        Self::record_run_in(scoped.connection().as_mut(), schedule_id, planned_at).await
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
