//! PURPOSE: The quota reservation store — the durable half of
//! `check → reserve → execute → settle → release`, including crash recovery.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: a reservation is authorised headroom that has
//! not yet been reconciled with what was actually consumed. It must survive
//! the process that created it: if that process dies between `reserve` and
//! `settle`, the reservation's lease lapses and the sweeper returns the
//! headroom to the tenant. Without that, a single crash would consume a
//! tenant's monthly allowance permanently.
//!
//! Security boundary: every statement is tenant-filtered and the table sits
//! under FORCE RLS. The reservation idempotency key is unique per tenant, so
//! a caller cannot reserve twice against the same logical work — and cannot
//! probe another tenant's keys, because it cannot see their rows.
//!
//! Data ownership: reservations are Rust-owned platform state.
//!
//! It must NOT:
//!   • settle more than was reserved (the database CHECK enforces it too);
//!   • reclaim a reservation that has already been settled — that would hand
//!     back headroom for work that was already metered;
//!   • let a reservation exist without a lease expiry;
//!   • be used as the usage record: a reservation authorises, usage accounts.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ReservationId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::quota::QuotaDimension;
use mas_domain::reservation::{QuotaReservation, ReservationState, Settlement};
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::map_sqlx;

/// One reservation row as stored.
#[derive(Debug, Clone)]
pub struct ReservationRow {
    /// Reservation id.
    pub id: uuid::Uuid,
    /// Owning tenant.
    pub tenant_id: uuid::Uuid,
    /// Metered dimension.
    pub dimension: String,
    /// Fixed-window bucket.
    pub bucket: i64,
    /// Authorised units.
    pub reserved_amount: i64,
    /// Consumed units.
    pub settled_amount: i64,
    /// Lifecycle state.
    pub state: String,
    /// Caller idempotency key.
    pub reservation_key: String,
    /// Causing execution, when set.
    pub execution_id: Option<uuid::Uuid>,
    /// Causing task, when set.
    pub task_id: Option<uuid::Uuid>,
    /// Lease expiry.
    pub lease_expires_at: chrono::DateTime<chrono::Utc>,
    /// Creation instant.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last mutation.
    pub updated_at: chrono::DateTime<chrono::Utc>,
    /// Terminal instant, when terminal.
    pub settled_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for ReservationRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            tenant_id: row.try_get("tenant_id")?,
            dimension: row.try_get("dimension")?,
            bucket: row.try_get("bucket")?,
            reserved_amount: row.try_get("reserved_amount")?,
            settled_amount: row.try_get("settled_amount")?,
            state: row.try_get("state")?,
            reservation_key: row.try_get("reservation_key")?,
            execution_id: row.try_get("execution_id")?,
            task_id: row.try_get("task_id")?,
            lease_expires_at: row.try_get("lease_expires_at")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            settled_at: row.try_get("settled_at")?,
        })
    }
}

impl ReservationRow {
    /// Hydrates into the domain aggregate.
    pub fn into_domain(self) -> Result<QuotaReservation> {
        let state = self.state.parse::<ReservationState>().map_err(|_| {
            AppError::database(format!("unknown reservation state '{}'", self.state))
        })?;
        let dimension = self.dimension.parse::<QuotaDimension>().map_err(|_| {
            AppError::database(format!("unknown quota dimension '{}'", self.dimension))
        })?;
        Ok(QuotaReservation {
            id: ReservationId::from_uuid(self.id),
            tenant_id: TenantId::from_uuid(self.tenant_id),
            dimension,
            bucket: self.bucket,
            reserved_amount: u64::try_from(self.reserved_amount).unwrap_or(0),
            settled_amount: u64::try_from(self.settled_amount).unwrap_or(0),
            state,
            reservation_key: self.reservation_key,
            execution_id: self.execution_id.map(ExecutionId::from_uuid),
            task_id: self.task_id.map(TaskId::from_uuid),
            lease_expires_at: Timestamp::from_datetime(self.lease_expires_at),
            created_at: Timestamp::from_datetime(self.created_at),
            updated_at: Timestamp::from_datetime(self.updated_at),
            settled_at: self.settled_at.map(Timestamp::from_datetime),
        })
    }
}

/// The reservation store.
#[derive(Debug, Clone)]
pub struct PostgresReservationStore {
    pool: PgPool,
}

impl PostgresReservationStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Acquires a transaction-scoped advisory lock for one tenant/key pair.
    ///
    /// `ControlPlane::reserve` holds this before its first read and keeps the
    /// authoritative request transaction open through counter charging and the
    /// reservation-row write. The unique row constraint alone cannot protect
    /// that larger sequence: two replicas can both see no row and both charge
    /// the counter before one discovers the other's row. PostgreSQL releases
    /// this lock automatically on commit or rollback, including a quota
    /// refusal, so it never needs a best-effort unlock path.
    pub async fn lock_key(&self, tenant: TenantId, reservation_key: &str) -> Result<()> {
        if !crate::scoped_request::has_scope() {
            return Err(AppError::internal(
                "quota reservation creation requires an authoritative request transaction",
            ));
        }
        let key = format!("{tenant}/{reservation_key}");
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(key)
            .execute(executor(&self.pool).await?.connection())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    /// Authorises `amount` units against a reservation key.
    ///
    /// Idempotent by `(tenant_id, reservation_key)`: a retry of the same
    /// logical work returns the *existing* reservation rather than creating a
    /// second one. The unique index is what enforces it — a read-then-write
    /// would let two concurrent retries both miss and both insert.
    ///
    /// The caller is responsible for having already charged the counter via
    /// `PostgresCounterStore::try_reserve`; this row is the record that makes
    /// that charge recoverable.
    #[allow(clippy::too_many_arguments)]
    pub async fn reserve(
        &self,
        tenant: TenantId,
        dimension: QuotaDimension,
        bucket: i64,
        amount: u64,
        reservation_key: &str,
        execution_id: Option<ExecutionId>,
        task_id: Option<TaskId>,
        lease_expires_at: &Timestamp,
        now: &Timestamp,
    ) -> Result<QuotaReservation> {
        if amount == 0 {
            return Err(AppError::validation(
                "a reservation must authorise a positive amount",
            ));
        }
        //
        // `DO UPDATE … WHERE state <> 'held'` rather than `DO NOTHING`.
        //
        // With `DO NOTHING`, a key that had been used before was a trap. Rows
        // are never deleted — they are settled or released and kept as the
        // record of the charge — so re-running the same task hit the old
        // terminal row, was handed it, and charged the counter for it. The
        // settlement that followed released nothing (the row was already
        // terminal), so every retry leaked one unit of the tenant's ceiling.
        // For `ConcurrentRuns`, whose counter is a gauge that only ever comes
        // back through a release, that leak is permanent.
        //
        // The `WHERE` is what keeps the idempotency the clause exists for: a
        // row that is still `held` is a live hold, and the caller gets that
        // hold back rather than a new one.
        let inserted: Option<ReservationRow> = sqlx::query_as::<_, ReservationRow>(
            "INSERT INTO quota_reservations
                (tenant_id, dimension, bucket, reserved_amount, reservation_key,
                 execution_id, task_id, lease_expires_at, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
             ON CONFLICT (tenant_id, reservation_key) DO UPDATE
                SET bucket = EXCLUDED.bucket,
                    reserved_amount = EXCLUDED.reserved_amount,
                    settled_amount = 0,
                    state = 'held',
                    execution_id = EXCLUDED.execution_id,
                    task_id = EXCLUDED.task_id,
                    lease_expires_at = EXCLUDED.lease_expires_at,
                    created_at = EXCLUDED.created_at,
                    updated_at = EXCLUDED.updated_at,
                    settled_at = NULL
              WHERE quota_reservations.state <> 'held'
             RETURNING id, tenant_id, dimension, bucket, reserved_amount,
                       settled_amount, state, reservation_key, execution_id,
                       task_id, lease_expires_at, created_at, updated_at, settled_at",
        )
        .bind(tenant.into_uuid())
        .bind(dimension.as_str())
        .bind(bucket)
        .bind(i64::try_from(amount).unwrap_or(i64::MAX))
        .bind(reservation_key)
        .bind(execution_id.map(ExecutionId::into_uuid))
        .bind(task_id.map(TaskId::into_uuid))
        .bind(lease_expires_at.into_datetime())
        // The caller's clock, so a lease is validated and stamped against the
        // same instant the rest of the request used.
        .bind(now.into_datetime())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        match inserted {
            Some(row) => row.into_domain(),
            // No row returned ⇒ a *live* hold already existed under this key
            // (the `WHERE` clause is what makes that the only way to get here).
            // Re-read it: the caller gets the original reservation back, not an
            // error, so a retry of the same logical work is transparent.
            None => self.get_by_key(tenant, reservation_key).await,
        }
    }

    /// Reads one reservation by its idempotency key.
    pub async fn get_by_key(
        &self,
        tenant: TenantId,
        reservation_key: &str,
    ) -> Result<QuotaReservation> {
        let row: ReservationRow = sqlx::query_as::<_, ReservationRow>(
            "SELECT id, tenant_id, dimension, bucket, reserved_amount,
                    settled_amount, state, reservation_key, execution_id,
                    task_id, lease_expires_at, created_at, updated_at, settled_at
               FROM quota_reservations
              WHERE tenant_id = $1 AND reservation_key = $2",
        )
        .bind(tenant.into_uuid())
        .bind(reservation_key)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| {
            AppError::not_found("quota_reservation", format!("{tenant}/{reservation_key}"))
        })?;
        row.into_domain()
    }

    /// Reads one reservation by id.
    pub async fn get(
        &self,
        tenant: TenantId,
        id: ReservationId,
    ) -> Result<Option<QuotaReservation>> {
        let row: Option<ReservationRow> = sqlx::query_as::<_, ReservationRow>(
            "SELECT id, tenant_id, dimension, bucket, reserved_amount,
                    settled_amount, state, reservation_key, execution_id,
                    task_id, lease_expires_at, created_at, updated_at, settled_at
               FROM quota_reservations
              WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(ReservationRow::into_domain).transpose()
    }

    /// Records actual consumption and returns the unused headroom.
    ///
    /// Idempotent: settling an already-terminal reservation returns
    /// `applied = false` and releases nothing, so a duplicated broker delivery
    /// cannot both meter the work and refund it. The `state = 'held'` guard in
    /// the WHERE clause is what makes the second delivery a no-op at the
    /// database level — not an application-level check that could race.
    pub async fn settle(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        actual: u64,
        at: &Timestamp,
    ) -> Result<Settlement> {
        let row: Option<ReservationRow> = sqlx::query_as::<_, ReservationRow>(
            "UPDATE quota_reservations
                SET settled_amount = LEAST($3, reserved_amount),
                    state = 'settled',
                    settled_at = $4,
                    updated_at = now()
              WHERE tenant_id = $1
                AND reservation_key = $2
                AND state = 'held'
          RETURNING id, tenant_id, dimension, bucket, reserved_amount,
                    settled_amount, state, reservation_key, execution_id,
                    task_id, lease_expires_at, created_at, updated_at, settled_at",
        )
        .bind(tenant.into_uuid())
        .bind(reservation_key)
        .bind(i64::try_from(actual).unwrap_or(i64::MAX))
        .bind(at.into_datetime())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        match row {
            None => {
                // Either it does not exist, or it is already terminal. Both
                // mean "do not meter this again"; the distinction matters only
                // for the caller's logging, and only one of them is an error.
                let existing = sqlx::query_scalar::<_, String>(
                    "SELECT state FROM quota_reservations
                      WHERE tenant_id = $1 AND reservation_key = $2",
                )
                .bind(tenant.into_uuid())
                .bind(reservation_key)
                .fetch_optional(executor(&self.pool).await?.connection())
                .await
                .map_err(map_sqlx)?;
                match existing {
                    // Already terminal. Report what it settled the first
                    // time and release nothing: a duplicate delivery must not
                    // both meter the work and refund it.
                    Some(_) => {
                        let reservation = self.get_by_key(tenant, reservation_key).await?;
                        Ok(Settlement {
                            released: 0,
                            consumed: reservation.settled_amount,
                            applied: false,
                        })
                    },
                    None => Err(AppError::not_found(
                        "quota_reservation",
                        format!("{tenant}/{reservation_key}"),
                    )),
                }
            },
            Some(row) => {
                let reservation = row.into_domain()?;
                Ok(Settlement {
                    released: reservation.unused(),
                    consumed: reservation.settled_amount,
                    applied: true,
                })
            },
        }
    }

    /// Extends the lease on a reservation that is still held.
    ///
    /// One statement, guarded on `state = 'held'`: a lease cannot be extended
    /// on a reservation that has already been settled, released or swept, and
    /// the guard is in the `WHERE` rather than in a preceding `SELECT` so two
    /// workers cannot both read "still held" and then race to extend it.
    ///
    /// The caller's `now` stamps `updated_at`, so a lease extension is
    /// auditable against the same clock the rest of the run used.
    pub async fn extend_lease(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        lease_expires_at: &Timestamp,
        now: &Timestamp,
    ) -> Result<Option<QuotaReservation>> {
        let row: Option<ReservationRow> = sqlx::query_as::<_, ReservationRow>(
            "UPDATE quota_reservations
                SET lease_expires_at = $3,
                    updated_at = $4
              WHERE tenant_id = $1
                AND reservation_key = $2
                AND state = 'held'
                AND lease_expires_at < $3
          RETURNING id, tenant_id, dimension, bucket, reserved_amount,
                    settled_amount, state, reservation_key, execution_id,
                    task_id, lease_expires_at, created_at, updated_at, settled_at",
        )
        .bind(tenant.into_uuid())
        .bind(reservation_key)
        .bind(lease_expires_at.into_datetime())
        .bind(now.into_datetime())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(ReservationRow::into_domain).transpose()
    }

    /// Returns the whole reservation without metering anything.
    pub async fn release(
        &self,
        tenant: TenantId,
        reservation_key: &str,
        at: &Timestamp,
    ) -> Result<Settlement> {
        let row: Option<ReservationRow> = sqlx::query_as::<_, ReservationRow>(
            "UPDATE quota_reservations
                SET settled_amount = 0,
                    state = 'released',
                    settled_at = $3,
                    updated_at = now()
              WHERE tenant_id = $1
                AND reservation_key = $2
                AND state = 'held'
          RETURNING id, tenant_id, dimension, bucket, reserved_amount,
                    settled_amount, state, reservation_key, execution_id,
                    task_id, lease_expires_at, created_at, updated_at, settled_at",
        )
        .bind(tenant.into_uuid())
        .bind(reservation_key)
        .bind(at.into_datetime())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        match row {
            Some(row) => {
                let reservation = row.into_domain()?;
                Ok(Settlement {
                    released: reservation.reserved_amount,
                    consumed: 0,
                    applied: true,
                })
            },
            None => {
                let state: Option<String> = sqlx::query_scalar(
                    "SELECT state FROM quota_reservations
                      WHERE tenant_id = $1 AND reservation_key = $2",
                )
                .bind(tenant.into_uuid())
                .bind(reservation_key)
                .fetch_optional(executor(&self.pool).await?.connection())
                .await
                .map_err(map_sqlx)?;
                match state {
                    None => Err(AppError::not_found(
                        "quota_reservation",
                        format!("{tenant}/{reservation_key}"),
                    )),
                    // Already terminal: releasing twice must not refund twice.
                    Some(_) => Ok(Settlement {
                        released: 0,
                        consumed: 0,
                        applied: false,
                    }),
                }
            },
        }
    }

    /// Reclaims every reservation whose lease has lapsed (crash recovery).
    ///
    /// Returns the units per reservation that must be subtracted from the
    /// counter. The `state = 'held'` guard means a reservation that was
    /// settled a millisecond ago is never reclaimed, so settling and reclaiming
    /// cannot both apply to the same row.
    ///
    /// `limit` bounds one sweep so a backlog after an outage is drained in
    /// bounded steps rather than in one long transaction.
    pub async fn reclaim_expired(
        &self,
        tenant: Option<TenantId>,
        now: &Timestamp,
        limit: i64,
    ) -> Result<Vec<(QuotaReservation, u64)>> {
        let rows: Vec<ReservationRow> = sqlx::query_as::<_, ReservationRow>(
            "UPDATE quota_reservations
                SET state = 'expired',
                    settled_at = $2,
                    updated_at = now()
              WHERE state = 'held'
                AND lease_expires_at <= $2
                AND ($1::uuid IS NULL OR tenant_id = $1)
                AND id IN (
                    SELECT id FROM quota_reservations
                     WHERE state = 'held' AND lease_expires_at <= $2
                       AND ($1::uuid IS NULL OR tenant_id = $1)
                     ORDER BY lease_expires_at
                     LIMIT $3
                     FOR UPDATE SKIP LOCKED
                )
          RETURNING id, tenant_id, dimension, bucket, reserved_amount,
                    settled_amount, state, reservation_key, execution_id,
                    task_id, lease_expires_at, created_at, updated_at, settled_at",
        )
        .bind(tenant.map(TenantId::into_uuid))
        .bind(now.into_datetime())
        .bind(limit.max(1))
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let released =
                    u64::try_from(row.reserved_amount.saturating_sub(row.settled_amount))
                        .unwrap_or(0);
                row.into_domain().map(|reservation| (reservation, released))
            })
            .collect()
    }

    /// Every reservation attached to one execution (operator/debug view).
    pub async fn list_for_execution(
        &self,
        tenant: TenantId,
        execution: ExecutionId,
    ) -> Result<Vec<QuotaReservation>> {
        let rows: Vec<ReservationRow> = sqlx::query_as::<_, ReservationRow>(
            "SELECT id, tenant_id, dimension, bucket, reserved_amount,
                    settled_amount, state, reservation_key, execution_id,
                    task_id, lease_expires_at, created_at, updated_at, settled_at
               FROM quota_reservations
              WHERE tenant_id = $1 AND execution_id = $2
              ORDER BY created_at",
        )
        .bind(tenant.into_uuid())
        .bind(execution.into_uuid())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(ReservationRow::into_domain).collect()
    }
}
