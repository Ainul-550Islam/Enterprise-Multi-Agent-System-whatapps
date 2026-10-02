//! PostgreSQL implementation of the transactional outbox port.
//!
//! Callers insert outbox rows **inside their own unit-of-work** via
//! [`PostgresOutboxStore::enqueue_in`], so the domain state change and the
//! event commit atomically. The pool-level [`PostgresOutboxStore`] port
//! implementation serves the dispatcher (fetch/mark), and must run under the
//! dedicated BYPASSRLS dispatcher role documented in `0012_outbox` — the rows
//! carry their own `tenant_id` because no single tenant GUC applies there.

use std::fmt;

use mas_common::error::AppError;
use mas_common::ids::EventId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_events::outbox::{OutboxRecord, OutboxStorePort};
use sqlx::{PgConnection, PgPool, Row};

use crate::error::map_sqlx;
use crate::rows::OutboxRow;

/// Outbox store bound to a pool.
#[derive(Clone)]
pub struct PostgresOutboxStore {
    pool: PgPool,
}

impl fmt::Debug for PostgresOutboxStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresOutboxStore")
            .finish_non_exhaustive()
    }
}

impl PostgresOutboxStore {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts inside a caller-managed transaction (the atomicity point).
    ///
    /// Idempotent on `event_id`: a conflicting insert returns `false` and the
    /// original row is untouched.
    pub async fn enqueue_in(executor: &mut PgConnection, record: &OutboxRecord) -> Result<bool> {
        let row = OutboxRow::from_record(record)?;
        let result = sqlx::query(
            "INSERT INTO outbox_events (
                 event_id, envelope, tenant_id, event_type,
                 aggregate_type, aggregate_id, status, attempts,
                 next_attempt_at, last_error, enqueued_at, published_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(row.event_id)
        .bind(&row.envelope)
        .bind(row.tenant_id)
        .bind(&row.event_type)
        .bind(&row.aggregate_type)
        .bind(&row.aggregate_id)
        .bind(&row.status)
        .bind(row.attempts)
        .bind(row.next_attempt_at)
        .bind(&row.last_error)
        .bind(row.enqueued_at)
        .bind(row.published_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }

    /// Deletes published rows older than `retention` — the dispatcher's
    /// periodic hygiene. Returns rows removed.
    ///
    /// Unpublished/dead-letter rows are never swept by this call; dead
    /// letters are an operator surface, not garbage.
    pub async fn sweep_published(&self, retention: std::time::Duration) -> Result<u64> {
        let retention_secs = i64::try_from(retention.as_secs()).unwrap_or(i64::MAX);
        let result = sqlx::query(
            "DELETE FROM outbox_events
             WHERE status = 'published'
               AND published_at < now() - ($1 || ' seconds')::interval",
        )
        .bind(retention_secs)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected())
    }
}

fn row_from_sqlx(row: &sqlx::postgres::PgRow) -> Result<OutboxRow> {
    Ok(OutboxRow {
        event_id: row.try_get("event_id").map_err(map_sqlx)?,
        envelope: row.try_get("envelope").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        event_type: row.try_get("event_type").map_err(map_sqlx)?,
        aggregate_type: row.try_get("aggregate_type").map_err(map_sqlx)?,
        aggregate_id: row.try_get("aggregate_id").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        attempts: row.try_get("attempts").map_err(map_sqlx)?,
        next_attempt_at: row.try_get("next_attempt_at").map_err(map_sqlx)?,
        last_error: row.try_get("last_error").map_err(map_sqlx)?,
        enqueued_at: row.try_get("enqueued_at").map_err(map_sqlx)?,
        published_at: row.try_get("published_at").map_err(map_sqlx)?,
    })
}

#[async_trait::async_trait]
impl OutboxStorePort for PostgresOutboxStore {
    async fn enqueue(&self, record: &OutboxRecord) -> Result<bool> {
        let mut conn = self.pool.acquire().await.map_err(map_sqlx)?;
        Self::enqueue_in(&mut conn, record).await
    }

    async fn fetch_due(&self, limit: usize) -> Result<Vec<OutboxRecord>> {
        let limit = i64::try_from(limit.clamp(1, 10_000)).unwrap_or(10_000);
        // FOR UPDATE SKIP LOCKED lets multiple dispatcher pods share work
        // without double-publishing.
        let rows = sqlx::query(
            "SELECT event_id, envelope, tenant_id, event_type, aggregate_type,
                    aggregate_id, status, attempts, next_attempt_at, last_error,
                    enqueued_at, published_at
             FROM outbox_events
             WHERE status IN ('pending', 'failed')
               AND next_attempt_at <= now()
             ORDER BY next_attempt_at ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter()
            .map(|row| row_from_sqlx(row).and_then(OutboxRow::into_record))
            .collect()
    }

    async fn mark_published(&self, event_id: EventId) -> Result<()> {
        let result = sqlx::query(
            "UPDATE outbox_events
             SET status = 'published', published_at = now(), last_error = NULL
             WHERE event_id = $1 AND status IN ('pending', 'failed')",
        )
        .bind(event_id.into_uuid())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::conflict(format!(
                "outbox event {event_id} is not pending/failed; not marking published"
            )));
        }
        Ok(())
    }

    async fn mark_failed(
        &self,
        event_id: EventId,
        error: &str,
        next_attempt_at: Timestamp,
    ) -> Result<()> {
        let trimmed: String = error.chars().take(500).collect();
        let result = sqlx::query(
            "UPDATE outbox_events
             SET status = 'failed', attempts = attempts + 1,
                 last_error = $2, next_attempt_at = $3
             WHERE event_id = $1 AND status IN ('pending', 'failed')",
        )
        .bind(event_id.into_uuid())
        .bind(trimmed)
        .bind(next_attempt_at.into_datetime())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::conflict(format!(
                "outbox event {event_id} is not pending/failed; not marking failed"
            )));
        }
        Ok(())
    }

    async fn mark_dead_lettered(&self, event_id: EventId) -> Result<()> {
        let result = sqlx::query(
            "UPDATE outbox_events
             SET status = 'dead_lettered'
             WHERE event_id = $1 AND status = 'failed'",
        )
        .bind(event_id.into_uuid())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::conflict(format!(
                "outbox event {event_id} is not failed; not dead-lettering"
            )));
        }
        Ok(())
    }
}
