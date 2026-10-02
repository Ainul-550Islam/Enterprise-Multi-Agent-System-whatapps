//! Append-only audit log store.
//!
//! The API deliberately offers **only** append and read methods — there is no
//! update/delete here, and the SQL triggers (`0011_audit`) reject mutations
//! for every role, including owners. Metadata must already be redacted by the
//! caller (the `audit` domain + `common::redaction`); the store trusts but
//! never re-inspects payload content. Reads use keyset pagination so the
//! compliance surface stays fast at append-only scale.

use std::fmt;

use mas_common::error::AppError;
use mas_common::ids::{AuditEventId, TenantId};
use mas_common::pagination::{Cursor, PageRequest, PageResponse};
use mas_common::result::Result;
use mas_domain::audit_event::AuditEvent;
use sqlx::{PgConnection, PgPool, Row};

use crate::error::map_sqlx;
use crate::rows::AuditRow;

const AUDIT_COLUMNS: &str = "id, tenant_id, organization_id, actor, action, resource_type, \
     resource_id, outcome, severity, compliance_class, correlation_id, metadata, occurred_at";

/// The append-only audit store.
#[derive(Clone)]
pub struct PostgresAuditLog {
    pool: PgPool,
}

impl fmt::Debug for PostgresAuditLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresAuditLog").finish_non_exhaustive()
    }
}

impl PostgresAuditLog {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Appends one event inside a caller-managed transaction, so the audit
    /// trail commits atomically with the mutation it describes.
    ///
    /// This is deliberately the **only** write operation the type exposes.
    pub async fn append_in(executor: &mut PgConnection, event: &AuditEvent) -> Result<()> {
        let row = AuditRow::from_domain(event)?;
        sqlx::query(&format!(
            "INSERT INTO audit_events ({AUDIT_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
        ))
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(row.organization_id)
        .bind(&row.actor)
        .bind(&row.action)
        .bind(&row.resource_type)
        .bind(&row.resource_id)
        .bind(&row.outcome)
        .bind(&row.severity)
        .bind(&row.compliance_class)
        .bind(&row.correlation_id)
        .bind(&row.metadata)
        .bind(row.occurred_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Standalone append (acquires its own connection).
    pub async fn append(&self, event: &AuditEvent) -> Result<()> {
        let mut conn = self.pool.acquire().await.map_err(map_sqlx)?;
        Self::append_in(&mut conn, event).await
    }

    /// Fetches one event by id.
    pub async fn get(&self, id: AuditEventId) -> Result<AuditEvent> {
        let row = sqlx::query(&format!(
            "SELECT {AUDIT_COLUMNS} FROM audit_events WHERE id = $1"
        ))
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("audit_event", id.to_string()))?;
        row_from_sqlx(&row)?.into_domain()
    }

    /// Pages a tenant's events, newest first, with keyset pagination.
    pub async fn list_for_tenant(
        &self,
        tenant: TenantId,
        params: &PageRequest,
    ) -> Result<PageResponse<AuditEvent>> {
        params.validate()?;
        let limit = i64::from(params.effective_limit()) + 1;
        let cursor = params.decode_cursor()?;
        let (cursor_ts, cursor_id) = match cursor.as_ref().map(Cursor::payload) {
            Some(payload) => (
                Some(ts_from_millis(payload.ts_ms)?),
                Some(payload.id.clone()),
            ),
            None => (None, None),
        };

        // Keyset: (occurred_at, id) strictly before the cursor position.
        let rows = sqlx::query(&format!(
            "SELECT {AUDIT_COLUMNS} FROM audit_events
             WHERE tenant_id = $1
               AND ($2::timestamptz IS NULL
                    OR (occurred_at, id::text) < ($2, $3))
             ORDER BY occurred_at DESC, id DESC
             LIMIT $4"
        ))
        .bind(tenant.into_uuid())
        .bind(cursor_ts)
        .bind(cursor_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        let mut events: Vec<AuditEvent> = rows
            .iter()
            .map(|row| row_from_sqlx(row).and_then(AuditRow::into_domain))
            .collect::<Result<_>>()?;

        let next_cursor = if events.len() as i64 == limit {
            let last = events.pop().expect("last element exists");
            let cursor = Cursor::new(last.occurred_at.to_unix_ms(), last.id.to_string());
            Some(cursor.encode()?)
        } else {
            None
        };
        Ok(PageResponse::with_items(events, next_cursor))
    }

    /// All events for one correlation id (request tracing), oldest first.
    /// Bounded at 1 000 entries — a single request causing more audit events
    /// than that is itself a finding.
    pub async fn list_for_correlation(&self, correlation_id: &str) -> Result<Vec<AuditEvent>> {
        if correlation_id.is_empty() || correlation_id.len() > 256 {
            return Err(AppError::invalid_field(
                "correlation_id",
                "length",
                "correlation id must be 1..=256 characters",
            ));
        }
        let rows = sqlx::query(&format!(
            "SELECT {AUDIT_COLUMNS} FROM audit_events
             WHERE correlation_id = $1
             ORDER BY occurred_at ASC, id ASC
             LIMIT 1000"
        ))
        .bind(correlation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter()
            .map(|row| row_from_sqlx(row).and_then(AuditRow::into_domain))
            .collect()
    }

    /// Counts events by outcome for a tenant (security dashboards).
    pub async fn outcome_counts(&self, tenant: TenantId) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            "SELECT outcome, COUNT(*) AS count
             FROM audit_events WHERE tenant_id = $1 GROUP BY outcome",
        )
        .bind(tenant.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("outcome").map_err(map_sqlx)?,
                    row.try_get::<i64, _>("count").map_err(map_sqlx)?,
                ))
            })
            .collect()
    }
}

/// Converts epoch-millis cursor payloads back into database timestamps.
fn ts_from_millis(ms: i64) -> Result<chrono::DateTime<chrono::Utc>> {
    Ok(mas_common::timestamps::Timestamp::from_unix_ms(ms)?.into_datetime())
}

fn row_from_sqlx(row: &sqlx::postgres::PgRow) -> Result<AuditRow> {
    Ok(AuditRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: row.try_get("organization_id").map_err(map_sqlx)?,
        actor: row.try_get("actor").map_err(map_sqlx)?,
        action: row.try_get("action").map_err(map_sqlx)?,
        resource_type: row.try_get("resource_type").map_err(map_sqlx)?,
        resource_id: row.try_get("resource_id").map_err(map_sqlx)?,
        outcome: row.try_get("outcome").map_err(map_sqlx)?,
        severity: row.try_get("severity").map_err(map_sqlx)?,
        compliance_class: row.try_get("compliance_class").map_err(map_sqlx)?,
        correlation_id: row.try_get("correlation_id").map_err(map_sqlx)?,
        metadata: row.try_get("metadata").map_err(map_sqlx)?,
        occurred_at: row.try_get("occurred_at").map_err(map_sqlx)?,
    })
}

#[async_trait::async_trait]
impl mas_application::audit::AuditSinkPort for PostgresAuditLog {
    async fn record(&self, event: &AuditEvent) -> Result<()> {
        self.append(event).await
    }
}
