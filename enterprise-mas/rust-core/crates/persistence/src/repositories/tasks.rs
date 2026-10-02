//! `tasks` repository — the worker's durability plane.
//!
//! The consumer's settlement protocol lives on the `tasks` row itself:
//! `fetch` claims `queued` rows atomically (`FOR UPDATE SKIP LOCKED`),
//! transitions land in the row (`running` → `completed|failed|queued|
//! dead_lettered`), and `not_before` carries backoff. The broker layer in
//! [`super::broker`] is deliberately a *wake-up* mechanism atop the same
//! rows: rows are truth, frames are hints, and a boot-time recovery scan
//! (the same query the broker uses) repairs any lost hint.

use mas_common::ids::TaskId;
use mas_common::result::Result;
use mas_domain::task::Task;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::TaskRow;

/// Full projected columns for task queries.
pub const TASK_COLUMNS: &str = "id, tenant_id, organization_id, project_id, execution_id, \
     idempotency_key, kind, status, priority, payload, result, not_before, deadline_at, \
     correlation_id, created_at, updated_at";

const TASK_UPSERT: &str = "INSERT INTO tasks (
        id, tenant_id, organization_id, project_id, execution_id, idempotency_key,
        kind, status, priority, payload, result, not_before, deadline_at,
        correlation_id, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    ON CONFLICT (id) DO UPDATE SET
        status = EXCLUDED.status,
        payload = EXCLUDED.payload,
        result = EXCLUDED.result,
        not_before = EXCLUDED.not_before,
        deadline_at = EXCLUDED.deadline_at,
        updated_at = EXCLUDED.updated_at";

fn fetch_task(pg_row: sqlx::postgres::PgRow) -> Result<TaskRow> {
    Ok(TaskRow {
        id: pg_row.try_get("id").map_err(map_sqlx)?,
        tenant_id: pg_row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: pg_row.try_get("organization_id").map_err(map_sqlx)?,
        project_id: pg_row.try_get("project_id").map_err(map_sqlx)?,
        execution_id: pg_row.try_get("execution_id").map_err(map_sqlx)?,
        idempotency_key: pg_row.try_get("idempotency_key").map_err(map_sqlx)?,
        kind: pg_row.try_get("kind").map_err(map_sqlx)?,
        status: pg_row.try_get("status").map_err(map_sqlx)?,
        priority: pg_row.try_get("priority").map_err(map_sqlx)?,
        payload: pg_row.try_get("payload").map_err(map_sqlx)?,
        result: pg_row.try_get("result").map_err(map_sqlx)?,
        not_before: pg_row.try_get("not_before").map_err(map_sqlx)?,
        deadline_at: pg_row.try_get("deadline_at").map_err(map_sqlx)?,
        correlation_id: pg_row.try_get("correlation_id").map_err(map_sqlx)?,
        created_at: pg_row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: pg_row.try_get("updated_at").map_err(map_sqlx)?,
    })
}

/// PostgreSQL store for `tasks` (implements the worker's `TaskLifecyclePort`).
#[derive(Debug, Clone)]
pub struct TaskStore {
    pool: PgPool,
}

impl TaskStore {
    /// Binds a store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Raw pool access for the wake-up broker's claim scan.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Loads one task row by primary key.
    pub async fn load(&self, task_id: TaskId) -> Result<Option<Task>> {
        let row = sqlx::query(&format!("SELECT {TASK_COLUMNS} FROM tasks WHERE id = $1"))
            .bind(Uuid::from(task_id))
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.map(fetch_task)
            .transpose()?
            .map(TaskRow::into_domain)
            .transpose()
    }

    /// Creates or rewrites the row (idempotent on
    /// `(tenant_id, idempotency_key)` via the schema's unique key — a
    /// duplicate surfaces as `AppError::conflict`, never a row rewrite).
    pub async fn save(&self, task: &Task) -> Result<()> {
        let row = TaskRow::from_domain(task)?;
        sqlx::query(TASK_UPSERT)
            .bind(row.id)
            .bind(row.tenant_id)
            .bind(row.organization_id)
            .bind(row.project_id)
            .bind(row.execution_id)
            .bind(&row.idempotency_key)
            .bind(&row.kind)
            .bind(&row.status)
            .bind(&row.priority)
            .bind(&row.payload)
            .bind(&row.result)
            .bind(row.not_before)
            .bind(row.deadline_at)
            .bind(&row.correlation_id)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|error| match &error {
                sqlx::Error::Database(db) if db.is_unique_violation() => {
                    mas_common::error::AppError::conflict(format!(
                        "task idempotency key '{}' already used in this tenant",
                        row.idempotency_key
                    ))
                },
                _ => map_sqlx(error),
            })?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl mas_worker::lifecycle::TaskLifecyclePort for TaskStore {
    async fn load(&self, task_id: TaskId) -> Result<Option<Task>> {
        TaskStore::load(self, task_id).await
    }

    async fn save(&self, task: &Task) -> Result<()> {
        TaskStore::save(self, task).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_arity_matches_projection() {
        assert_eq!(TASK_UPSERT.matches('$').count(), 16);
        assert_eq!(TASK_COLUMNS.split(',').count(), 16);
    }

    #[test]
    fn settlement_columns_are_rw() {
        for column in ["status", "result", "not_before", "updated_at"] {
            assert!(
                TASK_UPSERT.contains(column),
                "settlement column {column} not upserted"
            );
        }
        // identity columns deliberately excluded from the UPDATE SET:
        for frozen in ["idempotency_key", "tenant_id", "kind"] {
            let update_window = TASK_UPSERT.split("DO UPDATE SET").nth(1).expect("update");
            assert!(
                !update_window.contains(&format!("{frozen} = EXCLUDED.")),
                "frozen column {frozen} became mutable"
            );
        }
    }
}
