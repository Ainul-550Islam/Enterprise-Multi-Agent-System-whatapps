//! PostgreSQL persistence for the tenant-scoped execution-step timeline and
//! the execution runtime's finished-step store.
//!
//! Every method uses the request/worker task's authoritative scoped
//! transaction. The persisted detail object contains only step metadata and
//! opaque storage references; API projections deliberately omit those
//! references and payloads.

use std::collections::BTreeSet;

use mas_application::stores::ExecutionStepStorePort;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ExecutionStepId, TenantId};
use mas_common::result::Result;
use mas_domain::ExecutionStep;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::ExecutionStepRow;
use crate::scoped_request;

const STEP_COLUMNS: &str = "id, tenant_id, execution_id, node_key, ordinal, status, attempt, \
     detail, started_at, finished_at, created_at, updated_at";

// The first inserted ID is canonical for (execution_id, ordinal). The
// conflict-update list intentionally excludes `id`, so replaying a finished
// run updates its timeline contents without rotating external step references.
const STEP_UPSERT: &str = "INSERT INTO execution_steps (
        id, tenant_id, execution_id, node_key, ordinal, status, attempt, detail,
        started_at, finished_at, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT (execution_id, ordinal) DO UPDATE SET
        node_key = EXCLUDED.node_key,
        status = EXCLUDED.status,
        attempt = EXCLUDED.attempt,
        detail = EXCLUDED.detail,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        updated_at = EXCLUDED.updated_at
    WHERE execution_steps.tenant_id = EXCLUDED.tenant_id";

/// Shared PostgreSQL adapter for application timeline reads and the
/// execution runtime's batch step persistence.
#[derive(Debug, Clone)]
pub struct PostgresExecutionStepStore {
    /// Retained for consistent production composition and pool diagnostics.
    pool: PgPool,
}

impl PostgresExecutionStepStore {
    /// Binds the adapter to the shared application pool. Tenant-owned queries
    /// still require an active authoritative request/worker scope.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool accessor for composition diagnostics.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Persists a set of step rows in the current scoped transaction.
    pub async fn save_steps(
        &self,
        execution_id: ExecutionId,
        steps: &[ExecutionStep],
    ) -> Result<()> {
        let mut scoped = scoped_request::connection().await?;
        let tenant = scoped.scope().tenant_id();
        ensure_execution(&mut scoped, tenant, execution_id).await?;
        let mut sequences = BTreeSet::new();
        for step in steps {
            if step.execution_id != execution_id {
                return Err(AppError::invalid_field(
                    "execution_id",
                    "mismatch",
                    "all persisted steps must belong to the requested execution",
                ));
            }
            if !sequences.insert(step.sequence) {
                return Err(AppError::invalid_field(
                    "sequence",
                    "duplicate",
                    "an execution cannot persist two steps at the same sequence",
                ));
            }
            save_step_on(&mut scoped, tenant, step).await?;
        }
        Ok(())
    }

    /// Reads all steps for an execution in stable timeline order from the
    /// current scoped transaction.
    pub async fn load_steps(&self, execution_id: ExecutionId) -> Result<Vec<ExecutionStep>> {
        self.list(execution_id).await
    }
}

#[async_trait::async_trait]
impl ExecutionStepStorePort for PostgresExecutionStepStore {
    async fn save(&self, step: &ExecutionStep) -> Result<()> {
        let mut scoped = scoped_request::connection().await?;
        let tenant = scoped.scope().tenant_id();
        ensure_execution(&mut scoped, tenant, step.execution_id).await?;
        save_step_on(&mut scoped, tenant, step).await
    }

    async fn get(&self, id: ExecutionStepId) -> Result<Option<ExecutionStep>> {
        let mut scoped = scoped_request::connection().await?;
        let tenant = scoped.scope().tenant_id();
        let row = sqlx::query(&format!(
            "SELECT {STEP_COLUMNS} FROM execution_steps
             WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(Uuid::from(tenant))
        .bind(Uuid::from(id))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(fetch_step).transpose()
    }

    async fn list(&self, execution: ExecutionId) -> Result<Vec<ExecutionStep>> {
        let mut scoped = scoped_request::connection().await?;
        let tenant = scoped.scope().tenant_id();
        let rows = sqlx::query(&format!(
            "SELECT {STEP_COLUMNS} FROM execution_steps
             WHERE tenant_id = $1 AND execution_id = $2
             ORDER BY ordinal ASC, attempt ASC, id ASC"
        ))
        .bind(Uuid::from(tenant))
        .bind(Uuid::from(execution))
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(fetch_step).collect()
    }
}

#[async_trait::async_trait]
impl mas_execution::executor::StepStorePort for PostgresExecutionStepStore {
    async fn save_steps(&self, execution_id: ExecutionId, steps: &[ExecutionStep]) -> Result<()> {
        PostgresExecutionStepStore::save_steps(self, execution_id, steps).await
    }

    async fn load_steps(&self, execution_id: ExecutionId) -> Result<Vec<ExecutionStep>> {
        PostgresExecutionStepStore::load_steps(self, execution_id).await
    }
}

async fn ensure_execution(
    scoped: &mut crate::scoped_request::ScopedExecutor,
    tenant: TenantId,
    execution: ExecutionId,
) -> Result<()> {
    let found =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM executions WHERE tenant_id = $1 AND id = $2")
            .bind(Uuid::from(tenant))
            .bind(Uuid::from(execution))
            .fetch_optional(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?;
    found
        .map(|_| ())
        .ok_or_else(|| AppError::not_found("execution", execution.to_string()))
}

async fn save_step_on(
    scoped: &mut crate::scoped_request::ScopedExecutor,
    tenant: TenantId,
    step: &ExecutionStep,
) -> Result<()> {
    let row = ExecutionStepRow::from_domain(step, tenant)?;
    let result = sqlx::query(STEP_UPSERT)
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(row.execution_id)
        .bind(&row.node_key)
        .bind(row.ordinal)
        .bind(&row.status)
        .bind(row.attempt)
        .bind(&row.detail)
        .bind(row.started_at)
        .bind(row.finished_at)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
    if result.rows_affected() != 1 {
        return Err(AppError::conflict(
            "execution step sequence is already owned by another tenant",
        ));
    }
    Ok(())
}

fn fetch_step(row: PgRow) -> Result<ExecutionStep> {
    ExecutionStepRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        execution_id: row.try_get("execution_id").map_err(map_sqlx)?,
        node_key: row.try_get("node_key").map_err(map_sqlx)?,
        ordinal: row.try_get("ordinal").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        attempt: row.try_get("attempt").map_err(map_sqlx)?,
        detail: row.try_get("detail").map_err(map_sqlx)?,
        started_at: row.try_get("started_at").map_err(map_sqlx)?,
        finished_at: row.try_get("finished_at").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
    }
    .into_domain()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_keeps_the_first_persisted_id_for_an_execution_ordinal() {
        let normalized = STEP_UPSERT.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(normalized.contains("ON CONFLICT (execution_id, ordinal) DO UPDATE SET"));
        let update = normalized
            .split_once("DO UPDATE SET")
            .expect("upsert conflict update")
            .1;
        let assignments = update
            .split("WHERE execution_steps.tenant_id = EXCLUDED.tenant_id")
            .next()
            .expect("tenant ownership fence");
        assert!(
            !assignments
                .split(',')
                .any(|assignment| assignment.trim_start().starts_with("id =")),
            "a replay must not replace the canonical persisted step id"
        );
        assert!(normalized.contains("WHERE execution_steps.tenant_id = EXCLUDED.tenant_id"));
    }
}
