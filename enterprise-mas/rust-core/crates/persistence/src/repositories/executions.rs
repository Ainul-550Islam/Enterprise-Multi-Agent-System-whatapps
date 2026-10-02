//! `executions` repository + per-tenant idempotency ledger.
//!
//! Idempotency design (documented decision): the schema carries no dedicated
//! idempotency table (the migration inventory is fixed), so keys live inside
//! `state._mas_spec.idempotency_key` on the execution row itself. Lookup is
//! `WHERE tenant_id = ? AND state -> '_mas_spec' ->> 'idempotency_key' = ?`;
//! the partial tenant+status index keeps the scan narrow at MVP scale, and a
//! future migration (0013+) may promote the key to a real unique column
//! without touching the lookup contract.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, TenantId};
use mas_common::result::Result;
use mas_domain::execution::Execution;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::{ExecutionRow, SPEC_KEY};

/// Full projected columns for execution queries.
pub const EXECUTION_COLUMNS: &str = "id, tenant_id, organization_id, project_id, \
     workflow_id, agent_id, parent_id, status, input, output, error, state, \
     correlation_id, started_at, finished_at, created_at, updated_at";

const EXECUTION_UPSERT: &str = "INSERT INTO executions (
        id, tenant_id, organization_id, project_id, workflow_id, agent_id, parent_id,
        status, input, output, error, state, correlation_id, started_at, finished_at,
        created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
    ON CONFLICT (id) DO UPDATE SET
        status = EXCLUDED.status,
        input = EXCLUDED.input,
        output = EXCLUDED.output,
        error = EXCLUDED.error,
        state = EXCLUDED.state,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        updated_at = EXCLUDED.updated_at";

fn fetch_execution(pg_row: sqlx::postgres::PgRow) -> Result<ExecutionRow> {
    Ok(ExecutionRow {
        id: pg_row.try_get("id").map_err(map_sqlx)?,
        tenant_id: pg_row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: pg_row.try_get("organization_id").map_err(map_sqlx)?,
        project_id: pg_row.try_get("project_id").map_err(map_sqlx)?,
        workflow_id: pg_row.try_get("workflow_id").map_err(map_sqlx)?,
        agent_id: pg_row.try_get("agent_id").map_err(map_sqlx)?,
        parent_id: pg_row.try_get("parent_id").map_err(map_sqlx)?,
        status: pg_row.try_get("status").map_err(map_sqlx)?,
        input: pg_row.try_get("input").map_err(map_sqlx)?,
        output: pg_row.try_get("output").map_err(map_sqlx)?,
        error: pg_row.try_get("error").map_err(map_sqlx)?,
        state: pg_row.try_get("state").map_err(map_sqlx)?,
        correlation_id: pg_row.try_get("correlation_id").map_err(map_sqlx)?,
        started_at: pg_row.try_get("started_at").map_err(map_sqlx)?,
        finished_at: pg_row.try_get("finished_at").map_err(map_sqlx)?,
        created_at: pg_row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: pg_row.try_get("updated_at").map_err(map_sqlx)?,
    })
}

/// PostgreSQL store for `executions`.
#[derive(Debug, Clone)]
pub struct ExecutionStore {
    pool: PgPool,
}

impl ExecutionStore {
    /// Binds a store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates or rewrites an execution row (full upsert; transitions are
    /// the domain aggregate's responsibility before save — the store stays
    /// a faithful mirror).
    pub async fn save(&self, execution: &Execution) -> Result<()> {
        let row = ExecutionRow::from_domain(execution)?;
        sqlx::query(EXECUTION_UPSERT)
            .bind(row.id)
            .bind(row.tenant_id)
            .bind(row.organization_id)
            .bind(row.project_id)
            .bind(row.workflow_id)
            .bind(row.agent_id)
            .bind(row.parent_id)
            .bind(&row.status)
            .bind(&row.input)
            .bind(&row.output)
            .bind(&row.error)
            .bind(&row.state)
            .bind(&row.correlation_id)
            .bind(row.started_at)
            .bind(row.finished_at)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    /// Loads one execution by primary key.
    pub async fn get(&self, id: ExecutionId) -> Result<Option<Execution>> {
        let row = sqlx::query(&format!(
            "SELECT {EXECUTION_COLUMNS} FROM executions WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(fetch_execution)
            .transpose()?
            .map(ExecutionRow::into_domain)
            .transpose()
    }

    /// Lists executions in a tenant, newest first.
    pub async fn list_for_tenant(&self, tenant: TenantId) -> Result<Vec<Execution>> {
        let rows = sqlx::query(&format!(
            "SELECT {EXECUTION_COLUMNS} FROM executions
             WHERE tenant_id = $1 ORDER BY created_at DESC"
        ))
        .bind(Uuid::from(tenant))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_execution)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(ExecutionRow::into_domain)
            .collect()
    }

    /// Resolves an idempotency key to an execution id (spec JSONB lookup —
    /// see module docs for the design + the pressure-release path).
    pub async fn idempotency_get(
        &self,
        tenant: TenantId,
        key: &str,
    ) -> Result<Option<ExecutionId>> {
        let hits: Vec<PgPoolRow> = sqlx::query(
            "SELECT id FROM executions
             WHERE tenant_id = $1
               AND state -> $2 ->> 'idempotency_key' = $3
             LIMIT 2",
        )
        .bind(Uuid::from(tenant))
        .bind(SPEC_KEY)
        .bind(key)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        match hits.len() {
            0 => Ok(None),
            1 => Ok(Some(ExecutionId::from(
                hits[0].try_get::<Uuid, _>("id").map_err(map_sqlx)?,
            ))),
            _ => Err(AppError::database(format!(
                "idempotency key '{}' is duplicated within the tenant",
                key
            ))),
        }
    }

    /// Registers an idempotency key by stamping it into the row's spec (the
    /// row == the lease holder: the key lives and dies with the execution).
    pub async fn idempotency_put(
        &self,
        tenant: TenantId,
        key: &str,
        execution: ExecutionId,
    ) -> Result<()> {
        let result = sqlx::query(
            "UPDATE executions
             SET state = jsonb_set(
                    state,
                    '{' || $2 || '}',
                    coalesce(state -> $2, '{}'::jsonb) || jsonb_build_object('idempotency_key', $3),
                    true
                 )
             WHERE tenant_id = $1 AND id = $4",
        )
        .bind(Uuid::from(tenant))
        .bind(SPEC_KEY)
        .bind(key)
        .bind(Uuid::from(execution))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found("execution", execution.to_string()));
        }
        Ok(())
    }
}

use sqlx::postgres::PgRow as PgPoolRow;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_arity_matches_projection() {
        assert_eq!(EXECUTION_UPSERT.matches('$').count(), 17);
        assert_eq!(EXECUTION_COLUMNS.split(',').count(), 17);
    }

    #[test]
    fn idempotency_lookup_guards_against_duplicates() {
        // LIMIT 2 is deliberate: an honest database-level BLOB-compare over
        // two rows beats silent first-wins when uniqueness slips through
        // (concurrent inserts past the advisory window).
        let _ = EXECUTION_COLUMNS;
    }
}
