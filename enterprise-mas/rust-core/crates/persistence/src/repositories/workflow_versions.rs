//! PostgreSQL persistence for immutable workflow publish snapshots.
//!
//! All statements use the request-scoped connection, so snapshot insertion and
//! the workflow version-pointer update participate in the same RLS-armed
//! transaction as the rest of the request.

use mas_application::stores::WorkflowVersionStorePort;
use mas_common::error::AppError;
use mas_common::ids::{TenantId, WorkflowId, WorkflowVersionId};
use mas_common::result::Result;
use mas_domain::{Workflow, WorkflowVersion};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::WorkflowVersionRow;

/// Fixed projection for immutable workflow snapshots.
pub const WORKFLOW_VERSION_COLUMNS: &str = "id, tenant_id, workflow_id, version, graph, \
    topo_order, checksum, rollback_of, created_by, created_at";

/// PostgreSQL store for workflow version history.
#[derive(Debug, Clone)]
pub struct PostgresWorkflowVersionStore {
    pool: PgPool,
}

impl PostgresWorkflowVersionStore {
    /// Binds the store to the process pool. Queries still use only the
    /// request-scoped transaction.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool accessor for diagnostics; never use it for tenant-owned SQL.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn insert_snapshot(&self, version: &WorkflowVersion) -> Result<()> {
        let row = WorkflowVersionRow::from_domain(version)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "INSERT INTO workflow_versions (
                id, tenant_id, workflow_id, version, graph, topo_order,
                checksum, rollback_of, created_by, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (tenant_id, workflow_id, version) DO NOTHING",
        )
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(row.workflow_id)
        .bind(row.version)
        .bind(&row.graph)
        .bind(&row.topo_order)
        .bind(&row.checksum)
        .bind(row.rollback_of)
        .bind(row.created_by)
        .bind(row.created_at)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::conflict(format!(
                "workflow {} version {} already exists; published snapshots are immutable",
                row.workflow_id, row.version
            )));
        }
        Ok(())
    }

    async fn lock_workflow_and_check_current(
        &self,
        workflow_id: WorkflowId,
        tenant_id: TenantId,
        expected_current: u64,
    ) -> Result<()> {
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT COALESCE(NULLIF(spec ->> 'published_version', '')::bigint, 0) \
             AS published_version
             FROM workflows
             WHERE tenant_id = $1 AND id = $2
             FOR UPDATE",
        )
        .bind(Uuid::from(tenant_id))
        .bind(Uuid::from(workflow_id))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("workflow", workflow_id.to_string()))?;
        let current: i64 = row.try_get("published_version").map_err(map_sqlx)?;
        let current = u64::try_from(current)
            .map_err(|_| AppError::database("workflow has a negative published version"))?;
        if current != expected_current {
            return Err(AppError::conflict(format!(
                "workflow publish is stale: expected current version {expected_current}, found {current}"
            )));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl WorkflowVersionStorePort for PostgresWorkflowVersionStore {
    async fn append(&self, version: &WorkflowVersion) -> Result<()> {
        self.insert_snapshot(version).await
    }

    async fn publish_snapshot(&self, workflow: &Workflow, version: &WorkflowVersion) -> Result<()> {
        if version.tenant_id() != workflow.tenant_id
            || version.workflow_id() != workflow.id
            || workflow.published_version != Some(version.version_number())
        {
            return Err(AppError::validation(
                "workflow and published snapshot identity/version do not match",
            ));
        }
        let previous = version
            .version_number()
            .checked_sub(1)
            .ok_or_else(|| AppError::validation("workflow version numbers start at one"))?;
        self.lock_workflow_and_check_current(workflow.id, workflow.tenant_id, previous)
            .await?;
        self.insert_snapshot(version).await?;
        // `WorkflowStore` uses the same task-local transaction. If this fails,
        // the request transaction rolls back the inserted immutable snapshot.
        super::workflows::WorkflowStore::new(self.pool.clone())
            .save(workflow)
            .await
    }

    async fn get(&self, id: WorkflowVersionId) -> Result<Option<WorkflowVersion>> {
        let tenant_id = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {WORKFLOW_VERSION_COLUMNS} FROM workflow_versions \
             WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(Uuid::from(tenant_id))
        .bind(Uuid::from(id))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(workflow_version_from_row).transpose()
    }

    async fn get_number(
        &self,
        workflow: WorkflowId,
        version_number: u64,
    ) -> Result<Option<WorkflowVersion>> {
        let tenant_id = crate::scoped_request::current_scope()?.tenant_id();
        let version_number = i32::try_from(version_number)
            .map_err(|_| AppError::invalid_field("version_number", "out_of_range", "too large"))?;
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {WORKFLOW_VERSION_COLUMNS} FROM workflow_versions \
             WHERE tenant_id = $1 AND workflow_id = $2 AND version = $3"
        ))
        .bind(Uuid::from(tenant_id))
        .bind(Uuid::from(workflow))
        .bind(version_number)
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(workflow_version_from_row).transpose()
    }

    async fn list(&self, workflow: WorkflowId) -> Result<Vec<WorkflowVersion>> {
        let tenant_id = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {WORKFLOW_VERSION_COLUMNS} FROM workflow_versions \
             WHERE tenant_id = $1 AND workflow_id = $2 ORDER BY version DESC"
        ))
        .bind(Uuid::from(tenant_id))
        .bind(Uuid::from(workflow))
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(workflow_version_from_row).collect()
    }
}

fn workflow_version_from_row(row: sqlx::postgres::PgRow) -> Result<WorkflowVersion> {
    WorkflowVersionRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        workflow_id: row.try_get("workflow_id").map_err(map_sqlx)?,
        version: row.try_get("version").map_err(map_sqlx)?,
        graph: row.try_get("graph").map_err(map_sqlx)?,
        topo_order: row.try_get("topo_order").map_err(map_sqlx)?,
        checksum: row.try_get("checksum").map_err(map_sqlx)?,
        rollback_of: row.try_get("rollback_of").map_err(map_sqlx)?,
        created_by: row.try_get("created_by").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
    }
    .into_domain()
}
