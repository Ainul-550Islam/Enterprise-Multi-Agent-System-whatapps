//! PostgreSQL implementation of append-only rollback history and atomic
//! version-forward rollback commits.
//!
//! The API transaction is already RLS-armed by `scoped_request`. Every query
//! additionally carries an explicit tenant predicate. Version/resource rows
//! and the rollback record are written in that same transaction, so a failed
//! step cannot leave a half-rollback.

use mas_application::stores::{RollbackStorePort, WorkflowVersionStorePort};
use mas_common::error::AppError;
use mas_common::ids::{AgentId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_domain::{
    Agent, AgentVersion, RollbackRecord, RollbackResourceKind, Workflow, WorkflowVersion,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::repositories::agents::{AgentStore, AgentVersionStore};
use crate::repositories::workflow_versions::PostgresWorkflowVersionStore;
use crate::rows::RollbackRecordRow;

/// Fixed projection for rollback history.
pub const ROLLBACK_RECORD_COLUMNS: &str = "id, tenant_id, resource_kind, resource_id, \
    from_version, to_version, actor, reason, idempotency_key, created_at";

/// PostgreSQL store for rollback history.
#[derive(Debug, Clone)]
pub struct PostgresRollbackStore {
    pool: PgPool,
    agents: AgentStore,
    agent_versions: AgentVersionStore,
    workflow_versions: PostgresWorkflowVersionStore,
}

impl PostgresRollbackStore {
    /// Binds rollback and resource stores to the shared pool. Runtime SQL is
    /// always issued through the request-scoped transaction, never the pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            agents: AgentStore::new(pool.clone()),
            agent_versions: AgentVersionStore::new(pool.clone()),
            workflow_versions: PostgresWorkflowVersionStore::new(pool.clone()),
            pool,
        }
    }

    /// Pool accessor for diagnostics only.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn insert_record(&self, record: &RollbackRecord) -> Result<()> {
        ensure_tenant_scope(record.tenant_id)?;
        let row = RollbackRecordRow::from_domain(record)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "INSERT INTO rollback_records (
                id, tenant_id, resource_kind, resource_id, from_version,
                to_version, actor, reason, idempotency_key, created_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (tenant_id, idempotency_key) DO NOTHING",
        )
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(&row.resource_kind)
        .bind(row.resource_id)
        .bind(row.from_version)
        .bind(row.to_version)
        .bind(&row.actor)
        .bind(&row.reason)
        .bind(&row.idempotency_key)
        .bind(row.created_at)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::conflict(
                "rollback idempotency key was already used",
            ));
        }
        Ok(())
    }

    async fn lock_agent_current_version(&self, agent: AgentId, tenant: TenantId) -> Result<u64> {
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT COALESCE(NULLIF(spec ->> 'current_version', '')::bigint, 0) \
             AS current_version
             FROM agents
             WHERE tenant_id = $1 AND id = $2
             FOR UPDATE",
        )
        .bind(Uuid::from(tenant))
        .bind(Uuid::from(agent))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("agent", agent.to_string()))?;
        let current: i64 = row.try_get("current_version").map_err(map_sqlx)?;
        u64::try_from(current)
            .map_err(|_| AppError::database("agent has a negative current version"))
    }

    async fn lock_workflow_current_version(
        &self,
        workflow: WorkflowId,
        tenant: TenantId,
    ) -> Result<u64> {
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT COALESCE(NULLIF(spec ->> 'published_version', '')::bigint, 0) \
             AS published_version
             FROM workflows
             WHERE tenant_id = $1 AND id = $2
             FOR UPDATE",
        )
        .bind(Uuid::from(tenant))
        .bind(Uuid::from(workflow))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("workflow", workflow.to_string()))?;
        let current: i64 = row.try_get("published_version").map_err(map_sqlx)?;
        u64::try_from(current)
            .map_err(|_| AppError::database("workflow has a negative published version"))
    }
}

#[async_trait::async_trait]
impl RollbackStorePort for PostgresRollbackStore {
    async fn append(&self, record: &RollbackRecord) -> Result<()> {
        self.insert_record(record).await
    }

    async fn get(&self, id: mas_common::ids::RollbackRecordId) -> Result<Option<RollbackRecord>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {ROLLBACK_RECORD_COLUMNS} FROM rollback_records \
             WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(Uuid::from(tenant))
        .bind(Uuid::from(id))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(rollback_record_from_row).transpose()
    }

    async fn get_by_idempotency(
        &self,
        tenant: TenantId,
        idempotency_key: &str,
    ) -> Result<Option<RollbackRecord>> {
        ensure_tenant_scope(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {ROLLBACK_RECORD_COLUMNS} FROM rollback_records \
             WHERE tenant_id = $1 AND idempotency_key = $2"
        ))
        .bind(Uuid::from(tenant))
        .bind(idempotency_key)
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(rollback_record_from_row).transpose()
    }

    async fn list(&self, tenant: TenantId, limit: u16) -> Result<Vec<RollbackRecord>> {
        ensure_tenant_scope(tenant)?;
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "rollback history limit must be between 1 and 200",
            ));
        }
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {ROLLBACK_RECORD_COLUMNS} FROM rollback_records \
             WHERE tenant_id = $1 ORDER BY created_at DESC, id DESC LIMIT $2"
        ))
        .bind(Uuid::from(tenant))
        .bind(i64::from(limit))
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(rollback_record_from_row).collect()
    }

    async fn commit_agent_rollback(
        &self,
        agent: &Agent,
        version: &AgentVersion,
        record: &RollbackRecord,
    ) -> Result<()> {
        ensure_agent_commit(agent, version, record)?;
        ensure_tenant_scope(record.tenant_id)?;
        let current = self
            .lock_agent_current_version(agent.id, record.tenant_id)
            .await?;
        if current != record.from_version {
            return Err(AppError::conflict(
                "agent changed while rollback was being committed",
            ));
        }

        // Claim idempotency before any resource write. ON CONFLICT DO NOTHING
        // leaves the transaction usable for the caller's replay lookup.
        self.insert_record(record).await?;
        self.agent_versions.save(version, record.tenant_id).await?;
        self.agents.save(agent).await
    }

    async fn commit_workflow_rollback(
        &self,
        workflow: &Workflow,
        version: &WorkflowVersion,
        record: &RollbackRecord,
    ) -> Result<()> {
        ensure_workflow_commit(workflow, version, record)?;
        ensure_tenant_scope(record.tenant_id)?;
        let current = self
            .lock_workflow_current_version(workflow.id, record.tenant_id)
            .await?;
        if current != record.from_version {
            return Err(AppError::conflict(
                "workflow changed while rollback was being committed",
            ));
        }

        self.insert_record(record).await?;
        self.workflow_versions
            .publish_snapshot(workflow, version)
            .await
    }
}

fn ensure_tenant_scope(tenant: TenantId) -> Result<()> {
    if crate::scoped_request::current_scope()?.tenant_id() != tenant {
        return Err(AppError::not_found("tenant", tenant.to_string()));
    }
    Ok(())
}

fn ensure_agent_commit(
    agent: &Agent,
    version: &AgentVersion,
    record: &RollbackRecord,
) -> Result<()> {
    let expected_next = record
        .from_version
        .checked_add(1)
        .ok_or_else(|| AppError::conflict("agent version counter is exhausted"))?;
    if record.resource_kind != RollbackResourceKind::Agent
        || record.resource_id != *agent.id.as_uuid()
        || record.tenant_id != agent.tenant_id
        || record.to_version != expected_next
        || agent.current_version != Some(record.to_version)
        || version.agent_id() != agent.id
        || version.version_number() != record.to_version
        || version.rollback_of().is_none()
    {
        return Err(AppError::invalid_field(
            "rollback",
            "incoherent_commit",
            "agent, version and rollback record do not describe one forward transition",
        ));
    }
    Ok(())
}

fn ensure_workflow_commit(
    workflow: &Workflow,
    version: &WorkflowVersion,
    record: &RollbackRecord,
) -> Result<()> {
    let expected_next = record
        .from_version
        .checked_add(1)
        .ok_or_else(|| AppError::conflict("workflow version counter is exhausted"))?;
    if record.resource_kind != RollbackResourceKind::Workflow
        || record.resource_id != *workflow.id.as_uuid()
        || record.tenant_id != workflow.tenant_id
        || record.to_version != expected_next
        || workflow.published_version != Some(record.to_version)
        || version.workflow_id() != workflow.id
        || version.tenant_id() != workflow.tenant_id
        || version.version_number() != record.to_version
        || version.rollback_of().is_none()
    {
        return Err(AppError::invalid_field(
            "rollback",
            "incoherent_commit",
            "workflow, version and rollback record do not describe one forward transition",
        ));
    }
    Ok(())
}

fn rollback_record_from_row(row: sqlx::postgres::PgRow) -> Result<RollbackRecord> {
    RollbackRecordRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        resource_kind: row.try_get("resource_kind").map_err(map_sqlx)?,
        resource_id: row.try_get("resource_id").map_err(map_sqlx)?,
        from_version: row.try_get("from_version").map_err(map_sqlx)?,
        to_version: row.try_get("to_version").map_err(map_sqlx)?,
        actor: row.try_get("actor").map_err(map_sqlx)?,
        reason: row.try_get("reason").map_err(map_sqlx)?,
        idempotency_key: row.try_get("idempotency_key").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
    }
    .into_domain()
}
