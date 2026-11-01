//! PURPOSE: RLS-scoped PostgreSQL persistence for tool definitions and grants.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: implement the tenant-scoped tool port over the
//! existing `tools` and `tool_permissions` tables, with bounded keyset reads,
//! optimistic version writes, and savepoint-atomic permission replacement.
//!
//! Data ownership: PostgreSQL owns all tool aggregates and permission rows;
//! request scope and FORCE RLS remain the authority.
//!
//! MUST NOT: open an unscoped pool connection, bypass RLS, store secret bytes,
//! perform tool invocation, or build SQL from caller-controlled identifiers.

use mas_application::stores::{ToolCursor, ToolStorePort};
use mas_common::error::AppError;
use mas_common::ids::{AgentId, TenantId, ToolId};
use mas_common::result::Result;
use mas_domain::{Tool, ToolPermission};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const TOOL_COLUMNS: &str = "id, tenant_id, name, kind, spec, created_at, updated_at";
const MAX_TOOL_ROWS: i64 = 1_001;

/// Production implementation over `tools` and `tool_permissions`.
#[derive(Debug, Clone)]
pub struct PostgresToolStore {
    pool: PgPool,
}

impl PostgresToolStore {
    /// Binds the adapter to the normal request pool; every method uses the
    /// request transaction rather than acquiring from this handle directly.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool accessor for readiness wiring only.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl ToolStorePort for PostgresToolStore {
    async fn save(&self, tool: &Tool) -> Result<()> {
        tool.validate()?;
        let scope = crate::scoped_request::current_scope()?;
        if scope.tenant_id() != tool.tenant_id || scope.organization_id() != tool.organization_id {
            return Err(AppError::not_found("tool", tool.id.to_string()));
        }
        let spec = serde_json::to_value(tool)
            .map_err(|error| AppError::serialization(format!("tool aggregate: {error}")))?;
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "INSERT INTO tools (id, tenant_id, name, kind, spec, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                kind = EXCLUDED.kind,
                spec = EXCLUDED.spec,
                updated_at = EXCLUDED.updated_at
             WHERE tools.tenant_id = EXCLUDED.tenant_id
               AND COALESCE((tools.spec->>'version')::bigint, 0) =
                   COALESCE((EXCLUDED.spec->>'version')::bigint, 0) - 1",
        )
        .bind(tool.id.into_uuid())
        .bind(tool.tenant_id.into_uuid())
        .bind(&tool.name)
        .bind(tool.kind.to_string())
        .bind(spec)
        .bind(tool.created_at.into_datetime())
        .bind(tool.updated_at.into_datetime())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::conflict(
                "tool version changed concurrently or the tool belongs to another tenant",
            ));
        }
        Ok(())
    }

    async fn get(&self, id: ToolId) -> Result<Option<Tool>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {TOOL_COLUMNS} FROM tools WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(tool_from_row).transpose()
    }

    async fn list(
        &self,
        tenant: TenantId,
        project: Option<mas_common::ids::ProjectId>,
    ) -> Result<Vec<Tool>> {
        ensure_tenant(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {TOOL_COLUMNS} FROM tools
             WHERE tenant_id = $1 AND ($2::uuid IS NULL OR spec->>'project_id' = $2::text)
             ORDER BY created_at DESC, id DESC LIMIT $3"
        ))
        .bind(tenant.into_uuid())
        .bind(project.map(|id| id.into_uuid()))
        .bind(MAX_TOOL_ROWS)
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(tool_from_row).collect()
    }

    async fn list_page(
        &self,
        tenant: TenantId,
        project: Option<mas_common::ids::ProjectId>,
        after: Option<ToolCursor>,
        limit: u16,
    ) -> Result<Vec<Tool>> {
        ensure_tenant(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {TOOL_COLUMNS} FROM tools
             WHERE tenant_id = $1
               AND ($2::uuid IS NULL OR spec->>'project_id' = $2::text)
               AND ($3::timestamptz IS NULL OR created_at < $3 OR (created_at = $3 AND id < $4))
             ORDER BY created_at DESC, id DESC LIMIT $5"
        ))
        .bind(tenant.into_uuid())
        .bind(project.map(|id| id.into_uuid()))
        .bind(after.map(|cursor| cursor.created_at.into_datetime()))
        .bind(after.map(|cursor| cursor.id.into_uuid()))
        .bind(i64::from(limit.clamp(1, 201)))
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(tool_from_row).collect()
    }

    async fn permissions(&self, tenant: TenantId, tool: ToolId) -> Result<Vec<ToolPermission>> {
        ensure_tenant(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let exists = sqlx::query("SELECT 1 FROM tools WHERE tenant_id = $1 AND id = $2")
            .bind(tenant.into_uuid())
            .bind(tool.into_uuid())
            .fetch_optional(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?
            .is_some();
        if !exists {
            return Err(AppError::not_found("tool", tool.to_string()));
        }
        let rows = sqlx::query(
            "SELECT grant_spec FROM tool_permissions WHERE tenant_id = $1 AND tool_id = $2 ORDER BY subject ASC LIMIT 513"
        ).bind(tenant.into_uuid()).bind(tool.into_uuid())
            .fetch_all(scoped.connection().as_mut()).await.map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let value: Value = row.try_get("grant_spec").map_err(map_sqlx)?;
                serde_json::from_value(value).map_err(|_| {
                    AppError::database("tool permission row violates its stored contract")
                })
            })
            .collect()
    }

    async fn replace_permissions(
        &self,
        tenant: TenantId,
        tool: ToolId,
        permissions: &[ToolPermission],
    ) -> Result<()> {
        ensure_permission_set(tenant, tool, permissions)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let connection = scoped.connection().as_mut();
        sqlx::query("SAVEPOINT tool_permission_replace")
            .execute(&mut *connection)
            .await
            .map_err(map_sqlx)?;
        let operation = async {
            let exists = sqlx::query("SELECT 1 FROM tools WHERE tenant_id = $1 AND id = $2")
                .bind(tenant.into_uuid())
                .bind(tool.into_uuid())
                .fetch_optional(&mut *connection)
                .await
                .map_err(map_sqlx)?
                .is_some();
            if !exists {
                return Err(AppError::not_found("tool", tool.to_string()));
            }
            sqlx::query("DELETE FROM tool_permissions WHERE tenant_id = $1 AND tool_id = $2")
                .bind(tenant.into_uuid())
                .bind(tool.into_uuid())
                .execute(&mut *connection)
                .await
                .map_err(map_sqlx)?;
            for permission in permissions {
                insert_permission(&mut *connection, permission).await?;
            }
            Ok(())
        }
        .await;
        match operation {
            Ok(()) => {
                sqlx::query("RELEASE SAVEPOINT tool_permission_replace")
                    .execute(&mut *connection)
                    .await
                    .map_err(map_sqlx)?;
                Ok(())
            },
            Err(error) => {
                let _ = sqlx::query("ROLLBACK TO SAVEPOINT tool_permission_replace")
                    .execute(&mut *connection)
                    .await;
                let _ = sqlx::query("RELEASE SAVEPOINT tool_permission_replace")
                    .execute(&mut *connection)
                    .await;
                Err(error)
            },
        }
    }

    async fn replace_agent_permissions(
        &self,
        tenant: TenantId,
        agent: AgentId,
        permissions: &[ToolPermission],
    ) -> Result<()> {
        if permissions
            .iter()
            .any(|permission| permission.tenant_id != tenant || permission.agent_id != Some(agent))
        {
            return Err(AppError::invalid_field(
                "permissions",
                "scope_mismatch",
                "all grants must match the tenant and target agent",
            ));
        }
        ensure_tenant(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let connection = scoped.connection().as_mut();
        sqlx::query("SAVEPOINT agent_tool_permission_replace")
            .execute(&mut *connection)
            .await
            .map_err(map_sqlx)?;
        let operation = async {
            let agent_exists = sqlx::query("SELECT 1 FROM agents WHERE tenant_id = $1 AND id = $2")
                .bind(tenant.into_uuid()).bind(agent.into_uuid()).fetch_optional(&mut *connection).await.map_err(map_sqlx)?.is_some();
            if !agent_exists { return Err(AppError::not_found("agent", agent.to_string())); }
            for permission in permissions {
                let exists = sqlx::query("SELECT 1 FROM tools WHERE tenant_id = $1 AND id = $2")
                    .bind(tenant.into_uuid()).bind(permission.tool_id.into_uuid()).fetch_optional(&mut *connection).await.map_err(map_sqlx)?.is_some();
                if !exists { return Err(AppError::not_found("tool", permission.tool_id.to_string())); }
            }
            sqlx::query("DELETE FROM tool_permissions WHERE tenant_id = $1 AND grant_spec->>'agent_id' = $2")
                .bind(tenant.into_uuid()).bind(agent.to_string()).execute(&mut *connection).await.map_err(map_sqlx)?;
            for permission in permissions { insert_permission(&mut *connection, permission).await?; }
            Ok(())
        }.await;
        match operation {
            Ok(()) => {
                sqlx::query("RELEASE SAVEPOINT agent_tool_permission_replace")
                    .execute(&mut *connection)
                    .await
                    .map_err(map_sqlx)?;
                Ok(())
            },
            Err(error) => {
                let _ = sqlx::query("ROLLBACK TO SAVEPOINT agent_tool_permission_replace")
                    .execute(&mut *connection)
                    .await;
                let _ = sqlx::query("RELEASE SAVEPOINT agent_tool_permission_replace")
                    .execute(&mut *connection)
                    .await;
                Err(error)
            },
        }
    }
}

async fn insert_permission(
    connection: &mut sqlx::PgConnection,
    permission: &ToolPermission,
) -> Result<()> {
    let subject = permission_subject(permission);
    let grant_spec = serde_json::to_value(permission)
        .map_err(|error| AppError::serialization(format!("tool permission: {error}")))?;
    sqlx::query("INSERT INTO tool_permissions (id, tenant_id, tool_id, subject, grant_spec, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $6)")
        .bind(Uuid::now_v7()).bind(permission.tenant_id.into_uuid()).bind(permission.tool_id.into_uuid())
        .bind(subject).bind(grant_spec).bind(permission.created_at.into_datetime())
        .execute(connection).await.map_err(map_sqlx)?;
    Ok(())
}

fn permission_subject(permission: &ToolPermission) -> String {
    format!(
        "project={};agent={};role={}",
        permission
            .project_id
            .map_or_else(|| "*".to_owned(), |id| id.to_string()),
        permission
            .agent_id
            .map_or_else(|| "*".to_owned(), |id| id.to_string()),
        permission
            .role
            .map_or_else(|| "*".to_owned(), |role| role.to_string())
    )
}

fn ensure_permission_set(
    tenant: TenantId,
    tool: ToolId,
    permissions: &[ToolPermission],
) -> Result<()> {
    if permissions
        .iter()
        .any(|permission| permission.tenant_id != tenant || permission.tool_id != tool)
    {
        return Err(AppError::invalid_field(
            "permissions",
            "scope_mismatch",
            "permission set does not match its tenant and tool",
        ));
    }
    Ok(())
}

fn ensure_tenant(tenant: TenantId) -> Result<()> {
    if crate::scoped_request::current_scope()?.tenant_id() != tenant {
        return Err(AppError::not_found("tenant", tenant.to_string()));
    }
    Ok(())
}

fn tool_from_row(row: sqlx::postgres::PgRow) -> Result<Tool> {
    let tenant_id = TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?);
    let id = ToolId::from_uuid(row.try_get::<Uuid, _>("id").map_err(map_sqlx)?);
    let spec: Value = row.try_get("spec").map_err(map_sqlx)?;
    let tool: Tool = serde_json::from_value(spec)
        .map_err(|_| AppError::database("tool spec violates the domain contract"))?;
    if tool.id != id || tool.tenant_id != tenant_id {
        return Err(AppError::database(
            "tool projection and aggregate ownership disagree",
        ));
    }
    tool.validate()
        .map_err(|_| AppError::database("stored tool violates domain invariants"))?;
    Ok(tool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{OrganizationId, ProjectId};
    use mas_domain::{SafetyClassification, ToolKind, ToolRuntime};

    #[test]
    fn page_bound_and_permission_scope_are_fail_closed() {
        assert_eq!(MAX_TOOL_ROWS, 1_001);
        let invalid = ToolPermission {
            tool_id: ToolId::new(),
            tenant_id: TenantId::new(),
            project_id: None,
            agent_id: None,
            role: None,
            allowed_actions: Default::default(),
            created_at: mas_common::timestamps::Timestamp::now(),
        };
        assert!(ensure_permission_set(TenantId::new(), invalid.tool_id, &[invalid]).is_err());
    }

    #[test]
    fn aggregate_is_complete_and_serializable() {
        let tool = Tool::register(
            TenantId::new(),
            OrganizationId::new(),
            Some(ProjectId::new()),
            "safe-tool",
            ToolKind::Internal,
            serde_json::json!({"type":"object"}),
            None,
        )
        .expect("tool");
        assert!(serde_json::to_value(&tool).expect("json").is_object());
        assert_eq!(tool.safety, SafetyClassification::Standard);
        assert_eq!(tool.runtime, ToolRuntime::default());
    }
}
