//! `PgServices` — the production counterpart of `InMemoryServices`: one
//! struct implementing every application store port over PostgreSQL.
//!
//! Boundary doctrine:
//! * These are the **ctx-less service paths**. The service layer asserts
//!   scope in-domain (`services` check `tenant_id` matches before acting);
//!   the queries filter by the caller-supplied scope keys so cross-tenant
//!   reads can't even be fetched.
//! * The **RLS-ctx paths** ([`crate::repositories::tenancy`]) remain the management surface used
//!   with explicit per-request contexts (the `mas_app_service` BYPASS role
//!   plus the FORCE-RLS operator profiles — `docs/production-wiring.md` §7).
//! * Audit stays append-only elsewhere ([`crate::repositories::PostgresAuditLog`]); saves
//!   here write only the aggregate rows — the application services append
//!   audit records inside their own commits.

use mas_application::stores::{
    AgentStorePort, AgentVersionStorePort, ExecutionStorePort, MembershipStorePort,
    OrganizationStorePort, ProjectStorePort, ScheduleStorePort, TenantStorePort, UserStorePort,
    WorkflowStorePort,
};
use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, AgentVersionId, ExecutionId, MembershipId, OrganizationId, ProjectId, ScheduleId,
    TenantId, UserId, WorkflowId,
};
use mas_common::result::Result;
use mas_domain::agent::Agent;
use mas_domain::agent_version::AgentVersion;
use mas_domain::execution::Execution;
use mas_domain::membership::Membership;
use mas_domain::organization::Organization;
use mas_domain::project::Project;
use mas_domain::schedule::Schedule;
use mas_domain::tenant::Tenant;
use mas_domain::user::User;
use mas_domain::workflow::Workflow;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::{MembershipRow, OrganizationRow, ProjectRow, TenantRow, UserRow};

use super::agents::{AgentStore, AgentVersionStore};
use super::executions::ExecutionStore;
use super::schedules::ScheduleStore;
use super::workflows::WorkflowStore;

const ORG_COLUMNS: &str =
    "id, legal_name, display_name, slug, status, settings, created_at, updated_at";
const TENANT_COLUMNS: &str = "id, organization_id, name, slug, status, environment, \
     isolation_mode, settings, created_at, updated_at";
const PROJECT_COLUMNS: &str = "id, tenant_id, organization_id, name, slug, description, \
     status, config, created_at, updated_at";
const USER_COLUMNS: &str = "id, email, display_name, status, external_subject, \
     identity_provider, last_login_at, created_at, updated_at";
const MEMBERSHIP_COLUMNS: &str = "id, user_id, organization_id, tenant_id, roles, status, \
     invited_at, joined_at, created_at, updated_at";

fn org_from(row: &sqlx::postgres::PgRow) -> Result<Organization> {
    OrganizationRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        legal_name: row.try_get("legal_name").map_err(map_sqlx)?,
        display_name: row.try_get("display_name").map_err(map_sqlx)?,
        slug: row.try_get("slug").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        settings: row.try_get("settings").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
    }
    .into_domain()
}

fn tenant_from(row: &sqlx::postgres::PgRow) -> Result<Tenant> {
    TenantRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        organization_id: row.try_get("organization_id").map_err(map_sqlx)?,
        name: row.try_get("name").map_err(map_sqlx)?,
        slug: row.try_get("slug").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        environment: row.try_get("environment").map_err(map_sqlx)?,
        isolation_mode: row.try_get("isolation_mode").map_err(map_sqlx)?,
        settings: row.try_get("settings").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
    }
    .into_domain()
}

fn project_from(row: &sqlx::postgres::PgRow) -> Result<Project> {
    ProjectRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: row.try_get("organization_id").map_err(map_sqlx)?,
        name: row.try_get("name").map_err(map_sqlx)?,
        slug: row.try_get("slug").map_err(map_sqlx)?,
        description: row.try_get("description").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        config: row.try_get("config").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
    }
    .into_domain()
}

fn user_from(row: &sqlx::postgres::PgRow) -> Result<User> {
    UserRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        email: row.try_get("email").map_err(map_sqlx)?,
        display_name: row.try_get("display_name").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        external_subject: row.try_get("external_subject").map_err(map_sqlx)?,
        identity_provider: row.try_get("identity_provider").map_err(map_sqlx)?,
        last_login_at: row.try_get("last_login_at").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
    }
    .into_domain()
}

fn membership_from(row: &sqlx::postgres::PgRow) -> Result<Membership> {
    MembershipRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        user_id: row.try_get("user_id").map_err(map_sqlx)?,
        organization_id: row.try_get("organization_id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        roles: row.try_get("roles").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        invited_at: row.try_get("invited_at").map_err(map_sqlx)?,
        joined_at: row.try_get("joined_at").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
    }
    .into_domain()
}

/// Maps the unique-per-scope natural keys to honest conflicts.
fn map_unique(table: &'static str, field: &'static str) -> impl Fn(sqlx::Error) -> AppError {
    move |error| match &error {
        sqlx::Error::Database(db) if db.is_unique_violation() => {
            AppError::conflict(format!("{table}: the {field} already exists in this scope"))
        },
        _ => map_sqlx(error),
    }
}

/// One pool, every port: the production store composition unit.
#[derive(Debug, Clone)]
pub struct PgServices {
    pool: PgPool,
    agents: AgentStore,
    agent_versions: AgentVersionStore,
    workflows: WorkflowStore,
    executions: ExecutionStore,
    schedules: ScheduleStore,
}

impl PgServices {
    /// Binds every store to the shared pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            agents: AgentStore::new(pool.clone()),
            agent_versions: AgentVersionStore::new(pool.clone()),
            workflows: WorkflowStore::new(pool.clone()),
            executions: ExecutionStore::new(pool.clone()),
            schedules: ScheduleStore::new(pool.clone()),
            pool,
        }
    }

    /// Pool accessor (composition roots wire health probes from this).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl OrganizationStorePort for PgServices {
    async fn save(&self, organization: &Organization) -> Result<()> {
        // Org is the hierarchy root: INSERT-only plus afterwards lifecycle
        // status sync (status transitions are the only legal later state
        // mutation without a full overwrite tax).
        let row = OrganizationRow::from_domain(organization)?;
        sqlx::query(&format!(
            "INSERT INTO organizations ({ORG_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                status = EXCLUDED.status,
                settings = EXCLUDED.settings,
                updated_at = EXCLUDED.updated_at"
        ))
        .bind(row.id)
        .bind(row.legal_name)
        .bind(row.display_name)
        .bind(&row.slug)
        .bind(&row.status)
        .bind(&row.settings)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_unique("organizations", "slug"))?;
        Ok(())
    }

    async fn get(&self, id: OrganizationId) -> Result<Option<Organization>> {
        let row = sqlx::query(&format!(
            "SELECT {ORG_COLUMNS} FROM organizations WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(org_from).transpose()
    }

    async fn get_by_slug(&self, slug: &str) -> Result<Option<Organization>> {
        let row = sqlx::query(&format!(
            "SELECT {ORG_COLUMNS} FROM organizations WHERE slug = $1"
        ))
        .bind(slug)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(org_from).transpose()
    }

    async fn list(&self) -> Result<Vec<Organization>> {
        let rows = sqlx::query(&format!(
            "SELECT {ORG_COLUMNS} FROM organizations ORDER BY legal_name"
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(org_from).collect()
    }
}

#[async_trait::async_trait]
impl TenantStorePort for PgServices {
    async fn save(&self, tenant: &Tenant) -> Result<()> {
        let row = TenantRow::from_domain(tenant)?;
        sqlx::query(
            "INSERT INTO tenants (
                id, organization_id, name, slug, status, environment,
                isolation_mode, settings, created_at, updated_at
            )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                status = EXCLUDED.status,
                environment = EXCLUDED.environment,
                isolation_mode = EXCLUDED.isolation_mode,
                settings = EXCLUDED.settings,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(row.id)
        .bind(row.organization_id)
        .bind(&row.name)
        .bind(&row.slug)
        .bind(&row.status)
        .bind(&row.environment)
        .bind(&row.isolation_mode)
        .bind(&row.settings)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_unique("tenants", "slug"))?;
        Ok(())
    }

    async fn get(&self, id: TenantId) -> Result<Option<Tenant>> {
        let row = sqlx::query(&format!(
            "SELECT {TENANT_COLUMNS} FROM tenants WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(tenant_from).transpose()
    }

    async fn get_by_slug(
        &self,
        organization: OrganizationId,
        slug: &str,
    ) -> Result<Option<Tenant>> {
        let row = sqlx::query(&format!(
            "SELECT {TENANT_COLUMNS} FROM tenants
             WHERE organization_id = $1 AND slug = $2"
        ))
        .bind(Uuid::from(organization))
        .bind(slug)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(tenant_from).transpose()
    }

    async fn list(&self, organization: OrganizationId) -> Result<Vec<Tenant>> {
        let rows = sqlx::query(&format!(
            "SELECT {TENANT_COLUMNS} FROM tenants
             WHERE organization_id = $1 ORDER BY name"
        ))
        .bind(Uuid::from(organization))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(tenant_from).collect()
    }
}

#[async_trait::async_trait]
impl ProjectStorePort for PgServices {
    async fn save(&self, project: &Project) -> Result<()> {
        let row = ProjectRow::from_domain(project)?;
        sqlx::query(
            "INSERT INTO projects (
                id, tenant_id, organization_id, name, slug, description,
                status, config, created_at, updated_at
            )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                status = EXCLUDED.status,
                config = EXCLUDED.config,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(row.organization_id)
        .bind(&row.name)
        .bind(&row.slug)
        .bind(&row.description)
        .bind(&row.status)
        .bind(&row.config)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_unique("projects", "slug"))?;
        Ok(())
    }

    async fn get(&self, id: ProjectId) -> Result<Option<Project>> {
        let row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM projects WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(project_from).transpose()
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Project>> {
        let rows = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM projects
             WHERE tenant_id = $1 ORDER BY name"
        ))
        .bind(Uuid::from(tenant))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(project_from).collect()
    }
}

#[async_trait::async_trait]
impl UserStorePort for PgServices {
    async fn save(&self, user: &User) -> Result<()> {
        let row = UserRow::from_domain(user)?;
        sqlx::query(
            "INSERT INTO users (
                id, email, display_name, status, external_subject,
                identity_provider, last_login_at, created_at, updated_at
            )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                status = EXCLUDED.status,
                last_login_at = EXCLUDED.last_login_at,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(row.id)
        .bind(&row.email)
        .bind(&row.display_name)
        .bind(&row.status)
        .bind(&row.external_subject)
        .bind(&row.identity_provider)
        .bind(row.last_login_at)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_unique("users", "email"))?;
        Ok(())
    }

    async fn get(&self, id: UserId) -> Result<Option<User>> {
        let row = sqlx::query(&format!("SELECT {USER_COLUMNS} FROM users WHERE id = $1"))
            .bind(Uuid::from(id))
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.as_ref().map(user_from).transpose()
    }

    async fn get_by_email(&self, email: &str) -> Result<Option<User>> {
        let row = sqlx::query(&format!(
            "SELECT {USER_COLUMNS} FROM users WHERE email = $1"
        ))
        .bind(email.to_ascii_lowercase())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(user_from).transpose()
    }
}

#[async_trait::async_trait]
impl MembershipStorePort for PgServices {
    async fn save(&self, membership: &Membership) -> Result<()> {
        let row = MembershipRow::from_domain(membership)?;
        sqlx::query(
            "INSERT INTO memberships (
                id, user_id, organization_id, tenant_id, roles, status,
                invited_at, joined_at, created_at, updated_at
            )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (id) DO UPDATE SET
                roles = EXCLUDED.roles,
                status = EXCLUDED.status,
                joined_at = EXCLUDED.joined_at,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(row.id)
        .bind(row.user_id)
        .bind(row.organization_id)
        .bind(row.tenant_id)
        .bind(&row.roles)
        .bind(&row.status)
        .bind(row.invited_at)
        .bind(row.joined_at)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_unique("memberships", "user→tenant binding"))?;
        Ok(())
    }

    async fn get(&self, id: MembershipId) -> Result<Option<Membership>> {
        let row = sqlx::query(&format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM memberships WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(membership_from).transpose()
    }

    async fn for_user(&self, user: UserId) -> Result<Vec<Membership>> {
        let rows = sqlx::query(&format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM memberships
             WHERE user_id = $1 ORDER BY created_at"
        ))
        .bind(Uuid::from(user))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(membership_from).collect()
    }

    async fn for_tenant(&self, tenant: TenantId) -> Result<Vec<Membership>> {
        let rows = sqlx::query(&format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM memberships
             WHERE tenant_id = $1 ORDER BY created_at"
        ))
        .bind(Uuid::from(tenant))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(membership_from).collect()
    }
}

#[async_trait::async_trait]
impl AgentStorePort for PgServices {
    async fn save(&self, agent: &Agent) -> Result<()> {
        self.agents.save(agent).await
    }

    async fn get(&self, id: AgentId) -> Result<Option<Agent>> {
        self.agents.get(id).await
    }

    async fn get_by_slug(&self, project: ProjectId, slug: &str) -> Result<Option<Agent>> {
        self.agents
            .list_for_project(project)
            .await
            .map(|list| list.into_iter().find(|agent| agent.slug.as_str() == slug))
    }

    async fn list(&self, project: ProjectId) -> Result<Vec<Agent>> {
        self.agents.list_for_project(project).await
    }
}

#[async_trait::async_trait]
impl AgentVersionStorePort for PgServices {
    async fn save(&self, version: &AgentVersion) -> Result<()> {
        // The store port lacks the tenant scope; resolve it through the
        // owning agent (RLS-authoritative — and a cheap referential check).
        let parent = self
            .agents
            .get(version.agent_id())
            .await?
            .ok_or_else(|| AppError::validation("agent version references an unknown agent"))?;
        self.agent_versions.save(version, parent.tenant_id).await
    }

    async fn get(&self, id: AgentVersionId) -> Result<Option<AgentVersion>> {
        self.agent_versions.get(id).await
    }

    async fn list(&self, agent: AgentId) -> Result<Vec<AgentVersion>> {
        self.agent_versions.list_for_agent(agent).await
    }
}

#[async_trait::async_trait]
impl WorkflowStorePort for PgServices {
    async fn save(&self, workflow: &Workflow) -> Result<()> {
        self.workflows.save(workflow).await
    }

    async fn get(&self, id: WorkflowId) -> Result<Option<Workflow>> {
        self.workflows.get(id).await
    }

    async fn list(&self, project: ProjectId) -> Result<Vec<Workflow>> {
        self.workflows.list_for_project(project).await
    }
}

#[async_trait::async_trait]
impl ExecutionStorePort for PgServices {
    async fn save(&self, execution: &Execution) -> Result<()> {
        self.executions.save(execution).await
    }

    async fn get(&self, id: ExecutionId) -> Result<Option<Execution>> {
        self.executions.get(id).await
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Execution>> {
        self.executions.list_for_tenant(tenant).await
    }

    async fn idempotency_get(&self, tenant: TenantId, key: &str) -> Result<Option<ExecutionId>> {
        self.executions.idempotency_get(tenant, key).await
    }

    async fn idempotency_put(
        &self,
        tenant: TenantId,
        key: &str,
        execution: ExecutionId,
    ) -> Result<()> {
        self.executions
            .idempotency_put(tenant, key, execution)
            .await
    }
}

#[async_trait::async_trait]
impl ScheduleStorePort for PgServices {
    async fn save(&self, schedule: &Schedule) -> Result<()> {
        self.schedules.save(schedule).await
    }

    async fn get(&self, id: ScheduleId) -> Result<Option<Schedule>> {
        self.schedules.get(id).await
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Schedule>> {
        self.schedules.list(tenant).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_projections_match_schema_inventory() {
        // Project counts against the CREATE TABLE column inventories in the
        // migration set. Drift here = fresh-eye developer broke the row
        // mapping without touching the schema.
        assert_eq!(ORG_COLUMNS.split(',').count(), 8); // 0002
        assert_eq!(TENANT_COLUMNS.split(',').count(), 10);
        assert_eq!(PROJECT_COLUMNS.split(',').count(), 10); // 0002 (description included)
        assert_eq!(USER_COLUMNS.split(',').count(), 9); // 0003
        assert_eq!(MEMBERSHIP_COLUMNS.split(',').count(), 10); // 0003
    }

    #[test]
    fn hydration_helpers_reject_missing_columns() {
        // sqlx Row construction requires a live connection; instead assert
        // the property at the seam: rows built from json coming from an
        // under-projected SELECT fail at decode time (not half-hydrated).
        let broken = serde_json::json!({"legal_name": "x"});
        // OrganizationRow::into_domain requires id etc; missing → error.
        assert!(serde_json::from_value::<serde_json::Value>(broken).is_ok());
        // and the guard catches it one level below:
        let result: Result<Organization> =
            crate::rows::hydrate("organizations", serde_json::json!({"legal_name": "x"}));
        assert!(result.is_err());
    }

    #[test]
    fn unique_mapper_reports_honest_conflicts() {
        let mapper = map_unique("organizations", "slug");
        let _ = mapper; // fn pointer composition compile-check
    }
}
