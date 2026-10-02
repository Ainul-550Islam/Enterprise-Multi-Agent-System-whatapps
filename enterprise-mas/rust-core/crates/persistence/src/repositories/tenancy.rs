//! Tenant-facing CRUD repositories: organizations, tenants, projects, users,
//! memberships.
//!
//! Conventions (enforced here, not by convention-mouth):
//!
//! * **Writes** (`*_in` methods) take a caller-managed executor so they join
//!   the enclosing unit of work (state change + outbox insert + audit append
//!   commit atomically). Inserts of hierarchy rows require the caller's
//!   RLS context to satisfy the table's `WITH CHECK` (e.g. tenants require
//!   `app.current_org` — see `0002_tenancy`).
//! * **Reads** (`get_*`/`list_*` scoped methods) run through
//!   `ScopedStore::scoped_tx` with an explicit [`RlsContext`]; cross-scope
//!   lookups return `None`/`[]` (RLS hides them — the platform never
//!   reveals existence across tenants).
//! * Every SELECT projects the exact row column list from [`crate::rows`];
//!   hydration re-runs domain invariants, so a drifting column is a loud
//!   error, never a half-built aggregate.

use std::fmt;

use mas_common::enums::{TenantStatus, UserStatus};
use mas_common::error::AppError;
use mas_common::ids::{MembershipId, OrganizationId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_domain::membership::{Membership, MembershipStatus};
use mas_domain::organization::{Organization, OrganizationStatus};
use mas_domain::project::{Project, ProjectStatus};
use mas_domain::tenant::Tenant;
use mas_domain::user::User;
use sqlx::{PgConnection, PgPool, Row};

use crate::error::map_sqlx;
use crate::rls::RlsContext;
use crate::rows::{MembershipRow, OrganizationRow, ProjectRow, TenantRow, UserRow};
use crate::transaction::UnitOfWork;

const ORGANIZATION_COLUMNS: &str =
    "id, legal_name, display_name, slug, status, settings, created_at, updated_at";
const TENANT_COLUMNS: &str = "id, organization_id, name, slug, status, environment, \
     isolation_mode AS isolation, settings, created_at, updated_at";
const PROJECT_COLUMNS: &str = "id, tenant_id, organization_id, name, slug, description, \
     status, config, created_at, updated_at";
const USER_COLUMNS: &str = "id, email, display_name, status, external_subject, \
     identity_provider, last_login_at, created_at, updated_at";
const MEMBERSHIP_COLUMNS: &str = "id, user_id, organization_id, tenant_id, roles, status, \
     invited_at, joined_at, created_at, updated_at";

// ---------------------------------------------------------------------------
// organizations
// ---------------------------------------------------------------------------

/// Organization repository.
pub struct OrganizationStore {
    pool: PgPool,
}

impl fmt::Debug for OrganizationStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrganizationStore").finish_non_exhaustive()
    }
}

impl OrganizationStore {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts an organization inside a caller-managed transaction.
    ///
    /// Creating the root of a hierarchy is a privileged platform operation:
    /// the `organizations` RLS policy is `id = app.current_org`, which a
    /// brand-new id cannot yet satisfy — so org creation runs under the
    /// provisioning service role (BYPASSRLS), inside a real transaction,
    /// with the audit event in the same commit.
    pub async fn insert_in(executor: &mut PgConnection, organization: &Organization) -> Result<()> {
        let row = OrganizationRow::from_domain(organization)?;
        sqlx::query(&format!(
            "INSERT INTO organizations ({ORGANIZATION_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
        ))
        .bind(row.id)
        .bind(row.legal_name)
        .bind(row.display_name)
        .bind(row.slug)
        .bind(row.status)
        .bind(&row.settings)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Looks up an organization (org-scoped read; cross-org ids hide).
    pub async fn get_scoped(
        &self,
        ctx: &RlsContext,
        id: OrganizationId,
    ) -> Result<Option<Organization>> {
        if ctx.organization.is_none() {
            return Err(AppError::forbidden(
                "organization reads require an organization RLS scope",
            ));
        }
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow: UnitOfWork<'_> = store.scoped_tx(ctx).await?;
        let row = sqlx::query(&format!(
            "SELECT {ORGANIZATION_COLUMNS} FROM organizations WHERE id = $1"
        ))
        .bind(id.into_uuid())
        .fetch_optional(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        row.map(|r| org_from(&r)).transpose()
    }

    /// Updates lifecycle status inside a transaction (uses the domain's
    /// transition rules — this is only the write).
    pub async fn update_status_in(
        executor: &mut PgConnection,
        id: OrganizationId,
        status: OrganizationStatus,
    ) -> Result<()> {
        let result = sqlx::query("UPDATE organizations SET status = $2 WHERE id = $1")
            .bind(id.into_uuid())
            .bind(status.to_string())
            .execute(executor)
            .await
            .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::not_found("organization", id.to_string()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// tenants
// ---------------------------------------------------------------------------

/// Tenant repository.
pub struct TenantStore {
    pool: PgPool,
}

impl fmt::Debug for TenantStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantStore").finish_non_exhaustive()
    }
}

impl TenantStore {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a tenant inside a transaction. Requires an org RLS scope in
    /// the surrounding connection (the policy's `WITH CHECK` matches
    /// `organization_id = app.current_org`).
    pub async fn insert_in(executor: &mut PgConnection, tenant: &Tenant) -> Result<()> {
        let row = TenantRow::from_domain(tenant)?;
        sqlx::query(&format!(
            "INSERT INTO tenants ({TENANT_COLUMNS_NAMED})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
        ))
        .bind(row.id)
        .bind(row.organization_id)
        .bind(row.name)
        .bind(row.slug)
        .bind(row.status)
        .bind(row.environment)
        .bind(row.isolation_mode)
        .bind(&row.settings)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Looks up a tenant with the caller's tenant/org scope applied.
    pub async fn get_scoped(&self, ctx: &RlsContext, id: TenantId) -> Result<Option<Tenant>> {
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let row = sqlx::query(&format!(
            "SELECT {TENANT_COLUMNS} FROM tenants WHERE id = $1"
        ))
        .bind(id.into_uuid())
        .fetch_optional(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        row.map(|r| tenant_from(&r)).transpose()
    }

    /// Lists an organization's tenants, oldest first, bounded (admin catalog).
    pub async fn list_for_organization(
        &self,
        ctx: &RlsContext,
        organization: OrganizationId,
        limit: u32,
    ) -> Result<Vec<Tenant>> {
        if ctx.organization != Some(organization) {
            return Err(AppError::forbidden(
                "tenant catalog reads require the matching organization scope",
            ));
        }
        let limit = i64::from(limit.clamp(1, 1_000));
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let rows = sqlx::query(&format!(
            "SELECT {TENANT_COLUMNS} FROM tenants
             WHERE organization_id = $1 ORDER BY created_at ASC, id ASC LIMIT $2"
        ))
        .bind(organization.into_uuid())
        .bind(limit)
        .fetch_all(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        rows.iter().map(tenant_from).collect()
    }

    /// Finds a tenant by (organization, slug) — unambiguous by the unique
    /// constraint, used by routing/middleware.
    pub async fn find_by_slug(
        &self,
        ctx: &RlsContext,
        organization: OrganizationId,
        slug: &str,
    ) -> Result<Option<Tenant>> {
        if slug.is_empty() || slug.len() > 64 {
            return Err(AppError::invalid_field(
                "slug",
                "length",
                "tenant slug must be 1..=64 characters",
            ));
        }
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let row = sqlx::query(&format!(
            "SELECT {TENANT_COLUMNS} FROM tenants
             WHERE organization_id = $1 AND slug = $2"
        ))
        .bind(organization.into_uuid())
        .bind(slug)
        .fetch_optional(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        row.map(|r| tenant_from(&r)).transpose()
    }

    /// Updates the lifecycle status inside a transaction.
    pub async fn update_status_in(
        executor: &mut PgConnection,
        id: TenantId,
        status: TenantStatus,
    ) -> Result<()> {
        let result = sqlx::query("UPDATE tenants SET status = $2 WHERE id = $1")
            .bind(id.into_uuid())
            .bind(status.to_string())
            .execute(executor)
            .await
            .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::not_found("tenant", id.to_string()));
        }
        Ok(())
    }
}

const TENANT_COLUMNS_NAMED: &str = "id, organization_id, name, slug, status, environment, \
     isolation_mode, settings, created_at, updated_at";

// ---------------------------------------------------------------------------
// projects
// ---------------------------------------------------------------------------

/// Project repository.
pub struct ProjectStore {
    pool: PgPool,
}

impl fmt::Debug for ProjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectStore").finish_non_exhaustive()
    }
}

impl ProjectStore {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a project inside a transaction. The composite FK
    /// `(tenant_id, organization_id)` plus the tenant's own RLS scope make a
    /// cross-org link un-writable (defense in depth with the domain check).
    pub async fn insert_in(executor: &mut PgConnection, project: &Project) -> Result<()> {
        let row = ProjectRow::from_domain(project)?;
        sqlx::query(&format!(
            "INSERT INTO projects ({PROJECT_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
        ))
        .bind(row.id)
        .bind(row.tenant_id)
        .bind(row.organization_id)
        .bind(row.name)
        .bind(row.slug)
        .bind(row.description)
        .bind(row.status)
        .bind(&row.config)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Looks up a project under the caller's tenant scope.
    pub async fn get_scoped(&self, ctx: &RlsContext, id: ProjectId) -> Result<Option<Project>> {
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let row = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM projects WHERE id = $1"
        ))
        .bind(id.into_uuid())
        .fetch_optional(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        row.map(|r| project_from(&r)).transpose()
    }

    /// Lists a tenant's non-archived projects, newest first, bounded.
    pub async fn list_for_tenant(
        &self,
        ctx: &RlsContext,
        tenant: TenantId,
        limit: u32,
    ) -> Result<Vec<Project>> {
        if ctx.tenant != Some(tenant) {
            return Err(AppError::forbidden(
                "project catalog reads require the matching tenant scope",
            ));
        }
        let limit = i64::from(limit.clamp(1, 1_000));
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let rows = sqlx::query(&format!(
            "SELECT {PROJECT_COLUMNS} FROM projects
             WHERE tenant_id = $1 AND status <> 'archived'
             ORDER BY created_at DESC, id DESC LIMIT $2"
        ))
        .bind(tenant.into_uuid())
        .bind(limit)
        .fetch_all(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        rows.iter().map(project_from).collect()
    }

    /// Updates lifecycle status inside a transaction.
    pub async fn update_status_in(
        executor: &mut PgConnection,
        id: ProjectId,
        status: ProjectStatus,
    ) -> Result<()> {
        let result = sqlx::query("UPDATE projects SET status = $2 WHERE id = $1")
            .bind(id.into_uuid())
            .bind(status.to_string())
            .execute(executor)
            .await
            .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::not_found("project", id.to_string()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// users
// ---------------------------------------------------------------------------

/// User repository (global identity table — self-service RLS with
/// `app.current_user`; login/admin paths use the service role).
pub struct UserStore {
    pool: PgPool,
}

impl fmt::Debug for UserStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UserStore").finish_non_exhaustive()
    }
}

impl UserStore {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a user inside a transaction. Registration is a privileged
    /// flow (service role) — the `users` policy is self-service only.
    pub async fn insert_in(executor: &mut PgConnection, user: &User) -> Result<()> {
        let row = UserRow::from_domain(user)?;
        sqlx::query(&format!(
            "INSERT INTO users ({USER_COLUMNS})
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        ))
        .bind(row.id)
        .bind(row.email)
        .bind(row.display_name)
        .bind(row.status)
        .bind(row.external_subject)
        .bind(row.identity_provider)
        .bind(row.last_login_at)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Login-path lookup by email (case-insensitive via `citext`). Runs under
    /// the auth service role; callers must NOT branch externally on
    /// existence (user enumeration).
    pub async fn find_by_email(&self, email: &str) -> Result<Option<User>> {
        if email.is_empty() || email.len() > 320 {
            return Err(AppError::invalid_field(
                "email",
                "length",
                "email must be 1..=320 characters",
            ));
        }
        let row = sqlx::query(&format!(
            "SELECT {USER_COLUMNS} FROM users WHERE email = $1"
        ))
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|r| user_from(&r)).transpose()
    }

    /// Self-service lookup (requires the matching user RLS scope).
    pub async fn get_scoped(&self, ctx: &RlsContext, id: UserId) -> Result<Option<User>> {
        if ctx.user != Some(id) {
            return Err(AppError::forbidden("user profiles are self-service only"));
        }
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let row = sqlx::query(&format!("SELECT {USER_COLUMNS} FROM users WHERE id = $1"))
            .bind(id.into_uuid())
            .fetch_optional(uow.executor())
            .await
            .map_err(map_sqlx)?;
        uow.commit().await?;
        row.map(|r| user_from(&r)).transpose()
    }

    /// Persists a full user row after a domain mutation (activate / login /
    /// rename). Uses platform-owned write paths (service role) — callers
    /// re-read through the query first, mutate the aggregate, then save.
    pub async fn save_in(executor: &mut PgConnection, user: &User) -> Result<()> {
        let row = UserRow::from_domain(user)?;
        let result = sqlx::query(
            "UPDATE users SET email = $2, display_name = $3, status = $4,
                 external_subject = $5, identity_provider = $6,
                 last_login_at = $7, updated_at = $8
             WHERE id = $1",
        )
        .bind(row.id)
        .bind(row.email)
        .bind(row.display_name)
        .bind(row.status)
        .bind(row.external_subject)
        .bind(row.identity_provider)
        .bind(row.last_login_at)
        .bind(row.updated_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::not_found("user", row.id.to_string()));
        }
        Ok(())
    }

    /// Updates lifecycle status inside a transaction.
    pub async fn update_status_in(
        executor: &mut PgConnection,
        id: UserId,
        status: UserStatus,
    ) -> Result<()> {
        let result = sqlx::query("UPDATE users SET status = $2 WHERE id = $1")
            .bind(id.into_uuid())
            .bind(status.to_string())
            .execute(executor)
            .await
            .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::not_found("user", id.to_string()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// memberships
// ---------------------------------------------------------------------------

/// Membership repository.
pub struct MembershipStore {
    pool: PgPool,
}

impl fmt::Debug for MembershipStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MembershipStore").finish_non_exhaustive()
    }
}

impl MembershipStore {
    /// Binds the store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a membership inside a transaction (invite flow).
    pub async fn insert_in(executor: &mut PgConnection, membership: &Membership) -> Result<()> {
        let row = MembershipRow::from_domain(membership)?;
        sqlx::query(&format!(
            "INSERT INTO memberships ({MEMBERSHIP_COLUMNS})
             VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10)"
        ))
        .bind(row.id)
        .bind(row.user_id)
        .bind(row.organization_id)
        .bind(row.tenant_id)
        .bind(serde_json::to_value(&row.roles).map_err(|err| {
            AppError::serialization(format!("membership roles not serializable: {err}"))
        })?)
        .bind(row.status)
        .bind(row.invited_at)
        .bind(row.joined_at)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(executor)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Loads one membership (scoped: tenant memberships need tenant scope,
    /// org-wide memberships need org scope).
    pub async fn get_scoped(
        &self,
        ctx: &RlsContext,
        id: MembershipId,
    ) -> Result<Option<Membership>> {
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let row = sqlx::query(&format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM memberships WHERE id = $1"
        ))
        .bind(id.into_uuid())
        .fetch_optional(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        row.map(|r| membership_from(&r)).transpose()
    }

    /// All memberships of a user inside an organization (org-wide and
    /// tenant-scoped), used by the tenant resolver.
    pub async fn list_for_user(
        &self,
        ctx: &RlsContext,
        organization: OrganizationId,
        user: UserId,
    ) -> Result<Vec<Membership>> {
        if ctx.organization != Some(organization) {
            return Err(AppError::forbidden(
                "membership reads require the matching organization scope",
            ));
        }
        let store = crate::repositories::ScopedStore::new(self.pool.clone());
        let mut uow = store.scoped_tx(ctx).await?;
        let rows = sqlx::query(&format!(
            "SELECT {MEMBERSHIP_COLUMNS} FROM memberships
             WHERE organization_id = $1 AND user_id = $2
             ORDER BY created_at ASC LIMIT 500"
        ))
        .bind(organization.into_uuid())
        .bind(user.into_uuid())
        .fetch_all(uow.executor())
        .await
        .map_err(map_sqlx)?;
        uow.commit().await?;
        rows.iter().map(membership_from).collect()
    }

    /// Updates membership status inside a transaction (activate/suspend).
    pub async fn update_status_in(
        executor: &mut PgConnection,
        id: MembershipId,
        status: MembershipStatus,
    ) -> Result<()> {
        let result = sqlx::query("UPDATE memberships SET status = $2 WHERE id = $1")
            .bind(id.into_uuid())
            .bind(status.to_string())
            .execute(executor)
            .await
            .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            return Err(AppError::not_found("membership", id.to_string()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// row assembly helpers (column lists match crate::rows exactly)
// ---------------------------------------------------------------------------

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
        isolation_mode: row.try_get("isolation").map_err(map_sqlx)?,
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
    let roles_json: serde_json::Value = row.try_get("roles").map_err(map_sqlx)?;
    let roles: Vec<String> = serde_json::from_value(roles_json).map_err(|err| {
        AppError::database(format!(
            "memberships.roles column is not a string array: {err}"
        ))
    })?;
    MembershipRow {
        id: row.try_get("id").map_err(map_sqlx)?,
        user_id: row.try_get("user_id").map_err(map_sqlx)?,
        organization_id: row.try_get("organization_id").map_err(map_sqlx)?,
        tenant_id: row.try_get("tenant_id").map_err(map_sqlx)?,
        roles,
        status: row.try_get("status").map_err(map_sqlx)?,
        invited_at: row.try_get("invited_at").map_err(map_sqlx)?,
        joined_at: row.try_get("joined_at").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: row.try_get("updated_at").map_err(map_sqlx)?,
    }
    .into_domain()
}

#[cfg(test)]
mod tests {
    // The stores are thin SQL skins over rows.rs hydration: their logic
    // (scope guards, column lists, hydration bridges) is covered there and
    // the SQL itself lands in the integration suite (tests/integration).
    use super::*;

    #[test]
    fn select_column_lists_stay_in_sync_with_rows() {
        // Every hydrated row struct reads exactly these names — a drift here
        // fails at the first integration run, so pin the contract.
        for name in [
            "id",
            "legal_name",
            "display_name",
            "slug",
            "status",
            "settings",
            "created_at",
            "updated_at",
        ] {
            assert!(ORGANIZATION_COLUMNS.contains(name), "{name} missing");
        }
        for name in [
            "id",
            "organization_id",
            "tenant_id",
            "user_id",
            "roles",
            "invited_at",
            "joined_at",
        ] {
            assert!(MEMBERSHIP_COLUMNS.contains(name), "{name} missing");
        }
        assert!(
            TENANT_COLUMNS.contains("isolation_mode AS isolation"),
            "tenant select must alias isolation_mode → isolation for hydration"
        );
    }

    #[test]
    fn scope_guards_are_documented_in_errors() {
        // These guards fire before any SQL (no pool needed to reach them) —
        // verify the guard errors classify correctly by constructing the
        // stores against an unconnected pool (no query is issued).
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(async {
            let pool =
                PgPool::connect_lazy("postgres://mas:unused@127.0.0.1:1/mas").expect("lazy pool");
            let users = UserStore::new(pool);
            let ctx = RlsContext::for_user(UserId::new());
            let other = UserId::new();
            let err = users
                .get_scoped(&ctx, other)
                .await
                .expect_err("mismatched scope must be refused before SQL");
            assert_eq!(err.error_code(), "FORBIDDEN");
        });
    }
}
