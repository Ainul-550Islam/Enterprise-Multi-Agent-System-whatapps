//! PURPOSE: The PostgreSQL implementation of the tenancy directory ports.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: spec Phases 3 and 4. `mas_tenancy`'s
//! `mas_tenancy::TenantResolver` is correct and fail-closed, but until this module
//! existed its ports had only in-memory implementations — so the authoritative
//! resolver could not run in production at all, and every entrypoint instead
//! read `x-tenant-id` from the request. This module is what makes
//! "tenant comes from identity" true against a real database.
//!
//! Security boundary: these reads are the *only* unscoped reads in the
//! system. They run on a dedicated pool whose role (`mas_directory`, created
//! by migration `0015`) holds `SELECT` on the four hierarchy catalog tables
//! and `BYPASSRLS` for no other purpose. That is a deliberate, minimal,
//! documented exception — see the migration header for why it cannot be
//! avoided and why widening `mas_app` instead would be worse.
//!
//! It must NOT:
//!   • be constructed from the runtime pool — the constructors take a
//!     separately configured directory pool and the type name says so;
//!   • read or write any table outside the four catalog tables;
//!   • be used by a request-serving repository. It answers "what may this
//!     principal reach", never "give me this tenant's executions";
//!   • trust the ids it is handed. It looks facts up; whether a principal may
//!     use them is `TenantResolver`'s decision, made from the returned rows.

use mas_common::enums::TenantStatus;
use mas_common::error::AppError;
use mas_common::ids::{MembershipId, OrganizationId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{Membership, Organization, Project, ProjectConfig, Slug, Tenant};
use mas_tenancy::{MembershipDirectoryPort, TenantDirectoryPort};
use sqlx::{PgPool, Row};

use crate::error::map_sqlx;

/// The four catalog tables this module is permitted to touch.
///
/// Kept as a named constant so a reviewer can see the entire surface at a
/// glance, and so a future change that needs a fifth table is a visible
/// decision rather than a one-line edit.
pub const DIRECTORY_TABLES: &[&str] = &["organizations", "tenants", "projects", "memberships"];

/// PostgreSQL tenancy directory.
///
/// Holds the directory pool, never the runtime pool.
#[derive(Debug, Clone)]
pub struct PgTenantDirectory {
    pool: PgPool,
}

impl PgTenantDirectory {
    /// Binds the directory to the pool configured from
    /// `MAS_DATABASE_DIRECTORY_URL`.
    #[must_use]
    pub const fn new(directory_pool: PgPool) -> Self {
        Self {
            pool: directory_pool,
        }
    }
}

/// PostgreSQL membership directory.
#[derive(Debug, Clone)]
pub struct PgMembershipDirectory {
    pool: PgPool,
}

impl PgMembershipDirectory {
    /// Binds the directory to the pool configured from
    /// `MAS_DATABASE_DIRECTORY_URL`.
    #[must_use]
    pub const fn new(directory_pool: PgPool) -> Self {
        Self {
            pool: directory_pool,
        }
    }
}

/// Errors from a required text column that the schema declares `NOT NULL`.
fn missing(column: &str) -> AppError {
    AppError::internal(format!("directory row missing {column}"))
}

/// Parses a status text column into its domain enum, failing closed: an
/// unrecognised status is treated as unusable rather than as the most
/// permissive one.
fn parse_tenant_status(raw: &str) -> Result<TenantStatus> {
    raw.trim()
        .parse::<TenantStatus>()
        .map_err(|_| AppError::internal(format!("unknown tenant status {raw}")))
}

/// Hydrates a slug, which the database already constrains but which the type
/// re-validates so an out-of-band edit cannot smuggle a bad value into a
/// domain object.
fn slug(row: &sqlx::postgres::PgRow, column: &str) -> Result<Slug> {
    let raw: String = row.try_get(column).map_err(map_sqlx)?;
    Slug::new(&raw)
}

/// Hydrates a `jsonb` object column into a `serde_json::Map`.
///
/// A non-object value can only come from an out-of-band edit; it is refused
/// rather than coerced, because a settings blob of the wrong shape silently
/// changes behaviour.
fn object(
    row: &sqlx::postgres::PgRow,
    column: &str,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let value: serde_json::Value = row.try_get(column).map_err(map_sqlx)?;
    match value {
        serde_json::Value::Object(map) => Ok(map),
        other => Err(AppError::internal(format!(
            "column {column} must be a json object, found {other}"
        ))),
    }
}

/// Hydrates a typed `jsonb` column (`settings`, `config`).
fn typed<T: serde::de::DeserializeOwned>(row: &sqlx::postgres::PgRow, column: &str) -> Result<T> {
    let value: serde_json::Value = row
        .try_get(column)
        .map_err(map_sqlx)
        .unwrap_or(serde_json::Value::Object(Default::default()));
    serde_json::from_value(value)
        .map_err(|error| AppError::internal(format!("column {column} is malformed: {error}")))
}

fn parse_enum<T: std::str::FromStr>(raw: &str, what: &str) -> Result<T> {
    raw.trim()
        .parse::<T>()
        .map_err(|_| AppError::internal(format!("unknown {what} {raw}")))
}

fn ts(row: &sqlx::postgres::PgRow, column: &str) -> Result<Timestamp> {
    row.try_get::<chrono::DateTime<chrono::Utc>, _>(column)
        .map(Timestamp::from_datetime)
        .map_err(map_sqlx)
}

#[async_trait::async_trait]
impl TenantDirectoryPort for PgTenantDirectory {
    async fn get_tenant(&self, id: TenantId) -> Result<Option<Tenant>> {
        let row = sqlx::query(
            "SELECT id, organization_id, name, slug, status, environment, isolation_mode, settings, created_at, updated_at
               FROM tenants WHERE id = $1",
        )
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let id = TenantId::from_uuid(row.try_get::<uuid::Uuid, _>("id").map_err(map_sqlx)?);
            let organization_id = OrganizationId::from_uuid(
                row.try_get::<uuid::Uuid, _>("organization_id")
                    .map_err(map_sqlx)?,
            );
            let tenant = Tenant::reconstitute(
                id,
                organization_id,
                row.try_get::<String, _>("name").map_err(map_sqlx)?,
                slug(&row, "slug")?,
                parse_tenant_status(&row.try_get::<String, _>("status").map_err(map_sqlx)?)?,
                parse_enum(
                    &row.try_get::<String, _>("environment").map_err(map_sqlx)?,
                    "environment",
                )?,
                parse_enum(
                    &row.try_get::<String, _>("isolation_mode")
                        .map_err(map_sqlx)?,
                    "isolation mode",
                )?,
                object(&row, "settings")?,
                ts(&row, "created_at")?,
                ts(&row, "updated_at")?,
            );
            // The row is the authority on ownership, so this can only fire on
            // a schema/FK violation. Asserting it here means a directory read
            // can never hand back a tenant whose owner disagrees with itself.
            tenant.assert_belongs_to(organization_id)?;
            Ok(tenant)
        })
        .transpose()
    }

    async fn get_organization(&self, id: OrganizationId) -> Result<Option<Organization>> {
        let row = sqlx::query(
            "SELECT id, legal_name, display_name, slug, status, settings, created_at, updated_at
               FROM organizations WHERE id = $1",
        )
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            Ok(Organization {
                id: OrganizationId::from_uuid(
                    row.try_get::<uuid::Uuid, _>("id").map_err(map_sqlx)?,
                ),
                legal_name: row.try_get::<String, _>("legal_name").map_err(map_sqlx)?,
                display_name: row.try_get::<String, _>("display_name").map_err(map_sqlx)?,
                slug: slug(&row, "slug")?,
                status: parse_enum(
                    &row.try_get::<String, _>("status").map_err(map_sqlx)?,
                    "organization status",
                )?,
                settings: object(&row, "settings")?,
                created_at: ts(&row, "created_at")?,
                updated_at: ts(&row, "updated_at")?,
            })
        })
        .transpose()
    }

    async fn get_project(&self, id: ProjectId) -> Result<Option<Project>> {
        let row = sqlx::query(
            "SELECT id, tenant_id, organization_id, name, slug, description, status, config, created_at, updated_at
               FROM projects WHERE id = $1",
        )
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            Ok(Project {
                id: ProjectId::from_uuid(row.try_get::<uuid::Uuid, _>("id").map_err(map_sqlx)?),
                tenant_id: TenantId::from_uuid(
                    row.try_get::<uuid::Uuid, _>("tenant_id")
                        .map_err(map_sqlx)?,
                ),
                organization_id: OrganizationId::from_uuid(
                    row.try_get::<uuid::Uuid, _>("organization_id")
                        .map_err(map_sqlx)?,
                ),
                name: row.try_get::<String, _>("name").map_err(map_sqlx)?,
                slug: slug(&row, "slug")?,
                description: row
                    .try_get::<Option<String>, _>("description")
                    .map_err(map_sqlx)?,
                status: parse_enum(
                    &row.try_get::<String, _>("status").map_err(map_sqlx)?,
                    "project status",
                )?,
                config: typed::<ProjectConfig>(&row, "config")?,
                created_at: ts(&row, "created_at")?,
                updated_at: ts(&row, "updated_at")?,
            })
        })
        .transpose()
    }

    async fn list_tenants(&self, organization_id: OrganizationId) -> Result<Vec<Tenant>> {
        let rows = sqlx::query(
            "SELECT id, organization_id, name, slug, status, environment, isolation_mode, settings, created_at, updated_at
               FROM tenants WHERE organization_id = $1 ORDER BY id",
        )
        .bind(organization_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id = TenantId::from_uuid(row.try_get::<uuid::Uuid, _>("id").map_err(map_sqlx)?);
            // Reuse the single-row path so there is exactly one hydration of
            // a tenant row in the codebase.
            if let Some(tenant) = self.get_tenant(id).await? {
                out.push(tenant);
            }
        }
        Ok(out)
    }

    async fn list_projects(&self, tenant_id: TenantId) -> Result<Vec<Project>> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, organization_id, name, slug, description, status, config, created_at, updated_at
               FROM projects WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let id = ProjectId::from_uuid(row.try_get::<uuid::Uuid, _>("id").map_err(map_sqlx)?);
            if let Some(project) = self.get_project(id).await? {
                out.push(project);
            }
        }
        Ok(out)
    }

    async fn upsert_organization(&self, _organization: &Organization) -> Result<()> {
        Err(directory_is_read_only("upsert_organization"))
    }

    async fn upsert_tenant(&self, _tenant: &Tenant) -> Result<()> {
        Err(directory_is_read_only("upsert_tenant"))
    }

    async fn upsert_project(&self, _project: &Project) -> Result<()> {
        Err(directory_is_read_only("upsert_project"))
    }
}

/// The directory role has no write grants, by design.
///
/// Refusing here rather than letting PostgreSQL refuse means the failure is
/// self-explanatory and happens before a connection is checked out.
fn directory_is_read_only(operation: &str) -> AppError {
    AppError::internal(format!(
        "the tenancy directory is read-only; {operation} is a privileged \
         operation that must run through the tenancy service on the runtime pool"
    ))
}

fn hydrate_membership(row: &sqlx::postgres::PgRow) -> Result<Membership> {
    let roles: serde_json::Value = row.try_get("roles").map_err(map_sqlx)?;
    let roles = roles
        .as_array()
        .ok_or_else(|| missing("roles"))?
        .iter()
        .map(|value| parse_enum(value.as_str().unwrap_or_default(), "membership role"))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let status: String = row.try_get("status").map_err(map_sqlx)?;
    Ok(Membership {
        id: MembershipId::from_uuid(row.try_get::<uuid::Uuid, _>("id").map_err(map_sqlx)?),
        user_id: UserId::from_uuid(row.try_get::<uuid::Uuid, _>("user_id").map_err(map_sqlx)?),
        organization_id: OrganizationId::from_uuid(
            row.try_get::<uuid::Uuid, _>("organization_id")
                .map_err(map_sqlx)?,
        ),
        tenant_id: row
            .try_get::<Option<uuid::Uuid>, _>("tenant_id")
            .map_err(map_sqlx)?
            .map(TenantId::from_uuid),
        roles,
        status: parse_enum(&status, "membership status")?,
        invited_at: ts(row, "invited_at")?,
        joined_at: row
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("joined_at")
            .map_err(map_sqlx)?
            .map(Timestamp::from_datetime),
        created_at: ts(row, "created_at")?,
        updated_at: ts(row, "updated_at")?,
    })
}

#[async_trait::async_trait]
impl MembershipDirectoryPort for PgMembershipDirectory {
    async fn memberships_for_user(&self, user_id: UserId) -> Result<Vec<Membership>> {
        // Deliberately includes every status: the resolver decides what an
        // `invited` or `suspended` membership may do. Filtering here would
        // put authorization policy in a query.
        let rows = sqlx::query(
            "SELECT id, user_id, organization_id, tenant_id, roles, status, invited_at, joined_at, created_at, updated_at
               FROM memberships WHERE user_id = $1 ORDER BY id",
        )
        .bind(user_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(hydrate_membership).collect()
    }

    async fn memberships_in_organization(
        &self,
        organization_id: OrganizationId,
    ) -> Result<Vec<Membership>> {
        let rows = sqlx::query(
            "SELECT id, user_id, organization_id, tenant_id, roles, status, invited_at, joined_at, created_at, updated_at
               FROM memberships WHERE organization_id = $1 ORDER BY id",
        )
        .bind(organization_id.into_uuid())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(hydrate_membership).collect()
    }

    async fn upsert(&self, _membership: &Membership) -> Result<()> {
        Err(directory_is_read_only("upsert membership"))
    }

    async fn get(&self, id: MembershipId) -> Result<Option<Membership>> {
        let row = sqlx::query(
            "SELECT id, user_id, organization_id, tenant_id, roles, status, invited_at, joined_at, created_at, updated_at
               FROM memberships WHERE id = $1",
        )
        .bind(id.into_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(hydrate_membership).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The grant surface must stay exactly the four catalog tables. This is
    /// the assertion a reviewer of a future change will trip over.
    #[test]
    fn the_directory_surface_is_four_catalog_tables() {
        assert_eq!(
            DIRECTORY_TABLES,
            ["organizations", "tenants", "projects", "memberships"]
        );
        for table in DIRECTORY_TABLES {
            assert!(
                !matches!(
                    *table,
                    "executions" | "tasks" | "agents" | "credentials" | "usage" | "audit_events"
                ),
                "{table} is tenant data and must never be in the directory surface"
            );
        }
    }

    /// A status the platform does not recognise must fail closed rather than
    /// resolve to the most permissive status.
    #[test]
    fn unknown_statuses_fail_closed() {
        assert!(parse_tenant_status("active").is_ok());
        assert!(parse_tenant_status("suspended").is_ok());
        assert!(
            parse_tenant_status("totally-made-up").is_err(),
            "an unrecognised status must never read as active"
        );
    }

    #[test]
    fn writes_are_refused_before_reaching_the_database() {
        // Constructing a pool is not possible without a server; the refusal
        // is a pure function, so it is asserted directly.
        let error = directory_is_read_only("upsert_tenant");
        assert!(error.to_string().contains("read-only"));
    }
}
