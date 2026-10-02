//! `agents` + `agent_versions` repositories (spec-JSONB style).
//!
//! Doctrine (shared with the other production repositories):
//! * Writes upsert on the PRIMARY key; per-tenant uniqueness (`slug`,
//!   `(agent_id, version)`) maps to `AppError::conflict` — silent overwrite
//!   between replicas is the bug this design refuses to hide.
//! * Every SELECT projects a fixed column list; hydration re-runs domain
//!   invariants via serde (a drifting column defines-loud, never
//!   defines-half).
//! * ctx-less application-port calls run under the service role
//!   (`BYPASSRLS`); scope assertions remain the services' first-order duty
//!   (see `docs/production-wiring.md` §RLS role strategy).

use mas_common::error::AppError;
use mas_common::ids::{AgentId, AgentVersionId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_domain::agent::Agent;
use mas_domain::agent_version::AgentVersion;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::{AgentRow, AgentVersionRow};

/// Full projected row columns for `agents` queries.
pub const AGENT_COLUMNS: &str =
    "id, tenant_id, organization_id, name, slug, status, spec, created_at, updated_at";
/// Full projected columns for `agent_versions`.
pub const AGENT_VERSION_COLUMNS: &str =
    "id, agent_id, tenant_id, version, deployment, spec, created_at";

const AGENT_UPSERT: &str = "INSERT INTO agents (
        id, tenant_id, organization_id, name, slug, status, spec, created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        slug = EXCLUDED.slug,
        status = EXCLUDED.status,
        spec = EXCLUDED.spec,
        updated_at = EXCLUDED.updated_at";

const AGENT_VERSION_INSERT: &str = "INSERT INTO agent_versions (
        id, agent_id, tenant_id, version, deployment, spec, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (agent_id, version) DO NOTHING";

/// Turns a bind-ready row model into flattened query args. Pure so tests
/// cover the exact column inventory the SQL statement occupies.
#[cfg(test)]
use mas_common::timestamps::Timestamp;

#[cfg(test)]
fn agent_bind_projection(row: &AgentRow) -> Result<Vec<serde_json::Value>> {
    Ok(vec![
        serde_json::Value::String(row.id.to_string()),
        serde_json::Value::String(row.tenant_id.to_string()),
        serde_json::Value::String(row.organization_id.to_string()),
        serde_json::Value::String(row.name.clone()),
        serde_json::Value::String(row.slug.clone()),
        serde_json::Value::String(row.status.clone()),
        row.spec.clone(),
        serde_json::Value::from(Timestamp::from_datetime(row.created_at).to_rfc3339_millis()),
        serde_json::Value::from(Timestamp::from_datetime(row.updated_at).to_rfc3339_millis()),
    ])
}

fn fetch_agent(pg_row: sqlx::postgres::PgRow) -> Result<AgentRow> {
    Ok(AgentRow {
        id: pg_row.try_get("id").map_err(map_sqlx)?,
        tenant_id: pg_row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: pg_row.try_get("organization_id").map_err(map_sqlx)?,
        name: pg_row.try_get("name").map_err(map_sqlx)?,
        slug: pg_row.try_get("slug").map_err(map_sqlx)?,
        status: pg_row.try_get("status").map_err(map_sqlx)?,
        spec: pg_row.try_get("spec").map_err(map_sqlx)?,
        created_at: pg_row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: pg_row.try_get("updated_at").map_err(map_sqlx)?,
    })
}

fn fetch_agent_version(pg_row: sqlx::postgres::PgRow) -> Result<AgentVersionRow> {
    Ok(AgentVersionRow {
        id: pg_row.try_get("id").map_err(map_sqlx)?,
        agent_id: pg_row.try_get("agent_id").map_err(map_sqlx)?,
        tenant_id: pg_row.try_get("tenant_id").map_err(map_sqlx)?,
        version: pg_row.try_get("version").map_err(map_sqlx)?,
        deployment: pg_row.try_get("deployment").map_err(map_sqlx)?,
        spec: pg_row.try_get("spec").map_err(map_sqlx)?,
        created_at: pg_row.try_get("created_at").map_err(map_sqlx)?,
    })
}

/// PostgreSQL store for `agents`.
#[derive(Debug, Clone)]
pub struct AgentStore {
    pool: PgPool,
}

impl AgentStore {
    /// Binds a store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates or rewrites an agent row.
    pub async fn save(&self, agent: &Agent) -> Result<()> {
        let row = AgentRow::from_domain(agent)?;
        let result = sqlx::query(AGENT_UPSERT)
            .bind(row.id)
            .bind(row.tenant_id)
            .bind(row.organization_id)
            .bind(&row.name)
            .bind(&row.slug)
            .bind(&row.status)
            .bind(&row.spec)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|error| map_write_conflict(&error, "agents", &row.slug))?;
        if result.rows_affected() != 1 {
            return Err(AppError::database(format!(
                "agents upsert affected {} rows for id {}",
                result.rows_affected(),
                row.id
            )));
        }
        Ok(())
    }

    /// Loads one agent by primary key (service role; services assert scope).
    pub async fn get(&self, id: AgentId) -> Result<Option<Agent>> {
        let row = sqlx::query(&format!("SELECT {AGENT_COLUMNS} FROM agents WHERE id = $1"))
            .bind(Uuid::from(id))
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.map(fetch_agent)
            .transpose()?
            .map(AgentRow::into_domain)
            .transpose()
    }

    /// Loads by (tenant, slug) — the public-key lookup for runs/CLI.
    pub async fn get_by_slug(&self, tenant: TenantId, slug: &str) -> Result<Option<Agent>> {
        let row = sqlx::query(&format!(
            "SELECT {AGENT_COLUMNS} FROM agents WHERE tenant_id = $1 AND slug = $2"
        ))
        .bind(Uuid::from(tenant))
        .bind(slug)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(fetch_agent)
            .transpose()?
            .map(AgentRow::into_domain)
            .transpose()
    }

    /// Filters agents by the materialized `spec.project_id` (project is a
    /// spec-carried field on the agent aggregate — the schema owns no
    /// dedicated column).
    pub async fn list_for_project(&self, project: ProjectId) -> Result<Vec<Agent>> {
        let rows = sqlx::query(&format!(
            "SELECT {AGENT_COLUMNS} FROM agents
             WHERE spec ->> 'project_id' = $1
             ORDER BY name"
        ))
        .bind(Uuid::from(project).to_string())
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_agent)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(AgentRow::into_domain)
            .collect()
    }

    /// Lists tenant-scoped agents (scheduler/usage paths).
    pub async fn list_for_tenant(&self, tenant: TenantId) -> Result<Vec<Agent>> {
        let rows = sqlx::query(&format!(
            "SELECT {AGENT_COLUMNS} FROM agents WHERE tenant_id = $1 ORDER BY name"
        ))
        .bind(Uuid::from(tenant))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_agent)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(AgentRow::into_domain)
            .collect()
    }
}

/// PostgreSQL store for `agent_versions` (immutable snapshots).
#[derive(Debug, Clone)]
pub struct AgentVersionStore {
    pool: PgPool,
}

impl AgentVersionStore {
    /// Binds a store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Appends an immutable version; refuses to overwrite a snapshot (the
    /// whole point of versioned immutability is that History does not
    /// change silently).
    pub async fn save(&self, version: &AgentVersion, tenant: TenantId) -> Result<()> {
        let mut row = AgentVersionRow::from_domain(version)?;
        row.tenant_id = Uuid::from(tenant);
        let result = sqlx::query(AGENT_VERSION_INSERT)
            .bind(row.id)
            .bind(row.agent_id)
            .bind(row.tenant_id)
            .bind(row.version)
            .bind(&row.deployment)
            .bind(&row.spec)
            .bind(row.created_at)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        if result.rows_affected() == 0 {
            // ON CONFLICT DO NOTHING → the (agent_id, version) slot existed.
            // Do the taxonomy right: a snapshot republish is a conflict, not
            // a silent no-op.
            return Err(AppError::conflict(format!(
                "agent_versions ({}, {}) already exists — snapshots are immutable",
                row.agent_id, row.version
            )));
        }
        Ok(())
    }

    /// Loads one version by primary key.
    pub async fn get(&self, id: AgentVersionId) -> Result<Option<AgentVersion>> {
        let row = sqlx::query(&format!(
            "SELECT {AGENT_VERSION_COLUMNS} FROM agent_versions WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(fetch_agent_version)
            .transpose()?
            .map(AgentVersionRow::into_domain)
            .transpose()
    }

    /// Lists every version of an agent, newest first.
    pub async fn list_for_agent(&self, agent: AgentId) -> Result<Vec<AgentVersion>> {
        let rows = sqlx::query(&format!(
            "SELECT {AGENT_VERSION_COLUMNS} FROM agent_versions
             WHERE agent_id = $1 ORDER BY version DESC"
        ))
        .bind(Uuid::from(agent))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_agent_version)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(AgentVersionRow::into_domain)
            .collect()
    }
}

/// Classifies duplicate-per-tenant violations as conflict (with the natural
/// key named) so service callers can answer honestly.
fn map_write_conflict(error: &sqlx::Error, table: &'static str, slug: &str) -> AppError {
    let classified = map_sqlx_ref(error);
    match error {
        sqlx::Error::Database(db) if db.is_unique_violation() => AppError::conflict(format!(
            "{table} uniqueness violated for slug '{slug}' (per-tenant names are unique)"
        )),
        _ => classified,
    }
}

fn map_sqlx_ref(error: &sqlx::Error) -> AppError {
    match error {
        sqlx::Error::Database(db) if db.is_unique_violation() => {
            AppError::conflict("unique constraint violated")
        },
        sqlx::Error::Database(db) if db.is_foreign_key_violation() => {
            AppError::validation("foreign key constraint violated (scope row missing?)")
        },
        sqlx::Error::PoolTimedOut => AppError::database("timed out waiting for a free connection"),
        sqlx::Error::RowNotFound => AppError::not_found("row", "missing"),
        other => AppError::database(format!("database error: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_column_count_matches_projection() {
        // The 9 bind slots in AGENT_UPSERT line up with AgentRow's columns.
        let bind_marks = AGENT_UPSERT.matches('$').count();
        assert_eq!(bind_marks, 9, "statement arity drifted from AgentRow");
        assert_eq!(AGENT_COLUMNS.split(',').count(), 9);
        // And the bind projection's order is the semantic contract.
        let agent_row = AgentRow {
            id: Uuid::now_v7(),
            tenant_id: Uuid::now_v7(),
            organization_id: Uuid::now_v7(),
            name: "x".to_owned(),
            slug: "x".to_owned(),
            status: "draft".to_owned(),
            spec: serde_json::json!({}),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let args = agent_bind_projection(&agent_row).expect("projection");
        assert_eq!(args.len(), bind_marks);
        assert_eq!(args[0].as_str().unwrap(), agent_row.id.to_string());
        assert_eq!(args[5].as_str().unwrap(), "draft");
    }

    #[test]
    fn agent_version_insert_is_append_only() {
        assert!(AGENT_VERSION_INSERT.contains("ON CONFLICT (agent_id, version) DO NOTHING"));
        assert!(!AGENT_VERSION_INSERT.contains("DO UPDATE"));
    }
}
