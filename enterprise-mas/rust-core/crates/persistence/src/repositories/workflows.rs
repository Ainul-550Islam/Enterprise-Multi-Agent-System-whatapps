//! `workflows` repository (spec-JSONB style; graph travels inside the spec).
//!
//! The domain `Workflow` owns no slug as a field (name-only) but the schema
//! carries `slug citext NOT NULL UNIQUE (tenant_id, slug)` as its per-tenant
//! natural key. The repository therefore derives a stable slug from the name
//! on write; collisions surface as `AppError::conflict` (which is *the point*
//! of the natural key).

use mas_common::error::AppError;
use mas_common::ids::{ProjectId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_domain::workflow::Workflow;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::rows::WorkflowRow;

/// Full projected columns for `workflows` queries.
pub const WORKFLOW_COLUMNS: &str =
    "id, tenant_id, organization_id, project_id, name, slug, status, spec, created_at, updated_at";

const WORKFLOW_UPSERT: &str = "INSERT INTO workflows (
        id, tenant_id, organization_id, project_id, name, slug, status, spec,
        created_at, updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (id) DO UPDATE SET
        project_id = EXCLUDED.project_id,
        name = EXCLUDED.name,
        slug = EXCLUDED.slug,
        status = EXCLUDED.status,
        spec = EXCLUDED.spec,
        updated_at = EXCLUDED.updated_at";

/// Derives the per-tenant natural key from the display name. Deterministic
/// (same name → same slug, so the idempotent upsert by pk keeps the slug
/// stable unless the writer renamed deliberately).
pub fn slugify(name: &str) -> String {
    let mut out = String::with_capacity(name.len().min(200));
    let mut dash = false;
    for ch in name.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            if dash && !out.is_empty() {
                out.push('-');
            }
            dash = false;
            out.push(ch);
        } else if !dash {
            dash = true;
        }
    }
    if out.is_empty() {
        "workflow".to_owned()
    } else {
        out
    }
}

fn fetch_workflow(pg_row: sqlx::postgres::PgRow) -> Result<WorkflowRow> {
    Ok(WorkflowRow {
        id: pg_row.try_get("id").map_err(map_sqlx)?,
        tenant_id: pg_row.try_get("tenant_id").map_err(map_sqlx)?,
        organization_id: pg_row.try_get("organization_id").map_err(map_sqlx)?,
        project_id: pg_row.try_get("project_id").map_err(map_sqlx)?,
        name: pg_row.try_get("name").map_err(map_sqlx)?,
        slug: pg_row.try_get("slug").map_err(map_sqlx)?,
        status: pg_row.try_get("status").map_err(map_sqlx)?,
        spec: pg_row.try_get("spec").map_err(map_sqlx)?,
        created_at: pg_row.try_get("created_at").map_err(map_sqlx)?,
        updated_at: pg_row.try_get("updated_at").map_err(map_sqlx)?,
    })
}

/// PostgreSQL store for `workflows`.
#[derive(Debug, Clone)]
pub struct WorkflowStore {
    pool: PgPool,
}

impl WorkflowStore {
    /// Binds a store to a pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates or rewrites a workflow row (graph rides inside `spec`).
    pub async fn save(&self, workflow: &Workflow) -> Result<()> {
        let slug = slugify(&workflow.name);
        let row = WorkflowRow::from_domain(workflow, &slug)?;
        let result = sqlx::query(WORKFLOW_UPSERT)
            .bind(row.id)
            .bind(row.tenant_id)
            .bind(row.organization_id)
            .bind(row.project_id)
            .bind(&row.name)
            .bind(&row.slug)
            .bind(&row.status)
            .bind(&row.spec)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|error| match error {
                sqlx::Error::Database(db) if db.is_unique_violation() => AppError::conflict(
                    format!("workflow slug '{slug}' already in use in this tenant"),
                ),
                other => map_sqlx(other),
            })?;
        if result.rows_affected() != 1 {
            return Err(AppError::database(format!(
                "workflows upsert affected {} rows for id {}",
                result.rows_affected(),
                row.id
            )));
        }
        Ok(())
    }

    /// Loads one workflow by primary key.
    pub async fn get(&self, id: WorkflowId) -> Result<Option<Workflow>> {
        let row = sqlx::query(&format!(
            "SELECT {WORKFLOW_COLUMNS} FROM workflows WHERE id = $1"
        ))
        .bind(Uuid::from(id))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        row.map(fetch_workflow)
            .transpose()?
            .map(WorkflowRow::into_domain)
            .transpose()
    }

    /// Lists workflows attached to a project.
    pub async fn list_for_project(&self, project: ProjectId) -> Result<Vec<Workflow>> {
        let rows = sqlx::query(&format!(
            "SELECT {WORKFLOW_COLUMNS} FROM workflows
             WHERE project_id = $1 ORDER BY name"
        ))
        .bind(Uuid::from(project))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_workflow)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(WorkflowRow::into_domain)
            .collect()
    }

    /// Lists workflows in a tenant (name order for stable CLI output).
    pub async fn list_for_tenant(&self, tenant: TenantId) -> Result<Vec<Workflow>> {
        let rows = sqlx::query(&format!(
            "SELECT {WORKFLOW_COLUMNS} FROM workflows
             WHERE tenant_id = $1 ORDER BY name"
        ))
        .bind(Uuid::from(tenant))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(fetch_workflow)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(WorkflowRow::into_domain)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugs_are_stable_lowercase_natural_keys() {
        assert_eq!(slugify("Customer Onboarding"), "customer-onboarding");
        assert_eq!(slugify("order/pipeline v2"), "order-pipeline-v2");
        assert_eq!(slugify("---"), "workflow");
        assert_eq!(slugify("MiXeD SPACES  -- dash"), "mixed-spaces-dash");
    }

    #[test]
    fn upsert_arity_matches_projection() {
        assert_eq!(WORKFLOW_UPSERT.matches('$').count(), 10);
        assert_eq!(WORKFLOW_COLUMNS.split(',').count(), 10);
    }
}
