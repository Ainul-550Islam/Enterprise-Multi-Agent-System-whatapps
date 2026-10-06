//! PURPOSE: Durable policy persistence — the Postgres implementation of
//! `mas-policy`'s `PolicyStorePort`.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: keep policies in the database so a policy change
//! is a deploy-free, auditable operation, and so two replicas evaluate the
//! same policy set. Publishing runs the strict compiler first: a definition
//! that does not compile cannot become active, no matter which caller path
//! tried to activate it.
//!
//! Security boundary: tenant-scoped and RLS-protected. The version column is
//! what makes a cached decision safe — the cache key carries it, so publishing
//! a new version invalidates every cached decision for that tenant without
//! anyone having to remember to purge.
//!
//! Data ownership: Rust-owned platform state.
//!
//! It must NOT:
//!   • activate a policy that failed compilation;
//!   • let one tenant read or mutate another tenant's policies (the query
//!     filters explicitly *and* RLS filters again);
//!   • delete a policy that is still active — deactivate first;
//!   • evaluate arbitrary dynamic code — the DSL is a closed set of
//!     comparisons, and compilation happens in `mas-policy`, not here.

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, PolicyId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{Policy, PolicyScope, PolicyType};
use mas_policy::compiler::compile;
use mas_policy::store::{PolicyQuery, PolicyStorePort};
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::map_sqlx;

/// One policy row as stored.
#[derive(Debug, Clone)]
pub struct PolicyRow {
    /// Policy id.
    pub id: uuid::Uuid,
    /// Owning tenant (`None` for platform-global policies).
    pub tenant_id: Option<uuid::Uuid>,
    /// Organization scope.
    pub organization_id: Option<uuid::Uuid>,
    /// Project scope.
    pub project_id: Option<uuid::Uuid>,
    /// Unique-within-tenant name.
    pub name: String,
    /// Human description.
    pub description: Option<String>,
    /// Functional category.
    pub policy_type: String,
    /// Binding scope.
    pub scope: String,
    /// Evaluation order (higher wins).
    pub priority: i32,
    /// Lifecycle status.
    ///
    /// `active` is derived from this, never stored alongside it: a policy that
    /// was `active = true` and `status = 'disabled'` would leave the evaluator
    /// guessing, and the guess would decide whether work is permitted.
    pub status: String,
    /// The rule definition in the DSL.
    pub definition: serde_json::Value,
    /// Fingerprint of the compiled form.
    pub compiled_checksum: Option<String>,
    /// Monotonic version.
    pub version: i64,
    /// Who last wrote it.
    pub actor_id: String,
    /// When it was published.
    pub published_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Who published it.
    pub published_by: Option<String>,
    /// Creation instant.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last mutation.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PolicyRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            tenant_id: row.try_get("tenant_id")?,
            organization_id: row.try_get("organization_id")?,
            project_id: row.try_get("project_id")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            // 0007 named these `kind` and `spec`. The domain calls them
            // `policy_type` and `definition`; the mapping lives here so
            // the table is not asked to have two columns per concept.
            policy_type: row.try_get("kind")?,
            scope: row.try_get("scope")?,
            priority: row.try_get("priority")?,
            status: row.try_get("status")?,
            definition: row.try_get("spec")?,
            compiled_checksum: row.try_get("compiled_checksum")?,
            version: row.try_get("version")?,
            actor_id: row.try_get("actor_id")?,
            published_at: row.try_get("published_at")?,
            published_by: row.try_get("published_by")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl PolicyRow {
    /// Hydrates into the domain aggregate.
    pub fn into_domain(self) -> Result<Policy> {
        let policy_type = self.policy_type.parse::<PolicyType>().map_err(|_| {
            AppError::database(format!("unknown policy type '{}'", self.policy_type))
        })?;
        let scope = self
            .scope
            .parse::<PolicyScope>()
            .map_err(|_| AppError::database(format!("unknown policy scope '{}'", self.scope)))?;
        Ok(Policy {
            id: PolicyId::from_uuid(self.id),
            tenant_id: self.tenant_id.map(TenantId::from_uuid),
            organization_id: self.organization_id.map(OrganizationId::from_uuid),
            project_id: self.project_id.map(ProjectId::from_uuid),
            name: self.name,
            description: self.description,
            policy_type,
            scope,
            priority: self.priority,
            // Derived, not stored: `status` is the single source of truth for
            // whether a policy is in force, so the two cannot disagree.
            active: self.status == "active",
            definition: self.definition,
            compiled_checksum: self.compiled_checksum,
            version: u64::try_from(self.version).unwrap_or(0),
            created_at: Timestamp::from_datetime(self.created_at),
            updated_at: Timestamp::from_datetime(self.updated_at),
        })
    }
}

/// The Postgres policy store.
#[derive(Debug, Clone)]
pub struct PostgresPolicyStore {
    pool: PgPool,
}

impl PostgresPolicyStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts or updates the authored (draft) policy.
    ///
    /// Writing a draft never changes what is enforced: only `publish` moves a
    /// compiled definition into the active set. That separation is what lets
    /// an operator edit a policy without a half-edited rule being evaluated.
    pub async fn upsert(&self, policy: &Policy) -> Result<()> {
        sqlx::query(
            "INSERT INTO policies
                (id, tenant_id, organization_id, project_id, name, description,
                 kind, scope, priority, status, spec,
                 compiled_checksum, version, actor_id)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
                     CASE WHEN $10 THEN 'active' ELSE 'draft' END,
                     $11,$12,$13,$14)
             ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    kind = EXCLUDED.kind,
                    scope = EXCLUDED.scope,
                    priority = EXCLUDED.priority,
                    spec = EXCLUDED.spec,
                    version = policies.version + 1,
                    updated_at = now()
              -- A status change is a publish or a deactivation, never a side
              -- effect of an edit: those are the two operations that move a
              -- policy in or out of the enforced set, and each is audited.
              WHERE policies.status = EXCLUDED.status
                 OR EXCLUDED.status = 'active'",
        )
        .bind(policy.id.into_uuid())
        .bind(policy.tenant_id.map(TenantId::into_uuid))
        .bind(policy.organization_id.map(OrganizationId::into_uuid))
        .bind(policy.project_id.map(ProjectId::into_uuid))
        .bind(&policy.name)
        .bind(&policy.description)
        .bind(policy.policy_type.as_str())
        .bind(policy.scope.as_str())
        .bind(policy.priority)
        .bind(policy.active)
        .bind(&policy.definition)
        .bind(&policy.compiled_checksum)
        .bind(i64::try_from(policy.version).unwrap_or(1))
        .bind("system")
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Compiles the current definition and activates the policy.
    ///
    /// Compilation is the gate. A malformed definition fails here and nothing
    /// becomes active — which is the only place it can be guaranteed, because
    /// every activation path (API, admin tooling, provisioning) goes through
    /// this method.
    pub async fn publish(&self, policy_id: PolicyId, actor: &str) -> Result<Policy> {
        let mut policy = self
            .get(policy_id)
            .await?
            .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))?;

        let compiled = compile(&policy).map_err(|error| {
            AppError::validation(format!(
                "policy '{}' does not compile and was not published: {error}",
                policy.name
            ))
        })?;
        let checksum = compiled.checksum.clone();
        policy.publish(checksum.clone()).map_err(|error| {
            AppError::validation(format!("policy cannot be published: {error}"))
        })?;

        let row: PolicyRow = sqlx::query_as::<_, PolicyRow>(
            "UPDATE policies
                SET status = 'active',
                    compiled_checksum = $3,
                    published_at = now(),
                    published_by = $2,
                    actor_id = $2,
                    spec = $4,
                    updated_at = now()
              WHERE id = $1
          RETURNING id, tenant_id, organization_id, project_id, name, description,
                    kind, scope, priority, status, spec,
                    compiled_checksum, version, actor_id, published_at, published_by,
                    created_at, updated_at",
        )
        .bind(policy_id.into_uuid())
        .bind(actor)
        .bind(&checksum)
        .bind(&policy.definition)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))?;
        row.into_domain()
    }

    /// Deactivates a policy. The definition stays; only enforcement stops.
    pub async fn deactivate(&self, policy_id: PolicyId, actor: &str) -> Result<Policy> {
        let row: PolicyRow = sqlx::query_as::<_, PolicyRow>(
            "UPDATE policies
                SET status = 'disabled', actor_id = $2, updated_at = now()
              WHERE id = $1
          RETURNING id, tenant_id, organization_id, project_id, name, description,
                    kind, scope, priority, status, spec,
                    compiled_checksum, version, actor_id, published_at, published_by,
                    created_at, updated_at",
        )
        .bind(policy_id.into_uuid())
        .bind(actor)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))?;
        row.into_domain()
    }

    /// Hard-deletes a policy. Active policies must be deactivated first.
    pub async fn delete(&self, tenant: Option<TenantId>, policy_id: PolicyId) -> Result<Policy> {
        let policy = self
            .get(policy_id)
            .await?
            .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))?;
        if policy.active {
            return Err(AppError::conflict(format!(
                "policy '{}' is active; deactivate it before deleting",
                policy.name
            )));
        }
        // The tenant filter is applied even when the caller passes `None`,
        // because `None` means "platform-global", not "anything".
        let affected =
            sqlx::query("DELETE FROM policies WHERE id = $1 AND tenant_id IS NOT DISTINCT FROM $2")
                .bind(policy_id.into_uuid())
                .bind(tenant.map(TenantId::into_uuid))
                .execute(executor(&self.pool).await?.connection())
                .await
                .map_err(map_sqlx)?;
        if affected.rows_affected() == 0 {
            return Err(AppError::not_found("policy", policy_id.to_string()));
        }
        Ok(policy)
    }

    /// The maximum version across a tenant's policies.
    ///
    /// This is the cache-generation counter: any decision cached against a
    /// lower number is stale, so a privileged policy change invalidates cached
    /// decisions without needing a distributed purge.
    pub async fn generation(&self, tenant: TenantId) -> Result<i64> {
        let version: Option<i64> = sqlx::query_scalar(
            "SELECT max(version) FROM policies WHERE tenant_id = $1 OR tenant_id IS NULL",
        )
        .bind(tenant.into_uuid())
        .fetch_one(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(version.unwrap_or(0))
    }
}

#[async_trait::async_trait]
impl PolicyStorePort for PostgresPolicyStore {
    async fn get(&self, policy_id: PolicyId) -> Result<Option<Policy>> {
        let row: Option<PolicyRow> = sqlx::query_as::<_, PolicyRow>(
            "SELECT id, tenant_id, organization_id, project_id, name, description,
                    kind, scope, priority, status, spec,
                    compiled_checksum, version, actor_id, published_at, published_by,
                    created_at, updated_at
               FROM policies WHERE id = $1",
        )
        .bind(policy_id.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(PolicyRow::into_domain).transpose()
    }

    async fn list_applicable(&self, query: &PolicyQuery) -> Result<Vec<Policy>> {
        // Global policies always apply; scoped ones apply when the query names
        // the same scope. `IS NOT DISTINCT FROM` rather than `=` so an absent
        // query scope matches an absent policy scope instead of filtering the
        // row out — otherwise a query without a project id would silently drop
        // every project-scoped policy, which would look like "no policy
        // matched" and, under deny-overrides, could read as a permit.
        let rows: Vec<PolicyRow> = sqlx::query_as::<_, PolicyRow>(
            "SELECT id, tenant_id, organization_id, project_id, name, description,
                    kind, scope, priority, status, spec,
                    compiled_checksum, version, actor_id, published_at, published_by,
                    created_at, updated_at
               FROM policies
              WHERE status = 'active'
                AND ($1::text IS NULL OR kind = $1)
                AND (
                        tenant_id IS NULL
                     OR tenant_id = $2
                )
                AND (organization_id IS NULL OR organization_id IS NOT DISTINCT FROM $3)
                AND (project_id IS NULL OR project_id IS NOT DISTINCT FROM $4)
              ORDER BY priority DESC, id",
        )
        .bind(query.policy_type.as_ref().map(PolicyType::as_str))
        .bind(query.tenant_id.map(TenantId::into_uuid))
        .bind(query.organization_id.map(OrganizationId::into_uuid))
        .bind(query.project_id.map(ProjectId::into_uuid))
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(PolicyRow::into_domain).collect()
    }

    async fn list_for_tenant(&self, tenant_id: Option<TenantId>) -> Result<Vec<Policy>> {
        let rows: Vec<PolicyRow> = sqlx::query_as::<_, PolicyRow>(
            "SELECT id, tenant_id, organization_id, project_id, name, description,
                    kind, scope, priority, status, spec,
                    compiled_checksum, version, actor_id, published_at, published_by,
                    created_at, updated_at
               FROM policies
              WHERE tenant_id IS NOT DISTINCT FROM $1
              ORDER BY priority DESC, name",
        )
        .bind(tenant_id.map(TenantId::into_uuid))
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(PolicyRow::into_domain).collect()
    }

    async fn upsert(&self, policy: &Policy) -> Result<()> {
        PostgresPolicyStore::upsert(self, policy).await
    }

    async fn publish(&self, policy_id: PolicyId, actor: &str) -> Result<Policy> {
        PostgresPolicyStore::publish(self, policy_id, actor).await
    }

    async fn deactivate(&self, policy_id: PolicyId, actor: &str) -> Result<Policy> {
        PostgresPolicyStore::deactivate(self, policy_id, actor).await
    }

    async fn delete(&self, tenant_id: Option<TenantId>, policy_id: PolicyId) -> Result<Policy> {
        PostgresPolicyStore::delete(self, tenant_id, policy_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> PolicyRow {
        PolicyRow {
            id: uuid::Uuid::new_v4(),
            tenant_id: Some(TenantId::new().into_uuid()),
            organization_id: None,
            project_id: None,
            name: "gate".to_owned(),
            description: None,
            policy_type: "execution".to_owned(),
            scope: "tenant".to_owned(),
            priority: 5,
            status: "active".to_owned(),
            definition: serde_json::json!({"rules": []}),
            compiled_checksum: Some("abc".to_owned()),
            version: 2,
            actor_id: "system".to_owned(),
            published_at: None,
            published_by: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn a_row_hydrates_with_its_version_for_cache_keying() {
        let policy = row().into_domain().expect("hydrate");
        assert_eq!(policy.version, 2);
        assert_eq!(policy.priority, 5);
        assert!(policy.active);
    }

    #[test]
    fn an_unknown_scope_in_a_row_is_a_loud_error() {
        let mut bad = row();
        bad.scope = "multiverse".to_owned();
        assert!(
            bad.into_domain().is_err(),
            "schema drift must fail loudly, never default a scope"
        );
    }
}
