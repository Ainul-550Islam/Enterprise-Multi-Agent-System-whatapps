//! Bounded, tenant-scoped PostgreSQL command-palette search adapter.
//!
//! Unlike aggregate list ports, this adapter pushes matching and per-kind
//! limits into SQL. A short palette query must never hydrate every agent,
//! execution or connector in a large tenant merely to return twenty rows.

use mas_application::search_service::{SearchCandidate, SearchCatalogPort};
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::result::Result;
use sqlx::{PgPool, Row};

use crate::error::map_sqlx;

/// PostgreSQL implementation of the bounded palette search port.
#[derive(Debug, Clone)]
pub struct PostgresSearchCatalog {
    pool: PgPool,
}

impl PostgresSearchCatalog {
    /// Binds the catalogue to the request-scoped application pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Diagnostic-only pool handle; query execution always uses the scope's
    /// open transaction.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl SearchCatalogPort for PostgresSearchCatalog {
    async fn candidates(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        needle: &str,
        kinds: &std::collections::BTreeSet<String>,
        per_kind_limit: usize,
    ) -> Result<Vec<SearchCandidate>> {
        let scope = crate::scoped_request::current_scope()?;
        if scope.tenant_id() != tenant || scope.organization_id() != organization {
            return Err(AppError::not_found("tenant", tenant.to_string()));
        }
        let limit = i64::try_from(per_kind_limit.clamp(1, 101))
            .map_err(|_| AppError::internal("search candidate bound is not representable"))?;
        let escaped = escape_like(needle);
        let prefix = format!("{escaped}%");
        let contains = format!("%{escaped}%");
        let mut candidates = Vec::new();
        if wants(kinds, "agents") {
            candidates.extend(
                search_agents(tenant, organization, needle, &prefix, &contains, limit).await?,
            );
        }
        if wants(kinds, "workflows") {
            candidates.extend(
                search_workflows(tenant, organization, needle, &prefix, &contains, limit).await?,
            );
        }
        if wants(kinds, "executions") {
            candidates.extend(
                search_executions(tenant, organization, needle, &prefix, &contains, limit).await?,
            );
        }
        if wants(kinds, "connectors") {
            candidates.extend(
                search_connectors(tenant, organization, needle, &prefix, &contains, limit).await?,
            );
        }
        Ok(candidates)
    }
}

async fn search_agents(
    tenant: TenantId,
    organization: OrganizationId,
    exact: &str,
    prefix: &str,
    contains: &str,
    limit: i64,
) -> Result<Vec<SearchCandidate>> {
    let mut scoped = crate::scoped_request::connection().await?;
    let rows = sqlx::query(
        "SELECT id::text AS id, name, slug::text AS slug, status
         FROM agents
         WHERE tenant_id = $1 AND organization_id = $2
           AND (name ILIKE $5 ESCAPE '\\' OR slug::text ILIKE $5 ESCAPE '\\')
         ORDER BY CASE
             WHEN name ILIKE $3 ESCAPE '\\' OR slug::text ILIKE $3 ESCAPE '\\' THEN 3
             WHEN name ILIKE $4 ESCAPE '\\' OR slug::text ILIKE $4 ESCAPE '\\' THEN 2
             ELSE 1 END DESC,
             lower(name), id
         LIMIT $6",
    )
    .bind(tenant.into_uuid())
    .bind(organization.into_uuid())
    .bind(exact)
    .bind(prefix)
    .bind(contains)
    .bind(limit)
    .fetch_all(scoped.connection().as_mut())
    .await
    .map_err(map_sqlx)?;
    rows.into_iter()
        .map(|row| {
            let id: String = row.try_get("id").map_err(map_sqlx)?;
            let name: String = row.try_get("name").map_err(map_sqlx)?;
            let slug: String = row.try_get("slug").map_err(map_sqlx)?;
            let status: String = row.try_get("status").map_err(map_sqlx)?;
            Ok(SearchCandidate {
                kind: "agent".to_owned(),
                href: format!("/v1/agents/{id}"),
                id,
                title: name.clone(),
                subtitle: Some(format!("{slug} · {status}")),
                match_fields: vec![name, slug],
            })
        })
        .collect()
}

async fn search_workflows(
    tenant: TenantId,
    organization: OrganizationId,
    exact: &str,
    prefix: &str,
    contains: &str,
    limit: i64,
) -> Result<Vec<SearchCandidate>> {
    let mut scoped = crate::scoped_request::connection().await?;
    let rows = sqlx::query(
        "SELECT id::text AS id, name, status
         FROM workflows
         WHERE tenant_id = $1 AND organization_id = $2
           AND name ILIKE $5 ESCAPE '\\'
         ORDER BY CASE
             WHEN name ILIKE $3 ESCAPE '\\' THEN 3
             WHEN name ILIKE $4 ESCAPE '\\' THEN 2
             ELSE 1 END DESC,
             lower(name), id
         LIMIT $6",
    )
    .bind(tenant.into_uuid())
    .bind(organization.into_uuid())
    .bind(exact)
    .bind(prefix)
    .bind(contains)
    .bind(limit)
    .fetch_all(scoped.connection().as_mut())
    .await
    .map_err(map_sqlx)?;
    rows.into_iter()
        .map(|row| {
            let id: String = row.try_get("id").map_err(map_sqlx)?;
            let name: String = row.try_get("name").map_err(map_sqlx)?;
            let status: String = row.try_get("status").map_err(map_sqlx)?;
            Ok(SearchCandidate {
                kind: "workflow".to_owned(),
                href: format!("/v1/workflows/{id}"),
                id,
                title: name.clone(),
                subtitle: Some(status),
                match_fields: vec![name],
            })
        })
        .collect()
}

async fn search_executions(
    tenant: TenantId,
    organization: OrganizationId,
    exact: &str,
    prefix: &str,
    contains: &str,
    limit: i64,
) -> Result<Vec<SearchCandidate>> {
    let mut scoped = crate::scoped_request::connection().await?;
    let rows = sqlx::query(
        "SELECT id::text AS id,
                COALESCE('workflow ' || workflow_id::text, 'agent ' || agent_id::text, 'execution') AS title,
                correlation_id, status, created_at
         FROM executions
         WHERE tenant_id = $1 AND organization_id = $2
           AND (
                COALESCE('workflow ' || workflow_id::text, 'agent ' || agent_id::text, 'execution')
                    ILIKE $5 ESCAPE '\\'
                OR correlation_id ILIKE $5 ESCAPE '\\'
           )
         ORDER BY CASE
             WHEN COALESCE('workflow ' || workflow_id::text, 'agent ' || agent_id::text, 'execution')
                    ILIKE $3 ESCAPE '\\'
                  OR correlation_id ILIKE $3 ESCAPE '\\' THEN 3
             WHEN COALESCE('workflow ' || workflow_id::text, 'agent ' || agent_id::text, 'execution')
                    ILIKE $4 ESCAPE '\\'
                  OR correlation_id ILIKE $4 ESCAPE '\\' THEN 2
             ELSE 1 END DESC,
             created_at DESC, id
         LIMIT $6",
    )
    .bind(tenant.into_uuid())
    .bind(organization.into_uuid())
    .bind(exact)
    .bind(prefix)
    .bind(contains)
    .bind(limit)
    .fetch_all(scoped.connection().as_mut())
    .await
    .map_err(map_sqlx)?;
    rows.into_iter()
        .map(|row| {
            let id: String = row.try_get("id").map_err(map_sqlx)?;
            let title: String = row.try_get("title").map_err(map_sqlx)?;
            let correlation_id: String = row.try_get("correlation_id").map_err(map_sqlx)?;
            let status: String = row.try_get("status").map_err(map_sqlx)?;
            let created_at: chrono::DateTime<chrono::Utc> =
                row.try_get("created_at").map_err(map_sqlx)?;
            Ok(SearchCandidate {
                kind: "execution".to_owned(),
                href: format!("/v1/executions/{id}"),
                id,
                title: title.clone(),
                subtitle: Some(format!("{status} · {created_at}")),
                match_fields: vec![title, correlation_id],
            })
        })
        .collect()
}

async fn search_connectors(
    tenant: TenantId,
    organization: OrganizationId,
    exact: &str,
    prefix: &str,
    contains: &str,
    limit: i64,
) -> Result<Vec<SearchCandidate>> {
    let mut scoped = crate::scoped_request::connection().await?;
    let rows = sqlx::query(
        "SELECT id::text AS id, name, provider, status
         FROM connectors
         WHERE tenant_id = $1 AND organization_id = $2
           AND (name ILIKE $5 ESCAPE '\\' OR provider ILIKE $5 ESCAPE '\\')
         ORDER BY CASE
             WHEN name ILIKE $3 ESCAPE '\\' OR provider ILIKE $3 ESCAPE '\\' THEN 3
             WHEN name ILIKE $4 ESCAPE '\\' OR provider ILIKE $4 ESCAPE '\\' THEN 2
             ELSE 1 END DESC,
             lower(name), id
         LIMIT $6",
    )
    .bind(tenant.into_uuid())
    .bind(organization.into_uuid())
    .bind(exact)
    .bind(prefix)
    .bind(contains)
    .bind(limit)
    .fetch_all(scoped.connection().as_mut())
    .await
    .map_err(map_sqlx)?;
    rows.into_iter()
        .map(|row| {
            let id: String = row.try_get("id").map_err(map_sqlx)?;
            let name: String = row.try_get("name").map_err(map_sqlx)?;
            let provider: String = row.try_get("provider").map_err(map_sqlx)?;
            let status: String = row.try_get("status").map_err(map_sqlx)?;
            Ok(SearchCandidate {
                kind: "connector".to_owned(),
                href: format!("/v1/connectors/{id}"),
                id,
                title: name.clone(),
                subtitle: Some(format!("{provider} · {status}")),
                match_fields: vec![name, provider],
            })
        })
        .collect()
}

fn wants(kinds: &std::collections::BTreeSet<String>, kind: &str) -> bool {
    kinds.is_empty() || kinds.contains(kind)
}

/// Escapes SQL LIKE metacharacters so a user query is text, not a wildcard
/// program. Queries are bound values either way; this additionally preserves
/// the product semantics of literal substring/prefix matching.
fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_escaper_keeps_user_wildcards_literal() {
        assert_eq!(escape_like(r"a%_\\b"), r"a\%\_\\\\b");
    }
}
