//! Cross-resource tenant-scoped search ranked by exact/prefix match then recency.

use async_trait::async_trait;
use mas_common::result::Result;
use mas_contracts::search::{SearchQuery, SearchResponseDto, SearchResultDto};

use crate::context::ServiceContext;
use crate::stores::{
    AgentStorePort, ConnectorStorePort, ExecutionStorePort, ProjectStorePort, WorkflowStorePort,
};

/// One pre-filtered, non-secret catalogue row returned by a durable search
/// adapter. The application owns scoring and final deterministic ordering so
/// PostgreSQL and in-memory paths cannot disagree on relevance semantics.
#[derive(Debug, Clone)]
pub struct SearchCandidate {
    /// Stable singular API resource kind (`agent`, `workflow`, ...).
    pub kind: String,
    /// Resource identifier encoded for the wire union.
    pub id: String,
    /// Safe display title.
    pub title: String,
    /// Safe optional secondary label.
    pub subtitle: Option<String>,
    /// Relative API href.
    pub href: String,
    /// Non-secret searchable labels; not returned directly.
    pub match_fields: Vec<String>,
}

/// Bounded durable catalogue search port. Implementations must apply a
/// parameterized match predicate and a SQL limit before materializing rows.
#[async_trait]
pub trait SearchCatalogPort: std::fmt::Debug + Send + Sync {
    async fn candidates(
        &self,
        tenant: mas_common::ids::TenantId,
        organization: mas_common::ids::OrganizationId,
        needle: &str,
        kinds: &std::collections::BTreeSet<String>,
        per_kind_limit: usize,
    ) -> Result<Vec<SearchCandidate>>;
}

/// Search backend. The legacy composition exists for tests/local in-memory
/// services; durable composition uses `Catalogue` so it never scans an entire
/// tenant just to satisfy a capped command-palette request.
#[derive(Debug)]
enum SearchBackend {
    Legacy {
        projects: Box<dyn ProjectStorePort>,
        agents: Box<dyn AgentStorePort>,
        workflows: Box<dyn WorkflowStorePort>,
        executions: Box<dyn ExecutionStorePort>,
        connectors: Box<dyn ConnectorStorePort>,
    },
    Catalogue(Box<dyn SearchCatalogPort>),
}

/// Search across tenant-visible agents, workflows, executions and connectors.
#[derive(Debug)]
pub struct SearchService {
    backend: SearchBackend,
}

impl SearchService {
    /// Composes the service over tenant-scoped store ports.
    pub fn new(
        projects: Box<dyn ProjectStorePort>,
        agents: Box<dyn AgentStorePort>,
        workflows: Box<dyn WorkflowStorePort>,
        executions: Box<dyn ExecutionStorePort>,
        connectors: Box<dyn ConnectorStorePort>,
    ) -> Self {
        Self {
            backend: SearchBackend::Legacy {
                projects,
                agents,
                workflows,
                executions,
                connectors,
            },
        }
    }

    /// Composes the production bounded catalogue adapter. It is deliberately
    /// separate from `new`: a durable process must not accidentally fall back
    /// to full aggregate scans when a search endpoint is enabled.
    #[must_use]
    pub fn with_catalog(catalogue: Box<dyn SearchCatalogPort>) -> Self {
        Self {
            backend: SearchBackend::Catalogue(catalogue),
        }
    }

    /// Runs a bounded tenant search; no document content or secret-bearing
    /// connector configuration is projected into the results.
    pub async fn search(
        &self,
        ctx: &ServiceContext,
        query: &SearchQuery,
    ) -> Result<SearchResponseDto> {
        query.validate()?;
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let kinds = normalized_kinds(&query.kinds);
        let needle = query.q.trim().to_ascii_lowercase();
        let limit = usize::from(query.limit.unwrap_or(20));
        let mut hits = match &self.backend {
            SearchBackend::Legacy {
                projects,
                agents,
                workflows,
                executions,
                connectors,
            } => {
                legacy_hits(
                    projects.as_ref(),
                    agents.as_ref(),
                    workflows.as_ref(),
                    executions.as_ref(),
                    connectors.as_ref(),
                    tenant,
                    organization,
                    &needle,
                    &kinds,
                )
                .await?
            },
            SearchBackend::Catalogue(catalogue) => catalogue
                .candidates(
                    tenant,
                    organization,
                    &needle,
                    &kinds,
                    limit.saturating_add(1),
                )
                .await?
                .into_iter()
                // The durable adapter is an internal boundary, but it still
                // must not be able to widen a caller's requested kind filter.
                // Unknown singular kinds are discarded fail-closed.
                .filter(|candidate| candidate_kind_requested(&kinds, &candidate.kind))
                .filter_map(|candidate| {
                    match_score(&needle, candidate.match_fields.iter().map(String::as_str)).map(
                        |score| SearchResultDto {
                            kind: candidate.kind,
                            id: candidate.id,
                            title: candidate.title,
                            subtitle: candidate.subtitle,
                            score,
                            href: candidate.href,
                        },
                    )
                })
                .collect(),
        };
        sort_hits(&mut hits);
        let truncated = hits.len() > limit;
        hits.truncate(limit);
        Ok(SearchResponseDto {
            items: hits,
            truncated,
        })
    }
}

async fn legacy_hits(
    projects: &dyn ProjectStorePort,
    agents: &dyn AgentStorePort,
    workflows: &dyn WorkflowStorePort,
    executions: &dyn ExecutionStorePort,
    connectors: &dyn ConnectorStorePort,
    tenant: mas_common::ids::TenantId,
    organization: mas_common::ids::OrganizationId,
    needle: &str,
    kinds: &std::collections::BTreeSet<String>,
) -> Result<Vec<SearchResultDto>> {
    let all_kinds = kinds.is_empty();
    let mut hits = Vec::new();
    if all_kinds || kinds.contains("agents") || kinds.contains("workflows") {
        for project in projects.list(tenant).await? {
            if project.organization_id != organization {
                continue;
            }
            if all_kinds || kinds.contains("agents") {
                for agent in agents.list(project.id).await? {
                    if agent.tenant_id != tenant || agent.organization_id != organization {
                        continue;
                    }
                    if let Some(score) =
                        match_score(needle, [agent.name.as_str(), agent.slug.as_str()])
                    {
                        hits.push(SearchResultDto {
                            kind: "agent".to_owned(),
                            id: agent.id.to_string(),
                            title: agent.name,
                            subtitle: Some(format!("{} · {}", agent.slug, agent.status)),
                            score,
                            href: format!("/v1/agents/{}", agent.id),
                        });
                    }
                }
            }
            if all_kinds || kinds.contains("workflows") {
                for workflow in workflows.list(project.id).await? {
                    if workflow.tenant_id != tenant || workflow.organization_id != organization {
                        continue;
                    }
                    if let Some(score) = match_score(needle, [workflow.name.as_str()]) {
                        hits.push(SearchResultDto {
                            kind: "workflow".to_owned(),
                            id: workflow.id.to_string(),
                            title: workflow.name,
                            subtitle: Some(format!(
                                "{} · {} nodes",
                                workflow.status,
                                workflow.graph.nodes.len()
                            )),
                            score,
                            href: format!("/v1/workflows/{}", workflow.id),
                        });
                    }
                }
            }
        }
    }
    if all_kinds || kinds.contains("executions") {
        for execution in executions.list(tenant).await? {
            if execution.tenant_id != tenant || execution.organization_id != organization {
                continue;
            }
            let label = execution
                .workflow_id
                .map(|id| format!("workflow {id}"))
                .or_else(|| execution.agent_id.map(|id| format!("agent {id}")))
                .unwrap_or_else(|| "execution".to_owned());
            if let Some(score) =
                match_score(needle, [label.as_str(), execution.correlation_id.as_str()])
            {
                hits.push(SearchResultDto {
                    kind: "execution".to_owned(),
                    id: execution.id.to_string(),
                    title: label,
                    subtitle: Some(format!("{} · {}", execution.status, execution.created_at)),
                    score,
                    href: format!("/v1/executions/{}", execution.id),
                });
            }
        }
    }
    if all_kinds || kinds.contains("connectors") {
        for connector in connectors.list(tenant).await? {
            if connector.tenant_id != tenant || connector.organization_id != organization {
                continue;
            }
            if let Some(score) = match_score(
                needle,
                [connector.name.as_str(), connector.provider.as_str()],
            ) {
                hits.push(SearchResultDto {
                    kind: "connector".to_owned(),
                    id: connector.id.to_string(),
                    title: connector.name,
                    subtitle: Some(format!("{} · {}", connector.provider, connector.status)),
                    score,
                    href: format!("/v1/connectors/{}", connector.id),
                });
            }
        }
    }
    Ok(hits)
}

fn sort_hits(hits: &mut [SearchResultDto]) {
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| {
                left.title
                    .to_ascii_lowercase()
                    .cmp(&right.title.to_ascii_lowercase())
            })
            .then_with(|| left.id.cmp(&right.id))
    });
}

fn normalized_kinds(kinds: &[String]) -> std::collections::BTreeSet<String> {
    kinds.iter().map(|kind| kind.to_ascii_lowercase()).collect()
}

/// Verifies the singular kind an adapter projected remains inside the caller's
/// plural wire filter. A malformed adapter row is ignored rather than exposed.
fn candidate_kind_requested(kinds: &std::collections::BTreeSet<String>, kind: &str) -> bool {
    kinds.is_empty()
        || matches!(
            (
                kind,
                kinds.contains("agents"),
                kinds.contains("workflows"),
                kinds.contains("executions"),
                kinds.contains("connectors")
            ),
            ("agent", true, _, _, _)
                | ("workflow", _, true, _, _)
                | ("execution", _, _, true, _)
                | ("connector", _, _, _, true)
        )
}

fn match_score<'a>(needle: &str, candidates: impl IntoIterator<Item = &'a str>) -> Option<f64> {
    let mut best = 0.0_f64;
    for candidate in candidates {
        let candidate = candidate.to_ascii_lowercase();
        if candidate == needle {
            best = best.max(3.0);
        } else if candidate.starts_with(needle) {
            best = best.max(2.0);
        } else if candidate.contains(needle) {
            best = best.max(1.0);
        }
    }
    (best > 0.0).then_some(best)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;

    #[test]
    fn exact_match_outranks_prefix_which_outranks_substring() {
        assert_eq!(match_score("sales", ["sales"]), Some(3.0));
        assert_eq!(match_score("sales", ["salesforce"]), Some(2.0));
        assert_eq!(match_score("sales", ["enterprise sales bot"]), Some(1.0));
        assert_eq!(match_score("absent", ["sales"]), None);
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CatalogueCall {
        tenant: TenantId,
        organization: OrganizationId,
        needle: String,
        kinds: std::collections::BTreeSet<String>,
        per_kind_limit: usize,
    }

    #[derive(Debug)]
    struct CapturingCatalogue {
        rows: Vec<SearchCandidate>,
        calls: std::sync::Arc<std::sync::Mutex<Vec<CatalogueCall>>>,
    }

    #[async_trait]
    impl SearchCatalogPort for CapturingCatalogue {
        async fn candidates(
            &self,
            tenant: TenantId,
            organization: OrganizationId,
            needle: &str,
            kinds: &std::collections::BTreeSet<String>,
            per_kind_limit: usize,
        ) -> Result<Vec<SearchCandidate>> {
            self.calls
                .lock()
                .expect("test call lock")
                .push(CatalogueCall {
                    tenant,
                    organization,
                    needle: needle.to_owned(),
                    kinds: kinds.clone(),
                    per_kind_limit,
                });
            Ok(self.rows.clone())
        }
    }

    fn candidate(kind: &str, id: &str, title: &str) -> SearchCandidate {
        SearchCandidate {
            kind: kind.to_owned(),
            id: id.to_owned(),
            title: title.to_owned(),
            subtitle: None,
            href: format!("/v1/{kind}s/{id}"),
            match_fields: vec![title.to_owned()],
        }
    }

    #[tokio::test]
    async fn catalogue_search_defensively_filters_kinds_orders_and_truncates() {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let service = SearchService::with_catalog(Box::new(CapturingCatalogue {
            // The connector is deliberately returned despite the caller's
            // agents-only filter: adapter defects must not widen visibility.
            rows: vec![
                candidate("agent", "b", "Alphabet"),
                candidate("connector", "c", "Team Alpha"),
                candidate("agent", "a", "Alpha"),
                candidate("unknown", "d", "Alpha"),
            ],
            calls: calls.clone(),
        }));
        let ctx = ServiceContext::system("search-test", &Timestamp::now())
            .expect("context")
            .with_scope(tenant, organization);
        let response = service
            .search(
                &ctx,
                &SearchQuery {
                    q: "alpha".to_owned(),
                    kinds: vec!["agents".to_owned()],
                    limit: Some(1),
                },
            )
            .await
            .expect("search");

        assert!(
            response.truncated,
            "the second permitted agent is bounded out"
        );
        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].kind, "agent");
        assert_eq!(response.items[0].id, "a", "exact outranks prefix");
        let recorded = calls.lock().expect("test call lock");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].tenant, tenant);
        assert_eq!(recorded[0].organization, organization);
        assert_eq!(recorded[0].needle, "alpha");
        assert_eq!(
            recorded[0].kinds,
            std::collections::BTreeSet::from(["agents".to_owned()])
        );
        assert_eq!(
            recorded[0].per_kind_limit, 2,
            "global limit plus truncation sentinel"
        );
    }

    #[tokio::test]
    async fn query_is_validated_before_tenant_ports_are_used() {
        let store = std::sync::Arc::new(crate::stores::InMemoryServices::new());
        let service = SearchService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
        );
        let ctx = ServiceContext::system("search-test", &Timestamp::now())
            .expect("context")
            .with_scope(TenantId::new(), OrganizationId::new());
        let invalid = SearchQuery {
            q: " ".to_owned(),
            kinds: Vec::new(),
            limit: None,
        };
        assert!(service.search(&ctx, &invalid).await.is_err());
    }
}
