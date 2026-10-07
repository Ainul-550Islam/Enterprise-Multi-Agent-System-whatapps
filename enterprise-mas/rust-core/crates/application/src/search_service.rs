//! Cross-resource tenant-scoped search ranked by exact/prefix match then recency.

use mas_common::result::Result;
use mas_contracts::search::{SearchQuery, SearchResponseDto, SearchResultDto};

use crate::context::ServiceContext;
use crate::stores::{
    AgentStorePort, ConnectorStorePort, ExecutionStorePort, ProjectStorePort, WorkflowStorePort,
};

/// Search across tenant-visible agents, workflows, executions and connectors.
#[derive(Debug)]
pub struct SearchService {
    projects: Box<dyn ProjectStorePort>,
    agents: Box<dyn AgentStorePort>,
    workflows: Box<dyn WorkflowStorePort>,
    executions: Box<dyn ExecutionStorePort>,
    connectors: Box<dyn ConnectorStorePort>,
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
            projects,
            agents,
            workflows,
            executions,
            connectors,
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
        let all_kinds = kinds.is_empty();
        let needle = query.q.trim().to_ascii_lowercase();
        let mut hits = Vec::new();
        if all_kinds || kinds.contains("agents") || kinds.contains("workflows") {
            for project in self.projects.list(tenant).await? {
                if project.organization_id != organization {
                    continue;
                }
                if kinds.is_empty() || kinds.contains("agents") {
                    for agent in self.agents.list(project.id).await? {
                        if agent.tenant_id != tenant || agent.organization_id != organization {
                            continue;
                        }
                        if let Some(score) =
                            match_score(&needle, [agent.name.as_str(), agent.slug.as_str()])
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
                if kinds.is_empty() || kinds.contains("workflows") {
                    for workflow in self.workflows.list(project.id).await? {
                        if workflow.tenant_id != tenant || workflow.organization_id != organization
                        {
                            continue;
                        }
                        if let Some(score) = match_score(&needle, [workflow.name.as_str()]) {
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
        if kinds.is_empty() || kinds.contains("executions") {
            for execution in self.executions.list(tenant).await? {
                if execution.tenant_id != tenant {
                    continue;
                }
                let label = execution
                    .workflow_id
                    .map(|id| format!("workflow {id}"))
                    .or_else(|| execution.agent_id.map(|id| format!("agent {id}")))
                    .unwrap_or_else(|| "execution".to_owned());
                if let Some(score) =
                    match_score(&needle, [label.as_str(), execution.correlation_id.as_str()])
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
        if kinds.is_empty() || kinds.contains("connectors") {
            for connector in self.connectors.list(tenant).await? {
                if connector.tenant_id != tenant {
                    continue;
                }
                if let Some(score) = match_score(
                    &needle,
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
        let limit = usize::from(query.limit.unwrap_or(20));
        let truncated = hits.len() > limit;
        hits.truncate(limit);
        Ok(SearchResponseDto {
            items: hits,
            truncated,
        })
    }
}

fn normalized_kinds(kinds: &[String]) -> std::collections::BTreeSet<String> {
    kinds.iter().map(|kind| kind.to_ascii_lowercase()).collect()
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
