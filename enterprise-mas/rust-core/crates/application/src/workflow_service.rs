//! Workflow lifecycle use-cases: creation, versioned graph updates with
//! full structural validation (domain rules + acyclicity + a deterministic
//! topological order computed at the use-case boundary for the runtime).

use mas_common::error::AppError;
use mas_common::ids::{ProjectId, WorkflowId};
use mas_common::result::Result;
use mas_domain::{Workflow, WorkflowEdge, WorkflowGraph, WorkflowNode};
use std::collections::{BTreeSet, VecDeque};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::stores::{ProjectStorePort, WorkflowStorePort};

/// Use-cases over workflows and their graphs.
#[derive(Debug)]
pub struct WorkflowService {
    projects: Box<dyn ProjectStorePort>,
    workflows: Box<dyn WorkflowStorePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl WorkflowService {
    /// Composes the service over store ports + the audit sink.
    pub fn new(
        projects: Box<dyn ProjectStorePort>,
        workflows: Box<dyn WorkflowStorePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            projects,
            workflows,
            audit,
        }
    }

    /// Creates a workflow in `Draft` (empty graph) under a scope-coherent
    /// project.
    pub async fn create(
        &self,
        ctx: &ServiceContext,
        project: ProjectId,
        name: &str,
    ) -> Result<Workflow> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        self.projects
            .get(project)
            .await?
            .filter(|p| p.tenant_id == tenant_id && p.organization_id == organization_id)
            .ok_or_else(|| AppError::not_found("project", project.to_string()))?;

        let workflow = Workflow::create(tenant_id, organization_id, project, name)?;
        self.workflows.save(&workflow).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "workflow.create",
            "workflow",
            Some(workflow.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(workflow)
    }

    /// Replaces the graph after the full validation chain:
    /// 1. domain structural rules (`WorkflowGraph::validate`),
    /// 2. acyclicity (explicit, user-facing error — not just a bool),
    /// 3. deterministic topological order computed and returned so the
    ///    runtime never re-derives execution order.
    pub async fn update_graph(
        &self,
        ctx: &ServiceContext,
        workflow_id: WorkflowId,
        nodes: Vec<WorkflowNode>,
        edges: Vec<WorkflowEdge>,
    ) -> Result<Vec<String>> {
        let mut workflow = self.scope_checked(ctx, workflow_id).await?;
        let graph = WorkflowGraph::new(nodes, edges);
        graph.validate()?;
        if graph.has_cycle() {
            return Err(AppError::validation(
                "workflow graph must be acyclic: a cycle was detected among node references",
            ));
        }
        let order = topological_order(&graph)?;
        workflow.update_graph(graph)?;
        self.workflows.save(&workflow).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "workflow.update_graph",
            "workflow",
            Some(workflow.id.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        tracing::info!(workflow = %workflow.id, "workflow graph updated");
        Ok(order)
    }

    /// Scope-checked fetch (existence hiding).
    pub async fn get(&self, ctx: &ServiceContext, workflow_id: WorkflowId) -> Result<Workflow> {
        self.scope_checked(ctx, workflow_id).await
    }

    /// Lists a project's workflows in scope.
    pub async fn list(&self, ctx: &ServiceContext, project: ProjectId) -> Result<Vec<Workflow>> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        self.projects
            .get(project)
            .await?
            .filter(|p| p.tenant_id == tenant_id && p.organization_id == organization_id)
            .ok_or_else(|| AppError::not_found("project", project.to_string()))?;
        self.workflows.list(project).await
    }

    /// The topological order of a workflow's stored graph, when present.
    pub async fn graph_order(
        &self,
        ctx: &ServiceContext,
        workflow_id: WorkflowId,
    ) -> Result<Vec<String>> {
        let workflow = self.scope_checked(ctx, workflow_id).await?;
        topological_order(&workflow.graph)
    }

    async fn scope_checked(
        &self,
        ctx: &ServiceContext,
        workflow_id: WorkflowId,
    ) -> Result<Workflow> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        self.workflows
            .get(workflow_id)
            .await?
            .filter(|w| w.tenant_id == tenant_id && w.organization_id == organization_id)
            .ok_or_else(|| AppError::not_found("workflow", workflow_id.to_string()))
    }
}

/// Deterministic Kahn topological order over the graph's node keys:
/// ties break by node key ordering (BTreeSet frontier), so the same graph
/// always yields the same order — a requirement for checkpoint/replay.
pub fn topological_order(graph: &WorkflowGraph) -> Result<Vec<String>> {
    let mut in_degrees: std::collections::BTreeMap<String, usize> = graph
        .in_degrees()
        .into_iter()
        .map(|(key, degree)| (key.to_owned(), degree))
        .collect();
    let mut frontier: BTreeSet<String> = in_degrees
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(key, _)| key.clone())
        .collect();
    let adjacency = graph.adjacency();

    let mut queue: VecDeque<String> = frontier.iter().cloned().collect();
    let mut order = Vec::with_capacity(in_degrees.len());
    while let Some(node) = queue.pop_front() {
        frontier.remove(&node);
        order.push(node.clone());
        if let Some(children) = adjacency.get(node.as_str()) {
            let mut children: Vec<String> =
                children.iter().map(|child| (*child).to_owned()).collect();
            children.sort();
            for child in children {
                if let Some(degree) = in_degrees.get_mut(&child) {
                    *degree -= 1;
                    if *degree == 0 && !order.contains(&child) && frontier.insert(child.clone()) {
                        queue.push_back(child);
                    }
                }
            }
        }
    }
    if order.len() != in_degrees.len() {
        return Err(AppError::validation(
            "workflow graph could not be fully ordered (cycle or unreachable reference)",
        ));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use crate::stores::InMemoryServices;
    use crate::tenancy_service::TenancyService;
    use mas_common::enums::Environment;
    use mas_common::ids::OrganizationId;
    use mas_common::timestamps::Timestamp;
    use mas_domain::{IsolationMode, WorkflowNodeType};

    struct Fixture {
        service: WorkflowService,
        tenant_ctx: ServiceContext,
        project: ProjectId,
    }

    async fn fixture() -> Fixture {
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let tenancy = TenancyService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        );
        let service =
            WorkflowService::new(Box::new(store.clone()), Box::new(store), Box::new(audit));
        let system = ServiceContext::system("setup", &now).expect("ctx");
        let organization = tenancy
            .register_organization(&system, "Acme", "Acme", "acme-wf")
            .await
            .expect("org");
        let org_scoped = system.clone().with_scope(TenantId::new(), organization.id);
        let tenant = tenancy
            .register_tenant(
                &org_scoped,
                "Prod",
                "prod-wf",
                Environment::Production,
                IsolationMode::SharedRls,
            )
            .await
            .expect("tenant");
        let tenant_ctx = system.clone().with_scope(tenant.id, organization.id);
        let project = tenancy
            .register_project(&tenant_ctx, "Ops", "ops-wf")
            .await
            .expect("project");
        Fixture {
            service,
            tenant_ctx,
            project: project.id,
        }
    }

    use mas_common::ids::TenantId;
    use std::sync::Arc;

    fn node(key: &str, node_type: WorkflowNodeType) -> WorkflowNode {
        let mut node = WorkflowNode::new(key, node_type, key).expect("node");
        match node_type {
            WorkflowNodeType::Agent => {
                node.config.insert(
                    "agent_ref".to_owned(),
                    serde_json::Value::String("agent-x".to_owned()),
                );
            },
            WorkflowNodeType::Tool => {
                node.config.insert(
                    "tool_ref".to_owned(),
                    serde_json::Value::String("tool-x".to_owned()),
                );
            },
            _ => {},
        }
        node
    }

    #[tokio::test]
    async fn graph_updates_validate_order_and_persist() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;
        let workflow = fixture
            .service
            .create(ctx, fixture.project, "ticket-flow")
            .await
            .expect("create");

        let order = fixture
            .service
            .update_graph(
                ctx,
                workflow.id,
                vec![
                    node("start", WorkflowNodeType::Start),
                    node("triage", WorkflowNodeType::Agent),
                    node("repair", WorkflowNodeType::Tool),
                    node("end", WorkflowNodeType::End),
                ],
                vec![
                    WorkflowEdge::new("start", "triage").expect("edge"),
                    WorkflowEdge::new("triage", "repair").expect("edge"),
                    WorkflowEdge::new("repair", "end").expect("edge"),
                ],
            )
            .await
            .expect("update");
        assert_eq!(order, vec!["start", "triage", "repair", "end"]);

        // Persisted order matches the stored graph.
        let stored = fixture
            .service
            .graph_order(ctx, workflow.id)
            .await
            .expect("order");
        assert_eq!(stored, order);

        let foreign = ServiceContext::system(
            "foreign",
            &Timestamp::from_unix_seconds(1_700_000_000).expect("ts"),
        )
        .expect("ctx")
        .with_scope(TenantId::new(), OrganizationId::new());
        assert!(
            fixture.service.get(&foreign, workflow.id).await.is_err(),
            "existence hidden"
        );
    }

    #[tokio::test]
    async fn cycles_and_dangling_edges_reject_clearly() {
        let fixture = fixture().await;
        let ctx = &fixture.tenant_ctx;
        let workflow = fixture
            .service
            .create(ctx, fixture.project, "loop-flow")
            .await
            .expect("create");

        assert!(
            fixture
                .service
                .update_graph(
                    ctx,
                    workflow.id,
                    vec![
                        node("start", WorkflowNodeType::Start),
                        node("a", WorkflowNodeType::Agent),
                        node("b", WorkflowNodeType::Agent),
                    ],
                    vec![
                        WorkflowEdge::new("a", "b").expect("edge"),
                        WorkflowEdge::new("b", "a").expect("edge"),
                    ],
                )
                .await
                .is_err(),
            "2-cycles must reject with a validation error"
        );

        assert!(
            fixture
                .service
                .update_graph(
                    ctx,
                    workflow.id,
                    vec![node("start", WorkflowNodeType::Start)],
                    vec![WorkflowEdge::new("start", "ghost").expect("edge")],
                )
                .await
                .is_err(),
            "edges must resolve to declared nodes"
        );
        let stored = fixture.service.get(ctx, workflow.id).await.expect("get");
        assert!(stored.graph.is_empty(), "failed updates persist nothing");
    }
}
