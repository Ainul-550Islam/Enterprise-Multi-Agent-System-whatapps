//! Workflow routes.

use axum::extract::{Path, Query, State};
use axum::Json;
use mas_common::ids::WorkflowId;
use mas_contracts::workflow::{CreateWorkflowRequest, WorkflowGraphDto};
use mas_domain::{WorkflowEdge, WorkflowNode, WorkflowNodeType};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::handlers::Ctx;
use crate::response::{created, ApiResult, IntoApiResult};
use crate::state::AppState;

/// Compact workflow summary on the wire.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowResponse {
    /// Id.
    pub id: String,
    /// Project id.
    pub project_id: String,
    /// Name.
    pub name: String,
    /// Status (`draft` / `published` / …).
    pub status: String,
    /// Published graph version (None = draft).
    pub published_graph_version: Option<u64>,
    /// Node count of the stored graph.
    pub node_count: usize,
}

fn workflow_response(workflow: &mas_domain::Workflow) -> WorkflowResponse {
    WorkflowResponse {
        id: workflow.id.to_string(),
        project_id: workflow.project_id.to_string(),
        name: workflow.name.clone(),
        status: workflow.status.to_string(),
        published_graph_version: workflow.published_version,
        node_count: workflow.graph.nodes.len(),
    }
}

/// `POST /v1/workflows`.
pub async fn create(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<CreateWorkflowRequest>,
) -> ApiResult<WorkflowResponse> {
    scoped(&ctx)?;
    body.validate()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .workflows
        .create(&service_ctx, body.project_id, &body.name)
        .await
        .map(|workflow| created(&ctx, workflow_response(&workflow)))
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `PUT /v1/workflows/{workflow_id}/graph` — validates structurally,
/// rejects cycles, persists, and returns the deterministic topological
/// order the runtime will execute.
pub async fn update_graph(
    State(state): State<AppState>,
    Path(workflow_id): Path<WorkflowId>,
    Ctx(ctx): Ctx,
    Json(body): Json<WorkflowGraphDto>,
) -> ApiResult<WorkflowGraphUpdateResponse> {
    scoped(&ctx)?;
    let (nodes, edges) =
        dto_to_graph(&body).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let order = state
        .workflows
        .update_graph(&service_ctx, workflow_id, nodes, edges)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    Ok(crate::response::ok(
        &ctx,
        WorkflowGraphUpdateResponse {
            workflow_id,
            topological_order: order,
        },
    ))
}

/// Graph-update acknowledgement.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowGraphUpdateResponse {
    /// Updated workflow id.
    pub workflow_id: WorkflowId,
    /// Deterministic execution order of node keys.
    pub topological_order: Vec<String>,
}

/// `GET /v1/workflows/{workflow_id}`.
pub async fn get(
    State(state): State<AppState>,
    Path(workflow_id): Path<WorkflowId>,
    Ctx(ctx): Ctx,
) -> ApiResult<WorkflowResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .workflows
        .get(&service_ctx, workflow_id)
        .await
        .map(|workflow| workflow_response(&workflow))
        .into_api(&ctx)
}

/// `GET /v1/workflows?project_id=…`.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Project within scope.
    pub project_id: mas_common::ids::ProjectId,
}

/// `GET /v1/workflows`.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<WorkflowResponse>> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .workflows
        .list(&service_ctx, query.project_id)
        .await
        .map(|workflows| workflows.iter().map(workflow_response).collect::<Vec<_>>())
        .into_api(&ctx)
}

/// Contract DTO → domain graph primitives. Structural `node_type` strings
/// decode through serde (the string enum drives helpful errors).
pub fn dto_to_graph(
    dto: &WorkflowGraphDto,
) -> mas_common::result::Result<(Vec<WorkflowNode>, Vec<WorkflowEdge>)> {
    let mut seen = BTreeSet::new();
    let mut nodes = Vec::with_capacity(dto.nodes.len());
    for node in &dto.nodes {
        if !seen.insert(node.node_key.clone()) {
            return Err(mas_common::error::AppError::invalid_field(
                "nodes",
                "duplicate",
                format!("duplicate node_key '{}'", node.node_key),
            ));
        }
        let node_type = serde_json::from_value::<WorkflowNodeType>(
            serde_json::Value::String(node.node_type.clone()),
        )
        .map_err(|_| {
            mas_common::error::AppError::invalid_field(
                "nodes[].node_type",
                "invalid",
                "expected start | agent | tool | condition | parallel | join | delay | transform | approval | end",
            )
        })?;
        let mut built = WorkflowNode::new(&node.node_key, node_type, &node.name)?;
        built.config = node.config.clone();
        // Timeout/retry hints ride inside `config` (the domain node owns no
        // dedicated columns for them; workers read them from the graph JSON).
        if let Some(timeout_ms) = node.timeout_ms {
            built
                .config
                .insert("timeout_ms".to_owned(), serde_json::Value::from(timeout_ms));
        }
        if let Some(max_retries) = node.max_retries {
            built.config.insert(
                "max_retries".to_owned(),
                serde_json::Value::from(max_retries),
            );
        }
        nodes.push(built);
    }
    let mut edges = Vec::with_capacity(dto.edges.len());
    for edge in &dto.edges {
        edges.push(WorkflowEdge::new(&edge.from, &edge.to)?);
    }
    Ok((nodes, edges))
}

fn scoped(
    ctx: &crate::context::RequestContext,
) -> std::result::Result<(), crate::response::ApiError> {
    ctx.require_scope()
        .map(|_| ())
        .map_err(|e| crate::response::ApiError::new(e, ctx))
}
