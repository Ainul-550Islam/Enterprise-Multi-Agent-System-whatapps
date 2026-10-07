//! Workflow lifecycle DTOs.

use mas_common::ids::{ProjectId, UserId, WorkflowId, WorkflowVersionId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Node DTO (wire shape of `WorkflowNode`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowNodeDto {
    pub node_key: String,
    /// `start` | `agent` | `tool` | `condition` | `parallel` | `join` |
    /// `delay` | `transform` | `approval` | `end`
    pub node_type: String,
    pub name: String,
    #[serde(default)]
    pub config: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
}

/// Edge DTO (wire shape of `WorkflowEdge`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowEdgeDto {
    pub from: String,
    pub to: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<String>,
    #[serde(default)]
    pub priority: u32,
}

/// Graph DTO with structural (not full DAG) validation; the deep validation
/// (cycles, reachability) happens in the domain when composing the graph.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowGraphDto {
    #[serde(default)]
    pub nodes: Vec<WorkflowNodeDto>,
    #[serde(default)]
    pub edges: Vec<WorkflowEdgeDto>,
}

impl WorkflowGraphDto {
    pub fn validate(&self) -> Result<()> {
        if self.nodes.len() > mas_common::constants::MAX_NODES_PER_WORKFLOW {
            return Err(mas_common::error::AppError::invalid_field(
                "nodes",
                "too_many",
                "graph exceeds the node limit",
            ));
        }
        let mut keys = std::collections::HashSet::new();
        for node in &self.nodes {
            if !keys.insert(node.node_key.as_str()) {
                return Err(mas_common::error::AppError::invalid_field(
                    "nodes",
                    "duplicate_key",
                    format!("duplicate node key '{}'", node.node_key),
                ));
            }
            match node.node_type.as_str() {
                "start" | "agent" | "tool" | "condition" | "parallel" | "join" | "delay"
                | "transform" | "approval" | "end" => {},
                other => {
                    return Err(mas_common::error::AppError::invalid_field(
                        "node_type",
                        "invalid_enum_value",
                        format!("unknown node type '{other}'"),
                    ));
                },
            }
        }
        for edge in &self.edges {
            for endpoint in [&edge.from, &edge.to] {
                if !keys.contains(endpoint.as_str()) {
                    return Err(mas_common::error::AppError::invalid_field(
                        "edges",
                        "unknown_node",
                        format!("edge references unknown node '{endpoint}'"),
                    ));
                }
            }
            if edge.from == edge.to {
                return Err(mas_common::error::AppError::invalid_field(
                    "edges",
                    "self_loop",
                    "self-loops are not allowed",
                ));
            }
        }
        Ok(())
    }
}

/// Request to create a workflow (optionally with an initial graph draft).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWorkflowRequest {
    pub project_id: ProjectId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<WorkflowGraphDto>,
}

impl CreateWorkflowRequest {
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        if let Some(description) = &self.description {
            validation::validate_length(
                "description",
                description,
                0,
                mas_common::constants::MAX_DESCRIPTION_LENGTH,
            )?;
        }
        if let Some(graph) = &self.graph {
            graph.validate()?;
        }
        Ok(())
    }
}

/// Request to update draft metadata/graph.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UpdateWorkflowRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<WorkflowGraphDto>,
}

impl UpdateWorkflowRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(name) = &self.name {
            validation::validate_resource_name("name", name)?;
        }
        if let Some(graph) = &self.graph {
            graph.validate()?;
        }
        if self.name.is_none() && self.description.is_none() && self.graph.is_none() {
            return Err(mas_common::error::AppError::invalid_field(
                "update",
                "empty_patch",
                "update requests must change at least one field",
            ));
        }
        Ok(())
    }
}

/// Request to publish the current draft graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishWorkflowRequest {
    pub workflow_id: WorkflowId,
}

impl PublishWorkflowRequest {
    pub fn validate(&self) -> Result<()> {
        if self.workflow_id.is_nil() {
            return Err(mas_common::error::AppError::invalid_field(
                "workflow_id",
                "required",
                "workflow id must be concrete",
            ));
        }
        Ok(())
    }
}

/// Wire representation of a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowResponse {
    pub id: WorkflowId,
    pub project_id: ProjectId,
    pub tenant_id: mas_common::ids::TenantId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// `draft` | `published` | `disabled` | `archived`
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<WorkflowGraphDto>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Immutable published graph snapshot returned by version-history endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowVersionDto {
    /// Stable snapshot id.
    pub id: WorkflowVersionId,
    /// Owning workflow.
    pub workflow_id: WorkflowId,
    /// Tenant scope.
    pub tenant_id: mas_common::ids::TenantId,
    /// Monotonic per-workflow version number.
    pub version_number: u64,
    /// SHA-256 of the canonical graph and topological order.
    pub checksum: String,
    /// Immutable published graph.
    pub graph: WorkflowGraphDto,
    /// Deterministic node execution order.
    pub topo_order: Vec<String>,
    /// Authenticated user who published, when the caller is a user.
    pub created_by: Option<UserId>,
    /// Publication instant.
    pub created_at: Timestamp,
    /// Source version if this snapshot was created by rollback.
    pub rollback_of: Option<WorkflowVersionId>,
}
