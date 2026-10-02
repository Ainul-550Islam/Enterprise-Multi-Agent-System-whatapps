//! Workflow aggregate and its embedded graph.
//!
//! Lifecycle: `Draft → Validating → Published → Disabled → Archived`.
//! A workflow is only executable while `Published`.

use mas_common::constants;
use mas_common::enums::WorkflowStatus;
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::workflow_edge::WorkflowEdge;
use crate::workflow_node::{WorkflowNode, WorkflowNodeType};

/// A validated-or-validatable workflow graph (nodes + edges).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowGraph {
    #[serde(default)]
    pub nodes: Vec<WorkflowNode>,
    #[serde(default)]
    pub edges: Vec<WorkflowEdge>,
}

impl WorkflowGraph {
    #[must_use]
    pub fn new(nodes: Vec<WorkflowNode>, edges: Vec<WorkflowEdge>) -> Self {
        Self { nodes, edges }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Full static validation of the graph:
    /// node/edge structure, unique keys, endpoint existence, single start,
    /// at least one end, acyclicity, reachability from start, incoming/
    /// outgoing rules per node type.
    pub fn validate(&self) -> Result<()> {
        if self.nodes.len() > constants::MAX_NODES_PER_WORKFLOW {
            return Err(AppError::invalid_field(
                "nodes",
                "too_many",
                format!(
                    "workflows are limited to {} nodes",
                    constants::MAX_NODES_PER_WORKFLOW
                ),
            ));
        }
        if self.nodes.is_empty() {
            return Err(AppError::invalid_field(
                "nodes",
                "required",
                "a workflow graph needs at least a start and an end node",
            ));
        }

        // 1. Per-node + per-edge structural validation and key uniqueness.
        let mut keys = BTreeSet::new();
        for node in &self.nodes {
            node.validate()?;
            if !keys.insert(node.node_key.as_str()) {
                return Err(AppError::invalid_field(
                    "nodes",
                    "duplicate_key",
                    format!("duplicate node key '{}'", node.node_key),
                ));
            }
        }
        let by_key: BTreeMap<&str, &WorkflowNode> = self
            .nodes
            .iter()
            .map(|n| (n.node_key.as_str(), n))
            .collect();
        let mut seen_edges = HashSet::new();
        for edge in &self.edges {
            edge.validate()?;
            for endpoint in [&edge.from, &edge.to] {
                if !by_key.contains_key(endpoint.as_str()) {
                    return Err(AppError::invalid_field(
                        "edges",
                        "unknown_node",
                        format!("edge references unknown node '{endpoint}'"),
                    ));
                }
            }
            if !seen_edges.insert((edge.from.as_str(), edge.to.as_str())) {
                return Err(AppError::invalid_field(
                    "edges",
                    "duplicate_edge",
                    format!("duplicate edge {} → {}", edge.from, edge.to),
                ));
            }
        }

        // 2. Start/end rules.
        let start_count = self
            .nodes
            .iter()
            .filter(|n| n.node_type == WorkflowNodeType::Start)
            .count();
        let end_count = self
            .nodes
            .iter()
            .filter(|n| n.node_type == WorkflowNodeType::End)
            .count();
        if start_count != 1 {
            return Err(AppError::invalid_field(
                "nodes",
                "invalid_start_count",
                format!("a workflow needs exactly one start node, found {start_count}"),
            ));
        }
        if end_count == 0 {
            return Err(AppError::invalid_field(
                "nodes",
                "missing_end",
                "a workflow needs at least one end node",
            ));
        }

        // 3. Incoming/outgoing rules per node type.
        let mut incoming: HashMap<&str, usize> = HashMap::new();
        let mut outgoing: HashMap<&str, usize> = HashMap::new();
        for edge in &self.edges {
            *incoming.entry(edge.to.as_str()).or_insert(0) += 1;
            *outgoing.entry(edge.from.as_str()).or_insert(0) += 1;
        }
        for node in &self.nodes {
            let key = node.node_key.as_str();
            let inc = incoming.get(key).copied().unwrap_or(0);
            let out = outgoing.get(key).copied().unwrap_or(0);
            if !node.node_type.has_incoming() && inc > 0 {
                return Err(AppError::invalid_field(
                    "edges",
                    "invalid_start_edge",
                    format!("start node '{key}' must not have incoming edges"),
                ));
            }
            if !node.node_type.has_outgoing() && out > 0 {
                return Err(AppError::invalid_field(
                    "edges",
                    "invalid_end_edge",
                    format!("end node '{key}' must not have outgoing edges"),
                ));
            }
        }

        // 4. Acyclicity.
        if self.has_cycle() {
            return Err(AppError::invalid_field(
                "edges",
                "cycle_detected",
                "workflow graphs must be acyclic (DAG)",
            ));
        }

        // 5. Reachability: every node reachable from the single start node.
        let start = self
            .nodes
            .iter()
            .find(|n| n.node_type == WorkflowNodeType::Start)
            .expect("start existence checked above");
        let mut reachable = HashSet::new();
        let mut queue = VecDeque::from([start.node_key.as_str()]);
        reachable.insert(start.node_key.as_str());
        let children = self.adjacency();
        while let Some(key) = queue.pop_front() {
            if let Some(next) = children.get(key) {
                for child in next {
                    if reachable.insert(child) {
                        queue.push_back(child);
                    }
                }
            }
        }
        for node in &self.nodes {
            if !reachable.contains(node.node_key.as_str()) {
                return Err(AppError::invalid_field(
                    "nodes",
                    "orphan_node",
                    format!(
                        "node '{}' is not reachable from the start node",
                        node.node_key
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Adjacency map: node key → outgoing node keys (sorted for determinism).
    #[must_use]
    pub fn adjacency(&self) -> HashMap<&str, Vec<&str>> {
        let mut map: HashMap<&str, Vec<&str>> = HashMap::new();
        for edge in &self.edges {
            map.entry(edge.from.as_str())
                .or_default()
                .push(edge.to.as_str());
        }
        for children in map.values_mut() {
            children.sort_unstable();
        }
        map
    }

    /// Direct children of `node_key`, ordered by (priority, key).
    #[must_use]
    pub fn children_of(&self, node_key: &str) -> Vec<&str> {
        let mut edges: Vec<&WorkflowEdge> =
            self.edges.iter().filter(|e| e.from == node_key).collect();
        edges.sort_by(|a, b| (a.priority, &a.to).cmp(&(b.priority, &b.to)));
        edges.into_iter().map(|e| e.to.as_str()).collect()
    }

    /// Direct parents of `node_key` (sorted for determinism).
    #[must_use]
    pub fn parents_of(&self, node_key: &str) -> Vec<&str> {
        let mut parents: Vec<&str> = self
            .edges
            .iter()
            .filter(|e| e.to == node_key)
            .map(|e| e.from.as_str())
            .collect();
        parents.sort_unstable();
        parents
    }

    /// In-degree of every node (used by dependency resolution / Kahn's).
    #[must_use]
    pub fn in_degrees(&self) -> HashMap<&str, usize> {
        let mut degrees: HashMap<&str, usize> = self
            .nodes
            .iter()
            .map(|n| (n.node_key.as_str(), 0))
            .collect();
        for edge in &self.edges {
            *degrees.entry(edge.to.as_str()).or_insert(0) += 1;
        }
        degrees
    }

    /// Iterative three-color DFS cycle check.
    #[must_use]
    pub fn has_cycle(&self) -> bool {
        #[derive(Clone, Copy, PartialEq)]
        enum Color {
            White,
            Gray,
            Black,
        }
        let adjacency = self.adjacency();
        let mut color: HashMap<&str, Color> = self
            .nodes
            .iter()
            .map(|n| (n.node_key.as_str(), Color::White))
            .collect();

        for node in &self.nodes {
            let root = node.node_key.as_str();
            if color[root] != Color::White {
                continue;
            }
            // Explicit stack for iterative DFS with backtracking.
            let mut stack: Vec<(&str, usize)> = vec![(root, 0)];
            color.insert(root, Color::Gray);
            while let Some((current, child_index)) = stack.last().copied() {
                let children: &[&str] = adjacency.get(current).map(Vec::as_slice).unwrap_or(&[]);
                if child_index < children.len() {
                    let child = children[child_index];
                    stack.last_mut().expect("non-empty").1 += 1;
                    match color.get(child).copied().unwrap_or(Color::White) {
                        Color::Gray => return true, // back-edge ⇒ cycle
                        Color::White => {
                            color.insert(child, Color::Gray);
                            stack.push((child, 0));
                        },
                        Color::Black => {},
                    }
                } else {
                    color.insert(current, Color::Black);
                    stack.pop();
                }
            }
        }
        false
    }
}

/// The workflow aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: WorkflowId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub status: WorkflowStatus,
    /// Editable definition. Published content is captured immutably by the
    /// persistence layer as `workflow_versions` rows; this stays the draft.
    pub graph: WorkflowGraph,
    /// Monotonic publish counter; `None` until first publish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_version: Option<u64>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Workflow {
    /// Creates a workflow in `Draft` with an empty graph.
    pub fn create(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        name: impl Into<String>,
    ) -> Result<Self> {
        let name = name.into();
        validation::validate_resource_name("name", &name)?;
        let now = Timestamp::now();
        Ok(Self {
            id: WorkflowId::new(),
            tenant_id,
            organization_id,
            project_id,
            name,
            description: None,
            status: WorkflowStatus::Draft,
            graph: WorkflowGraph::default(),
            published_version: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Replaces the draft graph. Only allowed in `Draft` (editing a published
    /// workflow requires going back to draft via new version flow).
    pub fn update_graph(&mut self, graph: WorkflowGraph) -> Result<()> {
        match self.status {
            WorkflowStatus::Draft => {
                self.graph = graph;
                self.touch();
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "workflow in status '{other}' is not editable; only drafts accept graph updates"
            ))),
        }
    }

    /// Static graph validation (structure, DAG, reachability).
    pub fn validate_graph(&self) -> Result<()> {
        self.graph.validate()
    }

    /// Draft → Validating.
    pub fn begin_validation(&mut self) -> Result<()> {
        match self.status {
            WorkflowStatus::Draft => {
                self.status = WorkflowStatus::Validating;
                self.touch();
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "only drafts can enter validation, not '{other}'"
            ))),
        }
    }

    /// Validating → Published: validates the graph and bumps the publish
    /// counter. Returns the new version number.
    pub fn publish(&mut self) -> Result<u64> {
        match self.status {
            WorkflowStatus::Draft | WorkflowStatus::Validating | WorkflowStatus::Disabled => {},
            other => {
                return Err(AppError::conflict(format!(
                    "workflow in status '{other}' cannot be published"
                )));
            },
        }
        self.validate_graph()?;
        let version = self.published_version.unwrap_or(0) + 1;
        self.published_version = Some(version);
        self.status = WorkflowStatus::Published;
        self.touch();
        Ok(version)
    }

    /// Published → Disabled.
    pub fn disable(&mut self) -> Result<()> {
        match self.status {
            WorkflowStatus::Published => {
                self.status = WorkflowStatus::Disabled;
                self.touch();
                Ok(())
            },
            WorkflowStatus::Disabled => Ok(()),
            other => Err(AppError::conflict(format!(
                "workflow in status '{other}' cannot be disabled"
            ))),
        }
    }

    /// Disabled|Published → Archived (terminal).
    pub fn archive(&mut self) -> Result<()> {
        match self.status {
            WorkflowStatus::Archived => Ok(()),
            WorkflowStatus::Disabled | WorkflowStatus::Published => {
                self.status = WorkflowStatus::Archived;
                self.touch();
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "workflow in status '{other}' must be published/disabled before archiving"
            ))),
        }
    }

    /// Whether new executions may be started from this workflow.
    #[must_use]
    pub fn is_executable(&self) -> bool {
        self.status == WorkflowStatus::Published && self.published_version.is_some()
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
