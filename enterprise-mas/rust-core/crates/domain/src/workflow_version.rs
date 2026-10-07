//! PURPOSE: Immutable workflow publish snapshots.
//!
//! A version captures the validated graph, deterministic topological order,
//! checksum, actor and rollback provenance. After construction its fields are
//! private and there are no mutators; a rollback is a new version, never an
//! edit to historical bytes.

use mas_common::error::AppError;
use mas_common::ids::{TenantId, UserId, WorkflowId, WorkflowVersionId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::workflow::WorkflowGraph;

/// One immutable published workflow graph snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowVersion {
    id: WorkflowVersionId,
    tenant_id: TenantId,
    workflow_id: WorkflowId,
    version_number: u64,
    graph: WorkflowGraph,
    topo_order: Vec<String>,
    checksum: String,
    created_by: Option<UserId>,
    created_at: Timestamp,
    rollback_of: Option<WorkflowVersionId>,
}

impl WorkflowVersion {
    /// Creates a new immutable version from a validated workflow graph.
    pub fn publish(
        tenant_id: TenantId,
        workflow_id: WorkflowId,
        version_number: u64,
        graph: WorkflowGraph,
        topo_order: Vec<String>,
        created_by: Option<UserId>,
        rollback_of: Option<WorkflowVersionId>,
    ) -> Result<Self> {
        Self::validate_identity(tenant_id, workflow_id, version_number)?;
        Self::validate_graph_and_order(&graph, &topo_order)?;
        let checksum = checksum_for(&graph, &topo_order)?;
        Ok(Self {
            id: WorkflowVersionId::new(),
            tenant_id,
            workflow_id,
            version_number,
            graph,
            topo_order,
            checksum,
            created_by,
            created_at: Timestamp::now(),
            rollback_of,
        })
    }

    /// Rehydrates an immutable version and rechecks the graph, ordering and
    /// checksum. A mismatched persisted checksum is treated as corrupt storage.
    #[allow(clippy::too_many_arguments)]
    pub fn rehydrate(
        id: WorkflowVersionId,
        tenant_id: TenantId,
        workflow_id: WorkflowId,
        version_number: u64,
        graph: WorkflowGraph,
        topo_order: Vec<String>,
        checksum: impl Into<String>,
        created_by: Option<UserId>,
        created_at: Timestamp,
        rollback_of: Option<WorkflowVersionId>,
    ) -> Result<Self> {
        Self::validate_identity(tenant_id, workflow_id, version_number)?;
        Self::validate_graph_and_order(&graph, &topo_order)?;
        let checksum = checksum.into();
        validate_checksum(&checksum)?;
        let expected = checksum_for(&graph, &topo_order)?;
        if checksum != expected {
            return Err(AppError::database(
                "workflow version checksum does not match its immutable snapshot",
            ));
        }
        if id.is_nil() {
            return Err(AppError::database("workflow version has a nil identifier"));
        }
        Ok(Self {
            id,
            tenant_id,
            workflow_id,
            version_number,
            graph,
            topo_order,
            checksum,
            created_by,
            created_at,
            rollback_of,
        })
    }

    fn validate_identity(
        tenant_id: TenantId,
        workflow_id: WorkflowId,
        version_number: u64,
    ) -> Result<()> {
        if tenant_id.is_nil() || workflow_id.is_nil() {
            return Err(AppError::invalid_field(
                "workflow_version",
                "required",
                "tenant and workflow identifiers must be concrete",
            ));
        }
        if version_number == 0 || version_number > i32::MAX as u64 {
            return Err(AppError::invalid_field(
                "version_number",
                "out_of_range",
                "workflow version must fit a positive PostgreSQL integer",
            ));
        }
        Ok(())
    }

    fn validate_graph_and_order(graph: &WorkflowGraph, order: &[String]) -> Result<()> {
        graph.validate()?;
        if graph.has_cycle() {
            return Err(AppError::invalid_field(
                "graph",
                "cycle",
                "published workflow graph must be acyclic",
            ));
        }
        if order.len() != graph.nodes.len() {
            return Err(AppError::invalid_field(
                "topo_order",
                "incomplete_order",
                "topological order must contain every graph node exactly once",
            ));
        }
        let positions: BTreeMap<&str, usize> = order
            .iter()
            .enumerate()
            .map(|(index, key)| (key.as_str(), index))
            .collect();
        if positions.len() != order.len() {
            return Err(AppError::invalid_field(
                "topo_order",
                "duplicate_node",
                "topological order contains duplicate node keys",
            ));
        }
        let graph_keys: BTreeSet<&str> = graph
            .nodes
            .iter()
            .map(|node| node.node_key.as_str())
            .collect();
        let order_keys: BTreeSet<&str> = positions.keys().copied().collect();
        if graph_keys != order_keys {
            return Err(AppError::invalid_field(
                "topo_order",
                "unknown_node",
                "topological order must contain exactly the graph node keys",
            ));
        }
        for edge in &graph.edges {
            let from = positions.get(edge.from.as_str()).copied().ok_or_else(|| {
                AppError::invalid_field("topo_order", "unknown_node", "edge source is absent")
            })?;
            let to = positions.get(edge.to.as_str()).copied().ok_or_else(|| {
                AppError::invalid_field("topo_order", "unknown_node", "edge target is absent")
            })?;
            if from >= to {
                return Err(AppError::invalid_field(
                    "topo_order",
                    "not_topological",
                    "every edge source must precede its target",
                ));
            }
        }
        Ok(())
    }

    /// Stable SHA-256 checksum of the graph and its topological order.
    pub fn checksum_for(graph: &WorkflowGraph, topo_order: &[String]) -> Result<String> {
        checksum_for(graph, topo_order)
    }

    /// Whether a supplied checksum matches this version's canonical snapshot.
    #[must_use]
    pub fn checksum_matches(&self, candidate: &str) -> bool {
        self.checksum == candidate
    }

    /// Reconstitutes the immutable workflow graph for rollback.
    #[must_use]
    pub fn graph(&self) -> &WorkflowGraph {
        &self.graph
    }

    /// Reconstitutes the deterministic order used by the runtime.
    #[must_use]
    pub fn topo_order(&self) -> &[String] {
        &self.topo_order
    }

    #[must_use]
    pub const fn id(&self) -> WorkflowVersionId {
        self.id
    }

    #[must_use]
    pub const fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    #[must_use]
    pub const fn workflow_id(&self) -> WorkflowId {
        self.workflow_id
    }

    #[must_use]
    pub const fn version_number(&self) -> u64 {
        self.version_number
    }

    #[must_use]
    pub fn checksum(&self) -> &str {
        &self.checksum
    }

    #[must_use]
    pub const fn created_by(&self) -> Option<UserId> {
        self.created_by
    }

    #[must_use]
    pub const fn created_at(&self) -> Timestamp {
        self.created_at
    }

    #[must_use]
    pub const fn rollback_of(&self) -> Option<WorkflowVersionId> {
        self.rollback_of
    }
}

fn checksum_for(graph: &WorkflowGraph, topo_order: &[String]) -> Result<String> {
    let canonical = serde_json::to_vec(&serde_json::json!({
        "graph": graph,
        "topo_order": topo_order,
    }))
    .map_err(|error| AppError::serialization(format!("workflow version snapshot: {error}")))?;
    let digest = Sha256::digest(canonical);
    Ok(format!("{digest:x}"))
}

fn validate_checksum(checksum: &str) -> Result<()> {
    validation::validate_length("checksum", checksum, 64, 64)?;
    if checksum.len() != 64
        || !checksum
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AppError::invalid_field(
            "checksum",
            "invalid_format",
            "checksum must be lowercase SHA-256 hexadecimal",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WorkflowEdge, WorkflowNode, WorkflowNodeType};

    fn graph() -> WorkflowGraph {
        WorkflowGraph::new(
            vec![
                WorkflowNode::new("start", WorkflowNodeType::Start, "Start").expect("start"),
                WorkflowNode::new("finish", WorkflowNodeType::End, "Finish").expect("finish"),
            ],
            vec![WorkflowEdge::new("start", "finish").expect("edge")],
        )
    }

    #[test]
    fn immutable_version_computes_and_revalidates_checksum() {
        let tenant = TenantId::new();
        let workflow = WorkflowId::new();
        let graph = graph();
        let order = vec!["start".to_owned(), "finish".to_owned()];
        let version = WorkflowVersion::publish(
            tenant,
            workflow,
            1,
            graph.clone(),
            order.clone(),
            Some(UserId::new()),
            None,
        )
        .expect("version");
        assert_eq!(version.version_number(), 1);
        assert_eq!(version.graph(), &graph);
        assert_eq!(version.topo_order(), order);
        assert!(version.checksum_matches(version.checksum()));
        assert_eq!(version.checksum().len(), 64);
    }

    #[test]
    fn publish_rejects_an_order_that_violates_an_edge() {
        let error = WorkflowVersion::publish(
            TenantId::new(),
            WorkflowId::new(),
            1,
            graph(),
            vec!["finish".to_owned(), "start".to_owned()],
            None,
            None,
        )
        .expect_err("reverse order is not topological");
        assert!(matches!(error, AppError::Validation { .. }));
    }

    #[test]
    fn rehydrate_rejects_checksum_drift() {
        let tenant = TenantId::new();
        let workflow = WorkflowId::new();
        let graph = graph();
        let order = vec!["start".to_owned(), "finish".to_owned()];
        let error = WorkflowVersion::rehydrate(
            WorkflowVersionId::new(),
            tenant,
            workflow,
            1,
            graph,
            order,
            "0".repeat(64),
            None,
            Timestamp::now(),
            None,
        )
        .expect_err("checksum mismatch is corrupt storage");
        assert!(matches!(error, AppError::Database(_)));
    }
}
