//! DAG dependency resolution for workflow graphs.
//!
//! Pure computation over a [`WorkflowGraph`] plus per-node runtime states.
//! No I/O, no async: the workflow runtime calls this before/after every node
//! transition. Edge *conditions* are runtime-evaluated elsewhere; resolution
//! here is purely "have all my parents reached a state that releases me?".
//!
//! Release semantics:
//! * every parent `Completed`                 → [`NodeReadiness::Ready`]
//! * parent still pending/running/deferred    → [`NodeReadiness::Waiting`]
//! * a parent `Failed`/`Cancelled`, the rest terminal → [`NodeReadiness::UpstreamFailed`]
//! * every parent terminal & all `Skipped`    → [`NodeReadiness::SkipAll`] (cascade)
//! * mix of `Completed` + `Skipped`           → [`NodeReadiness::Ready`]
//!   (skipped optional branches never block a join point)

use crate::checkpoint::NodeExecutionState;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_domain::WorkflowGraph;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

/// Readiness verdict for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeReadiness {
    /// All parents completed (or completed+skipped); the node may run now.
    Ready,
    /// At least one parent is not terminal yet.
    Waiting { pending_parents: Vec<String> },
    /// A parent failed or was cancelled and no parent is still in flight —
    /// this node can never become ready. The runtime decides whether to fail
    /// the execution or skip the node per its own configuration.
    UpstreamFailed { failed_parents: Vec<String> },
    /// Every parent finished as `Skipped` — the skip cascades down.
    SkipAll,
}

impl NodeReadiness {
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }
}

/// Resolver over one validated workflow graph.
///
/// Cheap to construct (indexes edges once) and intended to be rebuilt cheaply
/// rather than persisted.
#[derive(Debug, Clone)]
pub struct DependencyResolver<'graph> {
    graph: &'graph WorkflowGraph,
    /// child → parents (deduped, every edge contributes one parent entry).
    parents: HashMap<&'graph str, Vec<&'graph str>>,
    /// parent → children.
    children: HashMap<&'graph str, Vec<&'graph str>>,
    /// in-degree per node key (duplicate edges collapse).
    in_degree: HashMap<&'graph str, usize>,
    /// every node key, ordered for deterministic output.
    node_keys: Vec<&'graph str>,
    /// nodes with no incoming edges (the Start node, normally exactly one).
    entry_points: Vec<&'graph str>,
}

impl<'graph> DependencyResolver<'graph> {
    /// Builds a resolver; fails when the graph is empty, unknown-keyd or cyclic.
    pub fn new(graph: &'graph WorkflowGraph) -> Result<Self> {
        if graph.nodes.is_empty() {
            return Err(AppError::validation("workflow graph has no nodes"));
        }
        let node_keys: Vec<&str> = graph.nodes.iter().map(|n| n.node_key.as_str()).collect();
        let key_set: BTreeSet<&str> = node_keys.iter().copied().collect();
        if key_set.len() != node_keys.len() {
            return Err(AppError::validation(
                "workflow graph contains duplicate node keys",
            ));
        }

        let mut parents: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut children: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut in_degree: HashMap<&str, usize> = node_keys.iter().map(|k| (*k, 0usize)).collect();

        for edge in &graph.edges {
            let from = edge.from.as_str();
            let to = edge.to.as_str();
            if !key_set.contains(from) {
                return Err(AppError::validation(format!(
                    "edge starts at unknown node '{from}'"
                )));
            }
            if !key_set.contains(to) {
                return Err(AppError::validation(format!(
                    "edge ends at unknown node '{to}'"
                )));
            }
            let parents_of_to = parents.entry(to).or_default();
            if !parents_of_to.contains(&from) {
                parents_of_to.push(from);
                *in_degree.get_mut(to).expect("node key present") += 1;
            }
            let children_of_from = children.entry(from).or_default();
            if !children_of_from.contains(&to) {
                children_of_from.push(to);
            }
        }

        let entry_points: Vec<&str> = node_keys
            .iter()
            .copied()
            .filter(|key| parents.get(key).is_none_or(|p| p.is_empty()))
            .collect();

        let resolver = Self {
            graph,
            parents,
            children,
            in_degree,
            node_keys,
            entry_points,
        };
        // Defensive: construction-time cycle check even when domain validation
        // already ran — resolvers are rebuilt from stored graphs on recovery.
        resolver.topological_order()?;
        Ok(resolver)
    }

    #[must_use]
    pub fn graph(&self) -> &'graph WorkflowGraph {
        self.graph
    }

    /// Nodes with no parents (workflow entry points).
    #[must_use]
    pub fn entry_points(&self) -> Vec<String> {
        self.entry_points.iter().map(|k| (*k).to_owned()).collect()
    }

    /// Whether `node_key` exists in the graph.
    #[must_use]
    pub fn contains(&self, node_key: &str) -> bool {
        self.node_keys.contains(&node_key)
    }

    /// Parents of a node (empty for entry points; errors on unknown key).
    pub fn parents_of(&self, node_key: &str) -> Result<Vec<String>> {
        self.assert_node(node_key)?;
        Ok(self
            .parents
            .get(node_key)
            .map(|ps| ps.iter().map(|p| (*p).to_owned()).collect())
            .unwrap_or_default())
    }

    /// Children of a node (errors on unknown key).
    pub fn children_of(&self, node_key: &str) -> Result<Vec<String>> {
        self.assert_node(node_key)?;
        Ok(self
            .children
            .get(node_key)
            .map(|cs| cs.iter().map(|c| (*c).to_owned()).collect())
            .unwrap_or_default())
    }

    /// Readiness of a single node given current runtime states.
    ///
    /// Nodes absent from `states` are treated as `Pending`.
    pub fn readiness(
        &self,
        node_key: &str,
        states: &BTreeMap<String, NodeExecutionState>,
    ) -> Result<NodeReadiness> {
        self.assert_node(node_key)?;
        let parents = self.parents.get(node_key);
        let Some(parents) = parents.filter(|ps| !ps.is_empty()) else {
            return Ok(NodeReadiness::Ready);
        };

        let mut pending = Vec::new();
        let mut failed = Vec::new();
        let mut completed = 0usize;
        let mut skipped = 0usize;
        for parent in parents {
            let state = states
                .get(*parent)
                .copied()
                .unwrap_or(NodeExecutionState::Pending);
            match state {
                NodeExecutionState::Completed => completed += 1,
                NodeExecutionState::Skipped => skipped += 1,
                NodeExecutionState::Failed | NodeExecutionState::Cancelled => {
                    failed.push((*parent).to_owned());
                },
                NodeExecutionState::Pending
                | NodeExecutionState::Running
                | NodeExecutionState::WaitingApproval
                | NodeExecutionState::Deferred => pending.push((*parent).to_owned()),
            }
        }

        if !pending.is_empty() {
            // Even with failures present, in-flight parents keep us waiting;
            // the UpstreamFailed verdict lands once everything settles.
            return Ok(NodeReadiness::Waiting {
                pending_parents: pending,
            });
        }
        if !failed.is_empty() {
            return Ok(NodeReadiness::UpstreamFailed {
                failed_parents: failed,
            });
        }
        if completed == 0 && skipped > 0 {
            return Ok(NodeReadiness::SkipAll);
        }
        Ok(NodeReadiness::Ready)
    }

    /// All nodes that are Ready right now and not yet started/terminal
    /// (`Pending` or absent from `states`).
    pub fn ready_nodes(&self, states: &BTreeMap<String, NodeExecutionState>) -> Vec<String> {
        self.node_keys
            .iter()
            .filter(|key| {
                matches!(
                    states
                        .get(**key)
                        .copied()
                        .unwrap_or(NodeExecutionState::Pending),
                    NodeExecutionState::Pending
                )
            })
            .filter(|key| {
                self.readiness(key, states)
                    .map(|readiness| readiness.is_ready())
                    .unwrap_or(false)
            })
            .map(|key| (*key).to_owned())
            .collect()
    }

    /// Nodes stuck waiting on parents (diagnostics / deadlock checks).
    pub fn blocked_nodes(
        &self,
        states: &BTreeMap<String, NodeExecutionState>,
    ) -> BTreeMap<String, NodeReadiness> {
        self.node_keys
            .iter()
            .filter(|key| {
                matches!(
                    states
                        .get(**key)
                        .copied()
                        .unwrap_or(NodeExecutionState::Pending),
                    NodeExecutionState::Pending
                )
            })
            .filter_map(|key| {
                self.readiness(key, states).ok().and_then(|r| match r {
                    NodeReadiness::Ready => None,
                    other => Some(((*key).to_owned(), other)),
                })
            })
            .collect()
    }

    /// `true` when every node is in a terminal state (or skipped).
    pub fn is_complete(&self, states: &BTreeMap<String, NodeExecutionState>) -> bool {
        self.node_keys.iter().all(|key| {
            states
                .get(*key)
                .copied()
                .unwrap_or(NodeExecutionState::Pending)
                .is_terminal()
        })
    }

    /// Nodes that should cascade-skip because every parent was skipped.
    pub fn skipped_by_cascade(&self, states: &BTreeMap<String, NodeExecutionState>) -> Vec<String> {
        self.node_keys
            .iter()
            .filter(|key| {
                matches!(
                    states
                        .get(**key)
                        .copied()
                        .unwrap_or(NodeExecutionState::Pending),
                    NodeExecutionState::Pending
                )
            })
            .filter(|key| matches!(self.readiness(key, states), Ok(NodeReadiness::SkipAll)))
            .map(|key| (*key).to_owned())
            .collect()
    }

    /// Kahn topological order (deterministic: ties broken by key).
    pub fn topological_order(&self) -> Result<Vec<String>> {
        let mut in_degree: BTreeMap<&str, usize> = self
            .node_keys
            .iter()
            .map(|k| (*k, self.in_degree.get(k).copied().unwrap_or(0)))
            .collect();
        let mut frontier: BTreeSet<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(key, _)| *key)
            .collect();
        let mut order = Vec::with_capacity(self.node_keys.len());
        while let Some(next) = frontier.iter().next().copied() {
            frontier.remove(next);
            order.push(next.to_owned());
            if let Some(children) = self.children.get(next) {
                for child in children {
                    let deg = in_degree
                        .get_mut(child)
                        .ok_or_else(|| AppError::internal("resolver index inconsistency"))?;
                    *deg = deg.saturating_sub(1);
                    if *deg == 0 {
                        frontier.insert(child);
                    }
                }
            }
        }
        if order.len() != self.node_keys.len() {
            return Err(AppError::validation(
                "workflow graph contains a dependency cycle",
            ));
        }
        Ok(order)
    }

    /// All nodes reachable from `node_key` (excluding itself).
    pub fn descendants_of(&self, node_key: &str) -> Result<Vec<String>> {
        self.assert_node(node_key)?;
        let mut visited = BTreeSet::new();
        let mut stack = vec![node_key];
        while let Some(current) = stack.pop() {
            if let Some(children) = self.children.get(current) {
                for child in children {
                    if visited.insert(*child) {
                        stack.push(child);
                    }
                }
            }
        }
        Ok(visited.into_iter().map(str::to_owned).collect())
    }

    fn assert_node(&self, node_key: &str) -> Result<()> {
        if self.contains(node_key) {
            Ok(())
        } else {
            Err(AppError::not_found("workflow_node", node_key))
        }
    }
}

impl fmt::Display for NodeReadiness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready => f.write_str("ready"),
            Self::Waiting { pending_parents } => {
                write!(f, "waiting on [{}]", pending_parents.join(", "))
            },
            Self::UpstreamFailed { failed_parents } => {
                write!(f, "upstream failed: [{}]", failed_parents.join(", "))
            },
            Self::SkipAll => f.write_str("skip cascade"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_domain::{NodeCapability, WorkflowEdge, WorkflowNode, WorkflowNodeType};
    use std::collections::BTreeMap;

    fn node(key: &str, kind: WorkflowNodeType) -> WorkflowNode {
        let node = WorkflowNode::new(key, kind, key.to_uppercase())
            .expect("valid node")
            .with_capability(NodeCapability::default());
        match kind {
            WorkflowNodeType::Agent => node.with_config(serde_json::Map::from_iter([(
                "agent_ref".to_owned(),
                serde_json::Value::from("agent-x"),
            )])),
            WorkflowNodeType::Tool => node.with_config(serde_json::Map::from_iter([(
                "tool_ref".to_owned(),
                serde_json::Value::from("tool-x"),
            )])),
            _ => node,
        }
    }

    fn diamond() -> WorkflowGraph {
        // start → {a, b} → c → end
        let nodes = vec![
            node("start", WorkflowNodeType::Start),
            node("a", WorkflowNodeType::Agent),
            node("b", WorkflowNodeType::Tool),
            node("c", WorkflowNodeType::Join),
            node("end", WorkflowNodeType::End),
        ];
        let edges = vec![
            WorkflowEdge::new("start", "a").unwrap(),
            WorkflowEdge::new("start", "b").unwrap(),
            WorkflowEdge::new("a", "c").unwrap(),
            WorkflowEdge::new("b", "c").unwrap(),
            WorkflowEdge::new("c", "end").unwrap(),
        ];
        WorkflowGraph::new(nodes, edges)
    }

    #[test]
    fn entry_and_readiness_flow() {
        let graph = diamond();
        graph.validate().expect("diamond validates");
        let resolver = DependencyResolver::new(&graph).expect("resolver");
        assert_eq!(resolver.entry_points(), vec!["start".to_owned()]);
        let order = resolver.topological_order().expect("topo");
        assert_eq!(order.first().map(String::as_str), Some("start"));
        assert_eq!(order.last().map(String::as_str), Some("end"));

        let mut states = BTreeMap::new();
        // Initially only start is ready.
        assert_eq!(resolver.ready_nodes(&states), vec!["start".to_owned()]);
        states.insert("start".to_owned(), NodeExecutionState::Completed);
        assert_eq!(
            resolver.ready_nodes(&states),
            vec!["a".to_owned(), "b".to_owned()]
        );
        states.insert("a".to_owned(), NodeExecutionState::Completed);
        // c waits on b.
        assert!(matches!(
            resolver.readiness("c", &states).unwrap(),
            NodeReadiness::Waiting { .. }
        ));
        states.insert("b".to_owned(), NodeExecutionState::Completed);
        assert!(resolver.readiness("c", &states).unwrap().is_ready());
        states.insert("c".to_owned(), NodeExecutionState::Completed);
        assert_eq!(resolver.ready_nodes(&states), vec!["end".to_owned()]);
        states.insert("end".to_owned(), NodeExecutionState::Completed);
        assert!(resolver.is_complete(&states));
    }

    #[test]
    fn upstream_failure_and_skip_cascade() {
        let graph = diamond();
        let resolver = DependencyResolver::new(&graph).expect("resolver");
        let mut states = BTreeMap::new();
        states.insert("start".to_owned(), NodeExecutionState::Completed);
        states.insert("a".to_owned(), NodeExecutionState::Completed);
        states.insert("b".to_owned(), NodeExecutionState::Failed);
        match resolver.readiness("c", &states).unwrap() {
            NodeReadiness::UpstreamFailed { failed_parents } => {
                assert_eq!(failed_parents, vec!["b".to_owned()]);
            },
            other => panic!("expected UpstreamFailed, got {other}"),
        }

        // Skip cascade: both parents skipped ⇒ c skipped; end follows.
        let mut states = BTreeMap::new();
        states.insert("start".to_owned(), NodeExecutionState::Completed);
        states.insert("a".to_owned(), NodeExecutionState::Skipped);
        states.insert("b".to_owned(), NodeExecutionState::Skipped);
        assert_eq!(
            resolver.readiness("c", &states).unwrap(),
            NodeReadiness::SkipAll
        );
        assert_eq!(resolver.skipped_by_cascade(&states), vec!["c".to_owned()]);

        // Mixed completed+skipped is ready.
        let mut states = BTreeMap::new();
        states.insert("start".to_owned(), NodeExecutionState::Completed);
        states.insert("a".to_owned(), NodeExecutionState::Completed);
        states.insert("b".to_owned(), NodeExecutionState::Skipped);
        assert!(resolver.readiness("c", &states).unwrap().is_ready());
    }

    #[test]
    fn cycles_and_unknown_nodes_are_rejected() {
        let nodes = vec![
            node("a", WorkflowNodeType::Agent),
            node("b", WorkflowNodeType::Agent),
        ];
        let edges = vec![
            WorkflowEdge::new("a", "b").unwrap(),
            WorkflowEdge::new("b", "a").unwrap(),
        ];
        let graph = WorkflowGraph::new(nodes, edges);
        assert!(DependencyResolver::new(&graph).is_err());

        let nodes = vec![node("a", WorkflowNodeType::Agent)];
        let edges = vec![WorkflowEdge::new("a", "ghost").unwrap()];
        let graph = WorkflowGraph::new(nodes, edges);
        assert!(DependencyResolver::new(&graph).is_err());

        let graph = diamond();
        let unresolved = DependencyResolver::new(&graph).unwrap();
        assert!(unresolved.readiness("ghost", &BTreeMap::new()).is_err());
    }

    #[test]
    fn descendants_cover_the_subtree() {
        let graph = diamond();
        let resolver = DependencyResolver::new(&graph).unwrap();
        let subtree = resolver.descendants_of("start").expect("descendants");
        assert_eq!(subtree.len(), 4);
        assert!(resolver.descendants_of("end").unwrap().is_empty());
    }
}
