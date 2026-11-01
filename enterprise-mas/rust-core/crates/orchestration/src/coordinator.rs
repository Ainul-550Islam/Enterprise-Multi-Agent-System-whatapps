//! Execution coordination: parent/child trees (agent delegation), parallel
//! branch tracking and join-condition resolution.
//!
//! In-memory by design: durable state lives in the execution repository; this
//! coordinator is rebuilt from it during `recover()`.

use mas_common::enums::ExecutionStatus;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, ExecutionId};
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// What a join node waits for, declared when the parallel forked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRequirement {
    /// Node keys that must reach a terminal state.
    pub expected_branches: BTreeSet<String>,
    /// When `false`, the first completed branch wins and the others may be
    /// cancelled (`race` semantics). Default: wait for all.
    #[serde(default = "default_wait_for_all")]
    pub wait_for_all: bool,
}

fn default_wait_for_all() -> bool {
    true
}

/// Outcome of one child execution (for join resolution).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildOutcome {
    pub execution_id: ExecutionId,
    pub node_key: Option<String>,
    pub status: ExecutionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct ExecutionTreeState {
    parent: Option<ExecutionId>,
    children: BTreeSet<ExecutionId>,
    child_statuses: BTreeMap<ExecutionId, ExecutionStatus>,
    /// Agent delegation: child → agent it runs.
    delegations: BTreeMap<ExecutionId, AgentId>,
}

/// Decision for a join evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinDecision {
    /// All (or race-winner) requirements satisfied; contains branch outcomes
    /// keyed by node key where known.
    Ready { outcomes: Vec<ChildOutcome> },
    /// Still waiting on these node keys.
    Waiting { pending: BTreeSet<String> },
    /// A required branch failed/cancelled; the join cannot succeed.
    Failed { reason: String },
}

#[derive(Debug, Default)]
struct Inner {
    trees: BTreeMap<ExecutionId, ExecutionTreeState>,
    /// join condition state: (parent, join_node_key) → (requirement, outcomes)
    joins: BTreeMap<(ExecutionId, String), (JoinRequirement, Vec<ChildOutcome>)>,
}

/// The execution coordinator. Cloning shares the same underlying state.
#[derive(Debug, Clone, Default)]
pub struct ExecutionCoordinator {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
}

impl ExecutionCoordinator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a root execution (no parent).
    pub fn register_root(&self, execution_id: ExecutionId) {
        let mut inner = self.lock();
        inner.trees.entry(execution_id).or_default();
    }

    /// Registers `child` under `parent` (agent delegation when `agent` is set).
    ///
    /// # Errors
    /// * parent unknown (register the parent first),
    /// * parent already terminal (children are immutable afterwards),
    /// * child depth would exceed [`mas_common::constants::MAX_CHILD_EXECUTIONS`].
    pub fn register_child(
        &self,
        parent: ExecutionId,
        child: ExecutionId,
        agent: Option<AgentId>,
    ) -> Result<()> {
        if parent == child {
            return Err(AppError::validation("an execution cannot parent itself"));
        }
        let mut inner = self.lock();
        // Cycle protection: child's subtree must not contain parent.
        if self.subtree_contains(&inner, child, parent) {
            return Err(AppError::conflict(
                "delegation cycle detected: parent is reachable from child",
            ));
        }
        if !inner.trees.contains_key(&parent) {
            return Err(AppError::not_found("parent_execution", parent.to_string()));
        }
        let depth = self.subtree_size(&inner, parent);
        if depth >= mas_common::constants::MAX_CHILD_EXECUTIONS as usize {
            return Err(AppError::rate_limited(format!(
                "execution {parent} exceeds the maximum child execution count"
            )));
        }
        let parent_state = inner
            .trees
            .get_mut(&parent)
            .expect("parent presence checked above");
        parent_state.children.insert(child);
        parent_state
            .child_statuses
            .insert(child, ExecutionStatus::Pending);
        if let Some(agent) = agent {
            parent_state.delegations.insert(child, agent);
        }
        inner.trees.entry(child).or_default().parent = Some(parent);
        Ok(())
    }

    /// Records a child's terminal status and wakes join waiters.
    ///
    /// # Errors
    /// * unknown child,
    /// * duplicate terminal report (reported once).
    pub fn child_completed(&self, child: ExecutionId, outcome: ChildOutcome) -> Result<()> {
        let mut inner = self.lock();
        let parent = inner
            .trees
            .get(&child)
            .and_then(|state| state.parent)
            .ok_or_else(|| AppError::not_found("child_execution", child.to_string()))?;
        if !outcome.status.is_terminal() {
            return Err(AppError::validation(
                "child_completed requires a terminal status",
            ));
        }
        let parent_state = inner
            .trees
            .get_mut(&parent)
            .ok_or_else(|| AppError::not_found("parent_execution", parent.to_string()))?;
        let previous = parent_state.child_statuses.insert(child, outcome.status);
        if previous.is_some_and(|status| status.is_terminal()) {
            return Err(AppError::conflict(format!(
                "child {child} already reported terminal status"
            )));
        }
        // Feed any joins that mention this child's node key.
        let join_keys: Vec<(ExecutionId, String)> = inner
            .joins
            .keys()
            .filter(|(execution, _)| *execution == parent)
            .cloned()
            .collect();
        for key in join_keys {
            if let Some((requirement, outcomes)) = inner.joins.get_mut(&key) {
                let matches_branch = match &outcome.node_key {
                    Some(node_key) => requirement.expected_branches.contains(node_key),
                    None => false,
                };
                if matches_branch {
                    outcomes.retain(|o| o.node_key != outcome.node_key);
                    outcomes.push(outcome.clone());
                }
            }
        }
        self.notify.notify_waiters();
        Ok(())
    }

    /// Declares/updates the join condition for a join node under `parent`.
    pub fn declare_join(
        &self,
        parent: ExecutionId,
        join_node_key: impl Into<String>,
        requirement: JoinRequirement,
    ) -> Result<()> {
        let join_node_key = join_node_key.into();
        if requirement.expected_branches.is_empty() {
            return Err(AppError::invalid_field(
                "expected_branches",
                "required",
                "a join must expect at least one branch",
            ));
        }
        let mut inner = self.lock();
        if !inner.trees.contains_key(&parent) {
            return Err(AppError::not_found("parent_execution", parent.to_string()));
        }
        inner
            .joins
            .insert((parent, join_node_key), (requirement, Vec::new()));
        Ok(())
    }

    /// Evaluates a join's readiness without consuming the records.
    pub fn resolve_join(&self, parent: ExecutionId, join_node_key: &str) -> Result<JoinDecision> {
        let inner = self.lock();
        let (requirement, outcomes) = inner
            .joins
            .get(&(parent, join_node_key.to_owned()))
            .ok_or_else(|| {
                AppError::not_found("join_condition", format!("{parent}/{join_node_key}"))
            })?;
        Ok(evaluate_join(requirement, outcomes))
    }

    /// Children of `parent` not yet in a terminal state.
    #[must_use]
    pub fn pending_children(&self, parent: ExecutionId) -> Vec<ExecutionId> {
        let inner = self.lock();
        inner
            .trees
            .get(&parent)
            .map(|state| {
                state
                    .child_statuses
                    .iter()
                    .filter(|(_, status)| !status.is_terminal())
                    .map(|(child, _)| *child)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// All children of `parent` reached terminal states?
    #[must_use]
    pub fn children_settled(&self, parent: ExecutionId) -> bool {
        let inner = self.lock();
        inner.trees.get(&parent).is_some_and(|state| {
            state
                .child_statuses
                .values()
                .all(ExecutionStatus::is_terminal)
        })
    }

    /// Immediate outcomes of every completed child (joined view).
    #[must_use]
    pub fn child_statuses(&self, parent: ExecutionId) -> Vec<(ExecutionId, ExecutionStatus)> {
        let inner = self.lock();
        inner
            .trees
            .get(&parent)
            .map(|state| {
                state
                    .child_statuses
                    .iter()
                    .map(|(child, status)| (*child, *status))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Waits until every child of `parent` is terminal. Resolves immediately
    /// when already settled. Cancellation of the *parent* is signaled by the
    /// engine calling [`ExecutionCoordinator::notify_all_waiters`].
    pub async fn wait_for_children(&self, parent: ExecutionId) {
        loop {
            if self.children_settled(parent) {
                return;
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.children_settled(parent) {
                return;
            }
            notified.await;
        }
    }

    /// Wakes every waiter (used on recovery/cancellation sweeps).
    pub fn notify_all_waiters(&self) {
        self.notify.notify_waiters();
    }

    /// Removes an execution tree (parent + descendants) — used after the
    /// parent's terminal state is durably stored.
    pub fn remove_tree(&self, root: ExecutionId) {
        let mut inner = self.lock();
        let descendants = self.collect_subtree(&inner, root);
        for id in descendants {
            inner.trees.remove(&id);
            inner.joins.retain(|(execution, _), _| *execution != id);
        }
    }

    #[must_use]
    pub fn tracked_executions(&self) -> usize {
        self.lock().trees.len()
    }

    // -- internals ------------------------------------------------------------

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn subtree_contains(&self, inner: &Inner, root: ExecutionId, needle: ExecutionId) -> bool {
        let mut stack = vec![root];
        let mut visited = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if current == needle {
                return true;
            }
            if !visited.insert(current) {
                continue;
            }
            if let Some(state) = inner.trees.get(&current) {
                stack.extend(state.children.iter().copied());
            }
        }
        false
    }

    fn subtree_size(&self, inner: &Inner, root: ExecutionId) -> usize {
        let mut count = 0;
        let mut stack = vec![root];
        let mut visited = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if !visited.insert(current) {
                continue;
            }
            count += 1;
            if let Some(state) = inner.trees.get(&current) {
                stack.extend(state.children.iter().copied());
            }
        }
        count
    }

    fn collect_subtree(&self, inner: &Inner, root: ExecutionId) -> Vec<ExecutionId> {
        let mut ids = Vec::new();
        let mut stack = vec![root];
        while let Some(current) = stack.pop() {
            ids.push(current);
            if let Some(state) = inner.trees.get(&current) {
                stack.extend(state.children.iter().copied());
            }
        }
        ids
    }
}

fn evaluate_join(requirement: &JoinRequirement, outcomes: &[ChildOutcome]) -> JoinDecision {
    let by_branch: BTreeMap<&String, &ChildOutcome> = outcomes
        .iter()
        .filter_map(|o| o.node_key.as_ref().map(|k| (k, o)))
        .collect();

    if !requirement.wait_for_all {
        // Race semantics: the first completed branch wins; a failed branch
        // only dooms the join when *no* branch can win anymore.
        let winners: Vec<ChildOutcome> = requirement
            .expected_branches
            .iter()
            .filter_map(|branch| {
                by_branch
                    .get(branch)
                    .filter(|outcome| outcome.status == ExecutionStatus::Completed)
                    .map(|outcome| (*outcome).clone())
            })
            .collect();
        if !winners.is_empty() {
            return JoinDecision::Ready { outcomes: winners };
        }
        let all_terminal = requirement.expected_branches.iter().all(|branch| {
            by_branch
                .get(branch)
                .is_some_and(|outcome| outcome.status.is_terminal())
        });
        if all_terminal {
            return JoinDecision::Failed {
                reason: "all branches finished without a winner".to_owned(),
            };
        }
        return JoinDecision::Waiting {
            pending: requirement
                .expected_branches
                .iter()
                .filter(|branch| !by_branch.contains_key(*branch))
                .cloned()
                .collect(),
        };
    }

    // Wait-for-all: any failed/cancelled expected branch dooms the join.
    for branch in &requirement.expected_branches {
        if let Some(outcome) = by_branch.get(branch) {
            if matches!(
                outcome.status,
                ExecutionStatus::Failed | ExecutionStatus::Cancelled
            ) {
                return JoinDecision::Failed {
                    reason: format!("branch '{branch}' ended with status {}", outcome.status),
                };
            }
        }
    }
    let pending: BTreeSet<String> = requirement
        .expected_branches
        .iter()
        .filter(|branch| !by_branch.contains_key(*branch))
        .cloned()
        .collect();
    if pending.is_empty() {
        JoinDecision::Ready {
            outcomes: requirement
                .expected_branches
                .iter()
                .filter_map(|branch| by_branch.get(branch).map(|outcome| (*outcome).clone()))
                .collect(),
        }
    } else {
        JoinDecision::Waiting { pending }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(id: ExecutionId, node: &str, status: ExecutionStatus) -> ChildOutcome {
        ChildOutcome {
            execution_id: id,
            node_key: Some(node.to_owned()),
            status,
            output_ref: None,
        }
    }

    #[test]
    fn register_and_track_children() {
        let coordinator = ExecutionCoordinator::new();
        let parent = ExecutionId::new();
        coordinator.register_root(parent);
        let c1 = ExecutionId::new();
        let c2 = ExecutionId::new();
        coordinator
            .register_child(parent, c1, Some(AgentId::new()))
            .unwrap();
        coordinator.register_child(parent, c2, None).unwrap();
        assert_eq!(coordinator.pending_children(parent).len(), 2);
        assert!(!coordinator.children_settled(parent));

        coordinator
            .child_completed(c1, outcome(c1, "a", ExecutionStatus::Completed))
            .unwrap();
        coordinator
            .child_completed(c2, outcome(c2, "b", ExecutionStatus::Failed))
            .unwrap();
        assert!(coordinator.children_settled(parent));
        // Duplicate terminal report conflicts.
        assert!(coordinator
            .child_completed(c1, outcome(c1, "a", ExecutionStatus::Completed))
            .is_err());
        // Unknown children are rejected.
        assert!(coordinator
            .child_completed(
                ExecutionId::new(),
                outcome(ExecutionId::new(), "x", ExecutionStatus::Completed)
            )
            .is_err());
    }

    #[test]
    fn delegation_cycles_are_rejected() {
        let coordinator = ExecutionCoordinator::new();
        let a = ExecutionId::new();
        let b = ExecutionId::new();
        coordinator.register_root(a);
        coordinator.register_root(b);
        coordinator.register_child(a, b, None).unwrap();
        // b → a would close a cycle (a is reachable from b's subtree root).
        assert!(coordinator.register_child(b, a, None).is_err());
        assert!(coordinator.register_child(a, a, None).is_err());
    }

    #[test]
    fn join_wait_all_semantics() {
        let requirement = JoinRequirement {
            expected_branches: BTreeSet::from(["left".to_owned(), "right".to_owned()]),
            wait_for_all: true,
        };
        // Only left finished.
        let outcomes = vec![outcome(
            ExecutionId::new(),
            "left",
            ExecutionStatus::Completed,
        )];
        match evaluate_join(&requirement, &outcomes) {
            JoinDecision::Waiting { pending } => {
                assert!(pending.contains("right"));
            },
            other => panic!("expected waiting, got {other:?}"),
        }
        // Both finished.
        let outcomes = vec![
            outcome(ExecutionId::new(), "left", ExecutionStatus::Completed),
            outcome(ExecutionId::new(), "right", ExecutionStatus::Completed),
        ];
        match evaluate_join(&requirement, &outcomes) {
            JoinDecision::Ready { outcomes } => assert_eq!(outcomes.len(), 2),
            other => panic!("expected ready, got {other:?}"),
        }
        // One failed.
        let outcomes = vec![
            outcome(ExecutionId::new(), "left", ExecutionStatus::Completed),
            outcome(ExecutionId::new(), "right", ExecutionStatus::Failed),
        ];
        assert!(matches!(
            evaluate_join(&requirement, &outcomes),
            JoinDecision::Failed { .. }
        ));
    }

    #[test]
    fn join_race_semantics() {
        let requirement = JoinRequirement {
            expected_branches: BTreeSet::from(["fast".to_owned(), "slow".to_owned()]),
            wait_for_all: false,
        };
        let outcomes = vec![outcome(
            ExecutionId::new(),
            "fast",
            ExecutionStatus::Completed,
        )];
        match evaluate_join(&requirement, &outcomes) {
            JoinDecision::Ready { outcomes } => assert_eq!(outcomes.len(), 1),
            other => panic!("expected race winner, got {other:?}"),
        }
        let outcomes = vec![outcome(ExecutionId::new(), "slow", ExecutionStatus::Failed)];
        assert!(matches!(
            evaluate_join(&requirement, &outcomes),
            JoinDecision::Waiting { .. }
        ));
    }

    #[tokio::test]
    async fn wait_for_children_resolves() {
        let coordinator = ExecutionCoordinator::new();
        let parent = ExecutionId::new();
        coordinator.register_root(parent);
        let child = ExecutionId::new();
        coordinator.register_child(parent, child, None).unwrap();
        let clone = coordinator.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            clone
                .child_completed(child, outcome(child, "n", ExecutionStatus::Completed))
                .unwrap();
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            coordinator.wait_for_children(parent),
        )
        .await
        .expect("wait must resolve");
    }
}
