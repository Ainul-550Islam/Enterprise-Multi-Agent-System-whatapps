//! Node execution: the plugin surface of the workflow runtime.
//!
//! A [`NodeExecutor`] knows how to execute one or more [`WorkflowNodeType`]s
//! (agent invocation, tool call, approval gate, delay, …). The runtime drives
//! the DAG; executors only see one node at a time through
//! [`NodeExecutionInput`] and report a single [`NodeOutcome`].
//!
//! Executors are registered in a [`NodeExecutorRegistry`]; missing executors
//! are configuration errors (the engine refuses to run the graph), never
//! silent no-ops.

use crate::runtime_context::RuntimeContext;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ExecutionStepId};
use mas_common::result::Result;
use mas_domain::{WorkflowNode, WorkflowNodeType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Structured node failure (kept distinct from [`AppError`]): a node *failing*
/// is normal control flow — retries, DLQ routing and step records use this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeFailure {
    /// Stable machine code (e.g. `tool_unavailable`, `approval_rejected`).
    pub code: String,
    /// Human-readable, already redacted, truncated at 2048 chars.
    pub message: String,
    /// Whether another attempt makes sense.
    pub retryable: bool,
    /// Optional server-supplied cooldown (e.g. rate limits).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_hint: Option<Duration>,
}

impl NodeFailure {
    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        let mut message = message.into();
        message.truncate(2048);
        Self {
            code: code.into(),
            message,
            retryable: true,
            retry_after_hint: None,
        }
    }

    pub fn permanent(code: impl Into<String>, message: impl Into<String>) -> Self {
        let mut message = message.into();
        message.truncate(2048);
        Self {
            code: code.into(),
            message,
            retryable: false,
            retry_after_hint: None,
        }
    }

    #[must_use]
    pub fn with_retry_after_hint(mut self, hint: Duration) -> Self {
        self.retry_after_hint = Some(hint);
        self
    }

    /// Classifies an [`AppError`] as a node failure (infrastructure errors
    /// bubble via `Err`; this is for executors mapping domain failures).
    pub fn from_app_error(code: impl Into<String>, error: &AppError) -> Self {
        Self {
            code: code.into(),
            message: {
                let mut message = error.to_string();
                message.truncate(2048);
                message
            },
            retryable: error.is_retryable(),
            retry_after_hint: error.retry_after_hint(),
        }
    }
}

/// The terminal verdict of one node invocation (one attempt).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum NodeOutcome {
    /// Node succeeded; `output` becomes available to downstream nodes.
    Completed { output: Value },
    /// Node failed; retry policy decides the follow-up.
    Failed { failure: NodeFailure },
    /// Node asks to be re-invoked later (delay node, external backoff).
    Deferred {
        wake_after: Duration,
        reason: String,
    },
    /// Node blocks on human approval; the runtime pauses the branch.
    WaitingApproval { approval_ref: String },
    /// Node decided not to act (condition evaluated false, branch not taken).
    Skipped { reason: String },
}

impl NodeOutcome {
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }

    /// Maps the outcome to the checkpoint state it implies.
    #[must_use]
    pub const fn to_node_state(&self) -> crate::checkpoint::NodeExecutionState {
        match self {
            Self::Completed { .. } => crate::checkpoint::NodeExecutionState::Completed,
            Self::Failed { .. } => crate::checkpoint::NodeExecutionState::Failed,
            Self::Deferred { .. } => crate::checkpoint::NodeExecutionState::Deferred,
            Self::WaitingApproval { .. } => crate::checkpoint::NodeExecutionState::WaitingApproval,
            Self::Skipped { .. } => crate::checkpoint::NodeExecutionState::Skipped,
        }
    }
}

/// Everything an executor needs for one node attempt.
///
/// The graph identity comes from `node`; execution identity and caller
/// context from the borrowed [`RuntimeContext`] (tenant, actor, correlation,
/// deadline, cancellation).
#[derive(Debug)]
pub struct NodeExecutionInput<'a> {
    pub execution_id: ExecutionId,
    /// The step row the engine created for this attempt (for output linkage).
    pub step_id: ExecutionStepId,
    /// The node to execute (config + capability included).
    pub node: &'a WorkflowNode,
    /// Rendered node input (template output / upstream result).
    pub input: Value,
    /// Caller + execution context; cancel and deadline must be honored.
    pub context: &'a RuntimeContext,
    /// 1-based attempt number (first attempt = 1).
    pub attempt: u32,
}

/// Executes one node type (or several shared shapes).
///
/// Contract:
/// * honor `input.context.cancellation()` and `deadline()` cooperatively,
/// * never block the thread (async from here on),
/// * return failures as [`NodeOutcome::Failed`] — reserve `Err` for
///   infrastructure breakdown (transport loss), which the engine treats as
///   retryable-by-default.
#[async_trait::async_trait]
pub trait NodeExecutor: Send + Sync + fmt::Debug {
    /// Whether this executor can run `node_type`.
    fn handles(&self, node_type: &WorkflowNodeType) -> bool;

    /// Executes exactly one attempt of the node. Attempt bookkeeping,
    /// timeouts and restart semantics belong to the runtime.
    async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome>;
}

/// Registry of node executors, keyed by the node types they handle.
#[derive(Clone, Default)]
pub struct NodeExecutorRegistry {
    executors: Vec<Arc<dyn NodeExecutor>>,
}

impl fmt::Debug for NodeExecutorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeExecutorRegistry")
            .field("registered_executors", &self.executors.len())
            .finish()
    }
}

impl NodeExecutorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder helper.
    #[must_use]
    pub fn with_executor(mut self, executor: Arc<dyn NodeExecutor>) -> Self {
        self.register(executor);
        self
    }

    /// Registers an executor. Later registrations shadow earlier ones for the
    /// same node type (explicit override beats accidental duplication).
    pub fn register(&mut self, executor: Arc<dyn NodeExecutor>) {
        self.executors.push(executor);
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.executors.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.executors.is_empty()
    }

    /// Finds the executor for a node type (last registered wins).
    #[must_use]
    pub fn executor_for(&self, node_type: &WorkflowNodeType) -> Option<Arc<dyn NodeExecutor>> {
        self.executors
            .iter()
            .rev()
            .find(|executor| executor.handles(node_type))
            .cloned()
    }

    /// `true` when every node type in `graph` has a registered executor.
    pub fn covers_graph(&self, graph: &mas_domain::WorkflowGraph) -> Result<()> {
        let mut missing: Vec<String> = graph
            .nodes
            .iter()
            .filter(|node| {
                // Control-flow node kinds are handled by the runtime itself.
                !matches!(
                    node.node_type,
                    WorkflowNodeType::Start
                        | WorkflowNodeType::End
                        | WorkflowNodeType::Join
                        | WorkflowNodeType::Parallel
                        | WorkflowNodeType::Condition
                ) && self.executor_for(&node.node_type).is_none()
            })
            .map(|node| format!("{} ({})", node.node_key, node.node_type))
            .collect();
        missing.sort();
        missing.dedup();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(AppError::validation(format!(
                "no executor registered for node types: {}",
                missing.join(", ")
            )))
        }
    }

    /// Dispatches one attempt to the matching executor.
    ///
    /// # Errors
    /// * executor missing for the node type (configuration error), or
    /// * the executor itself returned `Err` (infrastructure breakdown).
    pub async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
        let executor = self.executor_for(&input.node.node_type).ok_or_else(|| {
            AppError::validation(format!(
                "no executor registered for node type '{}'",
                input.node.node_type
            ))
        })?;
        input.context.guard_active()?;
        executor.execute(input).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::Environment;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId};

    #[derive(Debug)]
    struct EchoToolExecutor;

    #[async_trait::async_trait]
    impl NodeExecutor for EchoToolExecutor {
        fn handles(&self, node_type: &WorkflowNodeType) -> bool {
            matches!(node_type, WorkflowNodeType::Tool)
        }

        async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
            Ok(NodeOutcome::Completed {
                output: input.input,
            })
        }
    }

    #[derive(Debug)]
    struct FailingAgentExecutor;

    #[async_trait::async_trait]
    impl NodeExecutor for FailingAgentExecutor {
        fn handles(&self, node_type: &WorkflowNodeType) -> bool {
            matches!(node_type, WorkflowNodeType::Agent)
        }

        async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
            let _ = input;
            Ok(NodeOutcome::Failed {
                failure: NodeFailure::retryable("agent_upstream", "agent backend 503"),
            })
        }
    }

    fn test_context() -> RuntimeContext {
        RuntimeContext::new(
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            "tester",
            Environment::Development,
        )
        .expect("context")
    }

    #[tokio::test]
    async fn registry_dispatches_to_matching_executor() {
        let registry = NodeExecutorRegistry::new()
            .with_executor(Arc::new(EchoToolExecutor))
            .with_executor(Arc::new(FailingAgentExecutor));
        assert_eq!(registry.len(), 2);
        let context = test_context();
        let node = WorkflowNode::new("n1", WorkflowNodeType::Tool, "N1").expect("node");
        let outcome = registry
            .execute(NodeExecutionInput {
                execution_id: ExecutionId::new(),
                step_id: ExecutionStepId::new(),
                node: &node,
                input: serde_json::json!({"ping": true}),
                context: &context,
                attempt: 1,
            })
            .await
            .expect("execute");
        assert!(outcome.is_completed());

        // Missing executor for Delay ⇒ configuration error.
        let delay = WorkflowNode::new("d1", WorkflowNodeType::Delay, "D1").expect("node");
        assert!(registry
            .execute(NodeExecutionInput {
                execution_id: ExecutionId::new(),
                step_id: ExecutionStepId::new(),
                node: &delay,
                input: Value::Null,
                context: &context,
                attempt: 1,
            })
            .await
            .is_err());
    }

    #[test]
    fn failure_classification_and_states() {
        let failure =
            NodeFailure::from_app_error("rate_limited", &AppError::rate_limited("slow down"));
        assert!(failure.retryable);
        assert_eq!(
            NodeOutcome::Skipped {
                reason: "branch not taken".to_owned(),
            }
            .to_node_state(),
            crate::checkpoint::NodeExecutionState::Skipped
        );
        assert_eq!(
            NodeOutcome::WaitingApproval {
                approval_ref: "apr-1".to_owned(),
            }
            .to_node_state(),
            crate::checkpoint::NodeExecutionState::WaitingApproval
        );
    }

    #[test]
    fn covers_graph_ignores_control_flow_nodes() {
        use mas_domain::{WorkflowEdge, WorkflowGraph};
        let nodes = vec![
            WorkflowNode::new("start", WorkflowNodeType::Start, "Start").unwrap(),
            WorkflowNode::new("tool", WorkflowNodeType::Tool, "Tool").unwrap(),
            WorkflowNode::new("agent", WorkflowNodeType::Agent, "Agent").unwrap(),
            WorkflowNode::new("end", WorkflowNodeType::End, "End").unwrap(),
        ];
        let edges = vec![
            WorkflowEdge::new("start", "tool").unwrap(),
            WorkflowEdge::new("tool", "agent").unwrap(),
            WorkflowEdge::new("agent", "end").unwrap(),
        ];
        let graph = WorkflowGraph::new(nodes, edges);
        // Only the tool executor registered ⇒ agent missing.
        let registry = NodeExecutorRegistry::new().with_executor(Arc::new(EchoToolExecutor));
        assert!(registry.covers_graph(&graph).is_err());
        let registry = registry.with_executor(Arc::new(FailingAgentExecutor));
        registry.covers_graph(&graph).expect("now covered");
    }
}
