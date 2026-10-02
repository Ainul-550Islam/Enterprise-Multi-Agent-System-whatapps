//! The workflow runtime: drives a [`WorkflowGraph`] for one execution.
//!
//! The runtime is the *last mile* of orchestration. Given an execution row, a
//! validated graph and a [`RuntimeContext`], it:
//!
//! 1. marks the execution running on the engine,
//! 2. loops over the DAG using [`DependencyResolver`], dispatching ready nodes
//!    through the [`NodeExecutorRegistry`] with bounded parallelism
//!    ([`ExecutionSemaphore`]), per-node timeouts ([`Deadline`]) and retries
//!    ([`RetryPolicy`] from `node.capability`),
//! 3. reacts to cancellations, deadlines, deferred nodes and approval gates,
//! 4. checkpoints node states periodically ([`ExecutionCheckpoint`]),
//! 5. lands the execution in a terminal engine state.
//!
//! Control-flow nodes (`start`, `end`, `parallel`, `join`, `condition`) are
//! handled by the runtime itself; all other node types need a registered
//! executor (enforced at run start).
//!
//! Node attempts run as tokio tasks reporting over an unbounded channel — the
//! loop is single-owner over all state, so no locking is needed inside a run.
//!
//! Checkpoint durability lives behind the execution store in later phases;
//! here the runtime retains the latest checkpoint per execution in memory and
//! exposes [`WorkflowRuntime::latest_checkpoint`] to the caller that owns
//! persistence.

use crate::checkpoint::{ExecutionCheckpoint, NodeExecutionState};
use crate::concurrency::ExecutionSemaphore;
use crate::dependency_resolver::DependencyResolver;
use crate::engine::OrchestrationEngine;
use crate::node_executor::{NodeExecutionInput, NodeExecutorRegistry, NodeFailure, NodeOutcome};
use crate::retry::{BackoffStrategy, RetryPolicy};
use crate::runtime_context::{RuntimeContext, RuntimeContextSnapshot};
use crate::timeout::Deadline;
use mas_common::constants;
use mas_common::enums::ExecutionStatus;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ExecutionStepId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{WorkflowGraph, WorkflowNode, WorkflowNodeType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Why a run paused (resumable) instead of terminating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SuspensionReason {
    /// A node awaits human approval; resume when it arrives.
    WaitingApproval {
        node_key: String,
        approval_ref: String,
    },
}

/// Verdict of a complete [`WorkflowRuntime::run`] call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum RuntimeRunResult {
    /// Every node reached a terminal state; the execution completed.
    Completed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        output: Option<Value>,
    },
    /// A node failed without remaining retries (or the graph deadlocked);
    /// the execution is `Failed`.
    Failed { error: String },
    /// Cancellation landed; the execution is `Cancelled`.
    Cancelled,
    /// Resumable suspension; a checkpoint exists for later resume.
    Suspended { reason: SuspensionReason },
}

/// Report of one finished node attempt (`Err` was already converted to a
/// retryable/permanent [`NodeFailure`] inside the attempt task).
#[derive(Debug)]
struct NodeAttemptReport {
    node_key: String,
    outcome: NodeOutcome,
    attempts_used: u32,
}

/// Drives workflows for executions. One instance per worker process; runs
/// hold their own state, so many executions may run concurrently.
#[derive(Debug)]
pub struct WorkflowRuntime {
    engine: Arc<OrchestrationEngine>,
    executors: NodeExecutorRegistry,
    max_parallel: u32,
    checkpoint_interval: u32,
    /// Latest in-memory checkpoint per execution.
    checkpoints: Mutex<BTreeMap<ExecutionId, ExecutionCheckpoint>>,
}

impl WorkflowRuntime {
    pub fn new(engine: Arc<OrchestrationEngine>, executors: NodeExecutorRegistry) -> Self {
        Self {
            engine,
            executors,
            max_parallel: 8,
            checkpoint_interval: 4,
            checkpoints: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    pub fn with_max_parallel(mut self, max_parallel: u32) -> Self {
        self.max_parallel = max_parallel.max(1);
        self
    }

    #[must_use]
    pub fn with_checkpoint_interval(mut self, every_n_transitions: u32) -> Self {
        self.checkpoint_interval = every_n_transitions.max(1);
        self
    }

    #[must_use]
    pub const fn executors(&self) -> &NodeExecutorRegistry {
        &self.executors
    }

    #[must_use]
    pub const fn engine(&self) -> &Arc<OrchestrationEngine> {
        &self.engine
    }

    /// Latest retained checkpoint, if any.
    #[must_use]
    pub fn latest_checkpoint(&self, execution_id: ExecutionId) -> Option<ExecutionCheckpoint> {
        self.checkpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&execution_id)
            .cloned()
    }

    /// Runs a graph to a terminal/suspended result.
    ///
    /// Preconditions owned by the caller: the execution exists and is
    /// `Pending`/`Paused`; the graph passed `WorkflowGraph::validate`;
    /// `context` carries the execution tenant.
    pub async fn run(
        &self,
        execution_id: ExecutionId,
        graph: &WorkflowGraph,
        context: &RuntimeContext,
    ) -> Result<RuntimeRunResult> {
        self.executors.covers_graph(graph)?;
        let resolver = DependencyResolver::new(graph)?;
        let graph_definition = serde_json::to_value(graph)
            .map_err(|e| AppError::serialization(format!("graph snapshot: {e}")))?;
        let execution = self.engine.mark_execution_started(execution_id).await?;
        let run_token = self
            .engine
            .cancellation_token(execution_id)
            .unwrap_or_default();
        let context_hash = ExecutionCheckpoint::compute_context_hash(
            &execution.correlation_id,
            &graph_definition,
            &execution.input,
        );

        let mut runtime = context.for_execution(execution_id, execution.parent_execution_id);
        runtime.set_data("input", execution.input.clone());

        let mut states: BTreeMap<String, NodeExecutionState> = graph
            .nodes
            .iter()
            .map(|node| (node.node_key.clone(), NodeExecutionState::Pending))
            .collect();

        let semaphore = Arc::new(ExecutionSemaphore::new(
            format!("workflow:{execution_id}"),
            self.max_parallel,
        )?);
        let (report_tx, mut report_rx) = mpsc::unbounded_channel::<NodeAttemptReport>();
        let mut in_flight: u32 = 0;
        let mut steps_attempted: u32 = 0;
        let mut transitions_since_checkpoint: u32 = 0;

        let verdict = loop {
            // 1. Drain settled attempts.
            let mut drain_error: Option<AppError> = None;
            while let Ok(report) = report_rx.try_recv() {
                in_flight = in_flight.saturating_sub(1);
                match self
                    .apply_outcome(execution_id, &report, &mut states, &mut runtime)
                    .await
                {
                    Ok(()) => transitions_since_checkpoint += 1,
                    Err(error) => {
                        drain_error = Some(error);
                        break;
                    },
                }
            }
            if let Some(error) = drain_error {
                break Some(Err(error));
            }

            // 2. Cooperative cancellation.
            if run_token.is_cancelled() || runtime.cancellation().is_cancelled() {
                self.engine.finalize_cancellation(execution_id).await?;
                break Some(Ok(RuntimeRunResult::Cancelled));
            }

            // 3. Deadline / context guards.
            if let Err(error) = runtime.guard_active() {
                self.engine.fail_execution(execution_id, &error).await?;
                break Some(Ok(RuntimeRunResult::Failed {
                    error: error.public_message(),
                }));
            }

            // 4. Cascading skips settle without executors.
            let cascades = resolver.skipped_by_cascade(&states);
            transitions_since_checkpoint += cascades.len() as u32;
            for node_key in cascades {
                states.insert(node_key, NodeExecutionState::Skipped);
            }

            // 5. Suspension: any node waiting on approval pauses the run.
            let suspension = states.iter().find_map(|(key, state)| {
                (*state == NodeExecutionState::WaitingApproval).then(|| {
                    let approval_ref = runtime
                        .data()
                        .get(format!("nodes.{key}.approval_ref").as_str())
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_owned();
                    SuspensionReason::WaitingApproval {
                        node_key: key.clone(),
                        approval_ref,
                    }
                })
            });
            if let Some(reason) = suspension {
                self.engine.pause_execution(context, execution_id).await?;
                self.store_checkpoint(
                    execution_id,
                    ExecutionStatus::Paused,
                    &states,
                    &context_hash,
                    &runtime,
                    &resolver,
                )?;
                break Some(Ok(RuntimeRunResult::Suspended { reason }));
            }

            // 6. Completion / deadlock detection.
            if resolver.is_complete(&states) && in_flight == 0 {
                let output = runtime
                    .data()
                    .get("output")
                    .cloned()
                    .filter(|value| !value.is_null());
                self.engine
                    .complete_execution(execution_id, output.clone())
                    .await?;
                break Some(Ok(RuntimeRunResult::Completed { output }));
            }
            let deferred_pending = states
                .values()
                .any(|state| *state == NodeExecutionState::Deferred);
            if in_flight == 0
                && !deferred_pending
                && resolver.ready_nodes(&states).is_empty()
                && self.pending_cascades(&resolver, &states).is_empty()
            {
                let blocked = resolver.blocked_nodes(&states);
                let error =
                    AppError::internal(format!("workflow deadlocked; blocked: {blocked:?}"));
                self.engine.fail_execution(execution_id, &error).await?;
                break Some(Ok(RuntimeRunResult::Failed {
                    error: error.public_message(),
                }));
            }

            // 7. Dispatch newly ready nodes.
            let mut over_cap: Option<AppError> = None;
            for node_key in resolver.ready_nodes(&states) {
                let node = graph
                    .nodes
                    .iter()
                    .find(|node| node.node_key == node_key)
                    .ok_or_else(|| AppError::internal("resolver produced unknown node"))?
                    .clone();
                steps_attempted += 1;
                if steps_attempted > constants::MAX_STEPS_PER_EXECUTION {
                    over_cap = Some(AppError::rate_limited(format!(
                        "execution exceeded the step cap of {}",
                        constants::MAX_STEPS_PER_EXECUTION
                    )));
                    break;
                }
                states.insert(node_key.clone(), NodeExecutionState::Running);
                transitions_since_checkpoint += 1;

                if is_runtime_handled(&node.node_type) {
                    let outcome = run_control_flow_node(&node, &runtime)?;
                    let report = NodeAttemptReport {
                        node_key: node_key.clone(),
                        outcome,
                        attempts_used: 1,
                    };
                    self.apply_outcome(execution_id, &report, &mut states, &mut runtime)
                        .await?;
                } else {
                    in_flight += 1;
                    spawn_node_attempt(
                        execution_id,
                        node,
                        runtime.child_scope(),
                        self.executors.clone(),
                        Arc::clone(&semaphore),
                        report_tx.clone(),
                    );
                }
            }
            if let Some(error) = over_cap {
                self.engine.fail_execution(execution_id, &error).await?;
                break Some(Ok(RuntimeRunResult::Failed {
                    error: error.public_message(),
                }));
            }

            // 8. Deferred branches: promote due ones, otherwise wait (bounded,
            //    cancellable).
            if in_flight == 0 && deferred_pending {
                self.promote_due_deferred(&mut states, &runtime);
                if let Some(wait) = self.next_deferred_wake(&states, &runtime) {
                    let still_deferred = states
                        .values()
                        .any(|state| *state == NodeExecutionState::Deferred);
                    if still_deferred {
                        tokio::select! {
                            biased;
                            () = run_token.cancelled() => {},
                            () = tokio::time::sleep(wait) => {},
                        }
                    }
                    continue;
                }
            }

            // 9. Periodic checkpoints.
            if transitions_since_checkpoint >= self.checkpoint_interval {
                self.store_checkpoint(
                    execution_id,
                    ExecutionStatus::Running,
                    &states,
                    &context_hash,
                    &runtime,
                    &resolver,
                )?;
                transitions_since_checkpoint = 0;
            }

            // 10. Wait point: when attempts are in flight, park until the
            //     next one settles (or cancellation lands); otherwise yield.
            if in_flight > 0 {
                tokio::select! {
                    biased;
                    () = run_token.cancelled() => {},
                    received = report_rx.recv() => {
                        if let Some(report) = received {
                            in_flight = in_flight.saturating_sub(1);
                            self.apply_outcome(execution_id, &report, &mut states, &mut runtime)
                                .await?;
                            transitions_since_checkpoint += 1;
                        } else {
                            // All senders dropped without reports: impossible
                            // unless the runtime is being torn down.
                            return Err(AppError::internal("attempt channel closed"));
                        }
                    },
                }
            } else {
                tokio::task::yield_now().await;
            }
        };

        match verdict {
            Some(result) => result,
            None => Err(AppError::internal("run loop ended without a verdict")),
        }
    }

    /// Validates a checkpoint against a (graph, input) pair and restores the
    /// resumable view: node states (running/deferred demoted) + context.
    pub fn restore_view(
        &self,
        checkpoint: ExecutionCheckpoint,
        expected_context_hash: &str,
    ) -> Result<(BTreeMap<String, NodeExecutionState>, RuntimeContext)> {
        checkpoint.validate_checkpoint(checkpoint.execution_id, expected_context_hash)?;
        let restored = checkpoint.restore_checkpoint();
        let snapshot: RuntimeContextSnapshot =
            serde_json::from_value(restored.context_snapshot.clone())
                .map_err(|e| AppError::serialization(format!("context snapshot: {e}")))?;
        let context = RuntimeContext::from_snapshot(snapshot)?;
        Ok((restored.node_states, context))
    }

    // -- internals --------------------------------------------------------------

    /// Cascades eligible for immediate skip (pure view for deadlock checks).
    fn pending_cascades(
        &self,
        resolver: &DependencyResolver<'_>,
        states: &BTreeMap<String, NodeExecutionState>,
    ) -> Vec<String> {
        resolver.skipped_by_cascade(states)
    }

    /// Applies a settled attempt to the state map + context data.
    /// `Err` from here means *the run must fail* (engine already informed).
    async fn apply_outcome(
        &self,
        execution_id: ExecutionId,
        report: &NodeAttemptReport,
        states: &mut BTreeMap<String, NodeExecutionState>,
        runtime: &mut RuntimeContext,
    ) -> Result<()> {
        let node_key = &report.node_key;
        match &report.outcome {
            NodeOutcome::Completed { output } => {
                states.insert(node_key.clone(), NodeExecutionState::Completed);
                runtime.set_data(format!("nodes.{node_key}.output"), output.clone());
            },
            NodeOutcome::Failed { failure } => {
                states.insert(node_key.clone(), NodeExecutionState::Failed);
                let error = AppError::external_service(
                    format!("node:{node_key}"),
                    format!(
                        "{} failed after {} attempt(s): {}",
                        failure.code, report.attempts_used, failure.message
                    ),
                );
                self.engine.fail_execution(execution_id, &error).await?;
                return Err(error);
            },
            NodeOutcome::Deferred { wake_after, reason } => {
                states.insert(node_key.clone(), NodeExecutionState::Deferred);
                let wake_at = Timestamp::now()
                    .checked_add(*wake_after)
                    .unwrap_or_else(Timestamp::now);
                runtime.set_data(
                    format!("nodes.{node_key}.wake_at"),
                    Value::from(wake_at.to_rfc3339_millis()),
                );
                runtime.set_data(
                    format!("nodes.{node_key}.defer_reason"),
                    Value::from(reason.clone()),
                );
            },
            NodeOutcome::WaitingApproval { approval_ref } => {
                states.insert(node_key.clone(), NodeExecutionState::WaitingApproval);
                runtime.set_data(
                    format!("nodes.{node_key}.approval_ref"),
                    Value::from(approval_ref.clone()),
                );
            },
            NodeOutcome::Skipped { .. } => {
                states.insert(node_key.clone(), NodeExecutionState::Skipped);
            },
        }
        Ok(())
    }

    /// Smallest time until a deferred branch wakes (`None` when nothing could
    /// be parsed — callers fall through to promoting/re-driving).
    fn next_deferred_wake(
        &self,
        states: &BTreeMap<String, NodeExecutionState>,
        runtime: &RuntimeContext,
    ) -> Option<Duration> {
        states
            .iter()
            .filter(|(_, state)| **state == NodeExecutionState::Deferred)
            .filter_map(|(key, _)| {
                runtime
                    .data()
                    .get(format!("nodes.{key}.wake_at").as_str())
                    .and_then(Value::as_str)
                    .and_then(|raw| Timestamp::parse_rfc3339(raw).ok())
            })
            .map(|wake_at| {
                wake_at
                    .duration_since(&Timestamp::now())
                    .unwrap_or(Duration::ZERO)
                    // Cap the nap so cancellation stays responsive.
                    .min(Duration::from_secs(30))
            })
            .min()
    }

    /// Deferred nodes whose wake time arrived (or whose marker vanished)
    /// become pending again.
    fn promote_due_deferred(
        &self,
        states: &mut BTreeMap<String, NodeExecutionState>,
        runtime: &RuntimeContext,
    ) {
        let now = Timestamp::now();
        let due: Vec<String> = states
            .iter()
            .filter(|(_, state)| **state == NodeExecutionState::Deferred)
            .filter(|(key, _)| {
                runtime
                    .data()
                    .get(format!("nodes.{key}.wake_at").as_str())
                    .and_then(Value::as_str)
                    .and_then(|raw| Timestamp::parse_rfc3339(raw).ok())
                    .is_none_or(|wake_at| !wake_at.is_before(&now))
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in due {
            states.insert(key, NodeExecutionState::Pending);
        }
    }

    fn store_checkpoint(
        &self,
        execution_id: ExecutionId,
        status: ExecutionStatus,
        states: &BTreeMap<String, NodeExecutionState>,
        context_hash: &str,
        runtime: &RuntimeContext,
        resolver: &DependencyResolver<'_>,
    ) -> Result<()> {
        let previous_sequence = self
            .latest_checkpoint(execution_id)
            .map_or(0, |checkpoint| checkpoint.sequence);
        let snapshot = serde_json::to_value(runtime.snapshot())
            .map_err(|e| AppError::serialization(format!("context snapshot: {e}")))?;
        let checkpoint = ExecutionCheckpoint::create_checkpoint(
            previous_sequence,
            execution_id,
            status,
            states.clone(),
            Vec::<ExecutionStepId>::new(),
            resolver.ready_nodes(states),
            snapshot,
            context_hash.to_owned(),
        )?;
        self.checkpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(execution_id, checkpoint);
        Ok(())
    }
}

// -- node-level execution -----------------------------------------------------

fn is_runtime_handled(node_type: &WorkflowNodeType) -> bool {
    matches!(
        node_type,
        WorkflowNodeType::Start
            | WorkflowNodeType::End
            | WorkflowNodeType::Parallel
            | WorkflowNodeType::Join
            | WorkflowNodeType::Condition
    )
}

/// Control-flow nodes evaluated by the runtime itself.
fn run_control_flow_node(node: &WorkflowNode, runtime: &RuntimeContext) -> Result<NodeOutcome> {
    match node.node_type {
        WorkflowNodeType::Start | WorkflowNodeType::Parallel | WorkflowNodeType::Join => {
            Ok(NodeOutcome::Completed {
                output: Value::Object(runtime.data().clone()),
            })
        },
        WorkflowNodeType::End => Ok(NodeOutcome::Completed {
            output: runtime.data().get("output").cloned().unwrap_or(Value::Null),
        }),
        WorkflowNodeType::Condition => {
            // Minimal, deterministic semantics: `expression` is a JSON pointer
            // into context data; truthy ⇒ pass, falsy ⇒ skip downstream.
            let pointer = node
                .config
                .get("expression")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let data = Value::Object(runtime.data().clone());
            let value = data.pointer(pointer);
            let truthy = value.is_some_and(|v| match v {
                Value::Bool(b) => *b,
                Value::Null => false,
                Value::String(s) => !s.is_empty(),
                Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
                Value::Array(a) => !a.is_empty(),
                Value::Object(o) => !o.is_empty(),
            });
            if truthy {
                Ok(NodeOutcome::Completed {
                    output: Value::Bool(true),
                })
            } else {
                Ok(NodeOutcome::Skipped {
                    reason: format!("condition '{pointer}' evaluated falsy"),
                })
            }
        },
        other => Err(AppError::internal(format!(
            "node type '{other}' is not runtime-handled"
        ))),
    }
}

/// Spawns one node attempt task. Per-node retry loop with backoff stays
/// inside the task; the loop only learns about the final outcome.
fn spawn_node_attempt(
    execution_id: ExecutionId,
    node: WorkflowNode,
    context: RuntimeContext,
    executors: NodeExecutorRegistry,
    semaphore: Arc<ExecutionSemaphore>,
    report_tx: mpsc::UnboundedSender<NodeAttemptReport>,
) {
    tokio::spawn(async move {
        let report = run_node_attempt(execution_id, &node, &context, &executors, &semaphore).await;
        // Best effort: receiver gone means the run already ended.
        let _ = report_tx.send(report);
    });
}

async fn run_node_attempt(
    execution_id: ExecutionId,
    node: &WorkflowNode,
    context: &RuntimeContext,
    executors: &NodeExecutorRegistry,
    semaphore: &Arc<ExecutionSemaphore>,
) -> NodeAttemptReport {
    let node_key = node.node_key.clone();
    let report = |outcome: NodeOutcome, attempts_used: u32| NodeAttemptReport {
        node_key: node_key.clone(),
        outcome,
        attempts_used,
    };
    // Bounded parallelism across the whole run.
    let _slot = match semaphore.acquire().await {
        Ok(slot) => slot,
        Err(error) => {
            return report(
                NodeOutcome::Failed {
                    failure: NodeFailure::from_app_error("semaphore_closed", &error),
                },
                0,
            );
        },
    };
    let max_attempts = node
        .capability
        .max_retries
        .saturating_add(1)
        .clamp(1, constants::MAX_ALLOWED_RETRIES + 1);
    let strategy = BackoffStrategy::Exponential {
        base_ms: constants::DEFAULT_RETRY_BACKOFF_MS,
        factor: 2,
        max_delay_ms: constants::MAX_RETRY_BACKOFF_MS,
    };
    let policy = match RetryPolicy::new(max_attempts, strategy) {
        Ok(policy) => policy,
        Err(error) => {
            return report(
                NodeOutcome::Failed {
                    failure: NodeFailure::permanent("policy_invalid", error.to_string()),
                },
                0,
            );
        },
    };

    let mut attempt = 0u32;
    loop {
        attempt += 1;
        if let Err(error) = context.guard_active() {
            return report(
                NodeOutcome::Failed {
                    failure: NodeFailure::from_app_error("context_inactive", &error),
                },
                attempt,
            );
        }
        let deadline = Deadline::after(Duration::from_millis(node.capability.timeout_ms.max(1)));
        let outcome = match deadline
            .timeout_guard("node attempt", async {
                executors
                    .execute(NodeExecutionInput {
                        execution_id,
                        step_id: ExecutionStepId::new(),
                        node,
                        input: Value::Object(context.data().clone()),
                        context,
                        attempt,
                    })
                    .await
            })
            .await
        {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => NodeOutcome::Failed {
                failure: NodeFailure::from_app_error("executor_error", &error),
            },
            Err(timeout) => NodeOutcome::Failed {
                failure: NodeFailure::retryable("node_timeout", timeout.to_string()),
            },
        };

        match &outcome {
            NodeOutcome::Failed { failure }
                if failure.retryable && attempt < policy.max_attempts =>
            {
                let backoff = strategy.delay_for(attempt + 1);
                let delay = failure
                    .retry_after_hint
                    .map_or(backoff, |hint| hint.max(backoff));
                tokio::select! {
                    biased;
                    () = context.cancellation().cancelled() => {
                        return report(
                            NodeOutcome::Failed {
                                failure: NodeFailure::permanent(
                                    "cancelled",
                                    "cancelled during retry backoff",
                                ),
                            },
                            attempt,
                        );
                    },
                    () = tokio::time::sleep(delay) => continue,
                }
            },
            _ => return report(outcome, attempt),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concurrency::ConcurrencyLimits;
    use crate::dead_letter::InMemoryDeadLetterStore;
    use crate::engine::{
        EnginePorts, EventPublisher, ExecutionStorePort, OrchestrationEngine, PolicyPort,
        QuotaPort, StartExecutionCommand, TaskStorePort, WorkflowCatalogPort,
    };
    use crate::idempotency::InMemoryIdempotencyStore;
    use crate::node_executor::NodeExecutor;
    use mas_common::enums::{Environment, ExecutionStatus, PolicyDecision};
    use mas_common::ids::{
        AgentId, ExecutionId as EId, OrganizationId, ProjectId, TaskId as TId, TenantId, WorkflowId,
    };
    use mas_domain::{Execution, NodeCapability, QuotaDimension, Task, Workflow, WorkflowEdge};

    #[derive(Debug, Default)]
    struct MemoryStores {
        tasks: Mutex<BTreeMap<TId, Task>>,
        executions: Mutex<BTreeMap<EId, Execution>>,
    }

    #[async_trait::async_trait]
    impl TaskStorePort for MemoryStores {
        async fn insert_task(&self, task: &Task) -> Result<()> {
            self.tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task.id, task.clone());
            Ok(())
        }
        async fn update_task(&self, task: &Task) -> Result<()> {
            self.tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task.id, task.clone());
            Ok(())
        }
        async fn get_task(&self, task_id: TId) -> Result<Option<Task>> {
            Ok(self
                .tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&task_id)
                .cloned())
        }
        async fn task_by_idempotency(&self, _t: TenantId, _k: &str) -> Result<Option<Task>> {
            Ok(None)
        }
        async fn list_active(&self, _t: Option<TenantId>) -> Result<Vec<Task>> {
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl ExecutionStorePort for MemoryStores {
        async fn insert_execution(&self, execution: &Execution) -> Result<()> {
            self.executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(execution.id, execution.clone());
            Ok(())
        }
        async fn update_execution(&self, execution: &Execution) -> Result<()> {
            self.executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(execution.id, execution.clone());
            Ok(())
        }
        async fn get_execution(&self, id: EId) -> Result<Option<Execution>> {
            Ok(self
                .executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&id)
                .cloned())
        }
        async fn list_active(&self, _t: Option<TenantId>) -> Result<Vec<Execution>> {
            Ok(vec![])
        }
        async fn list_children(&self, _p: EId) -> Result<Vec<Execution>> {
            Ok(vec![])
        }
    }

    #[derive(Debug, Default)]
    struct Noop;
    #[async_trait::async_trait]
    impl WorkflowCatalogPort for Noop {
        async fn get_workflow(&self, _t: TenantId, _w: WorkflowId) -> Result<Option<Workflow>> {
            Ok(None)
        }
    }
    #[async_trait::async_trait]
    impl PolicyPort for Noop {
        async fn evaluate(
            &self,
            _ctx: &RuntimeContext,
            _a: &str,
            _r: &str,
            _attr: &Value,
        ) -> Result<PolicyDecision> {
            Ok(PolicyDecision::Allow)
        }
    }
    #[async_trait::async_trait]
    impl QuotaPort for Noop {
        async fn check_and_reserve(&self, _t: TenantId, _d: QuotaDimension, _n: u64) -> Result<()> {
            Ok(())
        }
        async fn release(&self, _t: TenantId, _d: QuotaDimension, _n: u64) -> Result<()> {
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl EventPublisher for Noop {
        async fn publish(&self, _ty: &str, _id: &str, _t: TenantId, _p: Value) -> Result<()> {
            Ok(())
        }
    }

    fn test_engine() -> (
        Arc<OrchestrationEngine>,
        TenantId,
        OrganizationId,
        ProjectId,
        Arc<MemoryStores>,
    ) {
        let tenant = TenantId::new();
        let org = OrganizationId::new();
        let project = ProjectId::new();
        let stores = Arc::new(MemoryStores::default());
        let ports = EnginePorts {
            tasks: stores.clone(),
            executions: stores.clone(),
            workflows: Arc::new(Noop),
            policy: Arc::new(Noop),
            quota: Arc::new(Noop),
            events: Arc::new(Noop),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            dead_letters: Arc::new(InMemoryDeadLetterStore::new()),
        };
        let engine = OrchestrationEngine::new(ports, ConcurrencyLimits::default()).expect("engine");
        (Arc::new(engine), tenant, org, project, stores)
    }

    fn test_ctx(tenant: TenantId, org: OrganizationId, project: ProjectId) -> RuntimeContext {
        RuntimeContext::new(tenant, org, project, "tester", Environment::Development).expect("ctx")
    }

    #[derive(Debug)]
    struct RecordingTool;

    #[async_trait::async_trait]
    impl NodeExecutor for RecordingTool {
        fn handles(&self, node_type: &WorkflowNodeType) -> bool {
            matches!(node_type, WorkflowNodeType::Tool)
        }

        async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
            Ok(NodeOutcome::Completed {
                output: serde_json::json!({
                    "node": input.node.node_key,
                    "attempt": input.attempt,
                }),
            })
        }
    }

    fn simple_graph() -> WorkflowGraph {
        let nodes = vec![
            WorkflowNode::new("start", WorkflowNodeType::Start, "Start").unwrap(),
            WorkflowNode::new("tool", WorkflowNodeType::Tool, "Tool")
                .unwrap()
                .with_capability(NodeCapability {
                    timeout_ms: 5_000,
                    max_retries: 1,
                })
                .with_config(serde_json::Map::from_iter([(
                    "tool_ref".to_owned(),
                    Value::from("echo-1"),
                )])),
            WorkflowNode::new("end", WorkflowNodeType::End, "End").unwrap(),
        ];
        let edges = vec![
            WorkflowEdge::new("start", "tool").unwrap(),
            WorkflowEdge::new("tool", "end").unwrap(),
        ];
        let graph = WorkflowGraph::new(nodes, edges);
        graph.validate().expect("graph validates");
        graph
    }

    async fn start_execution(
        engine: &OrchestrationEngine,
        ctx: &RuntimeContext,
        tenant: TenantId,
        org: OrganizationId,
        project: ProjectId,
    ) -> Execution {
        engine
            .start_execution(
                ctx,
                StartExecutionCommand::for_agent(
                    tenant,
                    org,
                    project,
                    AgentId::new(),
                    serde_json::json!({"goal": "test"}),
                    "tester",
                ),
            )
            .await
            .expect("start")
    }

    #[tokio::test]
    async fn run_simple_graph_to_completion() {
        let (engine, tenant, org, project, _stores) = test_engine();
        let executors = NodeExecutorRegistry::new().with_executor(Arc::new(RecordingTool));
        let runtime = WorkflowRuntime::new(Arc::clone(&engine), executors);
        let ctx = test_ctx(tenant, org, project);
        let execution = start_execution(&engine, &ctx, tenant, org, project).await;
        let graph = simple_graph();
        let result = runtime.run(execution.id, &graph, &ctx).await.expect("run");
        assert!(matches!(result, RuntimeRunResult::Completed { .. }));
        let finished = engine
            .get_execution(&ctx, execution.id)
            .await
            .expect("fetch");
        assert_eq!(finished.status, ExecutionStatus::Completed);
        // Completion landed via engine, checkpoints were taken along the way.
        assert!(runtime.latest_checkpoint(execution.id).is_some());
    }

    #[tokio::test]
    async fn engine_cancellation_terminates_the_run() {
        let (engine, tenant, org, project, _stores) = test_engine();

        #[derive(Debug)]
        struct HangingTool;
        #[async_trait::async_trait]
        impl NodeExecutor for HangingTool {
            fn handles(&self, node_type: &WorkflowNodeType) -> bool {
                matches!(node_type, WorkflowNodeType::Tool)
            }
            async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
                // Observe cancellation cooperatively.
                input.context.cancellation().cancelled().await;
                Err(AppError::cancelled("executor observed cancellation"))
            }
        }

        let executors = NodeExecutorRegistry::new().with_executor(Arc::new(HangingTool));
        let runtime = WorkflowRuntime::new(Arc::clone(&engine), executors)
            .with_max_parallel(2)
            .with_checkpoint_interval(1);
        let ctx = test_ctx(tenant, org, project);
        let execution = start_execution(&engine, &ctx, tenant, org, project).await;
        let graph = simple_graph();

        let engine2 = Arc::clone(&engine);
        let cancel_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            engine2
                .cancel(&test_ctx(tenant, org, project), execution.id)
                .await
        });

        let result = runtime.run(execution.id, &graph, &ctx).await.expect("run");
        cancel_handle.await.expect("join").expect("cancel");
        assert!(matches!(
            result,
            RuntimeRunResult::Cancelled | RuntimeRunResult::Failed { .. }
        ));
        let finished = engine
            .get_execution(&ctx, execution.id)
            .await
            .expect("fetch");
        assert!(finished.status.is_terminal());
    }

    #[tokio::test]
    async fn missing_executor_refuses_the_run() {
        let (engine, tenant, org, project, _stores) = test_engine();
        let runtime = WorkflowRuntime::new(Arc::clone(&engine), NodeExecutorRegistry::new());
        let ctx = test_ctx(tenant, org, project);
        let execution = start_execution(&engine, &ctx, tenant, org, project).await;
        let graph = simple_graph();
        assert!(runtime.run(execution.id, &graph, &ctx).await.is_err());
    }
}
