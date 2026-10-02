//! Node-executor adapters: bind mas-execution runners into the orchestration
//! `NodeExecutorRegistry`, making workflows runnable end-to-end.
//!
//! Every adapter:
//! * obeys deadlines + cancellation through the input's
//!   [`RuntimeContext`][mas_orchestration::runtime_context::RuntimeContext],
//! * charges the shared run-scope [`ResourceGuard`] and records step rows in
//!   the shared [`StepRunner`] (one per execution, registered by
//!   [`crate::executor::Executor`] before a run starts),
//! * processes outputs through [`ResultProcessor`] (redaction + caps) and
//!   [`OutputValidator`] (portability + declared contracts).
//!
//! Scoping: adapters are shared across concurrent executions inside a
//! worker, so run scope (guard + steps) is keyed by execution id in a
//! [`SharedScopes`] registry supplied at construction; nodes without a
//! registered scope run with sensible no-op bookkeeping (unit tests).

use crate::agent_bridge::{AgentBridge, AgentRunRequest, AgentRunVerdict};
use crate::output_validator::OutputValidator;
use crate::resource_guard::ResourceGuard;
use crate::result_processor::{classify_failure, ResultProcessor};
use crate::step_runner::StepRunner;
use crate::tool_runner::{ToolInvocationReport, ToolRunner};
use mas_common::constants;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, ExecutionId, ToolId};
use mas_common::result::Result;
use mas_domain::workflow_node::WorkflowNodeType;
use mas_orchestration::node_executor::{
    NodeExecutionInput, NodeExecutor, NodeExecutorRegistry, NodeFailure, NodeOutcome,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Per-execution shared state: the guard and the step book.
///
/// The guard lives behind a *tokio* mutex because tool invocations hold it
/// across `.await` (the tool runner charges the guard mid-call); the step
/// book is only touched synchronously, so `std::sync::Mutex` suffices there.
#[derive(Debug)]
pub struct RunScope {
    pub guard: tokio::sync::Mutex<ResourceGuard>,
    pub steps: Mutex<StepRunner>,
}

impl RunScope {
    pub fn new(guard: ResourceGuard, steps: StepRunner) -> Self {
        Self {
            guard: tokio::sync::Mutex::new(guard),
            steps: Mutex::new(steps),
        }
    }
}

/// Registry of live run scopes (key = execution id).
#[derive(Debug, Clone, Default)]
pub struct SharedScopes {
    scopes: Arc<Mutex<BTreeMap<ExecutionId, Arc<RunScope>>>>,
}

impl SharedScopes {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a scope for an execution; conflict when already registered.
    pub fn register(&self, scope: Arc<RunScope>) -> Result<()> {
        let execution_id = scope
            .steps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .execution_id();
        let mut map = self.scopes.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= constants::MAX_STEPS_PER_EXECUTION as usize {
            // Pathological leak guard for a single process.
            return Err(AppError::rate_limited(
                "too many concurrent runs in this process",
            ));
        }
        map.insert(execution_id, scope);
        Ok(())
    }

    /// Removes and returns the scope (end of run; caller reads the output).
    pub fn release(&self, execution_id: ExecutionId) -> Option<Arc<RunScope>> {
        self.scopes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&execution_id)
    }

    /// Snapshot of a registered scope.
    #[must_use]
    pub fn get(&self, execution_id: ExecutionId) -> Option<Arc<RunScope>> {
        self.scopes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&execution_id)
            .cloned()
    }

    #[must_use]
    pub fn active_runs(&self) -> usize {
        self.scopes.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// Shared processing plumbing for all adapters.
#[derive(Debug, Clone)]
pub struct AdapterWiring {
    pub scopes: SharedScopes,
    pub processor: ResultProcessor,
    pub validator: OutputValidator,
}

impl AdapterWiring {
    pub fn new(scopes: SharedScopes) -> Self {
        Self {
            scopes,
            processor: ResultProcessor::default(),
            validator: OutputValidator::new(),
        }
    }

    /// Executes `f` with the guard+steps of the scope registered for
    /// `execution_id`, if any (missing scope = tests/no-op bookkeeping).
    async fn with_scope<T>(
        &self,
        execution_id: ExecutionId,
        f: impl FnOnce(&mut ResourceGuard, &mut StepRunner) -> Result<T>,
    ) -> Result<Option<T>> {
        let Some(scope) = self.scopes.get(execution_id) else {
            return Ok(None);
        };
        // Lock order is always guard → steps (deadlock-free by convention).
        let mut guard = scope.guard.lock().await;
        let mut steps = scope.steps.lock().unwrap_or_else(|e| e.into_inner());
        f(&mut guard, &mut steps).map(Some)
    }

    fn failure(&self, error: &AppError) -> NodeFailure {
        let class = classify_failure(error);
        self.processor.failure_for(class, error)
    }

    fn output_or_failure(
        &self,
        node_key: &str,
        config: &serde_json::Map<String, Value>,
        output: Value,
    ) -> std::result::Result<Value, NodeFailure> {
        self.validator
            .validate_or_error(node_key, config, &output)
            .map_err(|error| self.failure(&error))?;
        Ok(self.processor.prepare_output(&output))
    }
}

/// Resolves a `*_ref` config value into a typed id; only UUID refs are
/// supported at this layer (slug resolution belongs to the integration
/// catalog adapters).
fn resolve_uuid_ref(field: &str, value: &str) -> Result<uuid::Uuid> {
    value.parse::<uuid::Uuid>().map_err(|_| {
        AppError::invalid_field(
            field,
            "invalid_ref",
            format!(
                "'{value}' is not a UUID reference (slug-based refs resolve in the catalog layer)"
            ),
        )
    })
}

fn required_config_str(node: &mas_domain::WorkflowNode, key: &str) -> Result<String> {
    node.config
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            AppError::invalid_field(
                format!("config.{key}"),
                "required",
                format!("node '{}' requires a '{key}' config", node.node_key),
            )
        })
}

// =============================================================================
// Agent node executor
// =============================================================================

/// Executes `agent` nodes through the [`AgentBridge`].
pub struct AgentNodeExecutor {
    bridge: Arc<dyn AgentBridge>,
    wiring: AdapterWiring,
}

impl fmt::Debug for AgentNodeExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentNodeExecutor")
            .field("bridge", &self.bridge.name())
            .finish()
    }
}

impl AgentNodeExecutor {
    pub fn new(bridge: Arc<dyn AgentBridge>, wiring: AdapterWiring) -> Self {
        Self { bridge, wiring }
    }
}

#[async_trait::async_trait]
impl NodeExecutor for AgentNodeExecutor {
    fn handles(&self, node_type: &WorkflowNodeType) -> bool {
        matches!(node_type, WorkflowNodeType::Agent)
    }

    async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
        let agent_ref = required_config_str(input.node, "agent_ref")?;
        let agent_id = AgentId::from_uuid(resolve_uuid_ref("config.agent_ref", &agent_ref)?);
        let node_key = input.node.node_key.clone();
        let config = input.node.config.clone();

        // Charge the step + record the attempt up front: a blown budget must
        // stop the run before any runtime traffic.
        self.wiring
            .with_scope(input.execution_id, |guard, steps| {
                guard.charge_step()?;
                steps.begin(&node_key, "agent.run")?;
                Ok(())
            })
            .await?;

        // Build the wire request. The run budget follows the scope's guard
        // when registered, else sensible defaults.
        let (limits, budget) = self
            .wiring
            .with_scope(input.execution_id, |guard, _steps| {
                let limits = *guard.limits();
                // Bridges are charged after the fact via charge_tokens, so
                // they get an unlimited per-call budget: a successful-but-
                // expensive run must surface and then be charged, not die
                // mid-stream with its actual usage unknown.
                let budget = mas_domain::TokenBudget::unlimited();
                Ok((limits, budget))
            })
            .await?
            .unwrap_or_else(|| {
                (
                    mas_domain::ResourceLimits::default(),
                    mas_domain::TokenBudget::unlimited(),
                )
            });

        let request = AgentRunRequest::from_context(
            input.context,
            agent_id,
            input.input.clone(),
            limits,
            &budget,
        )?;

        let verdict = match self.bridge.run(request).await {
            Ok(verdict) => verdict,
            Err(error) => {
                let failure = self.wiring.failure(&error);
                let _ = self
                    .wiring
                    .with_scope(input.execution_id, |_g, steps| {
                        steps
                            .fail(&node_key, mas_domain::StepError::from_app_error(&error))
                            .map(|_| ())
                    })
                    .await?;
                return Ok(NodeOutcome::Failed { failure });
            },
        };

        match verdict {
            AgentRunVerdict::Succeeded { output, usage } => {
                self.wiring
                    .with_scope(input.execution_id, |guard, steps| {
                        // Charge tool calls first: exhaustion blocks before a
                        // token-payment top-up can mask the depletion.
                        for _ in 0..usage.tool_calls {
                            guard.charge_tool_call()?;
                        }
                        guard.charge_tokens(usage.total_tokens())?;
                        steps.succeed(&node_key, Some(format!("agent:{agent_id}")))?;
                        Ok(())
                    })
                    .await?;
                match self.wiring.output_or_failure(&node_key, &config, output) {
                    Ok(output) => Ok(NodeOutcome::Completed { output }),
                    Err(failure) => Ok(NodeOutcome::Failed { failure }),
                }
            },
            AgentRunVerdict::Failed {
                code,
                message,
                retryable,
                usage,
            } => {
                let _ = self
                    .wiring
                    .with_scope(input.execution_id, |guard, steps| {
                        if let Err(best_effort) = guard.charge_tokens(usage.total_tokens()) {
                            tracing::debug!(%best_effort, "token accounting drift after agent failure");
                        }
                        steps.fail(
                            &node_key,
                            mas_domain::StepError {
                                code: code.clone(),
                                message: message.clone(),
                                retryable,
                            },
                        )?;
                        Ok(())
                    })
                    .await?;
                Ok(NodeOutcome::Failed {
                    failure: NodeFailure {
                        code,
                        message,
                        retryable,
                        retry_after_hint: None,
                    },
                })
            },
        }
    }
}

// =============================================================================
// Tool node executor
// =============================================================================

/// Executes `tool` nodes through the [`ToolRunner`].
pub struct ToolNodeExecutor {
    runner: Arc<ToolRunner>,
    wiring: AdapterWiring,
}

impl fmt::Debug for ToolNodeExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolNodeExecutor").finish_non_exhaustive()
    }
}

impl ToolNodeExecutor {
    pub fn new(runner: Arc<ToolRunner>, wiring: AdapterWiring) -> Self {
        Self { runner, wiring }
    }
}

#[async_trait::async_trait]
impl NodeExecutor for ToolNodeExecutor {
    fn handles(&self, node_type: &WorkflowNodeType) -> bool {
        matches!(node_type, WorkflowNodeType::Tool)
    }

    async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
        let tool_ref = required_config_str(input.node, "tool_ref")?;
        let tool_id = ToolId::from_uuid(resolve_uuid_ref("config.tool_ref", &tool_ref)?);
        let action = input
            .node
            .config
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("invoke")
            .to_owned();
        let arguments = input
            .node
            .config
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| input.input.clone());
        let node_key = input.node.node_key.clone();
        let config = input.node.config.clone();

        // Steps begin even when a scope is missing (unit tests exercise the
        // adapter without an executor shell).
        self.wiring
            .with_scope(input.execution_id, |_guard, steps| {
                steps.begin(&node_key, "tool.invoke")?;
                Ok(())
            })
            .await?;

        // The runner charges the guard itself (tool call budget); we hand it
        // live access through the scope.
        let report = match self.wiring.scopes.get(input.execution_id) {
            Some(scope) => {
                let mut guard = scope.guard.lock().await;
                self.runner
                    .invoke(
                        input.context,
                        tool_id,
                        &action,
                        arguments,
                        input.execution_id,
                        input.step_id,
                        &mut guard,
                    )
                    .await
            },
            None => {
                // No scope: run with a throwaway guard (tests).
                let mut guard = ResourceGuard::new(None, None)?;
                self.runner
                    .invoke(
                        input.context,
                        tool_id,
                        &action,
                        arguments,
                        input.execution_id,
                        input.step_id,
                        &mut guard,
                    )
                    .await
            },
        };

        match report {
            Ok(ToolInvocationReport::Succeeded { output, .. }) => {
                self.wiring
                    .with_scope(input.execution_id, |_g, steps| {
                        steps.succeed(&node_key, Some(format!("tool:{tool_id}")))?;
                        Ok(())
                    })
                    .await?;
                match self.wiring.output_or_failure(&node_key, &config, output) {
                    Ok(output) => Ok(NodeOutcome::Completed { output }),
                    Err(failure) => Ok(NodeOutcome::Failed { failure }),
                }
            },
            Ok(ToolInvocationReport::Failed {
                code,
                message,
                retryable,
                ..
            }) => {
                self.wiring
                    .with_scope(input.execution_id, |_g, steps| {
                        steps.fail(
                            &node_key,
                            mas_domain::StepError {
                                code: code.clone(),
                                message: message.clone(),
                                retryable,
                            },
                        )?;
                        Ok(())
                    })
                    .await?;
                Ok(NodeOutcome::Failed {
                    failure: NodeFailure {
                        code,
                        message,
                        retryable,
                        retry_after_hint: None,
                    },
                })
            },
            Err(error) => {
                let failure = self.wiring.failure(&error);
                let _ = self
                    .wiring
                    .with_scope(input.execution_id, |_g, steps| {
                        steps
                            .fail(&node_key, mas_domain::StepError::from_app_error(&error))
                            .map(|_| ())
                    })
                    .await?;
                // Policy denials are permanent at the attempt level; the
                // classifier's retry flag on the failure governs the runtime
                // retry loop.
                Ok(NodeOutcome::Failed { failure })
            },
        }
    }
}

// =============================================================================
// Delay node executor
// =============================================================================

/// Executes `delay` nodes: stays cheap by deferring through the runtime
/// instead of sleeping (the workflow runtime wakes the branch at `wake_at`).
#[derive(Debug, Clone, Default)]
pub struct DelayNodeExecutor;

#[async_trait::async_trait]
impl NodeExecutor for DelayNodeExecutor {
    fn handles(&self, node_type: &WorkflowNodeType) -> bool {
        matches!(node_type, WorkflowNodeType::Delay)
    }

    async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
        let duration_ms = input
            .node
            .config
            .get("duration_ms")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                AppError::invalid_field(
                    "config.duration_ms",
                    "required",
                    "delay nodes require an integer duration_ms",
                )
            })?;
        if duration_ms == 0 || duration_ms > constants::MAX_EXECUTION_DURATION_MS {
            return Err(AppError::invalid_field(
                "config.duration_ms",
                "out_of_range",
                "delay must be within the execution duration limit",
            ));
        }
        Ok(NodeOutcome::Deferred {
            wake_after: Duration::from_millis(duration_ms),
            reason: format!(
                "delay node '{}' sleeps {duration_ms} ms",
                input.node.node_key
            ),
        })
    }
}

// =============================================================================
// Approval node executor
// =============================================================================

/// Executes `approval` nodes: deterministically ref'd `WaitingApproval`
/// outcomes; resolution lands with the approvals service.
#[derive(Debug, Clone, Default)]
pub struct ApprovalNodeExecutor;

#[async_trait::async_trait]
impl NodeExecutor for ApprovalNodeExecutor {
    fn handles(&self, node_type: &WorkflowNodeType) -> bool {
        matches!(node_type, WorkflowNodeType::Approval)
    }

    async fn execute(&self, input: NodeExecutionInput<'_>) -> Result<NodeOutcome> {
        let approver_role = required_config_str(input.node, "approver_role")?;
        Ok(NodeOutcome::WaitingApproval {
            approval_ref: format!(
                "approval:{}:{}:{}",
                input.execution_id, input.node.node_key, approver_role
            ),
        })
    }
}

// =============================================================================
// Standard wiring
// =============================================================================

/// Builds the standard [`NodeExecutorRegistry`]: agent + tool + delay +
/// approval adapters over the supplied runtimes and shared scopes.
pub fn standard_registry(
    agent_bridge: Arc<dyn AgentBridge>,
    tool_runner: Arc<ToolRunner>,
    scopes: SharedScopes,
) -> NodeExecutorRegistry {
    let wiring = AdapterWiring::new(scopes);
    NodeExecutorRegistry::new()
        .with_executor(Arc::new(AgentNodeExecutor::new(
            agent_bridge,
            wiring.clone(),
        )))
        .with_executor(Arc::new(ToolNodeExecutor::new(tool_runner, wiring)))
        .with_executor(Arc::new(DelayNodeExecutor))
        .with_executor(Arc::new(ApprovalNodeExecutor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_bridge::{AgentUsage, InMemoryAgentBridge};
    use crate::tool_runner::{InMemoryToolBridge, ToolCatalogPort};
    use mas_common::enums::{Environment, PolicyDecision};
    use mas_common::ids::{ExecutionStepId, OrganizationId, ProjectId, TenantId};
    use mas_domain::{SafeUrl, Tool, ToolKind, ToolRuntime, WorkflowNode};
    use mas_orchestration::engine::PolicyPort;
    use mas_orchestration::runtime_context::RuntimeContext;
    use std::collections::HashMap;

    #[derive(Debug, Default)]
    struct MemoryCatalog {
        tools: Mutex<HashMap<ToolId, Tool>>,
    }

    #[async_trait::async_trait]
    impl ToolCatalogPort for MemoryCatalog {
        async fn get_tool(&self, _t: TenantId, tool_id: ToolId) -> Result<Option<Tool>> {
            Ok(self
                .tools
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&tool_id)
                .cloned())
        }
    }

    fn echo_tool(tenant: TenantId, tool_id: ToolId) -> Tool {
        // Tool ids are persistence-assigned; construct via a throwaway row
        // then swap in the deterministic id the test expects.
        let mut tool = Tool::register(
            tenant,
            OrganizationId::new(),
            Some(ProjectId::new()),
            format!("echo-{tool_id}"),
            ToolKind::Internal,
            serde_json::json!({"type": "object"}),
            None,
        )
        .expect("tool");
        tool.id = tool_id;
        tool.kind = ToolKind::Http;
        let runtime = ToolRuntime {
            endpoint: Some(SafeUrl::parse("https://api.tools.example/echo").expect("url")),
            timeout_ms: 2_000,
            sandbox_required: false,
            settings: serde_json::Map::new(),
        };
        tool.set_runtime(runtime).expect("runtime");
        tool.publish().expect("publish");
        tool
    }

    #[derive(Debug)]
    struct AllowAllPolicy;

    #[async_trait::async_trait]
    impl PolicyPort for AllowAllPolicy {
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

    fn ctx(tenant: TenantId) -> RuntimeContext {
        RuntimeContext::new(
            tenant,
            OrganizationId::new(),
            ProjectId::new(),
            "tester",
            Environment::Development,
        )
        .expect("ctx")
    }

    fn scope_for(execution_id: ExecutionId) -> (SharedScopes, Arc<RunScope>) {
        let scopes = SharedScopes::new();
        let runner = StepRunner::new(execution_id, ResourceGuard::new(None, None).expect("guard"));
        let scope = Arc::new(RunScope::new(
            ResourceGuard::new(None, None).expect("guard"),
            runner,
        ));
        scopes.register(scope.clone()).expect("register");
        (scopes, scope)
    }

    #[tokio::test]
    async fn agent_node_runs_and_charges_the_scope() {
        let tenant = TenantId::new();
        let execution_id = ExecutionId::new();
        let (scopes, scope) = scope_for(execution_id);
        let agent_id = AgentId::new();
        let bridge = Arc::new(InMemoryAgentBridge::new().with_handler(move |_request| {
            Ok(AgentRunVerdict::Succeeded {
                output: serde_json::json!({"answer": 42}),
                usage: AgentUsage {
                    tokens_input: 10,
                    tokens_output: 5,
                    tool_calls: 0,
                    steps: 1,
                    active_time_ms: 3,
                },
            })
        }));
        let executor = AgentNodeExecutor::new(bridge, AdapterWiring::new(scopes));
        let node = WorkflowNode::new("agent-1", WorkflowNodeType::Agent, "Agent")
            .expect("node")
            .with_config(serde_json::Map::from_iter([(
                "agent_ref".to_owned(),
                Value::from(agent_id.to_string()),
            )]));
        let context = ctx(tenant);
        let outcome = executor
            .execute(NodeExecutionInput {
                execution_id,
                step_id: ExecutionStepId::new(),
                node: &node,
                input: serde_json::json!({"q": "?"}),
                context: &context,
                attempt: 1,
            })
            .await
            .expect("execute");
        match outcome {
            NodeOutcome::Completed { output } => assert_eq!(output["answer"], 42),
            other => panic!("expected completion, got {other:?}"),
        }
        let snapshot = scope.guard.lock().await.snapshot();
        assert_eq!(snapshot.tokens_consumed, 15);
        let steps = scope
            .steps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .steps();
        assert_eq!(steps.len(), 1);
        assert_eq!(
            steps[0].status,
            mas_common::enums::ExecutionStepStatus::Completed
        );
    }

    #[tokio::test]
    async fn tool_node_maps_bridge_outcomes() {
        let tenant = TenantId::new();
        let execution_id = ExecutionId::new();
        let (scopes, _scope) = scope_for(execution_id);
        let tool_id = ToolId::new();
        let catalog = Arc::new(MemoryCatalog::default());
        catalog
            .tools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id, echo_tool(tenant, tool_id));
        let runner = Arc::new(ToolRunner::new(
            catalog,
            Arc::new(AllowAllPolicy),
            Arc::new(InMemoryToolBridge::new()),
            crate::sandbox::SandboxPolicy {
                egress: crate::sandbox::EgressPolicy::Unrestricted,
                ..crate::sandbox::SandboxPolicy::default()
            },
        ));
        let executor = ToolNodeExecutor::new(runner, AdapterWiring::new(scopes));
        let node = WorkflowNode::new("tool-1", WorkflowNodeType::Tool, "Tool")
            .expect("node")
            .with_config(serde_json::Map::from_iter([(
                "tool_ref".to_owned(),
                Value::from(tool_id.to_string()),
            )]));
        let context = ctx(tenant);
        let outcome = executor
            .execute(NodeExecutionInput {
                execution_id,
                step_id: ExecutionStepId::new(),
                node: &node,
                input: Value::Null,
                context: &context,
                attempt: 1,
            })
            .await
            .expect("execute");
        assert!(outcome.is_completed());

        // A node whose tool is unknown fails the node (not the process).
        let missing = WorkflowNode::new("tool-2", WorkflowNodeType::Tool, "Tool")
            .expect("node")
            .with_config(serde_json::Map::from_iter([(
                "tool_ref".to_owned(),
                Value::from(ToolId::new().to_string()),
            )]));
        let outcome = executor
            .execute(NodeExecutionInput {
                execution_id,
                step_id: ExecutionStepId::new(),
                node: &missing,
                input: Value::Null,
                context: &context,
                attempt: 1,
            })
            .await
            .expect("adapter maps runner Err to a failed outcome");
        match outcome {
            NodeOutcome::Failed { failure } => {
                assert!(
                    failure.code.contains("not_found"),
                    "unexpected failure code: {}",
                    failure.code
                );
            },
            other => panic!("expected Failed outcome, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn delay_and_approval_are_declarative() {
        let delay = DelayNodeExecutor;
        let node = WorkflowNode::new("delay-1", WorkflowNodeType::Delay, "Delay")
            .expect("node")
            .with_config(serde_json::Map::from_iter([(
                "duration_ms".to_owned(),
                Value::from(250u64),
            )]));
        let tenant = TenantId::new();
        let context = ctx(tenant);
        let outcome = delay
            .execute(NodeExecutionInput {
                execution_id: ExecutionId::new(),
                step_id: ExecutionStepId::new(),
                node: &node,
                input: Value::Null,
                context: &context,
                attempt: 1,
            })
            .await
            .expect("delay");
        match outcome {
            NodeOutcome::Deferred { wake_after, .. } => {
                assert_eq!(wake_after, Duration::from_millis(250));
            },
            other => panic!("expected deferred, got {other:?}"),
        }

        let approval = ApprovalNodeExecutor;
        let node = WorkflowNode::new("appr-1", WorkflowNodeType::Approval, "Approval")
            .expect("node")
            .with_config(serde_json::Map::from_iter([(
                "approver_role".to_owned(),
                Value::from("ops-lead"),
            )]));
        let outcome = approval
            .execute(NodeExecutionInput {
                execution_id: ExecutionId::new(),
                step_id: ExecutionStepId::new(),
                node: &node,
                input: Value::Null,
                context: &context,
                attempt: 1,
            })
            .await
            .expect("approval");
        match outcome {
            NodeOutcome::WaitingApproval { approval_ref } => {
                assert!(approval_ref.contains("ops-lead"));
            },
            other => panic!("expected waiting, got {other:?}"),
        }
    }

    #[test]
    fn registry_covers_standard_nodes() {
        let scopes = SharedScopes::new();
        let catalog = Arc::new(MemoryCatalog::default());
        let runner = Arc::new(ToolRunner::new(
            catalog,
            Arc::new(AllowAllPolicy),
            Arc::new(InMemoryToolBridge::new()),
            crate::sandbox::SandboxPolicy::default(),
        ));
        let registry = standard_registry(Arc::new(InMemoryAgentBridge::new()), runner, scopes);
        assert_eq!(registry.len(), 4);
        assert!(registry.executor_for(&WorkflowNodeType::Agent).is_some());
        assert!(registry.executor_for(&WorkflowNodeType::Approval).is_some());
        assert!(registry.executor_for(&WorkflowNodeType::Delay).is_some());
        assert!(registry.executor_for(&WorkflowNodeType::Tool).is_some());
    }
}
