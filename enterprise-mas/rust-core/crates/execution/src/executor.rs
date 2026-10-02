//! Executor: claims one execution and runs it end-to-end.
//!
//! The [`Executor`] is the unit a worker process owns. Per run it:
//!
//! 1. claims the execution via the distributed lock (single active runner),
//! 2. hydrates a [`ResourceGuard`] + [`StepRunner`] into the shared run scope
//!    (node adapters charge themselves against it),
//! 3. drives the orchestration `[`WorkflowRuntime`][mas_orchestration::workflow_runtime::WorkflowRuntime] over the graph,
//! 4. releases the scope and reports the full outcome: verdict, guard
//!    snapshot, and every step row produced (persistence happens through the
//!    optional [`StepStorePort`]).
//!
//! This crate deliberately contains no scheduling loop; the worker binary
//! pulls work from the dispatcher and calls [`Executor::run_execution`].

use crate::node_adapters::{RunScope, SharedScopes};
use crate::resource_guard::{GuardSnapshot, ResourceGuard};
use crate::step_runner::StepRunner;
use mas_common::error::AppError;
use mas_common::ids::ExecutionId;
use mas_common::result::Result;
use mas_domain::{ExecutionStep, ResourceLimits, TokenBudget, WorkflowGraph};
use mas_orchestration::engine::OrchestrationEngine;
use mas_orchestration::locks::DistributedLock;
use mas_orchestration::runtime_context::RuntimeContext;
use mas_orchestration::workflow_runtime::{RuntimeRunResult, WorkflowRuntime};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Executor configuration (worker-level).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutorConfig {
    /// Resource envelope for runs without a stricter execution-defined one.
    pub default_limits: ResourceLimits,
    /// Token budget per execution (`None` = unmetered).
    pub token_budget_max: Option<u64>,
    /// TTL of the single-runner lock (auto-takeover after crash).
    pub run_lock_ttl: Duration,
    /// Delay before failing a lock attempt (usually 0 — re-drive later).
    pub lock_acquire_wait: Duration,
    /// Guard charges consumed per node-level step. The step-book and the run
    /// guard both count steps; this factor lets a worker prove the step
    /// book's guard is a conservative *superset* of the outer one (outer
    /// cap × factor) so the external budget always bites first. Must be ≥ 1.
    pub step_guard_charge_per_step: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            default_limits: ResourceLimits::default(),
            token_budget_max: Some(200_000),
            run_lock_ttl: Duration::from_secs(120),
            lock_acquire_wait: Duration::ZERO,
            step_guard_charge_per_step: 1,
        }
    }
}

/// Persistence port for finished step rows.
#[async_trait::async_trait]
pub trait StepStorePort: Send + Sync + fmt::Debug {
    /// Persists the given steps (insert-or-update; ordering by sequence).
    async fn save_steps(&self, execution_id: ExecutionId, steps: &[ExecutionStep]) -> Result<()>;

    /// Reads back stored steps (recovery).
    async fn load_steps(&self, execution_id: ExecutionId) -> Result<Vec<ExecutionStep>>;
}

/// Complete picture of one run (also the worker's persistence DTO).
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub execution_id: ExecutionId,
    pub result: RuntimeRunResult,
    pub guard: GuardSnapshot,
    pub steps: Vec<ExecutionStep>,
}

impl RunOutcome {
    #[must_use]
    pub const fn succeeded(&self) -> bool {
        matches!(self.result, RuntimeRunResult::Completed { .. })
    }
}

/// The worker-side driver.
pub struct Executor {
    engine: Arc<OrchestrationEngine>,
    runtime: Arc<WorkflowRuntime>,
    scopes: SharedScopes,
    config: ExecutorConfig,
    steps: Option<Arc<dyn StepStorePort>>,
    locks: Option<Arc<dyn DistributedLock>>,
}

impl fmt::Debug for Executor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Executor")
            .field("config", &self.config)
            .field("has_step_store", &self.steps.is_some())
            .field("has_locks", &self.locks.is_some())
            .finish_non_exhaustive()
    }
}

impl Executor {
    pub fn new(
        engine: Arc<OrchestrationEngine>,
        runtime: Arc<WorkflowRuntime>,
        scopes: SharedScopes,
    ) -> Self {
        Self {
            engine,
            runtime,
            scopes,
            config: ExecutorConfig::default(),
            steps: None,
            locks: None,
        }
    }

    #[must_use]
    pub fn with_config(mut self, config: ExecutorConfig) -> Self {
        self.config = config;
        self
    }

    #[must_use]
    pub fn with_step_store(mut self, store: Arc<dyn StepStorePort>) -> Self {
        self.steps = Some(store);
        self
    }

    #[must_use]
    pub fn with_locks(mut self, locks: Arc<dyn DistributedLock>) -> Self {
        self.locks = Some(locks);
        self
    }

    #[must_use]
    pub fn scopes(&self) -> &SharedScopes {
        &self.scopes
    }

    /// Claims and runs one execution against `graph`.
    ///
    /// Returns the full outcome even for failed/cancelled runs (the worker
    /// persists from it); `Err` only surfaces *infrastructure* breakdown
    /// (lock loss, store failure, invalid setup).
    pub async fn run_execution(
        &self,
        execution_id: ExecutionId,
        graph: &WorkflowGraph,
        context: &RuntimeContext,
    ) -> Result<RunOutcome> {
        // 1. Claim.
        let lock_guard = match &self.locks {
            Some(lock) => {
                let key = format!("executor:run:{execution_id}");
                let guard = match lock
                    .acquire(
                        &key,
                        self.config.run_lock_ttl,
                        self.config.lock_acquire_wait,
                    )
                    .await
                {
                    Ok(guard) => guard,
                    Err(error) => {
                        return Err(AppError::conflict(format!(
                            "execution {execution_id} is already claimed by another runner: {error}"
                        )));
                    },
                };
                Some(guard)
            },
            None => None,
        };

        if self.config.step_guard_charge_per_step == 0 {
            return Err(AppError::invalid_field(
                "step_guard_charge_per_step",
                "out_of_range",
                "step guard charge multiplier must be ≥ 1",
            ));
        }

        // 2. Hydrate run scope. The step book's guard is a conservative
        // superset of the outer run guard so the external budget is the
        // binding constraint even if a runtime bug undercharges the outer one.
        let budget = match self.config.token_budget_max {
            Some(max) => Some(TokenBudget::new(max)?),
            None => None,
        };
        let guard = ResourceGuard::new(Some(self.config.default_limits), budget)?;
        let step_limits = self
            .config
            .default_limits
            .expand_steps_by(self.config.step_guard_charge_per_step);
        let step_runner =
            StepRunner::new(execution_id, ResourceGuard::new(Some(step_limits), None)?);
        let scope = Arc::new(RunScope::new(guard, step_runner));
        self.scopes.register(Arc::clone(&scope))?;

        // 3. Run (errors are infrastructure-level; outcomes are verdicts).
        let run_result = self.runtime.run(execution_id, graph, context).await;

        // 4. Release scope & collect state.
        let released = self.scopes.release(execution_id).unwrap_or(scope);
        let guard_snapshot = released.guard.lock().await.snapshot();
        let finished_steps = released
            .steps
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .steps();

        // 5. Release the runner claim BEFORE any store traffic takes us over
        // the lock TTL; the work is done, a takeover may proceed.
        if let (Some(guard), Some(lock)) = (lock_guard, &self.locks) {
            if let Err(error) = lock.release(&guard).await {
                tracing::warn!(%execution_id, %error, "run lock release failed");
            }
        }

        // 6. Persist steps (only after the claim is gone).
        if let (Some(store), Ok(_)) = (&self.steps, &run_result) {
            store.save_steps(execution_id, &finished_steps).await?;
        }

        // 7. Return only once no fallible work remains — the produced values
        // are complete and cannot leak.
        let result = run_result?;
        Ok(RunOutcome {
            execution_id,
            result,
            guard: guard_snapshot,
            steps: finished_steps,
        })
    }
}

impl Executor {
    /// Engine access for worker-side lifecycle calls.
    #[must_use]
    pub fn engine(&self) -> &Arc<OrchestrationEngine> {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_bridge::InMemoryAgentBridge;
    use crate::node_adapters::standard_registry;
    use crate::sandbox::{EgressPolicy, SandboxPolicy};
    use crate::tool_runner::{InMemoryToolBridge, ToolCatalogPort, ToolRunner};
    use mas_common::enums::{Environment, PolicyDecision};
    use mas_common::ids::{AgentId, OrganizationId, ProjectId, TenantId, ToolId};
    use mas_domain::QuotaDimension;
    use mas_domain::{
        SafeUrl, Tool, ToolKind, ToolRuntime, WorkflowEdge, WorkflowNode, WorkflowNodeType,
    };
    use mas_orchestration::concurrency::ConcurrencyLimits;
    use mas_orchestration::dead_letter::InMemoryDeadLetterStore;
    use mas_orchestration::engine::{
        EnginePorts, EventPublisher, ExecutionStorePort, OrchestrationEngine, PolicyPort,
        QuotaPort, StartExecutionCommand, TaskStorePort, WorkflowCatalogPort,
    };
    use mas_orchestration::idempotency::InMemoryIdempotencyStore;
    use mas_orchestration::locks::InMemoryDistributedLock;

    // -- port fakes --------------------------------------------------------------

    #[derive(Debug, Default)]
    struct MemoryStores {
        tasks: Mutex<std::collections::BTreeMap<mas_common::ids::TaskId, mas_domain::Task>>,
        executions: Mutex<std::collections::BTreeMap<ExecutionId, mas_domain::Execution>>,
        tools: Mutex<std::collections::BTreeMap<ToolId, Tool>>,
    }

    #[async_trait::async_trait]
    impl TaskStorePort for MemoryStores {
        async fn insert_task(&self, task: &mas_domain::Task) -> Result<()> {
            self.tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task.id, task.clone());
            Ok(())
        }
        async fn update_task(&self, task: &mas_domain::Task) -> Result<()> {
            self.tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task.id, task.clone());
            Ok(())
        }
        async fn get_task(
            &self,
            task_id: mas_common::ids::TaskId,
        ) -> Result<Option<mas_domain::Task>> {
            Ok(self
                .tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&task_id)
                .cloned())
        }
        async fn task_by_idempotency(
            &self,
            _t: TenantId,
            _k: &str,
        ) -> Result<Option<mas_domain::Task>> {
            Ok(None)
        }
        async fn list_active(&self, _t: Option<TenantId>) -> Result<Vec<mas_domain::Task>> {
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl ExecutionStorePort for MemoryStores {
        async fn insert_execution(&self, execution: &mas_domain::Execution) -> Result<()> {
            self.executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(execution.id, execution.clone());
            Ok(())
        }
        async fn update_execution(&self, execution: &mas_domain::Execution) -> Result<()> {
            self.executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(execution.id, execution.clone());
            Ok(())
        }
        async fn get_execution(&self, id: ExecutionId) -> Result<Option<mas_domain::Execution>> {
            Ok(self
                .executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&id)
                .cloned())
        }
        async fn list_active(&self, _t: Option<TenantId>) -> Result<Vec<mas_domain::Execution>> {
            Ok(vec![])
        }
        async fn list_children(&self, _p: ExecutionId) -> Result<Vec<mas_domain::Execution>> {
            Ok(vec![])
        }
    }

    #[async_trait::async_trait]
    impl ToolCatalogPort for MemoryStores {
        async fn get_tool(&self, _t: TenantId, tool_id: ToolId) -> Result<Option<Tool>> {
            Ok(self
                .tools
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&tool_id)
                .cloned())
        }
    }

    #[derive(Debug, Default)]
    struct Noop;
    #[async_trait::async_trait]
    impl WorkflowCatalogPort for Noop {
        async fn get_workflow(
            &self,
            _t: TenantId,
            _w: mas_common::ids::WorkflowId,
        ) -> Result<Option<mas_domain::Workflow>> {
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
            _attr: &serde_json::Value,
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
        async fn publish(
            &self,
            _ty: &str,
            _id: &str,
            _t: TenantId,
            _p: serde_json::Value,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct MemoryStepStore {
        saved: Mutex<Vec<(ExecutionId, Vec<ExecutionStep>)>>,
    }

    #[async_trait::async_trait]
    impl StepStorePort for MemoryStepStore {
        async fn save_steps(
            &self,
            execution_id: ExecutionId,
            steps: &[ExecutionStep],
        ) -> Result<()> {
            self.saved
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((execution_id, steps.to_vec()));
            Ok(())
        }
        async fn load_steps(&self, execution_id: ExecutionId) -> Result<Vec<ExecutionStep>> {
            Ok(self
                .saved
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .filter(|(id, _)| *id == execution_id)
                .flat_map(|(_, steps)| steps.clone())
                .collect())
        }
    }

    // -- helpers -----------------------------------------------------------------

    use std::sync::Mutex;

    struct TestWorld {
        executor: Executor,
        engine: Arc<OrchestrationEngine>,
        steps_store: Arc<MemoryStepStore>,
        tenant: TenantId,
        org: OrganizationId,
        project: ProjectId,
        agent_id: AgentId,
        tool_id: ToolId,
        scopes: SharedScopes,
    }

    fn world_org_placeholder() -> OrganizationId {
        OrganizationId::new()
    }

    fn world_project_placeholder() -> ProjectId {
        ProjectId::new()
    }

    fn world() -> TestWorld {
        let tenant = TenantId::new();
        let org = OrganizationId::new();
        let project = ProjectId::new();
        let stores = Arc::new(MemoryStores::default());
        let tool_id = ToolId::new();
        let mut tool = Tool::register(
            tenant,
            world_org_placeholder(),
            Some(world_project_placeholder()),
            "echo",
            ToolKind::Internal,
            serde_json::json!({"type": "object"}),
            None,
        )
        .expect("tool");
        tool.id = tool_id;
        tool.set_runtime(ToolRuntime {
            endpoint: Some(SafeUrl::parse("https://api.tools.example/echo").expect("url")),
            timeout_ms: 2_000,
            sandbox_required: false,
            settings: serde_json::Map::new(),
        })
        .expect("runtime");
        tool.publish().expect("publish");
        stores
            .tools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id, tool);

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
        let engine = Arc::new(
            OrchestrationEngine::new(ports, ConcurrencyLimits::default()).expect("engine"),
        );

        let scopes = SharedScopes::new();
        let tool_runner = Arc::new(ToolRunner::new(
            stores,
            Arc::new(Noop),
            Arc::new(InMemoryToolBridge::new()),
            SandboxPolicy {
                egress: EgressPolicy::AllowListOnly,
                allowed_hosts: std::collections::BTreeSet::from(["api.tools.example".to_owned()]),
                ..SandboxPolicy::default()
            },
        ));
        let agent_id = AgentId::new();
        let registry = standard_registry(
            Arc::new(InMemoryAgentBridge::new()),
            tool_runner,
            scopes.clone(),
        );
        let runtime = Arc::new(WorkflowRuntime::new(Arc::clone(&engine), registry));
        let steps_store = Arc::new(MemoryStepStore::default());
        let executor = Executor::new(Arc::clone(&engine), runtime, scopes.clone())
            .with_step_store(steps_store.clone())
            .with_locks(Arc::new(InMemoryDistributedLock::new()));
        TestWorld {
            executor,
            engine,
            steps_store,
            tenant,
            org,
            project,
            agent_id,
            tool_id,
            scopes,
        }
    }

    fn graph(agent_id: AgentId, tool_id: ToolId) -> WorkflowGraph {
        let nodes = vec![
            WorkflowNode::new("start", WorkflowNodeType::Start, "Start").unwrap(),
            WorkflowNode::new("agent", WorkflowNodeType::Agent, "Agent")
                .unwrap()
                .with_config(serde_json::Map::from_iter([(
                    "agent_ref".to_owned(),
                    serde_json::Value::from(agent_id.to_string()),
                )])),
            WorkflowNode::new("tool", WorkflowNodeType::Tool, "Tool")
                .unwrap()
                .with_config(serde_json::Map::from_iter([(
                    "tool_ref".to_owned(),
                    serde_json::Value::from(tool_id.to_string()),
                )])),
            WorkflowNode::new("end", WorkflowNodeType::End, "End").unwrap(),
        ];
        let edges = vec![
            WorkflowEdge::new("start", "agent").unwrap(),
            WorkflowEdge::new("agent", "tool").unwrap(),
            WorkflowEdge::new("tool", "end").unwrap(),
        ];
        let graph = WorkflowGraph::new(nodes, edges);
        graph.validate().expect("valid graph");
        graph
    }

    #[tokio::test]
    async fn executor_runs_a_real_graph_end_to_end() {
        let world = world();
        let ctx = RuntimeContext::new(
            world.tenant,
            world.org,
            world.project,
            "tester",
            Environment::Development,
        )
        .expect("ctx");
        let execution = world
            .engine
            .start_execution(
                &ctx,
                StartExecutionCommand::for_agent(
                    world.tenant,
                    world.org,
                    world.project,
                    world.agent_id,
                    serde_json::json!({"goal": "ship"}),
                    "tester",
                )
                .with_timeout(Duration::from_secs(60)),
            )
            .await
            .expect("start");
        let graph = graph(world.agent_id, world.tool_id);
        let outcome = world
            .executor
            .run_execution(execution.id, &graph, &ctx)
            .await
            .expect("run");
        assert!(outcome.succeeded(), "run failed: {:?}", outcome.result);
        assert_eq!(outcome.guard.tool_calls_used, 1);
        // agent + tool steps recorded, none failed.
        assert_eq!(outcome.steps.len(), 2);
        assert!(outcome
            .steps
            .iter()
            .all(|step| step.status == mas_common::enums::ExecutionStepStatus::Completed));
        // Steps were flushed through the store port too.
        let persisted = world
            .steps_store
            .load_steps(execution.id)
            .await
            .expect("load");
        assert_eq!(persisted.len(), 2);
        // And the engine marks the execution completed.
        let final_exec = world
            .engine
            .get_execution(&ctx, execution.id)
            .await
            .expect("fetch");
        assert_eq!(
            final_exec.status,
            mas_common::enums::ExecutionStatus::Completed
        );
        // No scopes leaked.
        assert_eq!(world.scopes.active_runs(), 0);
    }

    #[tokio::test]
    async fn second_concurrent_claim_conflicts() {
        let _world = world();
        let locks = Arc::new(InMemoryDistributedLock::new());
        let guard = locks
            .try_acquire(
                &format!("executor:run:{}", ExecutionId::new()),
                Duration::from_secs(30),
            )
            .await
            .expect("acquire")
            .expect("first claim wins");
        assert!(locks
            .try_acquire(&guard.key.clone(), Duration::from_secs(30))
            .await
            .expect("contended")
            .is_none());
    }
}
