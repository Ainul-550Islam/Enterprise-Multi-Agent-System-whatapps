//! The orchestration engine: all lifecycle entry points of the platform.
//!
//! The engine is a **coordination shell**: every state transition goes
//! through the domain aggregates ([`Task`], [`Execution`]), every side effect
//! (durable state, policy, quota, idempotency, dead letters, events) goes
//! through an injected port. Nothing here knows about SQL or NATS.
//!
//! Ordering of checks at intake (fail fast, cheapest first):
//! 1. runtime context guards (tenant match, cancellation, deadline),
//! 2. policy evaluation (deny ⇒ forbidden),
//! 3. idempotency reservation (replay short-circuits),
//! 4. quota + concurrency gates,
//! 5. aggregate construction, persistence, events.
//!
//! Event publication is best-effort here (`warn!` + continue): durability of
//! the event stream is provided by the outbox-backed publisher wired at a
//! higher layer; losing the process between store-write and publish is safe
//! because recovery reconciles from the authoritative stores.

use crate::cancellation::CancellationToken;
use crate::concurrency::{ConcurrencyLimits, ConcurrencyPermit, TenantConcurrencyLimiter};
use crate::coordinator::ExecutionCoordinator;
use crate::dead_letter::DeadLetterStore;
use crate::idempotency::{IdempotencyKey, IdempotencyStore, Reservation};
use crate::retry::{RetryDecision, RetryPolicy};
use crate::runtime_context::RuntimeContext;
use crate::timeout::TimeoutPolicy;
use mas_common::enums::{ExecutionStatus, PolicyDecision, TaskPriority, TaskStatus};
use mas_common::error::AppError;
use mas_common::ids::{AgentId, ExecutionId, TaskId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{Execution, QuotaDimension, Task, Workflow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::warn;

// =============================================================================
// Commands
// =============================================================================

/// Everything needed to submit one task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTaskCommand {
    pub tenant_id: TenantId,
    pub organization_id: mas_common::ids::OrganizationId,
    pub project_id: mas_common::ids::ProjectId,
    /// Exactly one of agent/workflow addresses the runtime (enforced).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    pub operation: String,
    #[serde(default)]
    pub input: Value,
    pub priority: TaskPriority,
    /// Client-supplied duplicate-submission guard, unique per tenant.
    pub idempotency_key: String,
    /// How the task was caused (engine, schedule, API request…).
    pub created_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    /// Owning execution when engine-spawned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
}

impl NewTaskCommand {
    /// Targets an agent (`agent.run`).
    pub fn agent_run(
        tenant_id: TenantId,
        organization_id: mas_common::ids::OrganizationId,
        project_id: mas_common::ids::ProjectId,
        agent_id: AgentId,
        input: Value,
        created_by: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            organization_id,
            project_id,
            agent_id: Some(agent_id),
            workflow_id: None,
            operation: "agent.run".to_owned(),
            input,
            priority: TaskPriority::Normal,
            idempotency_key: uuid::Uuid::now_v7().to_string(),
            created_by: created_by.into(),
            deadline: None,
            execution_id: None,
        }
    }

    #[must_use]
    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = operation.into();
        self
    }

    #[must_use]
    pub const fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    #[must_use]
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = key.into();
        self
    }

    #[must_use]
    pub const fn with_deadline(mut self, deadline: Timestamp) -> Self {
        self.deadline = Some(deadline);
        self
    }

    #[must_use]
    pub const fn with_execution(mut self, execution_id: ExecutionId) -> Self {
        self.execution_id = Some(execution_id);
        self
    }
}

/// Everything needed to launch one execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartExecutionCommand {
    pub tenant_id: TenantId,
    pub organization_id: mas_common::ids::OrganizationId,
    pub project_id: mas_common::ids::ProjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<WorkflowId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
    #[serde(default)]
    pub input: Value,
    /// End-to-end trace key; defaults to a fresh UUIDv7.
    pub correlation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub created_by: String,
    /// Requested execution timeout (clamped by [`TimeoutPolicy`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Duration>,
    /// Parent execution when this run is agent delegation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<ExecutionId>,
}

impl StartExecutionCommand {
    /// Launches a workflow run.
    pub fn for_workflow(
        tenant_id: TenantId,
        organization_id: mas_common::ids::OrganizationId,
        project_id: mas_common::ids::ProjectId,
        workflow_id: WorkflowId,
        input: Value,
        created_by: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            organization_id,
            project_id,
            workflow_id: Some(workflow_id),
            agent_id: None,
            input,
            correlation_id: uuid::Uuid::now_v7().to_string(),
            idempotency_key: None,
            created_by: created_by.into(),
            timeout: None,
            parent_execution_id: None,
        }
    }

    /// Launches an agent run.
    pub fn for_agent(
        tenant_id: TenantId,
        organization_id: mas_common::ids::OrganizationId,
        project_id: mas_common::ids::ProjectId,
        agent_id: AgentId,
        input: Value,
        created_by: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id,
            organization_id,
            project_id,
            workflow_id: None,
            agent_id: Some(agent_id),
            input,
            correlation_id: uuid::Uuid::now_v7().to_string(),
            idempotency_key: None,
            created_by: created_by.into(),
            timeout: None,
            parent_execution_id: None,
        }
    }

    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = correlation_id.into();
        self
    }

    #[must_use]
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub const fn with_parent(mut self, parent_execution_id: ExecutionId) -> Self {
        self.parent_execution_id = Some(parent_execution_id);
        self
    }
}

// =============================================================================
// Ports
// =============================================================================

/// Durable task store (PostgreSQL in production).
#[async_trait::async_trait]
pub trait TaskStorePort: Send + Sync + fmt::Debug {
    /// Inserts a new task; conflicts on duplicate id or `(tenant, idempotency_key)`.
    async fn insert_task(&self, task: &Task) -> Result<()>;
    /// Persists a state transition of an existing task.
    async fn update_task(&self, task: &Task) -> Result<()>;
    async fn get_task(&self, task_id: TaskId) -> Result<Option<Task>>;
    /// Idempotent replay lookup.
    async fn task_by_idempotency(&self, tenant_id: TenantId, key: &str) -> Result<Option<Task>>;
    /// All non-terminal tasks (recovery scan).
    async fn list_active(&self, tenant_id: Option<TenantId>) -> Result<Vec<Task>>;
}

/// Durable execution store.
#[async_trait::async_trait]
pub trait ExecutionStorePort: Send + Sync + fmt::Debug {
    async fn insert_execution(&self, execution: &Execution) -> Result<()>;
    async fn update_execution(&self, execution: &Execution) -> Result<()>;
    async fn get_execution(&self, execution_id: ExecutionId) -> Result<Option<Execution>>;
    /// All non-terminal executions (recovery scan).
    async fn list_active(&self, tenant_id: Option<TenantId>) -> Result<Vec<Execution>>;
    /// Direct children of an execution (tree rebuild on recovery).
    async fn list_children(&self, parent_execution_id: ExecutionId) -> Result<Vec<Execution>>;
}

/// Read-side of the workflow catalog.
#[async_trait::async_trait]
pub trait WorkflowCatalogPort: Send + Sync + fmt::Debug {
    /// Tenant-scoped fetch (cross-tenant looks are impossible by contract).
    async fn get_workflow(
        &self,
        tenant_id: TenantId,
        workflow_id: WorkflowId,
    ) -> Result<Option<Workflow>>;
}

/// Policy evaluation port (PDP in the policy crate).
#[async_trait::async_trait]
pub trait PolicyPort: Send + Sync + fmt::Debug {
    /// Evaluates `action` against `resource` under the caller's context.
    async fn evaluate(
        &self,
        context: &RuntimeContext,
        action: &str,
        resource: &str,
        attributes: &Value,
    ) -> Result<PolicyDecision>;
}

/// Quota reservation port (atomic counters in the quota crate).
#[async_trait::async_trait]
pub trait QuotaPort: Send + Sync + fmt::Debug {
    /// Reserves `amount` of a dimension or fails (`RATE_LIMITED`).
    async fn check_and_reserve(
        &self,
        tenant_id: TenantId,
        dimension: QuotaDimension,
        amount: u64,
    ) -> Result<()>;

    /// Releases a previous reservation (terminal-state cleanup).
    async fn release(
        &self,
        tenant_id: TenantId,
        dimension: QuotaDimension,
        amount: u64,
    ) -> Result<()>;
}

/// Downstream event stream publisher.
#[async_trait::async_trait]
pub trait EventPublisher: Send + Sync + fmt::Debug {
    /// Publishes a domain event. Payload should carry ids/status, not bodies.
    async fn publish(
        &self,
        event_type: &str,
        aggregate_id: &str,
        tenant_id: TenantId,
        payload: Value,
    ) -> Result<()>;
}

/// All injected infrastructure of the engine, one struct.
pub struct EnginePorts {
    pub tasks: Arc<dyn TaskStorePort>,
    pub executions: Arc<dyn ExecutionStorePort>,
    pub workflows: Arc<dyn WorkflowCatalogPort>,
    pub policy: Arc<dyn PolicyPort>,
    pub quota: Arc<dyn QuotaPort>,
    pub events: Arc<dyn EventPublisher>,
    pub idempotency: Arc<dyn IdempotencyStore>,
    pub dead_letters: Arc<dyn DeadLetterStore>,
}

impl fmt::Debug for EnginePorts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnginePorts").finish_non_exhaustive()
    }
}

impl Clone for EnginePorts {
    fn clone(&self) -> Self {
        Self {
            tasks: Arc::clone(&self.tasks),
            executions: Arc::clone(&self.executions),
            workflows: Arc::clone(&self.workflows),
            policy: Arc::clone(&self.policy),
            quota: Arc::clone(&self.quota),
            events: Arc::clone(&self.events),
            idempotency: Arc::clone(&self.idempotency),
            dead_letters: Arc::clone(&self.dead_letters),
        }
    }
}

// =============================================================================
// Reports & outcomes
// =============================================================================

/// What happened to a recovered/retried task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Task requeued; caller schedules dispatch `delay` from now.
    Requeued {
        task_id: TaskId,
        attempt: u32,
        delay: Duration,
    },
    /// Attempt budget exhausted or non-retryable; task is in the DLQ.
    DeadLettered {
        task_id: TaskId,
        record_id: uuid::Uuid,
    },
}

/// Summary of one `recover()` run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryReport {
    /// Non-terminal executions found in the store.
    pub executions_scanned: u32,
    /// Executions (re)registered with the coordinator.
    pub executions_registered: u32,
    /// Non-terminal tasks found in the store.
    pub tasks_scanned: u32,
    /// Queued tasks re-advertised for scheduling.
    pub tasks_requeued: u32,
    /// Running tasks whose worker died; moved back to queued per policy.
    pub running_tasks_recovered: u32,
    /// Tasks that exhausted attempts during recovery.
    pub tasks_dead_lettered: u32,
    /// Store rows that failed to load/transition (kept going).
    pub errors: Vec<String>,
}

// =============================================================================
// Engine
// =============================================================================

/// The orchestration engine.
pub struct OrchestrationEngine {
    ports: EnginePorts,
    coordinator: ExecutionCoordinator,
    concurrency: TenantConcurrencyLimiter,
    retry_policy: RetryPolicy,
    timeout_policy: TimeoutPolicy,
    idempotency_ttl: Duration,
    /// Live concurrency permits, released at terminal transitions.
    permits: Mutex<HashMap<ExecutionId, ConcurrencyPermit>>,
    /// Cancellation tokens of in-flight executions owned by this process.
    cancellers: Mutex<HashMap<ExecutionId, CancellationToken>>,
}

impl fmt::Debug for OrchestrationEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrchestrationEngine")
            .field("coordinator", &self.coordinator)
            .finish_non_exhaustive()
    }
}

impl OrchestrationEngine {
    pub fn new(ports: EnginePorts, concurrency_limits: ConcurrencyLimits) -> Result<Self> {
        concurrency_limits.validate()?;
        Ok(Self {
            ports,
            coordinator: ExecutionCoordinator::new(),
            concurrency: TenantConcurrencyLimiter::new(concurrency_limits)?,
            retry_policy: RetryPolicy::default(),
            timeout_policy: TimeoutPolicy::default(),
            idempotency_ttl: Duration::from_secs(24 * 3600),
            permits: Mutex::new(HashMap::new()),
            cancellers: Mutex::new(HashMap::new()),
        })
    }

    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    #[must_use]
    pub const fn with_timeout_policy(mut self, policy: TimeoutPolicy) -> Self {
        self.timeout_policy = policy;
        self
    }

    #[must_use]
    pub const fn with_idempotency_ttl(mut self, ttl: Duration) -> Self {
        self.idempotency_ttl = ttl;
        self
    }

    #[must_use]
    pub fn coordinator(&self) -> &ExecutionCoordinator {
        &self.coordinator
    }

    /// The live cancellation token of an execution owned by this process
    /// (`None` when the run lives elsewhere or hasn't started here).
    #[must_use]
    pub fn cancellation_token(&self, execution_id: ExecutionId) -> Option<CancellationToken> {
        self.cancellers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&execution_id)
            .cloned()
    }

    #[must_use]
    pub const fn ports(&self) -> &EnginePorts {
        &self.ports
    }

    // -- task lifecycle ------------------------------------------------------

    /// Validates + creates (or replays) + queues a task.
    ///
    /// Replay semantics: the same `(tenant, idempotency_key)` returns the
    /// previously created task — never a duplicate insert.
    pub async fn submit_task(&self, ctx: &RuntimeContext, command: NewTaskCommand) -> Result<Task> {
        self.guard_intake(ctx, command.tenant_id)?;
        self.evaluate_policy(ctx, "task.submit", &command.operation, &command.input)
            .await?;

        let key = IdempotencyKey::new(&command.idempotency_key)?;
        match self
            .ports
            .idempotency
            .reserve(command.tenant_id, &key, self.idempotency_ttl)
            .await?
        {
            Reservation::Acquired => {},
            Reservation::ReplayCompleted { result_ref } => {
                return self.replay_task_ref(&command, &result_ref).await;
            },
            Reservation::ConflictInFlight => {
                return Err(AppError::conflict(
                    "a request with this idempotency key is already in flight",
                ));
            },
        }

        let mut task = Task::new(
            command.tenant_id,
            command.organization_id,
            command.project_id,
            command.agent_id,
            command.workflow_id,
            &command.operation,
            command.input.clone(),
            command.priority,
            &command.idempotency_key,
            &command.created_by,
        )?;
        task.deadline = command.deadline;
        task.execution_id = command.execution_id;

        self.ports.tasks.insert_task(&task).await?;
        task.queue()?;
        self.ports.tasks.update_task(&task).await?;

        self.publish(
            "task.queued",
            &task.id.to_string(),
            task.tenant_id,
            serde_json::json!({
                "task_id": task.id,
                "operation": task.operation,
                "priority": task.priority.as_str(),
                "attempt_budget": task.max_attempts,
            }),
        )
        .await;
        self.ports
            .idempotency
            .complete(command.tenant_id, &key, &format!("task:{}", task.id))
            .await?;
        Ok(task)
    }

    /// Loads a task (tenant-checked by the caller's context).
    pub async fn get_task(&self, ctx: &RuntimeContext, task_id: TaskId) -> Result<Task> {
        let task = self
            .ports
            .tasks
            .get_task(task_id)
            .await?
            .ok_or_else(|| AppError::not_found("task", task_id.to_string()))?;
        self.guard_tenant(ctx, task.tenant_id)?;
        Ok(task)
    }

    /// Cancels a non-terminal task.
    pub async fn cancel_task(&self, ctx: &RuntimeContext, task_id: TaskId) -> Result<Task> {
        let mut task = self.get_task(ctx, task_id).await?;
        task.cancel()?;
        self.ports.tasks.update_task(&task).await?;
        self.publish(
            "task.cancelled",
            &task.id.to_string(),
            task.tenant_id,
            serde_json::json!({ "task_id": task.id }),
        )
        .await;
        Ok(task)
    }

    /// Applies the retry policy to a failed attempt: requeue with backoff, or
    /// route to the dead-letter store when exhausted/non-retryable.
    pub async fn retry_task(
        &self,
        ctx: &RuntimeContext,
        task_id: TaskId,
        error: &AppError,
    ) -> Result<RetryOutcome> {
        let mut task = self.get_task(ctx, task_id).await?;
        if task.status == TaskStatus::Running {
            // The worker reports the failed attempt; the engine owns the verdict.
            task.fail(error.public_message())?;
        }
        match self.retry_policy.decision(task.attempt_count, error) {
            RetryDecision::Retry { delay } => {
                task.retry()?;
                self.ports.tasks.update_task(&task).await?;
                self.publish(
                    "task.retry_scheduled",
                    &task.id.to_string(),
                    task.tenant_id,
                    serde_json::json!({
                        "task_id": task.id,
                        "attempt": task.attempt_count + 1,
                        "delay_ms": delay.as_millis(),
                    }),
                )
                .await;
                Ok(RetryOutcome::Requeued {
                    task_id: task.id,
                    attempt: task.attempt_count + 1,
                    delay,
                })
            },
            RetryDecision::DoNotRetry | RetryDecision::Exhausted => {
                let reason = if task.remaining_attempts() == 0 {
                    "attempts_exhausted"
                } else {
                    "non_retryable_error"
                };
                self.dead_letter(&mut task, error, reason).await?;
                Ok(RetryOutcome::DeadLettered {
                    task_id: task.id,
                    record_id: self
                        .last_record_for(&task)
                        .await
                        .unwrap_or(uuid::Uuid::nil()),
                })
            },
        }
    }

    async fn dead_letter(&self, task: &mut Task, error: &AppError, reason: &str) -> Result<()> {
        // The task must be Failed to leave; workers fail their attempt first,
        // but tolerate a Running task that never got marked by failing it here.
        match task.status {
            TaskStatus::Running => task.fail(error.public_message())?,
            TaskStatus::Failed => {},
            other => {
                return Err(AppError::conflict(format!(
                    "task {} in status '{other}' cannot be dead-lettered",
                    task.id
                )));
            },
        }
        let record = self
            .ports
            .dead_letters
            .enqueue_dead_letter(
                task.clone(),
                error.error_code(),
                &error.public_message(),
                reason,
            )
            .await?;
        task.dead_letter(reason)?;
        self.ports.tasks.update_task(task).await?;
        self.publish(
            "task.dead_lettered",
            &task.id.to_string(),
            task.tenant_id,
            serde_json::json!({
                "task_id": task.id,
                "record_id": record.id,
                "error_code": error.error_code(),
            }),
        )
        .await;
        Ok(())
    }

    /// Recovery-time decision for a task whose `Failed` state is already
    /// persisted (no reload, no re-fail). `Ok(true)` = requeued,
    /// `Ok(false)` = dead-lettered.
    async fn retry_task_direct(&self, task: &mut Task, error: &AppError) -> Result<bool> {
        match self.retry_policy.decision(task.attempt_count, error) {
            RetryDecision::Retry { .. } => {
                task.retry()?;
                self.ports.tasks.update_task(task).await?;
                Ok(true)
            },
            RetryDecision::DoNotRetry | RetryDecision::Exhausted => {
                self.dead_letter(task, error, "recovery_exhausted_attempts")
                    .await?;
                Ok(false)
            },
        }
    }

    async fn last_record_for(&self, task: &Task) -> Option<uuid::Uuid> {
        self.ports
            .dead_letters
            .list_task_records(None)
            .await
            .ok()?
            .into_iter()
            .find(|r| r.task.id == task.id)
            .map(|r| r.id)
    }

    async fn replay_task_ref(&self, command: &NewTaskCommand, result_ref: &str) -> Result<Task> {
        if let Some(id) = result_ref.strip_prefix("task:") {
            let task_id = id
                .parse::<uuid::Uuid>()
                .map(TaskId::from_uuid)
                .map_err(|_| AppError::internal("corrupt idempotency reference"))?;
            if let Some(task) = self.ports.tasks.get_task(task_id).await? {
                if task.tenant_id == command.tenant_id {
                    return Ok(task);
                }
            }
        }
        // Broken reference — treat as fresh work rather than panic.
        Err(AppError::conflict(format!(
            "idempotency reference '{result_ref}' cannot be replayed; retry with a new key"
        )))
    }

    // -- execution lifecycle ---------------------------------------------------

    /// Creates an execution (Pending) after full intake gating.
    ///
    /// The execution transitions to `Running` via
    /// [`OrchestrationEngine::mark_execution_started`] once a worker picks it
    /// up; the concurrency permit is already held from here on.
    pub async fn start_execution(
        &self,
        ctx: &RuntimeContext,
        command: StartExecutionCommand,
    ) -> Result<Execution> {
        self.guard_intake(ctx, command.tenant_id)?;
        let resource = command
            .workflow_id
            .map(|id| format!("workflow:{id}"))
            .or_else(|| command.agent_id.map(|id| format!("agent:{id}")))
            .unwrap_or_else(|| "execution".to_owned());
        self.evaluate_policy(ctx, "execution.start", &resource, &command.input)
            .await?;

        // Workflow target must exist, be visible to the tenant and runnable.
        if let Some(workflow_id) = command.workflow_id {
            let workflow = self
                .ports
                .workflows
                .get_workflow(command.tenant_id, workflow_id)
                .await?
                .ok_or_else(|| AppError::not_found("workflow", workflow_id.to_string()))?;
            if !workflow.is_executable() {
                return Err(AppError::conflict(format!(
                    "workflow '{workflow_id}' is not executable in status '{}'",
                    workflow.status
                )));
            }
        }

        if let Some(key_text) = &command.idempotency_key {
            let key = IdempotencyKey::new(key_text)?;
            match self
                .ports
                .idempotency
                .reserve(command.tenant_id, &key, self.idempotency_ttl)
                .await?
            {
                Reservation::Acquired => {},
                Reservation::ReplayCompleted { result_ref } => {
                    if let Some(id) = result_ref.strip_prefix("execution:") {
                        let execution_id = id
                            .parse::<uuid::Uuid>()
                            .map(ExecutionId::from_uuid)
                            .map_err(|_| AppError::internal("corrupt idempotency reference"))?;
                        if let Some(execution) =
                            self.ports.executions.get_execution(execution_id).await?
                        {
                            if execution.tenant_id == command.tenant_id {
                                return Ok(execution);
                            }
                        }
                    }
                    return Err(AppError::conflict(format!(
                        "idempotency reference '{result_ref}' cannot be replayed; retry with a new key"
                    )));
                },
                Reservation::ConflictInFlight => {
                    return Err(AppError::conflict(
                        "an execution with this idempotency key is already starting",
                    ));
                },
            }
        }

        // Quota + concurrency gates (permit lives until terminal state).
        self.ports
            .quota
            .check_and_reserve(command.tenant_id, QuotaDimension::Executions, 1)
            .await?;
        let permit = self
            .concurrency
            .acquire(command.tenant_id, command.project_id)?;

        let mut execution = match command.parent_execution_id {
            Some(parent_id) => {
                let parent = self
                    .ports
                    .executions
                    .get_execution(parent_id)
                    .await?
                    .ok_or_else(|| {
                        AppError::not_found("parent_execution", parent_id.to_string())
                    })?;
                let agent_id = command.agent_id.ok_or_else(|| {
                    AppError::invalid_field(
                        "agent_id",
                        "required",
                        "delegated child executions must target an agent",
                    )
                })?;
                parent.spawn_child(agent_id, command.input.clone())?
            },
            None => Execution::new(
                command.tenant_id,
                command.organization_id,
                command.project_id,
                command.workflow_id,
                command.agent_id,
                command.input.clone(),
                &command.correlation_id,
                &command.created_by,
            )?,
        };

        let deadline = self.timeout_policy.deadline_from(command.timeout);
        execution.set_deadline(deadline.expires_at())?;

        self.ports.executions.insert_execution(&execution).await?;
        self.permits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(execution.id, permit);
        match execution.parent_execution_id {
            Some(parent) => {
                self.coordinator
                    .register_child(parent, execution.id, execution.agent_id)?;
            },
            None => self.coordinator.register_root(execution.id),
        }

        self.publish(
            "execution.created",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({
                "execution_id": execution.id,
                "root_execution_id": execution.root_execution_id,
                "parent_execution_id": execution.parent_execution_id,
                "workflow_id": execution.workflow_id,
                "agent_id": execution.agent_id,
            }),
        )
        .await;

        if let Some(key_text) = &command.idempotency_key {
            let key = IdempotencyKey::new(key_text)?;
            self.ports
                .idempotency
                .complete(
                    command.tenant_id,
                    &key,
                    &format!("execution:{}", execution.id),
                )
                .await?;
        }
        Ok(execution)
    }

    /// Pending/Paused → Running, with a live cancellation token registered.
    pub async fn mark_execution_started(&self, execution_id: ExecutionId) -> Result<Execution> {
        let mut execution = self.require_execution(execution_id).await?;
        if execution.status == ExecutionStatus::Paused {
            execution.resume()?;
        } else {
            execution.start()?;
        }
        self.ports.executions.update_execution(&execution).await?;
        self.cancellers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(execution_id, CancellationToken::new());
        self.publish(
            "execution.started",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({ "execution_id": execution.id }),
        )
        .await;
        Ok(execution)
    }

    /// Pauses a running execution (resumable).
    pub async fn pause_execution(
        &self,
        ctx: &RuntimeContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let mut execution = self.require_execution_scoped(ctx, execution_id).await?;
        execution.pause()?;
        self.ports.executions.update_execution(&execution).await?;
        self.publish(
            "execution.paused",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({ "execution_id": execution.id }),
        )
        .await;
        Ok(execution)
    }

    /// Resumes a paused execution.
    pub async fn resume(
        &self,
        ctx: &RuntimeContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let mut execution = self.require_execution_scoped(ctx, execution_id).await?;
        execution.resume()?;
        self.ports.executions.update_execution(&execution).await?;
        self.publish(
            "execution.resumed",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({ "execution_id": execution.id }),
        )
        .await;
        Ok(execution)
    }

    /// Requests coordinated cancellation; the runtime observes the flag/token
    /// and lands in `Cancelled` at the next safe point. When the execution is
    /// not currently running, cancellation is applied immediately.
    pub async fn cancel(
        &self,
        ctx: &RuntimeContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let mut execution = self.require_execution_scoped(ctx, execution_id).await?;
        execution.request_cancellation()?;
        // Signal the in-process runtime, if this engine instance owns it.
        if let Some(token) = self
            .cancellers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&execution_id)
            .cloned()
        {
            token.cancel();
        }
        // Offline executions (Pending/Paused or foreign worker) cancel now.
        if matches!(
            execution.status,
            ExecutionStatus::Pending | ExecutionStatus::Paused
        ) {
            execution.cancel()?;
            self.release_terminal(&execution).await;
        }
        self.ports.executions.update_execution(&execution).await?;
        self.coordinator.notify_all_waiters();
        self.publish(
            "execution.cancellation_requested",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({
                "execution_id": execution.id,
                "status": execution.status.as_str(),
            }),
        )
        .await;
        Ok(execution)
    }

    /// Terminal transition: completed with `output`. Releases the permit.
    pub async fn complete_execution(
        &self,
        execution_id: ExecutionId,
        output: Option<Value>,
    ) -> Result<Execution> {
        let mut execution = self.require_execution(execution_id).await?;
        execution.complete(output)?;
        self.ports.executions.update_execution(&execution).await?;
        self.release_terminal(&execution).await;
        self.publish(
            "execution.completed",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({
                "execution_id": execution.id,
                "resource_usage": execution.resource_usage,
            }),
        )
        .await;
        Ok(execution)
    }

    /// Terminal transition: failed with `error`. Releases the permit.
    pub async fn fail_execution(
        &self,
        execution_id: ExecutionId,
        error: &AppError,
    ) -> Result<Execution> {
        let mut execution = self.require_execution(execution_id).await?;
        execution.fail(error.public_message())?;
        self.ports.executions.update_execution(&execution).await?;
        self.release_terminal(&execution).await;
        self.publish(
            "execution.failed",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({
                "execution_id": execution.id,
                "error_code": error.error_code(),
            }),
        )
        .await;
        Ok(execution)
    }

    /// Terminal transition: cancelled. Releases the permit.
    pub async fn finalize_cancellation(&self, execution_id: ExecutionId) -> Result<Execution> {
        let mut execution = self.require_execution(execution_id).await?;
        execution.cancel()?;
        self.ports.executions.update_execution(&execution).await?;
        self.release_terminal(&execution).await;
        self.publish(
            "execution.cancelled",
            &execution.id.to_string(),
            execution.tenant_id,
            serde_json::json!({ "execution_id": execution.id }),
        )
        .await;
        Ok(execution)
    }

    /// Loads an execution (tenant-scoped to the caller).
    pub async fn get_execution(
        &self,
        ctx: &RuntimeContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        self.require_execution_scoped(ctx, execution_id).await
    }

    // -- recovery ------------------------------------------------------------

    /// Rebuilds in-memory state from the authoritative stores after a restart.
    ///
    /// * non-terminal executions are (re)registered with the coordinator,
    /// * stuck `Running`/`Queued` tasks are requeued per the retry policy; the
    ///   exhausted ones are dead-lettered,
    /// * per-execution permits are not re-acquired (running executions resume
    ///   into fresh budget — the crash released their historical budget).
    pub async fn recover(&self) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();

        let executions = self.ports.executions.list_active(None).await?;
        report.executions_scanned = executions.len() as u32;
        for execution in &executions {
            match execution.parent_execution_id {
                Some(parent) => {
                    if self
                        .coordinator
                        .register_child(parent, execution.id, execution.agent_id)
                        .is_ok()
                    {
                        report.executions_registered += 1;
                    }
                },
                None => {
                    self.coordinator.register_root(execution.id);
                    report.executions_registered += 1;
                },
            }
        }

        let tasks = self.ports.tasks.list_active(None).await?;
        report.tasks_scanned = tasks.len() as u32;
        for mut task in tasks {
            match task.status {
                // Queued work just needs re-advertisement.
                TaskStatus::Queued => report.tasks_requeued += 1,
                // Pending never reached a queue: queue it now.
                TaskStatus::Pending => match task.queue() {
                    Ok(()) => match self.ports.tasks.update_task(&task).await {
                        Ok(()) => report.tasks_requeued += 1,
                        Err(error) => report.errors.push(format!("task {}: {error}", task.id)),
                    },
                    Err(error) => report.errors.push(format!("task {}: {error}", task.id)),
                },
                // A Running task means the worker that owned it is gone.
                // (Messaging-flavored so the policy classifies it as retryable
                // infrastructure loss, not a logic bug.)
                TaskStatus::Running => {
                    let synthetic = AppError::messaging("worker lost; attempt recovered");
                    if let Err(error) = task.fail(synthetic.to_string()) {
                        report.errors.push(format!("task {}: {error}", task.id));
                        continue;
                    }
                    if let Err(error) = self.ports.tasks.update_task(&task).await {
                        report.errors.push(format!("task {}: {error}", task.id));
                        continue;
                    }
                    match self.retry_task_direct(&mut task, &synthetic).await {
                        Ok(true) => report.running_tasks_recovered += 1,
                        Ok(false) => report.tasks_dead_lettered += 1,
                        Err(error) => report.errors.push(format!("task {}: {error}", task.id)),
                    }
                },
                _ => {},
            }
        }
        Ok(report)
    }

    // -- shared internals --------------------------------------------------------

    fn guard_intake(&self, ctx: &RuntimeContext, tenant_id: TenantId) -> Result<()> {
        self.guard_tenant(ctx, tenant_id)?;
        ctx.guard_active()
    }

    fn guard_tenant(&self, ctx: &RuntimeContext, tenant_id: TenantId) -> Result<()> {
        if ctx.tenant_id() != tenant_id {
            return Err(AppError::forbidden(
                "caller context does not belong to the target tenant",
            ));
        }
        Ok(())
    }

    async fn evaluate_policy(
        &self,
        ctx: &RuntimeContext,
        action: &str,
        resource: &str,
        attributes: &Value,
    ) -> Result<()> {
        match self
            .ports
            .policy
            .evaluate(ctx, action, resource, attributes)
            .await?
        {
            PolicyDecision::Allow | PolicyDecision::Transform => Ok(()),
            PolicyDecision::Deny => Err(AppError::forbidden(format!(
                "policy denied '{action}' on '{resource}'"
            ))),
            PolicyDecision::RequireApproval => Err(AppError::forbidden(format!(
                "'{action}' on '{resource}' requires approval before submission"
            ))),
            PolicyDecision::RateLimit => Err(AppError::rate_limited(format!(
                "policy rate-limited '{action}' on '{resource}'"
            ))),
        }
    }

    async fn require_execution(&self, execution_id: ExecutionId) -> Result<Execution> {
        self.ports
            .executions
            .get_execution(execution_id)
            .await?
            .ok_or_else(|| AppError::not_found("execution", execution_id.to_string()))
    }

    async fn require_execution_scoped(
        &self,
        ctx: &RuntimeContext,
        execution_id: ExecutionId,
    ) -> Result<Execution> {
        let execution = self.require_execution(execution_id).await?;
        self.guard_tenant(ctx, execution.tenant_id)?;
        Ok(execution)
    }

    /// Terminal-state cleanup: permit release, quota release, token drop,
    /// (optional) coordinator child-completion bookkeeping stays with the
    /// worker — parents get informed via events.
    async fn release_terminal(&self, execution: &Execution) {
        self.permits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&execution.id);
        self.cancellers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&execution.id);
        if let Err(error) = self
            .ports
            .quota
            .release(execution.tenant_id, QuotaDimension::Executions, 1)
            .await
        {
            warn!(execution_id = %execution.id, %error, "quota release failed");
        }
    }

    async fn publish(
        &self,
        event_type: &str,
        aggregate_id: &str,
        tenant_id: TenantId,
        payload: Value,
    ) {
        if let Err(error) = self
            .ports
            .events
            .publish(event_type, aggregate_id, tenant_id, payload)
            .await
        {
            warn!(event_type, aggregate_id, %error, "event publish failed (continuing)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dead_letter::InMemoryDeadLetterStore;
    use crate::idempotency::{IdempotencyRecord, InMemoryIdempotencyStore};
    use mas_common::enums::Environment;
    use mas_common::ids::{OrganizationId, ProjectId};
    use std::collections::HashMap as StdMap;

    // -- fakes ------------------------------------------------------------------

    #[derive(Debug, Default)]
    struct MemoryTaskStore {
        tasks: Mutex<StdMap<TaskId, Task>>,
    }

    #[async_trait::async_trait]
    impl TaskStorePort for MemoryTaskStore {
        async fn insert_task(&self, task: &Task) -> Result<()> {
            let mut store = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
            if store
                .values()
                .any(|t| t.tenant_id == task.tenant_id && t.idempotency_key == task.idempotency_key)
            {
                return Err(AppError::conflict("duplicate idempotency key"));
            }
            store.insert(task.id, task.clone());
            Ok(())
        }

        async fn update_task(&self, task: &Task) -> Result<()> {
            let mut store = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
            if store.insert(task.id, task.clone()).is_none() {
                return Err(AppError::not_found("task", task.id.to_string()));
            }
            Ok(())
        }

        async fn get_task(&self, task_id: TaskId) -> Result<Option<Task>> {
            Ok(self
                .tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&task_id)
                .cloned())
        }

        async fn task_by_idempotency(
            &self,
            tenant_id: TenantId,
            key: &str,
        ) -> Result<Option<Task>> {
            Ok(self
                .tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .find(|t| t.tenant_id == tenant_id && t.idempotency_key == key)
                .cloned())
        }

        async fn list_active(&self, _tenant_id: Option<TenantId>) -> Result<Vec<Task>> {
            Ok(self
                .tasks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .filter(|t| !t.is_terminal())
                .cloned()
                .collect())
        }
    }

    #[derive(Debug, Default)]
    struct MemoryExecutionStore {
        executions: Mutex<StdMap<ExecutionId, Execution>>,
    }

    #[async_trait::async_trait]
    impl ExecutionStorePort for MemoryExecutionStore {
        async fn insert_execution(&self, execution: &Execution) -> Result<()> {
            self.executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(execution.id, execution.clone());
            Ok(())
        }

        async fn update_execution(&self, execution: &Execution) -> Result<()> {
            let mut store = self.executions.lock().unwrap_or_else(|e| e.into_inner());
            if store.insert(execution.id, execution.clone()).is_none() {
                return Err(AppError::not_found("execution", execution.id.to_string()));
            }
            Ok(())
        }

        async fn get_execution(&self, execution_id: ExecutionId) -> Result<Option<Execution>> {
            Ok(self
                .executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&execution_id)
                .cloned())
        }

        async fn list_active(&self, _tenant_id: Option<TenantId>) -> Result<Vec<Execution>> {
            Ok(self
                .executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .filter(|e2| !e2.status.is_terminal())
                .cloned()
                .collect())
        }

        async fn list_children(&self, parent: ExecutionId) -> Result<Vec<Execution>> {
            Ok(self
                .executions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .filter(|e2| e2.parent_execution_id == Some(parent))
                .cloned()
                .collect())
        }
    }

    #[derive(Debug, Default)]
    struct NoopPorts;

    #[async_trait::async_trait]
    impl WorkflowCatalogPort for NoopPorts {
        async fn get_workflow(&self, _t: TenantId, _w: WorkflowId) -> Result<Option<Workflow>> {
            Ok(None)
        }
    }

    #[async_trait::async_trait]
    impl PolicyPort for NoopPorts {
        async fn evaluate(
            &self,
            _ctx: &RuntimeContext,
            _action: &str,
            _resource: &str,
            _attributes: &Value,
        ) -> Result<PolicyDecision> {
            Ok(PolicyDecision::Allow)
        }
    }

    #[async_trait::async_trait]
    impl QuotaPort for NoopPorts {
        async fn check_and_reserve(&self, _t: TenantId, _d: QuotaDimension, _n: u64) -> Result<()> {
            Ok(())
        }

        async fn release(&self, _t: TenantId, _d: QuotaDimension, _n: u64) -> Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl EventPublisher for NoopPorts {
        async fn publish(&self, _ty: &str, _id: &str, _t: TenantId, _p: Value) -> Result<()> {
            Ok(())
        }
    }

    /// Denying policy for gate tests.
    #[derive(Debug, Default)]
    struct DenyPolicy;

    #[async_trait::async_trait]
    impl PolicyPort for DenyPolicy {
        async fn evaluate(
            &self,
            _ctx: &RuntimeContext,
            _a: &str,
            _r: &str,
            _attr: &Value,
        ) -> Result<PolicyDecision> {
            Ok(PolicyDecision::Deny)
        }
    }

    fn engine() -> (
        OrchestrationEngine,
        Arc<MemoryTaskStore>,
        Arc<MemoryExecutionStore>,
    ) {
        engine_with_policy(Arc::new(NoopPorts))
    }

    fn engine_with_policy(
        policy: Arc<dyn PolicyPort>,
    ) -> (
        OrchestrationEngine,
        Arc<MemoryTaskStore>,
        Arc<MemoryExecutionStore>,
    ) {
        let tasks = Arc::new(MemoryTaskStore::default());
        let executions = Arc::new(MemoryExecutionStore::default());
        let ports = EnginePorts {
            tasks: tasks.clone(),
            executions: executions.clone(),
            workflows: Arc::new(NoopPorts),
            policy,
            quota: Arc::new(NoopPorts),
            events: Arc::new(NoopPorts),
            idempotency: Arc::new(InMemoryIdempotencyStore::new()),
            dead_letters: Arc::new(InMemoryDeadLetterStore::new()),
        };
        let engine = OrchestrationEngine::new(ports, ConcurrencyLimits::default()).expect("engine");
        (engine, tasks, executions)
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

    // -- tests -------------------------------------------------------------------

    #[tokio::test]
    async fn submit_task_is_idempotent_and_queued() {
        let (engine, tasks, _) = engine();
        let tenant = TenantId::new();
        let ctx = ctx(tenant);
        let command = NewTaskCommand::agent_run(
            tenant,
            OrganizationId::new(),
            ProjectId::new(),
            AgentId::new(),
            serde_json::json!({"prompt": "hi"}),
            "tester",
        )
        .with_idempotency_key("submit-42");

        let first = engine
            .submit_task(&ctx, command.clone())
            .await
            .expect("first");
        assert_eq!(first.status, TaskStatus::Queued);
        let second = engine.submit_task(&ctx, command).await.expect("replay");
        assert_eq!(first.id, second.id);
        assert_eq!(
            tasks.tasks.lock().unwrap_or_else(|e| e.into_inner()).len(),
            1
        );
    }

    #[tokio::test]
    async fn cross_tenant_access_is_forbidden() {
        let (engine, _, _) = engine();
        let tenant_a = TenantId::new();
        let command = NewTaskCommand::agent_run(
            tenant_a,
            OrganizationId::new(),
            ProjectId::new(),
            AgentId::new(),
            serde_json::json!({}),
            "tester",
        );
        let task = engine
            .submit_task(&ctx(tenant_a), command)
            .await
            .expect("submit");
        let error = engine
            .get_task(&ctx(TenantId::new()), task.id)
            .await
            .unwrap_err();
        assert_eq!(error.error_code(), "FORBIDDEN");
    }

    #[tokio::test]
    async fn policy_denies_submission() {
        let (engine, _, _) = engine_with_policy(Arc::new(DenyPolicy));
        let tenant = TenantId::new();
        let command = NewTaskCommand::agent_run(
            tenant,
            OrganizationId::new(),
            ProjectId::new(),
            AgentId::new(),
            serde_json::json!({}),
            "tester",
        );
        let error = engine.submit_task(&ctx(tenant), command).await.unwrap_err();
        assert_eq!(error.error_code(), "FORBIDDEN");
    }

    #[tokio::test]
    async fn execution_lifecycle_releases_permits() {
        let (engine, _, _) = engine();
        let tenant = TenantId::new();
        let ctx = ctx(tenant);
        let command = StartExecutionCommand::for_agent(
            tenant,
            OrganizationId::new(),
            ProjectId::new(),
            AgentId::new(),
            serde_json::json!({}),
            "tester",
        );
        let execution = engine
            .start_execution(&ctx, command.with_idempotency_key("exec-1"))
            .await
            .expect("start");
        assert_eq!(execution.status, ExecutionStatus::Pending);

        // Idempotent replay returns the same execution id.
        let replay = engine
            .start_execution(
                &ctx,
                StartExecutionCommand::for_agent(
                    tenant,
                    OrganizationId::new(),
                    ProjectId::new(),
                    AgentId::new(),
                    serde_json::json!({}),
                    "tester",
                )
                .with_idempotency_key("exec-1"),
            )
            .await
            .expect("replay");
        assert_eq!(execution.id, replay.id);

        let running = engine
            .mark_execution_started(execution.id)
            .await
            .expect("started");
        assert_eq!(running.status, ExecutionStatus::Running);

        let failed = engine
            .fail_execution(execution.id, &AppError::internal("boom"))
            .await
            .expect("fail");
        assert_eq!(failed.status, ExecutionStatus::Failed);
        // Permit released: global slot count back to zero.
        let (global, _, _) = engine.concurrency.usage(tenant, failed.project_id);
        assert_eq!(global, 0);
    }

    #[tokio::test]
    async fn retry_then_dead_letter() {
        let (engine, _, _) = engine();
        let tenant = TenantId::new();
        let ctx = ctx(tenant);
        let command = NewTaskCommand::agent_run(
            tenant,
            OrganizationId::new(),
            ProjectId::new(),
            AgentId::new(),
            serde_json::json!({}),
            "tester",
        );
        let mut task = engine.submit_task(&ctx, command).await.expect("submit");
        // Task defaults to 1 + DEFAULT_MAX_RETRIES attempts.
        let error = AppError::timeout("downstream hung");
        loop {
            task = engine.get_task(&ctx, task.id).await.expect("get");
            if task.is_terminal() {
                break;
            }
            // Simulate the worker claiming the attempt.
            if task.status == TaskStatus::Queued {
                task.start().expect("start");
                engine.ports.tasks.update_task(&task).await.expect("update");
            }
            match engine
                .retry_task(&ctx, task.id, &error)
                .await
                .expect("retry decision")
            {
                RetryOutcome::Requeued { .. } => {},
                RetryOutcome::DeadLettered { .. } => break,
            }
        }
        let task = engine.get_task(&ctx, task.id).await.expect("final");
        assert_eq!(task.status, TaskStatus::DeadLettered);
    }

    #[tokio::test]
    async fn cancel_offline_execution_is_immediate() {
        let (engine, _, _) = engine();
        let tenant = TenantId::new();
        let ctx = ctx(tenant);
        let execution = engine
            .start_execution(
                &ctx,
                StartExecutionCommand::for_agent(
                    tenant,
                    OrganizationId::new(),
                    ProjectId::new(),
                    AgentId::new(),
                    serde_json::json!({}),
                    "tester",
                ),
            )
            .await
            .expect("start");
        let cancelled = engine.cancel(&ctx, execution.id).await.expect("cancel");
        assert_eq!(cancelled.status, ExecutionStatus::Cancelled);
    }

    #[tokio::test]
    async fn recover_requeues_running_tasks() {
        let (engine, tasks, _) = engine();
        let tenant = TenantId::new();
        let ctx = ctx(tenant);
        let mut task = engine
            .submit_task(
                &ctx,
                NewTaskCommand::agent_run(
                    tenant,
                    OrganizationId::new(),
                    ProjectId::new(),
                    AgentId::new(),
                    serde_json::json!({}),
                    "tester",
                ),
            )
            .await
            .expect("submit");
        task.start().expect("start"); // pretend a worker owned it, then died
        engine.ports.tasks.update_task(&task).await.expect("update");
        drop(tasks); // recovery reads through the port only

        let report = engine.recover().await.expect("recover");
        assert_eq!(report.tasks_scanned, 1);
        assert_eq!(report.running_tasks_recovered, 1);
        assert!(report.errors.is_empty());
        let task = engine.get_task(&ctx, task.id).await.expect("get");
        assert_eq!(task.status, TaskStatus::Queued);
    }

    #[test]
    fn noop_ports_satisfy_traits() {
        // Compile-level check that fakes stay object-safe.
        let _p: Arc<dyn WorkflowCatalogPort> = Arc::new(NoopPorts);
        let _r: Arc<dyn IdempotencyStore> = Arc::new(InMemoryIdempotencyStore::new());
        let _ = IdempotencyRecord::Completed {
            result_ref: "task:abc".to_owned(),
        };
    }
}
