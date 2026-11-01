//! # mas-orchestration
//!
//! The orchestration subsystem:
//!
//! * [`OrchestrationEngine`] — submission, execution lifecycle and recovery
//!   entry points coordinating stores, policy, quota, executor and events.
//! * [`TaskDispatcher`] and [`RuntimeScheduler`] — task dispatch and fair,
//!   priority-ordered scheduling.
//! * [`WorkflowRuntime`] + [`DependencyResolver`] + [`NodeExecutor`] — DAG
//!   execution of workflows.
//! * Primitives: generic [`StateMachine`], [`RetryPolicy`], [`Deadline`]/
//!   [`TimeoutPolicy`], [`CancellationToken`], [`IdempotencyStore`],
//!   [`ExecutionCheckpoint`], [`DeadLetterStore`], [`DistributedLock`] and
//!   [`RuntimeContext`].
//!
//! Infrastructure is injected via port traits (`DispatchTransport`,
//! `TaskStorePort`, …); NATS/PostgreSQL adapters live outside this crate.

pub mod cancellation;
pub mod checkpoint;
pub mod concurrency;
pub mod coordinator;
pub mod dead_letter;
pub mod dependency_resolver;
pub mod dispatcher;
pub mod engine;
pub mod idempotency;
pub mod locks;
pub mod node_executor;
pub mod retry;
pub mod runtime_context;
pub mod scheduler;
pub mod state_machine;
pub mod timeout;
pub mod workflow_runtime;

pub use cancellation::CancellationToken;
pub use checkpoint::{ExecutionCheckpoint, NodeExecutionState};
pub use concurrency::{ConcurrencyLimits, ExecutionSemaphore, TenantConcurrencyLimiter};
pub use coordinator::{ChildOutcome, ExecutionCoordinator, JoinDecision, JoinRequirement};
pub use dead_letter::{
    DeadLetterEvent, DeadLetterRecordStatus, DeadLetterStore, DeadLetterTask,
    InMemoryDeadLetterStore,
};
pub use dependency_resolver::{DependencyResolver, NodeReadiness};
pub use dispatcher::{
    BasicTaskDispatcher, DispatchEnvelope, DispatchReceipt, DispatchTransport,
    InMemoryDispatchTransport, TaskDispatcher,
};
pub use engine::{
    EnginePorts, EventPublisher, ExecutionStorePort, NewTaskCommand, OrchestrationEngine,
    PolicyPort, QuotaPort, RecoveryReport, StartExecutionCommand, TaskStorePort,
    WorkflowCatalogPort,
};
pub use idempotency::{
    IdempotencyKey, IdempotencyRecord, IdempotencyStore, InMemoryIdempotencyStore, Reservation,
};
pub use locks::{DistributedLock, InMemoryDistributedLock, LockError, LockGuard};
pub use node_executor::{NodeExecutionInput, NodeExecutor, NodeExecutorRegistry, NodeOutcome};
pub use retry::{BackoffStrategy, RetryDecision, RetryPolicy};
pub use runtime_context::RuntimeContext;
pub use scheduler::{RuntimeScheduler, ScheduledWork};
pub use state_machine::{StateMachine, TransitionTable};
pub use timeout::{Deadline, TimeoutPolicy};
pub use workflow_runtime::WorkflowRuntime;
