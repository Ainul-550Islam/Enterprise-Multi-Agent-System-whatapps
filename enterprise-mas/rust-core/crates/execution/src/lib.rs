// PURPOSE:
// Public surface of the execution runtime.
//
// Re-exports the `AgentBridge` port that every agent-execution adapter
// implements, together with the request/verdict types an adapter carries and
// the scope helpers that make a tenant-less run unrepresentable.
//
// The in-memory test bridge lives here but is compiled only under the
// `dev-bridges` feature, so a release build cannot reach it.

//! # mas-execution
//!
//! The execution layer: everything between a *dispatched* task/execution and
//! the actual work — plus the adapters that let the orchestration workflow
//! runtime drive real node work.
//!
//! * [`Executor`] — claims an execution and runs it against a workflow graph
//!   via the orchestration [`WorkflowRuntime`][mas_orchestration::workflow_runtime::WorkflowRuntime].
//! * [`StepRunner`] — execution-step bookkeeping around work attempts (step
//!   rows begin/fail/succeed/skip in the domain's legal order).
//! * [`ToolRunner`] — tool invocation with policy, sandbox, timeout and
//!   payload guards; [`AgentBridge`]/`HttpToolBridge` are the pluggable
//!   runtimes (gRPC/HTTP adapters implement these traits downstream).
//! * [`ResultProcessor`] — normalizes raw runner results into redacted,
//!   size-capped step payloads; [`OutputValidator`] enforces node output
//!   contracts before they flow downstream.
//! * [`ResourceGuard`] — per-execution resource budgets (steps, tool calls,
//!   tokens, wall time).
//! * [`SandboxPolicy`] — hermetic execution policies checked into every run.
//! * [`classify_failure`] — the failure taxonomy feeding retry/DLQ decisions.
//!
//! Integration: [`AgentNodeExecutor`], [`ToolNodeExecutor`],
//! [`DelayNodeExecutor`] and [`ApprovalNodeExecutor`] implement the
//! orchestration `NodeExecutor` trait and plug into a
//! `NodeExecutorRegistry`, making workflows runnable end-to-end.

pub mod agent_bridge;
pub mod data_hub_bridge;
pub mod executor;
pub mod node_adapters;
pub mod output_validator;
pub mod resource_guard;
pub mod result_processor;
pub mod sandbox;
pub mod step_runner;
pub mod tool_runner;

#[cfg(any(test, feature = "dev-bridges"))]
pub use agent_bridge::InMemoryAgentBridge;
pub use agent_bridge::{
    AgentBridge, AgentRunRequest, AgentRunStatus, AgentRunVerdict, AgentUsage, CANCELLATION_CODE,
};
pub use data_hub_bridge::{
    DataHubBridgePort, DataHubCallContext, DataHubDocumentIngestionResult,
    DataHubIngestDocumentRequest, DataHubPurgeDocumentRequest, DataHubPurgeDocumentResult,
    DataHubSourceKind, DataHubSyncSourceRequest, DataHubSyncSourceResult, InMemoryDataHubBridge,
};
pub use executor::{Executor, ExecutorConfig, RunOutcome, StepStorePort};
pub use mas_orchestration::checkpoint::NodeExecutionState;
pub use node_adapters::{
    AgentNodeExecutor, ApprovalNodeExecutor, DelayNodeExecutor, ToolNodeExecutor,
};
pub use output_validator::{OutputValidator, PortabilityLimits};
pub use resource_guard::{GuardSnapshot, ResourceGuard};
pub use result_processor::{classify_failure, FailureClass, ResultProcessor};
pub use sandbox::{EgressPolicy, SandboxPolicy};
pub use step_runner::{StepRunner, StepTransition};
pub use tool_runner::{
    HttpToolBridge, InMemoryToolBridge, ToolInvocationReport, ToolInvocationRequest, ToolRunner,
};
