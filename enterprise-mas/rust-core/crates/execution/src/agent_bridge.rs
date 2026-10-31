//! PURPOSE: The port to the Python agent runtime — the contract the Rust side
//! uses to ask Python to run an agent.
//!
//! Owning layer: `mas-execution`.
//!
//! Production responsibility: define one request/response shape across the
//! language boundary, carrying tenant identity, correlation identity, deadline
//! and cancellation so a Python-side run is always attributable and stoppable.
//! The production adapter speaks gRPC over the shared protobuf definitions.
//!
//! It must NOT:
//!   • contain provider or LLM business logic — none lives in Rust;
//!   • let the in-memory test adapter become a production fallback;
//!   • drop the tenant or correlation identity on the way across.

//! Agent runtime bridge: the port to the Python agent runtime.
//!
//! **No LLM/provider logic lives in Rust.** This module only defines the
//! request/response contract; the real adapter speaks gRPC (messaging crate)
//! to the Python side using the shared protobuf definitions, and an
//! in-memory implementation serves dev/tests.
//!
//! Cancellation and deadlines are caller-driven: bridges observe
//! `request.deadline` and `request.cancellation` and *must* stop work when
//! either fires; the wire agent:run request carries the correlation id from
//! the runtime context.

use mas_common::error::AppError;
use mas_common::ids::{
    AgentId, ExecutionId, OrganizationId, ProjectId, TaskAttemptId, TaskId, TenantId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_orchestration::cancellation::CancellationToken;
use mas_orchestration::runtime_context::RuntimeContext;
use mas_orchestration::timeout::Deadline;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::time::Duration;

/// Final status of an agent run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRunStatus {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

/// Agent self-reported consumption (cousin of the quota layer; raw numbers,
/// aggregation happens downstream).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentUsage {
    pub tokens_input: u64,
    pub tokens_output: u64,
    pub tool_calls: u32,
    pub steps: u32,
    /// Wall-clock the agent itself reports (ms), excluding queue time.
    pub active_time_ms: u64,
}

impl AgentUsage {
    /// Combined token consumption. Saturates: a bridge reporting bogus
    /// `u64::MAX + x` totals must cap, not wrap around and dodge the budget.
    #[must_use]
    pub const fn total_tokens(&self) -> u64 {
        self.tokens_input.saturating_add(self.tokens_output)
    }
}

/// One agent:run request (the wire shape).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRunRequest {
    pub agent_id: AgentId,
    /// Pinned agent version when required (None = current published).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_version: Option<String>,
    #[serde(default)]
    pub input: Value,
    /// End-to-end trace key (execution correlation id).
    pub correlation_id: String,
    /// Absolute wall-clock cap visible to the runtime.
    pub deadline: Timestamp,
    /// Resource envelope the agent must respect.
    pub limits: mas_domain::ResourceLimits,
    /// Token allowance forwarded to the runtime (0 = unmetered).
    pub token_budget_max: u64,

    // -- authoritative platform identity --------------------------------------
    // The Python orchestrator must never derive tenancy from anything the AI
    // runtime itself supplies. These are populated from the `RuntimeContext` by
    // `from_context` and are optional only so that callers which legitimately
    // lack a scope (unit fixtures, health probes) keep compiling. An adapter
    // MUST refuse a request that is missing `tenant_id`: see
    // `AgentRunRequest::require_authoritative_scope`.
    /// Authoritative tenant (RLS scope on the platform side).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    /// Authoritative organization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    /// Authoritative project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    /// Owning execution, when the run was spawned by one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    /// Owning task, when the run was dispatched from the durable queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    /// Durable attempt created by the worker claim, when the run is a task
    /// delivery. It distinguishes retries from duplicate delivery of one
    /// attempt at the Python idempotency boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_attempt_id: Option<TaskAttemptId>,
    /// Platform request id (distinct from the end-to-end correlation id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Caller-supplied deduplication key, propagated from the durable task so a
    /// redelivered task cannot re-run paid AI work. `None` means "no
    /// deduplication requested", which is legal for one-shot runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,

    /// Cancellation is *not* serialized; reconstructed by the adapter.
    #[serde(skip)]
    pub cancellation: CancellationToken,
}

impl AgentRunRequest {
    /// Builds a request from the runtime context for one node attempt.
    pub fn from_context(
        context: &RuntimeContext,
        agent_id: AgentId,
        input: Value,
        limits: mas_domain::ResourceLimits,
        budget: &mas_domain::TokenBudget,
    ) -> Result<Self> {
        let deadline = context
            .deadline()
            .unwrap_or_else(|| Deadline::after(Duration::from_millis(limits.timeout_ms)));
        let snapshot = context.snapshot();
        Ok(Self {
            agent_id,
            agent_version: None,
            input,
            correlation_id: context.correlation_id().to_owned(),
            deadline: deadline.expires_at(),
            limits,
            token_budget_max: budget.max_tokens(),
            tenant_id: Some(snapshot.tenant_id),
            organization_id: Some(snapshot.organization_id),
            project_id: Some(snapshot.project_id),
            execution_id: snapshot.execution_id,
            task_id: None,
            task_attempt_id: None,
            request_id: snapshot.request_id.map(|id| id.to_string()),
            // A node attempt is one step of an execution, so it carries no
            // idempotency key of its own; deduplication for dispatched work is
            // set by the caller that owns the durable task.
            idempotency_key: None,
            cancellation: context.cancellation().child(),
        })
    }

    /// Refuses a request that lacks the authoritative tenant scope.
    ///
    /// Adapters that cross a trust boundary (the gRPC bridge to the Python
    /// orchestrator) must call this before sending: the far side is required to
    /// reject unscoped work, so sending it would only burn a round trip.
    pub fn require_authoritative_scope(&self) -> Result<()> {
        if self.tenant_id.is_none() {
            return Err(AppError::validation(
                "agent run request is missing the authoritative tenant scope",
            ));
        }
        if self.organization_id.is_none() {
            return Err(AppError::validation(
                "agent run request is missing the authoritative organization scope",
            ));
        }
        Ok(())
    }
}

/// Wire code reported when a run is cancelled.
///
/// This is the Python orchestrator's published `ErrorCode` value for
/// cancellation, not an ad-hoc literal: a cancellation is one physical
/// condition, so it must carry one code regardless of which side noticed it
/// first. Without this constant the gRPC bridge and the in-memory bridge could
/// disagree, and a caller branching on the code would silently miss one of
/// them.
pub const CANCELLATION_CODE: &str = "cancellation_requested";

/// The verdict of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AgentRunVerdict {
    /// Clean finish with output and the safe attribution metadata reported by
    /// the Python runtime. Empty or absent wire metadata stays `None`; the Rust
    /// side never guesses a provider/model or finish reason.
    Succeeded {
        output: Value,
        usage: AgentUsage,
        model_key: Option<String>,
        finish_reason: Option<String>,
    },
    /// Agent-side failure. `retryable` comes from the runtime which knows
    /// whether the failure is intrinsic (bad prompt schema) or transient.
    /// Correlation/request IDs are retained for logs and boundary validation;
    /// they are not used to rewrite the authoritative Rust request context.
    Failed {
        code: String,
        message: String,
        retryable: bool,
        usage: AgentUsage,
        correlation_id: Option<String>,
        request_id: Option<String>,
    },
}

/// The bridge port.
#[async_trait::async_trait]
pub trait AgentBridge: Send + Sync + fmt::Debug {
    /// Human-readable adapter name (logs/metrics).
    fn name(&self) -> &'static str;

    /// Runs the agent **once** (retries belong to the caller's policy). The
    /// implementation honors `request.deadline` and `request.cancellation`.
    async fn run(&self, request: AgentRunRequest) -> Result<AgentRunVerdict>;

    /// Health of the runtime behind the bridge (readiness probe shape).
    async fn health(&self) -> Result<()> {
        Ok(())
    }
}

// =============================================================================
// In-memory implementation
// =============================================================================
//
// PURPOSE:
// Test/dev-only stand-in for the production gRPC bridge. It is compiled out of
// release builds unless the `dev-bridges` feature is enabled explicitly, so the
// production composition root *cannot* select it: an operator who wants it has
// to name the feature, which is a deliberate act rather than a default.
//
// The gate is `any(test, feature = "dev-bridges")` rather than `test` alone so
// integration tests and local bring-up binaries outside this crate can still
// reach it when they opt in.

// Used only by the gated in-memory bridge below, so they are imported here
// rather than at the top of the module where they would be dead code in a
// release build.
#[cfg(any(test, feature = "dev-bridges"))]
use std::sync::Arc;
#[cfg(any(test, feature = "dev-bridges"))]
use std::sync::Mutex;

#[cfg(any(test, feature = "dev-bridges"))]
type AgentHandler =
    Arc<dyn Fn(AgentRunRequest) -> std::result::Result<AgentRunVerdict, AppError> + Send + Sync>;

#[cfg(any(test, feature = "dev-bridges"))]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum AgentInjection {
    #[default]
    None,
    AlwaysFail {
        retryable: bool,
    },
    AlwaysTimeout,
}

#[cfg(any(test, feature = "dev-bridges"))]
/// In-memory bridge for dev/tests: handler-driven canned responses, request
/// recording and failure injection. Clone-free; share via `Arc`.
pub struct InMemoryAgentBridge {
    handler: Mutex<Option<AgentHandler>>,
    injection: Mutex<AgentInjection>,
    requests: Mutex<Vec<RecordedAgentRun>>,
}

#[cfg(any(test, feature = "dev-bridges"))]
impl fmt::Debug for InMemoryAgentBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryAgentBridge")
            .field(
                "requests_recorded",
                &self
                    .requests
                    .lock()
                    .map(|requests| requests.len())
                    .unwrap_or(0),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(any(test, feature = "dev-bridges"))]
/// What the bridge saw (redacted input held for assertions).
#[derive(Debug, Clone)]
pub struct RecordedAgentRun {
    pub agent_id: AgentId,
    pub task_id: Option<TaskId>,
    pub task_attempt_id: Option<TaskAttemptId>,
    pub correlation_id: String,
    pub at: Timestamp,
}

#[cfg(any(test, feature = "dev-bridges"))]
impl Default for InMemoryAgentBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(test, feature = "dev-bridges"))]
impl InMemoryAgentBridge {
    #[must_use]
    pub fn new() -> Self {
        Self {
            handler: Mutex::new(None),
            injection: Mutex::new(AgentInjection::None),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// Default success path: handler overrides it; without one, echoes a
    /// synthesized LLM-ish reply.
    pub fn with_handler(
        self,
        handler: impl Fn(AgentRunRequest) -> std::result::Result<AgentRunVerdict, AppError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        *self.handler.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::new(handler));
        self
    }

    pub fn fail_next(&self, retryable: bool) {
        *self.injection.lock().unwrap_or_else(|e| e.into_inner()) =
            AgentInjection::AlwaysFail { retryable };
    }

    pub fn timeout_next(&self) {
        *self.injection.lock().unwrap_or_else(|e| e.into_inner()) = AgentInjection::AlwaysTimeout;
    }

    pub fn clear_injection(&self) {
        *self.injection.lock().unwrap_or_else(|e| e.into_inner()) = AgentInjection::None;
    }

    #[must_use]
    pub fn recorded_requests(&self) -> Vec<RecordedAgentRun> {
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[async_trait::async_trait]
#[cfg(any(test, feature = "dev-bridges"))]
impl AgentBridge for InMemoryAgentBridge {
    fn name(&self) -> &'static str {
        "in-memory-agent"
    }

    async fn run(&self, request: AgentRunRequest) -> Result<AgentRunVerdict> {
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(RecordedAgentRun {
                agent_id: request.agent_id,
                task_id: request.task_id,
                task_attempt_id: request.task_attempt_id,
                correlation_id: request.correlation_id.clone(),
                at: Timestamp::now(),
            });

        match *self.injection.lock().unwrap_or_else(|e| e.into_inner()) {
            AgentInjection::None => {},
            AgentInjection::AlwaysFail { retryable } => {
                return Ok(AgentRunVerdict::Failed {
                    code: "injected_failure".to_owned(),
                    message: "injected agent failure".to_owned(),
                    retryable,
                    usage: AgentUsage::default(),
                    correlation_id: None,
                    request_id: None,
                });
            },
            AgentInjection::AlwaysTimeout => {
                return Err(AppError::timeout(
                    "injected agent timeout (bridge-level Err)",
                ));
            },
        }

        if let Some(handler) = self
            .handler
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return handler(request);
        }

        // Default: bounded "thinking time" honoring cancellation, then echo.
        let wait = Duration::from_millis(5);
        tokio::select! {
            biased;
            () = request.cancellation.cancelled() => {
                Ok(AgentRunVerdict::Failed {
                    code: CANCELLATION_CODE.to_owned(),
                    message: "agent run cancelled".to_owned(),
                    retryable: false,
                    usage: AgentUsage::default(),
                    correlation_id: None,
                    request_id: None,
                })
            },
            () = tokio::time::sleep(wait) => {
                let tokens = request.input.to_string().len() as u64 + 32;
                Ok(AgentRunVerdict::Succeeded {
                    output: serde_json::json!({
                        "agent_id": request.agent_id.to_string(),
                        "correlation_id": request.correlation_id,
                        "echo": request.input,
                        "finish_reason": "stop",
                    }),
                    usage: AgentUsage {
                        tokens_input: tokens,
                        tokens_output: 32,
                        tool_calls: 0,
                        steps: 1,
                        active_time_ms: wait.as_millis() as u64,
                    },
                    model_key: None,
                    finish_reason: Some("completed".to_owned()),
                })
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::Environment;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId};

    fn context(tenant: TenantId) -> RuntimeContext {
        RuntimeContext::new(
            tenant,
            OrganizationId::new(),
            ProjectId::new(),
            "tester",
            Environment::Development,
        )
        .expect("ctx")
    }

    #[tokio::test]
    async fn default_run_echoes_and_reports_usage() {
        let bridge = InMemoryAgentBridge::new();
        let ctx = context(TenantId::new());
        let request = AgentRunRequest::from_context(
            &ctx,
            AgentId::new(),
            serde_json::json!({"prompt": "hi"}),
            mas_domain::ResourceLimits::default(),
            &mas_domain::TokenBudget::unlimited(),
        )
        .expect("request");
        let verdict = bridge.run(request).await.expect("run");
        match verdict {
            AgentRunVerdict::Succeeded {
                output,
                usage,
                model_key,
                finish_reason,
            } => {
                assert_eq!(output["finish_reason"], "stop");
                assert!(usage.total_tokens() > 0);
                assert!(model_key.is_none());
                assert_eq!(finish_reason.as_deref(), Some("completed"));
            },
            other => panic!("expected success, got {other:?}"),
        }
        assert_eq!(bridge.recorded_requests().len(), 1);
        assert_eq!(bridge.name(), "in-memory-agent");
    }

    #[tokio::test]
    async fn cancellation_and_injection_work() {
        let bridge = InMemoryAgentBridge::new();
        let ctx = context(TenantId::new());
        ctx.cancellation().cancel();
        let request = AgentRunRequest::from_context(
            &ctx,
            AgentId::new(),
            serde_json::json!({}),
            mas_domain::ResourceLimits::default(),
            &mas_domain::TokenBudget::unlimited(),
        )
        .expect("request");
        let verdict = bridge.run(request).await.expect("run");
        assert!(matches!(
            verdict,
            AgentRunVerdict::Failed { code, .. } if code == CANCELLATION_CODE
        ));

        bridge.fail_next(true);
        let ctx2 = context(TenantId::new());
        let request = AgentRunRequest::from_context(
            &ctx2,
            AgentId::new(),
            serde_json::json!({}),
            mas_domain::ResourceLimits::default(),
            &mas_domain::TokenBudget::unlimited(),
        )
        .expect("request");
        let verdict = bridge.run(request).await.expect("run");
        assert!(matches!(
            verdict,
            AgentRunVerdict::Failed {
                retryable: true,
                ..
            }
        ));

        bridge.timeout_next();
        let request = AgentRunRequest::from_context(
            &ctx2,
            AgentId::new(),
            serde_json::json!({}),
            mas_domain::ResourceLimits::default(),
            &mas_domain::TokenBudget::unlimited(),
        )
        .expect("request");
        assert!(bridge.run(request).await.is_err());
    }
}
