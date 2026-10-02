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
use mas_common::ids::AgentId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_orchestration::cancellation::CancellationToken;
use mas_orchestration::runtime_context::RuntimeContext;
use mas_orchestration::timeout::Deadline;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
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
        Ok(Self {
            agent_id,
            agent_version: None,
            input,
            correlation_id: context.correlation_id().to_owned(),
            deadline: deadline.expires_at(),
            limits,
            token_budget_max: budget.max_tokens(),
            cancellation: context.cancellation().child(),
        })
    }
}

/// The verdict of a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AgentRunVerdict {
    /// Clean finish with output.
    Succeeded { output: Value, usage: AgentUsage },
    /// Agent-side failure. `retryable` comes from the runtime which knows
    /// whether the failure is intrinsic (bad prompt schema) or transient.
    Failed {
        code: String,
        message: String,
        retryable: bool,
        usage: AgentUsage,
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

type AgentHandler =
    Arc<dyn Fn(AgentRunRequest) -> std::result::Result<AgentRunVerdict, AppError> + Send + Sync>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum AgentInjection {
    #[default]
    None,
    AlwaysFail {
        retryable: bool,
    },
    AlwaysTimeout,
}

/// In-memory bridge for dev/tests: handler-driven canned responses, request
/// recording and failure injection. Clone-free; share via `Arc`.
pub struct InMemoryAgentBridge {
    handler: Mutex<Option<AgentHandler>>,
    injection: Mutex<AgentInjection>,
    requests: Mutex<Vec<RecordedAgentRun>>,
}

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

/// What the bridge saw (redacted input held for assertions).
#[derive(Debug, Clone)]
pub struct RecordedAgentRun {
    pub agent_id: AgentId,
    pub correlation_id: String,
    pub at: Timestamp,
}

impl Default for InMemoryAgentBridge {
    fn default() -> Self {
        Self::new()
    }
}

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
                    code: "cancelled".to_owned(),
                    message: "agent run cancelled".to_owned(),
                    retryable: false,
                    usage: AgentUsage::default(),
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
            AgentRunVerdict::Succeeded { output, usage } => {
                assert_eq!(output["finish_reason"], "stop");
                assert!(usage.total_tokens() > 0);
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
            AgentRunVerdict::Failed { code, .. } if code == "cancelled"
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
