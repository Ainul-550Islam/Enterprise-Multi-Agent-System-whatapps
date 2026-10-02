//! Tool runner: policy → sandbox → bridge, with payload and timeout guards.
//!
//! The runner is the *only* way node/tool work reaches a tool runtime:
//!
//! 1. policy evaluation (`tool.invoke` on `tool:<id>`) — deny ⇒ forbidden,
//! 2. payload guards (arguments ≤ platform cap),
//! 3. sandbox policy check (per-tool `sandbox_required` ∧ egress rules),
//! 4. dispatch through the injected [`HttpToolBridge`] with a strict
//!    per-invocation timeout (min(tool timeout, execution remaining)),
//! 5. output guards + usage reporting.
//!
//! Tool rows are fetched per-tenant via [`ToolCatalogPort`]; unknown or
//! disabled tools fail *before* any network traffic.

use crate::resource_guard::ResourceGuard;
use crate::sandbox::SandboxPolicy;
use mas_common::constants;
use mas_common::enums::PolicyDecision;
use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ExecutionStepId, TenantId, ToolId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{SafetyClassification, Tool, ToolKind};
use mas_orchestration::engine::PolicyPort;
use mas_orchestration::runtime_context::RuntimeContext;
use mas_orchestration::timeout::Deadline;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Tool catalog read port (persistence crate adapts).
#[async_trait::async_trait]
pub trait ToolCatalogPort: Send + Sync + fmt::Debug {
    /// Tenant-scoped tool fetch.
    async fn get_tool(&self, tenant_id: TenantId, tool_id: ToolId) -> Result<Option<Tool>>;
}

/// What one invocation carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInvocationRequest {
    pub execution_id: ExecutionId,
    pub step_id: ExecutionStepId,
    pub tool_id: ToolId,
    /// Tool operation (e.g. `query`, `insert`, `send`).
    pub action: String,
    #[serde(default)]
    pub arguments: Value,
    pub correlation_id: String,
    /// Invoker identity (audit).
    pub actor: String,
    /// Absolute invocation deadline known by the tool.
    pub deadline: Timestamp,
    /// Enforcing policy snapshot (embedded for sandbox-aware bridges).
    pub sandbox_policy: SandboxPolicy,
}

impl ToolInvocationRequest {
    /// Serialized argument length (guards use this).
    pub fn arguments_len(&self) -> Result<usize> {
        serde_json::to_vec(&self.arguments)
            .map(|v| v.len())
            .map_err(|e| AppError::serialization(format!("arguments: {e}")))
    }
}

/// One invocation's report.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ToolInvocationReport {
    /// Successful invocation; output passed every guard.
    Succeeded { output: Value, latency_ms: u64 },
    /// Tool signaled a business failure (stock 4xx semantics).
    Failed {
        code: String,
        message: String,
        retryable: bool,
        latency_ms: u64,
    },
}

/// The runtime hop to a tool implementation. HTTP/connector/code runtimes
/// adapt this trait; the gRPC/HTTP adapters live in integrations/messaging.
#[async_trait::async_trait]
pub trait HttpToolBridge: Send + Sync + fmt::Debug {
    fn name(&self) -> &'static str;

    /// Invokes the tool once (retries belong to the caller's policy).
    async fn invoke(
        &self,
        tool: &Tool,
        request: ToolInvocationRequest,
    ) -> Result<ToolInvocationReport>;
}

/// The runner.
pub struct ToolRunner {
    catalog: Arc<dyn ToolCatalogPort>,
    policy: Arc<dyn PolicyPort>,
    bridge: Arc<dyn HttpToolBridge>,
    sandbox_policy: SandboxPolicy,
}

impl fmt::Debug for ToolRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolRunner")
            .field("bridge", &self.bridge.name())
            .finish_non_exhaustive()
    }
}

impl ToolRunner {
    pub fn new(
        catalog: Arc<dyn ToolCatalogPort>,
        policy: Arc<dyn PolicyPort>,
        bridge: Arc<dyn HttpToolBridge>,
        sandbox_policy: SandboxPolicy,
    ) -> Self {
        Self {
            catalog,
            policy,
            bridge,
            sandbox_policy,
        }
    }

    #[must_use]
    pub fn sandbox_policy(&self) -> &SandboxPolicy {
        &self.sandbox_policy
    }

    /// Runs one tool invocation (single attempt — retry scheduling is the
    /// caller's job). Resource charging happens *before* dispatch.
    pub async fn invoke(
        &self,
        context: &RuntimeContext,
        tool_id: ToolId,
        action: &str,
        arguments: Value,
        execution_id: ExecutionId,
        step_id: ExecutionStepId,
        guard: &mut ResourceGuard,
    ) -> Result<ToolInvocationReport> {
        // Lookup first: unknown tools must not generate traffic.
        let tool = self
            .catalog
            .get_tool(context.tenant_id(), tool_id)
            .await?
            .ok_or_else(|| AppError::not_found("tool", tool_id.to_string()))?;
        if !tool.is_invocable() {
            return Err(AppError::conflict(format!(
                "tool '{}' is not invocable (status: {})",
                tool.id, tool.status
            )));
        }

        // Policy gate.
        let resource = format!("tool:{tool_id}");
        let attributes = serde_json::json!({
            "action": action,
            "safety": tool.safety.as_str(),
        });
        match self
            .policy
            .evaluate(context, "tool.invoke", &resource, &attributes)
            .await?
        {
            PolicyDecision::Allow | PolicyDecision::Transform => {},
            PolicyDecision::Deny => {
                return Err(AppError::forbidden(format!(
                    "policy denied tool.invoke on {resource}"
                )));
            },
            PolicyDecision::RequireApproval => {
                return Err(AppError::forbidden(format!(
                    "tool '{tool_id}' requires approval before invocation"
                )));
            },
            PolicyDecision::RateLimit => {
                return Err(AppError::rate_limited(format!(
                    "policy rate-limited tool {tool_id}"
                )));
            },
        }

        // Sandbox: tools requiring sandboxing must run under it; network
        // requests must honor the egress rules.
        let endpoint_host = tool
            .runtime
            .endpoint
            .as_ref()
            .map(|url| url.host_str().to_owned());
        let requests_network = matches!(tool.kind, ToolKind::Http | ToolKind::Connector);
        let effective = if tool.runtime.sandbox_required {
            self.sandbox_policy.intersection(&SandboxPolicy::hermetic())
        } else {
            self.sandbox_policy.clone()
        };
        effective.enforce(requests_network, endpoint_host.as_deref())?;

        // Guard + argument size (tool cap first, then the execution cap).
        guard.charge_tool_call()?;
        let arguments_len = serde_json::to_vec(&arguments)
            .map(|v| v.len())
            .map_err(|e| AppError::serialization(format!("arguments: {e}")))?;
        if arguments_len > constants::MAX_TOOL_ARGUMENT_BYTES {
            return Err(AppError::invalid_field(
                "arguments",
                "too_large",
                format!(
                    "tool arguments are {arguments_len} bytes (limit {})",
                    constants::MAX_TOOL_ARGUMENT_BYTES
                ),
            ));
        }
        guard.check_payload_bytes(arguments_len)?;

        let mut tool = tool;
        if let SafetyClassification::Destructive = tool.safety {
            // Destructive tools never run unsandboxed, regardless of the
            // tool's own flag history.
            tool.runtime.sandbox_required = true;
        }

        let remaining = guard.remaining_time();
        let timeout = Duration::from_millis(tool.runtime.timeout_ms.max(1)).min(remaining);
        if timeout.is_zero() {
            return Err(AppError::timeout(
                "no time budget remains for this tool invocation",
            ));
        }

        let request = ToolInvocationRequest {
            execution_id,
            step_id,
            tool_id,
            action: action.to_owned(),
            arguments,
            correlation_id: context.correlation_id().to_owned(),
            actor: context.actor_id().to_owned(),
            deadline: Deadline::after(timeout).expires_at(),
            sandbox_policy: effective,
        };

        let started = Timestamp::now();
        let deadline = Deadline::after(timeout);
        let report = deadline
            .timeout_guard("tool invocation", async {
                self.bridge.invoke(&tool, request).await
            })
            .await
            .map_err(|_| {
                AppError::timeout(format!(
                    "tool '{tool_id}' exceeded its timeout of {} ms",
                    timeout.as_millis()
                ))
            })??;

        // Output size guard (hard platform cap).
        if let ToolInvocationReport::Succeeded { output, .. } = &report {
            let bytes = serde_json::to_vec(output)
                .map(|v| v.len())
                .map_err(|e| AppError::serialization(format!("tool output: {e}")))?;
            if bytes > constants::MAX_TOOL_OUTPUT_BYTES {
                return Err(AppError::invalid_field(
                    "tool_output",
                    "too_large",
                    format!(
                        "tool output is {bytes} bytes; the platform cap is {}",
                        constants::MAX_TOOL_OUTPUT_BYTES
                    ),
                ));
            }
        }
        let _elapsed = started.elapsed();
        Ok(report)
    }
}

// =============================================================================
// In-memory bridge (dev/tests)
// =============================================================================

type ToolHandler =
    Arc<dyn Fn(&Tool, ToolInvocationRequest) -> Result<ToolInvocationReport> + Send + Sync>;

/// Handler-driven in-memory tool bridge.
pub struct InMemoryToolBridge {
    handlers: Mutex<HashMap<ToolId, ToolHandler>>,
    invocations: Mutex<Vec<(ToolId, String)>>,
}

impl fmt::Debug for InMemoryToolBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryToolBridge")
            .field(
                "registered_tools",
                &self
                    .handlers
                    .lock()
                    .map(|handlers| handlers.len())
                    .unwrap_or(0),
            )
            .finish_non_exhaustive()
    }
}

impl Default for InMemoryToolBridge {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryToolBridge {
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: Mutex::new(HashMap::new()),
            invocations: Mutex::new(Vec::new()),
        }
    }

    /// Registers a handler for a tool; default behavior without one is a
    /// synthetic success echoing the arguments.
    pub fn with_handler(
        self,
        tool_id: ToolId,
        handler: impl Fn(&Tool, ToolInvocationRequest) -> Result<ToolInvocationReport>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.handlers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id, Arc::new(handler));
        self
    }

    #[must_use]
    pub fn invocation_count(&self, tool_id: ToolId) -> usize {
        self.invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(id, _)| *id == tool_id)
            .count()
    }
}

#[async_trait::async_trait]
impl HttpToolBridge for InMemoryToolBridge {
    fn name(&self) -> &'static str {
        "in-memory-tools"
    }

    async fn invoke(
        &self,
        tool: &Tool,
        request: ToolInvocationRequest,
    ) -> Result<ToolInvocationReport> {
        self.invocations
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((tool.id, request.action.clone()));
        let handler = self
            .handlers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&tool.id)
            .cloned();
        if let Some(handler) = handler {
            return handler(tool, request);
        }
        Ok(ToolInvocationReport::Succeeded {
            output: serde_json::json!({
                "tool_id": tool.id.to_string(),
                "action": request.action,
                "echo": request.arguments,
            }),
            latency_ms: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::Environment;
    use mas_common::ids::{OrganizationId, ProjectId};
    use mas_domain::SafeUrl;

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

    #[derive(Debug)]
    struct AllowAllPolicy;

    #[async_trait::async_trait]
    impl PolicyPort for AllowAllPolicy {
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

    #[derive(Debug)]
    struct DenyAllPolicy;

    #[async_trait::async_trait]
    impl PolicyPort for DenyAllPolicy {
        async fn evaluate(
            &self,
            _ctx: &RuntimeContext,
            _action: &str,
            _resource: &str,
            _attributes: &Value,
        ) -> Result<PolicyDecision> {
            Ok(PolicyDecision::Deny)
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

    fn http_tool(tenant: TenantId, sandbox_required: bool) -> Tool {
        let mut tool = Tool::register(
            tenant,
            OrganizationId::new(),
            Some(ProjectId::new()),
            "http-echo",
            ToolKind::Internal,
            serde_json::json!({"type": "object"}),
            None,
        )
        .expect("tool");
        tool.kind = ToolKind::Http;
        let runtime = mas_domain::ToolRuntime {
            endpoint: Some(SafeUrl::parse("https://api.tools.example/echo").expect("url")),
            timeout_ms: 5_000,
            sandbox_required,
            settings: serde_json::Map::new(),
        };
        tool.set_runtime(runtime).expect("http endpoint");
        tool.publish().expect("publish");
        tool
    }

    #[tokio::test]
    async fn policy_denies_before_any_bridge_call() {
        let tenant = TenantId::new();
        let catalog = Arc::new(MemoryCatalog::default());
        let tool = http_tool(tenant, false);
        let tool_id = tool.id;
        catalog
            .tools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id, tool);
        let bridge = Arc::new(InMemoryToolBridge::new());
        let runner = ToolRunner::new(
            catalog,
            Arc::new(DenyAllPolicy),
            bridge.clone(),
            SandboxPolicy::default(),
        );
        let mut guard = ResourceGuard::new(None, None).expect("guard");
        let error = runner
            .invoke(
                &ctx(tenant),
                tool_id,
                "echo",
                serde_json::json!({}),
                ExecutionId::new(),
                ExecutionStepId::new(),
                &mut guard,
            )
            .await
            .unwrap_err();
        assert_eq!(error.error_code(), "FORBIDDEN");
        assert_eq!(bridge.invocation_count(tool_id), 0);
    }

    #[tokio::test]
    async fn sandbox_blocks_unlisted_network_and_succeeds_allowlisted() {
        let tenant = TenantId::new();
        let catalog = Arc::new(MemoryCatalog::default());
        let tool = http_tool(tenant, true);
        let tool_id = tool.id;
        catalog
            .tools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id, tool);
        let runner = ToolRunner::new(
            catalog,
            Arc::new(AllowAllPolicy),
            Arc::new(InMemoryToolBridge::new()),
            SandboxPolicy::default(), // deny-all egress
        );
        let mut guard = ResourceGuard::new(None, None).expect("guard");
        let result = runner
            .invoke(
                &ctx(tenant),
                tool_id,
                "echo",
                serde_json::json!({"ping": true}),
                ExecutionId::new(),
                ExecutionStepId::new(),
                &mut guard,
            )
            .await;
        assert!(result.is_err(), "default egress must deny http tools");
        let error = result.unwrap_err();
        assert_eq!(error.error_code(), "FORBIDDEN");

        // Allow-listed host passes.
        let policy = SandboxPolicy {
            egress: crate::sandbox::EgressPolicy::AllowListOnly,
            allowed_hosts: std::collections::BTreeSet::from(["api.tools.example".to_owned()]),
            ..SandboxPolicy::default()
        };
        policy.validate().expect("valid policy");
        let catalog2 = Arc::new(MemoryCatalog::default());
        let tool = http_tool(tenant, false);
        let tool_id2 = tool.id;
        catalog2
            .tools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id2, tool);
        let runner2 = ToolRunner::new(
            catalog2,
            Arc::new(AllowAllPolicy),
            Arc::new(InMemoryToolBridge::new()),
            policy,
        );
        let report = runner2
            .invoke(
                &ctx(tenant),
                tool_id2,
                "echo",
                serde_json::json!({"ping": 1}),
                ExecutionId::new(),
                ExecutionStepId::new(),
                &mut guard,
            )
            .await
            .expect("invocation succeeds when host is allow-listed");
        assert!(matches!(report, ToolInvocationReport::Succeeded { .. }));
    }

    #[tokio::test]
    async fn unknown_and_uninvocable_tools_fail_fast() {
        let tenant = TenantId::new();
        let catalog = Arc::new(MemoryCatalog::default());
        let runner = ToolRunner::new(
            catalog.clone(),
            Arc::new(AllowAllPolicy),
            Arc::new(InMemoryToolBridge::new()),
            SandboxPolicy::default(),
        );
        let mut guard = ResourceGuard::new(None, None).expect("guard");
        let error = runner
            .invoke(
                &ctx(tenant),
                ToolId::new(),
                "op",
                serde_json::json!({}),
                ExecutionId::new(),
                ExecutionStepId::new(),
                &mut guard,
            )
            .await
            .unwrap_err();
        assert_eq!(error.error_code(), "RESOURCE_NOT_FOUND");

        let mut tool = http_tool(tenant, false);
        tool.disable().expect("disable");
        let tool_id = tool.id;
        catalog
            .tools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id, tool);
        let error = runner
            .invoke(
                &ctx(tenant),
                tool_id,
                "op",
                serde_json::json!({}),
                ExecutionId::new(),
                ExecutionStepId::new(),
                &mut guard,
            )
            .await
            .unwrap_err();
        assert_eq!(error.error_code(), "CONFLICT");
    }

    #[tokio::test]
    async fn argument_size_is_guarded() {
        let tenant = TenantId::new();
        let catalog = Arc::new(MemoryCatalog::default());
        let tool = http_tool(tenant, false);
        let tool_id = tool.id;
        catalog
            .tools
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(tool_id, tool);
        let runner = ToolRunner::new(
            catalog,
            Arc::new(AllowAllPolicy),
            Arc::new(InMemoryToolBridge::new()),
            SandboxPolicy {
                egress: crate::sandbox::EgressPolicy::Unrestricted,
                ..SandboxPolicy::default()
            },
        );
        let mut guard = ResourceGuard::new(None, None).expect("guard");
        let huge = Value::from("x".repeat(constants::MAX_TOOL_ARGUMENT_BYTES + 1));
        let error = runner
            .invoke(
                &ctx(tenant),
                tool_id,
                "op",
                huge,
                ExecutionId::new(),
                ExecutionStepId::new(),
                &mut guard,
            )
            .await
            .unwrap_err();
        assert_eq!(error.error_code(), "VALIDATION_FAILED");
    }
}
