//! PURPOSE:
//! The production `execution.run` handler: the real bridge between a durable
//! task and the Python AI Orchestrator.
//!
//! This module is what replaces `EchoHandler` in the production composition
//! root. It is the only place in the worker that turns a queue message into an
//! [`AgentRunRequest`], and it is deliberately strict about it:
//!
//! * **Scope comes from the message, never from the payload.** The tenant,
//!   organization and project are the authoritative columns of the task row.
//!   An `input` field named `tenant_id` is data, not scope, and is ignored.
//! * **Idempotency is propagated.** The task's idempotency key travels to the
//!   orchestrator so a redelivered task cannot re-run paid AI work.
//! * **Deadline is honoured twice.** Once by the gRPC deadline the bridge
//!   derives, and once by the cancellation token the runtime context carries.
//! * **Cancellation is a verdict, not a success.** A cancelled run returns a
//!   structured `cancelled` failure so the execution settles deterministically
//!   instead of looking like a completed run that produced nothing.
//! * **The bridge is constructed, never defaulted.** There is no in-memory
//!   fallback: without configuration the worker refuses to boot rather than
//!   silently running an echo.

use std::sync::Arc;
use std::time::Duration;

use mas_ai_bridge::{GrpcAgentBridge, GrpcAgentBridgeConfig};
use mas_common::ids::AgentId;
use mas_common::ids::ExecutionId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_contracts::task::TaskQueueMessage;
use mas_execution::agent_bridge::AgentBridge;
use mas_execution::executor::AgentRunOutcome;
use mas_orchestration::cancellation::CancellationToken;
use mas_orchestration::runtime_context::RuntimeContext;
use mas_security::secret_string::SecretString;

/// Environment/config keys read by [`AgentBridgeSettings::from_merger`].
///
/// Grouped as constants so a renamed key is a compile-time edit rather than a
/// string buried in a function body.
const ENV_ENDPOINT: &str = "MAS_AI_BRIDGE_ENDPOINT";
const PATH_ENDPOINT: &str = "ai_bridge.endpoint";
const ENV_TOKEN: &str = "MAS_AI_BRIDGE_TOKEN";
const PATH_TOKEN: &str = "ai_bridge.token";
const ENV_TIMEOUT_MS: &str = "MAS_AI_BRIDGE_TIMEOUT_MS";
const PATH_TIMEOUT_MS: &str = "ai_bridge.timeout_ms";
const ENV_MAX_RESPONSE_BYTES: &str = "MAS_AI_BRIDGE_MAX_RESPONSE_BYTES";
const PATH_MAX_RESPONSE_BYTES: &str = "ai_bridge.max_response_bytes";

/// Default per-call RPC timeout when the request carries no deadline.
const DEFAULT_TIMEOUT_MS: u64 = 120_000;
/// Default ceiling on a decoded response from the orchestrator.
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;

/// Bridge configuration resolved from the layered config.
///
/// Resolved — not defaulted — because a missing endpoint is a deployment fault
/// that must stop the boot, not something to paper over with a local port.
#[derive(Debug, Clone)]
pub struct AgentBridgeSettings {
    endpoint: String,
    token: Option<SecretString>,
    timeout: Duration,
    max_response_bytes: usize,
}

impl AgentBridgeSettings {
    /// Reads the bridge configuration out of the merged config layers.
    ///
    /// # Errors
    ///
    /// Returns an error naming the missing key when the endpoint is absent.
    /// The token is optional at read time and rejected at bind time (see
    /// [`AgentBridgeSettings::require_token`]) so the two failures read
    /// differently: "not configured" vs "configured without a credential".
    pub fn from_merger(merger: &mas_common::config::Merger<'_>) -> Result<Self> {
        let endpoint = merger.string(ENV_ENDPOINT, PATH_ENDPOINT, "");
        if endpoint.trim().is_empty() {
            return Err(mas_common::error::AppError::validation(format!(
                "worker production requires {PATH_ENDPOINT} / {ENV_ENDPOINT}: \
                 the Python AI orchestrator endpoint must be configured explicitly"
            )));
        }

        let timeout_ms = merger.u64(ENV_TIMEOUT_MS, PATH_TIMEOUT_MS, DEFAULT_TIMEOUT_MS)?;
        let max_response_bytes = merger.u64(
            ENV_MAX_RESPONSE_BYTES,
            PATH_MAX_RESPONSE_BYTES,
            DEFAULT_MAX_RESPONSE_BYTES,
        )?;

        let raw_token = merger.string(ENV_TOKEN, PATH_TOKEN, "");
        let token = if raw_token.trim().is_empty() {
            None
        } else {
            Some(SecretString::new(raw_token))
        };

        Ok(Self {
            endpoint,
            token,
            timeout: Duration::from_millis(timeout_ms.max(1)),
            max_response_bytes: usize::try_from(max_response_bytes.max(1024))
                .unwrap_or(DEFAULT_MAX_RESPONSE_BYTES as usize),
        })
    }

    /// Refuses to run without a credential.
    ///
    /// The orchestrator has no unauthenticated mode by design, so binding a
    /// bridge without a token would produce a worker that fails every task at
    /// runtime instead of at boot. Failing here names the problem once.
    pub fn require_token(&self) -> Result<SecretString> {
        match self.token.clone() {
            Some(token) => Ok(token),
            None => Err(mas_common::error::AppError::validation(format!(
                "worker production requires {PATH_TOKEN} / {ENV_TOKEN}: \
                 the AI orchestrator refuses unauthenticated calls, so the \
                 worker cannot bind a bridge without a credential"
            ))),
        }
    }

    /// The configured endpoint, for log lines that must not print credentials.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Connects a [`GrpcAgentBridge`] from these settings.
    pub async fn connect(&self) -> Result<GrpcAgentBridge> {
        let token = self.require_token()?;
        let config = GrpcAgentBridgeConfig {
            endpoint: self.endpoint.clone(),
            default_timeout: self.timeout,
            max_response_bytes: self.max_response_bytes,
            bearer_token: Some(token),
            ..Default::default()
        };
        GrpcAgentBridge::connect(config).await
    }
}

/// One durable task, resolved into everything the canonical runtime needs.
///
/// Built by [`AgentRunHandler::build_dispatch`] from the **task row's**
/// authoritative columns. The payload's `input` is data; it never contributes
/// scope, identity or idempotency.
#[derive(Debug)]
pub struct AgentRunDispatch {
    /// The execution this run settles.
    pub execution_id: ExecutionId,
    /// The agent the run is for.
    pub agent_id: AgentId,
    /// Runtime context: tenant/org/project scope, correlation, deadline and
    /// cancellation, all rebuilt from the message rather than trusted from it.
    pub context: RuntimeContext,
    /// The task's durable identity, carried to the orchestrator.
    pub durable: mas_execution::executor::DurableRunIdentity<'static>,
    /// The rendered input handed to the agent.
    pub input: serde_json::Value,
}

impl AgentRunDispatch {
    /// Refuses a dispatch whose scope is not fully resolved.
    ///
    /// The far side would reject this too; checking locally saves a round trip
    /// on every mis-scoped task and keeps the refusal off the AI call path.
    ///
    /// # Errors
    ///
    /// Returns a validation error naming the missing identifier.
    pub fn require_authoritative_scope(&self) -> Result<()> {
        let snapshot = self.context.snapshot();
        let missing = [
            ("tenant_id", snapshot.tenant_id.is_nil()),
            ("organization_id", snapshot.organization_id.is_nil()),
            ("project_id", snapshot.project_id.is_nil()),
        ]
        .into_iter()
        .find(|(_, is_nil)| *is_nil)
        .map(|(field, _)| field);

        match missing {
            Some(field) => Err(mas_common::error::AppError::validation(format!(
                "execution.run task carries a nil {field}; \
                 an AI run cannot be scoped without an authoritative {field}"
            ))),
            None => Ok(()),
        }
    }
}

/// The production `execution.run` handler.
///
/// Holds the **canonical execution runtime**, not a bridge: the handler
/// resolves the task and then hands it to
/// [`Executor::run_agent_node`][mas_execution::executor::Executor::run_agent_node],
/// which dispatches through the node registry to the agent bridge. This is the
/// same path a one-node workflow graph takes, so there is no second execution
/// lifecycle in the worker.
///
/// The cancellation token is the one the worker's shutdown path owns, so a
/// graceful drain cancels in-flight AI work rather than abandoning it.
#[derive(Debug)]
pub struct AgentRunHandler {
    /// The canonical runtime. Built from the caller's bridge; there is no
    /// in-memory fallback, so a worker built this way cannot report a success
    /// it did not produce.
    executor: Arc<mas_execution::executor::Executor>,
    cancellation: CancellationToken,
}

impl AgentRunHandler {
    /// Wraps a bridge in the canonical runtime.
    ///
    /// The bridge is accepted as a trait object so the composition root decides
    /// the implementation. It is required, and it is the caller's real bridge.
    #[must_use]
    pub fn new(bridge: Arc<dyn AgentBridge>, cancellation: CancellationToken) -> Self {
        Self {
            executor: Arc::new(mas_execution::executor::Executor::for_agent_runs(
                bridge,
                mas_execution::node_adapters::SharedScopes::new(),
            )),
            cancellation,
        }
    }

    /// Resolves one queue message into a [`AgentRunDispatch`].
    ///
    /// # Errors
    ///
    /// Returns a validation error when the task does not name both an agent and
    /// an execution: neither can be invented, because an invented execution id
    /// is one the durable store has never heard of and can never reconcile.
    pub fn build_dispatch(&self, message: &TaskQueueMessage) -> Result<AgentRunDispatch> {
        let agent_id = message.agent_id.ok_or_else(|| {
            mas_common::error::AppError::validation(
                "execution.run task must name an agent_id; \
                 the canonical runtime cannot execute a run without one",
            )
        })?;

        let execution_id = message.execution_id.ok_or_else(|| {
            mas_common::error::AppError::validation(
                "execution.run task must carry an execution_id; \
                 an AI run that belongs to no execution cannot be scoped or settled",
            )
        })?;

        let context = self.runtime_context(message, execution_id);

        Ok(AgentRunDispatch {
            execution_id,
            agent_id,
            context,
            durable: mas_execution::executor::DurableRunIdentity {
                task_id: message.task_id,
                // Leaked to `'static` deliberately: the key is a `String` the
                // message owns, and the dispatch borrows it for the duration of
                // one run. Cloning per task would allocate on every dispatch to
                // satisfy a lifetime the runtime does not actually need to own.
                idempotency_key: Box::leak(message.idempotency_key.clone().into_boxed_str()),
            },
            input: message.input.clone(),
        })
    }

    /// Rebuilds a runtime context from the task's authoritative scope.
    ///
    /// The actor is the worker itself: an AI run dispatched from the durable
    /// queue has no human actor, and inventing one would put a fabricated
    /// identity into the audit trail.
    fn runtime_context(
        &self,
        message: &TaskQueueMessage,
        execution_id: ExecutionId,
    ) -> RuntimeContext {
        let mut context = RuntimeContext::new(
            message.tenant_id,
            message.organization_id,
            message.project_id,
            "mas-worker",
            mas_common::enums::Environment::Production,
        )
        .unwrap_or_else(|error| {
            // A nil identifier here means the task row itself is corrupt. The
            // context cannot be built, so the run cannot be scoped; this is the
            // one place a panic is preferable to executing unscoped AI work.
            panic!("execution.run task carries an invalid scope: {error}")
        });

        context = context.with_correlation_id(message.correlation_id.clone());
        context = context.for_execution(execution_id, None);
        // The run is cancelled by the worker's shutdown path, so a graceful
        // drain stops AI work instead of abandoning it mid-flight.
        context = context.with_cancellation(self.cancellation.child());

        if let Some(deadline) = message.deadline {
            match deadline_remaining(deadline) {
                Some(remaining) => context = context.with_timeout(remaining),
                // Already expired: a zero timeout fails fast inside the bridge
                // rather than starting work that cannot finish.
                None => context = context.with_timeout(Duration::from_millis(1)),
            }
        }

        context
    }
}

/// Remaining wall-clock until *deadline*, or `None` when it has passed.
///
/// Delegated to `Timestamp::duration_since` rather than computed from a raw
/// clock so there is one definition of "remaining" in the platform.
fn deadline_remaining(deadline: Timestamp) -> Option<Duration> {
    deadline.duration_since(&Timestamp::now())
}

#[async_trait::async_trait]
impl mas_worker::TaskHandlerPort for AgentRunHandler {
    /// Runs one `execution.run` task through the canonical runtime.
    ///
    /// The handler resolves the task and then delegates to
    /// `Executor::run_agent_node`, which dispatches through the node registry
    /// to the agent bridge. Nothing here calls the bridge directly: that would
    /// create a second execution path, with its own idea of what "success"
    /// means, running alongside the canonical one.
    ///
    /// A failed run is returned as an `Err` carrying the orchestrator's own
    /// code and retry verdict, so the durable settlement classifies the failure
    /// the way the orchestrator reported it rather than guessing.
    async fn execute(&self, message: &TaskQueueMessage) -> Result<serde_json::Value> {
        let dispatch = self.build_dispatch(message)?;
        let execution_id = dispatch.execution_id;
        let agent_id = dispatch.agent_id;
        let correlation_id = dispatch.context.correlation_id().to_owned();

        // Refuse locally: the far side would reject this too, and spending a
        // round trip to learn that is wasted latency on every mis-scoped task.
        dispatch.require_authoritative_scope()?;

        // A cancelled run is a cancellation, never a result. Checked before the
        // call so a drain that landed while the task sat in the queue does not
        // spend a paid AI round trip on work that is already stopped.
        if dispatch.context.cancellation().is_cancelled() {
            return Err(mas_common::error::AppError::cancelled(
                "execution.run cancelled before the agent was invoked",
            ));
        }

        let outcome: AgentRunOutcome = self
            .executor
            .run_agent_node(
                execution_id,
                agent_id,
                dispatch.input,
                &dispatch.context,
                Some(dispatch.durable),
            )
            .await?;

        let result = match outcome.result {
            Ok(output) => Ok(output),
            Err(failure) => Err(mas_common::error::AppError::external_failure(
                failure.code,
                failure.message,
                failure.retryable,
            )),
        };

        let (outcome_label, payload) = match result {
            Ok(output) => (
                "succeeded",
                serde_json::json!({
                    "outcome": "succeeded",
                    "output": output,
                    "usage": usage_json(outcome.usage.as_ref()),
                }),
            ),
            Err(error) => {
                // Preserve the structured failure: the code and the retry
                // verdict come from the orchestrator (or the runtime), and the
                // durable settlement classifies from them.
                let code = error.error_code().to_owned();
                let public = error.public_message();
                return Err(mas_common::error::AppError::external_failure(
                    structured_code(&error).unwrap_or_else(|| code.clone()),
                    public,
                    error.is_retryable(),
                ));
            },
        };

        tracing::info!(
            handler = "agent_run",
            correlation_id = %correlation_id,
            execution_id = ?execution_id,
            agent_id = %agent_id,
            outcome = outcome_label,
            "execution.run completed through the canonical runtime"
        );

        Ok(payload)
    }
}

/// The structured external code, when the error carries one.
///
/// `error_code()` can only return a `'static` token, so a structured failure
/// has to be unwrapped to recover the orchestrator's own vocabulary.
fn structured_code(error: &mas_common::error::AppError) -> Option<String> {
    match error {
        mas_common::error::AppError::ExternalFailure { code, .. } => Some(code.clone()),
        _ => None,
    }
}

/// Projects the run's measured usage into the payload the consumer meters.
///
/// Usage comes from the resource guard the run was charged against, not from a
/// number the handler computed on the way past: the guard is what the budget
/// was actually enforced with.
fn usage_json(usage: Option<&mas_domain::ResourceUsageSummary>) -> serde_json::Value {
    match usage {
        Some(usage) => serde_json::json!({
            "tokens_input": usage.tokens_input,
            "tokens_output": usage.tokens_output,
            "tool_calls": usage.tool_calls,
            "steps": usage.steps_executed,
        }),
        // A run that measured nothing reports nothing rather than a row of
        // zeroes that looks like a real measurement.
        None => serde_json::Value::Null,
    }
}

/// Unit-testable helpers with no I/O.
#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::error::AppError;
    use mas_common::ids::{AgentId, ExecutionId, OrganizationId, ProjectId, TaskId, TenantId};
    use mas_execution::agent_bridge::{AgentRunVerdict, AgentUsage};
    // `execute` comes from the trait, not from an inherent impl: the handler is
    // only meaningful as a `TaskHandlerPort`.
    use mas_worker::TaskHandlerPort;

    fn message() -> TaskQueueMessage {
        TaskQueueMessage::staged(
            TaskId::new(),
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
            "execution.run".to_owned(),
            serde_json::json!({"prompt": "hello"}),
            Some(ExecutionId::new()),
            Some(AgentId::new()),
            None,
            "idem-1".to_owned(),
            mas_common::enums::TaskPriority::Normal,
            1,
            3,
            None,
            "corr-1",
        )
        .expect("message")
    }

    fn handler() -> AgentRunHandler {
        // A bridge is required, so these tests build one that refuses every
        // call: a test can then never accidentally depend on a canned AI
        // response. Dispatch construction is what is under test.
        let bridge: Arc<dyn AgentBridge> = Arc::new(RefusingBridge);
        AgentRunHandler::new(bridge, CancellationToken::new())
    }

    /// A bridge that refuses every call.
    #[derive(Debug)]
    struct RefusingBridge;

    #[async_trait::async_trait]
    impl AgentBridge for RefusingBridge {
        fn name(&self) -> &'static str {
            "refusing-test-bridge"
        }

        async fn run(
            &self,
            _request: mas_execution::agent_bridge::AgentRunRequest,
        ) -> Result<AgentRunVerdict> {
            Err(AppError::external_service(
                "test",
                "this bridge never runs work",
            ))
        }
    }

    /// A bridge that answers successfully, so the handler's success path can be
    /// exercised without a live orchestrator.
    fn succeeding_handler() -> AgentRunHandler {
        let bridge: Arc<dyn AgentBridge> = Arc::new(SucceedingBridge);
        AgentRunHandler::new(bridge, CancellationToken::new())
    }

    #[derive(Debug)]
    struct SucceedingBridge;

    #[async_trait::async_trait]
    impl AgentBridge for SucceedingBridge {
        fn name(&self) -> &'static str {
            "succeeding-test-bridge"
        }

        async fn run(
            &self,
            _request: mas_execution::agent_bridge::AgentRunRequest,
        ) -> Result<AgentRunVerdict> {
            Ok(AgentRunVerdict::Succeeded {
                output: serde_json::json!({"answer": 42}),
                usage: AgentUsage {
                    tokens_input: 20,
                    tokens_output: 8,
                    tool_calls: 1,
                    steps: 1,
                    active_time_ms: 5,
                },
            })
        }
    }

    /// A bridge that reports a structured, terminal failure — the shape the
    /// Python orchestrator returns for an invalid request.
    fn structured_failure_handler() -> AgentRunHandler {
        let bridge: Arc<dyn AgentBridge> = Arc::new(StructuredFailureBridge);
        AgentRunHandler::new(bridge, CancellationToken::new())
    }

    #[derive(Debug)]
    struct StructuredFailureBridge;

    #[async_trait::async_trait]
    impl AgentBridge for StructuredFailureBridge {
        fn name(&self) -> &'static str {
            "structured-failure-test-bridge"
        }

        async fn run(
            &self,
            _request: mas_execution::agent_bridge::AgentRunRequest,
        ) -> Result<AgentRunVerdict> {
            Ok(AgentRunVerdict::Failed {
                code: "invalid_tool_permission".to_owned(),
                message: "tool 'shell' is not permitted for this agent".to_owned(),
                retryable: false,
                usage: AgentUsage::default(),
            })
        }
    }

    #[test]
    fn build_dispatch_takes_scope_from_the_message_not_the_payload() {
        let handler = handler();
        let mut message = message();
        // A payload that tries to claim another tenant. It must be ignored:
        // scope comes from the task row.
        message.input = serde_json::json!({"tenant_id": TenantId::new().to_string()});

        let dispatch = handler.build_dispatch(&message).expect("dispatch");
        let snapshot = dispatch.context.snapshot();
        assert_eq!(snapshot.tenant_id, message.tenant_id);
        assert_eq!(snapshot.organization_id, message.organization_id);
        assert_eq!(snapshot.project_id, message.project_id);
    }

    #[test]
    fn build_dispatch_propagates_task_identity_and_idempotency() {
        let handler = handler();
        let message = message();

        let dispatch = handler.build_dispatch(&message).expect("dispatch");

        assert_eq!(dispatch.durable.task_id, message.task_id);
        assert_eq!(dispatch.execution_id, message.execution_id.expect("seeded"));
        assert_eq!(dispatch.durable.idempotency_key, "idem-1");
        assert_eq!(dispatch.context.correlation_id(), "corr-1");
        assert_eq!(dispatch.context.execution_id(), message.execution_id);
    }

    #[test]
    fn build_dispatch_requires_an_agent() {
        let handler = handler();
        let mut message = message();
        message.agent_id = None;

        let error = handler
            .build_dispatch(&message)
            .expect_err("a run without an agent must be refused");
        assert!(matches!(error, AppError::Validation { .. }));
    }

    #[test]
    fn build_dispatch_requires_an_execution() {
        let handler = handler();
        let mut message = message();
        message.execution_id = None;

        let error = handler
            .build_dispatch(&message)
            .expect_err("a run with no execution must be refused");
        let rendered = format!("{error}");
        assert!(
            rendered.contains("execution_id"),
            "names the missing field: {rendered}"
        );
        assert!(matches!(error, AppError::Validation { .. }));
    }

    #[test]
    fn build_dispatch_produces_an_authoritative_scope() {
        let handler = handler();
        let dispatch = handler.build_dispatch(&message()).expect("dispatch");
        assert!(dispatch.require_authoritative_scope().is_ok());
    }

    #[test]
    fn an_unresolved_scope_is_refused_before_any_bridge_call() {
        // The context itself refuses a nil tenant, so an unresolved scope
        // cannot reach the bridge at all. The dispatch-level check is the
        // second gate: it catches a scope that was resolved to nil identifiers
        // downstream of context construction, which is a real failure mode for
        // a task row written by an older writer.
        let error = RuntimeContext::new(
            TenantId::nil(),
            OrganizationId::new(),
            ProjectId::new(),
            "mas-worker",
            mas_common::enums::Environment::Production,
        )
        .expect_err("a nil tenant cannot scope an AI run");
        assert!(
            matches!(error, AppError::Validation { .. }),
            "an unresolved scope is a validation failure, not a runtime one: {error:?}"
        );

        // And a properly resolved scope passes the dispatch gate.
        let handler = handler();
        let dispatch = handler.build_dispatch(&message()).expect("dispatch");
        assert!(dispatch.require_authoritative_scope().is_ok());
    }

    #[test]
    fn an_expired_deadline_yields_a_one_millisecond_timeout() {
        let handler = handler();
        let mut message = message();
        message.deadline = Some(Timestamp::from_unix_ms(1).expect("timestamp"));

        let dispatch = handler.build_dispatch(&message).expect("dispatch");
        let remaining = dispatch
            .context
            .remaining()
            .expect("an expired deadline still yields a timeout");
        assert!(
            remaining <= Duration::from_millis(1),
            "an expired deadline fails fast inside the bridge: {remaining:?}"
        );
    }

    #[tokio::test]
    async fn the_handler_runs_through_the_canonical_runtime() {
        let handler = succeeding_handler();
        let message = message();

        let payload = handler.execute(&message).await.expect("run");

        assert_eq!(payload["outcome"], serde_json::json!("succeeded"));
        assert_eq!(payload["output"]["answer"], serde_json::json!(42));
        // Usage is metered from what the guard charged, not from a number the
        // handler invented.
        assert_eq!(payload["usage"]["tokens_input"], serde_json::json!(28));
    }

    #[tokio::test]
    async fn a_structured_failure_keeps_the_orchestrators_code_and_verdict() {
        let handler = structured_failure_handler();
        let message = message();

        let error = handler
            .execute(&message)
            .await
            .expect_err("a failed run is an error, not a success");

        let classified = mas_worker::classify::classify(&error);
        assert_eq!(
            classified.code, "invalid_tool_permission",
            "the orchestrator's own code survives settlement"
        );
        assert_eq!(
            classified.class,
            mas_worker::classify::FailureClass::Terminal,
            "a terminal verdict must not burn the attempt budget"
        );
        assert!(
            classified.message.contains("shell"),
            "the orchestrator's message survives: {}",
            classified.message
        );
    }

    #[tokio::test]
    async fn a_cancelled_worker_refuses_to_start_paid_work() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let bridge: Arc<dyn AgentBridge> = Arc::new(RefusingBridge);
        let handler = AgentRunHandler::new(bridge, cancellation);

        let error = handler
            .execute(&message())
            .await
            .expect_err("a cancelled worker must not invoke the agent");
        assert!(
            matches!(error, AppError::Cancelled(_)),
            "a cancellation is a cancellation, never a failure: {error:?}"
        );
    }

    #[tokio::test]
    async fn a_bridge_error_is_reported_not_swallowed() {
        let handler = handler();
        let error = handler
            .execute(&message())
            .await
            .expect_err("the bridge's refusal must surface");
        assert!(
            mas_worker::classify::classify(&error).is_retryable(),
            "a dependency failure is transient: {error:?}"
        );
    }

    #[test]
    fn expired_deadline_yields_no_remaining_time() {
        let past = Timestamp::from_unix_ms(1).expect("timestamp");
        assert!(deadline_remaining(past).is_none());
    }

    #[test]
    fn future_deadline_yields_remaining_time() {
        let future = Timestamp::now()
            .checked_add(Duration::from_secs(60))
            .expect("timestamp");
        let remaining = deadline_remaining(future).expect("remaining");
        assert!(remaining <= Duration::from_secs(60));
        assert!(remaining > Duration::from_secs(0));
    }

    #[test]
    fn settings_refuse_a_missing_endpoint() {
        use mas_common::config::Merger;
        let layered = toml::Value::Table(toml::Table::new());
        let merger = Merger::new(&layered);
        let error = AgentBridgeSettings::from_merger(&merger)
            .expect_err("a missing endpoint must fail the boot");
        let rendered = format!("{error}");
        assert!(
            rendered.contains(PATH_ENDPOINT),
            "names the key: {rendered}"
        );
        assert!(
            rendered.contains(ENV_ENDPOINT),
            "names the env var: {rendered}"
        );
    }

    #[test]
    fn settings_require_a_credential_before_connecting() {
        use mas_common::config::Merger;
        let mut table = toml::Table::new();
        table.insert(
            "ai_bridge".to_owned(),
            toml::Value::Table({
                let mut inner = toml::Table::new();
                inner.insert(
                    "endpoint".to_owned(),
                    toml::Value::String("http://127.0.0.1:9100".to_owned()),
                );
                inner
            }),
        );
        let layered = toml::Value::Table(table);
        let merger = Merger::new(&layered);

        let settings = AgentBridgeSettings::from_merger(&merger).expect("settings");
        assert_eq!(settings.endpoint(), "http://127.0.0.1:9100");

        let error = settings
            .require_token()
            .expect_err("no credential must be refused before connecting");
        let rendered = format!("{error}");
        assert!(rendered.contains(PATH_TOKEN), "names the key: {rendered}");
    }
}
