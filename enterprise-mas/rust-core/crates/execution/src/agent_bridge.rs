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
    AgentId, AgentVersionId, ExecutionId, OrganizationId, ProjectId, TaskAttemptId, TaskId,
    TenantId,
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

/// Execution limits the Python runtime must honour for one agent.
///
/// This is the *agent-defined* envelope, distinct from the per-run
/// [`mas_domain::ResourceLimits`] carried by [`AgentRunRequest`]: these values
/// come from the immutable agent version, while the request limits come from
/// the caller and the platform's own budgets. The runtime enforces whichever
/// is tighter.
///
/// It is serialized field-for-field into `configuration.limits` of the signed
/// runtime snapshot, so the JSON names are contract (`snake_case`, no renames).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRuntimeLimits {
    /// Maximum number of execution steps the agent may take.
    pub max_steps: u32,
    /// Maximum tool invocations. `0` means "no tool use permitted".
    pub max_tool_calls: u32,
    /// Maximum delegation depth for supervisor agents.
    pub max_delegation_depth: u32,
    /// Wall-clock timeout for one run, in seconds. `None` means "inherit the
    /// request deadline"; it is serialized as JSON `null` rather than omitted
    /// because the runtime's snapshot schema is closed (unknown *and* missing
    /// keys are both rejected).
    pub timeout_seconds: Option<f64>,
}

impl Default for AgentRuntimeLimits {
    fn default() -> Self {
        Self {
            max_steps: 100,
            max_tool_calls: 50,
            max_delegation_depth: 0,
            timeout_seconds: None,
        }
    }
}

/// The agent configuration an executable runtime needs, projected from the
/// immutable [`mas_domain::AgentVersion`] rather than from live mutable state.
///
/// The field set is the closed `configuration` object of the runtime snapshot
/// schema (`grpc/agent_snapshot.py`); adding or removing a field is a protocol
/// change on both sides.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRuntimeConfiguration {
    /// Operator-facing description.
    pub description: Option<String>,
    /// Opaque model/endpoint reference resolved by the Python runtime. Rust
    /// never interprets it.
    pub model_hint: Option<String>,
    /// The versioned system prompt.
    pub system_prompt: String,
    /// Agent-defined execution limits.
    pub limits: AgentRuntimeLimits,
    /// Free-form tags.
    pub tags: Vec<String>,
    /// Tool identifiers the agent is permitted to invoke. The Python runtime
    /// rejects unknown tools and Rust enforces the same list at the tool
    /// permission boundary.
    pub allowed_tools: Vec<String>,
}

/// A closed, tenant-scoped, immutable view of one agent version.
///
/// The worker resolves this from PostgreSQL inside an authoritative-scope
/// transaction and pins it onto the outbound run, so the Python runtime never
/// falls back to its own catalog for agent definitions. That is what keeps
/// Rust the sole authority on which agent version actually executes.
///
/// The JSON projection of this struct is the signed `agent_snapshot_json`
/// body; its schema is closed (`deny_unknown_fields` on the way out is the
/// Python parser's job, but this struct must serialize exactly the documented
/// keys and nothing else).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRuntimeSnapshot {
    /// Agent identity.
    pub agent_id: AgentId,
    /// Authoritative tenant scope the agent belongs to.
    pub tenant_id: TenantId,
    /// Authoritative organization scope.
    pub organization_id: OrganizationId,
    /// Authoritative project scope.
    pub project_id: ProjectId,
    /// The exact immutable version being executed.
    pub agent_version_id: AgentVersionId,
    /// Monotonic per-agent version number, `>= 1`.
    pub version_number: u64,
    /// Lowercase hex SHA-256 of the versioned configuration.
    pub configuration_checksum: String,
    /// Human-readable name.
    pub name: String,
    /// URL-safe slug.
    pub slug: String,
    /// Agent kind as its canonical wire string (`standard`, `supervisor`,
    /// `tool_user`).
    pub kind: String,
    /// Lifecycle status as its canonical wire string. Only `active` is
    /// executable; the runtime rejects anything else.
    pub status: String,
    /// Ownership class (`user` or `system`).
    pub origin: String,
    /// Whether the agent is platform-owned and therefore immutable for the
    /// tenant. Derived from `origin`, not stored separately.
    pub read_only: bool,
    /// The versioned runtime configuration.
    pub configuration: AgentRuntimeConfiguration,
}

/// Source of the versioned configuration that an [`AgentRuntimeConfiguration`]
/// is projected from.
///
/// The published `configuration_snapshot` JSON is written by the agent-publish
/// use case and is the authoritative record of what was published. It is
/// therefore the only input this projection accepts: reading the live mutable
/// [`mas_domain::Agent`] instead would let a post-publish edit silently change
/// what "version N" means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentSnapshotMetadataSource<'a> {
    /// The versioned, immutable snapshot document.
    PublishedSnapshot(&'a serde_json::Value),
}

impl AgentRuntimeConfiguration {
    /// Projects the closed runtime configuration out of a published version
    /// snapshot.
    ///
    /// Recognised keys (all optional except `system_prompt`):
    /// `description`, `model_hint`, `system_prompt`, `limits`, `tags`,
    /// `allowed_tools`.
    ///
    /// A document that is not a JSON object is refused rather than coerced.
    /// Missing optional fields take their documented defaults; a missing or
    /// non-string `system_prompt` is an error, because silently running an
    /// agent with an empty system prompt would be a behavioural change nobody
    /// asked for.
    pub fn from_published_snapshot(
        snapshot: AgentSnapshotMetadataSource<'_>,
    ) -> Result<AgentRuntimeConfiguration> {
        let AgentSnapshotMetadataSource::PublishedSnapshot(document) = snapshot;
        let object = document.as_object().ok_or_else(|| {
            AppError::invalid_field(
                "configuration_snapshot",
                "invalid_type",
                "published agent configuration must be a JSON object",
            )
        })?;

        let optional_string = |key: &str| -> Result<Option<String>> {
            match object.get(key) {
                None | Some(serde_json::Value::Null) => Ok(None),
                Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
                Some(_) => Err(AppError::invalid_field(
                    "configuration_snapshot",
                    "invalid_type",
                    format!("agent configuration field `{key}` must be a string"),
                )),
            }
        };

        let system_prompt = match object.get("system_prompt") {
            Some(serde_json::Value::String(value)) => value.clone(),
            Some(_) => {
                return Err(AppError::invalid_field(
                    "configuration_snapshot",
                    "invalid_type",
                    "agent configuration field `system_prompt` must be a string",
                ))
            },
            None => {
                return Err(AppError::invalid_field(
                    "configuration_snapshot",
                    "missing",
                    "published agent configuration has no system_prompt",
                ))
            },
        };

        let string_list = |key: &str| -> Result<Vec<String>> {
            match object.get(key) {
                None | Some(serde_json::Value::Null) => Ok(Vec::new()),
                Some(serde_json::Value::Array(items)) => items
                    .iter()
                    .map(|item| {
                        item.as_str().map(str::to_owned).ok_or_else(|| {
                            AppError::invalid_field(
                                "configuration_snapshot",
                                "invalid_type",
                                format!("agent configuration list `{key}` must contain strings"),
                            )
                        })
                    })
                    .collect(),
                Some(_) => Err(AppError::invalid_field(
                    "configuration_snapshot",
                    "invalid_type",
                    format!("agent configuration field `{key}` must be a list of strings"),
                )),
            }
        };

        let limits = match object.get("limits") {
            None | Some(serde_json::Value::Null) => AgentRuntimeLimits::default(),
            Some(value) => serde_json::from_value(value.clone()).map_err(|error| {
                AppError::invalid_field(
                    "configuration_snapshot",
                    "invalid_type",
                    format!("agent configuration `limits` is malformed: {error}"),
                )
            })?,
        };

        Ok(Self {
            description: optional_string("description")?,
            model_hint: optional_string("model_hint")?,
            system_prompt,
            limits,
            tags: string_list("tags")?,
            allowed_tools: string_list("allowed_tools")?,
        })
    }
}

impl AgentRuntimeSnapshot {
    /// Projects an executable snapshot from the loaded aggregate and the
    /// selected immutable version.
    ///
    /// Both aggregates are rechecked here, so a caller that forgets to filter
    /// cannot produce a snapshot whose identity disagrees with its own
    /// version: the version must belong to the agent, and the agent must be
    /// executable.
    pub fn from_domain(
        agent: &mas_domain::Agent,
        version: &mas_domain::AgentVersion,
    ) -> Result<Self> {
        if version.agent_id() != agent.id {
            return Err(AppError::validation(
                "selected agent version does not belong to the resolved agent",
            ));
        }
        if !agent.is_executable() {
            return Err(AppError::validation(
                "agent is not executable in its current lifecycle state",
            ));
        }
        if version.version_number() == 0 {
            return Err(AppError::validation(
                "agent version number must be positive",
            ));
        }
        let configuration = AgentRuntimeConfiguration::from_published_snapshot(
            AgentSnapshotMetadataSource::PublishedSnapshot(version.configuration_snapshot()),
        )?;
        let read_only = agent.origin == mas_domain::AgentOrigin::System;
        Ok(Self {
            agent_id: agent.id,
            tenant_id: agent.tenant_id,
            organization_id: agent.organization_id,
            project_id: agent.project_id,
            agent_version_id: version.id(),
            version_number: version.version_number(),
            configuration_checksum: version.configuration_checksum().to_owned(),
            name: agent.name.clone(),
            slug: agent.slug.as_str().to_owned(),
            kind: agent.kind.as_str().to_owned(),
            status: agent.status.as_str().to_owned(),
            origin: agent.origin.as_str().to_owned(),
            read_only,
            configuration,
        })
    }

    /// Serialises the snapshot into the closed JSON body the Python runtime
    /// validates, and enforces the same 512 KiB ceiling it enforces so an
    /// oversized snapshot fails here (with the caller's identity in the log)
    /// rather than across the wire.
    pub fn to_wire_json(&self) -> Result<String> {
        const MAX_SNAPSHOT_BYTES: usize = 512 * 1024;
        let encoded = serde_json::to_string(self).map_err(|error| {
            AppError::serialization(format!("agent runtime snapshot JSON: {error}"))
        })?;
        if encoded.len() > MAX_SNAPSHOT_BYTES {
            return Err(AppError::validation(format!(
                "agent runtime snapshot is {} bytes; the runtime accepts at most {}",
                encoded.len(),
                MAX_SNAPSHOT_BYTES
            )));
        }
        Ok(encoded)
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

    /// The Rust-resolved, signed agent snapshot pinned to this run.
    ///
    /// A production adapter MUST populate this before crossing the trust
    /// boundary and the Python runtime refuses a run without one. It is
    /// optional in the type so that in-process callers which never cross the
    /// boundary (workflow node executors, unit fixtures) need not fabricate a
    /// snapshot they did not resolve.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_snapshot: Option<AgentRuntimeSnapshot>,

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
            // A workflow node execution resolves its agent in-process; the
            // cross-boundary worker path attaches the snapshot through
            // `ResolvedAgentBridge` instead.
            agent_snapshot: None,
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

/// Resolves the immutable agent snapshot a run must execute against.
///
/// This is a separate port from [`AgentBridge`] on purpose: resolving the
/// snapshot is a Rust-owned, tenant-scoped database read, while running the
/// agent crosses into Python. Keeping them apart lets the transport adapter
/// stay purely a transport, and lets the worker wrap any bridge with a
/// resolver without the bridge knowing where snapshots come from.
#[async_trait::async_trait]
pub trait AgentRuntimeSnapshotResolver: Send + Sync + fmt::Debug {
    /// Resolves the exact version the request asked for (or the current
    /// published version when the request pins none).
    ///
    /// Implementations must already be inside an RLS-armed transaction or open
    /// one, and must recheck every identifier in the returned snapshot against
    /// the request scope.
    async fn resolve(&self, request: &AgentRunRequest) -> Result<AgentRuntimeSnapshot>;
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
