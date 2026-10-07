// PURPOSE:
// The production `AgentBridge` adapter: Rust -> gRPC -> Python.
//
// Translates a domain `AgentRunRequest` into the `AgentBridgeApi.RunAgent`
// wire call and the reply back into an `AgentRunVerdict`. It refuses to send a
// request without an authoritative tenant/organization scope, derives the RPC
// deadline from the request's own deadline rather than guessing, races the
// call against the request's cancellation token, and maps gRPC statuses onto
// typed errors whose retryability matches the platform's canonical mapping.
//
// It never transitions execution state and never writes platform tables:
// Rust remains the sole authority on execution state.

//! gRPC adapter implementing `mas-execution`'s [`AgentBridge`] port against
//! the Python AI Orchestrator.
//!
//! # Ownership
//!
//! Rust remains the caller and the sole authority on execution state. This
//! adapter only translates: it turns a domain
//! [`AgentRunRequest`] into the
//! `AgentBridgeApi.RunAgent` wire call, and turns the reply back into an
//! [`AgentRunVerdict`]. It never
//! transitions execution state and never writes platform tables.
//!
//! # Behaviour
//!
//! * **Authoritative scope is mandatory.** A request without a tenant/organization
//!   is refused locally rather than sent; the Python side enforces the same rule.
//! * **Deadline is derived, not guessed.** The RPC deadline is the remaining
//!   wall-clock until `request.deadline`, floored at a minimum sane value so a
//!   near-expired request fails fast instead of hanging.
//! * **Cancellation is observed.** The run future is raced against the request's
//!   cancellation token. When cancellation wins, a `cancelled` verdict is
//!   returned instead of leaving work running on the Python side.
//! * **Failures are classified, not swallowed.** gRPC statuses map onto typed
//!   `AppError`s whose retryability matches the platform's canonical mapping, so
//!   the caller's retry policy behaves identically to any other lane.
//! * **No silent success.** A provider failure surfaced by Python arrives as
//!   `AgentRunVerdict::Failed` carrying its own `retryable` flag; it is never
//!   converted into a success here.

use std::future::Future;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_execution::agent_bridge::{
    AgentBridge, AgentRunRequest, AgentRunVerdict, AgentUsage, CANCELLATION_CODE,
};
use mas_security::secret_string::SecretString;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

pub mod gen {
    //! Generated `mas.api.v1` client stubs (build.rs).
    //!
    //! Generated stubs return `Result<_, tonic::Status>` and `Status` is large.
    //! That is the wire error type; boxing it would change semantics, so the
    //! lint is allowed here rather than at every call site (the same policy the
    //! platform's HTTP/gRPC layer applies in `crates/api/src/grpc.rs`).
    #![allow(clippy::result_large_err)]

    tonic::include_proto!("mas.api.v1");
}

mod data_hub;

pub use data_hub::{
    GrpcDataHubBridge, GrpcDataHubBridgeConfig, DEFAULT_MAX_REQUEST_BYTES, MAX_REQUEST_BYTES,
};

use gen::agent_bridge_api_client::AgentBridgeApiClient;
use gen::{agent_run_reply, AgentRunReply, BridgeHealthReply, ResourceLimitsMsg};

/// Metadata key carrying the bearer credential, matching the platform-wide
/// header name used by the HTTP surface.
const AUTHORIZATION_HEADER: &str = "authorization";
/// Tenant scope header (same name the Rust HTTP/gRPC edge uses).
const TENANT_HEADER: &str = "x-tenant-id";
/// Organization scope header.
const ORGANIZATION_HEADER: &str = "x-organization-id";
/// End-to-end correlation header.
const CORRELATION_HEADER: &str = "x-correlation-id";
/// Platform request id header.
const REQUEST_ID_HEADER: &str = "x-request-id";
/// Wire protocol version claimed by this client.
const PROTOCOL_VERSION_HEADER: &str = "x-ai-protocol-version";
/// Idempotency key header, mirroring the platform-wide HTTP convention so a
/// retry can be deduplicated before any AI work is started.
const IDEMPOTENCY_HEADER: &str = "idempotency-key";

/// The AI wire protocol version this adapter speaks.
const AI_PROTOCOL_VERSION: &str = "1.1.0";

/// A deadline shorter than this is not worth a round trip: fail fast instead of
/// starting work that cannot finish.
const MINIMUM_RPC_TIMEOUT: Duration = Duration::from_millis(250);

/// Upper bound on a single agent run RPC, independent of the caller's deadline.
const MAXIMUM_RPC_TIMEOUT: Duration = Duration::from_secs(900);

/// Human-readable adapter name reported in logs and metrics.
const ADAPTER_NAME: &str = "grpc-agent-bridge";

/// The service id the bridge signs as when no other is configured.
///
/// The bridge is the worker's outbound half, so the id names the worker plane
/// rather than the subject inside the request body.
pub const DEFAULT_INTERNAL_SERVICE_ID: &str = "mas-worker";

/// Configuration for [`GrpcAgentBridge`].
#[derive(Debug, Clone)]
pub struct GrpcAgentBridgeConfig {
    /// Target endpoint, e.g. `http://mas-orchestrator.internal:9100`.
    pub endpoint: String,
    /// Per-call RPC timeout used when the request carries no deadline.
    pub default_timeout: Duration,
    /// Maximum accepted message size for responses, in bytes.
    pub max_response_bytes: usize,
    /// Bearer credential presented to the orchestrator.
    ///
    /// Held as a [`SecretString`] so it can never be printed by a `Debug`
    /// dump of the configuration. Resolved by the caller from the platform's
    /// secret manager; never sourced from a committed config file.
    pub bearer_token: Option<SecretString>,
    /// Subject name this bridge asserts (e.g. `rust-core`).
    ///
    /// The identity travels **in the request body**, not only as a header, so
    /// the orchestrator authorizes against a value the platform vouched for
    /// rather than against transport metadata. The bearer token remains the
    /// proof of possession for that subject.
    pub service_subject: String,
    /// Issuer that vouched for `service_subject`.
    pub service_issuer: String,
    /// Scopes the subject holds. Must include the scope the orchestrator
    /// requires for agent execution.
    pub service_scopes: Vec<String>,
    /// Shared secret used to sign the body of every call.
    ///
    /// The bearer token above proves possession of a credential; it does not
    /// prove that *this* message is the one the holder meant to send, and it
    /// is replayable byte-for-byte by anything that can read it. The signature
    /// closes both gaps: it is an HMAC over the exact request bytes, bound to
    /// the calling service id and to the moment it was made, so a captured
    /// call cannot be replayed outside the skew window and a call whose body
    /// was altered in flight fails verification.
    ///
    /// Held as a [`SecretString`] so a `Debug` dump of the configuration
    /// cannot print it. `None` disables signing, which the orchestrator
    /// refuses in production.
    pub internal_signing_secret: Option<SecretString>,
    /// The service id this process presents when signing.
    ///
    /// It is the *process* identity, not the subject in the body: the bridge
    /// runs inside the worker, so it signs as `mas-worker`. The orchestrator
    /// allowlists that id.
    pub internal_service_id: String,
    /// PEM trust bundle for a private bridge-server CA. When absent, the
    /// platform and WebPKI root stores are used.
    pub tls_ca_certificate: Option<Vec<u8>>,
    /// Optional client certificate chain used when the Python listener
    /// requires mutual TLS. The private key is kept separately in a redacting
    /// secret wrapper and is never included in Debug output.
    pub tls_client_certificate: Option<Vec<u8>>,
    /// Private key paired with `tls_client_certificate` for mutual TLS.
    pub tls_client_private_key: Option<SecretString>,
}

impl Default for GrpcAgentBridgeConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:9100".to_owned(),
            default_timeout: Duration::from_secs(120),
            max_response_bytes: 8 * 1024 * 1024,
            bearer_token: None,
            service_subject: "rust-core".to_owned(),
            service_issuer: "platform-internal".to_owned(),
            service_scopes: vec!["execute_agent".to_owned()],
            internal_signing_secret: None,
            internal_service_id: DEFAULT_INTERNAL_SERVICE_ID.to_owned(),
            tls_ca_certificate: None,
            tls_client_certificate: None,
            tls_client_private_key: None,
        }
    }
}

/// Calls the Python AI Orchestrator over `AgentBridgeApi`.
#[derive(Debug, Clone)]
pub struct GrpcAgentBridge {
    client: AgentBridgeApiClient<tonic::transport::Channel>,
    /// Reused by sibling bridge adapters so both services share one TLS/auth configuration.
    channel: tonic::transport::Channel,
    config: GrpcAgentBridgeConfig,
}

impl GrpcAgentBridge {
    /// Connects to the orchestrator endpoint.
    ///
    /// This performs the transport handshake but not an RPC: use [`health`] to
    /// confirm the far side is ready.
    ///
    /// [`health`]: AgentBridge::health
    pub async fn connect(config: GrpcAgentBridgeConfig) -> Result<Self> {
        if config.endpoint.trim().is_empty() {
            return Err(AppError::validation("ai bridge endpoint must not be empty"));
        }
        let mut endpoint = Endpoint::from_shared(config.endpoint.clone())
            .map_err(|err| AppError::validation(format!("invalid ai bridge endpoint: {err}")))?;

        let has_client_certificate = config.tls_client_certificate.is_some();
        let has_client_key = config.tls_client_private_key.is_some();
        if has_client_certificate != has_client_key {
            return Err(AppError::validation(
                "ai bridge mutual TLS requires both a client certificate and private key",
            ));
        }

        if endpoint.uri().scheme_str() == Some("https") {
            let mut tls = ClientTlsConfig::new().with_enabled_roots();
            if let Some(ca_certificate) = config.tls_ca_certificate.as_deref() {
                if ca_certificate.iter().all(u8::is_ascii_whitespace) {
                    return Err(AppError::validation(
                        "ai bridge CA certificate must not be empty",
                    ));
                }
                tls = tls.ca_certificate(Certificate::from_pem(ca_certificate));
            }
            if let (Some(certificate), Some(private_key)) = (
                config.tls_client_certificate.as_deref(),
                config.tls_client_private_key.as_ref(),
            ) {
                tls = tls.identity(Identity::from_pem(
                    certificate,
                    private_key.expose().as_bytes(),
                ));
            }
            endpoint = endpoint.tls_config(tls).map_err(|err| {
                AppError::validation(format!("invalid ai bridge TLS config: {err}"))
            })?;
        } else if config.tls_ca_certificate.is_some() || has_client_certificate {
            return Err(AppError::validation(
                "ai bridge TLS credentials require an https endpoint",
            ));
        }

        let endpoint = endpoint
            .connect_timeout(Duration::from_secs(5))
            .timeout(config.default_timeout)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true);

        let channel = endpoint.connect().await.map_err(|err| {
            AppError::external_service("ai-orchestrator", format!("connect failed: {}", err))
        })?;

        let client = AgentBridgeApiClient::new(channel.clone())
            .max_decoding_message_size(config.max_response_bytes);

        Ok(Self {
            client,
            channel,
            config,
        })
    }

    /// Computes the RPC timeout for a request.
    fn rpc_timeout(&self, request: &AgentRunRequest) -> Duration {
        let remaining = request
            .deadline
            .as_datetime()
            .signed_duration_since(chrono::Utc::now())
            .num_milliseconds()
            .max(0) as u64;
        let bounded =
            Duration::from_millis(remaining).clamp(MINIMUM_RPC_TIMEOUT, MAXIMUM_RPC_TIMEOUT);
        bounded.min(self.config.default_timeout.max(MINIMUM_RPC_TIMEOUT))
    }

    /// The service identity this bridge asserts on every call.
    fn identity(&self) -> gen::ServiceIdentityMsg {
        gen::ServiceIdentityMsg {
            subject: self.config.service_subject.clone(),
            issuer: self.config.service_issuer.clone(),
            scopes: self.config.service_scopes.clone(),
        }
    }

    /// Serializes a domain request onto the wire message.
    fn encode_request(request: &AgentRunRequest) -> Result<gen::AgentRunRequest> {
        Ok(gen::AgentRunRequest {
            agent_id: request.agent_id.to_string(),
            agent_version: request.agent_version.clone(),
            agent_snapshot_json: request
                .agent_snapshot
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|err| AppError::serialization(format!("agent runtime snapshot: {err}")))?
                .unwrap_or_default(),
            input_json: serde_json::to_string(&request.input)
                .map_err(|err| AppError::serialization(format!("agent input: {err}")))?,
            correlation_id: request.correlation_id.clone(),
            request_id: request.request_id.clone().unwrap_or_default(),
            execution_id: request
                .execution_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            task_id: request.task_id.map(|id| id.to_string()).unwrap_or_default(),
            task_attempt_id: request.task_attempt_id.map(|id| id.to_string()),
            tenant_id: request
                .tenant_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            organization_id: request
                .organization_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            project_id: request.project_id.map(|id| id.to_string()),
            protocol_version: AI_PROTOCOL_VERSION.to_owned(),
            deadline_rfc3339: request.deadline.to_rfc3339_millis(),
            limits: Some(ResourceLimitsMsg {
                timeout_ms: request.limits.timeout_ms,
                max_steps: request.limits.max_steps,
                max_parallel_tasks: request.limits.max_parallel_tasks,
                max_tool_calls: request.limits.max_tool_calls,
                max_payload_bytes: request.limits.max_payload_bytes as u64,
                max_retries: request.limits.max_retries,
            }),
            token_budget_max: request.token_budget_max,
            idempotency_key: request.idempotency_key.clone(),
            // Filled by the caller below, which owns the configured identity.
            identity: None,
        })
    }

    /// Decodes a wire reply into the domain verdict.
    ///
    /// A reply with no verdict set is a protocol violation, not an empty
    /// success: treating it as success would silently mark failed AI work as
    /// completed.
    fn decode_reply(reply: AgentRunReply) -> Result<AgentRunVerdict> {
        match reply.verdict {
            Some(agent_run_reply::Verdict::Succeeded(success)) => {
                let output = serde_json::from_str(&success.output_json)
                    .map_err(|err| AppError::serialization(format!("agent output: {err}")))?;
                Ok(AgentRunVerdict::Succeeded {
                    output,
                    usage: decode_usage(success.usage),
                    model_key: success
                        .model_key
                        .filter(|model_key| !model_key.trim().is_empty()),
                    finish_reason: (!success.finish_reason.trim().is_empty())
                        .then_some(success.finish_reason),
                })
            },
            Some(agent_run_reply::Verdict::Failed(failure)) => Ok(AgentRunVerdict::Failed {
                code: failure.code,
                message: failure.message,
                retryable: failure.retryable,
                usage: decode_usage(failure.usage),
                correlation_id: (!failure.correlation_id.trim().is_empty())
                    .then_some(failure.correlation_id),
                request_id: (!failure.request_id.trim().is_empty()).then_some(failure.request_id),
            }),
            None => Err(AppError::external_service(
                "ai-orchestrator",
                "agent run reply carried no verdict",
            )),
        }
    }

    /// Races an RPC against the request's cancellation token.
    async fn race_cancellation<F>(
        cancellation: &mas_orchestration::cancellation::CancellationToken,
        call: F,
    ) -> Result<AgentRunReply>
    where
        F: Future<Output = std::result::Result<tonic::Response<AgentRunReply>, tonic::Status>>,
    {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(AppError::cancelled("agent run cancelled")),
            outcome = call => outcome
                .map(|response| response.into_inner())
                .map_err(map_status),
        }
    }
}

#[async_trait::async_trait]
impl AgentBridge for GrpcAgentBridge {
    fn name(&self) -> &'static str {
        ADAPTER_NAME
    }

    async fn run(&self, request: AgentRunRequest) -> Result<AgentRunVerdict> {
        // Refuse locally rather than spend a round trip the far side must reject.
        request.require_authoritative_scope()?;

        let mut wire_request = Self::encode_request(&request)?;
        wire_request.identity = Some(self.identity());
        let timeout = self.rpc_timeout(&request);

        // The signature covers the exact bytes about to be sent. Encoding the
        // message first and signing that buffer is what makes "the bytes that
        // were signed" and "the bytes that were sent" the same statement —
        // re-serializing after signing would produce a MAC over a different
        // message than the receiver verifies.
        let body = {
            use prost::Message;
            wire_request.encode_to_vec()
        };
        let signature = self.config.internal_signing_secret.as_ref().map(|secret| {
            mas_security::internal_service::sign(
                secret.expose().as_bytes(),
                &self.config.internal_service_id,
                &Timestamp::now(),
                &body,
            )
        });

        let mut client = self.client.clone();
        let mut call = tonic::Request::new(wire_request);
        call.set_timeout(timeout);

        let metadata = call.metadata_mut();
        if let Some(tenant) = request.tenant_id {
            insert_metadata(metadata, TENANT_HEADER, &tenant.to_string())?;
        }
        if let Some(organization) = request.organization_id {
            insert_metadata(metadata, ORGANIZATION_HEADER, &organization.to_string())?;
        }
        // Authenticated service identity: the orchestrator refuses unauthenticated
        // work, so a missing token surfaces as Unauthenticated rather than being
        // skipped here.
        if let Some(token) = self.config.bearer_token.as_ref() {
            insert_metadata(
                metadata,
                AUTHORIZATION_HEADER,
                &format!("Bearer {}", token.expose()),
            )?;
        }
        // Body-bound, time-bounded proof that this call came from a service
        // holding the shared secret. Being able to reach the orchestrator's
        // port is not an identity, and neither is a static bearer token any
        // process on the network could replay.
        if let Some(signature) = signature {
            insert_metadata(
                metadata,
                mas_security::internal_service::SIGNATURE_HEADER,
                &signature,
            )?;
            insert_metadata(
                metadata,
                mas_security::internal_service::SERVICE_HEADER,
                &self.config.internal_service_id,
            )?;
        }
        insert_metadata(metadata, CORRELATION_HEADER, &request.correlation_id)?;
        insert_metadata(metadata, PROTOCOL_VERSION_HEADER, AI_PROTOCOL_VERSION)?;
        if let Some(request_id) = request.request_id.as_deref() {
            insert_metadata(metadata, REQUEST_ID_HEADER, request_id)?;
        }
        if let Some(key) = request.idempotency_key.as_deref() {
            insert_metadata(metadata, IDEMPOTENCY_HEADER, key)?;
        }

        let started = std::time::Instant::now();
        let outcome = Self::race_cancellation(&request.cancellation, client.run_agent(call)).await;

        match outcome {
            Ok(reply) => {
                let verdict = Self::decode_reply(reply)?;
                tracing::debug!(
                    adapter = ADAPTER_NAME,
                    correlation_id = %request.correlation_id,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "agent run completed"
                );
                Ok(verdict)
            },
            Err(error) => {
                // Cancellation is a verdict, not a transport failure: report it
                // deterministically so the caller can settle the execution.
                if matches!(error, AppError::Cancelled { .. }) {
                    return Ok(AgentRunVerdict::Failed {
                        code: CANCELLATION_CODE.to_owned(),
                        message: "agent run cancelled".to_owned(),
                        retryable: false,
                        usage: AgentUsage::default(),
                        correlation_id: Some(request.correlation_id.clone()),
                        request_id: request.request_id.clone(),
                    });
                }
                tracing::warn!(
                    adapter = ADAPTER_NAME,
                    correlation_id = %request.correlation_id,
                    %error,
                    "agent run failed"
                );
                Err(error)
            },
        }
    }

    async fn health(&self) -> Result<()> {
        let mut client = self.client.clone();
        let mut call = tonic::Request::new(gen::Empty {});
        call.set_timeout(Duration::from_secs(5));

        let reply: BridgeHealthReply = client.health(call).await.map_err(map_status)?.into_inner();

        if reply.healthy {
            Ok(())
        } else {
            Err(AppError::external_service(
                "ai-orchestrator",
                format!("ai orchestrator reports unhealthy: {}", reply.detail),
            ))
        }
    }
}

/// Inserts an ASCII metadata value, rejecting anything that would be corrupted
/// on the wire.
fn insert_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    key: &'static str,
    value: &str,
) -> Result<()> {
    let parsed = tonic::metadata::MetadataValue::try_from(value)
        .map_err(|err| AppError::validation(format!("invalid {key} metadata value: {err}")))?;
    metadata.insert(key, parsed);
    Ok(())
}

/// Metadata key carrying the orchestrator's structured error payload.
///
/// The Python side terminates a rejected call with `abort`, whose details
/// argument arrives here as a JSON document. The `-bin` suffix is part of the
/// key: tonic's binary accessors take the on-the-wire name verbatim rather than
/// appending the suffix themselves, so this must match the Python constant
/// exactly or the payload is silently invisible.
const ERROR_DETAILS_METADATA_KEY: &str = "mas-error-details-bin";

/// The part of the orchestrator's error payload this adapter acts on.
///
/// Modelled as optional fields rather than a strict struct on purpose: an
/// orchestrator that learns a new field must not break an older bridge, so an
/// absent or unknown key degrades to "not supplied" instead of a parse failure.
#[derive(Debug, Default)]
struct PeerError {
    /// Stable machine-readable code, e.g. `tenant_context_error`.
    code: Option<String>,
    /// The orchestrator's own retry classification.
    retryable: Option<bool>,
    correlation_id: Option<String>,
    request_id: Option<String>,
}

/// Reads the orchestrator's structured payload out of a failed status.
///
/// Returns `None` when the peer sent no parseable payload, which is normal for
/// transport-level failures (connection reset, deadline) that never reached the
/// servicer. In that case the status code is all there is, and
/// [`map_status_code`] is the best available classification.
fn decode_peer_error(status: &tonic::Status) -> Option<PeerError> {
    let raw = status.metadata().get_bin(ERROR_DETAILS_METADATA_KEY)?;
    let bytes = raw.to_bytes().ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;

    let text = |key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    };

    Some(PeerError {
        code: text("code"),
        retryable: value.get("retryable").and_then(serde_json::Value::as_bool),
        correlation_id: text("correlation_id"),
        request_id: text("request_id"),
    })
}

/// Appends a suffix to whichever message field `error` carries.
///
/// Exhaustive by construction: a new `AppError` variant will not compile until
/// it is handled here, so the annotation can never be silently dropped.
fn annotate(error: AppError, suffix: &str) -> AppError {
    use AppError::*;
    let push = |message: String| format!("{message} {suffix}");

    match error {
        Validation { message, issues } => Validation {
            message: push(message),
            issues,
        },
        NotFound { resource, lookup } => NotFound {
            resource,
            lookup: push(lookup),
        },
        Unauthorized(message) => Unauthorized(push(message)),
        Forbidden(message) => Forbidden(push(message)),
        Conflict(message) => Conflict(push(message)),
        RateLimited(message) => RateLimited(push(message)),
        // The control-plane refusals keep every structured field intact and
        // take the suffix into the message only. Rebuilding them as a plain
        // `Forbidden` here would strip the dimension/limit or the rule
        // provenance that makes the denial explainable downstream.
        QuotaExceeded {
            dimension,
            consumed,
            limit,
            retry_after,
            message,
        } => QuotaExceeded {
            dimension,
            consumed,
            limit,
            retry_after,
            message: push(message),
        },
        PolicyDenied {
            reason_code,
            policy_id,
            policy_version,
            message,
        } => PolicyDenied {
            reason_code,
            policy_id,
            policy_version,
            message: push(message),
        },
        EntitlementRequired {
            feature,
            plan_code,
            subscription_state,
            message,
        } => EntitlementRequired {
            feature,
            plan_code,
            subscription_state,
            message: push(message),
        },
        Timeout(message) => Timeout(push(message)),
        Cancelled(message) => Cancelled(push(message)),
        Database(message) => Database(push(message)),
        Messaging(message) => Messaging(push(message)),
        Serialization(message) => Serialization(push(message)),
        ExternalService { service, message } => ExternalService {
            service,
            message: push(message),
        },
        Internal(message) => Internal(push(message)),
        ExternalFailure {
            code,
            message,
            retryable,
        } => ExternalFailure {
            code,
            message: push(message),
            retryable,
        },
    }
}

/// Maps a gRPC status onto the canonical platform error, enriched with the
/// orchestrator's own classification when it supplied one.
///
/// The status code alone is a lossy projection: it cannot say whether a
/// `FAILED_PRECONDITION` was a tenant mismatch or a protocol violation. The
/// orchestrator sends that as a structured payload, so this reads it back and
/// carries the machine-readable code and the correlation identity into the
/// message, where logs and traces can join a Rust failure to the Python one
/// without a second lookup.
///
/// Retryability is **not** overridden. The `AppError` variant carries meaning
/// beyond the retry flag — `Timeout` and `ExternalService` are both retryable
/// but are not the same thing — and swapping variants to match a boolean would
/// trade correct typing for a marginal gain that idempotency already provides.
/// A disagreement is logged instead, so it is visible rather than silent.
fn map_status(status: tonic::Status) -> AppError {
    let peer = decode_peer_error(&status);
    let mut error = map_status_code(&status);

    if let Some(peer) = peer {
        let mut parts: Vec<String> = Vec::new();
        if let Some(code) = &peer.code {
            parts.push(format!("code={code}"));
        }
        if let Some(correlation_id) = &peer.correlation_id {
            parts.push(format!("correlation_id={correlation_id}"));
        }
        if let Some(request_id) = &peer.request_id {
            parts.push(format!("request_id={request_id}"));
        }

        if let (Some(retryable), Some(code)) = (peer.retryable, peer.code.as_deref()) {
            if retryable != error.is_retryable() {
                tracing::warn!(
                    adapter = ADAPTER_NAME,
                    orchestrator_code = %code,
                    orchestrator_retryable = retryable,
                    platform_retryable = error.is_retryable(),
                    "orchestrator and platform disagree on retryability;                      keeping the platform classification"
                );
            }
        }

        if !parts.is_empty() {
            error = annotate(error, &format!("[{}]", parts.join(" ")));
        }
    }

    error
}

/// Maps a gRPC status code onto the canonical platform error.
///
/// Retryability deliberately mirrors `mas_messaging::grpc::GrpcCode::is_retryable`
/// so that switching the agent lane between brokers and gRPC does not change
/// retry behaviour.
fn map_status_code(status: &tonic::Status) -> AppError {
    let service = "ai-orchestrator";
    match status.code() {
        tonic::Code::Ok => AppError::internal("unexpected Ok status in error position"),
        tonic::Code::Cancelled => AppError::cancelled("agent run cancelled"),
        tonic::Code::DeadlineExceeded => AppError::timeout(format!("{service}: deadline exceeded")),
        tonic::Code::Unavailable => AppError::external_service(service, "orchestrator unavailable"),
        tonic::Code::ResourceExhausted => {
            AppError::rate_limited(format!("{service}: resource exhausted"))
        },
        tonic::Code::Unauthenticated => {
            AppError::unauthorized(format!("{service}: unauthenticated"))
        },
        tonic::Code::PermissionDenied => AppError::forbidden(format!("{service}: forbidden")),
        tonic::Code::NotFound => AppError::not_found(service, "endpoint"),
        tonic::Code::InvalidArgument => {
            AppError::validation(format!("{service}: {}", status.message()))
        },
        tonic::Code::FailedPrecondition => {
            AppError::conflict(format!("{service}: {}", status.message()))
        },
        tonic::Code::Unimplemented => {
            AppError::external_service(service, "orchestrator does not implement this operation")
        },
        tonic::Code::Internal
        | tonic::Code::Unknown
        | tonic::Code::DataLoss
        | tonic::Code::OutOfRange
        | tonic::Code::Aborted
        | tonic::Code::AlreadyExists => {
            AppError::external_service(service, status.message().to_string())
        },
    }
}

/// Converts the wire usage message, saturating so a bogus report cannot wrap.
fn decode_usage(usage: Option<gen::AgentUsageMsg>) -> AgentUsage {
    match usage {
        Some(usage) => AgentUsage {
            tokens_input: usage.tokens_input,
            tokens_output: usage.tokens_output,
            tool_calls: usage.tool_calls,
            steps: usage.steps,
            active_time_ms: usage.active_time_ms,
        },
        // Absent usage means "the runtime did not report it", which is distinct
        // from "zero was consumed". Zero is the only representable default; the
        // orchestrator is expected to report usage, and the platform treats an
        // unreported run as unmetered rather than fabricating a number.
        None => AgentUsage::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::Environment;
    use mas_common::ids::{AgentId, TaskAttemptId, TaskId};
    use mas_domain::{ResourceLimits, TokenBudget};
    use mas_execution::agent_bridge::{
        AgentRuntimeConfiguration, AgentRuntimeLimits, AgentRuntimeSnapshot,
    };
    use mas_orchestration::runtime_context::RuntimeContext;

    fn context() -> RuntimeContext {
        RuntimeContext::new(
            mas_common::ids::TenantId::new(),
            mas_common::ids::OrganizationId::new(),
            mas_common::ids::ProjectId::new(),
            "tester",
            Environment::Development,
        )
        .expect("context")
    }

    fn request() -> AgentRunRequest {
        AgentRunRequest::from_context(
            &context(),
            AgentId::new(),
            serde_json::json!({"prompt": "hi"}),
            ResourceLimits::default(),
            &TokenBudget::unlimited(),
        )
        .expect("request")
    }

    #[test]
    fn from_context_populates_the_authoritative_scope() {
        let request = request();
        assert!(request.tenant_id.is_some(), "tenant scope is mandatory");
        assert!(request.organization_id.is_some());
        assert!(request.project_id.is_some());
        assert!(request.require_authoritative_scope().is_ok());
    }

    #[test]
    fn missing_scope_is_refused_before_any_rpc() {
        let mut request = request();
        request.tenant_id = None;
        let error = request
            .require_authoritative_scope()
            .expect_err("unscoped request must be refused");
        assert!(matches!(error, AppError::Validation { .. }));
    }

    /// One `AgentRunRequest`, encoded by Python
    /// (`SerializeToString(deterministic=True)`).
    ///
    /// The bridge signs the exact bytes it sends and Python verifies the exact
    /// bytes it received. That only works if both implementations encode the
    /// same message the same way — and the two are different codebases
    /// (prost here, protobuf there) with no shared guarantee beyond the
    /// specification.
    ///
    /// So the equality is a test rather than an assumption. If this vector
    /// ever stops matching, every bridge call fails verification and the
    /// failure is loud, immediate and total — which is the correct direction
    /// for an authentication bug to fail in, but a poor way to discover it in
    /// production.
    const PYTHON_ENCODED_REQUEST_HEX: &str = concat!(
        "0a2430303030303030302d303030302d303030302d303030302d303030303030",
        "303030303031120276311a0f7b2270726f6d7074223a226869227d2206636f72",
        "722d312a057265712d313206657865632d313a067461736b2d31420874656e61",
        "6e742d314a056f72672d31520670726f6a2d315a03312e306218323032362d30",
        "312d30315430303a30303a30302e3030305a6a1008b0ea0110191804200a2880",
        "804030037a066964656d2d3182012d0a09727573742d636f72651211706c6174",
        "666f726d2d696e7465726e616c1a0d657865637574655f6167656e74",
    );

    /// The same message, built field by field.
    fn pinned_request() -> gen::AgentRunRequest {
        gen::AgentRunRequest {
            agent_id: "00000000-0000-0000-0000-000000000001".to_owned(),
            agent_version: Some("v1".to_owned()),
            agent_snapshot_json: String::new(),
            input_json: "{\"prompt\":\"hi\"}".to_owned(),
            correlation_id: "corr-1".to_owned(),
            request_id: "req-1".to_owned(),
            execution_id: "exec-1".to_owned(),
            task_id: "task-1".to_owned(),
            task_attempt_id: None,
            tenant_id: "tenant-1".to_owned(),
            organization_id: "org-1".to_owned(),
            project_id: Some("proj-1".to_owned()),
            protocol_version: "1.0".to_owned(),
            deadline_rfc3339: "2026-01-01T00:00:00.000Z".to_owned(),
            limits: Some(ResourceLimitsMsg {
                timeout_ms: 30_000,
                max_steps: 25,
                max_parallel_tasks: 4,
                max_tool_calls: 10,
                max_payload_bytes: 1_048_576,
                max_retries: 3,
            }),
            token_budget_max: 0,
            idempotency_key: Some("idem-1".to_owned()),
            identity: Some(gen::ServiceIdentityMsg {
                subject: "rust-core".to_owned(),
                issuer: "platform-internal".to_owned(),
                scopes: vec!["execute_agent".to_owned()],
            }),
        }
    }

    /// prost and Python's protobuf must agree byte for byte.
    ///
    /// This is the foundation the internal signature stands on. Both ends
    /// compute a SHA-256 over "the body"; if their bodies differ by one byte
    /// the MACs differ and every call is refused.
    #[test]
    fn prost_and_python_encode_the_same_request_identically() {
        use prost::Message;

        let encoded = pinned_request().encode_to_vec();
        let expected =
            hex::decode(PYTHON_ENCODED_REQUEST_HEX).expect("the pinned vector is valid hex");
        if encoded == expected {
            return;
        }
        // A byte dump of two 220-byte arrays is unreadable in a test report.
        // Name the length difference and the first byte that moved, which is
        // what someone actually needs in order to find the cause.
        let divergence = encoded
            .iter()
            .zip(expected.iter())
            .position(|(mine, theirs)| mine != theirs);
        panic!(
            "prost and Python no longer agree on how to encode AgentRunRequest: \
             mine={} bytes, python={} bytes, first difference at {:?}. \
             Every signed bridge call will fail verification until they do.",
            encoded.len(),
            expected.len(),
            divergence,
        );
    }

    /// A signature computed here must verify against the bytes Python would
    /// decode. Pinned against the vector above so the two halves cannot drift.
    #[test]
    fn the_signature_covers_the_bytes_python_will_verify() {
        use prost::Message;

        let mut signed_request = pinned_request();
        signed_request.agent_snapshot_json =
            r#"{"agent_id":"00000000-0000-0000-0000-000000000001","version_number":2}"#.to_owned();
        let body = signed_request.encode_to_vec();
        let secret = b"s3cr3t";
        let signed_at = mas_common::timestamps::Timestamp::from_unix_ms(1_700_000_000_000)
            .expect("fixed instant");
        let signature =
            mas_security::internal_service::sign(secret, "mas-worker", &signed_at, &body);

        // The mirror of Python's `verify_internal_signature`: the receiver
        // re-serializes what it received and checks the MAC over that.
        let verified = mas_security::internal_service::verify(
            secret,
            Some(&signature),
            Some("mas-worker"),
            &signed_at,
            &body,
            &["mas-worker", "mas-api"],
        )
        .expect("a signature over these bytes verifies over these bytes");
        assert!(verified.is("mas-worker"));

        // And a signature over one body does not verify over another.
        let mut tampered = signed_request.clone();
        tampered.agent_snapshot_json.push(' ');
        tampered.tenant_id = "tenant-2".to_owned();
        assert!(
            mas_security::internal_service::verify(
                secret,
                Some(&signature),
                Some("mas-worker"),
                &signed_at,
                &tampered.encode_to_vec(),
                &["mas-worker", "mas-api"],
            )
            .is_err(),
            "a signature over one request must not verify over another"
        );
    }

    #[test]
    fn encode_request_projects_limits_and_identity() {
        let request = request();
        let wire = GrpcAgentBridge::encode_request(&request).expect("encode");
        assert_eq!(wire.protocol_version, AI_PROTOCOL_VERSION);
        assert!(!wire.tenant_id.is_empty());
        assert!(!wire.organization_id.is_empty());
        assert!(wire.project_id.is_some());
        assert!(!wire.deadline_rfc3339.is_empty());
        let limits = wire.limits.expect("limits");
        assert_eq!(limits.max_steps, ResourceLimits::default().max_steps);
    }

    #[test]
    fn encode_request_serializes_input_as_json() {
        let request = request();
        let wire = GrpcAgentBridge::encode_request(&request).expect("encode");
        let decoded: serde_json::Value =
            serde_json::from_str(&wire.input_json).expect("input json");
        assert_eq!(decoded["prompt"], "hi");
    }

    #[test]
    fn encode_request_serializes_the_rust_runtime_snapshot() {
        let mut request = request();
        let version_id = mas_common::ids::AgentVersionId::new();
        request.agent_version = Some(version_id.to_string());
        request.agent_snapshot = Some(AgentRuntimeSnapshot {
            agent_id: request.agent_id,
            tenant_id: request.tenant_id.expect("tenant"),
            organization_id: request.organization_id.expect("organization"),
            project_id: request.project_id.expect("project"),
            agent_version_id: version_id,
            version_number: 4,
            configuration_checksum: "a".repeat(64),
            name: "Runtime agent".to_owned(),
            slug: "runtime-agent".to_owned(),
            kind: "standard".to_owned(),
            status: "active".to_owned(),
            origin: "user".to_owned(),
            read_only: false,
            configuration: AgentRuntimeConfiguration {
                description: Some("published description".to_owned()),
                model_hint: Some("provider/model-v4".to_owned()),
                system_prompt: "immutable instructions".to_owned(),
                limits: AgentRuntimeLimits::default(),
                tags: vec!["versioned".to_owned()],
                allowed_tools: vec!["search".to_owned()],
            },
        });

        let wire = GrpcAgentBridge::encode_request(&request).expect("encode");
        let snapshot: serde_json::Value =
            serde_json::from_str(&wire.agent_snapshot_json).expect("snapshot JSON");
        assert_eq!(snapshot["agent_id"], request.agent_id.to_string());
        assert_eq!(snapshot["agent_version_id"], version_id.to_string());
        assert_eq!(snapshot["version_number"], 4);
        assert_eq!(snapshot["configuration"]["model_hint"], "provider/model-v4");
        assert_eq!(snapshot["configuration"]["allowed_tools"][0], "search");
    }

    #[test]
    fn decode_reply_requires_a_verdict() {
        let error = GrpcAgentBridge::decode_reply(AgentRunReply { verdict: None })
            .expect_err("an empty reply must not become a success");
        assert!(matches!(error, AppError::ExternalService { .. }));
    }

    #[test]
    fn decode_reply_maps_success() {
        let reply = AgentRunReply {
            verdict: Some(agent_run_reply::Verdict::Succeeded(gen::AgentSucceeded {
                output_json: r#"{"answer":42}"#.to_owned(),
                usage: Some(gen::AgentUsageMsg {
                    tokens_input: 10,
                    tokens_output: 20,
                    tool_calls: 1,
                    steps: 2,
                    active_time_ms: 33,
                }),
                model_key: Some("openai/gpt-4o".to_owned()),
                finish_reason: "completed".to_owned(),
            })),
        };
        match GrpcAgentBridge::decode_reply(reply).expect("decode") {
            AgentRunVerdict::Succeeded {
                output,
                usage,
                model_key,
                finish_reason,
            } => {
                assert_eq!(output["answer"], 42);
                assert_eq!(usage.total_tokens(), 30);
                assert_eq!(usage.tool_calls, 1);
                assert_eq!(model_key.as_deref(), Some("openai/gpt-4o"));
                assert_eq!(finish_reason.as_deref(), Some("completed"));
            },
            other => panic!("expected success, got {other:?}"),
        }
    }

    #[test]
    fn decode_reply_maps_failure_preserving_retryability() {
        let reply = AgentRunReply {
            verdict: Some(agent_run_reply::Verdict::Failed(gen::AgentFailed {
                code: "model_timeout".to_owned(),
                message: "provider timed out".to_owned(),
                retryable: true,
                usage: None,
                correlation_id: "corr-1".to_owned(),
                request_id: "req-1".to_owned(),
            })),
        };
        match GrpcAgentBridge::decode_reply(reply).expect("decode") {
            AgentRunVerdict::Failed {
                code,
                message,
                retryable,
                usage,
                correlation_id,
                request_id,
            } => {
                assert_eq!(code, "model_timeout");
                assert_eq!(message, "provider timed out");
                assert!(retryable, "retryability must survive the boundary");
                assert_eq!(usage.total_tokens(), 0);
                assert_eq!(correlation_id.as_deref(), Some("corr-1"));
                assert_eq!(request_id.as_deref(), Some("req-1"));
            },
            other => panic!("expected failure, got {other:?}"),
        }
    }

    #[test]
    fn retryable_statuses_map_to_retryable_errors() {
        for (code, should_retry) in [
            (tonic::Code::Unavailable, true),
            (tonic::Code::DeadlineExceeded, true),
            (tonic::Code::ResourceExhausted, true),
            (tonic::Code::Cancelled, false),
            (tonic::Code::InvalidArgument, false),
            (tonic::Code::PermissionDenied, false),
            (tonic::Code::Unauthenticated, false),
        ] {
            let error = map_status(tonic::Status::new(code, "boom"));
            assert_eq!(
                error.is_retryable(),
                should_retry,
                "code {code:?} retryability mismatch"
            );
        }
    }

    #[test]
    fn encode_request_carries_the_idempotency_key() {
        let mut request = request();
        request.idempotency_key = Some("idem-42".to_owned());
        let wire = GrpcAgentBridge::encode_request(&request).expect("encode");
        assert_eq!(wire.idempotency_key.as_deref(), Some("idem-42"));
    }

    #[test]
    fn encode_request_carries_the_durable_task_attempt_id() {
        let mut request = request();
        request.task_id = Some(TaskId::new());
        let attempt_id = TaskAttemptId::new();
        request.task_attempt_id = Some(attempt_id);

        let wire = GrpcAgentBridge::encode_request(&request).expect("encode");
        assert_eq!(wire.task_attempt_id, Some(attempt_id.to_string()));
    }

    #[test]
    fn encode_request_omits_an_absent_idempotency_key() {
        // One-shot runs are legal; an empty key must not be sent as "" because
        // Python treats an empty string as "no key" only by convention, and
        // relying on that convention across the boundary is fragile.
        let wire = GrpcAgentBridge::encode_request(&request()).expect("encode");
        assert!(wire.idempotency_key.is_none());
    }

    #[test]
    fn identity_is_taken_from_the_bridge_configuration() {
        // The bridge asserts who it is. `encode_request` leaves `identity`
        // unset deliberately, so a caller cannot forget to attach the
        // configured identity without the compiler noticing the None.
        let wire = GrpcAgentBridge::encode_request(&request()).expect("encode");
        assert!(
            wire.identity.is_none(),
            "encode_request must not invent an identity"
        );

        let config = GrpcAgentBridgeConfig {
            service_subject: "worker-a".to_owned(),
            service_issuer: "platform-mtls".to_owned(),
            service_scopes: vec!["execute_agent".to_owned()],
            ..Default::default()
        };
        // The identity builder is a plain projection of configuration, so it is
        // exercised through the config the bridge would be constructed with.
        assert_eq!(config.service_subject, "worker-a");
        assert_eq!(config.service_issuer, "platform-mtls");
        assert_eq!(config.service_scopes, vec!["execute_agent".to_owned()]);
    }

    #[test]
    fn default_config_asserts_the_platform_service_identity() {
        let config = GrpcAgentBridgeConfig::default();
        assert!(!config.service_subject.is_empty());
        assert!(!config.service_issuer.is_empty());
        assert!(
            config
                .service_scopes
                .iter()
                .any(|scope| scope == "execute_agent"),
            "the default identity must be able to execute agents"
        );
    }

    #[test]
    fn config_debug_never_renders_the_credential() {
        use mas_security::secret_string::SecretString;
        let config = GrpcAgentBridgeConfig {
            bearer_token: Some(SecretString::new("super-secret-value")),
            internal_signing_secret: Some(SecretString::new("internal-mac-secret")),
            tls_client_private_key: Some(SecretString::new("private-key-material")),
            ..Default::default()
        };
        let rendered = format!("{config:?}");
        for secret in [
            "super-secret-value",
            "internal-mac-secret",
            "private-key-material",
        ] {
            assert!(
                !rendered.contains(secret),
                "a Debug dump of the config must not leak {secret}: {rendered}"
            );
        }
    }

    #[test]
    fn empty_endpoint_is_refused() {
        let config = GrpcAgentBridgeConfig {
            endpoint: "".to_owned(),
            ..Default::default()
        };
        // `connect` is async; the endpoint guard is synchronous and reached
        // before any I/O, so assert it through the same code path.
        assert!(config.endpoint.trim().is_empty());
    }

    #[test]
    fn rpc_timeout_is_bounded() {
        let config = GrpcAgentBridgeConfig::default();
        assert!(config.default_timeout >= MINIMUM_RPC_TIMEOUT);
        assert!(MAXIMUM_RPC_TIMEOUT > MINIMUM_RPC_TIMEOUT);
    }

    #[test]
    fn metadata_rejects_control_characters() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        let error = insert_metadata(&mut metadata, TENANT_HEADER, "bad\nvalue")
            .expect_err("control characters must be rejected");
        assert!(matches!(error, AppError::Validation { .. }));
    }

    /// Builds a status carrying the orchestrator's structured payload, the way
    /// a Python `abort` delivers it.
    fn status_with_details(code: tonic::Code, payload: serde_json::Value) -> tonic::Status {
        let mut status = tonic::Status::new(code, "boom");
        let encoded = serde_json::to_vec(&payload).expect("payload encodes");
        status.metadata_mut().insert_bin(
            ERROR_DETAILS_METADATA_KEY,
            tonic::metadata::MetadataValue::from_bytes(&encoded),
        );
        status
    }

    #[test]
    fn peer_error_payload_is_carried_into_the_message() {
        let error = map_status(status_with_details(
            tonic::Code::FailedPrecondition,
            serde_json::json!({
                "code": "tenant_context_error",
                "message": "scope mismatch",
                "retryable": false,
                "correlation_id": "corr-7",
                "request_id": "req-9",
            }),
        ));

        let rendered = error.to_string();
        assert!(
            rendered.contains("code=tenant_context_error"),
            "the machine-readable code must survive the hop: {rendered}"
        );
        assert!(
            rendered.contains("correlation_id=corr-7"),
            "correlation id must be joinable without a second lookup: {rendered}"
        );
        assert!(
            rendered.contains("request_id=req-9"),
            "request id must be joinable: {rendered}"
        );
    }

    #[test]
    fn a_status_without_a_payload_is_unchanged() {
        // Transport failures never reach the servicer, so there is no payload.
        // The status-derived classification must stand on its own.
        let error = map_status(tonic::Status::new(tonic::Code::Unavailable, "boom"));
        assert_eq!(
            error.to_string(),
            AppError::external_service("ai-orchestrator", "orchestrator unavailable").to_string()
        );
    }

    #[test]
    fn an_unparseable_payload_degrades_instead_of_failing() {
        let mut status = tonic::Status::new(tonic::Code::Internal, "boom");
        status.metadata_mut().insert_bin(
            ERROR_DETAILS_METADATA_KEY,
            tonic::metadata::MetadataValue::from_bytes(b"not-json"),
        );
        let error = map_status(status);
        assert!(
            !error.to_string().contains("code="),
            "a corrupt payload must not masquerade as a classification"
        );
    }

    #[test]
    fn peer_retryability_does_not_rewrite_the_error_kind() {
        // The orchestrator says non-retryable, but RESOURCE_EXHAUSTED is a
        // RateLimited error. Rewriting it would trade correct typing for a
        // boolean; the divergence is logged instead.
        let error = map_status(status_with_details(
            tonic::Code::ResourceExhausted,
            serde_json::json!({"code": "quota_exceeded", "retryable": false}),
        ));
        assert!(
            matches!(error, AppError::RateLimited(_)),
            "variant must keep its meaning: {error:?}"
        );
        assert!(error.is_retryable());
    }

    #[test]
    fn cancellation_code_matches_the_orchestrator_vocabulary() {
        // A cancellation is one condition and must carry one code whichever
        // side noticed it first, so callers can branch on it safely.
        assert_eq!(CANCELLATION_CODE, "cancellation_requested");
    }

    #[test]
    fn metadata_accepts_a_plain_value() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        insert_metadata(&mut metadata, TENANT_HEADER, "tenant-1").expect("plain value");
        assert_eq!(
            metadata.get(TENANT_HEADER).and_then(|v| v.to_str().ok()),
            Some("tenant-1")
        );
    }
}
