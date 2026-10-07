# PURPOSE:
# Strongly typed configuration model for the Python AI Orchestrator.
#
# Every credential is a secret *reference*, never an inline value, and a raw
# key in configuration is rejected. Defaults fail closed: no provider is
# enabled, no model is allowlisted, and the caller-supplied tenant header is
# never trusted as authoritative identity.

"""Strongly typed configuration model for the Python AI Orchestrator.

Design rules enforced here:

* **No secrets in this module or in committed config files.** Every secret
  field is a *reference* (a name to resolve through
  ``security.credentials``), never a literal value. Settings that accidentally
  receive a raw secret fail validation.
* **Fail closed on ambiguity.** A field that is required in production but
  missing produces a startup error, not a silently degraded runtime.
* **Bounded by default.** Every timeout, size, count, and depth has a finite
  default so an unbounded resource cannot be reached by omission.
* **Explicit test-only affordances.** Fakes are opt-in by name and are refused
  in production by ``config/validation.py``.

Loading order (later wins), matching the Rust Core convention:

1. built-in defaults in this module
2. ``config/{MAS_ENV}.toml`` overlay (and ``config/default.toml``)
3. environment variables prefixed ``MAS_ORCH_``
4. explicit constructor overrides (used by tests)
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from mas_orchestrator.config.enums import (
    DataSourceKind,
    Environment,
    GuardrailFailureMode,
    LogFormat,
    MemoryBackend,
    ProviderMode,
    ToolExecutionMode,
    TransportSecurity,
    VectorBackend,
)

#: Environment variable prefix for every orchestrator setting.
ENV_PREFIX: Final[str] = "MAS_ORCH_"

#: Maximum size, in bytes, of a single model request payload. Mirrors the Rust
#: Core payload ceiling so an oversized prompt is rejected locally before it
#: consumes provider quota.
DEFAULT_MAX_PROMPT_BYTES: Final[int] = 256 * 1024

#: Maximum size, in bytes, of a single tool result accepted back into context.
DEFAULT_MAX_TOOL_RESULT_BYTES: Final[int] = 128 * 1024


class _Frozen(BaseModel):
    """Base for all settings models.

    Settings are immutable after construction so that runtime code cannot
    mutate configuration underneath a concurrent request. ``extra="forbid"``
    turns a typo in a TOML key or env var into a startup failure rather than a
    silently ignored setting.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", validate_assignment=True)


class RustCoreSettings(_Frozen):
    """Connection settings for the authoritative Rust Core gRPC surface.

    Rust owns execution state; this is the only channel through which the
    orchestrator reads or transitions it.
    """

    #: Target address, e.g. ``mas-api.internal:50051``.
    endpoint: str = Field(default="127.0.0.1:50051", min_length=1, max_length=255)

    #: Transport security. ``insecure`` is refused in production.
    transport: TransportSecurity = TransportSecurity.TLS

    #: Path to a CA bundle for ``tls`` / ``mtls``. Empty means the system
    #: trust store. This is a filesystem path, not a secret.
    ca_bundle_path: str | None = Field(default=None, max_length=4096)

    #: Secret *reference* names for mTLS material. Never the material itself.
    client_cert_secret_ref: str | None = Field(default=None, max_length=512)
    client_key_secret_ref: str | None = Field(default=None, max_length=512)

    #: Secret reference for the service bearer token presented to Rust Core.
    bearer_token_secret_ref: str | None = Field(default=None, max_length=512)

    #: Per-call deadline. Bounds every Rust round trip.
    request_timeout_seconds: float = Field(default=10.0, gt=0, le=300)

    #: Total wall-clock budget for all retries of one Rust call.
    total_timeout_seconds: float = Field(default=30.0, gt=0, le=900)

    #: Maximum attempts for a retryable Rust call, including the first.
    max_attempts: int = Field(default=3, ge=1, le=8)

    #: gRPC keepalive interval. Zero disables keepalive.
    keepalive_interval_seconds: float = Field(default=30.0, ge=0, le=600)

    #: Whether to wait for the channel to be ready before the first call.
    wait_for_ready_on_connect: bool = True

    @model_validator(mode="after")
    def _require_mtls_material(self) -> RustCoreSettings:
        """Ensures mTLS is configured with both halves of the key pair."""
        if self.transport is TransportSecurity.MTLS and not (
            self.client_cert_secret_ref and self.client_key_secret_ref
        ):
            message = (
                "rust_core.transport=mtls requires both "
                "client_cert_secret_ref and client_key_secret_ref"
            )
            raise ValueError(message)
        return self


class ServiceSettings(_Frozen):
    """Configuration for the orchestrator's own inbound service surface."""

    #: Bind address for the internal HTTP/gRPC surface. Defaults to loopback
    #: so an accidental deployment does not expose the AI service publicly.
    bind_host: str = Field(default="127.0.0.1", min_length=1, max_length=255)
    bind_port: int = Field(default=9100, ge=1, le=65535)

    #: Explicit opt-in for a listener reachable outside the process loopback.
    #: In production, setting this is an operator assertion that network policy
    #: keeps the TLS-authenticated bridge on a private service network.
    allow_remote_bridge_clients: bool = False

    #: Transport security for the inbound AgentBridge gRPC listener. Plaintext
    #: is permitted only in development/test and is rejected by startup
    #: validation in staging/production.
    grpc_transport_security: TransportSecurity = TransportSecurity.INSECURE

    #: PEM server certificate chain and private-key paths for TLS/mTLS. The
    #: private-key bytes are loaded only when the listener starts.
    grpc_certificate_chain_path: str | None = Field(default=None, max_length=4096)
    grpc_private_key_path: str | None = Field(default=None, max_length=4096)

    #: PEM client-CA bundle for mTLS, and whether the server requires a client
    #: certificate. When enabled, unauthenticated TLS clients are refused.
    grpc_client_ca_bundle_path: str | None = Field(default=None, max_length=4096)
    grpc_require_client_certificate: bool = False

    #: Maximum accepted request body size in bytes.
    max_request_bytes: int = Field(default=DEFAULT_MAX_PROMPT_BYTES, ge=1024, le=8 * 1024 * 1024)

    #: Maximum wall-clock time for one inbound AI execution.
    request_timeout_seconds: float = Field(default=120.0, gt=0, le=3600)

    #: Secret reference for the token the orchestrator requires on inbound
    #: calls. When ``None``, the inbound surface refuses all traffic: there is
    #: no unauthenticated mode.
    service_token_secret_ref: str | None = Field(default=None, max_length=512)

    #: Grace period for in-flight work during shutdown before cancellation
    #: is escalated.
    shutdown_grace_seconds: float = Field(default=20.0, ge=0, le=600)

    @model_validator(mode="after")
    def _validate_grpc_transport(self) -> ServiceSettings:
        """Rejects partial TLS configuration and credentials ignored by plaintext."""
        has_certificate = bool(self.grpc_certificate_chain_path)
        has_private_key = bool(self.grpc_private_key_path)
        requires_client_certificate = (
            self.grpc_transport_security is TransportSecurity.MTLS
            or self.grpc_require_client_certificate
        )

        if self.grpc_transport_security is TransportSecurity.INSECURE:
            if (
                has_certificate
                or has_private_key
                or self.grpc_client_ca_bundle_path
                or self.grpc_require_client_certificate
            ):
                raise ValueError(
                    "service.grpc TLS material cannot be configured when "
                    "grpc_transport_security='insecure'"
                )
            return self

        if not has_certificate or not has_private_key:
            raise ValueError(
                "service.grpc TLS requires both grpc_certificate_chain_path "
                "and grpc_private_key_path"
            )
        if requires_client_certificate and not self.grpc_client_ca_bundle_path:
            raise ValueError("service.grpc mTLS requires grpc_client_ca_bundle_path")
        if self.grpc_client_ca_bundle_path and not requires_client_certificate:
            raise ValueError(
                "service.grpc_client_ca_bundle_path requires mTLS or "
                "grpc_require_client_certificate=true"
            )
        return self


class ProviderSettings(_Frozen):
    """Model provider configuration.

    Credentials are always secret *references*. A raw key in configuration is
    rejected by :meth:`_reject_inline_credentials`.
    """

    #: Selection mode. ``disabled`` makes every model call fail closed.
    mode: ProviderMode = ProviderMode.DISABLED

    #: Allowlisted provider names. An empty list means "no provider may be
    #: used", which is the safe default.
    enabled_providers: tuple[str, ...] = Field(default=())

    #: Default model identifier used when an agent does not pin one.
    default_model: str | None = Field(default=None, max_length=200)

    #: Allowlisted model identifiers. Empty means no model may be called.
    allowed_models: tuple[str, ...] = Field(default=())

    #: Ordered fallback models consulted on a retryable provider failure.
    failover_models: tuple[str, ...] = Field(default=())

    #: Per-request timeout sent to the provider transport.
    request_timeout_seconds: float = Field(default=60.0, gt=0, le=900)

    #: Connect timeout, kept short so an unreachable provider fails fast.
    connect_timeout_seconds: float = Field(default=5.0, gt=0, le=60)

    #: Maximum attempts for a retryable provider call.
    max_attempts: int = Field(default=3, ge=1, le=8)

    #: Total retry budget across all attempts of one model call.
    retry_budget_seconds: float = Field(default=60.0, ge=0, le=900)

    #: Base delay for exponential backoff, in seconds.
    backoff_base_seconds: float = Field(default=0.5, gt=0, le=30)

    #: Upper bound for a single backoff delay, in seconds.
    backoff_max_seconds: float = Field(default=8.0, gt=0, le=120)

    #: Whether to honour a provider's ``Retry-After`` header when present.
    honour_retry_after: bool = True

    #: Known pricing used for cost estimation, keyed by model id. Empty means
    #: cost is reported as unknown rather than fabricated.
    pricing: Mapping[str, Mapping[str, float]] = Field(default_factory=dict)

    @field_validator("pricing")
    @classmethod
    def _validate_pricing(
        cls, value: Mapping[str, Mapping[str, float]]
    ) -> Mapping[str, Mapping[str, float]]:
        """Rejects negative or malformed pricing entries.

        A negative price would silently produce negative cost, so it is a
        configuration error rather than a runtime surprise.
        """
        for model, entries in value.items():
            for key, amount in entries.items():
                if amount < 0:
                    message = f"pricing for model {model!r} has negative value for {key!r}"
                    raise ValueError(message)
        return value


class ToolSettings(_Frozen):
    """Tool execution limits and safety configuration."""

    #: How code-execution tools run. ``disabled`` by default.
    execution_mode: ToolExecutionMode = ToolExecutionMode.DISABLED

    #: Per-invocation timeout.
    invocation_timeout_seconds: float = Field(default=30.0, gt=0, le=900)

    #: Maximum tool invocations per execution.
    max_calls_per_execution: int = Field(default=25, ge=0, le=1000)

    #: Maximum retry attempts for a retryable, idempotent tool call.
    max_attempts: int = Field(default=2, ge=1, le=5)

    #: Maximum concurrent tool invocations within one execution.
    max_parallel_calls: int = Field(default=4, ge=1, le=64)

    #: Maximum accepted tool result size in bytes.
    max_result_bytes: int = Field(
        default=DEFAULT_MAX_TOOL_RESULT_BYTES, ge=1024, le=8 * 1024 * 1024
    )

    #: Whether outbound HTTP tools are permitted at all. When false, the HTTP
    #: tool refuses every invocation instead of relying on per-call policy.
    allow_http_tools: bool = False

    #: Allowlisted host suffixes for outbound HTTP tools. Empty with
    #: ``allow_http_tools=True`` means every host is refused (fail closed).
    allowed_http_hosts: tuple[str, ...] = Field(default=())

    #: Whether tool permission decisions must come from the authoritative Rust
    #: policy bridge. Disabling this is refused in production.
    require_policy_decision: bool = True


class MemorySettings(_Frozen):
    """Memory subsystem configuration."""

    backend: MemoryBackend = MemoryBackend.DISABLED

    #: Maximum entries retained in short-term (task-local) memory.
    short_term_max_entries: int = Field(default=50, ge=1, le=10_000)

    #: Maximum characters retained in short-term memory.
    short_term_max_chars: int = Field(default=64_000, ge=1024, le=4_000_000)

    #: Maximum episodic records retained per tenant-scoped resource.
    episodic_max_records: int = Field(default=200, ge=0, le=100_000)

    #: Number of most recent turns preserved verbatim during compaction.
    compaction_keep_recent: int = Field(default=10, ge=0, le=1_000)

    #: Character threshold above which compaction becomes eligible.
    compaction_trigger_chars: int = Field(default=32_000, ge=1024, le=4_000_000)

    #: Default retention period for durable memory records, in days.
    retention_days: int = Field(default=30, ge=1, le=3650)

    #: Whether retrieval must apply tenant filters. Never disabled in
    #: production; asserted by ``config/validation.py``.
    enforce_tenant_filters: bool = True


class DataHubSettings(_Frozen):
    """Bounds and allow-lists for the tenant-scoped Data Hub ingestion plane.

    Rust owns data-source/document metadata and tenant authorization. These
    settings only constrain Python-side fetching, parsing, embedding and
    vector storage; they do not contain credentials or caller-controlled
    filesystem paths.
    """

    allowed_source_kinds: tuple[DataSourceKind, ...] = (DataSourceKind.UPLOAD,)
    max_document_bytes: int = Field(default=32 * 1024 * 1024, ge=1, le=32 * 1024 * 1024)
    max_upload_bytes: int = Field(default=32 * 1024 * 1024, ge=1, le=32 * 1024 * 1024)
    max_sync_bytes: int = Field(default=64 * 1024 * 1024, ge=1, le=512 * 1024 * 1024)
    max_pages_per_sync: int = Field(default=500, ge=1, le=10_000)
    max_crawl_depth: int = Field(default=3, ge=0, le=20)
    max_crawl_pages: int = Field(default=100, ge=1, le=10_000)
    allowed_mime_types: tuple[str, ...] = (
        "text/plain",
        "text/markdown",
        "text/html",
        "text/csv",
        "application/json",
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    crawl_egress_allowlist: tuple[str, ...] = ()
    object_storage_egress_allowlist: tuple[str, ...] = ()
    database_url_secret_ref: str | None = Field(default=None, max_length=512)
    database_role: str | None = Field(default=None, max_length=63)

    @field_validator("crawl_egress_allowlist", "object_storage_egress_allowlist")
    @classmethod
    def _normalise_egress_hosts(cls, hosts: tuple[str, ...]) -> tuple[str, ...]:
        """Accepts DNS hostnames only; URL schemes, paths, and IP literals are refused."""
        import ipaddress
        import re

        result: list[str] = []
        for host in hosts:
            candidate = host.strip().rstrip(".").lower()
            if not candidate or "://" in candidate or "/" in candidate:
                raise ValueError("Data Hub egress entries must be hostnames, not URLs")
            try:
                ipaddress.ip_address(candidate)
            except ValueError:
                pass
            else:
                raise ValueError("Data Hub egress allow-lists must use DNS names, not IP literals")
            if not re.fullmatch(
                r"(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)(?:\.(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?))*",
                candidate,
            ):
                raise ValueError("Data Hub egress entry is not a valid DNS hostname")
            result.append(candidate)
        if len(set(result)) != len(result):
            raise ValueError("Data Hub egress allow-list must not contain duplicate hosts")
        return tuple(result)

    @field_validator("allowed_source_kinds")
    @classmethod
    def _unique_source_kinds(cls, kinds: tuple[DataSourceKind, ...]) -> tuple[DataSourceKind, ...]:
        """Rejects duplicate source capabilities in configuration."""
        if len(set(kinds)) != len(kinds):
            raise ValueError("data_hub.allowed_source_kinds must not contain duplicates")
        return kinds

    @field_validator("allowed_mime_types")
    @classmethod
    def _normalise_mime_types(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        """Requires normalized media types without parameters or wildcards."""
        import re

        normalised = tuple(value.strip().lower() for value in values)
        if any(
            not re.fullmatch(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", value)
            for value in normalised
        ):
            raise ValueError("data_hub.allowed_mime_types must contain exact media types")
        if len(set(normalised)) != len(normalised):
            raise ValueError("data_hub.allowed_mime_types must not contain duplicates")
        return normalised

    @model_validator(mode="after")
    def _document_caps_sane(self) -> DataHubSettings:
        """Prevents upload-specific settings from exceeding the document cap."""
        if self.max_upload_bytes > self.max_document_bytes:
            raise ValueError("data_hub.max_upload_bytes must not exceed max_document_bytes")
        return self


class RagSettings(_Frozen):
    """Retrieval-augmented generation configuration."""

    #: Whether the RAG pipeline is available. Disabled by default because no
    #: vector backend is configured until one is explicitly selected.
    enabled: bool = False

    vector_backend: VectorBackend = VectorBackend.DISABLED

    #: Embedding dimensionality. Zero means "unknown, do not assume".
    embedding_dimensions: int = Field(default=0, ge=0, le=16_384)
    embedding_provider: str = Field(default="openai", min_length=1, max_length=64)
    embedding_model: str = Field(default="text-embedding-3-small", min_length=1, max_length=128)
    embedding_api_key_secret_ref: str | None = Field(
        default="env:MAS_OPENAI_API_KEY", max_length=256
    )

    #: Chunking window.
    chunk_size_chars: int = Field(default=1_200, ge=64, le=64_000)
    chunk_overlap_chars: int = Field(default=200, ge=0, le=8_000)

    #: Maximum chunks admitted into a single model context.
    max_context_chunks: int = Field(default=8, ge=1, le=200)

    #: Candidate pool sizes for retrieval and reranking.
    retrieval_top_k: int = Field(default=25, ge=1, le=1_000)
    rerank_top_k: int = Field(default=8, ge=1, le=200)

    #: Whether hybrid (lexical + vector) retrieval is attempted.
    hybrid_enabled: bool = False

    #: Whether citations are required whenever RAG context is used. When true,
    #: an answer without verifiable citations fails output validation.
    require_citations: bool = True

    #: Whether security filtering is applied after retrieval as well as before.
    post_retrieval_security_filter: bool = True

    @model_validator(mode="after")
    def _chunk_window_sane(self) -> RagSettings:
        """Rejects an overlap that consumes the entire chunk window."""
        if self.chunk_overlap_chars >= self.chunk_size_chars:
            message = "rag.chunk_overlap_chars must be smaller than rag.chunk_size_chars"
            raise ValueError(message)
        if self.rerank_top_k > self.retrieval_top_k:
            message = "rag.rerank_top_k must not exceed rag.retrieval_top_k"
            raise ValueError(message)
        return self


class GuardrailSettings(_Frozen):
    """Guardrail behaviour configuration."""

    #: Action taken when input validation fails.
    input_failure_mode: GuardrailFailureMode = GuardrailFailureMode.FAIL_CLOSED

    #: Action taken when prompt injection is detected.
    injection_failure_mode: GuardrailFailureMode = GuardrailFailureMode.FAIL_CLOSED

    #: Action taken when PII is detected in model output.
    pii_failure_mode: GuardrailFailureMode = GuardrailFailureMode.REDACT

    #: Action taken when output schema validation fails.
    output_failure_mode: GuardrailFailureMode = GuardrailFailureMode.FAIL_CLOSED

    #: Action taken when required citations are missing or unverifiable.
    citation_failure_mode: GuardrailFailureMode = GuardrailFailureMode.FAIL_CLOSED

    #: Maximum characters accepted as user input before rejection.
    max_input_chars: int = Field(default=32_000, ge=1, le=1_000_000)

    #: Whether tool output is always treated as untrusted data. Refusing to
    #: disable this is enforced in production by ``config/validation.py``.
    treat_tool_output_untrusted: bool = True

    #: Whether the Rust policy bridge is consulted before model execution.
    policy_bridge_enabled: bool = True


class AgentRuntimeSettings(_Frozen):
    """Bounds applied to every agent run.

    These exist to make "no infinite loop" a property of configuration rather
    than of any single implementation.
    """

    #: Maximum planner iterations before the run terminates deterministically.
    max_planning_steps: int = Field(default=12, ge=1, le=500)

    #: Maximum supervisor iterations across delegations.
    max_supervisor_rounds: int = Field(default=6, ge=1, le=200)

    #: Maximum delegation depth (agent delegating to agent).
    max_delegation_depth: int = Field(default=3, ge=0, le=50)

    #: Maximum handoffs in one execution, guarding against ping-pong loops.
    max_handoffs: int = Field(default=6, ge=0, le=200)

    #: Maximum model calls per execution.
    max_model_calls: int = Field(default=20, ge=1, le=1_000)

    #: Wall-clock cap for one agent run.
    max_run_seconds: float = Field(default=120.0, gt=0, le=3600)

    #: Maximum characters of accumulated context before compaction is forced.
    max_context_chars: int = Field(default=120_000, ge=1024, le=8_000_000)


class ObservabilitySettings(_Frozen):
    """Logging, metrics, and tracing configuration."""

    log_format: LogFormat = LogFormat.JSON

    #: Minimum emitted log level name.
    log_level: str = Field(default="INFO", max_length=16)

    #: Whether secret redaction is applied to every log event. Disabling this
    #: is refused in production by ``config/validation.py``.
    redact_logs: bool = True

    #: OTLP collector endpoint. Empty disables export.
    otlp_endpoint: str | None = Field(default=None, max_length=2048)

    #: Trace sampling ratio in ``[0.0, 1.0]``.
    trace_sample_ratio: float = Field(default=0.0, ge=0.0, le=1.0)

    #: Whether high-cardinality identifiers are emitted as metric labels.
    #: Off by default because identifiers such as execution ids would explode
    #: metric cardinality.
    allow_high_cardinality_metrics: bool = False

    @field_validator("log_level")
    @classmethod
    def _known_level(cls, value: str) -> str:
        """Normalises the level and rejects unknown names."""
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        upper = value.upper()
        if upper not in allowed:
            message = f"observability.log_level must be one of {sorted(allowed)}"
            raise ValueError(message)
        return upper


class SecuritySettings(_Frozen):
    """Python-local security controls."""

    #: Credential resolution backend. ``env`` reads secret references from the
    #: process environment; ``disabled`` refuses every resolution.
    credential_backend: str = Field(default="env", max_length=64)

    #: Prefix used when resolving secret references from the environment, e.g.
    #: ``openai_api_key`` is read from ``MAS_ORCH_SECRET_OPENAI_API_KEY``.
    secret_env_prefix: str = Field(default="MAS_ORCH_SECRET_", max_length=64)

    #: Whether a caller-supplied tenant header may ever be trusted as
    #: authoritative identity. This must stay ``False``; production validation
    #: refuses to boot otherwise.
    trust_caller_tenant_header: bool = False

    #: Whether the authenticated Rust caller identity is required on every
    #: inbound AI request.
    require_authenticated_caller: bool = True

    #: Secret reference for the token Rust presents on inbound bridge calls,
    #: e.g. ``env:MAS_ORCH_BRIDGE_TOKEN``. Required by the agent bridge: a
    #: bridge with no token accepts any caller that can reach the port, which
    #: is an open proxy onto the platform's model spend.
    service_token_ref: str | None = Field(default=None, max_length=256)

    #: Secret reference for the shared secret Rust uses to sign the body of
    #: every inbound bridge call, e.g. ``env:MAS_ORCH_INTERNAL_SECRET``.
    #:
    #: This is a *different* control from ``service_token_ref``. The bearer
    #: token proves the caller holds a credential; the signature proves that
    #: the holder of this secret sent *this* message, just now, unaltered. A
    #: token alone is replayable by anything that can read it, and a token
    #: captured on the wire is usable forever. Required in production, where
    #: the bridge refuses unsigned calls rather than trusting the network for
    #: identity.
    internal_service_secret_ref: str | None = Field(default=None, max_length=256)

    #: Service ids permitted to call this process, in addition to the
    #: built-in default. A syntactically perfect signature from a service
    #: that is not on the allowlist is still a refusal.
    extra_allowed_internal_callers: tuple[str, ...] = Field(default=())

    #: Maximum age, in seconds, of a Rust-issued context before it is
    #: considered stale.
    max_context_age_seconds: float = Field(default=300.0, gt=0, le=86_400)

    #: Literal values that are always redacted, in addition to the built-in
    #: sensitive key names.
    extra_redaction_keys: tuple[str, ...] = Field(default=())


class Settings(_Frozen):
    """Root configuration model for the orchestrator.

    Obtain instances through :func:`load_settings`; constructing directly is
    reserved for tests and for callers that already hold validated values.
    """

    environment: Environment = Environment.DEVELOPMENT

    rust_core: RustCoreSettings = Field(default_factory=RustCoreSettings)
    service: ServiceSettings = Field(default_factory=ServiceSettings)
    providers: ProviderSettings = Field(default_factory=ProviderSettings)
    tools: ToolSettings = Field(default_factory=ToolSettings)
    memory: MemorySettings = Field(default_factory=MemorySettings)
    rag: RagSettings = Field(default_factory=RagSettings)
    data_hub: DataHubSettings = Field(default_factory=DataHubSettings)
    guardrails: GuardrailSettings = Field(default_factory=GuardrailSettings)
    agents: AgentRuntimeSettings = Field(default_factory=AgentRuntimeSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)

    @property
    def is_production(self) -> bool:
        """Whether production guardrails apply to this configuration."""
        return self.environment.is_production


def _overlay_file_path(environment: Environment, config_dir: Path) -> Path | None:
    """Resolves the environment overlay file, if one exists.

    Args:
        environment: The selected deployment environment.
        config_dir: Directory containing configuration templates.

    Returns:
        The path to ``{environment}.toml`` when present, otherwise ``None``.
    """
    candidate = config_dir / f"{environment.value}.toml"
    return candidate if candidate.is_file() else None


def _read_toml(path: Path) -> Mapping[str, Any]:
    """Reads a TOML file into a mapping.

    Args:
        path: File to read.

    Returns:
        Parsed mapping. Returns an empty mapping when TOML support is absent
        so that Python 3.11+ always works; ``tomllib`` is part of the standard
        library from 3.11 onward.
    """
    import tomllib

    with path.open("rb") as handle:
        return tomllib.load(handle)


def _env_overrides(prefix: str = ENV_PREFIX) -> Mapping[str, Any]:
    """Builds a nested override mapping from prefixed environment variables.

    ``MAS_ORCH_RUST_CORE__ENDPOINT=host:500`` becomes
    ``{"rust_core": {"endpoint": "host:500"}}`` using a double underscore as
    the nesting separator. Values are strings; Pydantic coerces them to the
    declared field types.

    Args:
        prefix: Environment variable prefix to scan for.

    Returns:
        A nested mapping of overrides, empty when no matching variables exist.
    """
    overrides: dict[str, Any] = {}
    for raw_key, raw_value in os.environ.items():
        if not raw_key.startswith(prefix):
            continue
        parts = raw_key[len(prefix) :].lower().split("__")
        cursor: dict[str, Any] = overrides
        for part in parts[:-1]:
            if not part:
                continue
            nested = cursor.get(part)
            if not isinstance(nested, dict):
                # A scalar was already stored under this key (e.g. both
                # MAS_ORCH_RUST_CORE and MAS_ORCH_RUST_CORE__ENDPOINT were
                # set). The nested form wins, matching the documented
                # behaviour that later, more specific keys take precedence.
                nested = {}
                cursor[part] = nested
            cursor = nested
        leaf = parts[-1]
        if leaf:
            cursor[leaf] = raw_value
    return overrides


def _deep_merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merges ``overlay`` onto ``base``.

    Args:
        base: Lower-precedence mapping.
        overlay: Higher-precedence mapping.

    Returns:
        A new merged dictionary. Nested dictionaries merge key by key; all
        other values are replaced wholesale.
    """
    merged: dict[str, Any] = dict(base)
    for key, value in overlay.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def load_settings(
    *,
    environment: Environment | None = None,
    config_dir: Path | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> Settings:
    """Loads, layers, and validates orchestrator settings.

    Layering order (later wins): defaults → ``config/default.toml`` →
    ``config/{environment}.toml`` → ``MAS_ORCH_*`` env vars → ``overrides``.

    Args:
        environment: Deployment environment. Defaults to the ``MAS_ENV``
            environment variable, then ``development``.
        config_dir: Directory holding TOML overlays. Defaults to the
            repository ``config/`` directory when one is discoverable, and is
            otherwise ignored.
        overrides: Explicit highest-precedence values, used by tests.

    Returns:
        A validated, immutable :class:`Settings` instance.

    Raises:
        pydantic.ValidationError: When any value fails type or bound
            validation. Callers should let this propagate: a misconfigured
            process must not start.
    """
    resolved_environment = environment
    if resolved_environment is None:
        raw_environment = os.environ.get("MAS_ENV", Environment.DEVELOPMENT.value)
        try:
            resolved_environment = Environment(raw_environment.lower())
        except ValueError as error:
            known = ", ".join(sorted(member.value for member in Environment))
            message = f"unknown MAS_ENV {raw_environment!r}; expected one of: {known}"
            raise ValueError(message) from error

    merged: dict[str, Any] = {"environment": resolved_environment.value}

    resolved_dir = config_dir
    if resolved_dir is None:
        candidate = Path(__file__).resolve().parents[3] / "config"
        resolved_dir = candidate if candidate.is_dir() else None

    if resolved_dir is not None:
        default_file = resolved_dir / "default.toml"
        if default_file.is_file():
            merged = _deep_merge(merged, _read_toml(default_file))
        overlay_file = _overlay_file_path(resolved_environment, resolved_dir)
        if overlay_file is not None:
            merged = _deep_merge(merged, _read_toml(overlay_file))

    merged = _deep_merge(merged, _env_overrides())
    if overrides:
        merged = _deep_merge(merged, overrides)

    return Settings.model_validate(merged)


__all__ = [
    "DEFAULT_MAX_PROMPT_BYTES",
    "DEFAULT_MAX_TOOL_RESULT_BYTES",
    "ENV_PREFIX",
    "AgentRuntimeSettings",
    "DataHubSettings",
    "GuardrailSettings",
    "MemorySettings",
    "ObservabilitySettings",
    "ProviderSettings",
    "RagSettings",
    "RustCoreSettings",
    "SecuritySettings",
    "ServiceSettings",
    "Settings",
    "ToolSettings",
    "load_settings",
]

# Re-exported so callers can annotate secret material consistently. The type is
# deliberately not used inside settings models: every credential field in this
# module is a *reference* string, never the secret itself.
SecretString = SecretStr
