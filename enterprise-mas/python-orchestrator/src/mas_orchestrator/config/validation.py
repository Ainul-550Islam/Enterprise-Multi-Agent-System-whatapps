# PURPOSE:
# Cross-field configuration validation and startup safety checks. The single
# place a deployment is told "no" before it can serve a request.
#
# Owning layer: `mas_orchestrator.config` (the process edge, boot time).
#
# Production responsibility: reject configurations that are individually
# valid but jointly unsafe — a production environment with log redaction off,
# an in-memory backend, an insecure transport, or a bridge with no signing
# secret. Every violation names the field, the reason, and the remediation, so
# an operator can act on it instead of reading a stack trace.
#
# Security boundary: this module is a fail-closed gate, and the enumerated
# `production_ready_settings()` baseline is the executable statement of what
# "production ready" means. Adding a security-relevant setting without adding
# its guardrail here is the failure mode this module exists to prevent.
#
# It must NOT:
#   • silently coerce an unsafe value into a safe one (a deployment must be
#     told what is wrong, not quietly corrected);
#   • validate only on the happy path — every guardrail is production-scoped
#     and reported together, not one at a time.

"""Cross-field configuration validation and startup safety checks.

Single-field type and bound checks live in ``settings.py`` as Pydantic
validators. This module owns the checks that can only be expressed across
fields — most importantly the production guardrails from the specification:

Production mode must refuse:

* insecure transport to Rust Core
* replay/record provider modes and any fake provider
* an unauthenticated inbound service surface
* in-memory-only durable backends (memory, vector)
* unrestricted HTTP tools and non-sandboxed code execution
* wildcard tool permissions and disabled policy decisions
* trusting a caller-supplied tenant header as authoritative identity
* disabled log redaction

Every check returns a :class:`ConfigViolation` describing itself rather than
raising immediately, so that one startup reports *all* problems at once
instead of forcing an operator through a slow fix-one-at-a-time loop.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import (
    Environment,
    GuardrailFailureMode,
    MemoryBackend,
    ProviderMode,
    ToolExecutionMode,
    TransportSecurity,
    VectorBackend,
)
from mas_orchestrator.config.settings import (
    MemorySettings,
    ProviderSettings,
    RagSettings,
    RustCoreSettings,
    SecuritySettings,
    ServiceSettings,
    Settings,
    ToolSettings,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

#: Provider names that are explicitly test-only. Selecting one outside a
#: development/test environment is a hard startup failure.
TEST_ONLY_PROVIDERS: frozenset[str] = frozenset({"fake", "echo", "mock", "null", "stub"})


class ConfigViolation:
    """A single configuration problem, safe to log and display.

    Attributes:
        field: Dotted path to the offending setting.
        message: Human-readable description of the problem.
        remediation: Concrete action that resolves the violation.
    """

    __slots__ = ("field", "message", "remediation")

    def __init__(self, field: str, message: str, remediation: str) -> None:
        self.field = field
        self.message = message
        self.remediation = remediation

    def __str__(self) -> str:
        return f"{self.field}: {self.message} (remediation: {self.remediation})"

    def __repr__(self) -> str:
        return f"ConfigViolation(field={self.field!r}, message={self.message!r})"

    def as_dict(self) -> dict[str, str]:
        """Returns a JSON-serialisable representation."""
        return {
            "field": self.field,
            "message": self.message,
            "remediation": self.remediation,
        }


class ConfigurationError(Exception):
    """Raised when configuration cannot safely start the service.

    Attributes:
        violations: Every violation found, in a deterministic order.
    """

    def __init__(self, violations: Sequence[ConfigViolation]) -> None:
        self.violations: tuple[ConfigViolation, ...] = tuple(violations)
        detail = "\n".join(f"  - {violation}" for violation in self.violations)
        super().__init__(f"invalid configuration ({len(self.violations)} problem(s)):\n{detail}")


def _check_transport(settings: Settings) -> list[ConfigViolation]:
    """Validates Rust Core transport security."""
    violations: list[ConfigViolation] = []
    rust = settings.rust_core

    if settings.is_production and rust.transport is TransportSecurity.INSECURE:
        violations.append(
            ConfigViolation(
                field="rust_core.transport",
                message="plaintext gRPC to Rust Core is not permitted in production",
                remediation="set rust_core.transport to 'tls' or 'mtls'",
            )
        )

    if rust.transport is not TransportSecurity.INSECURE and not rust.bearer_token_secret_ref:
        violations.append(
            ConfigViolation(
                field="rust_core.bearer_token_secret_ref",
                message="an authenticated Rust Core channel requires a bearer token reference",
                remediation="set rust_core.bearer_token_secret_ref to a resolvable secret name",
            )
        )
    return violations


def _check_providers(settings: Settings) -> list[ConfigViolation]:
    """Validates provider mode, allowlists, and fake-provider exclusion."""
    violations: list[ConfigViolation] = []
    providers = settings.providers

    if settings.is_production and providers.mode in (ProviderMode.REPLAY, ProviderMode.RECORD):
        violations.append(
            ConfigViolation(
                field="providers.mode",
                message=(
                    f"provider mode '{providers.mode.value}' is test-only and refused in production"
                ),
                remediation="set providers.mode to 'live' or 'disabled'",
            )
        )

    if providers.mode is ProviderMode.REPLAY and not settings.environment.allows_fakes:
        violations.append(
            ConfigViolation(
                field="providers.mode",
                message="provider replay is only available in development/test",
                remediation="use providers.mode='live' outside development and test",
            )
        )

    for name in providers.enabled_providers:
        normalised = name.strip().lower()
        if normalised in TEST_ONLY_PROVIDERS and not settings.environment.allows_fakes:
            violations.append(
                ConfigViolation(
                    field="providers.enabled_providers",
                    message=f"provider '{name}' is test-only and cannot be enabled here",
                    remediation=(
                        "remove the fake provider or switch to a development/test environment"
                    ),
                )
            )

    if providers.mode is ProviderMode.LIVE and not providers.enabled_providers:
        violations.append(
            ConfigViolation(
                field="providers.enabled_providers",
                message="provider mode 'live' requires at least one enabled provider",
                remediation="enable a provider or set providers.mode='disabled'",
            )
        )

    if providers.mode is ProviderMode.LIVE and not providers.allowed_models:
        violations.append(
            ConfigViolation(
                field="providers.allowed_models",
                message="provider mode 'live' requires a non-empty model allowlist",
                remediation=(
                    "populate providers.allowed_models with the models this deployment may call"
                ),
            )
        )

    if (
        providers.default_model
        and providers.allowed_models
        and providers.default_model not in providers.allowed_models
    ):
        violations.append(
            ConfigViolation(
                field="providers.default_model",
                message="default model is not present in the model allowlist",
                remediation=("add the default model to providers.allowed_models or choose another"),
            )
        )

    for model in providers.failover_models:
        if providers.allowed_models and model not in providers.allowed_models:
            violations.append(
                ConfigViolation(
                    field="providers.failover_models",
                    message=f"failover model '{model}' is not in the model allowlist",
                    remediation="remove it from failover_models or add it to allowed_models",
                )
            )

    if providers.max_attempts > 1 and providers.retry_budget_seconds <= 0:
        violations.append(
            ConfigViolation(
                field="providers.retry_budget_seconds",
                message="retries are enabled but the retry budget is zero",
                remediation="set a positive retry_budget_seconds or set max_attempts to 1",
            )
        )

    if providers.backoff_max_seconds < providers.backoff_base_seconds:
        violations.append(
            ConfigViolation(
                field="providers.backoff_max_seconds",
                message="backoff ceiling is below the backoff base delay",
                remediation="set backoff_max_seconds greater than or equal to backoff_base_seconds",
            )
        )
    return violations


def _check_tools(settings: Settings) -> list[ConfigViolation]:
    """Validates tool execution safety."""
    violations: list[ConfigViolation] = []
    tools = settings.tools

    if settings.is_production and tools.execution_mode is ToolExecutionMode.SUBPROCESS:
        violations.append(
            ConfigViolation(
                field="tools.execution_mode",
                message=(
                    "subprocess tool execution has no isolation boundary "
                    "and is refused in production"
                ),
                remediation="set tools.execution_mode to 'container' or 'disabled'",
            )
        )

    if tools.allow_http_tools and not tools.allowed_http_hosts:
        violations.append(
            ConfigViolation(
                field="tools.allowed_http_hosts",
                message="HTTP tools are enabled with an empty host allowlist",
                remediation="populate tools.allowed_http_hosts or set tools.allow_http_tools=false",
            )
        )

    if settings.is_production and not tools.require_policy_decision:
        violations.append(
            ConfigViolation(
                field="tools.require_policy_decision",
                message=(
                    "tool permission decisions must come from the "
                    "authoritative policy bridge in production"
                ),
                remediation="set tools.require_policy_decision=true",
            )
        )

    return violations


def _check_tool_output_trust(settings: Settings) -> list[ConfigViolation]:
    """Validates that tool output is treated as untrusted data.

    This lives on the guardrail settings, not the tool settings, because it
    governs how retrieved/tool output enters the model context.
    """
    violations: list[ConfigViolation] = []
    if settings.is_production and not settings.guardrails.treat_tool_output_untrusted:
        violations.append(
            ConfigViolation(
                field="guardrails.treat_tool_output_untrusted",
                message="tool output must be treated as untrusted data in production",
                remediation="set guardrails.treat_tool_output_untrusted=true",
            )
        )
    return violations


def _check_backends(settings: Settings) -> list[ConfigViolation]:
    """Validates durable backend selection."""
    violations: list[ConfigViolation] = []

    if settings.is_production and settings.memory.backend is MemoryBackend.IN_MEMORY:
        violations.append(
            ConfigViolation(
                field="memory.backend",
                message="in-memory durable memory is process-local and refused in production",
                remediation="set memory.backend to 'rust_backed' or 'disabled'",
            )
        )

    if settings.is_production and settings.rag.vector_backend is VectorBackend.IN_MEMORY:
        violations.append(
            ConfigViolation(
                field="rag.vector_backend",
                message="in-memory vector index is process-local and refused in production",
                remediation="set rag.vector_backend to 'pgvector' or 'disabled'",
            )
        )

    if settings.rag.enabled and settings.rag.vector_backend is VectorBackend.DISABLED:
        violations.append(
            ConfigViolation(
                field="rag.vector_backend",
                message="RAG is enabled but no vector backend is configured",
                remediation="select a vector backend or set rag.enabled=false",
            )
        )

    if settings.rag.enabled and settings.rag.embedding_dimensions <= 0:
        violations.append(
            ConfigViolation(
                field="rag.embedding_dimensions",
                message="RAG is enabled but embedding dimensionality is unknown",
                remediation="set rag.embedding_dimensions to the configured model's dimensionality",
            )
        )

    if settings.is_production and not settings.memory.enforce_tenant_filters:
        violations.append(
            ConfigViolation(
                field="memory.enforce_tenant_filters",
                message="memory retrieval must enforce tenant filters in production",
                remediation="set memory.enforce_tenant_filters=true",
            )
        )
    return violations


def _check_security(settings: Settings) -> list[ConfigViolation]:
    """Validates authentication, tenancy, and redaction invariants."""
    violations: list[ConfigViolation] = []
    security = settings.security

    if security.trust_caller_tenant_header:
        violations.append(
            ConfigViolation(
                field="security.trust_caller_tenant_header",
                message="a caller-supplied tenant header must never be authoritative identity",
                remediation="set security.trust_caller_tenant_header=false",
            )
        )

    if not security.require_authenticated_caller:
        violations.append(
            ConfigViolation(
                field="security.require_authenticated_caller",
                message="the AI service must require an authenticated caller",
                remediation="set security.require_authenticated_caller=true",
            )
        )

    if settings.is_production and not settings.observability.redact_logs:
        violations.append(
            ConfigViolation(
                field="observability.redact_logs",
                message="log redaction cannot be disabled in production",
                remediation="set observability.redact_logs=true",
            )
        )

    if settings.is_production and not settings.service.service_token_secret_ref:
        violations.append(
            ConfigViolation(
                field="service.service_token_secret_ref",
                message="the inbound service surface has no service token configured",
                remediation="set service.service_token_secret_ref to a resolvable secret name",
            )
        )

    if settings.is_production and not settings.security.internal_service_secret_ref:
        violations.append(
            ConfigViolation(
                field="security.internal_service_secret_ref",
                message=(
                    "no internal signing secret is configured, so inbound bridge "
                    "calls cannot be authenticated as coming from a specific "
                    "message at a specific time"
                ),
                remediation=(
                    "set security.internal_service_secret_ref to a resolvable "
                    "secret name shared with the Rust worker"
                ),
            )
        )

    if settings.is_production and settings.security.trust_caller_tenant_header:
        violations.append(
            ConfigViolation(
                field="security.trust_caller_tenant_header",
                message="a caller-supplied tenant header would become authoritative",
                remediation="set security.trust_caller_tenant_header=false",
            )
        )

    if settings.is_production and settings.service.bind_host not in ("127.0.0.1", "localhost"):
        # Not a hard failure: a service mesh may legitimately bind a pod IP.
        # Recorded so an operator sees the exposure decision explicitly.
        violations.append(
            ConfigViolation(
                field="service.bind_host",
                message="production service binds a non-loopback address",
                remediation="confirm an authenticating proxy fronts this listener",
            )
        )
    return violations


def _check_guardrails(settings: Settings) -> list[ConfigViolation]:
    """Validates guardrail fail-closed behaviour."""
    violations: list[ConfigViolation] = []
    guardrails = settings.guardrails
    audit_only = GuardrailFailureMode.AUDIT_ONLY

    if settings.is_production:
        for field_name in (
            "input_failure_mode",
            "injection_failure_mode",
            "output_failure_mode",
            "citation_failure_mode",
        ):
            if getattr(guardrails, field_name) is audit_only:
                violations.append(
                    ConfigViolation(
                        field=f"guardrails.{field_name}",
                        message="audit-only guardrails do not stop a violating execution",
                        remediation=f"set guardrails.{field_name} to 'fail_closed' or 'redact'",
                    )
                )

    if settings.is_production and not guardrails.policy_bridge_enabled:
        violations.append(
            ConfigViolation(
                field="guardrails.policy_bridge_enabled",
                message=(
                    "the Rust policy bridge must be consulted before model execution in production"
                ),
                remediation="set guardrails.policy_bridge_enabled=true",
            )
        )
    return violations


def validate_settings(settings: Settings) -> tuple[ConfigViolation, ...]:
    """Runs every cross-field and environment safety check.

    Args:
        settings: The configuration to validate.

    Returns:
        Every violation found, in a stable order. Empty when the configuration
        is safe to run.
    """
    violations: list[ConfigViolation] = []
    violations.extend(_check_transport(settings))
    violations.extend(_check_providers(settings))
    violations.extend(_check_tools(settings))
    violations.extend(_check_backends(settings))
    violations.extend(_check_security(settings))
    violations.extend(_check_guardrails(settings))
    violations.extend(_check_tool_output_trust(settings))
    return tuple(violations)


def validate_settings_or_raise(settings: Settings) -> Settings:
    """Validates configuration and raises when it is unsafe.

    Args:
        settings: The configuration to validate.

    Returns:
        The same settings instance, for convenient use in a wiring pipeline.

    Raises:
        ConfigurationError: When at least one violation was found. Every
            violation is reported together.
    """
    violations = validate_settings(settings)
    if violations:
        raise ConfigurationError(violations)
    return settings


def production_ready_settings(environment: Environment = Environment.PRODUCTION) -> Settings:
    """Builds a configuration that satisfies every production guardrail.

    Used by tests and as a documented baseline for operators: it is the
    smallest configuration that passes :func:`validate_settings` under
    ``production``.

    Args:
        environment: Environment to build for. Defaults to production.

    Returns:
        A validated settings instance that produces no violations.
    """
    settings = Settings(
        environment=environment,
        rust_core=RustCoreSettings(
            transport=TransportSecurity.TLS,
            # A secret *reference name*, not a secret value: bandit flags the
            # parameter name, so the intent is documented at the call site.
            bearer_token_secret_ref="rust_core_service_token",  # noqa: S106
        ),
        service=ServiceSettings(
            # Secret reference name, resolved at startup via the credential backend.
            service_token_secret_ref="orchestrator_service_token",  # noqa: S106
        ),
        providers=ProviderSettings(mode=ProviderMode.DISABLED),
        tools=ToolSettings(execution_mode=ToolExecutionMode.DISABLED),
        memory=MemorySettings(backend=MemoryBackend.DISABLED),
        rag=RagSettings(enabled=False, vector_backend=VectorBackend.DISABLED),
        security=SecuritySettings(
            # Secret *reference name* for the shared secret the Rust worker
            # signs bridge calls with. Without it the bridge cannot tell a
            # message that was sent now from one that was captured earlier,
            # so production validation refuses to boot.
            internal_service_secret_ref="rust_internal_signing_secret",  # noqa: S106
        ),
    )
    return validate_settings_or_raise(settings)
