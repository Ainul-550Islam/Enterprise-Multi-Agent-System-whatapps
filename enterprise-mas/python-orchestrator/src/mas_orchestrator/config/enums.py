"""Stable configuration enumerations.

These values appear in configuration files, environment variables, log fields,
and metrics labels. They are therefore *contract*: renaming a member or
changing a string value is a breaking change and must be treated as one.

Two rules govern every enum in this module:

1. Values are lowercase snake_case strings so they are safe in TOML, env vars,
   log fields, and Prometheus labels without further normalisation.
2. There is always an explicit ``UNSPECIFIED`` / ``DISABLED`` member where a
   value may be absent, so ``None`` never has to mean two different things.
"""

from __future__ import annotations

from enum import StrEnum


class Environment(StrEnum):
    """Deployment environment selector.

    Drives the fail-closed production guardrails in ``config/validation.py``.
    """

    DEVELOPMENT = "development"
    TEST = "test"
    STAGING = "staging"
    PRODUCTION = "production"

    @property
    def is_production(self) -> bool:
        """Whether this environment enforces production guardrails."""
        return self is Environment.PRODUCTION

    @property
    def allows_fakes(self) -> bool:
        """Whether explicitly test-only adapters may be selected.

        Only true for development and test, and even then only when the
        configuration opts in explicitly. It is never true for production or
        staging.
        """
        return self in (Environment.DEVELOPMENT, Environment.TEST)


class LogFormat(StrEnum):
    """Structured log rendering format."""

    JSON = "json"
    PRETTY = "pretty"


class ProviderMode(StrEnum):
    """How model providers are selected for this deployment.

    ``LIVE``      — only real provider adapters may be used; fake providers are
                    refused at startup.
    ``RECORD``    — live calls with responses recorded for later replay.
    ``REPLAY``    — no network; responses are replayed from a cassette. This is
                    test/CI only and is refused in production.
    ``DISABLED``  — no provider is configured; any model call fails closed with
                    ``model_not_available`` rather than fabricating output.
    """

    LIVE = "live"
    RECORD = "record"
    REPLAY = "replay"
    DISABLED = "disabled"

    @property
    def requires_network(self) -> bool:
        """Whether this mode is expected to reach a real provider endpoint."""
        return self in (ProviderMode.LIVE, ProviderMode.RECORD)


class TransportSecurity(StrEnum):
    """Transport security mode for outbound Rust Core gRPC calls."""

    #: Plaintext. Development/test only; refused in production.
    INSECURE = "insecure"
    #: Server-authenticated TLS using the system trust store or a CA bundle.
    TLS = "tls"
    #: Mutual TLS with a client certificate and key.
    MTLS = "mtls"

    @property
    def uses_tls(self) -> bool:
        """Whether the channel is encrypted."""
        return self is not TransportSecurity.INSECURE


class MemoryBackend(StrEnum):
    """Long-term memory backend selector.

    ``IN_MEMORY`` is retained for development and tests and is refused in
    production by ``config/validation.py``: it is process-local and would
    silently lose durable AI state on restart.
    """

    IN_MEMORY = "in_memory"
    RUST_BACKED = "rust_backed"
    DISABLED = "disabled"


class VectorBackend(StrEnum):
    """Vector index backend selector.

    ``IN_MEMORY`` is a development/test affordance only and is refused in
    production: it cannot be shared between replicas and provides no
    durability.
    """

    IN_MEMORY = "in_memory"
    PGVECTOR = "pgvector"
    DISABLED = "disabled"


class ToolExecutionMode(StrEnum):
    """How tool code execution is performed.

    ``DISABLED`` is the safe default. ``SUBPROCESS`` exists only for
    development and is strictly refused in production by
    ``config/validation.py`` because it offers no isolation boundary.
    """

    DISABLED = "disabled"
    SUBPROCESS = "subprocess"
    CONTAINER = "container"


class GuardrailFailureMode(StrEnum):
    """What a guardrail violation does to the execution.

    ``FAIL_CLOSED`` denies the execution and returns a typed error.
    ``REDACT`` replaces the offending content and continues, recording the
    decision. ``AUDIT_ONLY`` records the decision and continues unchanged; it
    is a rollout affordance and is refused in production.
    """

    FAIL_CLOSED = "fail_closed"
    REDACT = "redact"
    AUDIT_ONLY = "audit_only"

    @property
    def denies_execution(self) -> bool:
        """Whether this mode stops the execution."""
        return self is GuardrailFailureMode.FAIL_CLOSED


__all__ = [
    "Environment",
    "GuardrailFailureMode",
    "LogFormat",
    "MemoryBackend",
    "ProviderMode",
    "StrEnum",
    "ToolExecutionMode",
    "TransportSecurity",
    "VectorBackend",
]
