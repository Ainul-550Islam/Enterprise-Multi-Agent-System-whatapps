"""API request and response schemas.

The important property of this module is what it does **not** contain: there is
no ``tenant_id`` field on any request schema.

That is the spec §6 rule made structural. Tenancy arrives in authenticated
metadata set by Rust, and a request body is attacker-controlled from the API's
point of view. A schema with a ``tenant_id`` field would spend the rest of its
life being defended by reviewers; one without it cannot be used to claim a
tenant at all.

Everything here is a Pydantic model, so validation happens at the boundary and
a malformed request never reaches the runtime.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

#: Ceilings applied to request bodies. Every list and string has one, because
#: an unbounded field is a request that can exhaust the service.
MAX_REQUEST_TEXT_CHARS: int = 100_000
MAX_METADATA_KEYS: int = 32
MAX_METADATA_VALUE_CHARS: int = 512


class _Strict(BaseModel):
    """Base model: immutable, and refuses unknown fields.

    Extra fields are rejected rather than ignored. Silently dropping an
    unrecognised field means a caller that sends ``tenent_id`` by mistake gets a
    default tenant rather than an error, which is the exact failure this package
    exists to prevent.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
    )


class ExecutionRequestSchema(_Strict):
    """A request to run an execution.

    Attributes:
        request: What to do.
        agent_id: Run a specific agent.
        metadata: Small annotations, bounded in size and count.
        protocol_version: The wire version the caller was built against.
    """

    request: str = Field(min_length=1, max_length=MAX_REQUEST_TEXT_CHARS)
    agent_id: str | None = Field(default=None, max_length=128)
    metadata: dict[str, str] = Field(default_factory=dict)
    protocol_version: str = Field(default="1.0.0", max_length=32)

    @field_validator("metadata")
    @classmethod
    def _bounded(cls, value: dict[str, str]) -> dict[str, str]:
        """Refuses oversized or over-large metadata."""
        if len(value) > MAX_METADATA_KEYS:
            raise ValueError(f"metadata may contain at most {MAX_METADATA_KEYS} keys")
        for key, item in value.items():
            if len(item) > MAX_METADATA_VALUE_CHARS:
                raise ValueError(f"metadata['{key}'] exceeds {MAX_METADATA_VALUE_CHARS} chars")
        return value

    @field_validator("agent_id")
    @classmethod
    def _non_empty(cls, value: str | None) -> str | None:
        """Treats an empty string as absent rather than as an empty id."""
        if value is not None and not value.strip():
            return None
        return value


class ExecutionResponseSchema(_Strict):
    """The outcome of an execution.

    Attributes:
        execution_id: Execution identifier.
        agent_id: Agent that produced the answer.
        status: Termination reason.
        content: The answer, when there is one.
        duration_ms: Wall-clock duration.
        usage: Token and cost summary, when measured.
        citations: Citation numbers the answer may reference.
    """

    execution_id: str
    agent_id: str
    status: str
    content: str = ""
    duration_ms: int = 0
    usage: dict[str, Any] = Field(default_factory=dict)
    citations: list[int] = Field(default_factory=list)


class HealthResponseSchema(_Strict):
    """A health or readiness answer.

    Attributes:
        status: ``ok`` or ``not_ready``.
        service: Service name.
        environment: Deployment environment.
        version: Build version.
        dependencies: Per-dependency states, on readiness.
    """

    status: str
    service: str
    environment: str
    version: str = "unknown"
    dependencies: list[dict[str, Any]] = Field(default_factory=list)


class ReadinessResponseSchema(_Strict):
    """A readiness answer, carrying dependency detail."""

    ready: bool
    state: str
    checked_epoch_ms: int = 0
    dependencies: list[dict[str, Any]] = Field(default_factory=list)


class MemoryWriteSchema(_Strict):
    """A request to record memory.

    Attributes:
        content: What to remember.
        scope: Which lifetime. ``task`` is the default, and the only scope that
            is safe by default because it dies with the execution.
        record_id: Caller-chosen identifier, so a write can be idempotent.
    """

    content: str = Field(min_length=1, max_length=MAX_REQUEST_TEXT_CHARS)
    scope: str = Field(default="task", max_length=32)
    record_id: str = Field(min_length=1, max_length=128)

    @field_validator("scope")
    @classmethod
    def _known_scope(cls, value: str) -> str:
        """Refuses an unknown scope."""
        allowed = {"task", "episodic", "durable", "semantic"}
        if value not in allowed:
            raise ValueError(f"scope must be one of {sorted(allowed)}")
        return value


class RetrievalRequestSchema(_Strict):
    """A request to retrieve context.

    Attributes:
        query: What to look for.
        top_k: How many passages to return.
    """

    query: str = Field(min_length=1, max_length=MAX_REQUEST_TEXT_CHARS)
    top_k: int = Field(default=8, ge=1, le=50)


class ErrorResponseSchema(_Strict):
    """An error response.

    Attributes:
        code: The domain error code.
        message: A caller-safe message. Never contains provider or tool text.
        retryable: Whether retrying could succeed.
        correlation_id: Identifier for the caller to quote in a bug report.
    """

    code: str
    message: str
    retryable: bool = False
    correlation_id: str = ""


def request_from_schema(schema: ExecutionRequestSchema) -> dict[str, Any]:
    """Flattens a request schema into runtime arguments.

    Deliberately does not forward ``metadata`` wholesale: the runtime's context
    is built from authenticated metadata, and merging caller-supplied keys into
    it would let the caller set fields the API does not control.
    """
    return {
        "request": schema.request,
        "agent_id": schema.agent_id,
        "protocol_version": schema.protocol_version,
    }
