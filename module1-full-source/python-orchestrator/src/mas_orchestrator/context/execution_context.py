"""Immutable AI execution context.

Every unit of AI work runs against an :class:`ExecutionContext`: the
authoritative description of *whose* execution this is, *what* it is, and *how
long* it may take. It is built once, at the boundary, from data supplied by the
Rust Core — never from user-controlled HTTP headers.

Ownership contract (spec §5/§6):

* ``tenant_id`` and ``organization_id`` are **authoritative values supplied by
  Rust**. They are never read from an inbound HTTP header at the Python edge,
  and there is no setter that would let a downstream layer rewrite them.
* The context is frozen and hashable, so passing it into concurrent tasks
  cannot produce divergent views of tenancy.
* Missing mandatory identity is a hard error raised at construction, not a
  warning: the spec requires rejecting requests that lack authoritative
  execution identity.

The context intentionally carries no credentials, no provider configuration and
no mutable accumulators (token counters, step counters). Those live in the
runtime's own budget objects so that the identity of an execution stays a value
object.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.contracts.errors import InvalidRequestError, TenantContextError
from mas_orchestrator.contracts.protocol_version import (
    ProtocolVersion,
    negotiate_protocol_version,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas_orchestrator.security.identity import ServiceIdentity

__all__ = [
    "MANDATORY_IDENTITY_FIELDS",
    "ExecutionContext",
    "build_execution_context",
]

#: Fields that must be present and non-empty. ``task_id`` is deliberately
#: absent: an agent run may be invoked directly by an execution without a task.
#: ``task_attempt_id`` and ``agent_version_id`` are optional for the same
#: reason — their absence is meaningful, not an error.
MANDATORY_IDENTITY_FIELDS: Final[tuple[str, ...]] = (
    "tenant_id",
    "organization_id",
    "execution_id",
    "agent_id",
    "correlation_id",
    "request_id",
)


@dataclass(frozen=True, slots=True)
class ExecutionContext:
    """Immutable identity and lifecycle scope for one AI execution.

    Attributes:
        tenant_id: Authoritative tenant supplied by Rust.
        organization_id: Authoritative organization supplied by Rust.
        execution_id: Canonical execution this work belongs to.
        agent_id: Agent to run.
        correlation_id: End-to-end correlation identifier.
        request_id: Platform request identifier.
        protocol_version: Negotiated wire protocol version.
        caller: Authenticated service identity of the Rust caller.
        project_id: Optional project scope.
        task_id: Optional owning task.
        task_attempt_id: Optional task attempt (retry discriminator).
        agent_version_id: Optional pinned agent version.
        causation_id: Optional identifier of the event that caused this work.
        deadline_epoch_ms: Optional absolute expiry, Unix epoch milliseconds.
        attributes: Additional non-identity metadata supplied by Rust. Values
            must be JSON primitives; they are never used for authorization.
    """

    tenant_id: str
    organization_id: str
    execution_id: str
    agent_id: str
    correlation_id: str
    request_id: str
    protocol_version: ProtocolVersion
    caller: ServiceIdentity
    project_id: str | None = None
    task_id: str | None = None
    task_attempt_id: str | None = None
    agent_version_id: str | None = None
    causation_id: str | None = None
    deadline_epoch_ms: int | None = None
    attributes: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        """Validates mandatory identity at construction time.

        Raises:
            TenantContextError: If a mandatory identity field is empty, or if
                the tenant/organization pair is present but internally
                inconsistent (equal values, which indicates a wiring bug where
                the caller passed the same identifier twice).
        """
        missing = [
            field_name
            for field_name in MANDATORY_IDENTITY_FIELDS
            if not str(getattr(self, field_name) or "").strip()
        ]
        if missing:
            raise TenantContextError(
                "execution context is missing mandatory identity fields",
                details={"missing_fields": missing},
            )

        if self.tenant_id == self.organization_id:
            raise TenantContextError(
                "execution context tenant and organization must be distinct",
                details={"reason": "duplicate_scope_identifier"},
            )
        if self.task_attempt_id and not self.task_id:
            raise TenantContextError(
                "execution context task attempt requires an owning task",
                details={"field": "task_attempt_id", "reason": "missing_task_id"},
            )

    def __str__(self) -> str:
        """Compact rendering suitable for log lines (no attributes dump)."""
        return (
            f"ExecutionContext(execution_id={self.execution_id}, "
            f"tenant_id={self.tenant_id}, agent_id={self.agent_id}, "
            f"correlation_id={self.correlation_id})"
        )

    @property
    def invocation_identity(self) -> str:
        """Stable identity of *this invocation*, used for idempotency.

        Includes ``task_attempt_id`` when present so that a deliberate retry of
        the same task is a distinct invocation, while a duplicate delivery of
        one attempt collapses to the same key. This supports Rust's
        authoritative execution state rather than competing with it: the key
        only de-duplicates *within* the orchestrator's own in-flight window.
        """
        parts = [
            self.execution_id,
            self.task_id or "-",
            self.task_attempt_id or "-",
            self.agent_id,
            self.agent_version_id or "-",
        ]
        return "|".join(parts)

    def remaining_seconds(self, *, now_epoch_ms: int | None = None) -> float | None:
        """Seconds until the deadline, or ``None`` when unbounded.

        Args:
            now_epoch_ms: Test seam for the current time. Production callers
                omit it so the real clock is used.

        Returns:
            Non-negative seconds remaining. A deadline already in the past
            yields ``0.0`` rather than a negative number, so callers can compare
            against ``<= 0`` without special-casing.
        """
        if self.deadline_epoch_ms is None:
            return None
        now = int(time.time() * 1000) if now_epoch_ms is None else now_epoch_ms
        return max(0.0, (self.deadline_epoch_ms - now) / 1000.0)

    def is_expired(self, *, now_epoch_ms: int | None = None) -> bool:
        """Returns whether the deadline has passed."""
        remaining = self.remaining_seconds(now_epoch_ms=now_epoch_ms)
        return remaining is not None and remaining <= 0.0

    def with_deadline(self, deadline_epoch_ms: int | None) -> ExecutionContext:
        """Returns a copy carrying a (possibly new) deadline.

        Used by the runtime to tighten an inherited deadline before an
        expensive step. Identity fields cannot be replaced this way — there is
        deliberately no generic ``with_tenant``.
        """
        return replace(self, deadline_epoch_ms=deadline_epoch_ms)

    def to_log_fields(self) -> dict[str, Any]:
        """Structured log fields for this context.

        Only identity and correlation identifiers are included; ``attributes``
        is intentionally excluded because its contents are caller-defined and
        may be large. Callers wanting them must log them explicitly.
        """
        fields: dict[str, Any] = {
            "tenant_id": self.tenant_id,
            "organization_id": self.organization_id,
            "execution_id": self.execution_id,
            "agent_id": self.agent_id,
            "correlation_id": self.correlation_id,
            "request_id": self.request_id,
            "protocol_version": str(self.protocol_version),
            "caller": self.caller.subject,
        }
        if self.project_id:
            fields["project_id"] = self.project_id
        if self.task_id:
            fields["task_id"] = self.task_id
        if self.task_attempt_id:
            fields["task_attempt_id"] = self.task_attempt_id
        if self.agent_version_id:
            fields["agent_version_id"] = self.agent_version_id
        return fields


def build_execution_context(
    *,
    tenant_id: str,
    organization_id: str,
    execution_id: str,
    agent_id: str,
    correlation_id: str,
    request_id: str,
    caller: ServiceIdentity,
    protocol_version: str,
    project_id: str | None = None,
    task_id: str | None = None,
    task_attempt_id: str | None = None,
    agent_version_id: str | None = None,
    causation_id: str | None = None,
    deadline_epoch_ms: int | None = None,
    attributes: Mapping[str, Any] | None = None,
) -> ExecutionContext:
    """Builds a validated :class:`ExecutionContext` from boundary inputs.

    This is the single sanctioned construction path. It negotiates the protocol
    version *before* validating identity so that an incompatible caller is told
    about the version problem first — otherwise an old client sending an old
    field set would get a confusing identity error.

    Args:
        tenant_id: Authoritative tenant from Rust.
        organization_id: Authoritative organization from Rust.
        execution_id: Canonical execution identifier.
        agent_id: Agent to run.
        correlation_id: End-to-end correlation identifier.
        request_id: Platform request identifier.
        caller: Already-resolved authenticated caller identity.
        protocol_version: Raw declared protocol version string.
        project_id: Optional project scope.
        task_id: Optional owning task.
        task_attempt_id: Optional task attempt.
        agent_version_id: Optional pinned agent version.
        causation_id: Optional causing event identifier.
        deadline_epoch_ms: Optional absolute expiry.
        attributes: Optional non-authorization metadata.

    Returns:
        A validated, immutable execution context.

    Raises:
        ProtocolError: If the declared protocol version is unsupported.
        TenantContextError: If mandatory identity is missing or inconsistent.
        InvalidRequestError: If the deadline is in the past at intake, which
            means the caller asked for work that cannot possibly run.
    """
    negotiated = negotiate_protocol_version(protocol_version)

    context = ExecutionContext(
        tenant_id=tenant_id,
        organization_id=organization_id,
        execution_id=execution_id,
        agent_id=agent_id,
        correlation_id=correlation_id,
        request_id=request_id,
        protocol_version=negotiated,
        caller=caller,
        project_id=project_id,
        task_id=task_id,
        task_attempt_id=task_attempt_id,
        agent_version_id=agent_version_id,
        causation_id=causation_id,
        deadline_epoch_ms=deadline_epoch_ms,
        attributes=attributes,
    )

    if context.is_expired():
        raise InvalidRequestError(
            "execution deadline is already expired at intake",
            details={
                "execution_id": execution_id,
                "deadline_epoch_ms": deadline_epoch_ms,
            },
        )

    return context
