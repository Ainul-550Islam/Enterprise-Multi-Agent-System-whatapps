"""Tool authorisation.

The model cannot authorise its own tool calls. Every invocation must be
approved by an *external* authority — Rust owns policy, tenancy and
authorization, so this module defines the interface to that authority and a
**fail-closed** default.

The important design property is that denial is the safe default and that a
missing bridge cannot be mistaken for permission.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    ToolPermissionDeniedError,
)

if TYPE_CHECKING:
    from mas_orchestrator.tools.types import ToolDefinition, ToolInvocationContext


class PermissionDecision(StrEnum):
    """Outcome of an authorisation check."""

    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_APPROVAL = "require_approval"


@dataclass(frozen=True, slots=True)
class PermissionResult:
    """A recorded authorisation decision.

    Attributes:
        decision: The outcome.
        tool_id: Tool the decision applies to.
        tool_version: Version of the definition the decision applies to.
        reason: Human-readable justification, carried into the audit trail.
        decision_id: Opaque identifier of the decision on the authority side,
            so a Python-side invocation can be correlated with the Rust-side
            audit record.
        source: Which bridge produced this decision.
    """

    decision: PermissionDecision
    tool_id: str
    tool_version: int
    reason: str = ""
    decision_id: str | None = None
    source: str = "unknown"

    @property
    def is_allowed(self) -> bool:
        """Whether execution may proceed.

        ``REQUIRE_APPROVAL`` is **not** an allow: a tool that needs a human
        approval step must wait for that step, not run optimistically.
        """
        return self.decision is PermissionDecision.ALLOW


@runtime_checkable
class ToolPermissionBridge(Protocol):
    """Interface to the authoritative (Rust-side) permission decision.

    Implementations must not decide permission locally from Python-side
    configuration, because Rust is the source of truth for authorization.
    """

    def check(
        self,
        tool: ToolDefinition,
        context: ToolInvocationContext,
    ) -> PermissionResult:
        """Returns the decision for one invocation.

        Implementations must never raise to signal denial; they return a
        ``DENY`` result. An exception here means the authority itself failed,
        which must be reported as a dependency failure rather than treated as
        permission.
        """
        ...


class DenyAllPermissionBridge:
    """The fail-closed default: nothing is permitted.

    This exists so that an executor built without an authority cannot execute
    anything, rather than executing everything. It is **not** a development
    convenience and it must never be selected in production: if it denies a
    call in production, the correct response is to wire up the real bridge,
    not to swap in a permissive one.
    """

    def __init__(self, reason: str = "no permission authority is configured") -> None:
        self._reason = reason

    def check(
        self,
        tool: ToolDefinition,
        context: ToolInvocationContext,
    ) -> PermissionResult:
        """Always denies."""
        return PermissionResult(
            decision=PermissionDecision.DENY,
            tool_id=tool.tool_id,
            tool_version=tool.version.number,
            reason=self._reason,
            source="deny_all",
        )


class AllowListPermissionBridge:
    """Permits an explicit set of ``(tenant_id, tool_id)`` pairs.

    Intended for **tests and local development only**. The constructor refuses
    to be built in a production environment, so this cannot be selected as a
    production implementation by accident.
    """

    def __init__(
        self,
        allowed: frozenset[tuple[str, str]] | set[tuple[str, str]],
        *,
        environment: str,
        require_sensitive_explicit: bool = True,
    ) -> None:
        if environment.lower() in {"production", "prod"}:
            raise ConfigurationError(
                "AllowListPermissionBridge must not be used in production; "
                "wire up the authoritative Rust permission bridge instead",
                details={"environment": environment},
            )
        self._allowed = frozenset(allowed)
        self._require_sensitive_explicit = require_sensitive_explicit

    def check(
        self,
        tool: ToolDefinition,
        context: ToolInvocationContext,
    ) -> PermissionResult:
        """Allows only the configured pairs.

        Cross-tenant access is refused even when the tool id matches, because
        the pair — not the tool id alone — is the permission.
        """
        pair = (context.tenant_id, tool.tool_id)
        if pair not in self._allowed:
            return PermissionResult(
                decision=PermissionDecision.DENY,
                tool_id=tool.tool_id,
                tool_version=tool.version.number,
                reason="tool is not on the allowlist for this tenant",
                source="allowlist",
            )

        if (
            self._require_sensitive_explicit
            and tool.safety.value in {"sensitive", "destructive"}
            and not self._is_explicitly_sensitive(pair)
        ):
            return PermissionResult(
                decision=PermissionDecision.REQUIRE_APPROVAL,
                tool_id=tool.tool_id,
                tool_version=tool.version.number,
                reason=f"tool is {tool.safety.value} and requires explicit approval",
                source="allowlist",
            )

        return PermissionResult(
            decision=PermissionDecision.ALLOW,
            tool_id=tool.tool_id,
            tool_version=tool.version.number,
            reason="allowed by development allowlist",
            source="allowlist",
        )

    def _is_explicitly_sensitive(self, pair: tuple[str, str]) -> bool:
        """Whether this pair was granted with sensitive tools in mind.

        The allowlist is deliberately flat, so a sensitive tool can only be
        approved by naming it here. This keeps "sensitive" from being a no-op
        in development while remaining impossible in production.
        """
        return pair in self._allowed and pair[1].startswith(("sensitive:", "destructive:"))


def enforce_permission(
    tool: ToolDefinition,
    context: ToolInvocationContext,
    bridge: ToolPermissionBridge,
) -> PermissionResult:
    """Runs the check and converts a non-allow into a typed error.

    Raises:
        ToolPermissionDeniedError: If the decision is not ``ALLOW``.
    """
    if not tool.is_invokable:
        raise ToolPermissionDeniedError(
            f"tool '{tool.tool_id}' is {tool.status.value} and cannot be invoked",
            details={
                "tool_id": tool.tool_id,
                "status": tool.status.value,
            },
        )

    if tool.tenant_id != context.tenant_id:
        # Defence in depth: Rust already scopes tools to a tenant, but the
        # executor must not rely solely on the registry returning the right
        # row — a cross-tenant call must be impossible even if one slips in.
        raise ToolPermissionDeniedError(
            "tool does not belong to the requesting tenant",
            details={"tool_id": tool.tool_id, "tool_tenant": tool.tenant_id},
        )

    result = bridge.check(tool, context)
    if not result.is_allowed:
        raise ToolPermissionDeniedError(
            f"tool invocation denied: {result.reason}",
            details={
                "tool_id": tool.tool_id,
                "tool_version": tool.version.number,
                "decision": result.decision.value,
                "reason": result.reason,
                "decision_id": result.decision_id,
                "source": result.source,
            },
        )
    return result
