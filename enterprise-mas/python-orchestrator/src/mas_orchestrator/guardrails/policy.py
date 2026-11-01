"""The policy bridge to the authoritative authority.

Rust owns authorization and policy (spec §5), so Python must not decide
permissions from its own configuration. This module defines the interface to
that decision and a **fail-closed** default.

The default denies. That is the entire point: if the bridge is not wired up,
the system must not start granting things because nothing said no.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class PolicyDecision(StrEnum):
    """The authority's answer."""

    ALLOW = "allow"
    DENY = "deny"
    REQUIRE_APPROVAL = "require_approval"


class PolicyStage(StrEnum):
    """Where in the lifecycle a check runs."""

    BEFORE_MODEL = "before_model"
    DURING_EXECUTION = "during_execution"
    AFTER_MODEL = "after_model"


@dataclass(frozen=True, slots=True)
class PolicyResult:
    """One decision.

    Attributes:
        decision: The answer.
        stage: Where it applies.
        subject: What it was about — a tool id, a model key, an action.
        reason: Why, carried into the audit trail.
        rule: Which rule produced it, when the authority names one.
        decision_id: Opaque id for correlation with the authority's record.
    """

    decision: PolicyDecision
    stage: PolicyStage
    subject: str
    reason: str = ""
    rule: str | None = None
    decision_id: str | None = None

    @property
    def allowed(self) -> bool:
        """Whether execution may proceed.

        ``REQUIRE_APPROVAL`` is not an allow: an action awaiting a human
        decision must wait for it.
        """
        return self.decision is PolicyDecision.ALLOW


@runtime_checkable
class PolicyBridge(Protocol):
    """Interface to the authoritative policy decision."""

    async def check(
        self,
        stage: PolicyStage,
        subject: str,
        context: Mapping[str, object],
    ) -> PolicyResult:
        """Asks the authority for one decision.

        Asynchronous because the real bridge asks a remote authority; a
        synchronous contract would force either blocking or a hidden thread.

        Implementations must return a denial rather than raising: an exception
        here means the authority itself failed, which is a dependency failure
        and not a policy outcome.
        """
        ...


class DenyAllPolicy:
    """The fail-closed default.

    Every check is denied with a reason that says so. This is what runs when
    no bridge is configured, and it is intentional: the correct response to a
    denied call in production is to wire up the real bridge, never to swap in
    something permissive.
    """

    async def check(
        self,
        stage: PolicyStage,
        subject: str,
        context: Mapping[str, object],
    ) -> PolicyResult:
        """Always denies."""
        return PolicyResult(
            decision=PolicyDecision.DENY,
            stage=stage,
            subject=subject,
            reason="no policy authority is configured",
            rule="fail_closed",
        )


class AllowAllPolicy:
    """Allows everything. Tests only.

    The constructor refuses a production environment, so this cannot be
    selected as a production implementation by accident or by default.
    """

    def __init__(self, *, environment: str) -> None:
        from mas_orchestrator.config.enums import Environment

        if Environment(environment) is Environment.PRODUCTION:
            raise ValueError("AllowAllPolicy must not be used in production")
        self._environment = environment

    async def check(
        self,
        stage: PolicyStage,
        subject: str,
        context: Mapping[str, object],
    ) -> PolicyResult:
        """Always allows."""
        return PolicyResult(
            decision=PolicyDecision.ALLOW,
            stage=stage,
            subject=subject,
            reason="allowed by test policy",
            rule="allow_all",
        )


def require_allowed(
    result: PolicyResult,
) -> PolicyResult:
    """Converts a non-allow into a typed error.

    Raises:
        PolicyDeniedError: If the decision is not ``ALLOW``, carrying the
            reason, rule and decision id into ``details`` so the audit trail
            can be joined up.
    """
    from mas_orchestrator.contracts.errors import PolicyDeniedError

    if result.allowed:
        return result

    _LOGGER.warning(
        "policy denied",
        extra={
            "event": "guardrail.policy_denied",
            "stage": result.stage.value,
            "subject": result.subject,
            "rule": result.rule,
        },
    )
    raise PolicyDeniedError(
        f"policy denied '{result.subject}': {result.reason}",
        details={
            "stage": result.stage.value,
            "subject": result.subject,
            "decision": result.decision.value,
            "reason": result.reason,
            "rule": result.rule,
            "decision_id": result.decision_id,
        },
    )


async def allowed_or_denied(
    bridge: PolicyBridge,
    stage: PolicyStage,
    subject: str,
    context: Mapping[str, object],
) -> PolicyResult:
    """Runs a check and raises unless it is allowed."""
    return require_allowed(await bridge.check(stage, subject, context))
