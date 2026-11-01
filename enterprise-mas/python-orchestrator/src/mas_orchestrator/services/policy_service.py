"""Policy service.

Policy is owned by Rust. This service is the Python-side front door for asking
about it, and it exists to make one guarantee easy to uphold and hard to
bypass:

    Nothing runs because nobody said no.

Every method here goes through an injected :class:`PolicyBridge`, whose default
is :class:`~mas_orchestrator.guardrails.policy.DenyAllPolicy`. With no
authority configured, every check denies. That is the opposite of the usual
default, and it is deliberate: an unconfigured system should fail loudly rather
than grant silently.

The second thing this service does is distinguish *denial* from *approval
required*. Both stop the work, but they are not the same event: a denial means
"never", while approval-required means "not yet, and a human can unblock it".
Collapsing them would make every blocked action look permanent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import PolicyDeniedError
from mas_orchestrator.guardrails.policy import (
    PolicyDecision,
    PolicyResult,
    PolicyStage,
)

if TYPE_CHECKING:
    from mas_orchestrator.events.emitter import EventEmitter
    from mas_orchestrator.guardrails.policy import PolicyBridge

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PolicyVerdict:
    """The outcome of a policy question, in domain terms.

    Attributes:
        allowed: Whether the work may proceed. Only ``ALLOW`` is true.
        decision: The raw decision.
        subject: What was asked about.
        reason: Why, when a reason was given.
        needs_approval: Whether a human could unblock this.
    """

    allowed: bool
    decision: PolicyDecision
    subject: str
    reason: str = ""
    needs_approval: bool = False

    @property
    def blocked(self) -> bool:
        """Whether the work must not proceed."""
        return not self.allowed


class PolicyService:
    """Asks the policy authority, and nothing else."""

    def __init__(
        self,
        bridge: PolicyBridge,
        *,
        emitter: EventEmitter | None = None,
    ) -> None:
        self._bridge = bridge
        self._emitter = emitter

    @property
    def bridge(self) -> PolicyBridge:
        """The bridge in use."""
        return self._bridge

    async def ask(
        self,
        stage: PolicyStage,
        subject: str,
        context: dict[str, str],
    ) -> PolicyVerdict:
        """Asks whether something is permitted, without raising.

        Returns:
            A :class:`PolicyVerdict`.
        """
        result = await self._bridge.check(stage, subject, context)
        verdict = _to_verdict(result)
        if verdict.blocked and self._emitter is not None:
            from mas_orchestrator.events.types import policy_denial

            await self._emitter.emit(
                policy_denial(
                    tenant_id=context.get("tenant_id", ""),
                    execution_id=context.get("execution_id", ""),
                    correlation_id=context.get("correlation_id", ""),
                    subject=subject,
                )
            )
        return verdict

    async def require(
        self,
        stage: PolicyStage,
        subject: str,
        context: dict[str, str],
    ) -> PolicyResult:
        """Asks, raising on anything other than an allow.

        Raises:
            PolicyDeniedError: If the authority does not allow.
        """
        result = await self._bridge.check(stage, subject, context)
        if result.decision is not PolicyDecision.ALLOW:
            verdict = _to_verdict(result)
            _LOGGER.info(
                "policy blocked an action",
                extra={
                    "event": "policy.blocked",
                    "subject": subject,
                    "decision": result.decision.value,
                    "needs_approval": verdict.needs_approval,
                },
            )
            raise PolicyDeniedError(
                f"policy does not permit {subject}",
                details={
                    "subject": subject,
                    "decision": result.decision.value,
                    "reason": result.reason,
                    "needs_approval": verdict.needs_approval,
                },
            )
        return result

    async def may_call_tool(
        self,
        subject: str,
        context: dict[str, str],
    ) -> PolicyVerdict:
        """Whether a tool call is permitted."""
        return await self.ask(PolicyStage.BEFORE_MODEL, subject, context)

    async def may_return_output(
        self,
        subject: str,
        context: dict[str, str],
    ) -> PolicyVerdict:
        """Whether a result may be returned."""
        return await self.ask(PolicyStage.AFTER_MODEL, subject, context)


def _to_verdict(result: PolicyResult) -> PolicyVerdict:
    """Converts a bridge result into a domain verdict."""
    return PolicyVerdict(
        allowed=result.decision is PolicyDecision.ALLOW,
        decision=result.decision,
        subject=result.subject,
        reason=result.reason,
        needs_approval=result.decision is PolicyDecision.REQUIRE_APPROVAL,
    )
