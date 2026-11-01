"""The bridge to the platform policy authority.

Policy is owned by Rust. Python asks; it does not decide. This module holds the
question, the answer, and the rule that makes the difference matter:

    A policy denial is never advisory.

The default bridge is :class:`DenyAllPolicy`, which refuses everything. That is
the safe default because an unconfigured deployment grants nothing rather than
everything — the converse, an allow-by-default bridge, would make a wiring
mistake into a security hole instead of an outage.

The bridge is also cached-with-care: a denial is never cached, because a
permission that was revoked must take effect immediately, while an approval may
be remembered briefly to keep the hot path cheap.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
)
from mas_orchestrator.guardrails.policy import (
    PolicyBridge,
    PolicyDecision,
    PolicyResult,
    PolicyStage,
    allowed_or_denied,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: How long an approval may be remembered. Short enough that a revocation takes
#: effect quickly, long enough to matter within one execution.
APPROVAL_CACHE_SECONDS: Final[float] = 30.0


@runtime_checkable
class PolicyAuthority(Protocol):
    """The remote authority that answers policy questions."""

    async def evaluate(
        self,
        stage: PolicyStage,
        subject: str,
        context: dict[str, str],
    ) -> PolicyResult:
        """Asks for a decision.

        Implementations must return a denial rather than raising when the
        authority is unreachable: a policy engine that is down must stop work,
        not let it through.
        """
        ...


@dataclass(frozen=True, slots=True)
class BridgeConfig:
    """How the bridge behaves.

    Attributes:
        cache_approvals: Whether to remember approvals briefly.
        cache_seconds: How long an approval is remembered.
        deny_on_error: Whether an authority failure denies. This must stay
            ``True`` in production; :func:`validate_bridge_config` enforces it.
    """

    cache_approvals: bool = True
    cache_seconds: float = APPROVAL_CACHE_SECONDS
    deny_on_error: bool = True

    def __post_init__(self) -> None:
        if self.cache_seconds <= 0:
            raise ConfigurationError(
                "cache_seconds must be positive",
                details={"field": "cache_seconds"},
            )


def validate_bridge_config(config: BridgeConfig, *, environment: str) -> None:
    """Refuses a fail-open bridge in production.

    Raises:
        ConfigurationError: In production with ``deny_on_error`` disabled.
    """
    from mas_orchestrator.config.enums import Environment

    if not config.deny_on_error and environment == Environment.PRODUCTION.value:
        raise ConfigurationError(
            "a policy bridge that allows on authority failure is not "
            "permitted in production: a policy outage must stop work, not "
            "waive it",
            details={"environment": environment},
        )


class CachingPolicyBridge:
    """A bridge that remembers approvals and never remembers denials.

    The asymmetry is the point. Caching an approval is a performance
    optimisation; caching a denial would let a revoked permission keep
    denying — or, worse, a stale approval keep allowing — past the moment the
    authority changed its mind.
    """

    def __init__(
        self,
        authority: PolicyAuthority,
        *,
        config: BridgeConfig | None = None,
    ) -> None:
        self._authority = authority
        self._config = config or BridgeConfig()
        self._approvals: dict[tuple[str, str], float] = {}

    @property
    def config(self) -> BridgeConfig:
        """The configuration in force."""
        return self._config

    async def check(
        self,
        stage: PolicyStage,
        subject: str,
        context: dict[str, str],
    ) -> PolicyResult:
        """Asks the authority, using a cached approval when one is fresh.

        Returns:
            A :class:`PolicyResult`. An unreachable authority produces a
            denial rather than an exception or an allow.
        """
        key = (stage.value, subject)

        if self._config.cache_approvals:
            cached_at = self._approvals.get(key)
            if cached_at is not None:
                if time.monotonic() - cached_at <= self._config.cache_seconds:
                    return PolicyResult(
                        decision=PolicyDecision.ALLOW,
                        stage=stage,
                        subject=subject,
                        reason="cached approval",
                    )
                # Expired: forget it and re-ask rather than serving it stale.
                del self._approvals[key]

        try:
            result = await self._authority.evaluate(stage, subject, context)
        except Exception as error:
            _LOGGER.error(
                "policy authority unreachable",
                extra={
                    "event": "policy.authority_unreachable",
                    "subject": subject,
                    "error": type(error).__name__,
                },
            )
            return PolicyResult(
                decision=(
                    PolicyDecision.DENY if self._config.deny_on_error else PolicyDecision.ALLOW
                ),
                stage=stage,
                subject=subject,
                reason=f"authority unreachable: {type(error).__name__}",
            )

        if result.decision is PolicyDecision.ALLOW and self._config.cache_approvals:
            self._approvals[key] = time.monotonic()
        elif result.decision is not PolicyDecision.ALLOW:
            # Drop any cached approval for this subject immediately: the
            # authority has just said something other than yes.
            self._approvals.pop(key, None)
        return result

    def invalidate(self, subject: str | None = None) -> None:
        """Forgets cached approvals.

        Called when permissions change. Passing ``None`` clears everything,
        which is the correct response to a bulk revocation.
        """
        if subject is None:
            self._approvals.clear()
            return
        for key in [k for k in self._approvals if k[1] == subject]:
            del self._approvals[key]


class StaticPolicyBridge:
    """A bridge backed by a fixed mapping of subject to decision.

    For tests and for a deployment whose policy is fully expressed in
    configuration. Refuses to construct in production, where policy belongs to
    the Rust authority.
    """

    def __init__(
        self,
        decisions: dict[str, PolicyDecision],
        *,
        environment: str,
        default: PolicyDecision = PolicyDecision.DENY,
    ) -> None:
        from mas_orchestrator.config.enums import Environment

        if environment == Environment.PRODUCTION.value:
            raise ConfigurationError(
                "a static policy bridge is not permitted in production: "
                "policy decisions belong to the Rust authority",
                details={"environment": environment},
            )
        self._decisions = dict(decisions)
        self._default = default

    async def check(
        self,
        stage: PolicyStage,
        subject: str,
        context: dict[str, str],
    ) -> PolicyResult:
        """Returns the configured decision."""
        decision = self._decisions.get(subject, self._default)
        return PolicyResult(decision=decision, stage=stage, subject=subject, reason="static policy")


async def enforce(
    bridge: PolicyBridge,
    stage: PolicyStage,
    subject: str,
    context: dict[str, str],
) -> PolicyResult:
    """Asks the bridge and raises on anything other than an allow.

    Raises:
        PolicyDeniedError: If the bridge does not allow.
    """
    return await allowed_or_denied(bridge, stage, subject, context)
