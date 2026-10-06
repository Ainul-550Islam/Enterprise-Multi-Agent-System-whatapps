"""Selecting which agent should handle a request.

Selection is policy, not preference: the same allowlist discipline that governs
models governs agents. An agent that is not published, belongs to another
tenant, or was not granted for this request is never selected, and the caller
is told *why* rather than being handed a substitute.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.agents.base import AgentKind, AgentSpec
from mas_orchestrator.contracts.errors import AgentNotFoundError, ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.agents.registry import AgentRegistry

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AgentRoutingRequest:
    """What a request needs from an agent.

    Attributes:
        require_tools: The agent must be able to call tools.
        require_delegation: The agent must be able to delegate.
        kind: Required agent kind, when one is specified.
        preferred_agents: Preference order, applied after filtering.
        tags: The agent must carry every one of these tags.
    """

    require_tools: bool = False
    require_delegation: bool = False
    kind: AgentKind | None = None
    preferred_agents: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AgentRejection:
    """Why an agent was not selected.

    Attributes:
        agent_id: The candidate.
        reason: Stable reason string.
    """

    agent_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class AgentRoutingPolicy:
    """Which agents a request may use.

    Attributes:
        allowed_agents: Permitted agent ids. Empty means none.
        preferred_agents: Preference order applied after filtering.
        allow_deprecated: Whether deprecated agents are eligible.
    """

    allowed_agents: frozenset[str] = frozenset()
    preferred_agents: tuple[str, ...] = ()
    allow_deprecated: bool = True

    def is_allowed(self, agent_id: str) -> bool:
        """Whether an agent is on the allowlist."""
        return agent_id in self.allowed_agents


class AgentRouter:
    """Selects an agent from a registry under a policy."""

    def __init__(
        self,
        registry: AgentRegistry,
        policy: AgentRoutingPolicy | None = None,
    ) -> None:
        self._registry = registry
        self._policy = policy or AgentRoutingPolicy()

    def evaluate(
        self,
        spec: AgentSpec,
        request: AgentRoutingRequest,
    ) -> AgentRejection | None:
        """Returns ``None`` if the agent is eligible, otherwise the reason."""
        if not self._policy.is_allowed(spec.agent_id):
            return AgentRejection(spec.agent_id, "not_allowlisted")

        if not spec.is_runnable:
            return AgentRejection(spec.agent_id, f"agent_{spec.status.value}")

        if not self._policy.allow_deprecated and spec.status.value == "deprecated":
            return AgentRejection(spec.agent_id, "deprecated")

        if request.kind is not None and spec.kind is not request.kind:
            return AgentRejection(spec.agent_id, "wrong_kind")

        # "Can call tools" means two things: the kind permits it, and at least
        # one tool was actually granted. An agent of a tool-capable kind with an
        # empty allowlist cannot call anything, so it must not satisfy a
        # requirement that the caller is relying on.
        if request.require_tools and not (spec.may_use_tools and spec.allowed_tools):
            return AgentRejection(spec.agent_id, "no_tool_support")

        if request.require_delegation and spec.kind is not AgentKind.SUPERVISOR:
            return AgentRejection(spec.agent_id, "no_delegation_support")

        missing_tags = [tag for tag in request.tags if tag not in spec.tags]
        if missing_tags:
            return AgentRejection(spec.agent_id, "missing_tags")

        return None

    def select(
        self,
        tenant_id: str,
        request: AgentRoutingRequest | None = None,
    ) -> AgentSpec:
        """Selects an agent.

        Raises:
            AgentNotFoundError: If no registered agent satisfies the policy and
                the request. The error carries every rejection, so a caller can
                see exactly what was considered and why.
        """
        request = request or AgentRoutingRequest()
        rejections: list[AgentRejection] = []
        eligible: list[AgentSpec] = []

        for spec in self._registry.list_agents(tenant_id):
            rejection = self.evaluate(spec, request)
            if rejection is None:
                eligible.append(spec)
            else:
                rejections.append(rejection)

        if not eligible:
            raise AgentNotFoundError(
                "no agent satisfies the routing policy",
                details={
                    "tenant_id": tenant_id,
                    "rejections": [
                        {"agent_id": item.agent_id, "reason": item.reason} for item in rejections
                    ],
                },
            )

        preference = request.preferred_agents or self._policy.preferred_agents
        eligible.sort(key=lambda spec: _rank(spec.agent_id, preference))
        selected = eligible[0]

        _LOGGER.info(
            "agent selected",
            extra={
                "event": "agent.selected",
                "agent_id": selected.agent_id,
                "kind": selected.kind.value,
                "tenant_id": tenant_id,
                "eligible": [spec.agent_id for spec in eligible],
            },
        )
        return selected

    def candidates(
        self,
        tenant_id: str,
        request: AgentRoutingRequest | None = None,
    ) -> tuple[AgentSpec, ...]:
        """Every eligible agent, in preference order."""
        request = request or AgentRoutingRequest()
        eligible = [
            spec
            for spec in self._registry.list_agents(tenant_id)
            if self.evaluate(spec, request) is None
        ]
        preference = request.preferred_agents or self._policy.preferred_agents
        eligible.sort(key=lambda spec: _rank(spec.agent_id, preference))
        return tuple(eligible)

    def explain(
        self,
        tenant_id: str,
        request: AgentRoutingRequest | None = None,
    ) -> tuple[AgentRejection, ...]:
        """Why each registered agent was rejected."""
        request = request or AgentRoutingRequest()
        return tuple(
            rejection
            for spec in self._registry.list_agents(tenant_id)
            if (rejection := self.evaluate(spec, request)) is not None
        )


def _rank(agent_id: str, preference: Sequence[str]) -> tuple[int, str]:
    """Sort key: preferred agents first, then a stable id ordering."""
    try:
        return (preference.index(agent_id), agent_id)
    except ValueError:
        return (len(preference), agent_id)


def build_policy(
    allowed: Sequence[str],
    *,
    preferred: Sequence[str] = (),
    allow_deprecated: bool = True,
) -> AgentRoutingPolicy:
    """Builds a routing policy from plain sequences.

    Raises:
        ConfigurationError: If a preferred agent is not on the allowlist,
            since such a preference could never be honoured.
    """
    allowed_set = frozenset(allowed)
    unknown = [agent_id for agent_id in preferred if agent_id not in allowed_set]
    if unknown:
        raise ConfigurationError(
            "preferred agents must be on the allowlist",
            details={"unknown": unknown},
        )
    return AgentRoutingPolicy(
        allowed_agents=allowed_set,
        preferred_agents=tuple(preferred),
        allow_deprecated=allow_deprecated,
    )
