"""Agent registry.

Mirrors the tool registry's rules, for the same reason: an agent whose
resolution depends on registration order is an agent whose behaviour cannot be
reproduced from its definition.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import AgentNotFoundError, ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable

    from mas_orchestrator.agents.base import AgentKind, AgentSpec, BaseAgent

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class AgentRegistry:
    """Holds agent factories, keyed by tenant and agent id.

    The registry stores **factories rather than instances**, because an agent
    run carries per-execution state (cost ledger, cancellation) that must not
    be shared between executions. Registering an already-built agent would leak
    one execution's state into the next.
    """

    def __init__(self, *, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._factories: dict[tuple[str, str], Callable[[], BaseAgent]] = {}
        self._specs: dict[tuple[str, str], AgentSpec] = {}

    def register(
        self,
        spec: AgentSpec,
        factory: Callable[[], BaseAgent],
        *,
        dev_only: bool = False,
    ) -> None:
        """Registers an agent.

        Args:
            spec: The agent definition.
            factory: Builds a fresh agent instance per execution.
            dev_only: Refused outside development and test.

        Raises:
            ConfigurationError: If the agent id is already registered for this
                tenant, or a ``dev_only`` agent is registered in production.
        """
        if dev_only and self._environment is Environment.PRODUCTION:
            raise ConfigurationError(
                "this agent is development-only and must not be registered in production",
                details={"agent_id": spec.agent_id, "environment": self._environment.value},
            )

        key = (spec.tenant_id, spec.agent_id)
        if key in self._specs:
            raise ConfigurationError(
                "agent is already registered",
                details={"agent_id": spec.agent_id, "tenant_id": spec.tenant_id},
            )

        self._specs[key] = spec
        self._factories[key] = factory
        _LOGGER.info(
            "agent registered",
            extra={
                "event": "agent.registered",
                "agent_id": spec.agent_id,
                "kind": spec.kind.value,
                "tenant_id": spec.tenant_id,
            },
        )

    def register_builtin(
        self,
        spec: AgentSpec,
        factory: Callable[[], BaseAgent],
    ) -> None:
        """Registers a tenant-visible, immutable product-owned agent.

        Built-ins are normal registry entries for routing/listing, but their
        spec is marked read-only. Re-registering the exact same built-in is
        idempotent; replacing its definition under the same id is refused.
        """
        from dataclasses import replace

        builtin = spec if spec.read_only else replace(spec, read_only=True)
        key = (builtin.tenant_id, builtin.agent_id)
        existing = self._specs.get(key)
        if existing is not None:
            if existing == builtin and existing.read_only:
                return
            raise ConfigurationError(
                "built-in agent id collides with a different registered agent",
                details={"agent_id": builtin.agent_id, "tenant_id": builtin.tenant_id},
            )
        self.register(builtin, factory)

    def is_read_only(self, tenant_id: str, agent_id: str) -> bool:
        """Whether an agent is a product-owned, non-editable built-in."""
        spec = self._specs.get((tenant_id, agent_id))
        return spec.read_only if spec is not None else False

    def spec(self, tenant_id: str, agent_id: str) -> AgentSpec:
        """Returns an agent's definition.

        Raises:
            AgentNotFoundError: If the agent is not registered for this tenant.
        """
        spec = self._specs.get((tenant_id, agent_id))
        if spec is None:
            raise AgentNotFoundError(
                f"agent '{agent_id}' is not registered",
                details={"agent_id": agent_id, "tenant_id": tenant_id},
            )
        return spec

    def has(self, tenant_id: str, agent_id: str) -> bool:
        """Whether an agent is registered for this tenant."""
        return (tenant_id, agent_id) in self._specs

    def build(self, tenant_id: str, agent_id: str) -> BaseAgent:
        """Builds a fresh agent instance.

        Raises:
            AgentNotFoundError: If the agent is not registered for this tenant.
        """
        key = (tenant_id, agent_id)
        factory = self._factories.get(key)
        if factory is None:
            raise AgentNotFoundError(
                f"agent '{agent_id}' is not registered",
                details={"agent_id": agent_id, "tenant_id": tenant_id},
            )
        return factory()

    def list_agents(self, tenant_id: str) -> tuple[AgentSpec, ...]:
        """Every agent for a tenant, sorted by id."""
        specs = [spec for (owner, _), spec in self._specs.items() if owner == tenant_id]
        return tuple(sorted(specs, key=lambda item: item.agent_id))

    def list_by_kind(self, tenant_id: str, kind: AgentKind) -> tuple[AgentSpec, ...]:
        """Agents of one kind, sorted by id."""
        return tuple(spec for spec in self.list_agents(tenant_id) if spec.kind is kind)

    def __len__(self) -> int:
        """Number of registered agents across all tenants."""
        return len(self._specs)
