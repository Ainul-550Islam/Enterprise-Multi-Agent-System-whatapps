"""Agent management service.

The agent registry does storage and lookup; this service decides what may be
*done* with an agent. The distinction matters because every operation here
changes what an execution can reach, so each one is tenant-scoped and
validated:

* :meth:`AgentService.resolve` refuses an agent that is not runnable, rather
  than returning it and letting the caller notice later.
* :meth:`AgentService.register` refuses to overwrite an existing version. Agent
  versions are immutable: an execution that started against version 3 must not
  discover halfway through that version 3 now means something else.

The service is also where lookup is timed and reported, because "agent lookup"
is one of the observables spec §17 asks for and it belongs to the operation,
not to the storage layer.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.agents.base import AgentKind, AgentSpec, BaseAgent
from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    AgentVersionNotFoundError,
    InvalidRequestError,
)
from mas_orchestrator.events.types import agent_lookup

if TYPE_CHECKING:
    from collections.abc import Callable

    from mas_orchestrator.agents.registry import AgentRegistry
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.contracts.enums import AgentStatus
    from mas_orchestrator.events.emitter import EventEmitter

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AgentSummary:
    """A registry entry as reported by the service.

    Attributes:
        agent_id: Identifier.
        agent_version_id: Version identifier.
        name: Human-readable name.
        kind: Agent kind.
        runnable: Whether it may execute.
        status: Lifecycle status.
        tool_count: How many tools it may call.
    """

    agent_id: str
    agent_version_id: str
    name: str
    kind: AgentKind
    runnable: bool
    status: str
    tool_count: int


def _observe_lookup_telemetry(task: asyncio.Task[bool]) -> None:
    """Retrieves and safely reports a failed background lookup emission.

    ``AgentService.spec`` is deliberately synchronous, so lookup telemetry is
    scheduled rather than awaited. A task that fails after the caller returns
    must still have its exception retrieved; otherwise asyncio reports it only
    later as an unobserved task exception and hides the useful operational
    signal. Cancellation is expected while the event loop drains and is not a
    telemetry delivery failure.
    """
    if task.cancelled():
        return
    try:
        error = task.exception()
    except asyncio.CancelledError:
        return
    if error is not None:
        _LOGGER.warning(
            "agent lookup telemetry emission failed",
            extra={
                "event": "agent.lookup_emit_failed",
                "error_type": type(error).__name__,
            },
        )


class AgentService:
    """Tenant-scoped agent operations."""

    def __init__(
        self,
        registry: AgentRegistry,
        *,
        emitter: EventEmitter | None = None,
    ) -> None:
        self._registry = registry
        self._emitter = emitter

    @property
    def registry(self) -> AgentRegistry:
        """The underlying registry."""
        return self._registry

    def summary(self, context: ExecutionContext) -> AgentSummary:
        """Builds a summary for an agent in the context.

        Raises:
            AgentNotFoundError: If it is not registered for this tenant.
        """
        spec = self.spec(context)
        return AgentSummary(
            agent_id=spec.agent_id,
            agent_version_id=spec.agent_version_id,
            name=spec.name,
            kind=spec.kind,
            runnable=spec.is_runnable,
            status=spec.status.value,
            tool_count=len(spec.allowed_tools),
        )

    def spec(self, context: ExecutionContext) -> AgentSpec:
        """Returns an agent's definition, emitting a lookup event.

        Raises:
            AgentNotFoundError: If it is not registered for this tenant.
        """
        started = time.monotonic()
        tenant_id = context.tenant_id
        agent_id = context.agent_id
        resolved = True
        try:
            spec = self._registry.spec(tenant_id, agent_id)
        except AgentNotFoundError:
            resolved = False
            raise
        finally:
            duration = int((time.monotonic() - started) * 1000)
            if self._emitter is not None:
                coro = self._emitter.emit(
                    agent_lookup(
                        tenant_id=tenant_id,
                        execution_id=context.execution_id,
                        correlation_id=context.correlation_id,
                        agent_id=agent_id,
                        resolved=resolved,
                        duration_ms=duration,
                    )
                )
                try:
                    task = asyncio.get_running_loop().create_task(coro)
                except RuntimeError:
                    coro.close()
                else:
                    task.add_done_callback(_observe_lookup_telemetry)
        return spec

    def resolve(self, context: ExecutionContext) -> AgentSpec:
        """Returns a runnable agent's definition.

        Unlike :meth:`spec`, this refuses an agent in a non-runnable state.
        Callers on the execution path should use this one: an agent that
        resolves but cannot run produces a failure much later, at the point of
        the first model call, which is the worst place to discover it.

        Raises:
            AgentNotFoundError: If not registered for this tenant.
            InvalidRequestError: If registered but not runnable.
        """
        spec = self.spec(context)
        if not spec.is_runnable:
            raise InvalidRequestError(
                "agent is not in a runnable state",
                details={
                    "agent_id": spec.agent_id,
                    "status": spec.status.value,
                },
            )
        return spec

    def build(self, context: ExecutionContext) -> BaseAgent:
        """Builds a runnable agent instance.

        Raises:
            AgentNotFoundError: If not registered for this tenant.
            InvalidRequestError: If not runnable.
        """
        self.resolve(context)
        return self._registry.build(context.tenant_id, context.agent_id)

    def register(
        self,
        spec: AgentSpec,
        factory: Callable[[], BaseAgent],
    ) -> None:
        """Registers a new agent version.

        The factory is required because the registry builds a fresh instance per
        execution. An agent that is shared between executions would carry state
        from one tenant's run into another's.

        Raises:
            ConfigurationError: If that version already exists. Versions are
                immutable: replacing one would change the meaning of an
                execution already in flight.
        """
        self._registry.register(spec, factory)

    def list_agents(self, tenant_id: str) -> tuple[AgentSpec, ...]:
        """Every agent registered to a tenant."""
        if not tenant_id.strip():
            raise InvalidRequestError("a tenant is required", details={"field": "tenant_id"})
        return self._registry.list_agents(tenant_id)

    def list_runnable(self, tenant_id: str) -> tuple[AgentSpec, ...]:
        """Agents in this tenant that may execute."""
        return tuple(spec for spec in self.list_agents(tenant_id) if spec.is_runnable)

    def list_by_kind(self, tenant_id: str, kind: AgentKind) -> tuple[AgentSpec, ...]:
        """Agents of one kind."""
        return self._registry.list_by_kind(tenant_id, kind)

    def assert_delegation_allowed(
        self,
        context: ExecutionContext,
        *,
        from_agent_id: str,
        to_agent_id: str,
    ) -> None:
        """Refuses a delegation the source agent is not permitted to make.

        Both agents must be in the caller's tenant, and the source must name
        the target in its allowlist. A supervisor that may delegate to anything
        is a capability-escalation path: it can reach an agent the caller was
        never granted.

        Raises:
            AgentNotFoundError: If either agent is missing.
            InvalidRequestError: If the delegation is not permitted.
        """
        source = self._registry.spec(context.tenant_id, from_agent_id)
        try:
            target = self._registry.spec(context.tenant_id, to_agent_id)
        except AgentNotFoundError as error:
            raise AgentVersionNotFoundError(
                "delegation target is not registered in this tenant",
                details={"from": from_agent_id, "to": to_agent_id},
            ) from error

        if source.kind is not AgentKind.SUPERVISOR:
            raise InvalidRequestError(
                "only a supervisor may delegate",
                details={"agent_id": from_agent_id, "kind": source.kind.value},
            )
        if to_agent_id not in source.allowed_tools:
            raise InvalidRequestError(
                "supervisor is not permitted to delegate to this agent",
                details={"from": from_agent_id, "to": to_agent_id},
            )
        if not target.is_runnable:
            raise InvalidRequestError(
                "delegation target is not runnable",
                details={"agent_id": to_agent_id, "status": target.status.value},
            )


def classify_status(status: AgentStatus) -> str:
    """Reports an agent status in a form safe for diagnostics."""
    return status.value
