"""The agent runtime: the composition root for an execution.

The runtime owns the things that must be shared across an execution — the
cancellation event, the cost ledger, the deadline — and hands each agent a
fresh instance built from them.

Two rules it enforces on every call, because they are the ones a caller can
accidentally break:

1. **Tenancy comes from the execution context, never from the arguments.** A
   caller that can name a tenant can read another tenant's agents.
2. **Every run is bounded.** An execution without a deadline still gets a
   ceiling, so a stuck agent cannot hold a worker forever.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.agents.base import (
    AgentKind,
    AgentLimits,
    AgentOutcome,
    AgentSpec,
    AgentState,
    Termination,
)
from mas_orchestrator.agents.router import AgentRoutingRequest
from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    ConfigurationError,
    ExecutionTimeoutError,
)
from mas_orchestrator.models.cost import CostLedger

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.agents.factory import AgentFactory
    from mas_orchestrator.agents.registry import AgentRegistry
    from mas_orchestrator.agents.router import AgentRouter
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.models.cost import PricingTable

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

# Applied when an execution arrives without a deadline. Never removing the
# bound entirely is what stops a hung agent from becoming a leaked worker.
DEFAULT_EXECUTION_TIMEOUT_SECONDS: Final[float] = 900.0


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    """Runtime-wide settings.

    Attributes:
        default_timeout_seconds: Ceiling applied when an execution has no
            deadline.
        max_parallel_agents: How many agents may run concurrently within one
            execution.
        cost_ceiling: Optional spend ceiling for the whole execution.
    """

    default_timeout_seconds: float = DEFAULT_EXECUTION_TIMEOUT_SECONDS
    max_parallel_agents: int = 4
    cost_ceiling: float | None = None

    def __post_init__(self) -> None:
        if self.default_timeout_seconds <= 0:
            raise ConfigurationError(
                "runtime timeout must be positive",
                details={"field": "default_timeout_seconds"},
            )
        if self.max_parallel_agents < 1:
            raise ConfigurationError(
                "max_parallel_agents must be at least 1",
                details={"field": "max_parallel_agents"},
            )
        if self.cost_ceiling is not None and self.cost_ceiling <= 0:
            raise ConfigurationError(
                "cost ceiling must be positive", details={"field": "cost_ceiling"}
            )


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """The outcome of a whole execution.

    Attributes:
        execution_id: Execution identifier.
        agent_id: Agent that produced the answer.
        termination: Why the run stopped.
        content: The final answer.
        cost: Cost totals for the execution.
        duration_ms: Wall-clock duration.
        error: The error that stopped it, when it failed.
    """

    execution_id: str
    agent_id: str
    termination: Termination
    content: str = ""
    cost: Any = None
    duration_ms: int = 0
    error: BaseException | None = None

    @property
    def succeeded(self) -> bool:
        """Whether the execution produced a final answer."""
        return self.termination is Termination.COMPLETED


class AgentRuntime:
    """Runs agents for executions."""

    def __init__(
        self,
        *,
        registry: AgentRegistry,
        router: AgentRouter,
        factory: AgentFactory,
        pricing: PricingTable | None = None,
        config: RuntimeConfig | None = None,
    ) -> None:
        self._registry = registry
        self._router = router
        self._factory = factory
        self._pricing = pricing
        self._config = config or RuntimeConfig()

    @property
    def config(self) -> RuntimeConfig:
        """The runtime configuration in force."""
        return self._config

    async def execute(
        self,
        request: str,
        context: ExecutionContext,
        *,
        routing: AgentRoutingRequest | None = None,
        state: AgentState | None = None,
        cancel_event: asyncio.Event | None = None,
        agent_id: str | None = None,
    ) -> ExecutionResult:
        """Runs one execution end to end.

        Args:
            request: The user's request.
            context: Authoritative execution context. The tenant and execution
                id are taken from here and nowhere else.
            routing: Optional agent selection constraints.
            state: Optional starting state.
            cancel_event: Optional cancellation signal.
            agent_id: Run a specific agent instead of selecting one. The
                agent must still satisfy the routing policy.

        Returns:
            An :class:`ExecutionResult`.

        Raises:
            AgentNotFoundError: If no agent is eligible. Raised rather than
                returned, because there is no agent outcome to report.
        """
        started = time.monotonic()
        ledger = CostLedger(self._pricing) if self._pricing is not None else None

        spec = self._select_agent(context.tenant_id, routing, agent_id)
        agent = self._factory.build(spec, ledger=ledger)

        initial = state or _seed(spec, request)
        cancel_event = cancel_event or asyncio.Event()

        try:
            outcome = await self._run_bounded(agent, initial, context, cancel_event)
        except asyncio.CancelledError as error:
            outcome = AgentOutcome(
                agent_id=spec.agent_id,
                termination=Termination.CANCELLED,
                error=error,
                duration_ms=int((time.monotonic() - started) * 1000),
            )

        duration_ms = int((time.monotonic() - started) * 1000)
        _LOGGER.info(
            "execution finished",
            extra={
                "event": "execution.finished",
                "execution_id": context.execution_id,
                "agent_id": spec.agent_id,
                "termination": outcome.termination.value,
                "duration_ms": duration_ms,
            },
        )

        return ExecutionResult(
            execution_id=context.execution_id,
            agent_id=spec.agent_id,
            termination=outcome.termination,
            content=outcome.content,
            cost=ledger.totals() if ledger is not None else None,
            duration_ms=duration_ms,
            error=outcome.error,
        )

    def _select_agent(
        self,
        tenant_id: str,
        routing: AgentRoutingRequest | None,
        agent_id: str | None,
    ) -> AgentSpec:
        """Selects the agent, honouring both the policy and an explicit choice."""
        if agent_id is not None:
            spec = self._registry.spec(tenant_id, agent_id)
            if self._router.evaluate(spec, routing or AgentRoutingRequest()) is not None:
                # An explicit choice is not an exemption from policy.
                raise AgentNotFoundError(
                    "the requested agent does not satisfy the routing policy",
                    details={"agent_id": agent_id, "tenant_id": tenant_id},
                )
            return spec
        return self._router.select(tenant_id, routing)

    async def _run_bounded(
        self,
        agent: Any,
        state: AgentState,
        context: ExecutionContext,
        cancel_event: asyncio.Event,
    ) -> AgentOutcome:
        """Runs an agent under the runtime's own deadline.

        The runtime's ceiling is a backstop for the agent's internal bounds. It
        exists because an agent's own limits are configured per agent, and a
        configuration mistake should surface as a timeout rather than as a
        worker that never returns.
        """
        remaining = context.remaining_seconds()
        timeout = (
            min(remaining, self._config.default_timeout_seconds)
            if remaining is not None
            else self._config.default_timeout_seconds
        )

        try:
            return await asyncio.wait_for(
                agent.run(state, context, cancel_event=cancel_event),
                timeout=timeout,
            )
        except TimeoutError as error:
            raise ExecutionTimeoutError(
                "execution exceeded the runtime deadline",
                details={"timeout_seconds": timeout},
            ) from error

    async def execute_parallel(
        self,
        request: str,
        context: ExecutionContext,
        agent_ids: Sequence[str],
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> list[ExecutionResult]:
        """Runs several agents on the same request concurrently.

        Used for comparison or ensemble work. Results arrive in the order of
        ``agent_ids`` regardless of which finished first, so a caller can
        correlate a result with the agent that produced it.
        """
        if len(agent_ids) > self._config.max_parallel_agents:
            raise ConfigurationError(
                "too many agents requested for one execution",
                details={
                    "requested": len(agent_ids),
                    "limit": self._config.max_parallel_agents,
                },
            )

        cancel_event = cancel_event or asyncio.Event()
        tasks = [
            self.execute(request, context, agent_id=agent_id, cancel_event=cancel_event)
            for agent_id in agent_ids
        ]
        return list(await asyncio.gather(*tasks, return_exceptions=False))


def _seed(spec: AgentSpec, request: str) -> AgentState:
    """Builds the starting state for an agent run."""
    from mas_orchestrator.agents.base import new_state

    return new_state(spec.system_prompt, request)


def build_default_limits(kind: AgentKind) -> AgentLimits:
    """Sensible per-kind limits.

    A standard agent needs no tools; a tool user gets a step budget large
    enough to actually use them; a supervisor delegates rather than iterating,
    so it needs few steps but a delegation allowance.
    """
    if kind is AgentKind.STANDARD:
        return AgentLimits(max_steps=4, max_tool_calls=0)
    if kind is AgentKind.SUPERVISOR:
        return AgentLimits(max_steps=8, max_tool_calls=0, max_delegation_depth=3)
    return AgentLimits(max_steps=25, max_tool_calls=50)
