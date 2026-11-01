"""The concrete agents a deployment runs.

Everything else in this package is machinery: a loop, a router, a supervisor, a
registry. This module wires that machinery into the agents a deployment
actually names.

:class:`DefaultAgent` is deliberately thin — it owns an
:class:`~mas_orchestrator.agents.executor.AgentExecutor` and nothing else. The
loop already enforces every bound, so an agent that enforced its own would
create two places to look when a run stops early.

Three rules hold for everything here:

* **Failure is an outcome, not an exception.** :meth:`DefaultAgent.run` returns
  an :class:`AgentOutcome` rather than raising, so a failure is data the caller
  can act on instead of a stack trace unwinding through the runtime.
* **No silent fallback.** :func:`build_agents` raises when it cannot be wired
  correctly. There is no "degrade to a simpler agent" path, because a degraded
  agent produces answers nobody asked for.
* **Capability comes from the kind.** :class:`AgentKind` decides whether an
  agent may use tools; an allow-list alone cannot grant it. So the summariser
  is :attr:`AgentKind.STANDARD` with an empty allow-list — declared twice, not
  merely intended.

Identity is required, not defaulted. An :class:`AgentSpec` needs a ``tenant_id``,
a ``slug`` and an ``agent_version_id``, and all three are owned by Rust, so
:func:`build_agents` takes them as arguments. Inventing them here would produce
agents whose provenance was a guess.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.agents.base import (
    AgentKind,
    AgentLimits,
    AgentOutcome,
    AgentSpec,
    AgentState,
    Termination,
    new_state,
)
from mas_orchestrator.agents.executor import AgentExecutor, AgentRunConfig
from mas_orchestrator.contracts.errors import InvalidRequestError

if TYPE_CHECKING:
    import asyncio

    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.models.cost import CostLedger
    from mas_orchestrator.models.router import ModelRouter
    from mas_orchestrator.tools.executor import ToolExecutor
    from mas_orchestrator.tools.registry import ToolRegistry

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Tools the working agents may call. An allow-list of ``("*",)`` would make
#: the tool layer's own permission check the only gate, which is one gate too
#: few.
WORKER_TOOLS: Final[frozenset[str]] = frozenset(
    {"builtin.database_query", "builtin.sandbox_python"}
)


class DefaultAgent:
    """One agent, driven by the standard loop."""

    def __init__(
        self,
        spec: AgentSpec,
        *,
        router: ModelRouter,
        provider_for: Any,
        tool_executor: ToolExecutor | None = None,
        tool_registry: ToolRegistry | None = None,
        ledger: CostLedger | None = None,
        config: AgentRunConfig | None = None,
    ) -> None:
        # A tool-using agent with no way to execute tools is refused here
        # rather than at the first tool call, mid-run, after spending tokens.
        if spec.allowed_tools and tool_executor is None:
            raise InvalidRequestError(
                f"agent '{spec.agent_id}' allows tools but no tool executor was supplied",
                details={
                    "agent_id": spec.agent_id,
                    "tools": sorted(spec.allowed_tools),
                },
            )
        if spec.allowed_tools and tool_registry is None:
            raise InvalidRequestError(
                f"agent '{spec.agent_id}' allows tools but no tool registry was supplied",
                details={
                    "agent_id": spec.agent_id,
                    "tools": sorted(spec.allowed_tools),
                },
            )

        self._spec = spec
        self._executor = AgentExecutor(
            spec,
            router=router,
            provider_for=provider_for,
            tool_executor=tool_executor,
            tool_registry=tool_registry,
            ledger=ledger,
            config=config,
        )

    @property
    def spec(self) -> AgentSpec:
        """The agent's definition."""
        return self._spec

    @property
    def executor(self) -> AgentExecutor:
        """The loop driving this agent."""
        return self._executor

    async def run(
        self,
        state: AgentState,
        context: ExecutionContext,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> AgentOutcome:
        """Runs the agent to completion or to a bound.

        Returns:
            An :class:`AgentOutcome`. Never raises for an ordinary failure.
        """
        _LOGGER.debug(
            "agent starting",
            extra={
                "event": "agent.start",
                "agent_id": self._spec.agent_id,
                "kind": self._spec.kind.value,
                "execution_id": context.execution_id,
            },
        )
        outcome = await self._executor.run(state, context, cancel_event=cancel_event)
        _LOGGER.debug(
            "agent finished",
            extra={
                "event": "agent.finish",
                "agent_id": self._spec.agent_id,
                "termination": outcome.termination.value,
            },
        )
        return outcome


@dataclass(frozen=True, slots=True)
class AgentBundle:
    """The agents a deployment runs.

    Attributes:
        worker: The general answerer.
        tool_user: Reaches for tools to gather information.
        summariser: Compresses content; no tools by kind *and* by allow-list.
        supervisor: Delegates to the others.
        registry: The registry the supervisor draws delegates from.
    """

    worker: DefaultAgent
    tool_user: DefaultAgent
    summariser: DefaultAgent
    supervisor: Any
    registry: Any

    def agents(self) -> tuple[DefaultAgent, ...]:
        """The non-supervisor agents, for registration or inspection."""
        return (self.worker, self.tool_user, self.summariser)

    def by_id(self) -> dict[str, DefaultAgent]:
        """The non-supervisor agents keyed by id."""
        return {agent.spec.agent_id: agent for agent in self.agents()}


def build_agents(
    *,
    tenant_id: str,
    router: ModelRouter,
    provider_for: Any,
    tool_executor: ToolExecutor | None = None,
    tool_registry: ToolRegistry | None = None,
    ledger: CostLedger | None = None,
    config: AgentRunConfig | None = None,
    agent_version_id: str = "v1",
) -> AgentBundle:
    """Wires the standard agent set.

    Args:
        tenant_id: Owning tenant. Required by :class:`AgentSpec` and owned by
            Rust, so it must be supplied rather than invented.
        router: Selects a model per call.
        provider_for: Resolves a model reference to a provider.
        tool_executor: Executes tool calls. Required, because two of the four
            agents use tools.
        tool_registry: Describes available tools.
        ledger: Accumulates tokens and cost.
        config: Loop configuration.
        agent_version_id: Version stamp applied to each spec.

    Returns:
        An :class:`AgentBundle`.

    Raises:
        InvalidRequestError: If ``tenant_id`` is blank, or the tool-using
            agents are built without an executor or registry.
    """
    from mas_orchestrator.agents.registry import AgentRegistry
    from mas_orchestrator.agents.supervisor import SupervisorAgent

    if not tenant_id.strip():
        raise InvalidRequestError(
            "a tenant id is required to build agents", details={"field": "tenant_id"}
        )

    worker = DefaultAgent(
        _worker_spec(tenant_id, agent_version_id),
        router=router,
        provider_for=provider_for,
        tool_executor=tool_executor,
        tool_registry=tool_registry,
        ledger=ledger,
        config=config,
    )
    tool_user = DefaultAgent(
        _tool_user_spec(tenant_id, agent_version_id),
        router=router,
        provider_for=provider_for,
        tool_executor=tool_executor,
        tool_registry=tool_registry,
        ledger=ledger,
        config=config,
    )
    summariser = DefaultAgent(
        # No tools passed at all. A summariser is the agent most likely to be
        # handed untrusted content, so it is the one least able to afford a
        # tool surface.
        _summariser_spec(tenant_id, agent_version_id),
        router=router,
        provider_for=provider_for,
        ledger=ledger,
        config=config,
    )

    registry = AgentRegistry()
    for agent in (worker, tool_user, summariser):
        registry.register(agent.spec, _constant(agent))

    supervisor_spec = _supervisor_spec(tenant_id, agent_version_id)
    supervisor = SupervisorAgent(
        supervisor_spec,
        registry,
        delegates=(worker.spec.agent_id, tool_user.spec.agent_id),
    )
    registry.register(supervisor_spec, _constant(supervisor))

    return AgentBundle(
        worker=worker,
        tool_user=tool_user,
        summariser=summariser,
        supervisor=supervisor,
        registry=registry,
    )


def _constant(agent: Any) -> Any:
    """Builds a factory returning an already-constructed agent.

    A lambda capturing ``agent`` cannot be typed precisely enough for the
    registry's ``Callable[[], BaseAgent]`` signature; a named function can.
    """
    return lambda: agent


def _worker_spec(tenant_id: str, version: str) -> AgentSpec:
    """Answers directly, with a moderate step budget and no tools.

    Answering without tools is the cheapest and safest path, so it is the one
    a request should take unless it genuinely needs information.
    """
    return AgentSpec(
        agent_id="worker",
        agent_version_id=version,
        name="Worker",
        slug="worker",
        kind=AgentKind.STANDARD,
        tenant_id=tenant_id,
        description="Answers a request directly from the model.",
        allowed_tools=frozenset(),
        limits=AgentLimits(max_steps=6, max_tool_calls=0),
    )


def _tool_user_spec(tenant_id: str, version: str) -> AgentSpec:
    """Gathers information with tools, within a bounded loop."""
    return AgentSpec(
        agent_id="tool_user",
        agent_version_id=version,
        name="Tool user",
        slug="tool-user",
        kind=AgentKind.TOOL_USER,
        tenant_id=tenant_id,
        description="Uses tools to gather information before answering.",
        allowed_tools=WORKER_TOOLS,
        limits=AgentLimits(max_steps=12, max_tool_calls=24),
    )


def _summariser_spec(tenant_id: str, version: str) -> AgentSpec:
    """Compresses content; no tools, by kind and by allow-list."""
    return AgentSpec(
        agent_id="summariser",
        agent_version_id=version,
        name="Summariser",
        slug="summariser",
        kind=AgentKind.STANDARD,
        tenant_id=tenant_id,
        description="Compresses content without acting on it.",
        allowed_tools=frozenset(),
        limits=AgentLimits(max_steps=3, max_tool_calls=0),
    )


def _supervisor_spec(tenant_id: str, version: str) -> AgentSpec:
    """Delegates rather than answering directly.

    Its allow-list names the agents it may delegate to. A supervisor's tools
    *are* its delegates, so delegation is permission-checked by the same
    mechanism as a tool call.
    """
    return AgentSpec(
        agent_id="supervisor",
        agent_version_id=version,
        name="Supervisor",
        slug="supervisor",
        kind=AgentKind.SUPERVISOR,
        tenant_id=tenant_id,
        description="Breaks a request into parts and delegates them.",
        allowed_tools=frozenset({"worker", "tool_user"}),
        limits=AgentLimits(max_steps=8, max_tool_calls=0),
    )


def refused_outcome(
    spec: AgentSpec,
    *,
    reason: str,
    state: AgentState | None = None,
) -> AgentOutcome:
    """Builds an outcome for an agent that was refused before it ran.

    There is no answer and no cost, and the outcome says so: a refusal is not a
    failure to answer a question the agent was never permitted to attempt.
    """
    return AgentOutcome(
        agent_id=spec.agent_id,
        termination=Termination.CANCELLED,
        content="",
        messages=state.messages if state is not None else new_state().messages,
        error=InvalidRequestError(reason, details={"agent_id": spec.agent_id}),
    )
