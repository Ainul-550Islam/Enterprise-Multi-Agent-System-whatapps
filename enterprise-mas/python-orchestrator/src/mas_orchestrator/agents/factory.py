"""Agent construction.

The factory is the only place that knows how the pieces fit together: an
:class:`AgentSpec`, a model router, a tool executor and a supervisor's delegate
list. Everything else receives a built agent.

Each factory returns a **callable that builds a fresh agent**, because agent
runs carry per-execution state. Storing built agents in a registry would share
a cost ledger and a cancellation event across executions that have nothing to
do with each other.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Final

from mas_orchestrator.agents.base import AgentKind, AgentSpec, BaseAgent
from mas_orchestrator.agents.executor import AgentExecutor, AgentRunConfig
from mas_orchestrator.agents.supervisor import SupervisorAgent
from mas_orchestrator.contracts.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from mas_orchestrator.agents.registry import AgentRegistry
    from mas_orchestrator.models.base import ModelProvider
    from mas_orchestrator.models.cost import CostLedger
    from mas_orchestrator.models.router import ModelRouter
    from mas_orchestrator.models.types import ModelRef
    from mas_orchestrator.prompts.registry import PromptRegistry
    from mas_orchestrator.tools.executor import ToolExecutor
    from mas_orchestrator.tools.registry import ToolRegistry

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class AgentFactory:
    """Builds agents from specs against a fixed set of collaborators."""

    def __init__(
        self,
        *,
        router: ModelRouter,
        provider_for: Callable[[ModelRef], ModelProvider],
        tool_executor: ToolExecutor | None = None,
        tool_registry: ToolRegistry | None = None,
        agent_registry: AgentRegistry | None = None,
        prompt_registry: PromptRegistry | None = None,
    ) -> None:
        self._router = router
        self._provider_for = provider_for
        self._tool_executor = tool_executor
        self._tool_registry = tool_registry
        self._agent_registry = agent_registry
        self._prompt_registry = prompt_registry

    def build(
        self,
        spec: AgentSpec,
        *,
        config: AgentRunConfig | None = None,
        delegates: Sequence[str] = (),
        ledger: CostLedger | None = None,
    ) -> BaseAgent:
        """Builds one agent instance.

        Args:
            spec: The agent definition.
            config: Model-side configuration for the run.
            delegates: For a supervisor, the agents it may delegate to.
            ledger: Cost ledger for this execution.

        Returns:
            A fresh agent.

        Raises:
            ConfigurationError: If the spec's kind needs a collaborator that
                was not supplied.
        """
        if spec.agent_id == "platform_assistant":
            from mas_orchestrator.agents.builtin.platform_assistant import PlatformAssistantAgent

            if spec.kind is not AgentKind.STANDARD or spec.allowed_tools or not spec.read_only:
                raise ConfigurationError(
                    "platform assistant must remain a read-only standard agent with no tools",
                    details={"agent_id": spec.agent_id},
                )
            executor = AgentExecutor(
                spec,
                router=self._router,
                provider_for=self._provider_for,
                tool_executor=None,
                tool_registry=None,
                ledger=ledger,
                config=config,
            )
            return PlatformAssistantAgent(spec, executor, prompts=self._prompt_registry)

        if spec.kind is AgentKind.SUPERVISOR:
            if self._agent_registry is None:
                raise ConfigurationError(
                    "supervisor agents require an agent registry",
                    details={"agent_id": spec.agent_id},
                )
            return SupervisorAgent(
                spec,
                self._agent_registry,
                delegates=delegates,
            )

        if spec.kind is AgentKind.TOOL_USER and self._tool_executor is None:
            # Refusing beats building a tool user that cannot call tools: the
            # model would be shown tools it can never invoke, and would then
            # answer from imagination while appearing to have checked.
            raise ConfigurationError(
                "tool-user agents require a tool executor",
                details={"agent_id": spec.agent_id},
            )

        return AgentExecutor(
            spec,
            router=self._router,
            provider_for=self._provider_for,
            tool_executor=self._tool_executor,
            tool_registry=self._tool_registry,
            ledger=ledger,
            config=config,
        )

    def factory_for(
        self,
        spec: AgentSpec,
        *,
        config: AgentRunConfig | None = None,
        delegates: Sequence[str] = (),
        ledger: CostLedger | None = None,
    ) -> Callable[[], BaseAgent]:
        """Returns a zero-argument callable that builds the agent.

        This is what gets registered, so that each execution receives a new
        instance.
        """

        def build() -> BaseAgent:
            return self.build(spec, config=config, delegates=delegates, ledger=ledger)

        return build


def build_agent_registry(
    factory: AgentFactory,
    specs: Sequence[AgentSpec],
    *,
    registry: AgentRegistry,
    delegates_by_agent: dict[str, Sequence[str]] | None = None,
) -> AgentRegistry:
    """Registers a collection of specs into a registry.

    Args:
        factory: Builds each agent.
        specs: The agent definitions.
        registry: Registry to populate.
        delegates_by_agent: Supervisor agent id -> delegate agent ids.

    Returns:
        The same registry, for chaining.

    Raises:
        ConfigurationError: If a supervisor names a delegate that is not among
            ``specs``. Refusing here means the misconfiguration is caught at
            startup rather than on the first delegation.
    """
    known = {spec.agent_id for spec in specs}
    delegates_by_agent = delegates_by_agent or {}

    for spec in specs:
        delegates = tuple(delegates_by_agent.get(spec.agent_id, ()))
        unknown = [item for item in delegates if item not in known]
        if unknown:
            raise ConfigurationError(
                "supervisor names a delegate that is not registered",
                details={"agent_id": spec.agent_id, "unknown": unknown},
            )
        registry.register(spec, factory.factory_for(spec, delegates=delegates))
        _LOGGER.info(
            "agent registered via factory",
            extra={
                "event": "agent.factory_registered",
                "agent_id": spec.agent_id,
                "kind": spec.kind.value,
            },
        )

    return registry
