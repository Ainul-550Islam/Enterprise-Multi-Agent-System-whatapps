"""Supervisor agent.

A supervisor does not answer the question itself. It decides which agent
should, delegates, and synthesises — which makes it the component most exposed
to a runaway: if a supervisor delegates to a supervisor that delegates back,
the system will happily recurse until something runs out.

The defence is structural rather than heuristic:

* depth is carried on every delegation and checked before the call,
* a supervisor's delegate list is an explicit allowlist, so a model cannot name
  an agent that was not granted to it,
* each delegate runs in its own child state, so one agent's untrusted content
  cannot reach another's context.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.agents.base import (
    AgentKind,
    AgentOutcome,
    AgentSpec,
    AgentState,
    Termination,
)
from mas_orchestrator.agents.delegation import (
    DelegationRequest,
    DelegationResult,
    check_delegation,
    new_delegation_id,
    state_for_delegate,
    summarise_for_caller,
    wrap_delegation_failure,
)
from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.agents.registry import AgentRegistry
    from mas_orchestrator.context import ExecutionContext

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

# How many delegates one supervisor turn may dispatch at once.
MAX_PARALLEL_DELEGATIONS: Final[int] = 8


@dataclass(frozen=True, slots=True)
class SupervisorOutcome:
    """What a supervisor produced.

    Attributes:
        agent_id: The supervisor.
        termination: Why it stopped.
        content: Synthesised answer.
        delegations: Every delegation it made, in order, including refusals.
        steps_taken: Model calls made by the supervisor itself.
        duration_ms: Wall-clock duration.
    """

    agent_id: str
    termination: Termination
    content: str = ""
    delegations: tuple[DelegationResult, ...] = ()
    steps_taken: int = 0
    duration_ms: int = 0

    @property
    def succeeded(self) -> bool:
        """Whether the supervisor reached a synthesised answer."""
        return self.termination is Termination.COMPLETED

    @property
    def refusal_count(self) -> int:
        """How many delegations were refused."""
        return sum(1 for item in self.delegations if item.refused)

    def to_agent_outcome(self) -> AgentOutcome:
        """Projects this into the common :class:`AgentOutcome` shape."""
        return AgentOutcome(
            agent_id=self.agent_id,
            termination=self.termination,
            content=self.content,
            steps_taken=self.steps_taken,
            tool_calls_made=len(self.delegations),
            duration_ms=self.duration_ms,
        )


class SupervisorAgent:
    """Delegates to named agents and synthesises their answers.

    The supervisor is given an explicit set of delegate ids. It does not
    discover agents by searching, and it does not accept a target suggested by
    the model unless that target is on the list.
    """

    def __init__(
        self,
        spec: AgentSpec,
        registry: AgentRegistry,
        *,
        delegates: Sequence[str] = (),
        max_depth: int | None = None,
        synthesiser: object = None,
    ) -> None:
        if spec.kind is not AgentKind.SUPERVISOR:
            raise ConfigurationError(
                "SupervisorAgent requires a supervisor-kind spec",
                details={"agent_id": spec.agent_id, "kind": spec.kind.value},
            )
        self._spec = spec
        self._registry = registry
        self._delegates = tuple(delegates)
        self._max_depth = max_depth or spec.limits.max_delegation_depth
        self._synthesiser = synthesiser

    @property
    def spec(self) -> AgentSpec:
        """The supervisor's definition."""
        return self._spec

    @property
    def delegates(self) -> tuple[str, ...]:
        """The agents this supervisor may delegate to."""
        return self._delegates

    async def run(
        self,
        state: AgentState,
        context: ExecutionContext,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> AgentOutcome:
        """Runs the supervisor and returns the common outcome shape.

        Use :meth:`supervise` when the individual delegation results are
        needed; this method satisfies the :class:`BaseAgent` protocol, which
        every caller in the runtime depends on.
        """
        return (await self.supervise(state, context, cancel_event=cancel_event)).to_agent_outcome()

    async def supervise(
        self,
        state: AgentState,
        context: ExecutionContext,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> SupervisorOutcome:
        """Runs the supervisor, keeping the per-delegation detail."""
        started = time.monotonic()

        if not self._spec.is_runnable:
            return SupervisorOutcome(
                agent_id=self._spec.agent_id,
                termination=Termination.FAILED,
                duration_ms=int((time.monotonic() - started) * 1000),
            )

        if not self._delegates:
            # A supervisor with no delegates cannot do anything useful. Reporting
            # that is better than silently answering the question itself, which
            # would quietly defeat the point of having a supervisor.
            return SupervisorOutcome(
                agent_id=self._spec.agent_id,
                termination=Termination.FAILED,
                content="",
                duration_ms=int((time.monotonic() - started) * 1000),
            )

        if len(self._delegates) > MAX_PARALLEL_DELEGATIONS:
            raise InvalidRequestError(
                "supervisor has more delegates than may be dispatched at once",
                details={"count": len(self._delegates), "limit": MAX_PARALLEL_DELEGATIONS},
            )

        task = self._task_from(state)
        depth = state.depth + 1

        tasks = [
            self._delegate(agent_id, task, context, depth, cancel_event)
            for agent_id in self._delegates
        ]
        results = tuple(await asyncio.gather(*tasks))

        content = await self._synthesise(task, results)
        duration_ms = int((time.monotonic() - started) * 1000)

        return SupervisorOutcome(
            agent_id=self._spec.agent_id,
            termination=Termination.COMPLETED,
            content=content,
            delegations=results,
            steps_taken=1 if self._synthesiser is not None else 0,
            duration_ms=duration_ms,
        )

    async def _delegate(
        self,
        agent_id: str,
        task: str,
        context: ExecutionContext,
        depth: int,
        cancel_event: asyncio.Event | None,
    ) -> DelegationResult:
        """Runs one delegation, refusing it if any precondition fails."""
        request = DelegationRequest(
            delegation_id=new_delegation_id(),
            from_agent_id=self._spec.agent_id,
            to_agent_id=agent_id,
            task=task,
            depth=depth,
        )

        target = (
            self._registry.spec(self._spec.tenant_id, agent_id)
            if self._registry.has(self._spec.tenant_id, agent_id)
            else None
        )

        refusal = check_delegation(target, depth, self._max_depth)
        if refusal is not None:
            return wrap_delegation_failure(request, refusal)

        if cancel_event is not None and cancel_event.is_set():
            from mas_orchestrator.contracts.errors import CancellationRequestedError

            raise CancellationRequestedError("cancelled before delegation")

        assert target is not None  # guaranteed by check_delegation
        agent = self._registry.build(self._spec.tenant_id, agent_id)
        child_state = state_for_delegate(task, None).with_metadata(
            delegation_id=request.delegation_id,
        )

        grandchild = AgentState(
            messages=child_state.messages,
            depth=depth,
            metadata=child_state.metadata,
        )

        try:
            outcome = await agent.run(grandchild, context, cancel_event=cancel_event)
        except Exception as error:
            _LOGGER.warning(
                "delegate failed",
                extra={
                    "event": "agent.delegate_failed",
                    "delegation_id": request.delegation_id,
                    "to": agent_id,
                    "error": type(error).__name__,
                },
            )
            outcome = AgentOutcome(
                agent_id=agent_id,
                termination=Termination.FAILED,
                error=error,
            )

        return DelegationResult(delegation_id=request.delegation_id, outcome=outcome)

    async def _synthesise(
        self,
        task: str,
        results: Sequence[DelegationResult],
    ) -> str:
        """Combines delegate answers into one response.

        With no language model wired for synthesis, the answers are returned
        verbatim and plainly labelled. Inventing a summary here would be a
        silent fallback: the caller would have no way to tell a model-generated
        synthesis from a concatenation.
        """
        if self._synthesiser is None:
            return "\n\n".join(summarise_for_caller(result) for result in results)

        raise ConfigurationError(
            "model-backed synthesis is not wired up",
            details={"agent_id": self._spec.agent_id},
        )

    @staticmethod
    def _task_from(state: AgentState) -> str:
        """Extracts the task text from the incoming state."""
        for message in reversed(state.messages):
            if getattr(message, "role", None) == "user":
                return str(getattr(message, "content", ""))
        return ""
