"""Node handlers.

A node is executed by a handler. The handler is where a workflow touches the
outside world — runs an agent, calls a tool — so it is also where a workflow can
do damage. Two constraints follow:

* A handler is **injected**, never resolved by name from configuration. A
  registry keyed on a string that can come from a workflow definition is a
  gadget chain waiting to be assembled.
* A handler returns a :class:`NodeResult` and does not raise to report a
  business failure. An exception means the handler itself broke, which the
  runtime treats differently from "this step did not succeed".

The agent handler is the interesting one: it receives the *whole* workflow
state, so an agent can be given the accumulated context of the run. What it
cannot be given is the tenant id — that comes from the context object, which
the handler does not construct.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Final, Protocol

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.workflows.models import NodeKind, WorkflowNode
from mas_orchestrator.workflows.state import NodeResult, NodeStatus, WorkflowState

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Invocation:
    """What a handler is asked to do.

    Attributes:
        node: The node being run.
        state: Current workflow state.
        input: Resolved input for this node.
    """

    node: WorkflowNode
    state: WorkflowState
    input: dict[str, Any]


class NodeHandler(Protocol):
    """Executes one kind of node."""

    @property
    def kind(self) -> NodeKind:
        """Which node kind this handler serves."""
        ...

    async def run(self, invocation: Invocation) -> NodeResult:
        """Runs the node and reports the outcome.

        Implementations must not raise for an expected failure: return a
        ``FAILED`` result instead.
        """
        ...


class StaticHandler:
    """A handler that returns a fixed value.

    Used for terminal nodes and for tests. It records what it was given rather
    than discarding it, so a workflow can pass data through a join point.
    """

    def __init__(self, output: dict[str, Any] | None = None) -> None:
        self._output = dict(output or {})

    @property
    def kind(self) -> NodeKind:
        """Serves terminal nodes."""
        return NodeKind.TERMINAL

    async def run(self, invocation: Invocation) -> NodeResult:
        """Returns the fixed output."""
        started = time.monotonic()
        return NodeResult(
            node_id=invocation.node.node_id,
            status=NodeStatus.COMPLETED,
            output=dict(self._output),
            duration_ms=int((time.monotonic() - started) * 1000),
        )


class AgentNodeHandler:
    """Runs an agent as a workflow step.

    The agent is injected as a callable, so this module never resolves an agent
    by name and a workflow definition cannot select one that was not registered.
    """

    def __init__(
        self,
        agent: Any,
        *,
        prompt_key: str = "prompt",
    ) -> None:
        self._agent = agent
        self._prompt_key = prompt_key

    @property
    def kind(self) -> NodeKind:
        """Serves agent nodes."""
        return NodeKind.AGENT

    async def run(self, invocation: Invocation) -> NodeResult:
        """Runs the agent.

        Returns:
            A completed or failed :class:`NodeResult`. A handler failure is
            reported as a failure rather than raised, so the runtime can decide
            whether the workflow continues.
        """
        started = time.monotonic()
        node = invocation.node
        prompt = invocation.input.get(self._prompt_key)
        if not isinstance(prompt, str) or not prompt.strip():
            return NodeResult(
                node_id=node.node_id,
                status=NodeStatus.FAILED,
                error="agent node received no prompt",
                duration_ms=int((time.monotonic() - started) * 1000),
            )

        try:
            content = await self._agent(prompt, invocation.state)
        except Exception as error:
            _LOGGER.warning(
                "agent node failed",
                extra={
                    "event": "workflow.agent_node_failed",
                    "node_id": node.node_id,
                    "error": type(error).__name__,
                },
            )
            return NodeResult(
                node_id=node.node_id,
                status=NodeStatus.FAILED,
                error=type(error).__name__,
                duration_ms=int((time.monotonic() - started) * 1000),
            )

        return NodeResult(
            node_id=node.node_id,
            status=NodeStatus.COMPLETED,
            output={"content": content if isinstance(content, str) else repr(content)},
            duration_ms=int((time.monotonic() - started) * 1000),
        )


class ToolNodeHandler:
    """Calls a tool as a workflow step.

    Like the agent handler, the tool executor is injected rather than looked up.
    """

    def __init__(self, executor: Any) -> None:
        self._executor = executor

    @property
    def kind(self) -> NodeKind:
        """Serves tool nodes."""
        return NodeKind.TOOL

    async def run(self, invocation: Invocation) -> NodeResult:
        """Calls the tool."""
        started = time.monotonic()
        node = invocation.node
        tool_id = invocation.input.get("tool_id")
        if not isinstance(tool_id, str) or not tool_id.strip():
            return NodeResult(
                node_id=node.node_id,
                status=NodeStatus.FAILED,
                error="tool node received no tool_id",
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        try:
            outcome = await self._executor(
                tool_id=tool_id,
                arguments=dict(invocation.input.get("arguments") or {}),
                state=invocation.state,
            )
        except Exception as error:
            return NodeResult(
                node_id=node.node_id,
                status=NodeStatus.FAILED,
                error=type(error).__name__,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        return NodeResult(
            node_id=node.node_id,
            status=NodeStatus.COMPLETED,
            output={"result": outcome},
            duration_ms=int((time.monotonic() - started) * 1000),
        )


def resolve_input(node: WorkflowNode, state: WorkflowState) -> dict[str, Any]:
    """Builds a node's input by substituting variable references.

    A string value of the form ``"${path}"`` is replaced by the variable at that
    dotted path. Anything else is passed through unchanged.

    The substitution is intentionally shallow: only whole-string references are
    replaced, so a workflow cannot interpolate a value into the middle of a
    prompt template and change the template's structure.
    """
    resolved: dict[str, Any] = {}
    for key, value in node.config.items():
        resolved[key] = _substitute(value, state)
    return resolved


def _substitute(value: Any, state: WorkflowState) -> Any:
    """Replaces whole-string variable references."""
    from mas_orchestrator.workflows.conditions import resolve_path

    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        path = value[2:-1].strip()
        if not path:
            raise InvalidRequestError(
                "empty variable reference", details={"node_id": state.current}
            )
        return resolve_path(state.variables, path)
    return value
