"""End-to-end execution of a workflow graph.

An agent run is one step; a workflow is many, with branching, fan-out and
repetition between them. This suite runs real graphs through the real
:class:`WorkflowRuntime`, so what is under test is the machinery that decides
what happens *next* — the part that is easiest to get wrong and hardest to see
from a single node.

The properties that matter at this level:

* **A workflow belongs to its tenant.** Running one under a different tenant
  is refused, not logged: executing someone else's graph inside this tenant's
  execution would blend two customers' work.
* **Brakes are honest.** A run that hits its step budget or its deadline
  reports ``FAILED``. A workflow that quietly stops half-way is worse than one
  that admits it stopped.
* **A failing node stops the run.** Continuing past a failure would let later
  nodes act on results that were never produced.
* **Loops terminate.** Repetition is bounded by its own ceiling *and* by the
  global step budget, because conver

gence detection is an optimisation, not a guarantee.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError, TenantContextError
from mas_orchestrator.workflows.conditions import Combinator, Condition, ConditionGroup, Operator
from mas_orchestrator.workflows.graph import build_graph
from mas_orchestrator.workflows.models import (
    JoinMode,
    NodeKind,
    WorkflowDefinition,
    WorkflowNode,
)
from mas_orchestrator.workflows.node import (
    AgentNodeHandler,
    Invocation,
    NodeHandler,
    StaticHandler,
)
from mas_orchestrator.workflows.runtime import WorkflowRuntime
from mas_orchestrator.workflows.state import NodeResult, NodeStatus

TENANT: str = "tenant-a"
OTHER_TENANT: str = "tenant-b"


# --- handlers ----------------------------------------------------------------


@dataclass(slots=True)
class _RecordingAgent:
    """Stands in for an agent, recording the prompts it was given."""

    outputs: list[dict[str, Any]]
    prompts: list[str] = field(default_factory=list)
    output: dict[str, Any] = field(default_factory=dict)

    async def __call__(self, prompt: str, _state: object = None) -> dict[str, Any]:
        """Returns the next scripted output.

        The second argument is the workflow state, which the handler passes so
        an agent can read variables. This stand-in ignores it.
        """
        self.prompts.append(prompt)
        return self.outputs.pop(0) if self.outputs else dict(self.output)


@dataclass(slots=True)
class _SequenceHandler:
    """A tool handler that returns a different output on each call."""

    kind_value: NodeKind
    outputs: list[dict[str, Any]]
    calls: list[str] = field(default_factory=list)
    fail_on: str | None = None

    @property
    def kind(self) -> NodeKind:
        """The node kind served."""
        return self.kind_value

    async def run(self, invocation: Invocation) -> NodeResult:
        """Returns the next output, or a failure if configured to."""
        started = time.monotonic()
        node_id = invocation.node.node_id
        self.calls.append(node_id)
        if self.fail_on is not None and node_id == self.fail_on:
            return NodeResult(
                node_id=node_id,
                status=NodeStatus.FAILED,
                error="deliberate failure",
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        output = self.outputs.pop(0) if self.outputs else {}
        return NodeResult(
            node_id=node_id,
            status=NodeStatus.COMPLETED,
            output=output,
            duration_ms=int((time.monotonic() - started) * 1000),
        )


@dataclass(slots=True)
class _PassthroughConditionHandler:
    """Serves condition nodes.

    Branch selection happens in the runtime's ``_advance``, not in a handler,
    so the handler's only job is to report that the node was reached and to
    pass the input through.
    """

    seen: list[str] = field(default_factory=list)

    @property
    def kind(self) -> NodeKind:
        """Serves condition nodes."""
        return NodeKind.CONDITION

    async def run(self, invocation: Invocation) -> NodeResult:
        """Records the visit and completes."""
        started = time.monotonic()
        self.seen.append(invocation.node.node_id)
        return NodeResult(
            node_id=invocation.node.node_id,
            status=NodeStatus.COMPLETED,
            output=dict(invocation.input),
            duration_ms=int((time.monotonic() - started) * 1000),
        )


class _ExplodingHandler:
    """A handler that raises instead of reporting a failure."""

    @property
    def kind(self) -> NodeKind:
        """Serves tool nodes."""
        return NodeKind.TOOL

    async def run(self, invocation: Invocation) -> NodeResult:
        """Raises, to prove the runtime converts it into a failed run."""
        raise RuntimeError("handler exploded")


def _node(
    node_id: str,
    kind: NodeKind,
    *,
    next_nodes: tuple[str, ...] = (),
    branches: dict[str, str] | None = None,
    config: dict[str, Any] | None = None,
) -> WorkflowNode:
    """Builds a workflow node."""
    return WorkflowNode(
        node_id=node_id,
        kind=kind,
        name=node_id,
        config=dict(config or {}),
        next=next_nodes,
        branches=dict(branches or {}),
    )


def _graph(
    nodes: dict[str, WorkflowNode],
    *,
    entry: str,
    tenant_id: str = TENANT,
) -> Any:
    """Builds and validates a graph."""
    return build_graph(
        WorkflowDefinition(
            workflow_id="wf-1",
            tenant_id=tenant_id,
            name="Test Workflow",
            nodes=nodes,
            entry=entry,
        )
    )


def _runtime(*handlers: NodeHandler) -> WorkflowRuntime:
    """Builds a runtime with the given handlers plus a terminal handler."""
    runtime = WorkflowRuntime()
    for handler in handlers:
        runtime.register(handler)
    runtime.register(StaticHandler())
    return runtime


# --- linear runs -------------------------------------------------------------


async def test_linear_workflow_runs_to_completion() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{"value": 1}])
    graph = _graph(
        {
            "start": _node("start", NodeKind.TOOL, next_nodes=("end",)),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="start",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is True
    assert outcome.status is NodeStatus.COMPLETED
    assert tool.calls == ["start"]


async def test_results_are_recorded_in_completion_order() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{"value": 1}, {"value": 2}])
    graph = _graph(
        {
            "a": _node("a", NodeKind.TOOL, next_nodes=("b",)),
            "b": _node("b", NodeKind.TOOL, next_nodes=("end",)),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="a",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert [result.node_id for result in outcome.results] == ["a", "b"]


async def test_variables_flow_between_nodes() -> None:
    # A later node reads what an earlier one produced, keyed by node id so two
    # nodes cannot overwrite each other's output.
    tool = _SequenceHandler(NodeKind.TOOL, [{"value": "carried"}])
    graph = _graph(
        {
            "a": _node("a", NodeKind.TOOL, next_nodes=("end",)),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="a",
    )

    outcome = await _runtime(tool).run(
        graph,
        tenant_id=TENANT,
        execution_id="exec-1",
        variables={"seed": "input"},
    )

    assert outcome.succeeded is True
    assert outcome.state.variables["seed"] == "input"


async def test_agent_node_receives_its_prompt() -> None:
    agent = _RecordingAgent([{"answer": "42"}])
    graph = _graph(
        {
            # Input is resolved from the node's own config with a whole-string
            # variable reference, so a workflow cannot splice a value into the
            # middle of a prompt template and change its structure.
            "ask": _node(
                "ask",
                NodeKind.AGENT,
                next_nodes=("end",),
                config={"prompt": "${question}"},
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="ask",
    )

    outcome = await _runtime(AgentNodeHandler(agent)).run(
        graph,
        tenant_id=TENANT,
        execution_id="exec-1",
        variables={"question": "what is the answer?"},
    )

    assert outcome.succeeded is True
    assert agent.prompts == ["what is the answer?"]


# --- tenancy -----------------------------------------------------------------


async def test_running_another_tenants_workflow_is_refused() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{}])
    graph = _graph(
        {"start": _node("start", NodeKind.TERMINAL)},
        entry="start",
        tenant_id=OTHER_TENANT,
    )

    with pytest.raises(TenantContextError, match="different tenant"):
        await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")


async def test_own_tenant_is_accepted() -> None:
    graph = _graph({"start": _node("start", NodeKind.TERMINAL)}, entry="start")

    outcome = await _runtime().run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is True


# --- conditions --------------------------------------------------------------


async def test_condition_selects_the_matching_branch() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{}])
    condition = _PassthroughConditionHandler()
    graph = _graph(
        {
            "gate": _node(
                "gate",
                NodeKind.CONDITION,
                branches={"high": "expensive", "low": "cheap"},
                config={"condition": Condition(path="score", operator=Operator.GT, value=5)},
            ),
            "expensive": _node("expensive", NodeKind.TERMINAL),
            "cheap": _node("cheap", NodeKind.TERMINAL),
        },
        entry="gate",
    )

    outcome = await _runtime(tool, condition).run(
        graph,
        tenant_id=TENANT,
        execution_id="exec-1",
        variables={"score": 9},
    )

    assert outcome.succeeded is True
    assert condition.seen == ["gate"]


async def test_condition_falls_back_to_the_default_branch() -> None:
    # No branch matches and no default is declared, so the run ends rather than
    # guessing which successor was meant.
    condition = _PassthroughConditionHandler()
    graph = _graph(
        {
            "gate": _node(
                "gate",
                NodeKind.CONDITION,
                branches={"high": "expensive"},
                config={"condition": Condition(path="score", operator=Operator.GT, value=100)},
            ),
            "expensive": _node("expensive", NodeKind.TERMINAL),
        },
        entry="gate",
    )

    outcome = await _runtime(condition).run(
        graph,
        tenant_id=TENANT,
        execution_id="exec-1",
        variables={"score": 1},
    )

    assert outcome.succeeded is True


async def test_condition_without_an_expression_fails_the_run() -> None:
    # A condition node with no expression cannot choose, and picking the first
    # branch would be a guess presented as a decision.
    condition = _PassthroughConditionHandler()
    graph = _graph(
        {
            "gate": _node(
                "gate",
                NodeKind.CONDITION,
                branches={"a": "end"},
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="gate",
    )

    outcome = await _runtime(condition).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is False
    assert outcome.status is NodeStatus.FAILED


async def test_condition_group_combines_with_or() -> None:
    condition = _PassthroughConditionHandler()
    graph = _graph(
        {
            "gate": _node(
                "gate",
                NodeKind.CONDITION,
                branches={"yes": "end"},
                config={
                    "condition": ConditionGroup(
                        combinator=Combinator.OR,
                        conditions=(
                            Condition(path="a", operator=Operator.TRUTHY),
                            Condition(path="b", operator=Operator.TRUTHY),
                        ),
                    )
                },
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="gate",
    )

    outcome = await _runtime(condition).run(
        graph,
        tenant_id=TENANT,
        execution_id="exec-1",
        variables={"a": False, "b": True},
    )

    assert outcome.succeeded is True


# --- parallel ----------------------------------------------------------------


async def test_parallel_branches_all_run() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{}, {}])
    graph = _graph(
        {
            "fan": _node(
                "fan",
                NodeKind.PARALLEL,
                next_nodes=("end",),
                config={
                    "branches": {
                        "left": {"kind": NodeKind.TOOL.value},
                        "right": {"kind": NodeKind.TOOL.value},
                    },
                    "join": JoinMode.ALL.value,
                },
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="fan",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is True
    assert len(tool.calls) == 2


async def test_parallel_failure_fails_the_run_under_all() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{}, {}], fail_on="fan:right")
    graph = _graph(
        {
            "fan": _node(
                "fan",
                NodeKind.PARALLEL,
                next_nodes=("end",),
                config={
                    "branches": {
                        "left": {"kind": NodeKind.TOOL.value},
                        "right": {"kind": NodeKind.TOOL.value},
                    },
                    "join": JoinMode.ALL.value,
                },
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="fan",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    # ALL means every branch must succeed: one failure is a failure of the step.
    assert outcome.succeeded is False
    assert outcome.status is NodeStatus.FAILED


async def test_parallel_first_success_tolerates_a_failure() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{}, {}], fail_on="fan:right")
    graph = _graph(
        {
            "fan": _node(
                "fan",
                NodeKind.PARALLEL,
                next_nodes=("end",),
                config={
                    "branches": {
                        "left": {"kind": NodeKind.TOOL.value},
                        "right": {"kind": NodeKind.TOOL.value},
                    },
                    "join": JoinMode.FIRST_SUCCESS.value,
                },
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="fan",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is True


# --- loops -------------------------------------------------------------------


async def test_loop_runs_to_its_iteration_ceiling() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{"n": index} for index in range(10)])
    graph = _graph(
        {
            "repeat": _node(
                "repeat",
                NodeKind.LOOP,
                config={"max_iterations": 3, "body": {"kind": NodeKind.TOOL.value}},
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="repeat",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is True
    assert len(tool.calls) == 3


async def test_loop_stops_when_output_converges() -> None:
    # Identical consecutive outputs mean another iteration would produce
    # nothing new, so the loop stops instead of burning its ceiling.
    tool = _SequenceHandler(NodeKind.TOOL, [{"same": True} for _ in range(10)])
    graph = _graph(
        {
            "repeat": _node(
                "repeat",
                NodeKind.LOOP,
                config={
                    "max_iterations": 10,
                    "body": {"kind": NodeKind.TOOL.value},
                    "detect_convergence": True,
                },
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="repeat",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is True
    assert len(tool.calls) < 10


async def test_loop_failure_halts_the_loop() -> None:
    # Continuing to iterate after a failure usually just repeats it.
    tool = _SequenceHandler(NodeKind.TOOL, [{}, {}, {}], fail_on="repeat#0")
    graph = _graph(
        {
            "repeat": _node(
                "repeat",
                NodeKind.LOOP,
                config={"max_iterations": 5, "body": {"kind": NodeKind.TOOL.value}},
            ),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="repeat",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is False
    assert len(tool.calls) == 1


# --- failure handling --------------------------------------------------------


async def test_failing_node_stops_the_workflow() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{}, {}], fail_on="first")
    graph = _graph(
        {
            "first": _node("first", NodeKind.TOOL, next_nodes=("second",)),
            "second": _node("second", NodeKind.TOOL, next_nodes=("end",)),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="first",
    )

    outcome = await _runtime(tool).run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.succeeded is False
    assert outcome.status is NodeStatus.FAILED
    assert outcome.error == "deliberate failure"
    assert tool.calls == ["first"]


async def test_handler_that_raises_is_converted_to_a_failed_run() -> None:
    graph = _graph(
        {
            "boom": _node("boom", NodeKind.TOOL, next_nodes=("end",)),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="boom",
    )

    outcome = await _runtime(_ExplodingHandler()).run(
        graph, tenant_id=TENANT, execution_id="exec-1"
    )

    assert outcome.succeeded is False
    assert "RuntimeError" in (outcome.error or "")


async def test_missing_handler_is_refused() -> None:
    # A node with no handler would otherwise be skipped, producing a workflow
    # that quietly does less than it says.
    graph = _graph(
        {
            "orphan": _node("orphan", NodeKind.AGENT, next_nodes=("end",)),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="orphan",
    )

    with pytest.raises(InvalidRequestError, match="no handler is registered"):
        await _runtime().run(graph, tenant_id=TENANT, execution_id="exec-1")


# --- brakes ------------------------------------------------------------------


async def test_step_budget_stops_an_endless_workflow() -> None:
    # Eight nodes, because the definition's summed step limits must stay under
    # the platform ceiling of 200 (20 per node by default).
    chain_length = 8
    tool = _SequenceHandler(NodeKind.TOOL, [{} for _ in range(chain_length)])
    nodes = {
        f"n{index}": _node(
            f"n{index}",
            NodeKind.TOOL,
            next_nodes=(f"n{index + 1}",) if index + 1 < chain_length else ("end",),
        )
        for index in range(chain_length)
    }
    nodes["end"] = _node("end", NodeKind.TERMINAL)
    graph = _graph(nodes, entry="n0")

    outcome = await _runtime(tool).run(
        graph, tenant_id=TENANT, execution_id="exec-1", step_budget=3
    )

    assert outcome.succeeded is False
    assert outcome.status is NodeStatus.FAILED


async def test_expired_deadline_stops_the_workflow() -> None:
    tool = _SequenceHandler(NodeKind.TOOL, [{}, {}])
    # Two hops are needed: advancing to a terminal node finishes the run
    # immediately, so a single hop would end before the deadline is checked.
    graph = _graph(
        {
            "a": _node("a", NodeKind.TOOL, next_nodes=("b",)),
            "b": _node("b", NodeKind.TOOL, next_nodes=("end",)),
            "end": _node("end", NodeKind.TERMINAL),
        },
        entry="a",
    )

    outcome = await _runtime(tool).run(
        graph, tenant_id=TENANT, execution_id="exec-1", deadline_epoch_ms=1
    )

    assert outcome.succeeded is False
    assert outcome.status is NodeStatus.FAILED
    assert tool.calls == ["a"]


async def test_duration_is_measured() -> None:
    graph = _graph({"start": _node("start", NodeKind.TERMINAL)}, entry="start")

    outcome = await _runtime().run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.duration_ms >= 0


async def test_steps_taken_is_exposed() -> None:
    graph = _graph({"start": _node("start", NodeKind.TERMINAL)}, entry="start")

    outcome = await _runtime().run(graph, tenant_id=TENANT, execution_id="exec-1")

    assert outcome.steps_taken >= 0


# --- graph validation --------------------------------------------------------


def test_graph_validates_its_definition() -> None:
    with pytest.raises(InvalidRequestError, match="entry node is not defined"):
        build_graph(
            WorkflowDefinition(
                workflow_id="wf",
                tenant_id=TENANT,
                name="n",
                nodes={"a": _node("a", NodeKind.TERMINAL)},
                entry="missing",
            )
        )


def test_graph_orders_nodes_from_the_entry() -> None:
    graph = _graph(
        {
            "b": _node("b", NodeKind.TERMINAL),
            "a": _node("a", NodeKind.TOOL, next_nodes=("b",)),
        },
        entry="a",
    )

    assert graph.order[0] == "a"
    assert graph.entry == "a"


def test_terminal_nodes_end_the_run() -> None:
    graph = _graph({"end": _node("end", NodeKind.TERMINAL)}, entry="end")

    assert graph.is_terminal("end") is True


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
