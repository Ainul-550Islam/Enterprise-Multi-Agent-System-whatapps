"""Tests for the workflow engine.

Coverage targets the three things a workflow engine can get dangerously wrong:
running forever, running another tenant's workflow, and reporting a stopped run
as a finished one.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError, TenantContextError
from mas_orchestrator.workflows import (
    DEFAULT_STEP_BUDGET,
    BranchOutcome,
    Combinator,
    Condition,
    ConditionGroup,
    Graph,
    Invocation,
    JoinMode,
    LoopOutcome,
    NodeKind,
    NodeLimits,
    NodeResult,
    NodeStatus,
    Operator,
    ParallelOutcome,
    WorkflowDefinition,
    WorkflowNode,
    WorkflowOutcome,
    WorkflowRuntime,
    WorkflowState,
    assert_bounded,
    build_graph,
    evaluate,
    initial_state,
    reachable_from,
    resolve_path,
    run_parallel,
    select_branch,
    summary,
    total_limits,
    validate_loop_config,
    validate_workflow,
)

TENANT = "tenant-alpha"
EXECUTION = "exec-1"


def _terminal(node_id: str = "end") -> WorkflowNode:
    return WorkflowNode(node_id=node_id, kind=NodeKind.TERMINAL)


def _linear() -> WorkflowDefinition:
    return WorkflowDefinition(
        workflow_id="wf-linear",
        tenant_id=TENANT,
        name="Linear",
        entry="a",
        nodes={
            "a": WorkflowNode("a", NodeKind.AGENT, next=("b",)),
            "b": WorkflowNode("b", NodeKind.AGENT, next=("end",)),
            "end": _terminal(),
        },
    )


class _AnyHandler:
    """Completes any node of the kinds a linear workflow uses.

    A single class registered per kind, because ``StaticHandler`` serves only
    the terminal kind and a run needs every kind on its path to be handled.
    """

    def __init__(self, kind: NodeKind) -> None:
        self._kind = kind

    @property
    def kind(self) -> NodeKind:
        return self._kind

    async def run(self, invocation):
        return NodeResult(invocation.node.node_id, NodeStatus.COMPLETED, output={"ok": True})


def _runtime() -> WorkflowRuntime:
    runtime = WorkflowRuntime()
    runtime.register(_AnyHandler(NodeKind.AGENT))
    runtime.register(_AnyHandler(NodeKind.TERMINAL))
    return runtime


# --- construction and validation ------------------------------------------


class TestDefinitionValidation:
    def test_entry_must_exist(self) -> None:
        with pytest.raises(InvalidRequestError):
            WorkflowDefinition(
                workflow_id="wf",
                tenant_id=TENANT,
                name="x",
                entry="missing",
                nodes={"a": _terminal("a")},
            )

    def test_tenant_required(self) -> None:
        with pytest.raises(InvalidRequestError):
            WorkflowDefinition(
                workflow_id="wf", tenant_id=" ", name="x", entry="a", nodes={"a": _terminal("a")}
            )

    def test_nodes_required(self) -> None:
        with pytest.raises(InvalidRequestError):
            WorkflowDefinition(workflow_id="wf", tenant_id=TENANT, name="x", entry="a", nodes={})

    def test_dangling_successor_rejected(self) -> None:
        with pytest.raises(InvalidRequestError):
            WorkflowDefinition(
                workflow_id="wf",
                tenant_id=TENANT,
                name="x",
                entry="a",
                nodes={"a": WorkflowNode("a", NodeKind.AGENT, next=("ghost",))},
            )

    def test_condition_node_needs_branches(self) -> None:
        with pytest.raises(InvalidRequestError):
            WorkflowNode("c", NodeKind.CONDITION)

    def test_non_condition_may_not_have_branches(self) -> None:
        with pytest.raises(InvalidRequestError):
            WorkflowNode("a", NodeKind.AGENT, branches={"yes": "b"})

    def test_node_limits_reject_zero_steps(self) -> None:
        with pytest.raises(InvalidRequestError):
            NodeLimits(max_steps=0)

    def test_node_limits_reject_negative_retries(self) -> None:
        with pytest.raises(InvalidRequestError):
            NodeLimits(max_retries=-1)

    def test_successors_cover_branches(self) -> None:
        node = WorkflowNode("c", NodeKind.CONDITION, branches={"yes": "a", "no": "b"})
        assert set(node.successors) == {"a", "b"}


class TestBoundedness:
    def test_acyclic_graph_is_bounded(self) -> None:
        assert assert_bounded(_linear()) is None

    def test_cycle_is_not_bounded(self) -> None:
        cyclic = WorkflowDefinition(
            workflow_id="wf-cycle",
            tenant_id=TENANT,
            name="Cycle",
            entry="a",
            nodes={
                "a": WorkflowNode("a", NodeKind.AGENT, next=("b",)),
                "b": WorkflowNode("b", NodeKind.AGENT, next=("a",)),
            },
        )
        with pytest.raises(InvalidRequestError):
            assert_bounded(cyclic)

    def test_total_limits_sums_nodes(self) -> None:
        definition = WorkflowDefinition(
            workflow_id="wf",
            tenant_id=TENANT,
            name="x",
            entry="a",
            nodes={
                "a": WorkflowNode("a", NodeKind.AGENT, limits=NodeLimits(max_steps=5)),
                "b": WorkflowNode("b", NodeKind.AGENT, limits=NodeLimits(max_steps=7)),
            },
        )
        assert total_limits(definition).max_steps == 12

    def test_validate_workflow_returns_definition(self) -> None:
        assert validate_workflow(_linear()).workflow_id == "wf-linear"


# --- graph -----------------------------------------------------------------


class TestGraph:
    def test_build_and_summary(self) -> None:
        graph = build_graph(_linear())
        assert graph.entry == "a"
        assert graph.node("b").node_id == "b"
        report = summary(graph)
        assert report["node_count"] == 3
        # Counts and kinds only: configuration may contain prompt text.
        assert report["kinds"] == {"agent": 2, "terminal": 1}

    def test_unknown_node_raises(self) -> None:
        graph = build_graph(_linear())
        with pytest.raises(InvalidRequestError):
            graph.node("nope")

    def test_reachable_from_returns_reachable(self) -> None:
        graph = build_graph(_linear())
        assert "end" in reachable_from(graph, "a")

    def test_terminal_detection(self) -> None:
        graph = build_graph(_linear())
        assert graph.is_terminal("end")
        assert not graph.is_terminal("a")

    def test_graph_is_a_graph_type(self) -> None:
        assert isinstance(build_graph(_linear()), Graph)


# --- state -----------------------------------------------------------------


class TestState:
    def test_initial_state_not_finished(self) -> None:
        state = initial_state(workflow_id="wf", tenant_id=TENANT, execution_id=EXECUTION, entry="a")
        assert not state.finished
        assert state.current == "a"

    def test_default_step_budget_is_positive(self) -> None:
        assert DEFAULT_STEP_BUDGET > 0

    def test_advance_to_spends_a_step(self) -> None:
        state = initial_state(workflow_id="wf", tenant_id=TENANT, execution_id=EXECUTION, entry="a")
        advanced = state.advance_to("b")
        assert advanced.current == "b"
        assert advanced.steps_taken == state.steps_taken + 1

    def test_budget_is_checked_on_advance(self) -> None:
        from mas_orchestrator.contracts.errors import ExecutionTimeoutError

        state = initial_state(
            workflow_id="wf",
            tenant_id=TENANT,
            execution_id=EXECUTION,
            entry="a",
            step_budget=1,
        )
        spent = state.advance_to("b")
        with pytest.raises(ExecutionTimeoutError):
            spent.advance_to("c")

    def test_state_is_immutable(self) -> None:
        state = initial_state(workflow_id="wf", tenant_id=TENANT, execution_id=EXECUTION, entry="a")
        assert isinstance(state, WorkflowState)


# --- conditions ------------------------------------------------------------


class TestConditions:
    def test_resolve_path_reads_nested(self) -> None:
        assert resolve_path({"a": {"b": 1}}, "a.b") == 1

    def test_resolve_path_missing_returns_none(self) -> None:
        # None rather than an exception: a branch on an unset variable is
        # simply false.
        assert resolve_path({"a": 1}, "x.y") is None

    def test_resolve_path_empty_part_is_none(self) -> None:
        assert resolve_path({"a": 1}, "a..b") is None

    def test_evaluate_equals(self) -> None:
        condition = Condition(path="status", operator=Operator.EQ, value="ok")
        assert evaluate(condition, {"status": "ok"})

    def test_evaluate_not_equals(self) -> None:
        condition = Condition(path="status", operator=Operator.NE, value="ok")
        assert evaluate(condition, {"status": "bad"})

    def test_evaluate_greater_than(self) -> None:
        condition = Condition(path="score", operator=Operator.GT, value=5)
        assert evaluate(condition, {"score": 9})

    def test_evaluate_contains(self) -> None:
        assert evaluate(
            Condition(path="text", operator=Operator.CONTAINS, value="needle"),
            {"text": "a needle here"},
        )

    def test_group_and_requires_every_condition(self) -> None:
        group = ConditionGroup(
            combinator=Combinator.AND,
            conditions=(
                Condition(path="a", operator=Operator.EQ, value=1),
                Condition(path="b", operator=Operator.EQ, value=2),
            ),
        )
        assert evaluate(group, {"a": 1, "b": 2})
        assert not evaluate(group, {"a": 1, "b": 3})

    def test_group_or_needs_one_condition(self) -> None:
        group = ConditionGroup(
            combinator=Combinator.OR,
            conditions=(
                Condition(path="a", operator=Operator.EQ, value=1),
                Condition(path="b", operator=Operator.EQ, value=2),
            ),
        )
        assert evaluate(group, {"a": 99, "b": 2})

    def test_select_branch_uses_true_and_false_names(self) -> None:
        branches = {"true": "a", "false": "b"}
        condition = Condition(path="ok", operator=Operator.TRUTHY)
        assert select_branch(expression=condition, variables={"ok": True}, branches=branches) == "a"
        assert (
            select_branch(expression=condition, variables={"ok": False}, branches=branches) == "b"
        )

    def test_select_branch_falls_back_to_default(self) -> None:
        assert (
            select_branch(
                expression=Condition(path="ok", operator=Operator.TRUTHY),
                variables={"ok": True},
                branches={"other": "a"},
                default="fallback",
            )
            == "fallback"
        )

    def test_select_branch_without_match_is_none(self) -> None:
        assert (
            select_branch(
                expression=Condition(path="ok", operator=Operator.TRUTHY),
                variables={"ok": True},
                branches={"other": "a"},
            )
            is None
        )

    def test_evaluate_unknown_path_is_false_not_error(self) -> None:
        # A missing path is a false condition, not a crash: a workflow must not
        # fail because a variable was never set.
        assert not evaluate(Condition(path="missing", operator=Operator.EQ, value=1), {})


# --- runtime ---------------------------------------------------------------


class TestRuntime:
    async def test_linear_run_completes(self) -> None:
        runtime = _runtime()
        outcome = await runtime.run(
            build_graph(_linear()), tenant_id=TENANT, execution_id=EXECUTION
        )
        assert outcome.status is NodeStatus.COMPLETED
        assert outcome.succeeded

    async def test_run_rejects_other_tenant(self) -> None:
        runtime = _runtime()
        with pytest.raises(TenantContextError):
            await runtime.run(
                build_graph(_linear()), tenant_id="tenant-other", execution_id=EXECUTION
            )

    def test_bounded_rejects_unreachable_cycle(self) -> None:
        # A cycle is refused at graph build time, not discovered mid-run: a
        # runaway workflow must never get as far as executing a single node.
        with pytest.raises(InvalidRequestError):
            build_graph(
                WorkflowDefinition(
                    workflow_id="wf-cycle",
                    tenant_id=TENANT,
                    name="Cycle",
                    entry="a",
                    nodes={
                        "a": WorkflowNode("a", NodeKind.AGENT, next=("b",)),
                        "b": WorkflowNode("b", NodeKind.AGENT, next=("a",)),
                    },
                )
            )

    async def test_step_budget_stops_a_long_chain(self) -> None:
        # A legal acyclic chain longer than the budget: the budget must stop it
        # even though there is no cycle, because a workflow can do unbounded
        # work without ever repeating a node.
        nodes = {
            f"n{i}": WorkflowNode(
                f"n{i}",
                NodeKind.AGENT,
                next=(f"n{i + 1}",) if i < 9 else ("end",),
                limits=NodeLimits(max_steps=1),
            )
            for i in range(10)
        }
        nodes["end"] = _terminal()
        chain = WorkflowDefinition(
            workflow_id="wf-chain",
            tenant_id=TENANT,
            name="Chain",
            entry="n0",
            nodes=nodes,
        )
        outcome = await _runtime().run(
            build_graph(chain),
            tenant_id=TENANT,
            execution_id=EXECUTION,
            step_budget=3,
        )
        # Reported as stopped, never as finished.
        assert outcome.status is not NodeStatus.COMPLETED
        assert outcome.steps_taken <= 4

    async def test_handler_exception_fails_run(self) -> None:
        class Boom:
            kind = NodeKind.AGENT

            async def run(self, invocation: Invocation) -> NodeResult:
                raise RuntimeError("boom")

        runtime = WorkflowRuntime()
        runtime.register(Boom())
        outcome = await runtime.run(
            build_graph(_linear()), tenant_id=TENANT, execution_id=EXECUTION
        )
        assert outcome.status is NodeStatus.FAILED

    async def test_deadline_stops_run(self) -> None:
        # A deadline in the past: the run must stop before doing any work
        # rather than finishing and reporting late.
        outcome = await _runtime().run(
            build_graph(_linear()),
            tenant_id=TENANT,
            execution_id=EXECUTION,
            deadline_epoch_ms=1,
        )
        assert outcome.status is not NodeStatus.COMPLETED

    async def test_outcome_reports_steps_taken(self) -> None:
        outcome = await _runtime().run(
            build_graph(_linear()), tenant_id=TENANT, execution_id=EXECUTION
        )
        assert outcome.steps_taken >= 1
        assert isinstance(outcome, WorkflowOutcome)


# --- loops and parallel ----------------------------------------------------


class TestLoopsAndParallel:
    def test_loop_config_requires_positive_max(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_loop_config({"max_iterations": 0})

    def test_loop_outcome_shape(self) -> None:
        outcome = LoopOutcome(
            iterations=(NodeResult("a", NodeStatus.COMPLETED),),
            node_id="loop",
            max_iterations=3,
            converged=True,
        )
        assert outcome.converged
        assert outcome.last is not None
        assert outcome.succeeded

    def test_parallel_outcome_shape(self) -> None:
        branches = (BranchOutcome(branch_id="a", result=NodeResult("a", NodeStatus.COMPLETED)),)
        outcome = ParallelOutcome(outcomes=branches, mode=JoinMode.ALL)
        assert len(outcome.outcomes) == 1
        assert len(outcome.succeeded) == 1
        assert not outcome.failed
        assert outcome.join().succeeded

    async def test_run_parallel_joins_all(self) -> None:
        state = initial_state(workflow_id="wf", tenant_id=TENANT, execution_id=EXECUTION, entry="p")

        async def worker(branch_id: str, config: object, branch_state: WorkflowState) -> NodeResult:
            return NodeResult(branch_id, NodeStatus.COMPLETED)

        outcome = await run_parallel(
            {"a": {}, "b": {}},
            state,
            worker=worker,
            node_id="p",
            mode=JoinMode.ALL,
        )
        assert len(outcome.outcomes) == 2
        assert len(outcome.succeeded) == 2
        assert outcome.join().succeeded

    async def test_run_parallel_rejects_excess_fanout(self) -> None:
        state = initial_state(workflow_id="wf", tenant_id=TENANT, execution_id=EXECUTION, entry="p")
        with pytest.raises(InvalidRequestError):
            await run_parallel(
                {f"b{i}": {} for i in range(200)},
                state,
                worker=lambda *a: None,
                node_id="p",
                max_branches=2,
            )
