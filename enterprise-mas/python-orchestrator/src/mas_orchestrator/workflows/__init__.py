"""Multi-step workflow execution.

A workflow is a graph of bounded steps: agents, tools, conditions, parallel
fan-out and loops. The package is built around one non-negotiable property —
**a workflow must stop**. Three independent brakes enforce it:

* the graph is validated at construction, and a cycle is only permitted through
  a loop node carrying its own iteration ceiling
  (:func:`~mas_orchestrator.workflows.models.bounded`);
* every transition spends a step from a finite budget, checked inside
  :meth:`~mas_orchestrator.workflows.state.WorkflowState.advance_to`;
* loops detect convergence, so a body that stops making progress stops running
  rather than burning its ceiling doing nothing.

State is immutable. Parallel branches are handed the *same* state object
rather than a shared mutable one, so a branch cannot observe another branch's
partial writes — which is what makes concurrency here a non-problem instead of
a class of bug.

Conditions are data, not code: a :class:`~.conditions.Condition` is a comparison
over a dotted variable path, evaluated by :func:`~.conditions.evaluate`. There
is no expression language to escape from.
"""

from __future__ import annotations

from mas_orchestrator.workflows.conditions import (
    Combinator,
    Condition,
    ConditionGroup,
    Operator,
    evaluate,
    resolve_path,
    select_branch,
)
from mas_orchestrator.workflows.graph import (
    Graph,
    build_graph,
    reachable_from,
    summary,
)
from mas_orchestrator.workflows.loops import (
    LoopOutcome,
    run_loop,
    validate_loop_config,
)
from mas_orchestrator.workflows.models import (
    MAX_LOOP_ITERATIONS,
    MAX_PARALLEL_BRANCHES,
    MAX_WORKFLOW_STEPS,
    JoinMode,
    NodeKind,
    NodeLimits,
    WorkflowDefinition,
    WorkflowNode,
    assert_bounded,
    bounded,
    total_limits,
    validate_workflow,
)
from mas_orchestrator.workflows.node import (
    AgentNodeHandler,
    Invocation,
    NodeHandler,
    StaticHandler,
    ToolNodeHandler,
    resolve_input,
)
from mas_orchestrator.workflows.parallel import (
    BranchOutcome,
    ParallelOutcome,
    run_parallel,
)
from mas_orchestrator.workflows.runtime import (
    WorkflowOutcome,
    WorkflowRuntime,
)
from mas_orchestrator.workflows.state import (
    DEFAULT_STEP_BUDGET,
    NodeResult,
    NodeStatus,
    WorkflowState,
    initial_state,
)

__all__ = [
    "DEFAULT_STEP_BUDGET",
    "MAX_LOOP_ITERATIONS",
    "MAX_PARALLEL_BRANCHES",
    "MAX_WORKFLOW_STEPS",
    "AgentNodeHandler",
    "BranchOutcome",
    "Combinator",
    "Condition",
    "ConditionGroup",
    "Graph",
    "Invocation",
    "JoinMode",
    "LoopOutcome",
    "NodeHandler",
    "NodeKind",
    "NodeLimits",
    "NodeResult",
    "NodeStatus",
    "Operator",
    "ParallelOutcome",
    "StaticHandler",
    "ToolNodeHandler",
    "WorkflowDefinition",
    "WorkflowNode",
    "WorkflowOutcome",
    "WorkflowRuntime",
    "WorkflowState",
    "assert_bounded",
    "bounded",
    "build_graph",
    "evaluate",
    "initial_state",
    "reachable_from",
    "resolve_input",
    "resolve_path",
    "run_loop",
    "run_parallel",
    "select_branch",
    "summary",
    "total_limits",
    "validate_loop_config",
    "validate_workflow",
]
