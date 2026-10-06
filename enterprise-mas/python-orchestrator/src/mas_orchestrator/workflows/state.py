"""Workflow state.

State is immutable and every transition returns a new object. That is not
stylistic: workflow execution branches, and a mutable store shared between
branches means a parallel branch can observe another branch's partial writes.
Immutable state makes each branch's view a value, so concurrency stops being a
correctness problem.

The state also carries a **step budget**. Every transition spends one, and the
runtime stops when it is exhausted. This is the backstop that makes the
:func:`~mas_orchestrator.workflows.models.bounded` check a guarantee rather than
an intention: even a workflow whose graph analysis was wrong cannot run forever.
"""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

from mas_orchestrator.contracts.errors import ExecutionTimeoutError, InvalidRequestError

#: Default step budget. Deliberately finite.
DEFAULT_STEP_BUDGET: int = 200


class NodeStatus(StrEnum):
    """Where a node has got to."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class NodeResult:
    """What one node produced.

    Attributes:
        node_id: The node.
        status: How it ended.
        output: A JSON-serialisable result.
        error: Error message, when it failed.
        duration_ms: How long it took.
    """

    node_id: str
    status: NodeStatus
    output: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    duration_ms: int = 0

    @property
    def succeeded(self) -> bool:
        """Whether the node completed.

        ``skipped`` is not a success. A skipped node produced nothing, and a
        caller that treats it as success will read an empty output as a result.
        """
        return self.status is NodeStatus.COMPLETED


@dataclass(frozen=True, slots=True)
class WorkflowState:
    """The immutable state of one workflow run.

    Attributes:
        workflow_id: The definition being run.
        tenant_id: Owning tenant.
        execution_id: The execution this belongs to.
        current: The node about to run.
        variables: Accumulated data. Replaced, never mutated.
        results: Per-node results, in completion order.
        steps_taken: How many transitions have happened.
        step_budget: How many are permitted.
        started_epoch_ms: When the run began.
        deadline_epoch_ms: When it must stop, if bounded by the caller.
    """

    workflow_id: str
    tenant_id: str
    execution_id: str
    current: str = ""
    variables: dict[str, Any] = field(default_factory=dict)
    results: tuple[NodeResult, ...] = ()
    steps_taken: int = 0
    step_budget: int = DEFAULT_STEP_BUDGET
    started_epoch_ms: int = 0
    deadline_epoch_ms: int | None = None

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "workflow state must belong to a tenant", details={"field": "tenant_id"}
            )
        if self.step_budget < 1:
            raise InvalidRequestError(
                "step_budget must be at least 1",
                details={"step_budget": self.step_budget},
            )
        if not self.started_epoch_ms:
            object.__setattr__(self, "started_epoch_ms", int(time.time() * 1000))

    @property
    def finished(self) -> bool:
        """Whether the run has stopped because it reached the end.

        Deliberately **not** true when the budget is merely exhausted. A run
        that used up its budget did not finish; it was stopped, and the runtime
        must report it as stopped. Folding the two together made a truncated
        workflow indistinguishable from a completed one, which is the one
        answer a caller must never get wrong.
        """
        return self.current == ""

    @property
    def exhausted(self) -> bool:
        """Whether the step budget is used up.

        Reported separately from :attr:`finished` so a caller can tell "ran to
        the end" from "ran out of allowance".
        """
        return self.steps_taken >= self.step_budget

    @property
    def steps_remaining(self) -> int:
        """How many transitions are left."""
        return max(0, self.step_budget - self.steps_taken)

    @property
    def last_result(self) -> NodeResult | None:
        """The most recent node result, if any."""
        return self.results[-1] if self.results else None

    def with_variable(self, key: str, value: Any) -> WorkflowState:
        """Returns a copy with one variable set.

        The nested dictionary is copied rather than shared, so a later mutation
        of the original cannot reach back into state that has already been
        handed to another branch.
        """
        updated = copy.deepcopy(dict(self.variables))
        updated[key] = value
        return replace(self, variables=updated)

    def with_variables(self, values: dict[str, Any]) -> WorkflowState:
        """Returns a copy with several variables set."""
        updated = copy.deepcopy(dict(self.variables))
        updated.update(values)
        return replace(self, variables=updated)

    def variable(self, key: str, default: Any = None) -> Any:
        """Reads a variable."""
        return self.variables.get(key, default)

    def with_result(self, result: NodeResult) -> WorkflowState:
        """Returns a copy with one more result recorded."""
        return replace(self, results=(*self.results, result))

    def advance_to(self, node_id: str) -> WorkflowState:
        """Returns a copy positioned at a new node, spending one step.

        Raises:
            ExecutionTimeoutError: If the budget is exhausted or the deadline
                has passed. Both are checked here rather than by the runtime,
                so there is no code path that advances without being checked.
        """
        if self.steps_taken >= self.step_budget:
            raise ExecutionTimeoutError(
                "workflow exhausted its step budget",
                details={
                    "workflow_id": self.workflow_id,
                    "steps_taken": self.steps_taken,
                    "budget": self.step_budget,
                },
            )
        if self.deadline_epoch_ms is not None:
            now = int(time.time() * 1000)
            if now >= self.deadline_epoch_ms:
                raise ExecutionTimeoutError(
                    "workflow passed its deadline",
                    details={
                        "workflow_id": self.workflow_id,
                        "deadline_epoch_ms": self.deadline_epoch_ms,
                    },
                )
        return replace(self, current=node_id, steps_taken=self.steps_taken + 1)

    def finish(self) -> WorkflowState:
        """Returns a copy that has stopped."""
        return replace(self, current="")

    def result_for(self, node_id: str) -> NodeResult | None:
        """The result a node produced, if it has run."""
        for result in self.results:
            if result.node_id == node_id:
                return result
        return None


def initial_state(
    *,
    workflow_id: str,
    tenant_id: str,
    execution_id: str,
    entry: str,
    variables: dict[str, Any] | None = None,
    step_budget: int = DEFAULT_STEP_BUDGET,
    deadline_epoch_ms: int | None = None,
) -> WorkflowState:
    """Builds the starting state for a run."""
    return WorkflowState(
        workflow_id=workflow_id,
        tenant_id=tenant_id,
        execution_id=execution_id,
        current=entry,
        variables=dict(variables or {}),
        step_budget=step_budget,
        deadline_epoch_ms=deadline_epoch_ms,
    )
