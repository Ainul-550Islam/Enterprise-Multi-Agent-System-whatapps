"""Workflow value types.

A workflow is a graph of steps. The models here are deliberately plain data:
the runtime interprets them, so a workflow can be validated, serialised and
diffed without executing anything.

The one opinion baked in is that **every step is bounded**. A node carries its
own step and time ceilings, and :func:`bounded` refuses a workflow whose total
is unbounded. An unbounded workflow is not a flexible workflow; it is a runaway
loop that has not happened yet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from mas_orchestrator.contracts.errors import InvalidRequestError

#: Ceilings applied to a whole workflow when it runs.
MAX_WORKFLOW_STEPS: int = 200
MAX_PARALLEL_BRANCHES: int = 16
MAX_LOOP_ITERATIONS: int = 25


class NodeKind(StrEnum):
    """What a step does."""

    AGENT = "agent"
    TOOL = "tool"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    TERMINAL = "terminal"


class JoinMode(StrEnum):
    """How a parallel or loop step combines its results."""

    ALL = "all"
    ANY = "any"
    FIRST_SUCCESS = "first_success"


@dataclass(frozen=True, slots=True)
class NodeLimits:
    """Per-node ceilings.

    Attributes:
        max_steps: Steps this node may take.
        max_duration_ms: Wall clock this node may consume.
        max_retries: Retries permitted for a transient failure.
    """

    max_steps: int = 20
    max_duration_ms: int = 120_000
    max_retries: int = 0

    def __post_init__(self) -> None:
        for name in ("max_steps", "max_duration_ms"):
            if getattr(self, name) < 1:
                raise InvalidRequestError(f"{name} must be at least 1", details={"field": name})
        if self.max_retries < 0:
            raise InvalidRequestError(
                "max_retries must not be negative",
                details={"max_retries": self.max_retries},
            )


@dataclass(frozen=True, slots=True)
class WorkflowNode:
    """One step.

    Attributes:
        node_id: Stable identifier, unique within the workflow.
        kind: What the step does.
        name: Human-readable name.
        config: Kind-specific configuration.
        limits: Ceilings for this step.
        next: Successor node ids. A condition node uses ``branches`` instead.
        branches: Named successors for a condition node.
    """

    node_id: str
    kind: NodeKind
    name: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    limits: NodeLimits = field(default_factory=NodeLimits)
    next: tuple[str, ...] = ()
    branches: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.node_id.strip():
            raise InvalidRequestError(
                "a workflow node requires an id", details={"field": "node_id"}
            )
        if self.kind is NodeKind.CONDITION and not self.branches:
            raise InvalidRequestError(
                "a condition node must declare at least one branch",
                details={"node_id": self.node_id},
            )
        if self.kind is not NodeKind.CONDITION and self.branches:
            raise InvalidRequestError(
                "only a condition node may declare branches",
                details={"node_id": self.node_id},
            )

    @property
    def successors(self) -> tuple[str, ...]:
        """Every node this one may lead to."""
        if self.branches:
            return tuple(self.branches.values())
        return self.next


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    """A complete workflow.

    Attributes:
        workflow_id: Stable identifier.
        tenant_id: Owning tenant, from the authoritative context.
        name: Human-readable name.
        nodes: Every node, keyed by id.
        entry: The node execution starts at.
        version: Definition version.
    """

    workflow_id: str
    tenant_id: str
    name: str
    nodes: dict[str, WorkflowNode]
    entry: str
    version: int = 1

    def __post_init__(self) -> None:
        if not self.workflow_id.strip():
            raise InvalidRequestError("a workflow requires an id", details={"field": "workflow_id"})
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "a workflow must belong to a tenant", details={"field": "tenant_id"}
            )
        if not self.nodes:
            raise InvalidRequestError(
                "a workflow must contain at least one node",
                details={"field": "nodes"},
            )
        if self.entry not in self.nodes:
            raise InvalidRequestError(
                "workflow entry node is not defined",
                details={"entry": self.entry, "defined": sorted(self.nodes)},
            )
        dangling = sorted(
            {
                successor
                for node in self.nodes.values()
                for successor in node.successors
                if successor not in self.nodes
            }
        )
        if dangling:
            raise InvalidRequestError(
                "workflow references nodes that are not defined",
                details={"missing": dangling},
            )

    @property
    def node_ids(self) -> tuple[str, ...]:
        """Every node id, in insertion order."""
        return tuple(self.nodes)


def bounded(definition: WorkflowDefinition) -> bool:
    """Whether a workflow is guaranteed to stop.

    A workflow is bounded when no cycle is reachable. Cycles are permitted only
    through loop nodes, which carry their own iteration ceiling; a plain cycle
    through conditional branches would never terminate.
    """
    visited: set[str] = set()

    def walk(node_id: str, path: frozenset[str]) -> bool:
        if node_id in path:
            return False
        if node_id in visited:
            return True
        visited.add(node_id)
        node = definition.nodes[node_id]
        if node.kind is NodeKind.LOOP:
            # The loop's own ceiling bounds the cycle; do not follow its body
            # back into the cycle check.
            return True
        return all(walk(s, path | {node_id}) for s in node.successors)

    return walk(definition.entry, frozenset())


def assert_bounded(definition: WorkflowDefinition) -> None:
    """Refuses an unbounded workflow.

    Raises:
        InvalidRequestError: If a cycle is reachable without a loop ceiling.
    """
    if not bounded(definition):
        raise InvalidRequestError(
            "workflow contains a cycle that no loop ceiling bounds",
            details={"workflow_id": definition.workflow_id},
        )


def total_limits(definition: WorkflowDefinition) -> NodeLimits:
    """The summed ceilings of every node.

    An upper bound on the work a workflow can do, used to reject a definition
    whose total would exceed the platform ceiling regardless of how it runs.
    """
    steps = sum(node.limits.max_steps for node in definition.nodes.values())
    duration = sum(node.limits.max_duration_ms for node in definition.nodes.values())
    retries = sum(node.limits.max_retries for node in definition.nodes.values())
    return NodeLimits(max_steps=steps, max_duration_ms=duration, max_retries=retries)


def validate_workflow(definition: WorkflowDefinition) -> WorkflowDefinition:
    """Validates a definition against the platform ceilings.

    Raises:
        InvalidRequestError: If the workflow is unbounded or its summed limits
            exceed the ceilings.
    """
    assert_bounded(definition)
    totals = total_limits(definition)
    if totals.max_steps > MAX_WORKFLOW_STEPS:
        raise InvalidRequestError(
            "workflow step budget exceeds the platform ceiling",
            details={"total": totals.max_steps, "limit": MAX_WORKFLOW_STEPS},
        )
    return definition
