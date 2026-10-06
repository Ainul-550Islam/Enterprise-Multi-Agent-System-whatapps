"""Workflow graph.

The graph is the validated, executable form of a
:class:`~mas_orchestrator.workflows.models.WorkflowDefinition`. Validation
happens once, at construction, so the runtime can walk the graph without
re-checking it at every step.

The checks that matter are structural rather than cosmetic:

* every successor is defined,
* the entry node exists,
* no cycle is reachable except through a loop node, which carries its own
  ceiling,
* the summed step budget is within the platform ceiling.

The last two are what make :func:`~mas_orchestrator.workflows.models.bounded`
a guarantee. A workflow that passes here cannot spin forever, and it cannot do
unbounded amounts of work.
"""

from __future__ import annotations

from dataclasses import dataclass

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.workflows.models import (
    MAX_WORKFLOW_STEPS,
    NodeKind,
    WorkflowDefinition,
    WorkflowNode,
    bounded,
    total_limits,
    validate_workflow,
)


@dataclass(frozen=True, slots=True)
class Graph:
    """A validated workflow.

    Attributes:
        definition: The source definition.
        order: Node ids in a topological-ish order: entry first, then depth
            first. Used for deterministic iteration where order would otherwise
            depend on dictionary insertion.
    """

    definition: WorkflowDefinition
    order: tuple[str, ...]

    @property
    def workflow_id(self) -> str:
        """The workflow identifier."""
        return self.definition.workflow_id

    @property
    def entry(self) -> str:
        """The entry node id."""
        return self.definition.entry

    def node(self, node_id: str) -> WorkflowNode:
        """Looks a node up.

        Raises:
            InvalidRequestError: If the node is not in the graph.
        """
        node = self.definition.nodes.get(node_id)
        if node is None:
            raise InvalidRequestError(
                "node is not in the workflow",
                details={"node_id": node_id, "workflow_id": self.workflow_id},
            )
        return node

    def successors(self, node_id: str) -> tuple[str, ...]:
        """The nodes a node may lead to."""
        return self.node(node_id).successors

    def is_terminal(self, node_id: str) -> bool:
        """Whether a node ends the run."""
        node = self.node(node_id)
        return node.kind is NodeKind.TERMINAL or not node.successors

    def validate(self) -> Graph:
        """Re-runs validation, returning self.

        Cheap enough to call at run time, and it closes the gap between a graph
        deserialised from storage and one built in memory.
        """
        validate_workflow(self.definition)
        return self


def build_graph(definition: WorkflowDefinition) -> Graph:
    """Validates a definition and produces an executable graph.

    Raises:
        InvalidRequestError: If the definition is structurally invalid,
            unbounded, or over budget.
    """
    validate_workflow(definition)

    order: list[str] = []
    seen: set[str] = set()

    def walk(node_id: str) -> None:
        if node_id in seen:
            return
        seen.add(node_id)
        order.append(node_id)
        for successor in definition.nodes[node_id].successors:
            walk(successor)

    walk(definition.entry)
    # Nodes unreachable from the entry are appended so an inspection of the
    # graph can see them; they will never execute.
    for node_id in definition.nodes:
        if node_id not in seen:
            order.append(node_id)

    return Graph(definition=definition, order=tuple(order))


def reachable_from(graph: Graph, node_id: str) -> tuple[str, ...]:
    """Every node reachable from a node, including itself."""
    found: list[str] = []
    seen: set[str] = set()
    stack = [node_id]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        found.append(current)
        stack.extend(graph.successors(current))
    return tuple(found)


def summary(graph: Graph) -> dict[str, object]:
    """A short description of a graph, for diagnostics.

    Reports counts and kinds only — no configuration values, which may contain
    prompt text.
    """
    kinds: dict[str, int] = {}
    for node in graph.definition.nodes.values():
        kinds[node.kind.value] = kinds.get(node.kind.value, 0) + 1
    return {
        "workflow_id": graph.workflow_id,
        "entry": graph.entry,
        "node_count": len(graph.definition.nodes),
        "kinds": kinds,
        "bounded": bounded(graph.definition),
        "total_steps": total_limits(graph.definition).max_steps,
        "step_ceiling": MAX_WORKFLOW_STEPS,
    }
