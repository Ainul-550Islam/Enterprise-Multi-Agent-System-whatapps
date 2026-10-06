"""The workflow runtime.

Walks a validated graph one node at a time. The runtime is deliberately dumb:
it decides which node runs next and asks a handler to run it. All policy lives
in the graph (structure, ceilings) and in the handlers (behaviour), so this
module has no opinion about what a node *means*.

What the runtime owns is **stopping**. Three independent brakes, because each
can fail:

1. The loop refuses to advance once ``steps_taken`` reaches the budget.
2. Every transition goes through
   :meth:`~mas_orchestrator.workflows.state.WorkflowState.advance_to`, which
   also enforces the wall-clock deadline.
3. A node that raises is recorded as failed and stops the run, rather than
   being retried indefinitely.

Handlers are looked up by node *kind*, not by name. A workflow definition
cannot name a handler, so it cannot select one that was never registered —
which is what keeps a definition from being a gadget chain.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import InvalidRequestError, TenantContextError
from mas_orchestrator.workflows.conditions import select_branch
from mas_orchestrator.workflows.loops import run_loop
from mas_orchestrator.workflows.models import NodeKind
from mas_orchestrator.workflows.node import (
    Invocation,
    NodeHandler,
    resolve_input,
)
from mas_orchestrator.workflows.parallel import run_parallel

if TYPE_CHECKING:
    from mas_orchestrator.workflows.graph import Graph

from mas_orchestrator.workflows.state import (
    NodeResult,
    NodeStatus,
    WorkflowState,
    initial_state,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WorkflowOutcome:
    """How a workflow run ended.

    Attributes:
        state: Final state.
        status: Why it stopped.
        results: Every node result, in completion order.
        duration_ms: Wall-clock duration.
        error: Error message, when it failed.
    """

    state: WorkflowState
    status: NodeStatus
    results: tuple[NodeResult, ...]
    duration_ms: int = 0
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        """Whether the workflow completed."""
        return self.status is NodeStatus.COMPLETED

    @property
    def steps_taken(self) -> int:
        """How many transitions the run made."""
        return self.state.steps_taken


class WorkflowRuntime:
    """Executes validated workflows."""

    def __init__(self, handlers: dict[NodeKind, NodeHandler] | None = None) -> None:
        self._handlers: dict[NodeKind, NodeHandler] = dict(handlers or {})

    def register(self, handler: NodeHandler) -> None:
        """Registers a handler for its kind.

        Registering a second handler for the same kind replaces the first. That
        is deliberate — a strict refusal would make composition awkward — but it
        is logged, because silently losing a handler is a good way to spend an
        afternoon.
        """
        kind = handler.kind
        if kind in self._handlers:
            _LOGGER.warning(
                "workflow handler replaced",
                extra={"event": "workflow.handler_replaced", "kind": kind.value},
            )
        self._handlers[kind] = handler

    def handler_for(self, kind: NodeKind) -> NodeHandler:
        """Looks up a handler.

        Raises:
            InvalidRequestError: If no handler is registered. A node with no
                handler is a configuration error, and skipping it silently
                would produce a workflow that quietly does less than it says.
        """
        handler = self._handlers.get(kind)
        if handler is None:
            raise InvalidRequestError(
                f"no handler is registered for {kind.value} nodes",
                details={"kind": kind.value},
            )
        return handler

    async def run(
        self,
        graph: Graph,
        *,
        tenant_id: str,
        execution_id: str,
        variables: dict[str, object] | None = None,
        step_budget: int | None = None,
        deadline_epoch_ms: int | None = None,
    ) -> WorkflowOutcome:
        """Runs a workflow to completion.

        Args:
            graph: A validated graph.
            tenant_id: Owning tenant, from the authoritative context.
            execution_id: The execution this run belongs to.
            variables: Starting variables.
            step_budget: Overrides the graph's default budget.
            deadline_epoch_ms: When the run must stop.

        Returns:
            A :class:`WorkflowOutcome`. A run that hits a brake reports
            ``FAILED`` rather than pretending to have finished.
        """
        # A workflow is owned by the tenant that defined it. Running it under
        # another tenant would execute someone else's workflow inside this
        # tenant's execution, so the mismatch is refused rather than logged.
        if graph.definition.tenant_id != tenant_id:
            raise TenantContextError(
                "workflow belongs to a different tenant",
                details={
                    "workflow_id": graph.workflow_id,
                    "workflow_tenant": graph.definition.tenant_id,
                    "run_tenant": tenant_id,
                },
            )

        started = time.monotonic()
        state = initial_state(
            workflow_id=graph.workflow_id,
            tenant_id=tenant_id,
            execution_id=execution_id,
            entry=graph.entry,
            variables=dict(variables or {}),
            step_budget=step_budget or 200,
            deadline_epoch_ms=deadline_epoch_ms,
        )

        while not state.finished:
            node = graph.node(state.current)
            if node.kind is NodeKind.PARALLEL:
                state, stop = await self._run_parallel(graph, node.node_id, state)
                if stop is not None:
                    return self._finish(state, stop, started)
                continue
            if node.kind is NodeKind.LOOP:
                state, stop = await self._run_loop(node.node_id, node.config, state)
                if stop is not None:
                    return self._finish(state, stop, started)
                continue

            handler = self.handler_for(node.kind)
            invocation = Invocation(node=node, state=state, input=resolve_input(node, state))
            try:
                result = await handler.run(invocation)
            except Exception as error:
                _LOGGER.error(
                    "workflow node raised",
                    extra={
                        "event": "workflow.node_raised",
                        "node_id": node.node_id,
                        "error": type(error).__name__,
                    },
                )
                return self._finish(
                    state.with_result(
                        NodeResult(
                            node_id=node.node_id,
                            status=NodeStatus.FAILED,
                            error=type(error).__name__,
                        )
                    ),
                    NodeStatus.FAILED,
                    started,
                    error=f"{type(error).__name__}: {error}",
                )

            state = state.with_result(result)
            if result.status is NodeStatus.FAILED:
                return self._finish(state, NodeStatus.FAILED, started, error=result.error)

            state = state.with_variables(_output_variables(node.node_id, result))
            state, stop = self._advance(graph, node.node_id, state, result)
            if stop is not None:
                return self._finish(state, stop, started)

        return self._finish(state, NodeStatus.COMPLETED, started)

    # --- internals ---------------------------------------------------------

    def _advance(
        self,
        graph: Graph,
        node_id: str,
        state: WorkflowState,
        result: NodeResult,
    ) -> tuple[WorkflowState, NodeStatus | None]:
        """Chooses the next node.

        Returns:
            ``(state, None)`` to continue, or ``(state, status)`` to stop.
        """
        node = graph.node(node_id)

        successor: str | None = None
        if node.branches:
            condition = node.config.get("condition")
            if condition is None:
                return state, NodeStatus.FAILED
            successor = select_branch(
                expression=condition,
                variables=state.variables,
                branches=node.branches,
                default=node.config.get("default"),
            )
        elif node.next:
            successor = node.next[0]

        if successor is None or graph.is_terminal(successor):
            return state.finish(), None

        try:
            return state.advance_to(successor), None
        except Exception as error:
            _LOGGER.warning(
                "workflow stopped by a limit",
                extra={
                    "event": "workflow.limit_reached",
                    "node_id": node_id,
                    "error": type(error).__name__,
                },
            )
            return state, NodeStatus.FAILED

    async def _run_parallel(
        self,
        graph: Graph,
        node_id: str,
        state: WorkflowState,
    ) -> tuple[WorkflowState, NodeStatus | None]:
        """Runs a parallel node."""
        from mas_orchestrator.workflows.models import JoinMode

        node = graph.node(node_id)
        branches = node.config.get("branches") or {}
        if not isinstance(branches, dict):
            return state, NodeStatus.FAILED

        mode_name = node.config.get("join", JoinMode.ALL.value)
        try:
            mode = JoinMode(mode_name)
        except ValueError:
            return state, NodeStatus.FAILED

        handler = self.handler_for(NodeKind.TOOL) if NodeKind.TOOL in self._handlers else None
        del handler  # branches are dispatched through the shared worker below

        async def worker(branch_id: str, config: object, branch_state: WorkflowState) -> NodeResult:
            """Runs one branch as an inline node."""
            branch_config = config if isinstance(config, dict) else {}
            kind_name = branch_config.get("kind", NodeKind.TERMINAL.value)
            try:
                kind = NodeKind(kind_name)
            except ValueError:
                return NodeResult(
                    node_id=node_id,
                    status=NodeStatus.FAILED,
                    error=f"unknown branch kind: {kind_name}",
                )
            branch_handler = self.handler_for(kind)
            from mas_orchestrator.workflows.models import WorkflowNode

            branch_node = WorkflowNode(
                node_id=f"{node_id}:{branch_id}",
                kind=kind,
                config=branch_config,
            )
            invocation = Invocation(
                node=branch_node,
                state=branch_state,
                input=resolve_input(branch_node, branch_state),
            )
            return await branch_handler.run(invocation)

        outcome = await run_parallel(
            branches,
            state,
            worker=worker,
            node_id=node_id,
            mode=mode,
            timeout_seconds=node.config.get("timeout_seconds"),
        )
        joined = outcome.join()
        state = state.with_result(joined)
        state = state.with_variables(_output_variables(node_id, joined))
        if not joined.succeeded:
            return state, NodeStatus.FAILED

        state, stop = self._advance(graph, node_id, state, joined)
        return state, stop

    async def _run_loop(
        self,
        node_id: str,
        config: dict[str, object],
        state: WorkflowState,
    ) -> tuple[WorkflowState, NodeStatus | None]:
        """Runs a loop node."""
        from mas_orchestrator.workflows.models import NodeKind as Kind
        from mas_orchestrator.workflows.models import WorkflowNode

        body_config = config.get("body") or {}
        body_kind_name = (
            body_config.get("kind", Kind.TERMINAL.value)
            if isinstance(body_config, dict)
            else Kind.TERMINAL.value
        )
        try:
            body_kind = Kind(body_kind_name)
        except ValueError:
            return state, NodeStatus.FAILED

        handler = self.handler_for(body_kind)

        async def body(iteration: int, iteration_state: WorkflowState) -> NodeResult:
            body_node = WorkflowNode(
                node_id=f"{node_id}#{iteration}",
                kind=body_kind,
                config=body_config if isinstance(body_config, dict) else {},
            )
            invocation = Invocation(
                node=body_node,
                state=iteration_state,
                input=resolve_input(body_node, iteration_state),
            )
            return await handler.run(invocation)

        outcome = await run_loop(
            node_id=node_id,
            config=config,
            state=state,
            body=body,
            continue_while=None,
            detect_convergence=bool(config.get("detect_convergence", True)),
        )

        last = outcome.last
        merged = NodeResult(
            node_id=node_id,
            status=NodeStatus.COMPLETED if outcome.succeeded else NodeStatus.FAILED,
            output={
                "iterations": len(outcome.iterations),
                "converged": outcome.converged,
                "halted": outcome.halted,
                "last_output": last.output if last else {},
            },
            error=None if outcome.succeeded else "loop did not complete",
            duration_ms=outcome.duration_ms,
        )
        state = state.with_result(merged)
        state = state.with_variables(_output_variables(node_id, merged))
        if not merged.succeeded:
            return state, NodeStatus.FAILED

        successor = state.variable(f"{node_id}.next")
        if isinstance(successor, str) and successor:
            try:
                return state.advance_to(successor), None
            except Exception:
                return state, NodeStatus.FAILED
        return state.finish(), None

    def _finish(
        self,
        state: WorkflowState,
        status: NodeStatus,
        started: float,
        error: str | None = None,
    ) -> WorkflowOutcome:
        """Builds the outcome for a finished run."""
        duration = int((time.monotonic() - started) * 1000)
        _LOGGER.info(
            "workflow finished",
            extra={
                "event": "workflow.finished",
                "workflow_id": state.workflow_id,
                "status": status.value,
                "steps": state.steps_taken,
                "duration_ms": duration,
            },
        )
        return WorkflowOutcome(
            state=state.finish(),
            status=status,
            results=state.results,
            duration_ms=duration,
            error=error,
        )


def _output_variables(node_id: str, result: NodeResult) -> dict[str, object]:
    """Records a node's output under a namespaced key.

    Namespaced so two nodes cannot overwrite each other's output, and the
    variables mapping stays a record of what actually ran.
    """
    return {f"{node_id}.status": result.status.value, f"{node_id}.output": result.output}
