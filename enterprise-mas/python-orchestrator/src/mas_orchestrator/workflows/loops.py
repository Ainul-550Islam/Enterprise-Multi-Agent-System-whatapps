"""Loops.

A loop is the only way a workflow may cycle, and it may cycle only a bounded
number of times. That is enforced in three places, because each alone can be
caught out:

* :data:`MAX_LOOP_ITERATIONS` caps the configured ceiling at definition time.
* :func:`run_loop` counts iterations and stops at the configured maximum.
* Every iteration spends a step from the workflow budget through
  :meth:`~mas_orchestrator.workflows.state.WorkflowState.advance_to`, so a loop
  whose per-iteration bookkeeping is wrong still exhausts the global budget.

A loop also carries a convergence check. :func:`run_loop` stops when a body
produces the same output as the previous iteration, because the common failure
is not an infinite loop but a loop that makes no progress and would otherwise
run to its ceiling doing nothing.

Results accumulate per iteration and are returned in order, so a caller can see
what happened rather than only the final state.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.workflows.models import MAX_LOOP_ITERATIONS
from mas_orchestrator.workflows.state import NodeResult, NodeStatus, WorkflowState

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LoopOutcome:
    """What a loop produced.

    Attributes:
        iterations: One result per iteration, in order.
        node_id: The loop node.
        max_iterations: The ceiling applied.
        converged: Whether the loop stopped early because an iteration produced
            the same output as the one before.
        halted: Whether the loop stopped for a reason other than the condition:
            ceiling, budget, or a failed body.
        duration_ms: How long the loop took.
    """

    iterations: tuple[NodeResult, ...]
    node_id: str
    max_iterations: int
    converged: bool = False
    halted: bool = False
    duration_ms: int = 0

    @property
    def last(self) -> NodeResult | None:
        """The final iteration's result."""
        return self.iterations[-1] if self.iterations else None

    @property
    def succeeded(self) -> bool:
        """Whether every iteration completed."""
        if not self.iterations:
            return False
        return all(result.succeeded for result in self.iterations)


def validate_loop_config(config: dict[str, Any]) -> int:
    """Validates a loop node's configuration and returns the ceiling.

    Raises:
        InvalidRequestError: If the ceiling is missing, not positive, or above
            the platform maximum.
    """
    raw = config.get("max_iterations", MAX_LOOP_ITERATIONS)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise InvalidRequestError(
            "max_iterations must be an integer", details={"field": "max_iterations"}
        )
    if raw < 1:
        raise InvalidRequestError(
            "max_iterations must be at least 1", details={"max_iterations": raw}
        )
    if raw > MAX_LOOP_ITERATIONS:
        raise InvalidRequestError(
            "max_iterations exceeds the platform ceiling",
            details={"max_iterations": raw, "limit": MAX_LOOP_ITERATIONS},
        )
    return raw


async def run_loop(
    *,
    node_id: str,
    config: dict[str, Any],
    state: WorkflowState,
    body: Any,
    continue_while: Any = None,
    detect_convergence: bool = True,
) -> LoopOutcome:
    """Runs a loop body until it stops.

    Args:
        node_id: The loop node's id.
        config: The node configuration, supplying ``max_iterations``.
        state: Input state. Not modified; each iteration receives the state
            returned by the previous one.
        body: An async callable ``(iteration, state) -> NodeResult``.
        continue_while: An optional predicate ``(iteration, result, state) ->
            bool``. When absent, the loop runs to its ceiling or until the body
            fails or converges.
        detect_convergence: Whether to stop when two consecutive iterations
            produce identical output.

    Returns:
        A :class:`LoopOutcome`. Never raises for a body failure: a failed
        iteration is recorded and the loop halts, because continuing to
        iterate after a failure usually means repeating the failure.
    """
    from mas_orchestrator.workflows.conditions import evaluate

    max_iterations = validate_loop_config(config)
    condition = config.get("condition")
    started = time.monotonic()

    results: list[NodeResult] = []
    current = state
    previous_output: Any = _SENTINEL
    converged = False
    halted = False

    for iteration in range(max_iterations):
        # Spend a step per iteration, so the global budget bounds the loop even
        # if its own accounting is wrong.
        try:
            current = current.advance_to(f"{node_id}#{iteration}")
        except Exception:
            halted = True
            break

        try:
            result = await body(iteration, current)
        except Exception as error:
            results.append(
                NodeResult(
                    node_id=node_id,
                    status=NodeStatus.FAILED,
                    error=type(error).__name__,
                )
            )
            halted = True
            break

        results.append(result)

        if not result.succeeded:
            halted = True
            break

        # Two identical consecutive outputs mean the body has stopped making
        # progress; running to the ceiling would just burn budget.
        if (
            detect_convergence
            and previous_output is not _SENTINEL
            and previous_output == result.output
        ):
            converged = True
            break
        previous_output = result.output

        if continue_while is not None and not continue_while(iteration, result, current):
            break
        if condition is not None and not evaluate(condition, current.variables):
            break

    duration = int((time.monotonic() - started) * 1000)
    _LOGGER.info(
        "loop finished",
        extra={
            "event": "workflow.loop",
            "node_id": node_id,
            "iterations": len(results),
            "converged": converged,
            "halted": halted,
            "duration_ms": duration,
        },
    )
    return LoopOutcome(
        iterations=tuple(results),
        node_id=node_id,
        max_iterations=max_iterations,
        converged=converged,
        halted=halted,
        duration_ms=duration,
    )


class _Sentinel:
    """A unique marker distinct from any real output."""

    def __repr__(self) -> str:
        return "<no previous output>"


_SENTINEL: Final[Any] = _Sentinel()
