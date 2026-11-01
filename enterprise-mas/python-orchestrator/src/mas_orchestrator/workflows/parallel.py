"""Parallel execution.

Branches run concurrently and each sees **the same input state**. That is the
whole point of the immutability in :mod:`.state`: because state is a value, two
branches can be handed the same one without either observing the other's
writes.

Two things this module will not do:

* **No unbounded fan-out.** Branch count is capped. A workflow that spawns a
  branch per item of unbounded input is a resource exhaustion bug.
* **No partial-success ambiguity.** A join either waits for all branches, stops
  at the first success, or accepts any success — and which one it is comes from
  :class:`~mas_orchestrator.workflows.models.JoinMode`, not from whichever
  branch happened to finish first.

Cancellation is propagated to every sibling: when one branch is cancelled, the
others are too. Leaving siblings running after the workflow has moved on would
orphan work that keeps consuming budget.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.workflows.models import (
    MAX_PARALLEL_BRANCHES,
    JoinMode,
)
from mas_orchestrator.workflows.state import NodeResult, NodeStatus, WorkflowState

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BranchOutcome:
    """What one branch produced.

    Attributes:
        branch_id: Branch identifier.
        result: The node result.
    """

    branch_id: str
    result: NodeResult


@dataclass(frozen=True, slots=True)
class ParallelOutcome:
    """The combined result of a parallel step.

    Attributes:
        outcomes: Every branch's outcome, in declaration order.
        mode: The join mode applied.
        cancelled: Whether the step was cancelled.
        duration_ms: How long the slowest branch took.
    """

    outcomes: tuple[BranchOutcome, ...]
    mode: JoinMode
    cancelled: bool = False
    duration_ms: int = 0

    @property
    def succeeded(self) -> list[BranchOutcome]:
        """Branches that completed."""
        return [o for o in self.outcomes if o.result.succeeded]

    @property
    def failed(self) -> list[BranchOutcome]:
        """Branches that did not complete."""
        return [o for o in self.outcomes if not o.result.succeeded]

    def join(self) -> NodeResult:
        """Collapses the branches into one node result.

        The interpretation depends on the mode:

        * ``ALL`` — succeeds only if every branch succeeded.
        * ``ANY`` — succeeds if at least one branch succeeded.
        * ``FIRST_SUCCESS`` — succeeds if any branch succeeded, and the merged
          output is the first success in declaration order rather than the
          first to finish, so the result does not depend on scheduling.
        """
        node_id = self.outcomes[0].result.node_id if self.outcomes else ""
        if not self.outcomes:
            return NodeResult(node_id=node_id, status=NodeStatus.SKIPPED)

        succeeded = self.succeeded
        if self.cancelled:
            return NodeResult(
                node_id=node_id,
                status=NodeStatus.FAILED,
                error="parallel step was cancelled",
                duration_ms=self.duration_ms,
            )

        ok = len(succeeded) == len(self.outcomes) if self.mode is JoinMode.ALL else bool(succeeded)

        # Declaration order, not completion order: a workflow that produces a
        # different answer depending on which coroutine was scheduled first is
        # not reproducible.
        chosen = succeeded[0] if succeeded else self.outcomes[0]
        return NodeResult(
            node_id=node_id,
            status=NodeStatus.COMPLETED if ok else NodeStatus.FAILED,
            output={
                "branches": [
                    {
                        "branch_id": outcome.branch_id,
                        "status": outcome.result.status.value,
                        "output": outcome.result.output,
                    }
                    for outcome in self.outcomes
                ],
                "chosen": chosen.branch_id,
            },
            error=None if ok else f"{len(self.failed)} branch(es) did not succeed",
            duration_ms=self.duration_ms,
        )


async def run_parallel(
    branches: dict[str, Any],
    state: WorkflowState,
    *,
    worker: Any,
    node_id: str,
    mode: JoinMode = JoinMode.ALL,
    max_branches: int = MAX_PARALLEL_BRANCHES,
    timeout_seconds: float | None = None,
) -> ParallelOutcome:
    """Runs branches concurrently and joins them.

    Args:
        branches: Branch id to branch configuration.
        state: The shared input state. Not modified.
        worker: An async callable ``(branch_id, config, state) -> NodeResult``.
        node_id: The parallel node's id, stamped onto the joined result.
        mode: How to join.
        max_branches: Fan-out ceiling.
        timeout_seconds: Wall-clock ceiling for the whole step.

    Returns:
        A :class:`ParallelOutcome`.

    Raises:
        InvalidRequestError: If there are no branches or too many.
    """
    if not branches:
        raise InvalidRequestError(
            "a parallel step requires at least one branch",
            details={"node_id": node_id},
        )
    if len(branches) > max_branches:
        raise InvalidRequestError(
            "parallel step exceeds the fan-out ceiling",
            details={
                "node_id": node_id,
                "count": len(branches),
                "limit": max_branches,
            },
        )

    started = time.monotonic()
    names = tuple(branches)

    async def one(branch_id: str) -> BranchOutcome:
        config = branches[branch_id]
        try:
            result = await worker(branch_id, config, state)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return BranchOutcome(
                branch_id=branch_id,
                result=NodeResult(
                    node_id=node_id,
                    status=NodeStatus.FAILED,
                    error=type(error).__name__,
                ),
            )
        return BranchOutcome(branch_id=branch_id, result=result)

    tasks = [asyncio.create_task(one(name), name=f"branch:{name}") for name in names]

    cancelled = False
    if timeout_seconds is not None:
        _done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
        if pending:
            # Cancel every sibling, not just the stragglers: leaving branches
            # running after the step has given up would orphan work that keeps
            # consuming budget.
            for task in tasks:
                task.cancel()

    try:
        gathered: list[Any] = list(await asyncio.gather(*tasks, return_exceptions=True))
    except asyncio.CancelledError:
        cancelled = True
        for task in tasks:
            task.cancel()
        gathered = list(await asyncio.gather(*tasks, return_exceptions=True))

    outcomes: list[BranchOutcome] = []
    for name, result in zip(names, gathered, strict=False):
        if isinstance(result, BranchOutcome):
            outcomes.append(result)
        elif isinstance(result, BaseException):
            outcomes.append(
                BranchOutcome(
                    branch_id=name,
                    result=NodeResult(
                        node_id=node_id,
                        status=NodeStatus.FAILED,
                        error=type(result).__name__,
                    ),
                )
            )
        else:
            outcomes.append(
                BranchOutcome(
                    branch_id=name,
                    result=NodeResult(node_id=node_id, status=NodeStatus.SKIPPED),
                )
            )

    duration = int((time.monotonic() - started) * 1000)
    _LOGGER.info(
        "parallel step finished",
        extra={
            "event": "workflow.parallel",
            "node_id": node_id,
            "branches": len(outcomes),
            "succeeded": len([o for o in outcomes if o.result.succeeded]),
            "duration_ms": duration,
        },
    )
    return ParallelOutcome(
        outcomes=tuple(outcomes),
        mode=mode,
        cancelled=cancelled,
        duration_ms=duration,
    )
