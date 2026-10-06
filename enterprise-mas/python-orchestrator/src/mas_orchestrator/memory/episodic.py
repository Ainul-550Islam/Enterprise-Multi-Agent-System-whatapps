"""Episodic memory.

Episodic memory records what happened in a run: the steps taken, the tools
called, what worked and what did not. It is the scope that makes an agent able
to say "last time this failed" without having to be told.

Its defining property is that it is **scoped to the run it describes**. An
episode written by execution A is only meaningful to execution A, and exposing
it to execution B would let one tenant's workflow read another's — or, more
commonly, would let two concurrent runs of the same workflow read each other's
intermediate state and confuse themselves.

So :func:`build_episode` requires an execution id and
:func:`episode_query` filters on it. There is no path that recalls episodes
across executions.
"""

from __future__ import annotations

import logging
import time
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.memory.models import (
    MAX_MEMORY_CONTENT_CHARS,
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Sensitivity,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: How many episodes one execution may accumulate. Bounded because an episode
#: log is only useful while it is small enough to be read into a prompt.
MAX_EPISODES_PER_EXECUTION: Final[int] = 200


def validate_summary(summary: str) -> str:
    """Checks an episode summary.

    Raises:
        InvalidRequestError: If it is empty or oversized.
    """
    trimmed = (summary or "").strip()
    if not trimmed:
        raise InvalidRequestError(
            "an episodic memory must not be empty", details={"field": "content"}
        )
    if len(trimmed) > MAX_MEMORY_CONTENT_CHARS:
        raise InvalidRequestError(
            "episodic memory exceeds the maximum size",
            details={"limit": MAX_MEMORY_CONTENT_CHARS, "length": len(trimmed)},
        )
    return trimmed


def build_episode(
    *,
    tenant_id: str,
    execution_id: str,
    content: str,
    record_id: str,
    step_index: int | None = None,
    agent_id: str | None = None,
    task_id: str | None = None,
    sensitivity: Sensitivity = Sensitivity.INTERNAL,
    occurred_epoch_ms: int = 0,
) -> MemoryRecord:
    """Builds an episodic memory record.

    The execution id is mandatory. An episode without one has no owner, and
    would be readable by every execution in the tenant.
    """
    if not execution_id.strip():
        raise InvalidRequestError(
            "an episodic memory must name the execution it describes",
            details={"field": "execution_id"},
        )

    return MemoryRecord(
        record_id=record_id,
        tenant_id=tenant_id,
        scope=MemoryScope.EPISODIC,
        content=validate_summary(content),
        sensitivity=sensitivity,
        agent_id=agent_id,
        execution_id=execution_id,
        task_id=task_id,
        created_epoch_ms=occurred_epoch_ms or int(time.time() * 1000),
        metadata=({"step_index": step_index} if step_index is not None else {}),
    )


def episode_query(
    *,
    tenant_id: str,
    execution_id: str,
    text: str = "",
    limit: int = 20,
) -> MemoryQuery:
    """A query that can only return episodes from one execution.

    Returns:
        A :class:`MemoryQuery` pinned to the scope and execution.
    """
    if not execution_id.strip():
        raise InvalidRequestError(
            "episode queries must name an execution: recalling episodes across "
            "executions would leak one run's state into another",
            details={"field": "execution_id"},
        )
    return MemoryQuery(
        tenant_id=tenant_id,
        scopes=(MemoryScope.EPISODIC,),
        text=text,
        execution_id=execution_id,
        limit=max(1, min(limit, MAX_EPISODES_PER_EXECUTION)),
    )
