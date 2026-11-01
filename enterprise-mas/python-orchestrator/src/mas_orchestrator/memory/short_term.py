"""Short-term (task-scoped) memory.

Short-term memory is the working set of one execution: intermediate results,
partial answers, the state a multi-step run needs to carry forward. Its
defining property is that it **dies with the execution**.

That is enforced in two places, because either alone can be bypassed:

* :func:`build_task_memory` sets ``delete_on_execution_end`` on the retention
  policy, so the retention sweep removes it.
* :func:`task_query` pins the query to one execution id, so even a record that
  outlived its run cannot be read by a different one.

The second matters more than it looks. A cross-execution leak of short-term
memory is the mechanism by which one tenant's workflow would read another's
in-progress data, which is why the execution id is mandatory rather than
optional.
"""

from __future__ import annotations

import time
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.memory.models import (
    MAX_MEMORY_CONTENT_CHARS,
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Retention,
    Sensitivity,
)

#: Working-set ceiling for one execution. Small, because everything here is
#: destined for a prompt.
MAX_TASK_RECORDS_PER_EXECUTION: Final[int] = 100


def build_task_memory(
    *,
    tenant_id: str,
    execution_id: str,
    content: str,
    record_id: str,
    agent_id: str | None = None,
    task_id: str | None = None,
    sensitivity: Sensitivity = Sensitivity.INTERNAL,
) -> MemoryRecord:
    """Builds a task-scoped record.

    The retention policy always carries ``delete_on_execution_end=True``: a
    caller cannot create working memory that outlives its run by omitting an
    argument.
    """
    if not execution_id.strip():
        raise InvalidRequestError(
            "short-term memory must name the execution it belongs to",
            details={"field": "execution_id"},
        )
    trimmed = (content or "").strip()
    if not trimmed:
        raise InvalidRequestError(
            "short-term memory must not be empty", details={"field": "content"}
        )
    if len(trimmed) > MAX_MEMORY_CONTENT_CHARS:
        raise InvalidRequestError(
            "short-term memory exceeds the maximum size",
            details={"limit": MAX_MEMORY_CONTENT_CHARS, "length": len(trimmed)},
        )

    return MemoryRecord(
        record_id=record_id,
        tenant_id=tenant_id,
        scope=MemoryScope.TASK,
        content=trimmed,
        sensitivity=sensitivity,
        retention=Retention(delete_on_execution_end=True),
        agent_id=agent_id,
        execution_id=execution_id,
        task_id=task_id,
        created_epoch_ms=int(time.time() * 1000),
    )


def task_query(
    *,
    tenant_id: str,
    execution_id: str,
    text: str = "",
    agent_id: str | None = None,
    limit: int = 20,
) -> MemoryQuery:
    """A query pinned to one execution's working set."""
    if not execution_id.strip():
        raise InvalidRequestError(
            "short-term memory queries must name an execution: the working set "
            "of one run must not be readable by another",
            details={"field": "execution_id"},
        )
    return MemoryQuery(
        tenant_id=tenant_id,
        scopes=(MemoryScope.TASK,),
        text=text,
        agent_id=agent_id,
        execution_id=execution_id,
        limit=max(1, min(limit, MAX_TASK_RECORDS_PER_EXECUTION)),
    )


def is_task_scoped(record: MemoryRecord) -> bool:
    """Whether a record is short-term working memory."""
    return record.scope is MemoryScope.TASK and record.retention.delete_on_execution_end


def assert_disposable(record: MemoryRecord) -> None:
    """Refuses a task-scoped record that would survive its execution.

    Raises:
        InvalidRequestError: If the record is task-scoped but not marked for
            deletion at execution end.
    """
    if record.scope is not MemoryScope.TASK:
        return
    if not record.retention.delete_on_execution_end:
        raise InvalidRequestError(
            "task-scoped memory must be deleted when the execution ends",
            details={"record_id": record.record_id},
        )
