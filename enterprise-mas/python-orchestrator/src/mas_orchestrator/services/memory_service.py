"""Memory service.

The manager stores and retrieves; this service decides what an *execution* is
allowed to see and remember. Two rules shape it.

**Recall is fenced.** Everything returned here is marked as recalled data
before it goes near a prompt, because memory written during an earlier
execution may contain text that came from a tool or a document. Storing
something does not make it trusted.

**Durable writes are screened.** :meth:`MemoryService.remember_fact` refuses
content that looks like an instruction. That check lives in
:mod:`~mas_orchestrator.memory.long_term` and is applied here because this is
the boundary an agent actually calls; relying on the agent to remember to
screen is not a control.

The service also owns the end-of-execution cleanup. Task-local memory that
outlives its execution is a slow leak of other people's data, and it is the
kind of leak nobody notices because each individual record is harmless.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.memory.compaction import compact
from mas_orchestrator.memory.long_term import build_durable_memory
from mas_orchestrator.memory.models import (
    MemoryHit,
    MemoryRecord,
    MemoryScope,
    Sensitivity,
)
from mas_orchestrator.memory.short_term import build_task_memory

if TYPE_CHECKING:
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.memory.manager import MemoryManager

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RecalledContext:
    """Memory prepared for a prompt.

    Attributes:
        text: The fenced, rendered memory. Empty when nothing was recalled.
        hit_count: How many records contributed.
        compacted: Whether the set was reduced to fit.
        dropped: How many records were dropped by compaction.
    """

    text: str
    hit_count: int
    compacted: bool = False
    dropped: int = 0

    @property
    def empty(self) -> bool:
        """Whether anything was recalled."""
        return self.hit_count == 0


class MemoryService:
    """Execution-facing memory operations."""

    def __init__(self, manager: MemoryManager) -> None:
        self._manager = manager

    @property
    def manager(self) -> MemoryManager:
        """The underlying manager."""
        return self._manager

    async def remember_task(
        self,
        content: str,
        context: ExecutionContext,
        *,
        record_id: str,
        sensitivity: Sensitivity = Sensitivity.INTERNAL,
    ) -> MemoryRecord:
        """Records working memory for the current execution.

        Automatically scoped to the execution and marked for deletion when it
        ends, so a caller cannot create working memory that outlives the run.
        """
        record = build_task_memory(
            tenant_id=context.tenant_id,
            execution_id=context.execution_id,
            content=content,
            record_id=record_id,
            agent_id=context.agent_id,
            task_id=context.task_id,
            sensitivity=sensitivity,
        )
        # The record is built here so its execution binding and retention
        # policy are validated first; the write goes through the manager, which
        # stamps the tenant from the authoritative context.
        await self._manager.remember(
            content,
            context,
            scope=MemoryScope.TASK,
            sensitivity=sensitivity,
            retention=record.retention,
        )
        return record

    async def remember_fact(
        self,
        content: str,
        context: ExecutionContext,
        *,
        record_id: str,
        sensitivity: Sensitivity = Sensitivity.INTERNAL,
    ) -> MemoryRecord:
        """Records a durable fact, screened for injection first.

        Raises:
            PromptInjectionDetectedError: If the content looks like an
                instruction. Durable memory is replayed into every future
                execution, so an injected instruction would persist.
        """
        record = build_durable_memory(
            tenant_id=context.tenant_id,
            content=content,
            record_id=record_id,
            agent_id=context.agent_id,
            sensitivity=sensitivity,
        )
        await self._manager.remember(
            content,
            context,
            scope=MemoryScope.DURABLE,
            sensitivity=sensitivity,
        )
        return record

    async def recall(
        self,
        context: ExecutionContext,
        *,
        scopes: tuple[MemoryScope, ...] = (MemoryScope.TASK,),
        text: str = "",
        limit: int = 20,
        include_sensitive: bool = False,
    ) -> list[MemoryHit]:
        """Recalls memory visible to this execution.

        ``include_sensitive`` defaults to ``False``: sensitive records are
        hidden unless the caller explicitly asks, because the common case is
        building a prompt and the common mistake is putting a credential in it.
        """
        if not context.tenant_id.strip():
            raise InvalidRequestError("a tenant is required", details={"field": "tenant_id"})
        return await self._manager.recall(
            context,
            scopes=scopes,
            text=text,
            limit=limit,
            include_sensitive=include_sensitive,
        )

    async def build_context(
        self,
        context: ExecutionContext,
        *,
        scopes: tuple[MemoryScope, ...] = (MemoryScope.TASK, MemoryScope.DURABLE),
        text: str = "",
        limit: int = 20,
        budget_chars: int = 16_000,
    ) -> RecalledContext:
        """Recalls memory and renders it for a prompt, compacted to fit.

        Args:
            context: Authoritative execution context.
            scopes: Which lifetimes to search.
            text: Query text.
            limit: Record ceiling.
            budget_chars: Character budget for the rendered result.

        Returns:
            A :class:`RecalledContext`.
        """
        hits = await self.recall(context, scopes=scopes, text=text, limit=limit)
        if not hits:
            return RecalledContext(text="", hit_count=0)

        reduction = compact([hit.record for hit in hits], budget_chars=budget_chars)
        rendered = self._manager.render_context(
            [hit for hit in hits if hit.record.record_id in _kept_ids(reduction.kept)]
        )
        return RecalledContext(
            text=rendered,
            hit_count=len(reduction.kept),
            compacted=reduction.lossy,
            dropped=reduction.dropped,
        )

    async def forget(self, context: ExecutionContext, record_id: str) -> bool:
        """Deletes one record in this execution's tenant."""
        return await self._manager.forget(context, record_id)

    async def close_execution(self, context: ExecutionContext) -> int:
        """Deletes task-local memory for a finished execution.

        Returns:
            How many records were removed.
        """
        removed = await self._manager.forget_execution(context)
        if removed:
            _LOGGER.info(
                "task memory released",
                extra={
                    "event": "memory.execution_released",
                    "execution_id": context.execution_id,
                    "records": removed,
                },
            )
        return removed


def _kept_ids(records: tuple[MemoryRecord, ...]) -> set[str]:
    """The ids of records kept by compaction."""
    return {record.record_id for record in records}
