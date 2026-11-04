"""Memory manager: the interface agents actually use.

The manager is deliberately thin. Its jobs are to stamp authoritative tenant
context onto every write, refuse reads that would cross a tenant boundary, and
route sensitive content through a redaction hook before it becomes visible.

It does not decide *what* to remember — that is the agent's job — and it does
not provide semantic retrieval. Semantic retrieval is RAG, and conflating the
two is how "memory" ends up meaning "a vector store with no provenance".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import ConfigurationError, MemoryBackendError
from mas_orchestrator.memory.models import (
    MemoryHit,
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Retention,
    Sensitivity,
    new_record_id,
)

if TYPE_CHECKING:
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.memory.backends import MemoryStore

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_CONTEXT_RECORDS: Final[int] = 32


@runtime_checkable
class RedactionHook(Protocol):
    """Transforms content before it is stored or returned."""

    def redact(self, content: str) -> str:
        """Returns a redacted version of ``content``."""
        ...


class NoRedaction:
    """A hook that changes nothing.

    Acceptable only for content already classified below the redaction
    threshold. It exists so the hook slot is always filled rather than
    optional, which removes a branch that could silently skip redaction.
    """

    def redact(self, content: str) -> str:
        """Returns the input unchanged."""
        return content


@dataclass(frozen=True, slots=True)
class MemoryConfig:
    """Manager settings.

    Attributes:
        default_retention: Applied to records that specify none.
        max_context_records: Ceiling on records injected into a prompt.
    """

    default_retention: Retention = field(default_factory=Retention)
    max_context_records: int = MAX_CONTEXT_RECORDS

    def __post_init__(self) -> None:
        if self.max_context_records < 1:
            raise ConfigurationError(
                "max_context_records must be at least 1",
                details={"field": "max_context_records"},
            )


class MemoryManager:
    """Reads and writes memory for one tenant at a time."""

    def __init__(
        self,
        store: MemoryStore,
        *,
        redaction: RedactionHook | None = None,
        config: MemoryConfig | None = None,
    ) -> None:
        self._store = store
        self._redaction = redaction or NoRedaction()
        self._config = config or MemoryConfig()

    async def remember(
        self,
        content: str,
        context: ExecutionContext,
        *,
        scope: MemoryScope = MemoryScope.TASK,
        sensitivity: Sensitivity = Sensitivity.INTERNAL,
        retention: Retention | None = None,
        metadata: dict[str, object] | None = None,
        record_id: str | None = None,
    ) -> MemoryRecord:
        """Stores a memory.

        The tenant, execution, task and agent are taken from the authoritative
        execution context. A caller cannot pass them in, because a caller that
        can name a tenant can write into another tenant's memory.

        Returns:
            The stored record.

        Raises:
            MemoryBackendError: If the store rejects the write.
        """
        effective_retention = retention or self._config.default_retention
        if scope is MemoryScope.TASK:
            effective_retention = Retention(delete_on_execution_end=True)

        applied_redaction = False
        if sensitivity.requires_redaction:
            if isinstance(self._redaction, NoRedaction):
                raise ConfigurationError("sensitive memory requires a configured redaction hook")
            content = self._redaction.redact(content)
            applied_redaction = True

        record = MemoryRecord(
            record_id=new_record_id() if record_id is None else record_id,
            tenant_id=context.tenant_id,
            scope=scope,
            content=content,
            sensitivity=sensitivity,
            retention=effective_retention,
            agent_id=context.agent_id,
            execution_id=context.execution_id,
            task_id=context.task_id,
            metadata=dict(metadata or {}),
            redacted=applied_redaction,
        )

        try:
            create = getattr(self._store, "create", None)
            if not callable(create):
                raise ConfigurationError("memory store does not support atomic creation")
            if not await create(record):
                raise ConfigurationError("memory record id already exists")
        except ConfigurationError as error:
            raise MemoryBackendError(
                "memory write was rejected",
                details={"scope": scope.value, "reason": str(error)},
            ) from error

        _LOGGER.info(
            "memory written",
            extra={
                "event": "memory.write",
                "record_id": record.record_id,
                "scope": scope.value,
                "sensitivity": sensitivity.value,
            },
        )
        return record

    async def recall(
        self,
        context: ExecutionContext,
        *,
        scopes: tuple[MemoryScope, ...] = (MemoryScope.TASK,),
        text: str = "",
        limit: int | None = None,
        include_sensitive: bool = False,
    ) -> list[MemoryHit]:
        """Retrieves memory for this execution's tenant.

        The tenant comes from the context, never from the arguments.
        """
        max_results = min(
            limit if limit is not None else self._config.max_context_records,
            self._config.max_context_records,
        )
        if max_results < 1:
            raise ConfigurationError("memory recall limit must be positive")
        hits: list[MemoryHit] = []
        try:
            # Lifetime buckets have different authorization scopes. Query them
            # separately so one global agent/execution filter never widens the
            # task-local or same-agent episodic boundaries.
            for scope in dict.fromkeys(scopes):
                remaining = max_results - len(hits)
                if not remaining:
                    break
                query = MemoryQuery(
                    tenant_id=context.tenant_id,
                    scopes=(scope,),
                    text=text,
                    agent_id=context.agent_id if scope is MemoryScope.EPISODIC else None,
                    execution_id=context.execution_id if scope is MemoryScope.TASK else None,
                    limit=remaining,
                    include_sensitive=include_sensitive,
                )
                for hit in await self._store.query(query):
                    record = hit.record
                    if (
                        record.tenant_id != context.tenant_id
                        or record.scope is not scope
                        or (
                            scope is MemoryScope.TASK
                            and record.execution_id != context.execution_id
                        )
                        or (scope is MemoryScope.EPISODIC and record.agent_id != context.agent_id)
                        or record.is_expired()
                        or (not include_sensitive and record.needs_redaction())
                    ):
                        continue
                    hits.append(hit)
                    if len(hits) >= max_results:
                        return hits
            return hits
        except ConfigurationError as error:
            raise MemoryBackendError(
                "memory read failed", details={"reason": str(error)}
            ) from error

    async def forget(self, context: ExecutionContext, record_id: str) -> bool:
        """Atomically deletes a record visible in this execution's scope."""
        delete = getattr(self._store, "delete_visible", None)
        if not callable(delete):
            raise MemoryBackendError("memory store does not support scoped deletion")
        result = await delete(
            context.tenant_id,
            record_id,
            execution_id=context.execution_id,
            agent_id=context.agent_id,
        )
        if not isinstance(result, bool):
            raise MemoryBackendError("memory store returned an invalid deletion result")
        return result

    async def forget_execution(self, context: ExecutionContext) -> int:
        """Deletes task-local records for the current execution.

        Called when an execution ends. Task-local memory that outlives its
        execution is a leak, and one that is easy to miss because the records
        are individually harmless.
        """
        close = getattr(self._store, "close_execution", None)
        if not callable(close):
            raise MemoryBackendError("memory store does not support atomic execution cleanup")
        try:
            removed = await close(context.tenant_id, context.execution_id)
        except ConfigurationError as error:
            raise MemoryBackendError("memory execution cleanup failed") from error
        if not isinstance(removed, int) or isinstance(removed, bool) or removed < 0:
            raise MemoryBackendError("memory store returned an invalid cleanup result")
        if removed:
            _LOGGER.info(
                "task memory released",
                extra={
                    "event": "memory.forget_execution",
                    "execution_id": context.execution_id,
                    "count": removed,
                },
            )
        return removed

    async def purge_tenant(self, tenant_id: str) -> int:
        """Deletes every record for a tenant.

        Used to honour a deletion request. Deletion that cannot delete
        everything is not deletion.
        """
        return await self._store.purge_tenant(tenant_id)

    def render_context(self, hits: list[MemoryHit]) -> str:
        """Renders retrieved memory for inclusion in a prompt.

        The output is fenced and labelled as recalled data: memory written
        during an earlier execution may contain content that came from a tool
        or a document, which means it is untrusted no matter how trustworthy
        the storage layer is.
        """
        if not hits:
            return ""

        from mas_orchestrator.prompts.renderer import fence

        lines = []
        for index, hit in enumerate(hits, start=1):
            scope = hit.record.scope.value
            lines.append(f"[{index}] ({scope}) {hit.record.content}")

        return fence("\n".join(lines), label="memory")
