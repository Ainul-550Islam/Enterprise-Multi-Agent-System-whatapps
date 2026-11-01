"""Memory record types.

Spec §11 requires four distinct kinds of memory, and the distinction matters
because they have completely different lifetimes and trust properties:

* **Task-local** — the working context of one execution. Gone when it ends.
* **Episodic** — what happened in past executions. Useful for "last time we
  tried this".
* **Durable** — facts that outlive any execution, written deliberately.
* **Semantic** — content retrieved by meaning, always backed by a citation.

Every durable record carries authoritative tenant context. That is not a
formality: a memory layer that can return another tenant's data is a data
breach with a friendly interface.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

from mas_orchestrator.contracts.errors import InvalidRequestError

MAX_MEMORY_CONTENT_CHARS: Final[int] = 64_000
MAX_MEMORY_METADATA_KEYS: Final[int] = 64


class MemoryScope(StrEnum):
    """How long a record lives and who may read it.

    * ``TASK`` — one execution only.
    * ``EPISODIC`` — past executions of the same agent.
    * ``DURABLE`` — survives indefinitely until explicitly deleted.
    * ``SEMANTIC`` — retrieved by meaning; always carries a citation.
    """

    TASK = "task"
    EPISODIC = "episodic"
    DURABLE = "durable"
    SEMANTIC = "semantic"

    @property
    def outlives_execution(self) -> bool:
        """Whether the record persists after the execution that wrote it."""
        return self in (
            MemoryScope.EPISODIC,
            MemoryScope.DURABLE,
            MemoryScope.SEMANTIC,
        )


class Sensitivity(StrEnum):
    """Classification driving redaction and retention.

    * ``PUBLIC`` — no special handling.
    * ``INTERNAL`` — business data.
    * ``SENSITIVE`` — redaction hooks apply.
    * ``RESTRICTED`` — redaction required before it may leave the store.
    """

    PUBLIC = "public"
    INTERNAL = "internal"
    SENSITIVE = "sensitive"
    RESTRICTED = "restricted"

    @property
    def requires_redaction(self) -> bool:
        """Whether the value must pass a redaction hook before use."""
        return self in (Sensitivity.SENSITIVE, Sensitivity.RESTRICTED)


@dataclass(frozen=True, slots=True)
class Retention:
    """When a record must be deleted.

    Attributes:
        retain_until_epoch_ms: Expiry, or ``None`` for no automatic expiry.
        delete_on_execution_end: For task-local records.
    """

    retain_until_epoch_ms: int | None = None
    delete_on_execution_end: bool = False

    def __post_init__(self) -> None:
        if self.retain_until_epoch_ms is not None and self.retain_until_epoch_ms <= 0:
            raise InvalidRequestError(
                "retention timestamp must be positive",
                details={"field": "retain_until_epoch_ms"},
            )
        if self.retain_until_epoch_ms is not None and self.delete_on_execution_end:
            raise InvalidRequestError(
                "a record cannot both expire and be deleted at execution end",
                details={"field": "retention"},
            )

    def is_expired(self, *, now_epoch_ms: int | None = None) -> bool:
        """Whether the record has passed its retention deadline."""
        if self.retain_until_epoch_ms is None:
            return False
        now = now_epoch_ms if now_epoch_ms is not None else int(time.time() * 1000)
        return now >= self.retain_until_epoch_ms


@dataclass(frozen=True, slots=True)
class MemoryRecord:
    """One stored memory.

    Attributes:
        record_id: Unique identifier.
        tenant_id: Owning tenant — authoritative, never caller-supplied.
        scope: Lifetime and visibility.
        content: The stored text.
        sensitivity: Classification driving redaction.
        retention: Deletion policy.
        agent_id: Writing agent, when applicable.
        execution_id: Writing execution, when applicable.
        task_id: Writing task, when applicable.
        created_epoch_ms: Creation time.
        metadata: Small, bounded annotations.
        redacted: Whether content has passed a redaction hook.
    """

    record_id: str
    tenant_id: str
    scope: MemoryScope
    content: str
    sensitivity: Sensitivity = Sensitivity.INTERNAL
    retention: Retention = field(default_factory=Retention)
    agent_id: str | None = None
    execution_id: str | None = None
    task_id: str | None = None
    created_epoch_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    redacted: bool = False

    def __post_init__(self) -> None:
        if not self.record_id.strip():
            raise InvalidRequestError(
                "memory record id must not be empty", details={"field": "record_id"}
            )
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "memory record must belong to a tenant", details={"field": "tenant_id"}
            )
        if not self.content.strip():
            raise InvalidRequestError(
                "memory content must not be empty", details={"field": "content"}
            )
        if len(self.content) > MAX_MEMORY_CONTENT_CHARS:
            raise InvalidRequestError(
                "memory content exceeds the maximum size",
                details={"limit": MAX_MEMORY_CONTENT_CHARS},
            )
        if len(self.metadata) > MAX_MEMORY_METADATA_KEYS:
            raise InvalidRequestError(
                "memory metadata has too many keys",
                details={"limit": MAX_MEMORY_METADATA_KEYS},
            )
        if self.scope.outlives_execution and not self.execution_id and not self.agent_id:
            # A record that outlives its execution with no provenance cannot be
            # audited later: nobody could say who wrote it or why.
            raise InvalidRequestError(
                "a persistent memory record must record its origin",
                details={"scope": self.scope.value},
            )

        if not self.created_epoch_ms:
            object.__setattr__(self, "created_epoch_ms", int(time.time() * 1000))

    def belongs_to(self, tenant_id: str) -> bool:
        """Whether this record belongs to a tenant."""
        return self.tenant_id == tenant_id

    def is_expired(self, *, now_epoch_ms: int | None = None) -> bool:
        """Whether retention has elapsed."""
        return self.retention.is_expired(now_epoch_ms=now_epoch_ms)

    def needs_redaction(self) -> bool:
        """Whether content must be redacted before use downstream."""
        return self.sensitivity.requires_redaction and not self.redacted


def new_record_id() -> str:
    """Generates a fresh memory record identifier."""
    return f"mem-{uuid.uuid4().hex}"


@dataclass(frozen=True, slots=True)
class MemoryQuery:
    """A read request against memory.

    Attributes:
        tenant_id: Requester's tenant. Enforced by the store, not honoured
            from the query alone.
        scopes: Which lifetime buckets to search.
        text: Substring or semantic query.
        agent_id: Restrict to one agent.
        execution_id: Restrict to one execution.
        limit: Maximum records to return.
        include_sensitive: Whether unredacted sensitive records may be
            returned. Defaults to ``False``.
    """

    tenant_id: str
    scopes: tuple[MemoryScope, ...] = (MemoryScope.TASK,)
    text: str = ""
    agent_id: str | None = None
    execution_id: str | None = None
    limit: int = 20
    include_sensitive: bool = False

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "memory query must name a tenant", details={"field": "tenant_id"}
            )
        if self.limit < 1:
            raise InvalidRequestError(
                "memory query limit must be positive", details={"field": "limit"}
            )


@dataclass(frozen=True, slots=True)
class MemoryHit:
    """A record returned by a query.

    Attributes:
        record: The record.
        score: Relevance, when a ranking was applied.
        source: How it was found, for the audit trail.
    """

    record: MemoryRecord
    score: float = 1.0
    source: str = "store"

    @property
    def content(self) -> str:
        """The record's content."""
        return self.record.content
