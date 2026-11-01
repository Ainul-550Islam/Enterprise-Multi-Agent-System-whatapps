"""Retention and deletion policy.

Retention is only real if something enforces it. This module turns a policy
into a decision, so that the thing which actually deletes records has a single
place to ask "should this still exist?".
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.memory.models import MemoryRecord, MemoryScope, Retention


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Per-scope retention, applied when a record does not specify its own.

    Attributes:
        task_scope: Task-local records; deleted at execution end by default.
        episodic_days: Days to keep episodic records. ``None`` keeps forever.
        durable_days: Days to keep durable records. ``None`` keeps forever.
        semantic_days: Days to keep semantic records. ``None`` keeps forever.
    """

    episodic_days: int | None = 30
    durable_days: int | None = None
    semantic_days: int | None = 90

    def __post_init__(self) -> None:
        for field_name in ("episodic_days", "durable_days", "semantic_days"):
            value = getattr(self, field_name)
            if value is not None and value <= 0:
                raise ConfigurationError(
                    "retention period must be positive",
                    details={"field": field_name, "value": value},
                )

    def days_for(self, scope: MemoryScope) -> int | None:
        """Retention period for a scope, in days."""
        mapping = {
            MemoryScope.TASK: 0,
            MemoryScope.EPISODIC: self.episodic_days,
            MemoryScope.DURABLE: self.durable_days,
            MemoryScope.SEMANTIC: self.semantic_days,
        }
        return mapping.get(scope)

    def retention_for(self, scope: MemoryScope) -> Retention:
        """Builds a :class:`Retention` for a scope.

        Task-local records are marked for deletion at execution end rather than
        given a timestamp, because their lifetime is tied to the execution and
        not to the clock.
        """
        days = self.days_for(scope)
        if scope is MemoryScope.TASK or days == 0:
            return Retention(delete_on_execution_end=True)
        if days is None:
            return Retention()
        return Retention(retain_until_epoch_ms=_days_from_now(days))

    def apply(self, record: MemoryRecord) -> MemoryRecord:
        """Returns the record with the policy's retention applied.

        A record that already carries an explicit retention keeps it: an
        explicit decision by the writer outranks a default.
        """
        if record.retention.retain_until_epoch_ms is not None:
            return record
        if record.retention.delete_on_execution_end:
            return record

        from dataclasses import replace

        return replace(record, retention=self.retention_for(record.scope))


def _days_from_now(days: int, *, now_epoch_ms: int | None = None) -> int:
    """Epoch milliseconds ``days`` from now."""
    now = now_epoch_ms if now_epoch_ms is not None else int(time.time() * 1000)
    return now + int(days * 24 * 60 * 60 * 1000)


def should_delete(
    record: MemoryRecord,
    *,
    now_epoch_ms: int | None = None,
) -> bool:
    """Whether a record has outlived its retention."""
    return record.is_expired(now_epoch_ms=now_epoch_ms)


def task_records_for_execution(
    records: list[MemoryRecord],
    execution_id: str,
) -> list[MemoryRecord]:
    """Task-local records belonging to one execution."""
    return [
        record
        for record in records
        if record.scope is MemoryScope.TASK and record.execution_id == execution_id
    ]


def describe_retention(record: MemoryRecord) -> str:
    """Human-readable retention, for an audit trail or a deletion request."""
    if record.retention.delete_on_execution_end:
        return "deleted at execution end"
    if record.retention.retain_until_epoch_ms is None:
        return "retained indefinitely"
    return f"retained until epoch {record.retention.retain_until_epoch_ms} ms"
