"""Unit tests for the PostgreSQL memory-plane store that need no database."""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    RetrievalError,
)
from mas_orchestrator.memory.models import MemoryRecord, MemoryScope, Sensitivity
from mas_orchestrator.memory.postgres_store import (
    REQUIRED_ROLE,
    PostgresMemoryStoreConfig,
    _decode_row,
    _json_dumps,
)


def _row(record: MemoryRecord, *, tenant_id: str | None = None) -> tuple:
    """Shapes a driver row the way psycopg would return one."""
    return (
        record.record_id,
        tenant_id or record.tenant_id,
        record.scope.value,
        record.content,
        record.sensitivity.value,
        record.retention.retain_until_epoch_ms,
        record.retention.delete_on_execution_end,
        record.agent_id,
        record.execution_id,
        record.task_id,
        record.created_epoch_ms,
        dict(record.metadata),
        record.redacted,
    )


def test_config_rejects_inverted_pool_bounds() -> None:
    with pytest.raises(ConfigurationError, match="pool bounds are invalid"):
        PostgresMemoryStoreConfig(min_size=4, max_size=2)


def test_config_rejects_an_empty_role() -> None:
    with pytest.raises(ConfigurationError, match="role must not be empty"):
        PostgresMemoryStoreConfig(database_role="  ")


def test_required_role_is_the_memory_plane_role() -> None:
    assert REQUIRED_ROLE == "mas_memory"


def test_decode_row_round_trips_a_record() -> None:
    record = MemoryRecord(
        record_id="mem-abc",
        tenant_id="11111111-1111-1111-1111-111111111111",
        scope=MemoryScope.DURABLE,
        content="a durable fact",
        sensitivity=Sensitivity.INTERNAL,
        agent_id="agent-1",
        execution_id="exec-1",
        created_epoch_ms=1_700_000_000_000,
        metadata={"origin": "test"},
        redacted=True,
    )
    decoded = _decode_row(_row(record), record.tenant_id)
    assert decoded == record


def test_decode_row_refuses_a_foreign_tenant_row() -> None:
    record = MemoryRecord(
        record_id="mem-abc",
        tenant_id="11111111-1111-1111-1111-111111111111",
        scope=MemoryScope.DURABLE,
        content="a durable fact",
        agent_id="agent-1",
    )
    with pytest.raises(RetrievalError, match="does not match the bound tenant"):
        _decode_row(
            _row(record, tenant_id="22222222-2222-2222-2222-222222222222"),
            record.tenant_id,
        )


def test_decode_row_rejects_an_unknown_scope() -> None:
    record = MemoryRecord(
        record_id="mem-abc",
        tenant_id="11111111-1111-1111-1111-111111111111",
        scope=MemoryScope.DURABLE,
        content="a durable fact",
        agent_id="agent-1",
    )
    row = _row(record)
    corrupted = (*row[:2], "not-a-scope", *row[3:])
    with pytest.raises(InvalidRequestError, match="malformed"):
        _decode_row(corrupted, record.tenant_id)


def test_json_dumps_is_compact_and_ascii_safe() -> None:
    assert _json_dumps({"a": "b"}) == '{"a":"b"}'
