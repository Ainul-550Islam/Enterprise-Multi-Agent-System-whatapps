"""PostgresMemoryStore: tenant isolation, TTL, role confinement, MemoryStore."""

from __future__ import annotations

from typing import Any

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError, RetrievalError
from mas_orchestrator.memory.backends import MemoryStore
from mas_orchestrator.memory.models import (
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Retention,
    Sensitivity,
)
from mas_orchestrator.memory.postgres_store import (
    REQUIRED_ROLE,
    PostgresMemoryStore,
    _assert_memory_session_authority,
    _decode_row,
    _enter_memory_tenant_scope,
)


class _Cursor:
    def __init__(
        self, row: tuple[Any, ...] | None = None, rows: list[tuple[Any, ...]] | None = None
    ):
        self._row = row
        self._rows = rows or ([] if row is None else [row])
        self.rowcount = 1 if (row is not None or rows) else 0

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self._row

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _Connection:
    def __init__(self, identity: tuple[Any, ...] | None = None) -> None:
        self.statements: list[str] = []
        self.params: list[Any] = []
        self.identity = identity or ("app", "app", True, False)

    def transaction(self) -> Any:
        connection = self

        class _Tx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *args: object) -> None:
                return None

        del connection
        return _Tx()

    async def execute(self, statement: str, parameters: Any = None) -> _Cursor:
        self.statements.append(statement)
        self.params.append(parameters)
        if "session_user" in statement:
            return _Cursor(row=self.identity)
        return _Cursor(row=None, rows=[])


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection_obj = connection

    def connection(self) -> Any:
        pool = self

        class _Ctx:
            async def __aenter__(self) -> _Connection:
                return pool.connection_obj

            async def __aexit__(self, *args: object) -> None:
                return None

        return _Ctx()


def _record(*, tenant: str = "11111111-1111-1111-1111-111111111111") -> MemoryRecord:
    return MemoryRecord(
        record_id="mem-1",
        tenant_id=tenant,
        scope=MemoryScope.DURABLE,
        content="a durable fact",
        sensitivity=Sensitivity.INTERNAL,
        retention=Retention(retain_until_epoch_ms=1_800_000_000_000),
        agent_id="agent-1",
        created_epoch_ms=1_700_000_000_000,
    )


def test_store_satisfies_the_memory_store_protocol() -> None:
    store = PostgresMemoryStore(_Pool(_Connection()))
    assert isinstance(store, MemoryStore)


def test_required_role_is_mas_memory() -> None:
    assert REQUIRED_ROLE == "mas_memory"


async def test_put_sets_local_role_and_tenant_guc() -> None:
    connection = _Connection()
    store = PostgresMemoryStore(_Pool(connection))
    await store.put(_record())
    assert any("SET LOCAL ROLE mas_memory" in item for item in connection.statements)
    assert any("set_config('app.current_tenant'" in item for item in connection.statements)
    tenant_params = [
        item for item in connection.params if item == ("11111111-1111-1111-1111-111111111111",)
    ]
    assert tenant_params


async def test_query_binds_the_caller_tenant() -> None:
    connection = _Connection()
    store = PostgresMemoryStore(_Pool(connection))
    await store.query(
        MemoryQuery(
            tenant_id="11111111-1111-1111-1111-111111111111",
            scopes=(MemoryScope.DURABLE,),
            text="",
            limit=10,
        )
    )
    assert any("SET LOCAL ROLE mas_memory" in item for item in connection.statements)


async def test_purge_expired_uses_ttl_predicate() -> None:
    connection = _Connection()
    store = PostgresMemoryStore(_Pool(connection))
    await store.purge_expired("11111111-1111-1111-1111-111111111111")
    ttl_sql = [item for item in connection.statements if "retain_until_epoch_ms" in item]
    assert ttl_sql
    assert "tenant_id" in ttl_sql[0]


async def test_identity_probe_rejects_bypassrls() -> None:
    connection = _Connection(identity=("postgres", "postgres", True, True))
    with pytest.raises(ConfigurationError, match="non-privileged"):
        await _assert_memory_session_authority(connection)


async def test_identity_probe_rejects_missing_set_role() -> None:
    connection = _Connection(identity=("app", "app", False, False))
    with pytest.raises(ConfigurationError, match="mas_memory"):
        await _assert_memory_session_authority(connection)


async def test_enter_scope_uses_set_local() -> None:
    connection = _Connection()
    await _enter_memory_tenant_scope(connection, "11111111-1111-1111-1111-111111111111")
    assert connection.statements[0] == "SET LOCAL ROLE mas_memory"


def test_decode_refuses_cross_tenant_rows() -> None:
    record = _record()
    row = (
        record.record_id,
        "22222222-2222-2222-2222-222222222222",
        record.scope.value,
        record.content,
        record.sensitivity.value,
        record.retention.retain_until_epoch_ms,
        record.retention.delete_on_execution_end,
        record.agent_id,
        record.execution_id,
        record.task_id,
        record.created_epoch_ms,
        {},
        record.redacted,
    )
    with pytest.raises(RetrievalError, match="does not match the bound tenant"):
        _decode_row(row, record.tenant_id)
