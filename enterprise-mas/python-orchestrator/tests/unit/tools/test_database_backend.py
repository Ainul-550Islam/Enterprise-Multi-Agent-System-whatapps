"""PostgresReadOnlyBackend: read-only txn, timeout, row/byte cap, schema allowlist."""

from __future__ import annotations

from typing import Any

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError, ToolFailedError
from mas_orchestrator.tools.backends.database import PostgresReadOnlyBackend


class _FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]], names: list[str]) -> None:
        self._rows = rows
        self.description = [type("Col", (), {"name": name})() for name in names]

    async def fetchmany(self, count: int) -> list[tuple[Any, ...]]:
        return self._rows[:count]

    async def close(self) -> None:
        return None


class _FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]], names: list[str]) -> None:
        self.statements: list[str] = []
        self._rows = rows
        self._names = names

    def transaction(self) -> Any:
        class _Tx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *args: object) -> None:
                return None

        return _Tx()

    async def execute(self, statement: str, parameters: list[Any] | None = None) -> _FakeCursor:
        self.statements.append(statement)
        if statement.strip().upper().startswith("SET"):
            return _FakeCursor([], [])
        return _FakeCursor(self._rows, self._names)


class _FakePool:
    def __init__(self, rows: list[tuple[Any, ...]], names: list[str]) -> None:
        self.connection_obj = _FakeConnection(rows, names)

    def connection(self) -> Any:
        pool = self

        class _Ctx:
            async def __aenter__(self) -> _FakeConnection:
                return pool.connection_obj

            async def __aexit__(self, *args: object) -> None:
                return None

        return _Ctx()


async def test_select_is_read_only_with_statement_timeout_and_row_cap() -> None:
    pool = _FakePool([(1, "a"), (2, "b"), (3, "c")], ["id", "name"])
    backend = PostgresReadOnlyBackend(pool=pool)
    rows = await backend.fetch(
        "select id, name from t",
        parameters=[],
        limit=2,
        timeout_seconds=1.5,
    )
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert pool.connection_obj.statements[0] == "SET TRANSACTION READ ONLY"
    assert pool.connection_obj.statements[1] == "SET LOCAL statement_timeout = 1500"


async def test_schema_allowlist_refuses_foreign_schema() -> None:
    backend = PostgresReadOnlyBackend(pool=_FakePool([], []), allowed_schemas=frozenset({"app"}))
    with pytest.raises(ToolFailedError, match="schema"):
        await backend.fetch(
            "select * from other.secrets",
            parameters=[],
            limit=1,
            timeout_seconds=1.0,
        )


async def test_schema_allowlist_permits_named_schema() -> None:
    pool = _FakePool([(1,)], ["id"])
    backend = PostgresReadOnlyBackend(pool=pool, allowed_schemas=frozenset({"app"}))
    rows = await backend.fetch(
        "select id from app.items",
        parameters=[],
        limit=1,
        timeout_seconds=1.0,
    )
    assert rows == [{"id": 1}]


async def test_byte_cap_refuses_an_oversized_result() -> None:
    pool = _FakePool([("x" * 100,)], ["blob"])
    backend = PostgresReadOnlyBackend(pool=pool, max_response_bytes=16)
    with pytest.raises(ToolFailedError, match="byte cap"):
        await backend.fetch("select blob from t", parameters=[], limit=10, timeout_seconds=1.0)


def test_writes_are_refused() -> None:
    backend = PostgresReadOnlyBackend(pool=_FakePool([], []))
    with pytest.raises(ToolFailedError):
        backend._refuse_unsafe("insert into t values (1)")


async def test_zero_timeout_is_refused() -> None:
    backend = PostgresReadOnlyBackend(pool=_FakePool([], []))
    with pytest.raises(InvalidRequestError, match="timeout"):
        await backend.fetch("select 1", parameters=[], limit=1, timeout_seconds=0)
