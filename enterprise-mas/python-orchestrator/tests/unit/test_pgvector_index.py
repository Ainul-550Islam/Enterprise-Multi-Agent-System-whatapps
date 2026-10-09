"""Security-boundary tests for the durable pgvector index.

The RAG database URL is a secret reference, so configuration cannot prove which
PostgreSQL role actually authenticated. These tests pin the server-side identity
probe and the per-transaction `SET LOCAL ROLE` fence that prevents a broad DSN
from silently becoming a Rust-table-capable vector-plane connection.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.rag.pgvector_index import (
    _RAG_IDENTITY_SQL,
    _SET_LOCAL_RAG_ROLE_SQL,
    PgVectorIndex,
    _enter_rag_tenant_scope,
)

_TENANT_ID = "11111111-1111-4111-8111-111111111111"
_VALID_IDENTITY_ROW = ("rag_runtime_login", "rag_runtime_login", True, False)


@dataclass(slots=True)
class _Cursor:
    """Minimal async cursor returning one configured identity row."""

    row: object | None

    async def fetchone(self) -> object | None:
        """Returns the configured row."""
        return self.row


@dataclass(slots=True)
class _Transaction:
    """No-op async transaction context for the pool double."""

    async def __aenter__(self) -> _Transaction:
        """Enters the synthetic transaction."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> bool:
        """Leaves the synthetic transaction without suppressing errors."""
        del exc_type, exc, traceback
        return False


@dataclass(slots=True)
class _Connection:
    """Connection double recording statements in their execution order."""

    identity_row: object | None
    statements: list[tuple[str, tuple[object, ...] | None]] = field(default_factory=list)

    async def __aenter__(self) -> _Connection:
        """Returns itself for ``async with pool.connection()``."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> bool:
        """Leaves the synthetic connection without suppressing errors."""
        del exc_type, exc, traceback
        return False

    def transaction(self) -> _Transaction:
        """Returns a no-op async transaction context."""
        return _Transaction()

    async def execute(
        self,
        statement: str,
        parameters: tuple[object, ...] | None = None,
    ) -> _Cursor:
        """Records a statement and returns an identity cursor when requested."""
        self.statements.append((statement, parameters))
        if statement == _RAG_IDENTITY_SQL:
            return _Cursor(self.identity_row)
        return _Cursor(None)


@dataclass(slots=True)
class _Pool:
    """Pool double with one deterministic connection."""

    connection_value: _Connection
    opened: bool = False

    async def open(self, *, wait: bool) -> None:
        """Records the startup open request."""
        assert wait
        self.opened = True

    def connection(self) -> _Connection:
        """Returns the sole synthetic connection."""
        return self.connection_value


@pytest.mark.asyncio
async def test_open_rejects_a_superuser_even_when_config_claims_mas_rag() -> None:
    """A textual role setting must not override the DSN's real authority."""
    connection = _Connection(("mas_test_migrator", "mas_test_migrator", True, True))
    index = PgVectorIndex(_Pool(connection), dimensions=1536, database_role="mas_rag")

    with pytest.raises(
        ConfigurationError,
        match="non-privileged and able to assume mas_rag",
    ):
        await index.open()

    assert connection.statements == [(_RAG_IDENTITY_SQL, None)]


@pytest.mark.asyncio
async def test_open_rejects_a_login_that_cannot_assume_the_rag_role() -> None:
    """An ordinary app login cannot access the vector plane by configuration alone."""
    connection = _Connection(("mas_app", "mas_app", False, False))
    index = PgVectorIndex(_Pool(connection), dimensions=1536, database_role="mas_rag")

    with pytest.raises(
        ConfigurationError,
        match="non-privileged and able to assume mas_rag",
    ):
        await index.open()

    assert connection.statements == [(_RAG_IDENTITY_SQL, None)]


@pytest.mark.asyncio
async def test_open_rejects_a_dsn_that_preselects_a_broader_role() -> None:
    """The pool must begin as its dedicated login, not a role from DSN options."""
    connection = _Connection(("rag_runtime_login", "mas_app", True, False))
    index = PgVectorIndex(_Pool(connection), dimensions=1536, database_role="mas_rag")

    with pytest.raises(
        ConfigurationError,
        match="non-privileged and able to assume mas_rag",
    ):
        await index.open()

    assert connection.statements == [(_RAG_IDENTITY_SQL, None)]


@pytest.mark.asyncio
async def test_open_preserves_a_direct_authority_configuration_failure() -> None:
    """A rejected preflight must not degrade into a pool-timeout availability error."""
    connection = _Connection(_VALID_IDENTITY_ROW)
    pool = _Pool(connection)
    probe_calls: list[str] = []

    async def rejected_probe() -> None:
        probe_calls.append("called")
        raise ConfigurationError("RAG login cannot assume mas_rag")

    index = PgVectorIndex(
        pool,
        dimensions=1536,
        database_role="mas_rag",
        authority_probe=rejected_probe,
    )

    with pytest.raises(ConfigurationError, match="cannot assume mas_rag"):
        await index.open()

    assert probe_calls == ["called"]
    assert not pool.opened
    assert connection.statements == []


@pytest.mark.asyncio
async def test_health_check_uses_transaction_local_rag_role_before_table_probe() -> None:
    """A pooled RAG probe activates only the dedicated role for its transaction."""
    connection = _Connection(_VALID_IDENTITY_ROW)
    index = PgVectorIndex(_Pool(connection), dimensions=1536, database_role="mas_rag")

    assert await index.health_check()

    assert connection.statements == [
        (_RAG_IDENTITY_SQL, None),
        ("SELECT 1", None),
        (_SET_LOCAL_RAG_ROLE_SQL, None),
        ("SELECT 1 FROM document_chunks LIMIT 0", None),
    ]


@pytest.mark.asyncio
async def test_tenant_operations_activate_rag_role_before_arming_the_tenant_guc() -> None:
    """Tenant scope cannot be armed while inherited broad privileges remain active."""
    connection = _Connection(_VALID_IDENTITY_ROW)

    await _enter_rag_tenant_scope(connection, _TENANT_ID)

    assert connection.statements == [
        (_SET_LOCAL_RAG_ROLE_SQL, None),
        ("SELECT set_config('app.current_tenant', %s, true)", (_TENANT_ID,)),
    ]
