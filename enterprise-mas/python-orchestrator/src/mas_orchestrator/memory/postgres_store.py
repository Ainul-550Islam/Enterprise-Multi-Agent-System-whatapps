"""Durable memory store on PostgreSQL — the ``mas_memory`` plane.

:class:`~mas_orchestrator.memory.backends.InProcessMemoryStore` is the only
backend that ships for development. It refuses production because in-process
state is per-worker and per-restart. This module is the production backend: the
same :class:`~mas_orchestrator.memory.backends.MemoryStore` protocol, backed by
the ``memory_records`` table owned by the Rust schema and written only through
the dedicated ``mas_memory`` role.

The boundary is the same one the RAG plane uses (see
:mod:`mas_orchestrator.rag.pgvector_index`): the configured ``database_role``
string is a claim, not evidence, so the real session identity is probed on the
server and must be an unprivileged login that can ``SET ROLE mas_memory``. Every
data transaction then runs ``SET LOCAL ROLE mas_memory`` and binds the
authoritative tenant to ``app.current_tenant``, so the database policy — not the
caller — enforces tenancy.

This module never invents a tenant: the tenant comes from the execution context
and is passed to each call, and the RLS policy refuses a row whose ``tenant_id``
does not match the bound GUC.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    RetrievalError,
)
from mas_orchestrator.memory.models import (
    MemoryHit,
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Retention,
    Sensitivity,
)
from mas_orchestrator.memory.retention import RetentionPolicy

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: The only role permitted to touch ``memory_records``.
REQUIRED_ROLE: Final[str] = "mas_memory"

_SET_LOCAL_MEMORY_ROLE_SQL: Final[str] = "SET LOCAL ROLE mas_memory"

#: Server-side identity probe. ``database_role`` is configuration, not authority:
#: a DSN can still authenticate as a superuser or a broadly privileged runtime
#: login. The final predicate catches an elevated role reachable through
#: membership as well as one that is directly authenticated.
_MEMORY_IDENTITY_SQL: Final[str] = """
    SELECT
        session_user::text,
        current_user::text,
        pg_has_role(session_user, 'mas_memory', 'SET'),
        EXISTS (
            SELECT 1
            FROM pg_roles AS candidate
            WHERE (candidate.rolsuper OR candidate.rolbypassrls)
              AND (
                  pg_has_role(session_user, candidate.oid, 'SET')
                  OR pg_has_role(current_user, candidate.oid, 'SET')
              )
        )
"""

_INSERT_SQL: Final[str] = """
    INSERT INTO memory_records
        (id, tenant_id, scope, content, sensitivity, retain_until_epoch_ms,
         delete_on_execution_end, agent_id, execution_id, task_id,
         created_epoch_ms, metadata, redacted)
    VALUES
        (%(id)s, %(tenant_id)s::uuid, %(scope)s, %(content)s, %(sensitivity)s,
         %(retain_until_epoch_ms)s, %(delete_on_execution_end)s, %(agent_id)s,
         %(execution_id)s, %(task_id)s, %(created_epoch_ms)s,
         %(metadata)s::jsonb, %(redacted)s)
    ON CONFLICT (tenant_id, id) DO UPDATE
    SET scope = EXCLUDED.scope,
        content = EXCLUDED.content,
        sensitivity = EXCLUDED.sensitivity,
        retain_until_epoch_ms = EXCLUDED.retain_until_epoch_ms,
        delete_on_execution_end = EXCLUDED.delete_on_execution_end,
        agent_id = EXCLUDED.agent_id,
        execution_id = EXCLUDED.execution_id,
        task_id = EXCLUDED.task_id,
        metadata = EXCLUDED.metadata,
        redacted = EXCLUDED.redacted,
        updated_at = now()
"""

_SELECT_BY_ID_SQL: Final[str] = """
    SELECT id, tenant_id::text, scope, content, sensitivity,
           retain_until_epoch_ms, delete_on_execution_end, agent_id,
           execution_id, task_id, created_epoch_ms, metadata, redacted
    FROM memory_records
    WHERE id = %(id)s AND tenant_id = %(tenant_id)s::uuid
"""

_SELECT_QUERY_SQL: Final[str] = """
    SELECT id, tenant_id::text, scope, content, sensitivity,
           retain_until_epoch_ms, delete_on_execution_end, agent_id,
           execution_id, task_id, created_epoch_ms, metadata, redacted
    FROM memory_records
    WHERE tenant_id = %(tenant_id)s::uuid
      AND scope = ANY(%(scopes)s)
      AND (%(agent_id)s IS NULL OR agent_id = %(agent_id)s)
      AND (%(execution_id)s IS NULL OR execution_id = %(execution_id)s)
      AND (%(include_sensitive)s OR sensitivity NOT IN ('sensitive', 'restricted'))
      AND (%(text)s = '' OR content ILIKE %(like)s)
    ORDER BY created_epoch_ms DESC, id
    LIMIT %(limit)s
"""

_DELETE_SQL: Final[str] = """
    DELETE FROM memory_records
    WHERE id = %(id)s AND tenant_id = %(tenant_id)s::uuid
"""

_PURGE_SQL: Final[str] = """
    DELETE FROM memory_records WHERE tenant_id = %(tenant_id)s::uuid
"""

_PURGE_EXPIRED_SQL: Final[str] = """
    DELETE FROM memory_records
    WHERE tenant_id = %(tenant_id)s::uuid
      AND retain_until_epoch_ms IS NOT NULL
      AND retain_until_epoch_ms <= %(now_ms)s
"""


@dataclass(frozen=True, slots=True)
class PostgresMemoryStoreConfig:
    """Pool bounds and the expected database role.

    Attributes:
        min_size: Minimum pooled connections.
        max_size: Maximum pooled connections.
        database_role: The role the deployment intends to assume. It is
            recorded for operators; the server-side probe is the enforcement.
    """

    min_size: int = 1
    max_size: int = 8
    database_role: str = REQUIRED_ROLE

    def __post_init__(self) -> None:
        if not 1 <= self.min_size <= self.max_size:
            raise ConfigurationError(
                "memory pool bounds are invalid",
                details={"min_size": self.min_size, "max_size": self.max_size},
            )
        if not self.database_role.strip():
            raise ConfigurationError("memory database role must not be empty")


_Connection = Any
_Pool = Any


class PostgresMemoryStore:
    """A durable, tenant-isolated :class:`MemoryStore` on PostgreSQL.

    Args:
        pool: An already-open Psycopg ``AsyncConnectionPool``. Production
            construction should use :meth:`connect`; injection exists for tests.
        database_role: The role the deployment intends to assume.
        authority_probe: Optional hook run once against a fresh connection.
    """

    def __init__(
        self,
        pool: _Pool,
        *,
        database_role: str = REQUIRED_ROLE,
        authority_probe: Any = None,
        retention_policy: RetentionPolicy | None = None,
    ) -> None:
        self._pool = pool
        self._database_role = database_role
        self._authority_probe = authority_probe
        self._retention_policy = retention_policy or RetentionPolicy()

    # -- construction -------------------------------------------------------

    @classmethod
    def builder(
        cls,
        database_url: str,
        *,
        min_size: int = 1,
        max_size: int = 8,
        database_role: str = REQUIRED_ROLE,
    ) -> Any:
        """Builds a bounded pool without opening network connections."""
        from psycopg_pool import AsyncConnectionPool

        config = PostgresMemoryStoreConfig(
            min_size=min_size, max_size=max_size, database_role=database_role
        )

        async def configure(connection: _Connection) -> None:
            try:
                await _assert_memory_session_authority(connection)
            finally:
                await connection.rollback()

        pool: _Pool = AsyncConnectionPool(
            conninfo=database_url,
            min_size=config.min_size,
            max_size=config.max_size,
            open=False,
            configure=configure,
            kwargs={"autocommit": False, "application_name": "mas-memory-plane"},
        )
        return pool, config

    @classmethod
    async def connect(
        cls,
        database_url: str,
        *,
        min_size: int = 1,
        max_size: int = 8,
        database_role: str = REQUIRED_ROLE,
    ) -> PostgresMemoryStore:
        """Opens the pool and verifies the server-side authority before use.

        Raises:
            ConfigurationError: If the DSN's real login cannot assume
                ``mas_memory`` or is privileged.
        """
        if not database_url:
            raise ConfigurationError("a memory database URL is required")
        pool, config = cls.builder(
            database_url,
            min_size=min_size,
            max_size=max_size,
            database_role=database_role,
        )

        async def authority_probe() -> None:
            from psycopg import AsyncConnection

            connection = await AsyncConnection.connect(
                database_url,
                autocommit=True,
                application_name="mas-memory-plane-preflight",
            )
            try:
                await _assert_memory_session_authority(connection)
            finally:
                await connection.close()

        try:
            opener = getattr(pool, "open", None)
            if opener is not None:
                await opener(wait=True, timeout=30)
        except Exception as error:
            raise ConfigurationError(
                "memory-plane PostgreSQL pool could not be configured",
                details={"error_type": type(error).__name__},
            ) from error

        instance = cls(
            pool,
            database_role=config.database_role,
            authority_probe=authority_probe,
        )
        await authority_probe()
        return instance

    async def aclose(self) -> None:
        """Closes the owned pool so shutdown drains cleanly."""
        closer = getattr(self._pool, "close", None)
        if closer is not None:
            await closer()

    async def health(self) -> None:
        """Checks connectivity and the memory table without revealing data."""
        async with self._pool.connection() as connection:
            await _assert_memory_session_authority(connection)
            await connection.execute("SELECT 1")

    # -- MemoryStore --------------------------------------------------------

    async def put(self, record: MemoryRecord) -> None:
        """Stores a record, overwriting any record with the same id."""
        record = self._retention_policy.apply(record)
        params = {
            "id": record.record_id,
            "tenant_id": record.tenant_id,
            "scope": record.scope.value,
            "content": record.content,
            "sensitivity": record.sensitivity.value,
            "retain_until_epoch_ms": record.retention.retain_until_epoch_ms,
            "delete_on_execution_end": record.retention.delete_on_execution_end,
            "agent_id": record.agent_id,
            "execution_id": record.execution_id,
            "task_id": record.task_id,
            "created_epoch_ms": record.created_epoch_ms,
            "metadata": _json_dumps(record.metadata),
            "redacted": record.redacted,
        }
        async with self._pool.connection() as connection, connection.transaction():
            await _enter_memory_tenant_scope(connection, record.tenant_id)
            await connection.execute(_INSERT_SQL, params)

    async def get(self, tenant_id: str, record_id: str) -> MemoryRecord | None:
        """Returns a record, or ``None``. Never another tenant's record."""
        async with self._pool.connection() as connection, connection.transaction():
            await _enter_memory_tenant_scope(connection, tenant_id)
            cursor = await connection.execute(
                _SELECT_BY_ID_SQL, {"id": record_id, "tenant_id": tenant_id}
            )
            row = await cursor.fetchone()
        return _decode_row(row, tenant_id) if row is not None else None

    async def query(self, query: MemoryQuery) -> list[MemoryHit]:
        """Searches within one tenant only."""
        if not query.scopes:
            return []
        params = {
            "tenant_id": query.tenant_id,
            "scopes": [scope.value for scope in query.scopes],
            "agent_id": query.agent_id,
            "execution_id": query.execution_id,
            "include_sensitive": query.include_sensitive,
            "text": query.text.strip(),
            "like": f"%{query.text.strip()}%",
            "limit": query.limit,
        }
        async with self._pool.connection() as connection, connection.transaction():
            await _enter_memory_tenant_scope(connection, query.tenant_id)
            cursor = await connection.execute(_SELECT_QUERY_SQL, params)
            rows = await cursor.fetchall()
        return [
            MemoryHit(record=_decode_row(row, query.tenant_id), source="postgres") for row in rows
        ]

    async def delete(self, tenant_id: str, record_id: str) -> bool:
        """Deletes a record. Returns whether it existed."""
        async with self._pool.connection() as connection, connection.transaction():
            await _enter_memory_tenant_scope(connection, tenant_id)
            cursor = await connection.execute(
                _DELETE_SQL, {"id": record_id, "tenant_id": tenant_id}
            )
            return int(getattr(cursor, "rowcount", 0)) > 0

    async def purge_tenant(self, tenant_id: str) -> int:
        """Deletes every record for a tenant. Returns the count."""
        async with self._pool.connection() as connection, connection.transaction():
            await _enter_memory_tenant_scope(connection, tenant_id)
            cursor = await connection.execute(_PURGE_SQL, {"tenant_id": tenant_id})
            return int(getattr(cursor, "rowcount", 0))

    async def purge_expired(self, tenant_id: str) -> int:
        """Deletes records past their retention deadline. Returns the count."""
        async with self._pool.connection() as connection, connection.transaction():
            await _enter_memory_tenant_scope(connection, tenant_id)
            cursor = await connection.execute(
                _PURGE_EXPIRED_SQL,
                {"tenant_id": tenant_id, "now_ms": int(time.time() * 1000)},
            )
            removed = int(getattr(cursor, "rowcount", 0))
        if removed:
            _LOGGER.info(
                "memory records expired",
                extra={"event": "memory.expired", "count": removed},
            )
        return removed


async def _assert_memory_session_authority(connection: _Connection) -> None:
    """Refuses a DSN whose real login can bypass the memory-plane boundary."""
    cursor = await connection.execute(_MEMORY_IDENTITY_SQL)
    row = await cursor.fetchone()
    if row is None:
        raise ConfigurationError(
            "memory database identity probe returned no result",
            details={"required_role": REQUIRED_ROLE},
        )
    try:
        session_user, current_user, can_set_memory_role, elevated = row
    except (TypeError, ValueError) as error:
        raise ConfigurationError(
            "memory database identity probe returned an invalid result",
            details={"required_role": REQUIRED_ROLE},
        ) from error
    if (
        not isinstance(session_user, str)
        or not isinstance(current_user, str)
        or session_user != current_user
        or not bool(can_set_memory_role)
        or bool(elevated)
    ):
        raise ConfigurationError(
            "memory database login must be non-privileged and able to assume mas_memory",
            details={"required_role": REQUIRED_ROLE},
        )


async def _enter_memory_tenant_scope(connection: _Connection, tenant_id: str) -> None:
    """Narrows one transaction to the memory role and one tenant.

    ``SET LOCAL`` expires with the surrounding transaction, so a pooled socket
    cannot carry memory authority or tenant scope into a later request.
    """
    await connection.execute(_SET_LOCAL_MEMORY_ROLE_SQL)
    await connection.execute("SELECT set_config('app.current_tenant', %s, true)", (tenant_id,))


def _json_dumps(metadata: dict[str, Any]) -> str:
    import json

    return json.dumps(metadata, separators=(",", ":"), ensure_ascii=False)


def _decode_row(row: Sequence[Any], tenant_id: str) -> MemoryRecord:
    """Builds a validated domain record from a driver row."""
    (
        record_id,
        row_tenant,
        scope,
        content,
        sensitivity,
        retain_until,
        delete_on_end,
        agent_id,
        execution_id,
        task_id,
        created_epoch_ms,
        metadata,
        redacted,
    ) = row
    if row_tenant != tenant_id:
        # Defence in depth: the RLS policy already scopes the query, but a
        # decoder that trusts the row would silently launder a cross-tenant read.
        raise RetrievalError("memory row tenant does not match the bound tenant")
    try:
        record = MemoryRecord(
            record_id=str(record_id),
            tenant_id=str(row_tenant),
            scope=MemoryScope(scope),
            content=str(content),
            sensitivity=Sensitivity(sensitivity),
            retention=Retention(
                retain_until_epoch_ms=(int(retain_until) if retain_until is not None else None),
                delete_on_execution_end=bool(delete_on_end),
            ),
            agent_id=str(agent_id) if agent_id is not None else None,
            execution_id=str(execution_id) if execution_id is not None else None,
            task_id=str(task_id) if task_id is not None else None,
            created_epoch_ms=int(created_epoch_ms),
            metadata=dict(metadata) if metadata else {},
            redacted=bool(redacted),
        )
    except (ValueError, TypeError) as error:
        raise InvalidRequestError(
            "stored memory record is malformed",
            details={"record_id": str(record_id)},
        ) from error
    return record
