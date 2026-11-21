"""Read-only PostgreSQL backend for the database tool.

Implements :class:`~mas_orchestrator.tools.builtins.database.DatabaseBackend`.
The tool layer already vets statements; this adapter independently re-checks
them, because two guards is the point. On top of the Python-side check every
call runs inside a server-side ``READ ONLY`` transaction with a
``statement_timeout``, so even a statement that slipped past both parsers
cannot write.

Rows are returned as dictionaries keyed by column name, capped at ``limit``.
"""

from __future__ import annotations

import re
from typing import Any

from mas_orchestrator.contracts.errors import (
    DependencyUnavailableError,
    InvalidRequestError,
    ToolFailedError,
)

# Statements this adapter will ever send.
_READ_ONLY_STARTS = ("select", "with", "show", "explain", "values", "table")

# Words that must not appear anywhere in the statement: a read-only surface
# has no business containing them, even inside a CTE or a string (a caller
# who needs such literal text can pass it as a parameter).
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|upsert|replace|drop|create|alter|truncate|"
    r"grant|revoke|execute|call|copy|vacuum|analyze|reindex|cluster|comment|"
    r"prepare|deallocate|discard|listen|notify|do|refresh|lock|set|reset|"
    r"security|owner|import|export)\b",
    re.IGNORECASE,
)

_MULTI_STATEMENT = re.compile(r";\s*\S")


class PostgresReadOnlyBackend:
    """Executes vetted read-only statements against a pooled PostgreSQL.

    Args:
        pool: A Psycopg ``AsyncConnectionPool`` (or anything whose
            ``connection()`` async context manager yields a connection with
            ``execute``/``cursor``). Production wiring passes the same pool
            shape :class:`~mas_orchestrator.memory.postgres_store.PostgresMemoryStore`
            builds; tests inject a recording fake.

    Raises:
        InvalidRequestError: If no pool is supplied.
    """

    def __init__(
        self,
        *,
        pool: Any,
        allowed_schemas: frozenset[str] | None = None,
        max_response_bytes: int = 256 * 1024,
    ) -> None:
        if pool is None or not hasattr(pool, "connection"):
            raise InvalidRequestError(
                "postgres read-only backend requires a connection pool",
                details={"field": "pool"},
            )
        if max_response_bytes <= 0:
            raise InvalidRequestError(
                "max_response_bytes must be positive",
                details={"field": "max_response_bytes"},
            )
        self._pool = pool
        self._allowed_schemas = (
            frozenset(schema.lower() for schema in allowed_schemas)
            if allowed_schemas is not None
            else None
        )
        self._max_response_bytes = int(max_response_bytes)

    async def fetch(
        self,
        statement: str,
        *,
        parameters: list[Any],
        limit: int,
        timeout_seconds: float,
    ) -> list[dict[str, Any]]:
        """Runs one read-only statement and returns up to ``limit`` rows."""
        self._refuse_unsafe(statement)
        self._refuse_foreign_schema(statement)
        if not isinstance(limit, int) or limit <= 0:
            raise InvalidRequestError("row limit must be positive", details={"field": "limit"})
        if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise InvalidRequestError(
                "statement timeout must be positive", details={"field": "timeout_seconds"}
            )

        timeout_ms = max(1, int(float(timeout_seconds) * 1000))
        try:
            async with self._pool.connection() as connection, connection.transaction():
                # Server-side second guard: the transaction itself cannot
                # write, and it cannot outlive the requested budget.
                await connection.execute("SET TRANSACTION READ ONLY")
                await connection.execute(f"SET LOCAL statement_timeout = {timeout_ms}")
                cursor = await connection.execute(statement, list(parameters))
                rows = await cursor.fetchmany(limit)
                names = [column.name for column in cursor.description or []]
                await cursor.close()
        except InvalidRequestError:
            raise
        except Exception as error:
            raise DependencyUnavailableError(
                "postgres read-only query failed",
                details={"tool_id": "database", "backend": "postgres-read-only"},
            ) from error

        mapped = [dict(zip(names, row, strict=False)) for row in rows]
        encoded = repr(mapped).encode("utf-8")
        if len(encoded) > self._max_response_bytes:
            raise ToolFailedError(
                "read-only query result exceeded the byte cap",
                details={
                    "tool_id": "database",
                    "backend": "postgres-read-only",
                    "max_response_bytes": self._max_response_bytes,
                },
            )
        return mapped

    @staticmethod
    def _refuse_unsafe(statement: str) -> None:
        """Independent second check: only trivially read-only shapes pass."""
        if not isinstance(statement, str) or not statement.strip():
            raise InvalidRequestError("statement must not be empty", details={"field": "statement"})
        if _MULTI_STATEMENT.search(statement):
            raise ToolFailedError(
                "multi-statement input is refused by the read-only backend",
                details={"tool_id": "database", "backend": "postgres-read-only"},
            )
        first = statement.strip().split(None, 1)[0].lower()
        if first not in _READ_ONLY_STARTS:
            raise ToolFailedError(
                "only read-only statements execute on this backend",
                details={"tool_id": "database", "backend": "postgres-read-only"},
            )
        if _FORBIDDEN.search(statement):
            raise ToolFailedError(
                "statement contains a verb the read-only backend forbids",
                details={"tool_id": "database", "backend": "postgres-read-only"},
            )

    def _refuse_foreign_schema(self, statement: str) -> None:
        """When an operator allowlist is set, only those schemas may be named."""
        if self._allowed_schemas is None:
            return
        for match in re.finditer(r"\b([a-zA-Z_][\w]*)\s*\.\s*([a-zA-Z_][\w]*)", statement):
            schema = match.group(1).lower()
            if schema not in self._allowed_schemas:
                raise ToolFailedError(
                    "statement names a schema outside the operator allowlist",
                    details={
                        "tool_id": "database",
                        "backend": "postgres-read-only",
                        "schema": schema,
                    },
                )
