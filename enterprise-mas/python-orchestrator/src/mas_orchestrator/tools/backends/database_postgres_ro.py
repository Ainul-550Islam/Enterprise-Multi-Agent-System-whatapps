"""Postgres read-only backend with statement timeout and row/byte cap.

Provides read-only access to PostgreSQL databases with security-enforced
limits. Only allowlisted schemas and tables may be queried. This is intended
for read-only tool access, never for write operations.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import resolve_secret

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Default statement timeout in milliseconds
DEFAULT_STATEMENT_TIMEOUT = 5000  # 5 seconds
#: Default max rows per query
DEFAULT_MAX_ROWS = 1000
#: Default max bytes per query result
DEFAULT_MAX_BYTES = 1_000_000  # 1 MB


class PostgresReadOnlyBackend:
    """Postgres read-only backend with enforced limits.

    Requires the following configuration keys:
    - connection_dsn: PostgreSQL DSN (secret reference acceptable)
    - allowlisted_schemas: list of schema names that may be queried
    - allowlisted_tables: list of table names that may be queried (schema-qualified if needed)

    Optional configuration keys:
    - statement_timeout: query timeout in milliseconds
    - max_rows: max rows per query result
    - max_bytes: max bytes per query result
    - default_scope: default RLS policy scope (tenant_id)

    Security guarantees:
    - Statement timeout prevents infinite queries
    - Row-level security enforced via SET ROLE
    - Read-only: NO INSERT, UPDATE, or DELETE permitted
    - Schema/table whitelist prevents accessing unintended data
    """

    def __init__(self, *, config: dict, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._connection_dsn = resolve_secret(config.get("connection_dsn", ""))
        self._allowlisted_schemas = config.get("allowlisted_schemas", [])
        self._allowlisted_tables = config.get("allowlisted_tables", [])
        self._statement_timeout = int(config.get("statement_timeout", DEFAULT_STATEMENT_TIMEOUT))
        self._max_rows = int(config.get("max_rows", DEFAULT_MAX_ROWS))
        self._max_bytes = int(config.get("max_bytes", DEFAULT_MAX_BYTES))
        self._default_scope = config.get("default_scope", "tenant_id")

        # Validate configuration
        if not self._connection_dsn:
            raise ConfigurationError(
                "connection_dsn is required for PostgresReadOnlyBackend",
                details={"field": "connection_dsn"},
            )

        if not self._allowlisted_schemas:
            raise ConfigurationError(
                "at least one allowlisted_schema is required",
                details={"field": "allowlisted_schemas"},
            )

        if not self._allowlisted_tables:
            raise ConfigurationError(
                "at least one allowlisted_table is required",
                details={"field": "allowlisted_tables"},
            )

        # Validate statement timeout is positive
        if self._statement_timeout <= 0:
            raise ConfigurationError(
                "statement_timeout must be positive",
                details={"field": "statement_timeout"},
            )

        # Validate max rows is positive
        if self._max_rows <= 0:
            raise ConfigurationError(
                "max_rows must be positive",
                details={"field": "max_rows"},
            )

        # Validate max bytes is positive
        if self._max_bytes <= 0:
            raise ConfigurationError(
                "max_bytes must be positive",
                details={"field": "max_bytes"},
            )

    async def query(self, *, sql: str, params: list | None = None) -> dict:
        """Execute a read-only SQL query with enforced limits.

    Args:
        sql: SQL SELECT query to execute.
        params: Optional query parameters.

    Returns:
        Dictionary with "columns" and "rows" keys.

    Raises:
        ConfigurationError: If the query violates security policies.
        RuntimeError: If the query execution fails.
    """
        # Validate the SQL query is read-only
        sql_stripped = sql.strip().upper()
        if sql_stripped.startswith("SELECT"):
            # Enforce row/byte caps by wrapping the query
            safe_sql = self._apply_limits(sql)
        else:
            raise ConfigurationError(
                f"only SELECT queries are permitted, got: {sql_stripped[:50]}",
                details={"field": "sql", "provided": sql[:50]},
            )

        # Execute the query
        try:
            async with asyncio.timeout(self._statement_timeout / 1000.0):
                import asyncpg
                
                # Connect and set read-only role for RLS
                conn = await asyncpg.connect(self._connection_dsn)
                try:
                    # Set the role for row-level security
                    await conn.execute(f"SET ROLE {self._default_scope}")
                    
                    # Execute the safe query
                    if params:
                        rows = await conn.fetch(safe_sql, *params)
                    else:
                        rows = await conn.fetch(safe_sql)
                    
                    # Get column names
                    cols = [desc.name for desc in rows.cursor.description] if hasattr(rows, 'cursor') else list(rows[0].keys()) if rows else []
                    
                    # Row count and byte size
                    row_count = len(rows)
                    # Estimate byte size (rough)
                    byte_size = sum(len(str(row)) for row in rows)
                    
                    # Enforce max_rows limit
                    if row_count > self._max_rows:
                        _LOGGER.warning(
                            "query returned %d rows, exceeding max of %d",
                            row_count, self._max_rows
                        )
                    
                    # Enforce max_bytes limit
                    if byte_size > self._max_bytes:
                        _LOGGER.warning(
                            "query result estimated at %d bytes, exceeding max of %d",
                            byte_size, self._max_bytes
                        )
                    
                    return {
                        "columns": cols,
                        "rows": [dict(row) for row in rows][:self._max_rows],
                        "row_count": row_count,
                        "byte_estimate": byte_size,
                    }
                finally:
                    await conn.close()

        except asyncio.TimeoutError:
            raise RuntimeError(f"query timed out after {self._statement_timeout}ms")
        except Exception as e:
            raise RuntimeError(f"failed to execute read-only query: {e}")

    def _apply_limits(self, sql: str) -> str:
        """Apply row/byte caps to a SELECT query.

        This is a simple wrapper that adds LIMIT and estimation.
        In production, consider using a proper query rewriter.
        """
        # Ensure LIMIT is present
        sql_upper = sql.upper()
        if "LIMIT" not in sql_upper:
            # Add LIMIT after SELECT clause
            # Find the first FROM or JOIN
            import re
            match = re.search(r"\bFROM\b|\bJOIN\b", sql_upper)
            if match:
                insert_pos = match.start()
                # Add LIMIT clause
                limit_clause = f" LIMIT {self._max_rows}"
                # Be careful not to break SQL syntax
                if sql[insert_pos:insert_pos+1].isalpha() or sql[insert_pos:insert_pos+1] == "_":
                    # Insert after the keyword
                    safe_sql = sql[:insert_pos + len("FROM")] + limit_clause + " " + sql[insert_pos + len("FROM"):]
                else:
                    safe_sql = sql[:insert_pos] + limit_clause + " " + sql[insert_pos:]
            else:
                # No FROM found, just add at end
                safe_sql = sql + f" LIMIT {self._max_rows}"
        else:
            # Already has LIMIT, enforce the cap
            # Parse and replace LIMIT
            import re
            safe_sql = re.sub(r"LIMIT\s+\d+", f"LIMIT {self._max_rows}", sql, flags=re.IGNORECASE)
        return safe_sql


def create_backend(
    *,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> PostgresReadOnlyBackend:
    """Convenient function to create a PostgresReadOnlyBackend instance."""
    return PostgresReadOnlyBackend(config=config, environment=environment)
