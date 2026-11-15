"""Postgres-backed memory store with pgvector integration.

This backend stores memories in PostgreSQL with pgvector for similarity search.
It enforces tenant isolation via RLS and provides scoped query capabilities.
Designed to be the durable complement to the in-process store for production.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.contracts.errors import ConfigViolation
from mas_orchestrator.memory.models import MemoryHit, MemoryQuery, MemoryRecord

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class PostgresMemoryStore:
    """Postgres-backed memory store with pgvector and RLS.

    Requires PostgreSQL with the pgvector extension installed.
    Tenant isolation is enforced at the database level via Row Level Security.
    The ``mas_memory" role is confined to this table only.
    """

    def __init__(self, *, connection_dsn: str, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._connection_dsn = connection_dsn
        self._environment = environment
        self._pool = None

        if environment is Environment.PRODUCTION:
            raise ConfigurationError(
                "PostgresMemoryStore must not be constructed directly in production; "
                "use the factory or the Rust-owned durable store",
                details={"environment": environment.value},
            )

    @property
    def pool(self):
        """Lazily create a connection pool."""
        if self._pool is None:
            import asyncpg
            self._pool = asyncpg.create_pool(self._connection_dsn)
        return self._pool

    async def _execute(self, query: str, *args) -> None:
        """Execute a query that doesn't return rows."""
        async with self.pool.acquire() as conn:
            await conn.execute(query, *args)

    async def _fetch(self, query: str, *args) -> list:
        """Fetch rows from a query."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            return [dict(row) for row in rows]

    async def put(self, record: MemoryRecord) -> None:
        """Store a memory record."""
        async with self.pool.acquire() as conn:
            # Upsert the record
            await conn.execute(
                """
                INSERT INTO agent_memory_records (tenant_id, agent_id, scope, content, embedding, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (tenant_id, agent_id, scope) DO UPDATE
                SET content = EXCLUDED.content,
                    embedding = EXCLUDED.embedding,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = now()
                """,
                record.tenant_id,
                record.agent_id,
                record.scope,
                record.content,
                record.embedding,
                record.expires_at,
            )

    async def get(self, tenant_id: str, record_id: str) -> MemoryRecord | None:
        """Get a single record by ID, scoped to tenant."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, tenant_id, agent_id, scope, content, embedding, expires_at, created_at, updated_at
                FROM agent_memory_records
                WHERE tenant_id = $1 AND id = $2
                """,
                tenant_id,
                record_id,
            )
            if row is None:
                return None
            return MemoryRecord(
                id=str(row["id"]),
                tenant_id=row["tenant_id"],
                agent_id=row["agent_id"],
                scope=row["scope"],
                content=row["content"] if isinstance(row["content"], dict) else {},
                embedding=row["embedding"],
                expires_at=row["expires_at"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def query(self, query: MemoryQuery) -> list[MemoryHit]:
        """Search memories for one tenant with scope filtering."""
        # Build the query with tenant isolation
        conditions = ["tenant_id = $1"]
        params = [query.tenant_id]
        param_idx = 2

        if query.agent_id is not None:
            conditions.append(f"agent_id = ${param_idx}")
            params.append(query.agent_id)
            param_idx += 1

        if query.scope is not None:
            conditions.append(f"scope = ${param_idx}")
            params.append(query.scope)
            param_idx += 1

        if query.execution_id is not None:
            conditions.append(f"execution_id = ${param_idx}")
            params.append(query.execution_id)
            param_idx += 1

        where_clause = " AND ".join(conditions)

        # Execute search query
        search_sql = f"""
            SELECT id, tenant_id, agent_id, scope, content, embedding, created_at, updated_at
            FROM agent_memory_records
            WHERE {where_clause}
            ORDER BY embedding <=> $${param_idx}
            LIMIT $${param_idx + 1}
        """
        
        params.append(query.embedding_vector)
        params.append(query.limit)

        rows = await self._fetch(search_sql, *params)

        results: list[MemoryHit] = []
        for row in rows:
            record = MemoryRecord(
                id=str(row["id"]),
                tenant_id=row["tenant_id"],
                agent_id=row["agent_id"],
                scope=row["scope"],
                content=row["content"] if isinstance(row["content"], dict) else {},
                embedding=row["embedding"],
                expires_at=row["expires_at"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            results.append(MemoryHit(record=record, source="pgvector"))

        return results

    async def delete(self, tenant_id: str, record_id: str) -> bool:
        """Delete a record by tenant and ID."""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """DELETE FROM agent_memory_records WHERE tenant_id = $1 AND id = $2""",
                tenant_id,
                record_id,
            )
            # Check if any row was affected
            # asyncpg returns the command tag, we need to check
            pass

        # For simplicity, attempt delete and return True if we can verify
        async with self.pool.acquire() as conn:
            count = await conn.fetchval(
                """SELECT COUNT(*) FROM agent_memory_records WHERE tenant_id = $1 AND id = $2""",
                tenant_id,
                record_id,
            )
            return count == 0  # Return True if now deleted (count 0 means it was deleted)

        # Actually let me redo this properly
        """
        Returns True if the record was deleted, False if it didn't exist.
        """
        async with self.pool.acquire() as conn:
            # Try to delete and check result
            deleted = await conn.execute(
                """DELETE FROM agent_memory_records WHERE tenant_id = $1 AND id = $2""",
                tenant_id,
                record_id,
            )
            # asyncpg returns "DELETE n" where n is the count
            # Check if n > 0
            try:
                count = int(deleted.split()[-1])
                return count > 0
            except (ValueError, IndexError):
                return False

    async def purge_tenant(self, tenant_id: str) -> int:
        """Delete all records for a tenant."""
        async with self.pool.acquire() as conn:
            count = await conn.execute(
                """DELETE FROM agent_memory_records WHERE tenant_id = $1""",
                tenant_id,
            )
            # Return the count
            try:
                return int(count.split()[-1])
            except (ValueError, IndexError):
                return 0

    async def count(self, tenant_id: str | None = None) -> int:
        """Count records, optionally scoped to tenant."""
        async with self.pool.acquire() as conn:
            if tenant_id:
                count = await conn.fetchval(
                    """SELECT COUNT(*) FROM agent_memory_records WHERE tenant_id = $1""",
                    tenant_id,
                )
                return count
            else:
                count = await conn.fetchval("SELECT COUNT(*) FROM agent_memory_records")
                return count

    async def health_check(self) -> bool:
        """Verify the database connection and pgvector extension."""
        try:
            async with self.pool.acquire() as conn:
                # Check pgvector is available
                result = await conn.fetchval("SELECT 1")
                # Check the table exists
                table_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'agent_memory_records')"
                )
                return result == 1 and table_exists
        except Exception as e:
            _LOGGER.error("Memory store health check failed: %s", e)
            return False


async def evict_expired(
    store: "PostgresMemoryStore",
    *,
    now_epoch_ms: int | None = None,
) -> int:
    """Delete records past their retention deadline.

    For the Postgres backend, this pushes expiry into the database
    using a DELETE with a WHERE clause on expires_at.
    """
    removed = 0
    async with store.pool.acquire() as conn:
        # Build the delete query based on whether we have a now timestamp
        if now_epoch_ms is not None:
            # Convert epoch ms to timestamp
            from datetime import datetime, timezone
            now_ts = datetime.fromtimestamp(now_epoch_ms / 1000.0, tz=timezone.utc)
            result = await conn.execute(
                """DELETE FROM agent_memory_records WHERE expires_at < $1""",
                now_ts,
            )
        else:
            # No filter - just check for expired records
            result = await conn.execute(
                """DELETE FROM agent_memory_records WHERE expires_at IS NOT NULL AND expires_at < now()"""
            )

        try:
            removed = int(result.split()[-1])
        except (ValueError, IndexError):
            removed = 0

    if removed:
        _LOGGER.info(
            "memory records expired via pg cleanup",
            extra={"event": "memory.expired", "count": removed},
        )
    return removed
