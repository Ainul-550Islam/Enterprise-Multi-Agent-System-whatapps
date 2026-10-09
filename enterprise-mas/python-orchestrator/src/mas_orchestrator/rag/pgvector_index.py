"""Tenant-isolated PostgreSQL/pgvector implementation of :class:`VectorIndex`.

This adapter requires a non-privileged login that can assume the dedicated
``mas_rag`` role and only queries ``document_chunks`` while that role is
active. Rust-owned data-source/document metadata is never read or written
here. Pool startup verifies the actual authenticated/session identity rather
than trusting a configuration label; every RAG transaction then executes
``SET LOCAL ROLE mas_rag`` and binds the authoritative tenant to the RLS GUC.
The database policy and SQL predicate therefore enforce the tenant boundary
independently.

The optional ``pgvector`` package is imported only when this backend is
constructed. A deployment that selects pgvector without installing the extra
fails with a typed configuration error instead of silently falling back to an
in-memory index.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    DependencyUnavailableError,
    InvalidRequestError,
    RetrievalError,
)
from mas_orchestrator.rag.models import Chunk, DocumentAccess, EmbeddedChunk

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from mas_orchestrator.rag.models import SecurityContext

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)
MAX_VECTOR_BATCH: Final[int] = 512
MAX_SEARCH_LIMIT: Final[int] = 200
VECTOR_DIMENSION_MIN: Final[int] = 1536
VECTOR_DIMENSION_MAX: Final[int] = 1536
_REQUIRED_ROLE: Final[str] = "mas_rag"

# The configured `database_role` string is not authority: a DSN could still
# authenticate as a superuser or a broadly privileged runtime login. Validate
# the server-side session identity before the service becomes ready. `SET` is
# intentional: every data transaction uses `SET LOCAL ROLE mas_rag`, so a
# login that merely inherits privileges but cannot assume the role is refused.
# The final predicate catches an elevated role reachable through membership as
# well as one that is directly authenticated: a login that can `SET ROLE` to a
# superuser/BYPASSRLS role has that authority even when its own role flags are
# false.
_RAG_IDENTITY_SQL: Final[str] = """
    SELECT
        session_user::text,
        current_user::text,
        pg_has_role(session_user, 'mas_rag', 'SET'),
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
_SET_LOCAL_RAG_ROLE_SQL: Final[str] = "SET LOCAL ROLE mas_rag"


_Connection = Any
_Pool = Any


class PgVectorIndex:
    """Durable tenant-filtered vector index on PostgreSQL + pgvector.

    Args:
        pool: An already-open Psycopg ``AsyncConnectionPool``. Production
            construction should use :meth:`connect`; injection exists for
            controlled adapters and unit tests.
        dimensions: Exact embedding-vector dimension selected by configuration.
        batch_size: Maximum number of chunks accepted in one upsert call.

    Raises:
        ConfigurationError: If dimensions or batch size violate their hard
            bounds, or if the deployment did not select the dedicated role.
    """

    def __init__(
        self,
        pool: _Pool,
        *,
        dimensions: int,
        database_role: str = _REQUIRED_ROLE,
        batch_size: int = 128,
        authority_probe: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        if not VECTOR_DIMENSION_MIN <= dimensions <= VECTOR_DIMENSION_MAX:
            raise ConfigurationError(
                "pgvector embedding dimensions are outside the supported range",
                details={"dimensions": dimensions},
            )
        if database_role != _REQUIRED_ROLE:
            raise ConfigurationError(
                "pgvector must use the dedicated RAG-plane database role",
                details={"required_role": _REQUIRED_ROLE},
            )
        if not 1 <= batch_size <= MAX_VECTOR_BATCH:
            raise ConfigurationError(
                "pgvector batch size is outside the supported range",
                details={"batch_size": batch_size, "maximum": MAX_VECTOR_BATCH},
            )
        self._pool = pool
        self._dimensions = dimensions
        self._batch_size = batch_size
        self._authority_probe = authority_probe
        self._opened = False

    @classmethod
    def deferred(
        cls,
        database_url: str,
        *,
        dimensions: int,
        database_role: str,
        min_size: int = 1,
        max_size: int = 8,
        batch_size: int = 128,
    ) -> PgVectorIndex:
        """Builds a bounded pool without opening network connections.

        This is the synchronous composition-root path. :meth:`open` is called
        by server startup before the listener accepts traffic, so a missing
        database or extension remains a fail-closed startup error.
        """
        if not database_url.strip():
            raise ConfigurationError(
                "pgvector database URL is empty",
                details={"field": "data_hub.database_url_secret_ref"},
            )
        if database_role != _REQUIRED_ROLE:
            raise ConfigurationError(
                "pgvector must use the dedicated RAG-plane database role",
                details={"required_role": _REQUIRED_ROLE},
            )
        if min_size < 1 or max_size < min_size or max_size > 64:
            raise ConfigurationError(
                "pgvector pool bounds are invalid",
                details={"min_size": min_size, "max_size": max_size},
            )
        try:
            from pgvector.psycopg import register_vector_async
            from psycopg import AsyncConnection
            from psycopg_pool import AsyncConnectionPool
        except ImportError as error:
            raise ConfigurationError(
                "pgvector backend selected but the optional pgvector extra is not installed",
                details={"extra": "pgvector"},
            ) from error

        async def configure(connection: Any) -> None:
            # Psycopg invokes this for every newly-created pooled connection,
            # not just the first startup probe. Each socket therefore verifies
            # its authenticated authority before it can join the RAG pool.
            # The pool requires configure hooks to leave the socket idle; the
            # authority SELECT starts a transaction because normal RAG use has
            # autocommit disabled, so roll it back before returning.
            try:
                await register_vector_async(connection)
                await _assert_rag_session_authority(connection)
            finally:
                await connection.rollback()

        async def authority_probe() -> None:
            # A pool reports configure-hook failures as connection retries and
            # eventually as PoolTimeout. Probe once directly first so an
            # invalid DSN principal remains the typed ConfigurationError that
            # operators can correct, rather than masquerading as downtime.
            connection = await AsyncConnection.connect(
                database_url,
                autocommit=True,
                application_name="mas-rag-plane-preflight",
            )
            try:
                await _assert_rag_session_authority(connection)
            finally:
                await connection.close()

        try:
            pool: Any = AsyncConnectionPool(
                conninfo=database_url,
                min_size=min_size,
                max_size=max_size,
                open=False,
                configure=configure,
                kwargs={"autocommit": False, "application_name": "mas-rag-plane"},
            )
        except Exception as error:
            raise ConfigurationError(
                "RAG-plane PostgreSQL pool could not be configured",
                details={"error_type": type(error).__name__},
            ) from error
        return cls(
            pool,
            dimensions=dimensions,
            database_role=database_role,
            batch_size=batch_size,
            authority_probe=authority_probe,
        )

    @classmethod
    async def connect(
        cls,
        database_url: str,
        *,
        dimensions: int,
        database_role: str,
        min_size: int = 1,
        max_size: int = 8,
        batch_size: int = 128,
    ) -> PgVectorIndex:
        """Creates and verifies the RAG-plane pool before returning it."""
        index = cls.deferred(
            database_url,
            dimensions=dimensions,
            database_role=database_role,
            min_size=min_size,
            max_size=max_size,
            batch_size=batch_size,
        )
        await index.open()
        return index

    async def open(self) -> None:
        """Opens the pool and verifies its effective database authority.

        A value of ``data_hub.database_role='mas_rag'`` is only deployment
        metadata; it does not constrain the login encoded in the DSN. The
        startup probe therefore rejects a superuser, an RLS-bypass role, or a
        login unable to assume ``mas_rag`` before this service accepts work.
        """
        if self._opened:
            return
        try:
            if self._authority_probe is not None:
                await self._authority_probe()
            opener = getattr(self._pool, "open", None)
            if opener is not None:
                await opener(wait=True)
            async with self._pool.connection() as connection:
                await _assert_rag_session_authority(connection)
                await connection.execute("SELECT 1")
        except ConfigurationError:
            raise
        except Exception as error:
            _LOGGER.error(
                "RAG-plane PostgreSQL connection failed",
                extra={"event": "rag.pgvector.connect_failed", "error_type": type(error).__name__},
            )
            raise DependencyUnavailableError(
                "RAG-plane PostgreSQL is unavailable",
                details={"dependency": "postgres-rag"},
            ) from error
        self._opened = True

    @property
    def dimensions(self) -> int:
        """The exact embedding dimension accepted by this index."""
        return self._dimensions

    async def aclose(self) -> None:
        """Closes the owned pool so process shutdown drains cleanly."""
        await self._pool.close()
        self._opened = False

    async def health_check(self) -> bool:
        """Checks pool connectivity and the RAG table without revealing data."""
        await self.open()
        try:
            async with self._pool.connection() as connection, connection.transaction():
                await _enter_rag_tenant_scope(connection, None)
                await connection.execute("SELECT 1 FROM document_chunks LIMIT 0")
            return True
        except Exception as error:
            _LOGGER.warning(
                "RAG-plane health probe failed",
                extra={"event": "rag.pgvector.health_failed", "error_type": type(error).__name__},
            )
            return False

    async def upsert(self, items: Sequence[EmbeddedChunk]) -> None:
        """Atomically upserts a bounded, single-tenant batch of vectors.

        Tenant mixing is rejected even though each row carries its own scope:
        a single index operation must not become a cross-tenant transaction.
        """
        if not items:
            return
        if len(items) > self._batch_size:
            raise InvalidRequestError(
                "pgvector batch exceeds the configured limit",
                details={"limit": self._batch_size, "received": len(items)},
            )
        tenant_ids = {item.chunk.access.tenant_id for item in items}
        if len(tenant_ids) != 1:
            raise InvalidRequestError(
                "pgvector upsert batch must contain exactly one tenant",
                details={"tenant_count": len(tenant_ids)},
            )
        tenant_id = _uuid_text(next(iter(tenant_ids)), "tenant_id")
        for item in items:
            _validate_embedding(item, self._dimensions)
            _uuid_text(item.chunk.document_id, "document_id")
            if len(item.chunk.text.encode("utf-8")) > 32 * 1024:
                raise InvalidRequestError(
                    "chunk text exceeds the pgvector row bound",
                    details={"chunk_id": item.chunk.chunk_id},
                )

        try:
            from pgvector import Vector
            from psycopg.types.json import Jsonb

            async with self._pool.connection() as connection, connection.transaction():
                await _enter_rag_tenant_scope(connection, tenant_id)
                for item in items:
                    await connection.execute(
                        """INSERT INTO document_chunks (
                                   tenant_id, document_id, chunk_id, ordinal, start_char,
                                   end_char, chunk_text, embedding, embedding_model, title,
                                   source_uri, required_roles, denied_principals, metadata
                               ) VALUES (
                                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                               )
                               ON CONFLICT (tenant_id, document_id, chunk_id)
                               DO UPDATE SET ordinal = EXCLUDED.ordinal,
                                             start_char = EXCLUDED.start_char,
                                             end_char = EXCLUDED.end_char,
                                             chunk_text = EXCLUDED.chunk_text,
                                             embedding = EXCLUDED.embedding,
                                             embedding_model = EXCLUDED.embedding_model,
                                             title = EXCLUDED.title,
                                             source_uri = EXCLUDED.source_uri,
                                             required_roles = EXCLUDED.required_roles,
                                             denied_principals = EXCLUDED.denied_principals,
                                             metadata = EXCLUDED.metadata""",
                        (
                            UUID(tenant_id),
                            UUID(item.chunk.document_id),
                            item.chunk.chunk_id,
                            item.chunk.index,
                            item.chunk.start_char,
                            item.chunk.end_char,
                            item.chunk.text,
                            Vector(list(item.vector)),
                            item.model,
                            str(item.chunk.metadata.get("title", ""))[:512],
                            item.chunk.metadata.get("source_uri"),
                            sorted(item.chunk.access.required_roles),
                            sorted(item.chunk.access.denied_principals),
                            Jsonb(dict(item.chunk.metadata)),
                        ),
                    )
        except (ConfigurationError, InvalidRequestError):
            raise
        except Exception as error:
            _LOGGER.error(
                "RAG vector upsert failed",
                extra={
                    "event": "rag.pgvector.upsert_failed",
                    "tenant_id": tenant_id,
                    "batch_size": len(items),
                    "error_type": type(error).__name__,
                },
            )
            raise RetrievalError(
                "vector index upsert failed",
                details={"dependency": "postgres-rag", "batch_size": len(items)},
            ) from error

    async def search(
        self,
        vector: Sequence[float],
        *,
        context: SecurityContext,
        limit: int,
    ) -> list[tuple[EmbeddedChunk, float]]:
        """Returns nearest authorized chunks, tenant-filtered before ranking."""
        if not 1 <= limit <= MAX_SEARCH_LIMIT:
            raise InvalidRequestError(
                "pgvector search limit is outside the supported range",
                details={"limit": limit, "maximum": MAX_SEARCH_LIMIT},
            )
        if len(vector) != self._dimensions or any(not math.isfinite(value) for value in vector):
            raise InvalidRequestError(
                "query embedding dimension or values are invalid",
                details={"expected_dimensions": self._dimensions, "received": len(vector)},
            )
        tenant_id = _uuid_text(context.tenant_id, "tenant_id")

        try:
            from pgvector import Vector

            async with self._pool.connection() as connection, connection.transaction():
                await _enter_rag_tenant_scope(connection, tenant_id)
                cursor = await connection.execute(
                    """SELECT chunk_id, document_id, ordinal, start_char, end_char, chunk_text,
                              embedding, embedding_model, title, source_uri, metadata,
                              required_roles, denied_principals,
                              1.0 - (embedding <=> %s) AS similarity
                       FROM document_chunks
                       WHERE tenant_id = %s
                         AND (cardinality(required_roles) = 0
                              OR required_roles && %s::text[])
                         AND NOT (denied_principals && %s::text[])
                       ORDER BY embedding <=> %s, chunk_id
                       LIMIT %s""",
                    (
                        Vector(list(vector)),
                        UUID(tenant_id),
                        sorted(context.allowed_roles),
                        sorted(context.allowed_principals),
                        Vector(list(vector)),
                        limit,
                    ),
                )
                rows = await cursor.fetchall()
            return [_decode_row(row, tenant_id) for row in rows]
        except (ConfigurationError, InvalidRequestError):
            raise
        except Exception as error:
            _LOGGER.error(
                "RAG vector search failed",
                extra={
                    "event": "rag.pgvector.search_failed",
                    "tenant_id": tenant_id,
                    "error_type": type(error).__name__,
                },
            )
            raise RetrievalError(
                "vector search failed",
                details={"dependency": "postgres-rag", "tenant_id": tenant_id},
            ) from error

    async def delete_document(self, tenant_id: str, document_id: str) -> int:
        """Deletes all vectors for exactly one tenant/document pair."""
        tenant_uuid = _uuid_text(tenant_id, "tenant_id")
        document_uuid = _uuid_text(document_id, "document_id")
        try:
            async with self._pool.connection() as connection, connection.transaction():
                await _enter_rag_tenant_scope(connection, tenant_uuid)
                cursor = await connection.execute(
                    "DELETE FROM document_chunks WHERE tenant_id = %s AND document_id = %s",
                    (UUID(tenant_uuid), UUID(document_uuid)),
                )
                return int(cursor.rowcount)
        except Exception as error:
            _LOGGER.error(
                "RAG vector delete failed",
                extra={
                    "event": "rag.pgvector.delete_failed",
                    "tenant_id": tenant_uuid,
                    "document_id": document_uuid,
                    "error_type": type(error).__name__,
                },
            )
            raise RetrievalError(
                "vector delete failed",
                details={"dependency": "postgres-rag", "document_id": document_uuid},
            ) from error


def _uuid_text(value: str, field: str) -> str:
    """Validates an identifier before it reaches UUID-typed SQL binds."""
    try:
        return str(UUID(value))
    except (ValueError, AttributeError, TypeError) as error:
        raise InvalidRequestError(
            f"{field} must be a canonical UUID",
            details={"field": field},
        ) from error


async def _assert_rag_session_authority(connection: _Connection) -> None:
    """Refuses a DSN whose actual login can bypass the RAG-plane boundary.

    ``database_role`` is a configuration assertion, not evidence of the
    principal authenticated by PostgreSQL. This probe checks the session on
    the server and requires an unprivileged login that can issue ``SET ROLE
    mas_rag``. It rejects pre-selected roles, plus any superuser/BYPASSRLS
    role directly active or reachable through ``SET ROLE`` membership.
    """
    cursor = await connection.execute(_RAG_IDENTITY_SQL)
    row = await cursor.fetchone()
    if row is None:
        raise ConfigurationError(
            "RAG database identity probe returned no result",
            details={"required_role": _REQUIRED_ROLE},
        )
    try:
        session_user, current_user, can_set_rag_role, elevated = row
    except (TypeError, ValueError) as error:
        raise ConfigurationError(
            "RAG database identity probe returned an invalid result",
            details={"required_role": _REQUIRED_ROLE},
        ) from error
    if (
        not isinstance(session_user, str)
        or not isinstance(current_user, str)
        or session_user != current_user
        or not bool(can_set_rag_role)
        or bool(elevated)
    ):
        raise ConfigurationError(
            "RAG database login must be non-privileged and able to assume mas_rag",
            details={"required_role": _REQUIRED_ROLE},
        )


async def _enter_rag_tenant_scope(connection: _Connection, tenant_id: str | None) -> None:
    """Narrows one transaction to the RAG role and, optionally, one tenant.

    ``SET LOCAL`` expires with the surrounding transaction, so a pooled socket
    cannot carry either RAG authority or tenant scope into a later request.
    """
    await connection.execute(_SET_LOCAL_RAG_ROLE_SQL)
    if tenant_id is not None:
        await connection.execute("SELECT set_config('app.current_tenant', %s, true)", (tenant_id,))


def _validate_embedding(item: EmbeddedChunk, dimensions: int) -> None:
    """Rejects non-finite, wrongly-sized or oversized stored vectors."""
    if len(item.vector) != dimensions:
        raise InvalidRequestError(
            "embedding dimension does not match configured pgvector dimension",
            details={"expected": dimensions, "received": len(item.vector)},
        )
    if any(not math.isfinite(value) for value in item.vector):
        raise InvalidRequestError(
            "embedding contains a non-finite value",
            details={"chunk_id": item.chunk.chunk_id},
        )
    if not item.model.strip() or len(item.model) > 255:
        raise InvalidRequestError(
            "embedding model id is invalid",
            details={"chunk_id": item.chunk.chunk_id},
        )
    if len(item.chunk.chunk_id) > 128:
        raise InvalidRequestError(
            "chunk id exceeds the pgvector row bound",
            details={"limit": 128},
        )


def _decode_row(row: Any, tenant_id: str) -> tuple[EmbeddedChunk, float]:
    """Builds a validated domain chunk from a driver row."""
    access = DocumentAccess(
        tenant_id=tenant_id,
        required_roles=frozenset(str(role) for role in row["required_roles"]),
        denied_principals=frozenset(str(subject) for subject in row["denied_principals"]),
    )
    metadata = dict(row["metadata"] or {})
    if row["title"]:
        metadata["title"] = str(row["title"])
    if row["source_uri"]:
        metadata["source_uri"] = str(row["source_uri"])
    chunk = Chunk(
        chunk_id=str(row["chunk_id"]),
        document_id=str(row["document_id"]),
        text=str(row["chunk_text"]),
        index=int(row["ordinal"]),
        start_char=int(row["start_char"]),
        end_char=int(row["end_char"]),
        access=access,
        metadata=metadata,
    )
    vector = tuple(float(value) for value in row["embedding"])
    model = str(row["embedding_model"])
    return EmbeddedChunk(chunk=chunk, vector=vector, model=model), float(row["similarity"])
