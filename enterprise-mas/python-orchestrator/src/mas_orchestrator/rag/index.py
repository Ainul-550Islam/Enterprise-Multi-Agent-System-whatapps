"""Vector indexes.

A vector database connection must be configurable (spec §12), and a local
in-memory index is acceptable — but only for development and tests, and it must
be *prevented* from being selected in production rather than merely discouraged.

That last point is why :class:`InMemoryVectorIndex` raises in its constructor
for a production environment. A warning would be read past; a refusal cannot
be.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.rag.embeddings import cosine_similarity

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.rag.models import EmbeddedChunk, SecurityContext

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_INDEXED_CHUNKS: Final[int] = 100_000


@runtime_checkable
class VectorIndex(Protocol):
    """What a vector index must do."""

    async def upsert(self, items: Sequence[EmbeddedChunk]) -> None:
        """Inserts or replaces vectors."""
        ...

    async def search(
        self,
        vector: Sequence[float],
        *,
        context: SecurityContext,
        limit: int,
    ) -> list[tuple[EmbeddedChunk, float]]:
        """Searches for nearest neighbours within one tenant's access rules.

        Security filtering happens inside the search, before ranking, so a
        forbidden document is never scored and can never surface through a
        similarity short-cut.
        """
        ...

    async def delete_document(self, tenant_id: str, document_id: str) -> int:
        """Removes every vector for a document. Returns the count."""
        ...


class InMemoryVectorIndex:
    """A brute-force in-memory index for development and tests.

    Scans every vector on each query, which is fine at development scale and
    hopeless in production — one more reason it refuses to run there.
    """

    def __init__(self, *, environment: Environment = Environment.DEVELOPMENT) -> None:
        if environment is Environment.PRODUCTION:
            raise ConfigurationError(
                "InMemoryVectorIndex must not be used in production; "
                "configure a vector database connection instead",
                details={"environment": environment.value},
            )
        self._environment = environment
        self._items: dict[str, EmbeddedChunk] = {}
        self._models: set[str] = set()

    async def upsert(self, items: Sequence[EmbeddedChunk]) -> None:
        """Inserts or replaces vectors.

        Raises:
            ConfigurationError: If the batch mixes embedding models or the
                index is full.
        """
        for item in items:
            self._models.add(item.model)
            if len(self._models) > 1:
                raise ConfigurationError(
                    "index cannot hold vectors from more than one embedding model",
                    details={"models": sorted(self._models)},
                )
            key = f"{item.chunk.access.tenant_id}:{item.chunk.chunk_id}"
            if key not in self._items and len(self._items) >= MAX_INDEXED_CHUNKS:
                raise ConfigurationError(
                    "in-memory vector index is full",
                    details={"limit": MAX_INDEXED_CHUNKS},
                )
            self._items[key] = item

    async def search(
        self,
        vector: Sequence[float],
        *,
        context: SecurityContext,
        limit: int,
    ) -> list[tuple[EmbeddedChunk, float]]:
        """Searches within the caller's access rules."""
        if limit < 1:
            raise ConfigurationError("search limit must be positive", details={"limit": limit})

        scored: list[tuple[EmbeddedChunk, float]] = []
        for item in self._items.values():
            if not item.chunk.access.permits(context):
                continue
            scored.append((item, cosine_similarity(vector, item.vector)))

        scored.sort(key=lambda pair: (-pair[1], pair[0].chunk.chunk_id))
        return scored[:limit]

    async def delete_document(self, tenant_id: str, document_id: str) -> int:
        """Removes every vector belonging to a document."""
        doomed = [
            key
            for key, item in self._items.items()
            if item.chunk.access.tenant_id == tenant_id and item.chunk.document_id == document_id
        ]
        for key in doomed:
            del self._items[key]
        return len(doomed)

    def count(self, tenant_id: str | None = None) -> int:
        """Number of indexed vectors, for tests."""
        if tenant_id is None:
            return len(self._items)
        return sum(1 for item in self._items.values() if item.chunk.access.tenant_id == tenant_id)


class VectorIndexUnavailable:
    """An index that always fails.

    The placeholder for "no vector database configured". Failing loudly beats
    returning an empty result set, which a caller could easily read as "nothing
    relevant exists" rather than "retrieval is not wired up".
    """

    async def upsert(self, items: Sequence[EmbeddedChunk]) -> None:
        """Always raises."""
        raise ConfigurationError("no vector index is configured", details={"items": len(items)})

    async def search(
        self,
        vector: Sequence[float],
        *,
        context: SecurityContext,
        limit: int,
    ) -> list[tuple[EmbeddedChunk, float]]:
        """Always raises."""
        raise ConfigurationError("no vector index is configured")

    async def delete_document(self, tenant_id: str, document_id: str) -> int:
        """Always raises."""
        raise ConfigurationError("no vector index is configured")


def new_index_id() -> str:
    """Generates an identifier for an index instance."""
    return f"idx-{uuid.uuid4().hex}"
