"""Retrieval: vector search, lexical search, security filtering, fusion.

The security filter runs *before* ranking, not after. That ordering is the
whole point: if forbidden documents were ranked and then removed, a relevance
score could still leak information about them, and a bug in the filter would
surface a document that should never have been a candidate.

Hybrid search fuses the two rankings with reciprocal rank fusion, which uses
rank positions rather than raw scores. Raw cosine similarity and a lexical
score are not on the same scale, and normalising them to fake a common scale is
how one signal silently dominates the other.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import ConfigurationError, RetrievalError
from mas_orchestrator.rag.models import (
    EmbeddedChunk,
    RetrievalHit,
    SecurityContext,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.rag.embeddings import Embedder
    from mas_orchestrator.rag.index import VectorIndex

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

_TOKEN: Final[re.Pattern[str]] = re.compile(r"[a-z0-9]+")

# Reciprocal rank fusion constant. Larger values flatten the difference between
# top ranks; 60 is the value from the original paper and is a reasonable
# default rather than a tuned one.
RRF_K: Final[int] = 60


@runtime_checkable
class LexicalIndex(Protocol):
    """A keyword search over chunk text.

    Mirrors :class:`~mas_orchestrator.rag.index.VectorIndex` in shape, so a
    backend can implement both and ingestion can keep them in step. Deletion
    is part of the interface rather than optional: an index that can be
    written but not deleted leaves content retrievable after it was removed.
    """

    async def upsert(self, items: Sequence[EmbeddedChunk]) -> None:
        """Adds or replaces chunks."""
        ...

    async def search(
        self,
        query: str,
        *,
        context: SecurityContext,
        limit: int,
    ) -> list[tuple[EmbeddedChunk, float]]:
        """Keyword search within the caller's access rules."""
        ...

    async def delete_document(self, tenant_id: str, document_id: str) -> int:
        """Removes every chunk for a document. Returns the count."""
        ...


class InMemoryLexicalIndex:
    """A BM25-flavoured keyword index for development and tests."""

    def __init__(self) -> None:
        self._items: dict[str, EmbeddedChunk] = {}

    async def upsert(self, items: Sequence[EmbeddedChunk]) -> None:
        """Adds chunks to the index."""
        for item in items:
            self._items[item.chunk.chunk_id] = item

    async def search(
        self,
        query: str,
        *,
        context: SecurityContext,
        limit: int,
    ) -> list[tuple[EmbeddedChunk, float]]:
        """Scores chunks by term overlap, normalised by chunk length."""
        wanted = set(_TOKEN.findall(query.lower()))
        if not wanted:
            return []

        scored: list[tuple[EmbeddedChunk, float]] = []
        for item in self._items.values():
            if not item.chunk.access.permits(context):
                continue
            tokens = _TOKEN.findall(item.chunk.text.lower())
            if not tokens:
                continue
            matches = sum(1 for token in tokens if token in wanted)
            if matches == 0:
                continue
            # Length-normalised term frequency. Not true BM25: there is no
            # inverse-document-frequency term, because a development index has
            # no meaningful document statistics to draw one from.
            scored.append((item, matches / len(tokens)))

        scored.sort(key=lambda pair: (-pair[1], pair[0].chunk.chunk_id))
        return scored[:limit]

    async def delete_document(self, tenant_id: str, document_id: str) -> int:
        """Removes chunks belonging to a document."""
        doomed = [
            key
            for key, item in self._items.items()
            if item.chunk.access.tenant_id == tenant_id and item.chunk.document_id == document_id
        ]
        for key in doomed:
            del self._items[key]
        return len(doomed)


@dataclass(frozen=True, slots=True)
class RetrievalConfig:
    """Retrieval settings.

    Attributes:
        top_k: Final number of hits.
        candidate_k: Candidates drawn from each index before fusion. Larger
            than ``top_k`` so fusion has something to work with.
        hybrid: Whether to run the lexical index alongside the vector index.
    """

    top_k: int = 8
    candidate_k: int = 24
    hybrid: bool = True

    def __post_init__(self) -> None:
        if self.top_k < 1:
            raise ConfigurationError("top_k must be positive", details={"field": "top_k"})
        if self.candidate_k < self.top_k:
            raise ConfigurationError(
                "candidate_k must not be below top_k",
                details={"field": "candidate_k", "top_k": self.top_k},
            )


class Retriever:
    """Runs the retrieval stages of the pipeline."""

    def __init__(
        self,
        *,
        embedder: Embedder,
        index: VectorIndex,
        lexical: LexicalIndex | None = None,
        config: RetrievalConfig | None = None,
    ) -> None:
        self._embedder = embedder
        self._index = index
        self._lexical = lexical
        self._config = config or RetrievalConfig()

    async def retrieve(
        self,
        query: str,
        context: SecurityContext,
        *,
        top_k: int | None = None,
    ) -> list[RetrievalHit]:
        """Retrieves passages for a query.

        Args:
            query: The search query.
            context: The reader's security context.
            top_k: Override for the configured ceiling; the smaller of the two
                wins, so a caller cannot widen the result set beyond policy.

        Returns:
            Hits ordered by fused score. Every hit has already passed the
            access check.

        Raises:
            RetrievalError: If retrieval fails.
        """
        if not query.strip():
            return []

        limit = min(top_k or self._config.top_k, self._config.top_k)

        try:
            vectors = await self._embedder.embed([query])
        except ConfigurationError as error:
            raise RetrievalError(
                "query embedding failed", details={"reason": str(error)}
            ) from error

        if not vectors:
            raise RetrievalError("embedder returned no vector for the query")

        try:
            vector_hits = await self._index.search(
                vectors[0], context=context, limit=self._config.candidate_k
            )
        except ConfigurationError as error:
            raise RetrievalError("vector search failed", details={"reason": str(error)}) from error

        keyword_hits: list[tuple[EmbeddedChunk, float]] = []
        if self._config.hybrid and self._lexical is not None:
            keyword_hits = await self._lexical.search(
                query, context=context, limit=self._config.candidate_k
            )

        fused = fuse(vector_hits, keyword_hits, limit=limit)

        _LOGGER.info(
            "retrieval complete",
            extra={
                "event": "rag.retrieved",
                "tenant_id": context.tenant_id,
                "vector_hits": len(vector_hits),
                "keyword_hits": len(keyword_hits),
                "returned": len(fused),
            },
        )
        return fused


def fuse(
    vector_hits: Sequence[tuple[EmbeddedChunk, float]],
    keyword_hits: Sequence[tuple[EmbeddedChunk, float]],
    *,
    limit: int,
) -> list[RetrievalHit]:
    """Fuses two ranked lists with reciprocal rank fusion.

    Each list contributes ``1 / (k + rank)`` per item. An item present in both
    lists accumulates both contributions, which is how agreement between the
    two signals is expressed without needing their scores to be comparable.
    """
    scores: dict[str, float] = {}
    vector_scores: dict[str, float] = {}
    keyword_scores: dict[str, float] = {}
    items: dict[str, EmbeddedChunk] = {}

    for rank, (item, score) in enumerate(vector_hits, start=1):
        key = item.chunk.chunk_id
        items[key] = item
        vector_scores[key] = score
        scores[key] = scores.get(key, 0.0) + 1.0 / (RRF_K + rank)

    for rank, (item, score) in enumerate(keyword_hits, start=1):
        key = item.chunk.chunk_id
        items[key] = item
        keyword_scores[key] = score
        scores[key] = scores.get(key, 0.0) + 1.0 / (RRF_K + rank)

    ordered = sorted(scores.items(), key=lambda pair: (-pair[1], pair[0]))

    hits: list[RetrievalHit] = []
    for key, score in ordered[:limit]:
        item = items[key]
        hits.append(
            RetrievalHit(
                chunk=item.chunk,
                score=score,
                vector_score=vector_scores.get(key),
                keyword_score=keyword_scores.get(key),
                document_title=str(item.chunk.metadata.get("title", "")),
                source_uri=item.chunk.metadata.get("source_uri"),
            )
        )
    return hits


def filter_by_access(
    hits: Sequence[RetrievalHit],
    context: SecurityContext,
) -> list[RetrievalHit]:
    """Removes hits the reader may not see.

    Retrieval already filters, so this is a second pass rather than the only
    one. It exists because a caller may hold hits that came from somewhere
    else — a cache, a hand-built list — and needs the same guarantee.
    """
    return [hit for hit in hits if hit.chunk.access.permits(context)]
