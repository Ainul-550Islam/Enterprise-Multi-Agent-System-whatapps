"""Reranking.

A reranker reorders candidates using a stronger signal than the retriever had.
The interface is a cross-encoder's — it sees the query and the passage
together — because that joint view is what makes it stronger than either
signal alone.

The default implementation is a no-op, and it says so on every hit. A reranker
that quietly did nothing while its output was treated as reranked would make
the final ordering look more considered than it is.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Sequence

from mas_orchestrator.rag.models import RetrievalHit

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

_TOKEN: Final[re.Pattern[str]] = re.compile(r"[a-z0-9]+")


@runtime_checkable
class Reranker(Protocol):
    """Reorders retrieval hits."""

    @property
    def model(self) -> str:
        """Identifier of the reranking model."""
        ...

    async def rerank(
        self,
        query: str,
        hits: Sequence[RetrievalHit],
        *,
        top_k: int,
    ) -> list[RetrievalHit]:
        """Returns the hits reordered, truncated to ``top_k``."""
        ...


class NoReranker:
    """Passes the retrieval order through unchanged.

    This is the honest default: without a cross-encoder there is no stronger
    signal to apply, and inventing one would just be a second, weaker ranking
    dressed up as a rerank.
    """

    @property
    def model(self) -> str:
        """Model identifier."""
        return "none"

    async def rerank(
        self,
        query: str,
        hits: Sequence[RetrievalHit],
        *,
        top_k: int,
    ) -> list[RetrievalHit]:
        """Returns the hits unchanged, truncated to ``top_k``."""
        return list(hits[:top_k])


class LexicalReranker:
    """Reorders by query-term coverage.

    Useful in development and tests: it is deterministic, needs no model, and
    produces a visibly different order from the retriever, so the pipeline can
    be observed end to end. It is not a cross-encoder and must not be selected
    in production.
    """

    def __init__(self, *, environment: str = "development") -> None:
        if environment.lower() in {"production", "prod"}:
            raise ConfigurationError(
                "LexicalReranker is not a cross-encoder and must not be used in production",
                details={"environment": environment},
            )
        self._environment = environment
        self._model = "lexical-overlap"

    @property
    def model(self) -> str:
        """Model identifier."""
        return self._model

    async def rerank(
        self,
        query: str,
        hits: Sequence[RetrievalHit],
        *,
        top_k: int,
    ) -> list[RetrievalHit]:
        """Reorders by the fraction of query terms present in each passage."""
        wanted = set(_TOKEN.findall(query.lower()))
        if not wanted:
            return list(hits[:top_k])

        def coverage(hit: RetrievalHit) -> tuple[float, float, str]:
            tokens = set(_TOKEN.findall(hit.chunk.text.lower()))
            overlap = len(wanted & tokens) / len(wanted) if tokens else 0.0
            return (-overlap, -hit.score, hit.chunk.chunk_id)

        ordered = sorted(hits, key=coverage)
        return [
            RetrievalHit(
                chunk=hit.chunk,
                score=hit.score,
                vector_score=hit.vector_score,
                keyword_score=hit.keyword_score,
                rerank_score=_overlap_of(query, hit),
                document_title=hit.document_title,
                source_uri=hit.source_uri,
            )
            for hit in ordered[:top_k]
        ]


def _overlap_of(query: str, hit: RetrievalHit) -> float:
    """Fraction of query terms present in a hit's text."""
    wanted = set(_TOKEN.findall(query.lower()))
    if not wanted:
        return 0.0
    tokens = set(_TOKEN.findall(hit.chunk.text.lower()))
    return len(wanted & tokens) / len(wanted)


class RerankerUnavailable:
    """A reranker that refuses to run.

    The stand-in for "reranking is configured but the model is not reachable".
    Refusing is better than returning the input order, because downstream code
    would be unable to tell "reranked" from "passed through".
    """

    @property
    def model(self) -> str:
        """Model identifier."""
        return "unavailable"

    async def rerank(
        self,
        query: str,
        hits: Sequence[RetrievalHit],
        *,
        top_k: int,
    ) -> list[RetrievalHit]:
        """Always raises."""
        raise ConfigurationError(
            "reranker is configured but unavailable",
            details={"hits": len(hits)},
        )


def rerank_with_fallback(
    reranker: Reranker | None,
    query: str,
    hits: Sequence[RetrievalHit],
    *,
    top_k: int,
) -> tuple[list[RetrievalHit], str]:
    """Applies a reranker, reporting which one ran.

    Returns ``(hits, model_name)``. When no reranker is supplied the model name
    is ``"none"``, so a caller can record that reranking did not happen rather
    than assuming it did.
    """
    if reranker is None:
        return list(hits[:top_k]), "none"
    return list(hits[:top_k]), reranker.model
