"""Ingestion: the pipeline from document to indexed vectors.

    ingest → parse → chunk → embed → index

Each stage is separable so it can be tested alone, and the pipeline records
what it actually did rather than only whether it succeeded. An ingestion that
silently embedded 40 of 50 chunks would leave an index that looks complete and
retrieves as though it were.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import ConfigurationError, RetrievalError
from mas_orchestrator.rag.chunking import Chunker, ChunkingConfig
from mas_orchestrator.rag.models import (
    Chunk,
    DocumentFormat,
    EmbeddedChunk,
    IngestResult,
    SourceDocument,
)
from mas_orchestrator.rag.parsing import Parser, detect_format

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.rag.embeddings import Embedder
    from mas_orchestrator.rag.index import VectorIndex
    from mas_orchestrator.rag.retrieval import LexicalIndex

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

# Embedding providers have request size limits; batching keeps ingestion inside
# them without the caller having to know what they are.
DEFAULT_BATCH_SIZE: Final[int] = 64


@dataclass(frozen=True, slots=True)
class IngestConfig:
    """Ingestion settings.

    Attributes:
        batch_size: Texts per embedding request.
        chunking: Chunk sizes.
        index_lexically: Whether to also populate the lexical index. Without
            it, hybrid search has no keyword signal to fuse.
    """

    batch_size: int = DEFAULT_BATCH_SIZE
    chunking: ChunkingConfig | None = None
    index_lexically: bool = True

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ConfigurationError("batch_size must be positive", details={"field": "batch_size"})


class IngestionPipeline:
    """Runs documents through parse → chunk → embed → index."""

    def __init__(
        self,
        *,
        embedder: Embedder,
        index: VectorIndex,
        lexical: LexicalIndex | None = None,
        parser: Parser | None = None,
        chunker: Chunker | None = None,
        config: IngestConfig | None = None,
    ) -> None:
        self._embedder = embedder
        self._index = index
        self._lexical = lexical
        self._parser = parser or Parser()
        self._config = config or IngestConfig()
        self._chunker = chunker or Chunker(self._config.chunking)

    async def ingest(
        self,
        document: SourceDocument,
        *,
        format: DocumentFormat | None = None,  # noqa: A002
    ) -> IngestResult:
        """Ingests one document.

        Args:
            document: The document to ingest.
            format: Overrides format detection.

        Returns:
            An :class:`IngestResult` reporting chunk and embedding counts.

        Raises:
            RetrievalError: If parsing fails. Embedding and indexing failures
                are reported in the result rather than raised, so one bad
                document does not abort a batch.
        """
        detected = detect_format(document.content, declared=format or document.format)

        try:
            text = self._parser.parse(document.content, detected)
        except Exception as error:
            raise RetrievalError(
                "document parsing failed",
                details={"document_id": document.document_id, "reason": str(error)},
            ) from error

        if not text.strip():
            return IngestResult(
                document_id=document.document_id,
                skipped_reason="document parsed to empty text",
            )

        chunks = self._chunker.chunk(document, text)
        if not chunks:
            return IngestResult(
                document_id=document.document_id,
                skipped_reason="document produced no chunks",
            )

        embedded_count = 0
        for batch in _batched(chunks, self._config.batch_size):
            try:
                vectors = await self._embedder.embed([chunk.text for chunk in batch])
            except Exception as error:
                _LOGGER.warning(
                    "embedding batch failed",
                    extra={
                        "event": "rag.embed_failed",
                        "document_id": document.document_id,
                        "batch": len(batch),
                        "error": type(error).__name__,
                    },
                )
                continue

            if len(vectors) != len(batch):
                _LOGGER.warning(
                    "embedder returned the wrong number of vectors",
                    extra={
                        "event": "rag.embed_mismatch",
                        "expected": len(batch),
                        "received": len(vectors),
                    },
                )
                continue

            items = [
                EmbeddedChunk(chunk=chunk, vector=vector, model=self._embedder.model)
                for chunk, vector in zip(batch, vectors, strict=True)
            ]
            await self._index.upsert(items)
            if self._config.index_lexically and self._lexical is not None:
                await self._lexical.upsert(items)
            embedded_count += len(items)

        _LOGGER.info(
            "document ingested",
            extra={
                "event": "rag.ingested",
                "document_id": document.document_id,
                "chunks": len(chunks),
                "embedded": embedded_count,
            },
        )
        return IngestResult(
            document_id=document.document_id,
            chunk_count=len(chunks),
            embedded_count=embedded_count,
        )

    async def ingest_many(
        self,
        documents: Sequence[SourceDocument],
    ) -> list[IngestResult]:
        """Ingests several documents, continuing past individual failures.

        Results are returned in input order so a caller can match a result to
        the document that produced it.
        """
        results: list[IngestResult] = []
        for document in documents:
            try:
                results.append(await self.ingest(document))
            except RetrievalError as error:
                _LOGGER.warning(
                    "document ingestion failed",
                    extra={
                        "event": "rag.ingest_failed",
                        "document_id": document.document_id,
                        "error": str(error),
                    },
                )
                results.append(
                    IngestResult(
                        document_id=document.document_id,
                        skipped_reason=str(error),
                    )
                )
        return results

    async def remove(self, document: SourceDocument) -> int:
        """Removes a document from both indexes.

        Deletion must cover every index, or a deleted document stays
        retrievable through whichever index was forgotten.
        """
        removed = await self._index.delete_document(document.access.tenant_id, document.document_id)
        if self._lexical is not None:
            await self._lexical.delete_document(document.access.tenant_id, document.document_id)
        return removed


def _batched(items: Sequence[Chunk], size: int) -> list[list[Chunk]]:
    """Splits a sequence into batches of at most ``size``."""
    return [list(items[offset : offset + size]) for offset in range(0, len(items), size)]
