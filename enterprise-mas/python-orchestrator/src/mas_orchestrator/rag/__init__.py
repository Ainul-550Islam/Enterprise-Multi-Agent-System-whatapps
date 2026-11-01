"""Retrieval-augmented generation.

Implements the spec §12 pipeline:

    ingest → parse → chunk → embed → index → retrieve
    → filter by security context → optionally hybrid search → rerank
    → construct context → cite sources → validate citations

Two properties are structural rather than incidental:

* **Security filtering precedes ranking.** A document the reader may not see is
  never scored, so it cannot surface through a relevance short-cut.
* **Nothing here pretends to be production infrastructure.** The in-memory
  vector index and the hashing embedder are refused in production by their
  constructors, not merely discouraged in a comment. A vector database
  connection is configurable; when it is absent, the pipeline fails loudly
  rather than returning an empty result set that looks like "nothing relevant".

Retrieved content is fenced as untrusted before it reaches a prompt, because
documents are written by whoever wrote them.
"""

from __future__ import annotations

from mas_orchestrator.rag.chunking import (
    Chunker,
    ChunkingConfig,
    chunk_document,
)
from mas_orchestrator.rag.citations import (
    MAX_CONTEXT_CHARS,
    MIN_QUOTE_SIMILARITY,
    CitationCheck,
    ContextConfig,
    assert_supported,
    build_citation,
    construct_context,
    referenced_numbers,
    references_are_in_range,
    validate_citation,
    validate_citations,
)
from mas_orchestrator.rag.embeddings import (
    Embedder,
    EmbedderUnavailable,
    HashingEmbedder,
    OpenAIEmbedder,
    cosine_similarity,
)
from mas_orchestrator.rag.index import (
    MAX_INDEXED_CHUNKS,
    InMemoryVectorIndex,
    VectorIndex,
    VectorIndexUnavailable,
    new_index_id,
)
from mas_orchestrator.rag.ingestion import (
    DEFAULT_BATCH_SIZE,
    IngestConfig,
    IngestionPipeline,
)
from mas_orchestrator.rag.models import (
    MAX_CHUNK_CHARS,
    MAX_DOCUMENT_BYTES,
    Chunk,
    Citation,
    DocumentAccess,
    DocumentFormat,
    EmbeddedChunk,
    IngestResult,
    RagContext,
    RetrievalHit,
    SecurityContext,
    SourceDocument,
    new_chunk_id,
)
from mas_orchestrator.rag.parsing import (
    MAX_PARSED_CHARS,
    Parser,
    detect_format,
)
from mas_orchestrator.rag.pgvector_index import PgVectorIndex
from mas_orchestrator.rag.reranking import (
    LexicalReranker,
    NoReranker,
    Reranker,
    RerankerUnavailable,
    rerank_with_fallback,
)
from mas_orchestrator.rag.retrieval import (
    RRF_K,
    InMemoryLexicalIndex,
    LexicalIndex,
    RetrievalConfig,
    Retriever,
    filter_by_access,
    fuse,
)
from mas_orchestrator.rag.security import (
    REASON_DENIED_PRINCIPAL,
    REASON_MISSING_ROLE,
    REASON_OK,
    REASON_TENANT_MISMATCH,
    AccessDecision,
    assert_tenant_isolated,
    explain,
    filter_chunks,
    filter_documents,
    permits,
    union_access,
    visible_fraction,
)

__all__ = [
    "DEFAULT_BATCH_SIZE",
    "MAX_CHUNK_CHARS",
    "MAX_CONTEXT_CHARS",
    "MAX_DOCUMENT_BYTES",
    "MAX_INDEXED_CHUNKS",
    "MAX_PARSED_CHARS",
    "MIN_QUOTE_SIMILARITY",
    "REASON_DENIED_PRINCIPAL",
    "REASON_MISSING_ROLE",
    "REASON_OK",
    "REASON_TENANT_MISMATCH",
    "RRF_K",
    "AccessDecision",
    "Chunk",
    "Chunker",
    "ChunkingConfig",
    "Citation",
    "CitationCheck",
    "ContextConfig",
    "DocumentAccess",
    "DocumentFormat",
    "EmbeddedChunk",
    "Embedder",
    "EmbedderUnavailable",
    "HashingEmbedder",
    "InMemoryLexicalIndex",
    "InMemoryVectorIndex",
    "IngestConfig",
    "IngestResult",
    "IngestionPipeline",
    "LexicalIndex",
    "LexicalReranker",
    "NoReranker",
    "OpenAIEmbedder",
    "Parser",
    "PgVectorIndex",
    "RagContext",
    "Reranker",
    "RerankerUnavailable",
    "RetrievalConfig",
    "RetrievalHit",
    "Retriever",
    "SecurityContext",
    "SourceDocument",
    "VectorIndex",
    "VectorIndexUnavailable",
    "assert_supported",
    "assert_tenant_isolated",
    "build_citation",
    "chunk_document",
    "construct_context",
    "cosine_similarity",
    "detect_format",
    "explain",
    "filter_by_access",
    "filter_chunks",
    "filter_documents",
    "fuse",
    "new_chunk_id",
    "new_index_id",
    "permits",
    "referenced_numbers",
    "references_are_in_range",
    "rerank_with_fallback",
    "union_access",
    "validate_citation",
    "validate_citations",
    "visible_fraction",
]
