"""RAG data types.

The pipeline from spec §12 is:

    ingest → parse → chunk → embed → index → retrieve
    → filter by security context → optionally hybrid search → rerank
    → construct context → cite sources → validate citations

Every stage has a type here, and the types carry what the later stages need:
a chunk knows which document it came from and where, so a citation can point
at a span rather than at "a document, somewhere".
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

from mas_orchestrator.contracts.errors import InvalidRequestError

MAX_DOCUMENT_BYTES: Final[int] = 32 * 1024 * 1024
MAX_CHUNK_CHARS: Final[int] = 8_000
MAX_EMBEDDING_DIMENSIONS: Final[int] = 4_096


class DocumentFormat(StrEnum):
    """Parsed source formats."""

    TEXT = "text"
    MARKDOWN = "markdown"
    JSON = "json"
    HTML = "html"
    CSV = "csv"
    PDF = "pdf"
    DOCX = "docx"


@dataclass(frozen=True, slots=True)
class SecurityContext:
    """Who may see a document.

    Retrieval filters on this *before* ranking, so a document a caller may not
    see is never scored — and therefore can never surface through a relevance
    shortcut.

    Attributes:
        tenant_id: Owning tenant. Always required.
        allowed_roles: Roles permitted to read. Empty means no role gate.
        allowed_principals: Explicit principal ids. Empty means no gate.
        classification: Minimum sensitivity handled by this index.
    """

    tenant_id: str
    allowed_roles: frozenset[str] = frozenset()
    allowed_principals: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "security context must name a tenant", details={"field": "tenant_id"}
            )


@dataclass(frozen=True, slots=True)
class DocumentAccess:
    """The access rules attached to one document.

    Attributes:
        tenant_id: Owning tenant.
        required_roles: A reader must hold at least one. Empty means open
            within the tenant.
        denied_principals: Explicitly barred, overriding roles.
    """

    tenant_id: str
    required_roles: frozenset[str] = frozenset()
    denied_principals: frozenset[str] = frozenset()

    def permits(self, context: SecurityContext) -> bool:
        """Whether a reader may see this document.

        Tenant mismatch is an absolute refusal — a role match in another tenant
        grants nothing.
        """
        if context.tenant_id != self.tenant_id:
            return False
        if context.allowed_principals & self.denied_principals:
            return False
        if not self.required_roles:
            return True
        return bool(self.required_roles & context.allowed_roles)


@dataclass(frozen=True, slots=True)
class SourceDocument:
    """A document entered into the pipeline.

    Attributes:
        document_id: Stable identifier.
        title: Human-readable title.
        content: Raw content.
        format: Source format.
        access: Who may read it.
        source_uri: Where it came from, for citations.
        metadata: Bounded annotations.
    """

    document_id: str
    title: str
    content: str | bytes
    format: DocumentFormat = DocumentFormat.TEXT
    access: DocumentAccess = field(default_factory=lambda: DocumentAccess(tenant_id=""))
    source_uri: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.document_id.strip():
            raise InvalidRequestError(
                "document id must not be empty", details={"field": "document_id"}
            )
        if isinstance(self.content, bytes):
            if not self.content:
                raise InvalidRequestError(
                    "document content must not be empty", details={"field": "content"}
                )
            encoded = len(self.content)
        else:
            if not self.content.strip():
                raise InvalidRequestError(
                    "document content must not be empty", details={"field": "content"}
                )
            encoded = len(self.content.encode("utf-8"))
        if encoded > MAX_DOCUMENT_BYTES:
            raise InvalidRequestError(
                "document exceeds the maximum size",
                details={"limit": MAX_DOCUMENT_BYTES, "size": encoded},
            )
        if not self.access.tenant_id.strip():
            raise InvalidRequestError(
                "document must belong to a tenant", details={"field": "access"}
            )


@dataclass(frozen=True, slots=True)
class Chunk:
    """A slice of a document.

    Attributes:
        chunk_id: Unique identifier.
        document_id: Owning document.
        text: The chunk text.
        index: Position within the document.
        start_char: Offset of the chunk in the document.
        end_char: End offset, exclusive.
        access: Access rules, copied from the document so retrieval can filter
            without a join back to the document.
        metadata: Bounded annotations.
    """

    chunk_id: str
    document_id: str
    text: str
    index: int
    start_char: int
    end_char: int
    access: DocumentAccess
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.chunk_id.strip():
            raise InvalidRequestError("chunk id must not be empty", details={"field": "chunk_id"})
        if not self.text.strip():
            raise InvalidRequestError("chunk text must not be empty", details={"field": "text"})
        if len(self.text) > MAX_CHUNK_CHARS:
            raise InvalidRequestError(
                "chunk exceeds the maximum size",
                details={"limit": MAX_CHUNK_CHARS, "size": len(self.text)},
            )
        if self.end_char < self.start_char:
            raise InvalidRequestError("chunk offsets are inverted", details={"field": "end_char"})


@dataclass(frozen=True, slots=True)
class EmbeddedChunk:
    """A chunk with its vector.

    Attributes:
        chunk: The chunk.
        vector: The embedding.
        model: Which embedding model produced it, recorded because a vector
            from one model is not comparable with a vector from another.
    """

    chunk: Chunk
    vector: tuple[float, ...]
    model: str

    def __post_init__(self) -> None:
        if not self.vector:
            raise InvalidRequestError(
                "embedding vector must not be empty", details={"field": "vector"}
            )
        if len(self.vector) > MAX_EMBEDDING_DIMENSIONS:
            raise InvalidRequestError(
                "embedding exceeds the maximum dimension",
                details={"limit": MAX_EMBEDDING_DIMENSIONS},
            )
        if not self.model.strip():
            raise InvalidRequestError("embedding model must be named", details={"field": "model"})


@dataclass(frozen=True, slots=True)
class RetrievalHit:
    """A chunk that matched, with its scores.

    Attributes:
        chunk: The chunk.
        score: Final ranking score.
        vector_score: Similarity from the vector search, when used.
        keyword_score: Lexical score, when a hybrid search was run.
        rerank_score: Score from the reranker, when one ran.
        document_title: Title, carried for citation rendering.
        source_uri: Origin, for citations.
    """

    chunk: Chunk
    score: float
    vector_score: float | None = None
    keyword_score: float | None = None
    rerank_score: float | None = None
    document_title: str = ""
    source_uri: str | None = None


@dataclass(frozen=True, slots=True)
class Citation:
    """A reference to the source of a retrieved passage.

    Attributes:
        chunk_id: The chunk cited.
        document_id: The document it came from.
        document_title: Title for display.
        source_uri: Origin, when known.
        quote: The exact text cited, validated against the chunk.
        start_char: Offset within the document.
        end_char: End offset.
    """

    chunk_id: str
    document_id: str
    document_title: str
    quote: str
    start_char: int = 0
    end_char: int = 0
    source_uri: str | None = None

    def __post_init__(self) -> None:
        if not self.chunk_id.strip():
            raise InvalidRequestError("citation must name a chunk", details={"field": "chunk_id"})
        if not self.quote.strip():
            raise InvalidRequestError("citation must quote the source", details={"field": "quote"})


@dataclass(frozen=True, slots=True)
class RagContext:
    """Constructed context ready for a prompt.

    Attributes:
        text: The assembled context.
        citations: Citations, in the same order as the passages in ``text``.
        hit_count: How many passages were used.
        truncated: Whether the context hit a size ceiling.
        query: The query that produced it.
    """

    text: str
    citations: tuple[Citation, ...] = ()
    hit_count: int = 0
    truncated: bool = False
    query: str = ""


def new_chunk_id() -> str:
    """Generates a fresh chunk identifier."""
    return f"chk-{uuid.uuid4().hex}"


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Outcome of ingesting one document.

    Attributes:
        document_id: The document.
        chunk_count: Chunks produced.
        embedded_count: Chunks embedded. Fewer than ``chunk_count`` means the
            embedding step failed for some chunks and the shortfall is
            reported rather than hidden.
        skipped: Whether the document was skipped entirely, and why.
    """

    document_id: str
    chunk_count: int = 0
    embedded_count: int = 0
    skipped_reason: str | None = None

    @property
    def complete(self) -> bool:
        """Whether every chunk was embedded."""
        return self.skipped_reason is None and self.embedded_count == self.chunk_count
