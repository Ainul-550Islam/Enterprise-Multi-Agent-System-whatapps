"""Hybrid retrieval must not bypass pgvector ownership and deletion fences."""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.rag.embeddings import HashingEmbedder
from mas_orchestrator.rag.index import InMemoryVectorIndex
from mas_orchestrator.rag.models import Chunk, DocumentAccess, EmbeddedChunk, SecurityContext
from mas_orchestrator.rag.pgvector_index import PgVectorIndex
from mas_orchestrator.rag.retrieval import InMemoryLexicalIndex, RetrievalConfig, Retriever


@pytest.mark.asyncio
@pytest.mark.parametrize("allow_unowned", [False, True])
async def test_pgvector_rejects_hybrid_index_that_can_bypass_fences(
    allow_unowned: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    lexical = InMemoryLexicalIndex()
    embedder = HashingEmbedder()
    item = EmbeddedChunk(
        chunk=Chunk(
            chunk_id="chunk",
            document_id="document",
            text="confidential example",
            index=0,
            start_char=0,
            end_char=20,
            access=DocumentAccess(tenant_id="tenant"),
        ),
        vector=(await embedder.embed(["confidential example"]))[0],
        model=embedder.model,
    )
    await lexical.upsert([item])
    # A tenant-only lexical index would return this without knowing whether
    # the document is fenced, belongs to another project, or was deleted.
    assert await lexical.search(
        "confidential", context=SecurityContext(tenant_id="tenant"), limit=1
    )
    index = PgVectorIndex(object(), dimensions=1536, allow_unowned_documents=allow_unowned)
    with pytest.raises(ConfigurationError, match="does not support a separate lexical index"):
        Retriever(embedder=embedder, index=index, lexical=lexical)

    # An inactive lexical dependency cannot contribute hits, and generic
    # in-memory hybrid retrieval remains available.
    async def no_vector_hits(*args: object, **kwargs: object) -> list[object]:
        return []

    monkeypatch.setattr(index, "search", no_vector_hits)
    retriever = Retriever(
        embedder=embedder, index=index, lexical=lexical, config=RetrievalConfig(hybrid=False)
    )
    assert not await retriever.retrieve("confidential", SecurityContext(tenant_id="tenant"))
    generic = Retriever(embedder=embedder, index=InMemoryVectorIndex(), lexical=lexical)
    assert len(await generic.retrieve("confidential", SecurityContext(tenant_id="tenant"))) == 1


def test_wrapped_pgvector_cannot_bypass_separate_lexical_guard() -> None:
    class WrappedIndex:
        def __init__(self, wrapped: PgVectorIndex) -> None:
            self.wrapped = wrapped

        async def search(self, *args: object, **kwargs: object) -> list[object]:
            return []

        async def upsert(self, items: object) -> None:
            pass

        async def delete_document(self, tenant_id: str, document_id: str) -> int:
            return 0

    wrapper = WrappedIndex(PgVectorIndex(object(), dimensions=1536))
    with pytest.raises(ConfigurationError, match="does not support a separate lexical index"):
        Retriever(embedder=HashingEmbedder(), index=wrapper, lexical=InMemoryLexicalIndex())


def test_custom_lexical_backend_cannot_bypass_development_pair_guard() -> None:
    class UnfencedLexical(InMemoryLexicalIndex):
        pass

    with pytest.raises(ConfigurationError, match="does not support a separate lexical index"):
        Retriever(
            embedder=HashingEmbedder(), index=InMemoryVectorIndex(), lexical=UnfencedLexical()
        )
