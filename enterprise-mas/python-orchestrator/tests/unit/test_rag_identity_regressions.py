"""Regression tests for composite RAG identities and defensive access filtering."""

from dataclasses import replace

import pytest

import mas_orchestrator.rag.index as index_module
from mas_orchestrator.contracts.errors import ConfigurationError, RetrievalError
from mas_orchestrator.rag.citations import validate_citations
from mas_orchestrator.rag.embeddings import HashingEmbedder
from mas_orchestrator.rag.index import InMemoryVectorIndex
from mas_orchestrator.rag.models import (
    Chunk,
    Citation,
    DocumentAccess,
    EmbeddedChunk,
    SecurityContext,
)
from mas_orchestrator.rag.retrieval import InMemoryLexicalIndex, Retriever, fuse


def item(tenant: str, document: str, chunk: str, text: str = "alpha") -> EmbeddedChunk:
    return EmbeddedChunk(
        chunk=Chunk(
            chunk_id=chunk,
            document_id=document,
            text=text,
            index=0,
            start_char=0,
            end_char=len(text),
            access=DocumentAccess(tenant_id=tenant),
        ),
        vector=(1.0, 0.0),
        model="model-a",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["vector", "lexical"])
async def test_same_chunk_ids_are_distinct_across_tenants_and_documents(kind: str) -> None:
    index = InMemoryVectorIndex() if kind == "vector" else InMemoryLexicalIndex()
    records = [item("a:b", "d", "c"), item("a", "d", "b:c"), item("a", "other", "b:c")]
    await index.upsert(records)
    context = SecurityContext(tenant_id="a")
    if kind == "vector":
        hits = await index.search((1.0, 0.0), context=context, limit=10)
    else:
        hits = await index.search("alpha", context=context, limit=10)
    assert {hit.chunk.document_id for hit, _ in hits} == {"d", "other"}
    assert await index.delete_document("a", "d") == 1
    if kind == "vector":
        other = await index.search((1.0, 0.0), context=SecurityContext(tenant_id="a:b"), limit=10)
    else:
        other = await index.search("alpha", context=SecurityContext(tenant_id="a:b"), limit=10)
    assert len(other) == 1


@pytest.mark.asyncio
async def test_vector_batch_failure_neither_partially_writes_nor_poisons_models(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    index = InMemoryVectorIndex()
    original = item("t", "d", "one")
    await index.upsert([original])
    with pytest.raises(ConfigurationError):
        await index.upsert(
            [item("t", "d", "two"), replace(item("t", "d", "three"), model="model-b")]
        )
    assert index.count() == 1
    await index.upsert([item("t", "d", "two")])
    monkeypatch.setattr(index_module, "MAX_INDEXED_CHUNKS", 2)
    with pytest.raises(ConfigurationError):
        await index.upsert(
            [
                replace(original, chunk=replace(original.chunk, text="changed")),
                item("t", "d", "four"),
            ]
        )
    hits = await index.search((1.0, 0.0), context=SecurityContext(tenant_id="t"), limit=10)
    assert {hit.chunk.text for hit, _ in hits} == {"alpha"}


def test_fusion_does_not_merge_chunk_ids_from_distinct_documents() -> None:
    first, second = item("t", "first", "same"), item("t", "second", "same")
    hits = fuse([(first, 1.0)], [(second, 1.0)], limit=10)
    assert {hit.chunk.document_id for hit in hits} == {"first", "second"}
    assert all(hit.vector_score is None or hit.keyword_score is None for hit in hits)


def test_citation_must_match_document_as_well_as_chunk() -> None:
    wrong_document = Citation(
        chunk_id="same", document_id="forged", document_title="wrong", quote="alpha"
    )
    checks = validate_citations([wrong_document], {("real", "same"): "alpha"})
    assert not checks[0].supported


@pytest.mark.asyncio
async def test_retriever_filters_misbehaving_backends_before_fusion() -> None:
    allowed = item("t", "d1", "a")
    other_tenant = item("other", "d2", "b")
    denied = replace(
        item("t", "d3", "c"),
        chunk=replace(
            item("t", "d3", "c").chunk,
            access=DocumentAccess(tenant_id="t", denied_principals=frozenset({"u"})),
        ),
    )

    class UnsafeIndex:
        async def search(
            self, *args: object, **kwargs: object
        ) -> list[tuple[EmbeddedChunk, float]]:
            return [(other_tenant, 1.0), (denied, 0.9), (allowed, 0.8)]

        async def upsert(self, items: object) -> None:
            pass

        async def delete_document(self, tenant_id: str, document_id: str) -> int:
            return 0

    retriever = Retriever(embedder=HashingEmbedder(), index=UnsafeIndex())
    hits = await retriever.retrieve(
        "alpha", SecurityContext(tenant_id="t", allowed_principals=frozenset({"u"}))
    )
    assert [hit.chunk.document_id for hit in hits] == ["d1"]


def test_fusion_rejects_conflicting_copies_of_the_same_chunk() -> None:
    first = item("t", "d", "same", "public data")
    conflicting = item("t", "d", "same", "stale data")
    with pytest.raises(RetrievalError, match="disagree"):
        fuse([(first, 1.0)], [(conflicting, 1.0)], limit=10)
