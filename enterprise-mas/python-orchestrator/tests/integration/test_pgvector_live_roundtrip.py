"""Real pgvector round-trip through the least-privilege RAG pool."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from uuid import uuid4

import pytest

from mas_orchestrator.contracts.errors import RetrievalError
from mas_orchestrator.rag.models import Chunk, DocumentAccess, EmbeddedChunk, SecurityContext
from mas_orchestrator.rag.pgvector_index import PgVectorIndex

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_live_vector_roundtrip_and_access_isolation() -> None:
    """Stored vectors can be retrieved only under the right tenant and roles."""
    tenant = str(uuid4())
    other_tenant = str(uuid4())
    document = str(uuid4())
    vector = (1.0,) + (0.0,) * 1535
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"],
        dimensions=1536,
        database_role="mas_rag",
        allow_unowned_documents=True,
        min_size=1,
        max_size=1,
    )
    try:
        await index.upsert(
            [
                EmbeddedChunk(
                    chunk=Chunk(
                        chunk_id="live-roundtrip-chunk",
                        document_id=document,
                        text="Index test content",
                        index=0,
                        start_char=0,
                        end_char=18,
                        access=DocumentAccess(
                            tenant_id=tenant,
                            required_roles=frozenset({"reader"}),
                            denied_principals=frozenset({"blocked"}),
                        ),
                        metadata={"title": "Index test"},
                    ),
                    vector=vector,
                    model="test-vector-model",
                )
            ]
        )
        assert not await index.search(
            vector, context=SecurityContext(tenant_id=other_tenant), limit=5
        )
        assert not await index.search(vector, context=SecurityContext(tenant_id=tenant), limit=5)
        assert not await index.search(
            vector,
            context=SecurityContext(
                tenant_id=tenant,
                allowed_roles=frozenset({"reader"}),
                allowed_principals=frozenset({"blocked"}),
            ),
            limit=5,
        )
        hits = await index.search(
            vector,
            context=SecurityContext(tenant_id=tenant, allowed_roles=frozenset({"reader"})),
            limit=5,
        )
        assert len(hits) == 1
        assert hits[0][0].chunk.document_id == document
        assert hits[0][0].chunk.text == "Index test content"
        assert hits[0][1] == pytest.approx(1.0)
        assert await index.delete_document(other_tenant, document) == 0
    finally:
        await index.delete_document(tenant, document)
        await index.aclose()


@pytest.mark.asyncio
async def test_document_replace_is_atomic_on_retry_and_database_error() -> None:
    """Failed replacement rolls back deletion; repeated replacement has no stale rows."""
    tenant, document = str(uuid4()), str(uuid4())
    vector = (1.0,) + (0.0,) * 1535
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"],
        dimensions=1536,
        database_role="mas_rag",
        allow_unowned_documents=True,
    )
    access = DocumentAccess(tenant_id=tenant)
    first = EmbeddedChunk(
        chunk=Chunk(
            chunk_id="first",
            document_id=document,
            text="old content",
            index=0,
            start_char=0,
            end_char=11,
            access=access,
        ),
        vector=vector,
        model="test-vector-model",
    )
    second = replace(first, chunk=replace(first.chunk, chunk_id="second", text="new content"))
    try:
        await index.replace_document([first])
        await index.replace_document([second])
        await index.replace_document([second])
        context = SecurityContext(tenant_id=tenant)
        found = await index.search(vector, context=context, limit=10)
        assert [hit[0].chunk.text for hit in found] == ["new content"]
        # The row violates a database CHECK only after the DELETE has run.
        invalid = replace(second, chunk=replace(second.chunk, start_char=-1))
        with pytest.raises(RetrievalError):
            await index.replace_document([invalid])
        found = await index.search(vector, context=context, limit=10)
        assert [hit[0].chunk.text for hit in found] == ["new content"]
    finally:
        await index.delete_document(tenant, document)
        await index.aclose()


@pytest.mark.asyncio
async def test_concurrent_document_replacements_never_mix_snapshots() -> None:
    """Separate PostgreSQL sessions serialize whole-document replacement."""
    tenant, document = str(uuid4()), str(uuid4())
    vector = (1.0,) + (0.0,) * 1535
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"],
        dimensions=1536,
        database_role="mas_rag",
        allow_unowned_documents=True,
        min_size=2,
        max_size=2,
    )

    def snapshot(prefix: str) -> list[EmbeddedChunk]:
        return [
            EmbeddedChunk(
                chunk=Chunk(
                    chunk_id=f"{prefix}-{number}",
                    document_id=document,
                    text=f"{prefix} content {number}",
                    index=number,
                    start_char=number * 20,
                    end_char=number * 20 + 10,
                    access=DocumentAccess(tenant_id=tenant),
                ),
                vector=vector,
                model="test-vector-model",
            )
            for number in range(2)
        ]

    try:
        await asyncio.gather(
            index.replace_document(snapshot("a")), index.replace_document(snapshot("b"))
        )
        found = await index.search(vector, context=SecurityContext(tenant_id=tenant), limit=10)
        texts = {item.chunk.text for item, _ in found}
        assert texts in ({"a content 0", "a content 1"}, {"b content 0", "b content 1"})
    finally:
        await index.delete_document(tenant, document)
        await index.aclose()
