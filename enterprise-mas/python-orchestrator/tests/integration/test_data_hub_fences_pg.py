"""Live RAG-plane ownership and durable purge-fencing regressions."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.rag.ingestion import IngestConfig, IngestionPipeline
from mas_orchestrator.rag.models import (
    Chunk,
    DataHubOwnership,
    DocumentAccess,
    EmbeddedChunk,
    SecurityContext,
    SourceDocument,
)
from mas_orchestrator.rag.parsing import Parser
from mas_orchestrator.rag.pgvector_index import PgVectorIndex

pytestmark = pytest.mark.integration


def owner() -> DataHubOwnership:
    return DataHubOwnership(str(uuid4()), str(uuid4()), str(uuid4()))


def authorized(tenant: str, owning: DataHubOwnership) -> SecurityContext:
    return SecurityContext(
        tenant_id=tenant, organization_id=owning.organization_id, project_id=owning.project_id
    )


def chunk(tenant: str, document: str) -> EmbeddedChunk:
    return EmbeddedChunk(
        chunk=Chunk(
            chunk_id="one",
            document_id=document,
            text="example",
            index=0,
            start_char=0,
            end_char=7,
            access=DocumentAccess(tenant_id=tenant),
        ),
        vector=(1.0,) + (0.0,) * 1535,
        model="test",
    )


@pytest.mark.asyncio
async def test_fence_rejects_wrong_scope_legacy_claim_and_generic_bypass() -> None:
    tenant, document, legacy, missing = (str(uuid4()) for _ in range(4))
    correct = owner()
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"], dimensions=1536, database_role="mas_rag"
    )
    try:
        await index.begin_data_hub_ingest(tenant, document, correct)
        await index.replace_document([chunk(tenant, document)], owner=correct)
        for wrong in (
            DataHubOwnership(str(uuid4()), correct.project_id, correct.source_id),
            DataHubOwnership(correct.organization_id, str(uuid4()), correct.source_id),
            DataHubOwnership(correct.organization_id, correct.project_id, str(uuid4())),
        ):
            with pytest.raises(InvalidRequestError):
                await index.purge_data_hub_document(tenant, document, wrong)
            with pytest.raises(InvalidRequestError):
                await index.begin_data_hub_ingest(tenant, document, wrong)
        assert (
            len(
                await index.search(
                    chunk(tenant, document).vector,
                    context=SecurityContext(
                        tenant_id=tenant,
                        organization_id=correct.organization_id,
                        project_id=correct.project_id,
                    ),
                    limit=5,
                )
            )
            == 1
        )
        with pytest.raises(InvalidRequestError):
            await index.upsert([chunk(tenant, document)])
        with pytest.raises(InvalidRequestError):
            await index.delete_document(tenant, document)
        assert await index.purge_data_hub_document(tenant, document, correct) == 1
        assert await index.purge_data_hub_document(tenant, document, correct) == 0
        with pytest.raises(InvalidRequestError):
            await index.replace_document([chunk(tenant, document)], owner=correct)
        with pytest.raises(InvalidRequestError):
            await index.begin_data_hub_ingest(tenant, document, correct)
        assert not await index.search(
            chunk(tenant, document).vector, context=authorized(tenant, correct), limit=5
        )
        await index.upsert([chunk(tenant, legacy)])
        # Legacy vectors of unknown ownership must not become tenant-wide
        # Data Hub search results, even with a valid project selector.
        assert not await index.search(
            chunk(tenant, legacy).vector, context=authorized(tenant, correct), limit=5
        )
        assert not await index.search(
            chunk(tenant, legacy).vector, context=SecurityContext(tenant_id=tenant), limit=5
        )
        with pytest.raises(InvalidRequestError, match="legacy"):
            await index.begin_data_hub_ingest(tenant, legacy, correct)
        with pytest.raises(InvalidRequestError, match="legacy"):
            await index.purge_data_hub_document(tenant, legacy, correct)
        assert await index.delete_document(tenant, legacy) == 1
        assert await index.purge_data_hub_document(tenant, missing, correct) == 0
        with pytest.raises(InvalidRequestError):
            await index.begin_data_hub_ingest(tenant, missing, correct)
    finally:
        await index.aclose()


@pytest.mark.asyncio
async def test_purge_fences_ingestion_that_was_already_embedding() -> None:
    tenant, document = str(uuid4()), str(uuid4())
    correct = owner()
    started, proceed = asyncio.Event(), asyncio.Event()

    class PausedEmbedder:
        model = "test"

        async def embed(self, texts: list[str]) -> list[tuple[float, ...]]:
            started.set()
            await proceed.wait()
            return [(1.0,) + (0.0,) * 1535 for _ in texts]

    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"], dimensions=1536, database_role="mas_rag"
    )
    pipeline = IngestionPipeline(
        index=index,
        embedder=PausedEmbedder(),
        parser=Parser(),  # type: ignore[arg-type]
        config=IngestConfig(index_lexically=False, atomic_document_replace=True),
    )
    source = SourceDocument(
        document_id=document,
        title="test",
        content="example content",
        access=DocumentAccess(tenant_id=tenant),
    )
    task = asyncio.create_task(pipeline.ingest(source, data_hub_owner=correct))
    try:
        await asyncio.wait_for(started.wait(), timeout=5)
        assert await index.purge_data_hub_document(tenant, document, correct) == 0
        proceed.set()
        with pytest.raises(InvalidRequestError):
            await task
        assert not await index.search(
            (1.0,) + (0.0,) * 1535, context=authorized(tenant, correct), limit=5
        )
    finally:
        proceed.set()
        await asyncio.gather(task, return_exceptions=True)
        await index.aclose()


@pytest.mark.asyncio
async def test_search_hides_preexisting_chunks_after_backfilled_tombstone() -> None:
    import psycopg

    tenant, document = str(uuid4()), str(uuid4())
    correct = owner()
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"], dimensions=1536, database_role="mas_rag"
    )
    try:
        await index.upsert([chunk(tenant, document)])
        async with await psycopg.AsyncConnection.connect(
            os.environ["MAS_TEST_RAG_ELEVATED_DATABASE_URL"]
        ) as admin:
            await admin.execute(
                """INSERT INTO rag_document_fences
                   (tenant_id,document_id,organization_id,project_id,source_id,deleted)
                   VALUES (%s,%s,%s,%s,%s,TRUE)""",
                (tenant, document, correct.organization_id, correct.project_id, correct.source_id),
            )
        assert not await index.search(
            chunk(tenant, document).vector, context=authorized(tenant, correct), limit=5
        )
        assert await index.purge_data_hub_document(tenant, document, correct) == 1
    finally:
        await index.aclose()


@pytest.mark.asyncio
async def test_fence_rows_are_rls_scoped_and_not_deletable_by_rag_role() -> None:
    import psycopg

    tenant, document = str(uuid4()), str(uuid4())
    correct = owner()
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"], dimensions=1536, database_role="mas_rag"
    )
    try:
        await index.begin_data_hub_ingest(tenant, document, correct)
        async with await psycopg.AsyncConnection.connect(
            os.environ["MAS_TEST_RAG_DATABASE_URL"]
        ) as connection:
            async with connection.transaction():
                await connection.execute("SET LOCAL ROLE mas_rag")
                await connection.execute(
                    "SELECT set_config('app.current_tenant', %s, true)", (tenant,)
                )
                cursor = await connection.execute(
                    "SELECT count(*) FROM rag_document_fences "
                    "WHERE tenant_id = %s AND document_id = %s",
                    (tenant, document),
                )
                assert (await cursor.fetchone())[0] == 1
            async with connection.transaction():
                await connection.execute("SET LOCAL ROLE mas_rag")
                await connection.execute(
                    "SELECT set_config('app.current_tenant', %s, true)", (str(uuid4()),)
                )
                cursor = await connection.execute(
                    "SELECT count(*) FROM rag_document_fences "
                    "WHERE tenant_id = %s AND document_id = %s",
                    (tenant, document),
                )
                assert (await cursor.fetchone())[0] == 0
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                async with connection.transaction():
                    await connection.execute("SET LOCAL ROLE mas_rag")
                    await connection.execute(
                        "SELECT set_config('app.current_tenant', %s, true)", (tenant,)
                    )
                    await connection.execute(
                        "DELETE FROM rag_document_fences WHERE tenant_id = %s AND document_id = %s",
                        (tenant, document),
                    )
        await index.purge_data_hub_document(tenant, document, correct)
    finally:
        await index.aclose()


@pytest.mark.asyncio
async def test_project_scope_filters_fenced_documents_before_ranking_and_limit() -> None:
    from dataclasses import replace

    tenant = str(uuid4())
    first_id, second_id = str(uuid4()), str(uuid4())
    first, second = owner(), owner()
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"], dimensions=1536, database_role="mas_rag"
    )
    first_item, second_item = chunk(tenant, first_id), chunk(tenant, second_id)
    first_item = replace(first_item, chunk=replace(first_item.chunk, chunk_id="a"))
    second_item = replace(second_item, chunk=replace(second_item.chunk, chunk_id="z"))
    try:
        for document_id, owning, item in (
            (first_id, first, first_item),
            (second_id, second, second_item),
        ):
            await index.begin_data_hub_ingest(tenant, document_id, owning)
            await index.replace_document([item], owner=owning)
        vector = first_item.vector
        # An unscoped RAG caller must not see a project-owned Data Hub row.
        assert not await index.search(vector, context=SecurityContext(tenant_id=tenant), limit=1)
        # The first row sorts earlier; filtering after LIMIT would hide second.
        scoped = SecurityContext(
            tenant_id=tenant, organization_id=second.organization_id, project_id=second.project_id
        )
        results = await index.search(vector, context=scoped, limit=1)
        assert [item.chunk.document_id for item, _ in results] == [second_id]
        for organization, project in (
            (first.organization_id, second.project_id),
            (second.organization_id, first.project_id),
            (str(uuid4()), str(uuid4())),
        ):
            assert not await index.search(
                vector,
                context=SecurityContext(
                    tenant_id=tenant, organization_id=organization, project_id=project
                ),
                limit=1,
            )
        with pytest.raises(InvalidRequestError):
            SecurityContext(tenant_id=tenant, project_id=first.project_id)
        with pytest.raises(InvalidRequestError):
            await index.search(
                vector,
                context=SecurityContext(
                    tenant_id=tenant,
                    organization_id="00000000-0000-0000-0000-000000000000",
                    project_id=first.project_id,
                ),
                limit=1,
            )
    finally:
        await index.purge_data_hub_document(tenant, first_id, first)
        await index.purge_data_hub_document(tenant, second_id, second)
        await index.aclose()
