"""Authenticated Data Hub gRPC and live pgvector integration (test embeddings)."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import grpc
import pytest

from mas_orchestrator.config.enums import VectorBackend
from mas_orchestrator.config.settings import RagSettings, Settings
from mas_orchestrator.data_hub.service import DataHubService
from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.data_hub_servicer import DataHubBridgeServicer
from mas_orchestrator.grpc.generated import mas_api_pb2 as pb
from mas_orchestrator.grpc.generated import mas_api_pb2_grpc as pbg
from mas_orchestrator.rag.embeddings import HashingEmbedder
from mas_orchestrator.rag.ingestion import IngestConfig, IngestionPipeline
from mas_orchestrator.rag.models import DataHubOwnership, SecurityContext
from mas_orchestrator.rag.pgvector_index import PgVectorIndex
from mas_orchestrator.security.identity import ServiceScope, StaticTokenServiceIdentityResolver
from mas_orchestrator.security.internal_replay import InMemoryReplayGuard
from mas_orchestrator.security.internal_service import (
    SERVICE_HEADER,
    SIGNATURE_HEADER,
    now_millis,
    signed_internal_signature,
)

pytestmark = pytest.mark.integration
_TOKEN = "test-data-hub-token"
_SECRET = "test-data-hub-signing-secret"


@pytest.mark.asyncio
async def test_signed_grpc_ingest_search_purge_with_live_pgvector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use real transport/auth/parser/index; hashing embedding is a test double."""
    tenant, organization, project, source, document = (str(uuid4()) for _ in range(5))
    index = await PgVectorIndex.connect(
        os.environ["MAS_TEST_RAG_DATABASE_URL"], dimensions=1536, database_role="mas_rag"
    )
    embedder = HashingEmbedder(dimensions=1536)
    pipeline = IngestionPipeline(
        embedder=embedder,
        index=index,
        config=IngestConfig(index_lexically=False, atomic_document_replace=True),
    )
    settings = Settings(
        rag=RagSettings(
            enabled=True, vector_backend=VectorBackend.PGVECTOR, embedding_dimensions=1536
        )
    )
    service = DataHubService(settings=settings, pipeline=pipeline, index=index, embedder=embedder)
    boundary = AgentBridgeServicer(
        execution_service=object(),  # type: ignore[arg-type]
        identity_resolver=StaticTokenServiceIdentityResolver(
            _TOKEN, scopes={ServiceScope.DATA_HUB_INGEST, ServiceScope.DATA_HUB_PURGE}
        ),
        config=AgentBridgeServicerConfig(
            internal_service_secret=_SECRET, require_internal_signature=True
        ),
        replay_guard=InMemoryReplayGuard(),
    )
    server = grpc.aio.server()
    pbg.add_DataHubBridgeApiServicer_to_server(
        DataHubBridgeServicer(service=service, agent_bridge=boundary), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = pbg.DataHubBridgeApiStub(channel)

    def headers(request):
        signature = signed_internal_signature(
            _SECRET, "mas-api", now_millis(), request.SerializeToString(deterministic=True)
        )
        return [
            ("authorization", f"Bearer {_TOKEN}"),
            ("x-tenant-id", tenant),
            ("x-organization-id", organization),
            ("x-ai-protocol-version", "1.1.0"),
            (SERVICE_HEADER, "mas-api"),
            (SIGNATURE_HEADER, signature),
        ]

    common = {
        "tenant_id": tenant,
        "organization_id": organization,
        "project_id": project,
        "source_id": source,
        "document_id": document,
        "correlation_id": "integration",
        "request_id": str(uuid4()),
        "protocol_version": "1.1.0",
        "deadline_rfc3339": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        "identity": pb.ServiceIdentityMsg(subject="rust-core"),
    }
    content = b"Data hub security and durability integration test."
    ingest = pb.IngestDocumentRequest(
        **common,
        source_kind="upload",
        title="Integration",
        mime_type="text/plain",
        content=content,
        content_sha256=hashlib.sha256(content).hexdigest(),
        metadata_json="{}",
        required_roles=["reader"],
    )
    try:
        reply = await stub.IngestDocument(ingest, metadata=headers(ingest))
        assert reply.chunk_count == reply.embedded_count == 1
        # A legitimate retry with a fresh authentication nonce must not append
        # duplicate vectors for the same document.
        retry = await stub.IngestDocument(ingest, metadata=headers(ingest))
        assert retry.chunk_count == retry.embedded_count == 1
        vector = (await embedder.embed([content.decode()]))[0]
        assert not await index.search(vector, context=SecurityContext(tenant_id=tenant), limit=10)
        hits = await index.search(
            vector,
            context=SecurityContext(
                tenant_id=tenant,
                organization_id=organization,
                project_id=project,
                allowed_roles=frozenset({"reader"}),
            ),
            limit=10,
        )
        assert len(hits) == 1 and hits[0][0].chunk.document_id == document

        original_embed = embedder.embed

        async def failed_embed(_texts):
            raise RuntimeError("simulated embedding provider outage")

        monkeypatch.setattr(embedder, "embed", failed_embed)
        with pytest.raises(grpc.aio.AioRpcError):
            await stub.IngestDocument(ingest, metadata=headers(ingest))
        preserved = await index.search(
            vector,
            context=SecurityContext(
                tenant_id=tenant,
                organization_id=organization,
                project_id=project,
                allowed_roles=frozenset({"reader"}),
            ),
            limit=10,
        )
        assert len(preserved) == 1 and preserved[0][0].chunk.document_id == document
        wrong_scope = pb.PurgeDocumentRequest(**{**common, "source_id": str(uuid4())})
        with pytest.raises(grpc.aio.AioRpcError) as denied:
            await stub.PurgeDocument(wrong_scope, metadata=headers(wrong_scope))
        assert denied.value.code() is grpc.StatusCode.INVALID_ARGUMENT
        assert (
            len(
                await index.search(
                    vector,
                    context=SecurityContext(
                        tenant_id=tenant,
                        organization_id=organization,
                        project_id=project,
                        allowed_roles=frozenset({"reader"}),
                    ),
                    limit=10,
                )
            )
            == 1
        )
        purge = pb.PurgeDocumentRequest(**common)
        result = await stub.PurgeDocument(purge, metadata=headers(purge))
        assert result.removed_chunks == 1
        monkeypatch.setattr(embedder, "embed", original_embed)
        with pytest.raises(grpc.aio.AioRpcError) as stale:
            await stub.IngestDocument(ingest, metadata=headers(ingest))
        assert stale.value.code() is grpc.StatusCode.INVALID_ARGUMENT
        assert not await index.search(
            vector,
            context=SecurityContext(
                tenant_id=tenant,
                organization_id=organization,
                project_id=project,
                allowed_roles=frozenset({"reader"}),
            ),
            limit=10,
        )
    finally:
        await channel.close()
        await server.stop(None)
        await index.purge_data_hub_document(
            tenant, document, DataHubOwnership(organization, project, source)
        )
        await service.aclose()
