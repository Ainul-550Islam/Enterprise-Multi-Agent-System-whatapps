"""Live Data Hub listener for the Rust cross-language integration test.

The gRPC boundary, parser, durable replay guard and pgvector index are real.
HashingEmbedder is explicitly a test double; this does not prove a live model
provider or the complete production composition root.
"""

from __future__ import annotations

import asyncio
import os

import grpc

from mas_orchestrator.config.enums import VectorBackend
from mas_orchestrator.config.settings import RagSettings, Settings
from mas_orchestrator.data_hub.service import DataHubService
from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.data_hub_servicer import DataHubBridgeServicer
from mas_orchestrator.grpc.generated import mas_api_pb2_grpc
from mas_orchestrator.rag.embeddings import HashingEmbedder
from mas_orchestrator.rag.ingestion import IngestConfig, IngestionPipeline
from mas_orchestrator.rag.pgvector_index import PgVectorIndex
from mas_orchestrator.security.identity import ServiceScope, StaticTokenServiceIdentityResolver
from mas_orchestrator.security.internal_replay import PostgresReplayGuard


async def main() -> None:
    """Bind an ephemeral loopback listener and announce readiness on stdout."""
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
            "test-data-hub-token",
            scopes={ServiceScope.DATA_HUB_INGEST, ServiceScope.DATA_HUB_PURGE},
        ),
        config=AgentBridgeServicerConfig(
            internal_service_secret="test-data-hub-signing-secret", require_internal_signature=True
        ),
        replay_guard=PostgresReplayGuard(os.environ["MAS_TEST_REPLAY_DATABASE_URL"]),
    )
    server = grpc.aio.server(
        options=[("grpc.max_receive_message_length", 2 * 1024 * 1024 + 128 * 1024)]
    )
    mas_api_pb2_grpc.add_DataHubBridgeApiServicer_to_server(
        DataHubBridgeServicer(service=service, agent_bridge=boundary), server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    if not port:
        raise RuntimeError("could not bind Data Hub test listener")
    try:
        await server.start()
        print(f"PORT={port}", flush=True)
        await server.wait_for_termination()
    finally:
        await server.stop(None)
        await service.aclose()


if __name__ == "__main__":
    asyncio.run(main())
