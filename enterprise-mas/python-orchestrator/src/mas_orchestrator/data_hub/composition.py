"""Build the configured Data Hub from bounded, real RAG dependencies."""

from __future__ import annotations

from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import DataSourceKind, VectorBackend
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.data_hub.service import DataHubService, SourceSyncAdapter

if TYPE_CHECKING:
    from mas_orchestrator.config.settings import Settings


def build_data_hub_service(settings: Settings) -> DataHubService:
    """Build a disabled service or a real guarded embedding and pgvector pipeline.

    Network pools are deferred until server startup. Unsupported configurations
    fail rather than silently selecting local memory or a mock embedder.
    """
    if not settings.rag.enabled:
        return DataHubService(settings=settings, pipeline=None, index=None, embedder=None)
    if settings.rag.vector_backend is not VectorBackend.PGVECTOR:
        raise ConfigurationError("Data Hub requires the dedicated pgvector backend")
    if settings.rag.hybrid_enabled:
        raise ConfigurationError("Data Hub hybrid indexing is not configured")
    if settings.rag.embedding_dimensions != 1536:
        raise ConfigurationError("Data Hub pgvector requires 1536-dimensional embeddings")

    from mas_orchestrator.data_hub.sources import S3CompatibleAdapter, WebCrawlAdapter
    from mas_orchestrator.rag.chunking import ChunkingConfig
    from mas_orchestrator.rag.embeddings import OpenAIEmbedder
    from mas_orchestrator.rag.ingestion import IngestConfig, IngestionPipeline
    from mas_orchestrator.rag.pgvector_index import PgVectorIndex
    from mas_orchestrator.security.credentials import resolve_secret

    data = settings.data_hub
    db_url = resolve_secret(data.database_url_secret_ref).reveal()
    api_key = resolve_secret(data.embedding_api_key_secret_ref)
    index = PgVectorIndex.deferred(db_url, dimensions=1536, database_role="mas_rag")
    embedder = OpenAIEmbedder(api_key=api_key, model=data.embedding_model, dimensions=1536)
    pipeline = IngestionPipeline(
        index=index,
        embedder=embedder,
        config=IngestConfig(
            chunking=ChunkingConfig(
                target_chars=settings.rag.chunk_size_chars,
                max_chars=settings.rag.chunk_size_chars,
                overlap_chars=settings.rag.chunk_overlap_chars,
            ),
            index_lexically=False,
            atomic_document_replace=True,
        ),
    )
    adapters: dict[DataSourceKind, SourceSyncAdapter] = {}
    if DataSourceKind.WEB in data.allowed_source_kinds:
        web = WebCrawlAdapter(
            allowed_hosts=data.allowed_web_hosts,
            max_depth=data.max_crawl_depth,
            max_pages=data.max_pages_per_sync,
            max_document_bytes=data.max_document_bytes,
            max_sync_bytes=data.max_sync_bytes,
        )
        adapters[DataSourceKind.WEB] = web.sync
    if DataSourceKind.OBJECT_STORAGE in data.allowed_source_kinds:
        objects = S3CompatibleAdapter(
            allowed_hosts=data.allowed_object_hosts,
            max_pages=data.max_pages_per_sync,
            max_document_bytes=data.max_document_bytes,
            max_sync_bytes=data.max_sync_bytes,
            bindings=data.object_storage_bindings,
        )
        adapters[DataSourceKind.OBJECT_STORAGE] = objects.sync
    return DataHubService(
        settings=settings, pipeline=pipeline, index=index, embedder=embedder, adapters=adapters
    )
