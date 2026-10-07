"""Tenant-scoped Data Hub ingestion, synchronization, and purge service.

Rust is the authority for source/document records and access policy. A caller
reaches this service only after the DataHubBridge servicer has authenticated the
Rust service, verified the body-bound signature, checked the service scope, and
reconciled tenant/organization headers. This service uses the signed request's
tenant and document ids for RAG-plane operations; it never infers scope from
network metadata or writes Rust-owned catalogue tables.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from mas_orchestrator.config.enums import DataSourceKind, VectorBackend
from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    RetrievalError,
    ToolPermissionDeniedError,
)
from mas_orchestrator.data_hub.models import DataHubDocument, DataHubSource
from mas_orchestrator.data_hub.sources import SourceSyncResult
from mas_orchestrator.rag.models import DocumentAccess, DocumentFormat, IngestResult, SourceDocument

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas_orchestrator.config.settings import DataHubSettings, RagSettings, Settings
    from mas_orchestrator.rag.embeddings import Embedder
    from mas_orchestrator.rag.index import VectorIndex
    from mas_orchestrator.rag.ingestion import IngestionPipeline

_LOGGER = logging.getLogger(__name__)
SourceSyncAdapter = Callable[[DataHubSource], Awaitable[SourceSyncResult]]

_MIME_TO_FORMAT: dict[str, DocumentFormat] = {
    "text/plain": DocumentFormat.TEXT,
    "text/markdown": DocumentFormat.MARKDOWN,
    "text/html": DocumentFormat.HTML,
    "text/csv": DocumentFormat.CSV,
    "application/json": DocumentFormat.JSON,
    "application/pdf": DocumentFormat.PDF,
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": DocumentFormat.DOCX,
}


@dataclass(frozen=True, slots=True)
class DocumentIngestionReport:
    """Result for one document; the catalogue status transition remains Rust-owned."""

    document_id: str
    sha256: str
    byte_count: int
    chunk_count: int
    embedded_count: int
    skipped_reason: str | None = None


@dataclass(frozen=True, slots=True)
class SourceSyncReport:
    """Summary returned to Rust after one bounded source synchronization."""

    source_id: str
    discovered_count: int
    changed_count: int
    indexed_count: int
    failed_count: int
    next_cursor: str
    errors: tuple[str, ...] = ()


class DataHubService:
    """Owns Python-side source adapters and the RAG ingestion pipeline."""

    def __init__(
        self,
        *,
        settings: Settings,
        pipeline: IngestionPipeline | None,
        index: VectorIndex | None,
        embedder: Embedder | None,
        adapters: Mapping[DataSourceKind, SourceSyncAdapter] | None = None,
    ) -> None:
        self._settings = settings
        self._data_settings: DataHubSettings = settings.data_hub
        self._rag_settings: RagSettings = settings.rag
        self._pipeline = pipeline
        self._index = index
        self._embedder = embedder
        self._adapters = dict(adapters or {})
        if settings.rag.enabled and (pipeline is None or index is None or embedder is None):
            raise ConfigurationError(
                "RAG is enabled but its ingestion pipeline is incomplete",
                details={"field": "rag"},
            )
        if settings.rag.vector_backend is VectorBackend.PGVECTOR and index is not None:
            from mas_orchestrator.rag.pgvector_index import PgVectorIndex

            if not isinstance(index, PgVectorIndex):
                raise ConfigurationError(
                    "pgvector was selected but the composed index is not PgVectorIndex",
                    details={"field": "rag.vector_backend"},
                )

    @property
    def enabled(self) -> bool:
        """Whether parsing, embedding, and durable indexing are configured."""
        return self._rag_settings.enabled

    async def startup(self) -> None:
        """Opens and probes a durable index before the gRPC listener is bound."""
        if not self.enabled:
            return
        opener = getattr(self._index, "open", None)
        if opener is not None:
            await opener()
        probe = getattr(self._index, "health_check", None)
        if probe is not None and not await probe():
            raise ConfigurationError(
                "configured Data Hub vector backend failed its startup health check",
                details={"field": "rag.vector_backend"},
            )

    async def aclose(self) -> None:
        """Closes all resources owned by the Data Hub plane."""
        owners = [getattr(adapter, "__self__", None) for adapter in self._adapters.values()]
        owners.extend((self._embedder, self._index))
        errors: list[Exception] = []
        seen: set[int] = set()
        for owner in owners:
            close = getattr(owner, "aclose", None)
            if close is None or id(owner) in seen:
                continue
            seen.add(id(owner))
            try:
                await close()
            except Exception as error:
                errors.append(error)
        if errors:
            raise ExceptionGroup("Data Hub resource cleanup failed", errors)

    async def health_check(self) -> bool:
        """Returns whether the configured Data Hub backend is ready."""
        if not self.enabled:
            return True
        checker = getattr(self._index, "health_check", None)
        if checker is None:
            return self._index is not None
        return bool(await checker())

    async def ingest_document(self, document: DataHubDocument) -> DocumentIngestionReport:
        """Validates and indexes one tenant-authorized document snapshot."""
        self._require_enabled()
        self._require_source_kind(document.source_kind)
        mime_type = _normalise_mime(document.mime_type)
        if mime_type not in self._data_settings.allowed_mime_types:
            raise InvalidRequestError(
                "document MIME type is not in the configured allow-list",
                details={"mime_type": mime_type},
            )
        max_bytes = (
            self._data_settings.max_upload_bytes
            if document.source_kind is DataSourceKind.UPLOAD
            else self._data_settings.max_document_bytes
        )
        if len(document.content) > min(max_bytes, self._data_settings.max_document_bytes):
            raise InvalidRequestError(
                "document exceeds the configured byte limit",
                details={
                    "limit_bytes": min(max_bytes, self._data_settings.max_document_bytes),
                    "received_bytes": len(document.content),
                },
            )
        source_uri = _safe_source_uri(document.source_uri)
        digest = hashlib.sha256(document.content).hexdigest()
        if document.expected_sha256 is not None and digest != document.expected_sha256:
            raise InvalidRequestError(
                "document SHA-256 does not match its signed metadata",
                details={"document_id": document.document_id},
            )
        metadata = _safe_metadata(document.metadata)
        if document.expected_sha256:
            metadata["content_sha256"] = digest

        source = SourceDocument(
            document_id=document.document_id,
            title=document.title,
            content=document.content,
            format=_MIME_TO_FORMAT[mime_type],
            access=DocumentAccess(
                tenant_id=document.tenant_id,
                required_roles=document.required_roles,
                denied_principals=document.denied_principals,
            ),
            source_uri=source_uri,
            metadata=metadata,
        )
        pipeline = self._require_pipeline()
        try:
            result: IngestResult = await pipeline.ingest(source, format=source.format)
        except Exception as error:
            if isinstance(error, (InvalidRequestError, RetrievalError, ConfigurationError)):
                raise
            raise RetrievalError(
                "Data Hub document ingestion failed",
                details={"document_id": document.document_id, "error_type": type(error).__name__},
            ) from error

        if result.skipped_reason is not None:
            raise RetrievalError(
                "Data Hub document produced no indexable content",
                details={"document_id": document.document_id, "reason": result.skipped_reason},
            )
        if result.embedded_count != result.chunk_count:
            await pipeline.remove(source)
            raise RetrievalError(
                "Data Hub document was not indexed completely",
                details={
                    "document_id": document.document_id,
                    "expected_chunks": result.chunk_count,
                    "indexed_chunks": result.embedded_count,
                },
            )
        return DocumentIngestionReport(
            document_id=document.document_id,
            sha256=digest,
            byte_count=len(document.content),
            chunk_count=result.chunk_count,
            embedded_count=result.embedded_count,
            skipped_reason=None,
        )

    async def sync_source(self, source: DataHubSource) -> SourceSyncReport:
        """Synchronizes a configured web/S3 source and indexes changed objects.

        The cursor is advanced only if every changed document indexed
        successfully. A partial failure returns the previous cursor so a later
        call retries the failed content instead of losing it.
        """
        self._require_enabled()
        self._require_source_kind(source.kind)
        adapter = self._adapters.get(source.kind)
        if adapter is None:
            if source.kind is DataSourceKind.UPLOAD:
                return SourceSyncReport(
                    source_id=source.source_id,
                    discovered_count=0,
                    changed_count=0,
                    indexed_count=0,
                    failed_count=0,
                    next_cursor=source.cursor or "",
                )
            raise ConfigurationError(
                "Data Hub source adapter is not configured",
                details={"source_kind": source.kind.value},
            )

        try:
            batch = await adapter(source)
        except Exception as error:
            if isinstance(error, (ConfigurationError, InvalidRequestError, RetrievalError)):
                raise
            raise RetrievalError(
                "Data Hub source synchronization failed",
                details={"source_id": source.source_id, "error_type": type(error).__name__},
            ) from error
        if batch.discovered_count > self._data_settings.max_pages_per_sync:
            raise InvalidRequestError(
                "source adapter returned more pages than the configured sync cap",
                details={
                    "limit": self._data_settings.max_pages_per_sync,
                    "received": batch.discovered_count,
                },
            )
        changed_bytes = sum(len(item.content) for item in batch.documents)
        if changed_bytes > self._data_settings.max_sync_bytes:
            raise InvalidRequestError(
                "source synchronization exceeds the total byte budget",
                details={
                    "limit_bytes": self._data_settings.max_sync_bytes,
                    "received_bytes": changed_bytes,
                },
            )
        if len(batch.documents) > self._data_settings.max_pages_per_sync:
            raise InvalidRequestError("source adapter returned too many changed documents")

        errors: list[str] = []
        indexed = 0
        for item in batch.documents:
            document_id = str(
                uuid.uuid5(
                    uuid.UUID(source.source_id),
                    f"{source.tenant_id}\x00{item.locator}",
                )
            )
            try:
                await self.ingest_document(
                    DataHubDocument(
                        source_id=source.source_id,
                        document_id=document_id,
                        tenant_id=source.tenant_id,
                        organization_id=source.organization_id,
                        project_id=source.project_id,
                        title=item.title,
                        mime_type=item.mime_type,
                        source_kind=source.kind,
                        content=item.content,
                        source_uri=item.locator,
                        expected_sha256=hashlib.sha256(item.content).hexdigest(),
                        required_roles=source.required_roles,
                        denied_principals=source.denied_principals,
                        metadata={
                            "etag": item.etag,
                            "last_modified": (
                                item.last_modified.astimezone(UTC).isoformat()
                                if item.last_modified is not None
                                else None
                            ),
                        },
                    )
                )
            except Exception as error:
                errors.append(f"{document_id}:{type(error).__name__}")
                _LOGGER.warning(
                    "Data Hub document failed during source sync",
                    extra={
                        "event": "data_hub.sync_document_failed",
                        "source_id": source.source_id,
                        "document_id": document_id,
                        "error_type": type(error).__name__,
                    },
                )
            else:
                indexed += 1

        failed = len(errors)
        next_cursor = batch.cursor if failed == 0 else (source.cursor or "")
        return SourceSyncReport(
            source_id=source.source_id,
            discovered_count=batch.discovered_count,
            changed_count=len(batch.documents),
            indexed_count=indexed,
            failed_count=failed,
            next_cursor=next_cursor,
            errors=tuple(errors),
        )

    async def purge_document(self, *, tenant_id: str, document_id: str) -> int:
        """Removes all vector and lexical records for one signed tenant/document."""
        self._require_enabled()
        _require_uuid(tenant_id, "tenant_id")
        _require_uuid(document_id, "document_id")
        if self._index is None:
            raise ConfigurationError("Data Hub vector index is not configured")
        removed = await self._index.delete_document(tenant_id, document_id)
        pipeline_lexical = getattr(self._pipeline, "_lexical", None)
        if pipeline_lexical is not None:
            await pipeline_lexical.delete_document(tenant_id, document_id)
        return removed

    def _require_enabled(self) -> None:
        """Rejects Data Hub work if RAG is deliberately disabled."""
        if not self.enabled:
            raise ConfigurationError(
                "Data Hub ingestion is disabled because RAG has no configured backend",
                details={"field": "rag.enabled"},
            )

    def _require_pipeline(self) -> IngestionPipeline:
        """Returns the configured ingestion pipeline or fails closed."""
        if self._pipeline is None:
            raise ConfigurationError("Data Hub ingestion pipeline is not configured")
        return self._pipeline

    def _require_source_kind(self, kind: DataSourceKind) -> None:
        """Enforces the deployment's source-kind allow-list."""
        if kind not in self._data_settings.allowed_source_kinds:
            raise ToolPermissionDeniedError(
                "Data Hub source kind is disabled by deployment policy",
                details={"source_kind": kind.value},
            )


def parse_source_config_json(raw: str) -> dict[str, Any]:
    """Parses a small JSON object while rejecting malformed/unbounded input."""
    import json

    if len(raw.encode("utf-8")) > 64 * 1024:
        raise InvalidRequestError("Data Hub source configuration exceeds its size limit")
    try:
        value: object = json.loads(raw)
    except json.JSONDecodeError as error:
        raise InvalidRequestError("Data Hub source configuration is not valid JSON") from error
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise InvalidRequestError("Data Hub source configuration must be a JSON object")
    return value


def document_format_for_mime(mime_type: str) -> DocumentFormat:
    """Resolves exact media type to a parser; unknown formats are refused."""
    normalised = _normalise_mime(mime_type)
    try:
        return _MIME_TO_FORMAT[normalised]
    except KeyError as error:
        raise InvalidRequestError(
            "document MIME type has no parser",
            details={"mime_type": normalised},
        ) from error


def _normalise_mime(value: str) -> str:
    """Normalizes a media type without trusting parameters from a caller."""
    return value.split(";", 1)[0].strip().lower()


def _safe_source_uri(value: str | None) -> str | None:
    """Removes fragments/query secrets and rejects unsafe citation URLs."""
    if value is None:
        return None
    if len(value) > 2048 or any(character in value for character in "\r\n\x00"):
        raise InvalidRequestError("Data Hub source URI is malformed")
    parsed = urlsplit(value)
    if parsed.scheme and parsed.scheme.lower() not in {"http", "https"}:
        raise InvalidRequestError("Data Hub source URI uses an unsupported scheme")
    if parsed.username is not None or parsed.password is not None:
        raise InvalidRequestError("Data Hub source URI must not include user credentials")
    return (
        urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", "")) if parsed.scheme else value
    )


def _safe_metadata(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keeps only bounded JSON scalar metadata and excludes secret-like keys."""
    result: dict[str, Any] = {}
    for key, item in value.items():
        normalized_key = str(key).strip().lower().replace("-", "_")
        if not normalized_key or len(normalized_key) > 64:
            continue
        if any(
            fragment in normalized_key for fragment in ("secret", "password", "token", "api_key")
        ):
            continue
        if item is None or isinstance(item, (str, int, float, bool)):
            if isinstance(item, str):
                result[normalized_key] = item[:2048]
            elif isinstance(item, float):
                if item == item and abs(item) != float("inf"):
                    result[normalized_key] = item
            else:
                result[normalized_key] = item
        if len(result) >= 32:
            break
    return result


def _require_uuid(value: str, field: str) -> None:
    """Validates the tenant/document scope before issuing a vector operation."""
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError, TypeError) as error:
        raise InvalidRequestError(f"{field} must be a UUID", details={"field": field}) from error
