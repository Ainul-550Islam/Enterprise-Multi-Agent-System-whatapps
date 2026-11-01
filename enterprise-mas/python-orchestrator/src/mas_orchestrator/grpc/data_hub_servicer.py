"""Rust-authorized DataHubBridgeApi gRPC servicer.

Every stateful operation passes through the existing AgentBridge
``validate_call_context`` boundary. No second token resolver, metadata parser,
HMAC implementation, or tenant-header policy exists here. The signed request
body carries Rust's authoritative tenant/project/source/document snapshot;
Python only parses and indexes the content in the isolated RAG plane.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any

from mas_orchestrator.config.enums import DataSourceKind
from mas_orchestrator.contracts.errors import (
    ExecutionTimeoutError,
    InternalError,
    InvalidRequestError,
    OrchestratorError,
    ProtocolError,
)
from mas_orchestrator.contracts.protocol_version import negotiate_protocol_version
from mas_orchestrator.data_hub.models import DataHubDocument, DataHubSource
from mas_orchestrator.data_hub.service import DataHubService, parse_source_config_json
from mas_orchestrator.grpc.agent_bridge_servicer import AgentBridgeServicer, parse_deadline
from mas_orchestrator.grpc.errors import abort_with_error
from mas_orchestrator.grpc.generated import mas_api_pb2, mas_api_pb2_grpc
from mas_orchestrator.security.identity import ServiceScope

if TYPE_CHECKING:
    import grpc

_LOGGER = logging.getLogger(__name__)
_HEALTHY_DETAIL = "data hub ready"
_DISABLED_DETAIL = "data hub is disabled"


class DataHubBridgeServicer(mas_api_pb2_grpc.DataHubBridgeApiServicer):
    """Serves source synchronization and document-index operations from Rust."""

    def __init__(
        self,
        *,
        service: DataHubService,
        agent_bridge: AgentBridgeServicer,
    ) -> None:
        self._service = service
        self._agent_bridge = agent_bridge

    async def startup(self) -> None:
        """Opens configured database pools before the gRPC server accepts work."""
        await self._service.startup()

    async def aclose(self) -> None:
        """Closes source transports and the RAG-plane pool at process shutdown."""
        await self._service.aclose()

    async def SyncSource(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
    ) -> mas_api_pb2.SyncSourceReply:
        """Synchronizes one Rust-authorized source and indexes its changed items."""
        started = time.monotonic()
        _metadata, identity = await self._validate(request, context, ServiceScope.DATA_HUB_SYNC)
        self._log_call("sync_source", request, identity.subject)
        try:
            kind = _source_kind(request.source_kind)
            config = parse_source_config_json(request.configuration_json)
            source = DataHubSource(
                source_id=request.source_id,
                tenant_id=request.tenant_id,
                organization_id=request.organization_id,
                project_id=request.project_id,
                name=request.source_name,
                kind=kind,
                config=config,
                credential_secret_ref=(
                    request.credential_secret_ref
                    if request.HasField("credential_secret_ref")
                    else None
                ),
                cursor=request.cursor if request.HasField("cursor") else None,
                required_roles=frozenset(request.required_roles),
                denied_principals=frozenset(request.denied_principals),
            )
            report = await self._run_with_deadline(
                request,
                context,
                self._service.sync_source(source),
            )
            _LOGGER.info(
                "Data Hub source synchronization finished",
                extra={
                    "event": "data_hub.sync_finished",
                    "source_id": report.source_id,
                    "tenant_id": request.tenant_id,
                    "discovered": report.discovered_count,
                    "changed": report.changed_count,
                    "indexed": report.indexed_count,
                    "failed": report.failed_count,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                },
            )
            return mas_api_pb2.SyncSourceReply(
                source_id=report.source_id,
                discovered_count=report.discovered_count,
                changed_count=report.changed_count,
                indexed_count=report.indexed_count,
                failed_count=report.failed_count,
                next_cursor=report.next_cursor,
                errors=list(report.errors),
            )
        except asyncio.CancelledError:
            raise
        except OrchestratorError as error:
            await abort_with_error(context, error)
            raise AssertionError("abort_with_error never returns") from None
        except Exception as error:
            _LOGGER.exception(
                "unmodelled Data Hub source synchronization failure",
                extra={"event": "data_hub.sync_error", "source_id": request.source_id},
            )
            await abort_with_error(context, InternalError("Data Hub source synchronization failed"))
            raise AssertionError("abort_with_error never returns") from error

    async def IngestDocument(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
    ) -> mas_api_pb2.DocumentIngestionReply:
        """Indexes one bounded upload/discovered document under Rust's tenant id."""
        metadata, identity = await self._validate(request, context, ServiceScope.DATA_HUB_INGEST)
        del metadata
        self._log_call("ingest_document", request, identity.subject)
        try:
            kind = _source_kind(request.source_kind)
            try:
                document_metadata: object = json.loads(request.metadata_json or "{}")
            except json.JSONDecodeError as error:
                raise InvalidRequestError(
                    "Data Hub document metadata is not valid JSON",
                    details={"field": "metadata_json"},
                ) from error
            if not isinstance(document_metadata, dict) or any(
                not isinstance(key, str) for key in document_metadata
            ):
                raise InvalidRequestError("Data Hub document metadata must be a JSON object")
            document = DataHubDocument(
                source_id=request.source_id,
                document_id=request.document_id,
                tenant_id=request.tenant_id,
                organization_id=request.organization_id,
                project_id=request.project_id,
                title=request.title,
                mime_type=request.mime_type,
                source_kind=kind,
                content=bytes(request.content),
                source_uri=request.source_uri if request.HasField("source_uri") else None,
                expected_sha256=(
                    request.content_sha256 if request.HasField("content_sha256") else None
                ),
                required_roles=frozenset(request.required_roles),
                denied_principals=frozenset(request.denied_principals),
                metadata=document_metadata,
            )
            report = await self._run_with_deadline(
                request, context, self._service.ingest_document(document)
            )
            return mas_api_pb2.DocumentIngestionReply(
                document_id=report.document_id,
                sha256=report.sha256,
                byte_count=report.byte_count,
                chunk_count=report.chunk_count,
                embedded_count=report.embedded_count,
                skipped_reason=report.skipped_reason,
            )
        except asyncio.CancelledError:
            raise
        except OrchestratorError as error:
            await abort_with_error(context, error)
            raise AssertionError("abort_with_error never returns") from None
        except Exception as error:
            _LOGGER.exception(
                "unmodelled Data Hub document ingestion failure",
                extra={"event": "data_hub.ingest_error", "document_id": request.document_id},
            )
            await abort_with_error(context, InternalError("Data Hub document ingestion failed"))
            raise AssertionError("abort_with_error never returns") from error

    async def PurgeDocument(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
    ) -> mas_api_pb2.PurgeDocumentReply:
        """Removes exactly one tenant/document pair from the RAG plane."""
        metadata, identity = await self._validate(request, context, ServiceScope.DATA_HUB_PURGE)
        del metadata
        self._log_call("purge_document", request, identity.subject)
        try:
            removed = await self._run_with_deadline(
                request,
                context,
                self._service.purge_document(
                    tenant_id=request.tenant_id,
                    document_id=request.document_id,
                ),
            )
            return mas_api_pb2.PurgeDocumentReply(
                document_id=request.document_id,
                removed_chunks=removed,
            )
        except asyncio.CancelledError:
            raise
        except OrchestratorError as error:
            await abort_with_error(context, error)
            raise AssertionError("abort_with_error never returns") from None
        except Exception as error:
            _LOGGER.exception(
                "unmodelled Data Hub document purge failure",
                extra={"event": "data_hub.purge_error", "document_id": request.document_id},
            )
            await abort_with_error(context, InternalError("Data Hub document purge failed"))
            raise AssertionError("abort_with_error never returns") from error

    async def Health(
        self,
        request: mas_api_pb2.Empty,
        context: grpc.aio.ServicerContext,
    ) -> mas_api_pb2.DataHubHealthReply:
        """Returns a secret-free readiness signal with no tenant data."""
        del request, context
        if not self._service.enabled:
            return mas_api_pb2.DataHubHealthReply(healthy=True, detail=_DISABLED_DETAIL)
        try:
            healthy = await self._service.health_check()
        except Exception as error:
            _LOGGER.warning(
                "Data Hub readiness probe failed",
                extra={"event": "data_hub.health_failed", "error_type": type(error).__name__},
            )
            healthy = False
        return mas_api_pb2.DataHubHealthReply(
            healthy=healthy,
            detail=_HEALTHY_DETAIL if healthy else "data hub backend unavailable",
        )

    async def _validate(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
        required_scope: ServiceScope,
    ) -> tuple[Any, Any]:
        """Reuses AgentBridge authentication, signature, scope, and tenant checks."""
        metadata, identity = await self._agent_bridge.validate_call_context(
            request,
            context,
            required_scope=required_scope,
        )
        if not request.HasField("identity") or not request.identity.subject.strip():
            await abort_with_error(
                context,
                InvalidRequestError(
                    "Data Hub request is missing the signed service identity",
                    details={"field": "identity"},
                ),
            )
            raise AssertionError("abort_with_error never returns") from None
        if (
            request.protocol_version
            and metadata.protocol_version
            and request.protocol_version != metadata.protocol_version
        ):
            await abort_with_error(
                context,
                ProtocolError(
                    "Data Hub body protocol version disagrees with bridge metadata",
                    details={"reason": "version_mismatch"},
                ),
            )
            raise AssertionError("abort_with_error never returns") from None
        version = request.protocol_version or metadata.protocol_version or ""
        try:
            negotiate_protocol_version(version)
        except ProtocolError as error:
            await abort_with_error(context, error)
            raise
        deadline_ms = parse_deadline(request.deadline_rfc3339)
        if deadline_ms is not None and deadline_ms <= int(time.time() * 1000):
            await abort_with_error(
                context,
                ExecutionTimeoutError("Data Hub request deadline has expired"),
            )
            raise AssertionError("abort_with_error never returns") from None
        return metadata, identity

    async def _run_with_deadline(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
        operation: Any,
    ) -> Any:
        """Bounds one awaited operation by the signed absolute deadline."""
        deadline_ms = parse_deadline(request.deadline_rfc3339)
        remaining = None
        if deadline_ms is not None:
            remaining = max(0.0, (deadline_ms - int(time.time() * 1000)) / 1000.0)
        wire_remaining = context.time_remaining()
        if wire_remaining is not None:
            remaining = wire_remaining if remaining is None else min(remaining, wire_remaining)
        try:
            if remaining is None:
                return await operation
            if remaining <= 0:
                raise ExecutionTimeoutError("Data Hub request deadline has expired")
            async with asyncio.timeout(remaining):
                return await operation
        except TimeoutError as error:
            raise ExecutionTimeoutError("Data Hub operation exceeded its deadline") from error

    @staticmethod
    def _log_call(operation: str, request: Any, subject: str) -> None:
        """Emits only signed identity/scope ids, never content or source config."""
        _LOGGER.info(
            "Data Hub bridge call accepted",
            extra={
                "event": "data_hub.call_accepted",
                "operation": operation,
                "source_id": request.source_id,
                "tenant_id": request.tenant_id,
                "project_id": request.project_id,
                "request_id": request.request_id,
                "subject": subject,
            },
        )


def _source_kind(value: str) -> DataSourceKind:
    """Parses the wire source-kind string without inventing a default."""
    try:
        return DataSourceKind(value)
    except ValueError as error:
        raise InvalidRequestError(
            "Data Hub source kind is unsupported",
            details={"field": "source_kind"},
        ) from error
