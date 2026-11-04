"""Real gRPC calls exercise shared Data Hub authentication before work."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import grpc
import pytest

from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.data_hub_servicer import DataHubBridgeServicer
from mas_orchestrator.grpc.generated import mas_api_pb2 as pb
from mas_orchestrator.grpc.generated import mas_api_pb2_grpc as pbg
from mas_orchestrator.security.identity import ServiceScope, StaticTokenServiceIdentityResolver
from mas_orchestrator.security.internal_replay import InMemoryReplayGuard
from mas_orchestrator.security.internal_service import (
    SERVICE_HEADER,
    SIGNATURE_HEADER,
    now_millis,
    signed_internal_signature,
)

_TOKEN = "test-boundary-token"
_SECRET = "test-boundary-signing-secret"
_TENANT = "00000000-0000-4000-8000-000000000001"
_ORG = "00000000-0000-4000-8000-000000000002"
_PROJECT = "00000000-0000-4000-8000-000000000003"
_SOURCE = "00000000-0000-4000-8000-000000000004"


class _DataService:
    enabled = True

    def __init__(self) -> None:
        self.calls = 0

    async def sync_source(self, source):
        self.calls += 1
        return SimpleNamespace(
            source_id=source.source_id,
            discovered_count=0,
            changed_count=0,
            indexed_count=0,
            failed_count=0,
            next_cursor=None,
            errors=[],
        )


def _request() -> pb.SyncSourceRequest:
    return pb.SyncSourceRequest(
        tenant_id=_TENANT,
        organization_id=_ORG,
        project_id=_PROJECT,
        source_id=_SOURCE,
        source_name="test source",
        source_kind="web",
        configuration_json="{}",
        correlation_id="corr-test",
        request_id="req-test",
        protocol_version="1.1.0",
        deadline_rfc3339=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        identity=pb.ServiceIdentityMsg(subject="rust-core"),
    )


def _headers(request, *, body=None, tenant=_TENANT):
    signed = signed_internal_signature(
        _SECRET,
        "mas-api",
        now_millis(),
        (body if body is not None else request).SerializeToString(deterministic=True),
    )
    return [
        ("authorization", f"Bearer {_TOKEN}"),
        ("x-tenant-id", tenant),
        ("x-organization-id", _ORG),
        ("x-ai-protocol-version", "1.1.0"),
        (SERVICE_HEADER, "mas-api"),
        (SIGNATURE_HEADER, signed),
    ]


@pytest.mark.asyncio
async def test_signed_data_hub_scope_body_replay_and_tenant_are_enforced() -> None:
    service = _DataService()
    boundary = AgentBridgeServicer(
        execution_service=object(),  # type: ignore[arg-type]
        identity_resolver=StaticTokenServiceIdentityResolver(
            _TOKEN, scopes={ServiceScope.DATA_HUB_SYNC}
        ),
        config=AgentBridgeServicerConfig(
            require_scope=ServiceScope.EXECUTE_AGENT,
            internal_service_secret=_SECRET,
            require_internal_signature=True,
        ),
        replay_guard=InMemoryReplayGuard(),
    )
    data_hub = DataHubBridgeServicer(service=service, agent_bridge=boundary)  # type: ignore[arg-type]
    server = grpc.aio.server()
    pbg.add_DataHubBridgeApiServicer_to_server(data_hub, server)
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = pbg.DataHubBridgeApiStub(channel)
    try:
        request = _request()
        headers = _headers(request)
        result = await stub.SyncSource(request, metadata=headers)
        assert result.source_id == _SOURCE
        assert service.calls == 1
        with pytest.raises(grpc.aio.AioRpcError) as replay:
            await stub.SyncSource(request, metadata=headers)
        assert replay.value.code() is grpc.StatusCode.UNAUTHENTICATED

        altered = _request()
        altered.source_name = "tampered source"
        with pytest.raises(grpc.aio.AioRpcError) as tampered:
            await stub.SyncSource(altered, metadata=_headers(altered, body=request))
        assert tampered.value.code() is grpc.StatusCode.UNAUTHENTICATED

        with pytest.raises(grpc.aio.AioRpcError) as foreign:
            await stub.SyncSource(request, metadata=_headers(request, tenant="foreign"))
        assert foreign.value.code() is grpc.StatusCode.FAILED_PRECONDITION

        purge = pb.PurgeDocumentRequest(
            tenant_id=_TENANT,
            organization_id=_ORG,
            project_id=_PROJECT,
            source_id=_SOURCE,
            document_id=_SOURCE,
            correlation_id="corr-test",
            request_id="req-test",
            protocol_version="1.1.0",
            deadline_rfc3339=request.deadline_rfc3339,
            identity=pb.ServiceIdentityMsg(subject="rust-core"),
        )
        with pytest.raises(grpc.aio.AioRpcError) as denied:
            await stub.PurgeDocument(purge, metadata=_headers(purge))
        assert denied.value.code() is grpc.StatusCode.PERMISSION_DENIED
        assert service.calls == 1
    finally:
        await channel.close()
        await server.stop(None)


@pytest.mark.asyncio
async def test_signed_purge_rejects_invalid_scope_before_index_work() -> None:
    class PurgeService:
        enabled = True
        calls = 0

        async def purge_document(
            self, *, tenant_id, organization_id, project_id, source_id, document_id
        ):
            self.calls += 1
            return 0

    service = PurgeService()
    boundary = AgentBridgeServicer(
        execution_service=object(),  # type: ignore[arg-type]
        identity_resolver=StaticTokenServiceIdentityResolver(
            _TOKEN, scopes={ServiceScope.DATA_HUB_PURGE}
        ),
        config=AgentBridgeServicerConfig(
            internal_service_secret=_SECRET, require_internal_signature=True
        ),
        replay_guard=InMemoryReplayGuard(),
    )
    server = grpc.aio.server()
    pbg.add_DataHubBridgeApiServicer_to_server(
        DataHubBridgeServicer(service=service, agent_bridge=boundary),
        server,  # type: ignore[arg-type]
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = pbg.DataHubBridgeApiStub(channel)
    try:
        for field in ("project_id", "source_id", "document_id"):
            for bad in ("not-a-uuid", "00000000-0000-0000-0000-000000000000"):
                request = pb.PurgeDocumentRequest(
                    tenant_id=_TENANT,
                    organization_id=_ORG,
                    project_id=_PROJECT,
                    source_id=_SOURCE,
                    document_id=_SOURCE,
                    correlation_id="corr-test",
                    request_id="req-test",
                    protocol_version="1.1.0",
                    deadline_rfc3339=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
                    identity=pb.ServiceIdentityMsg(subject="rust-core"),
                )
                setattr(request, field, bad)
                with pytest.raises(grpc.aio.AioRpcError) as rejected:
                    await stub.PurgeDocument(request, metadata=_headers(request))
                assert rejected.value.code() is grpc.StatusCode.INVALID_ARGUMENT
                assert service.calls == 0
        request.project_id = _PROJECT
        request.source_id = _SOURCE
        request.document_id = _SOURCE
        await stub.PurgeDocument(request, metadata=_headers(request))
        assert service.calls == 1
    finally:
        await channel.close()
        await server.stop(None)


@pytest.mark.asyncio
async def test_signed_data_hub_deadline_must_be_valid_and_unexpired() -> None:
    service = _DataService()
    boundary = AgentBridgeServicer(
        execution_service=object(),  # type: ignore[arg-type]
        identity_resolver=StaticTokenServiceIdentityResolver(
            _TOKEN, scopes={ServiceScope.DATA_HUB_SYNC}
        ),
        config=AgentBridgeServicerConfig(
            internal_service_secret=_SECRET, require_internal_signature=True
        ),
        replay_guard=InMemoryReplayGuard(),
    )
    server = grpc.aio.server()
    pbg.add_DataHubBridgeApiServicer_to_server(
        DataHubBridgeServicer(service=service, agent_bridge=boundary),
        server,  # type: ignore[arg-type]
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    stub = pbg.DataHubBridgeApiStub(channel)
    try:
        for deadline, expected in (
            ("", grpc.StatusCode.INVALID_ARGUMENT),
            ("invalid", grpc.StatusCode.INVALID_ARGUMENT),
            (
                (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
                grpc.StatusCode.DEADLINE_EXCEEDED,
            ),
        ):
            request = _request()
            request.deadline_rfc3339 = deadline
            with pytest.raises(grpc.aio.AioRpcError) as error:
                await stub.SyncSource(request, metadata=_headers(request))
            assert error.value.code() is expected
            assert service.calls == 0
        request = _request()
        await stub.SyncSource(request, metadata=_headers(request))
        assert service.calls == 1
    finally:
        await channel.close()
        await server.stop(None)
