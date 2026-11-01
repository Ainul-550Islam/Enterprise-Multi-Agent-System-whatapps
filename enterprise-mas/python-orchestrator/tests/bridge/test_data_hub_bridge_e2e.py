"""End-to-end tests for the Data Hub bridge through the *real* AgentBridge.

``tests/bridge/test_data_hub_security.py`` injects a double in place of the
AgentBridge servicer, so it proves that the Data Hub delegating boundary is
*called* but not that the thing it delegates to exists. That gap is exactly
where a missing method hides: the double satisfied the interface and the
production object did not.

These tests therefore stand up the real :class:`AgentBridgeServicer`, the real
:class:`DataHubBridgeServicer`, and a real gRPC server on a loopback port, and
drive them over the wire. Nothing between the client stub and the servicer is
faked except the Data Hub storage backend, which is not what is under test.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import grpc
import pytest

from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.data_hub_servicer import DataHubBridgeServicer
from mas_orchestrator.grpc.generated import mas_api_pb2, mas_api_pb2_grpc
from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig
from mas_orchestrator.security.identity import ServiceScope
from mas_orchestrator.security.internal_service import (
    SERVICE_HEADER,
    SIGNATURE_HEADER,
    signed_internal_signature,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

ALL_SCOPES = frozenset(ServiceScope)
CREDENTIAL = "test-credential-value"
SECRET = "test-internal-service-secret-value"
SUBJECT = "mas-api"  # must be on DEFAULT_ALLOWED_CALLERS
DEADLINE = timedelta(minutes=5).total_seconds()

#: The Data Hub schema requires UUID identifiers everywhere, because a
#: non-UUID id cannot be bound to a verified database row.
TENANT = "11111111-1111-4111-8111-111111111111"
ORG = "22222222-2222-4222-8222-222222222222"
PROJECT = "33333333-3333-4333-8333-333333333333"
SOURCE = "44444444-4444-4444-8444-444444444444"

#: The identity Rust signs into every Data Hub body. It is inside the HMAC, so
#: a caller cannot add or alter it after signing.
_IDENTITY = mas_api_pb2.ServiceIdentityMsg(
    subject=SUBJECT,
    issuer="static",
    scopes=(ServiceScope.DATA_HUB_SYNC.value,),
)


class _Resolver:
    """A resolver granting an explicit scope set to one bearer token."""

    def __init__(self, scopes: frozenset[ServiceScope]) -> None:
        self._scopes = scopes

    async def resolve(self, credential: str | None) -> Any:
        """Returns the identity, or raises when the token does not match.

        Args:
            credential: The presented bearer credential.

        Returns:
            The authenticated identity.

        Raises:
            Exception: When the credential does not match.
        """
        if credential != f"Bearer {CREDENTIAL}":
            raise RuntimeError("credential rejected")
        from mas_orchestrator.security.identity import ServiceIdentity

        return ServiceIdentity(subject=SUBJECT, issuer="static", scopes=self._scopes)


class _SyncableService:
    """The smallest Data Hub service that can answer ``SyncSource``.

    Storage is not under test here; the trust boundary is. The double records
    the request it received so the test can assert the servicer forwarded the
    authenticated, parsed body rather than something it rebuilt.
    """

    def __init__(self) -> None:
        self.synced: list[Any] = []
        self.closed = False

    async def startup(self) -> None:
        """No-op: no pools are held."""

    async def aclose(self) -> None:
        """Records that shutdown reached the service."""
        self.closed = True

    async def sync_source(self, source: Any) -> Any:
        """Records the source snapshot and returns an empty, successful report.

        Args:
            source: The parsed, validated source snapshot.

        Returns:
            A report with no work done and a stable cursor.
        """
        from mas_orchestrator.data_hub.service import SourceSyncReport

        self.synced.append(source)
        return SourceSyncReport(
            source_id=source.source_id,
            discovered_count=0,
            changed_count=0,
            indexed_count=0,
            failed_count=0,
            next_cursor="",
        )


def _metadata(request: Any) -> tuple[tuple[str, str], ...]:
    """Builds signed inbound metadata for *request*.

    The signature is produced by the same routine Rust uses, over the exact
    deterministic serialisation of the message, so this exercises the real
    verification path rather than bypassing it.

    Args:
        request: The protobuf message about to be sent.

    Returns:
        Metadata pairs to attach to the call.
    """
    body = request.SerializeToString(deterministic=True)
    signature = signed_internal_signature(
        SECRET,
        service=SUBJECT,
        signed_at_ms=int(datetime.now(UTC).timestamp() * 1000),
        body=body,
    )
    return (
        ("authorization", f"Bearer {CREDENTIAL}"),
        (SERVICE_HEADER, SUBJECT),
        (SIGNATURE_HEADER, signature),
    )


@pytest.fixture()
async def data_hub_bridge() -> AsyncIterator[tuple[str, _SyncableService]]:
    """Runs a real Data Hub + AgentBridge server on a loopback port.

    Yields:
        The target address and the service double behind the servicer.
    """
    service = _SyncableService()
    bridge = AgentBridgeServicer(
        execution_service=object(),  # type: ignore[arg-type]
        identity_resolver=_Resolver(ALL_SCOPES),
        config=AgentBridgeServicerConfig(
            require_scope=ServiceScope.EXECUTE_AGENT,
            internal_service_secret=SECRET,
        ),
    )
    servicer = DataHubBridgeServicer(
        service=service,  # type: ignore[arg-type]
        agent_bridge=bridge,
    )
    server = GrpcServer(
        servicer=bridge,
        data_hub_servicer=servicer,
        closeables=(service,),
        config=GrpcServerConfig(host="127.0.0.1", port=0),
    )
    port = await server.start()
    try:
        yield f"127.0.0.1:{port}", service
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_signed_data_hub_call_reaches_the_service(
    data_hub_bridge: tuple[str, _SyncableService],
) -> None:
    """A correctly signed call authenticates through the real bridge."""
    target, service = data_hub_bridge
    request = mas_api_pb2.SyncSourceRequest(
        tenant_id=TENANT,
        organization_id=ORG,
        project_id=PROJECT,
        source_id=SOURCE,
        source_name="Uploads",
        source_kind="upload",
        configuration_json="{}",
        protocol_version="1.1.0",
        deadline_rfc3339=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        identity=_IDENTITY,
        cursor="",
    )
    async with grpc.aio.insecure_channel(target) as channel:
        stub = mas_api_pb2_grpc.DataHubBridgeApiStub(channel)
        reply = await stub.SyncSource(
            request,
            metadata=_metadata(request),
            timeout=DEADLINE,
        )

    assert reply.source_id == SOURCE
    assert len(service.synced) == 1, "the service was never reached"
    # The servicer forwards the *parsed* snapshot, so tenancy as Rust signed it
    # is what reaches the pipeline. Asserting on it is what proves the body was
    # parsed rather than passed through as an opaque blob.
    forwarded = service.synced[0]
    assert forwarded.tenant_id == TENANT
    assert forwarded.source_id == SOURCE
    assert forwarded.kind.value == "upload"


@pytest.mark.asyncio
async def test_tampered_data_hub_body_is_refused(
    data_hub_bridge: tuple[str, _SyncableService],
) -> None:
    """A signature lifted from one body cannot authorise a different body."""
    target, service = data_hub_bridge
    signed = mas_api_pb2.SyncSourceRequest(
        tenant_id=TENANT,
        organization_id=ORG,
        project_id=PROJECT,
        source_id=SOURCE,
        source_name="Uploads",
        source_kind="upload",
        configuration_json="{}",
        protocol_version="1.1.0",
        identity=_IDENTITY,
    )
    metadata = _metadata(signed)

    # Same metadata, different body: the HMAC no longer covers what arrived.
    tampered = mas_api_pb2.SyncSourceRequest(
        tenant_id="99999999-9999-4999-8999-999999999999",
        source_id=SOURCE,
        source_kind="upload",
        protocol_version="1.1.0",
        identity=_IDENTITY,
    )
    async with grpc.aio.insecure_channel(target) as channel:
        stub = mas_api_pb2_grpc.DataHubBridgeApiStub(channel)
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.SyncSource(tampered, metadata=metadata, timeout=DEADLINE)

    assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
    assert service.synced == [], "a tampered body reached the service"


@pytest.mark.asyncio
async def test_missing_scope_is_refused_before_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An authenticated caller without the operation's scope is refused."""
    service = _SyncableService()
    bridge = AgentBridgeServicer(
        execution_service=object(),  # type: ignore[arg-type]
        identity_resolver=_Resolver(frozenset({ServiceScope.EXECUTE_AGENT})),
        config=AgentBridgeServicerConfig(
            require_scope=ServiceScope.EXECUTE_AGENT,
            internal_service_secret=SECRET,
        ),
    )
    servicer = DataHubBridgeServicer(
        service=service,  # type: ignore[arg-type]
        agent_bridge=bridge,
    )
    server = GrpcServer(
        servicer=bridge,
        data_hub_servicer=servicer,
        closeables=(service,),
        config=GrpcServerConfig(host="127.0.0.1", port=0),
    )
    port = await server.start()
    try:
        request = mas_api_pb2.SyncSourceRequest(
            tenant_id=TENANT,
            organization_id=ORG,
            project_id=PROJECT,
            source_id=SOURCE,
            source_name="Uploads",
            source_kind="upload",
            configuration_json="{}",
            protocol_version="1.1.0",
            identity=_IDENTITY,
        )
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = mas_api_pb2_grpc.DataHubBridgeApiStub(channel)
            with pytest.raises(grpc.aio.AioRpcError) as raised:
                await stub.SyncSource(
                    request,
                    metadata=_metadata(request),
                    timeout=DEADLINE,
                )
    finally:
        await server.stop()

    assert raised.value.code() is grpc.StatusCode.PERMISSION_DENIED
    assert service.synced == [], "an unauthorised call reached the service"
