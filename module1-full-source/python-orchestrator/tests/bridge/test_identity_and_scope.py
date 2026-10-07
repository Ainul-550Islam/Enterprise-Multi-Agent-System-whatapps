# PURPOSE:
# Tests that the bridge rejects callers it cannot authenticate or authorise,
# and that tenancy is never taken from a header.
#
# Covers spec §10: reject missing, malformed and forged identity, and reject a
# mismatched org/tenant scope. Every rejection must be a gRPC abort with
# UNAUTHENTICATED or PERMISSION_DENIED — never a retryable transport error,
# which would make Rust retry a call that can never succeed.

"""Identity, authentication and tenant-scope tests for the bridge.

The threat these tests encode is a caller that can reach the port but should
not be able to run agents. Three variants matter and are tested separately:

* **Absent** — no credential header at all.
* **Forged** — a well-formed header with the wrong token.
* **Authenticated but unauthorised** — a valid credential without the
  ``execute_agent`` scope.

Each must fail *before* any AI work is dispatched, and none may be classified
retryable: a retry would repeat the same rejection forever.

Tenant scope is separate. A tenant header is **cross-checked** against the
request body, never trusted on its own; and an identity declared in the body
must agree with the credential that was actually authenticated.
"""

from __future__ import annotations

import grpc
import pytest
from tests.bridge.conftest import (
    CREDENTIAL,
    ORGANIZATION,
    OTHER_TENANT,
    SUBJECT,
    TENANT,
    BridgeHarness,
    FakeContext,
    abort_code,
    abort_detail,
    metadata,
    request,
    resolver,
)

from mas_orchestrator.grpc.errors import (
    ERROR_DETAILS_METADATA_KEY,
    details_from_abort,
    is_retryable_status,
)
from mas_orchestrator.grpc.generated import mas_api_pb2, mas_api_pb2_grpc
from mas_orchestrator.security.identity import ServiceScope

pytestmark = pytest.mark.asyncio(loop_scope="function")


async def _run(harness: BridgeHarness, **request_kwargs: object):
    """Calls ``RunAgent`` over the wire with metadata that agrees with the body.

    Args:
        harness: The running bridge.
        **request_kwargs: Overrides for the request.

    Returns:
        The reply.

    Raises:
        grpc.aio.AioRpcError: When the call is aborted.

    The tenant and organization headers are derived from the request so that
    only a test that *means* to disagree produces a mismatch.
    """
    message = request(**request_kwargs)  # type: ignore[arg-type]
    stub, channel = harness.stub()
    try:
        return await stub.RunAgent(
            message,
            metadata=metadata(tenant_id=message.tenant_id, organization_id=message.organization_id),
        )
    finally:
        await channel.close()


class TestCredentialRejection:
    """A caller that cannot prove who it is must not reach the runtime."""

    async def test_a_missing_credential_is_rejected_before_dispatch(
        self, bridge: BridgeHarness
    ) -> None:
        """No credential header: aborted with UNAUTHENTICATED, nothing run."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(), metadata=metadata(credential=None))
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_a_forged_credential_is_rejected_before_dispatch(
        self, bridge: BridgeHarness
    ) -> None:
        """A well-formed header carrying the wrong token is refused."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(), metadata=metadata(credential="not-the-token"))
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_a_malformed_credential_is_rejected(self, bridge: BridgeHarness) -> None:
        """A header with no token after the scheme is refused."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(), metadata=[("authorization", "Bearer ")])
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_the_rejection_is_not_retryable(self, bridge: BridgeHarness) -> None:
        """An authentication failure must never be retried by Rust."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(), metadata=metadata(credential="wrong"))
        await channel.close()

        assert is_retryable_status(raised.value) is False

    async def test_the_rejection_carries_structured_details(self, bridge: BridgeHarness) -> None:
        """The abort carries a machine-readable reason for the caller's logs."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(), metadata=metadata(credential="wrong"))
        await channel.close()

        detail = details_from_abort(raised.value)
        assert detail["code"] == "authentication_error"
        assert detail["details"]["reason"] == "mismatch"

    async def test_the_credential_is_never_echoed_back(self, bridge: BridgeHarness) -> None:
        """Neither the status message nor details may contain the token."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(), metadata=metadata(credential="super-secret"))
        await channel.close()

        blob = f"{raised.value.details()}{raised.value.debug_error_string()}"
        assert "super-secret" not in blob


class TestScopeEnforcement:
    """An authenticated caller without the right scope is refused."""

    async def test_a_caller_without_execute_scope_is_refused(self) -> None:
        """A read-health-only credential cannot run agents."""
        from mas_orchestrator.grpc.agent_bridge_servicer import (
            AgentBridgeServicer,
            AgentBridgeServicerConfig,
        )
        from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig

        service = _no_dispatch_service()
        servicer = AgentBridgeServicer(
            execution_service=service,
            identity_resolver=resolver(scopes=frozenset({ServiceScope.READ_HEALTH})),
            config=AgentBridgeServicerConfig(require_scope=ServiceScope.EXECUTE_AGENT),
        )
        server = GrpcServer(servicer=servicer, config=GrpcServerConfig(host="127.0.0.1", port=0))
        port = await server.start()
        try:
            channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
            stub = mas_api_pb2_grpc.AgentBridgeApiStub(channel)
            with pytest.raises(grpc.aio.AioRpcError) as raised:
                await stub.RunAgent(request(), metadata=metadata())
            await channel.close()
        finally:
            await server.stop()

        assert raised.value.code() is grpc.StatusCode.PERMISSION_DENIED
        assert is_retryable_status(raised.value) is False
        assert service.call_count == 0

    async def test_health_does_not_require_the_execute_scope(self, bridge: BridgeHarness) -> None:
        """Readiness is readable by any authenticated caller."""
        stub, channel = bridge.stub()
        reply = await stub.Health(mas_api_pb2.Empty(), metadata=metadata())
        await channel.close()

        assert reply.healthy is True

    async def test_a_caller_with_execute_scope_is_accepted(self, bridge: BridgeHarness) -> None:
        """The happy path: a valid, scoped credential reaches the runtime."""
        reply = await _run(bridge)

        assert reply.WhichOneof("verdict") == "succeeded"
        assert bridge.service.call_count == 1


class TestTenantScope:
    """Tenancy is authoritative in the body and cross-checked everywhere."""

    async def test_a_missing_tenant_is_rejected(self, bridge: BridgeHarness) -> None:
        """An unscoped run would execute outside every tenant's boundary."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(tenant_id=""), metadata=metadata(tenant_id=None))
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.FAILED_PRECONDITION
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_a_missing_organization_is_rejected(self, bridge: BridgeHarness) -> None:
        """The organization scope is as load-bearing as the tenant."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(
                request(organization_id=""), metadata=metadata(organization_id=None)
            )
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.FAILED_PRECONDITION
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_a_header_tenant_that_disagrees_with_the_body_is_rejected(
        self, bridge: BridgeHarness
    ) -> None:
        """A spliced header must not redirect the run to another tenant."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(
                request(tenant_id=TENANT), metadata=metadata(tenant_id=OTHER_TENANT)
            )
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.FAILED_PRECONDITION
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_a_header_organization_that_disagrees_is_rejected(
        self, bridge: BridgeHarness
    ) -> None:
        """The same cross-check applies to the organization."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(
                request(organization_id=ORGANIZATION),
                metadata=metadata(organization_id="org-other"),
            )
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.FAILED_PRECONDITION
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_a_body_identity_that_disagrees_with_the_credential_is_rejected(
        self, bridge: BridgeHarness
    ) -> None:
        """A declared subject must match the identity actually authenticated."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(identity_subject="someone-else"), metadata=metadata())
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert is_retryable_status(raised.value) is False
        assert bridge.service.call_count == 0

    async def test_the_run_executes_under_the_body_tenant(self, bridge: BridgeHarness) -> None:
        """Isolation: the context carries the body's tenant, not a default."""
        await _run(bridge, tenant_id=OTHER_TENANT)

        recorded = bridge.service.runs[0]
        assert recorded.context.tenant_id == OTHER_TENANT
        assert recorded.context.organization_id == ORGANIZATION


class TestTenantIsolation:
    """One tenant's run must not touch another tenant's data."""

    async def test_each_run_sees_only_its_own_tenant(self, bridge: BridgeHarness) -> None:
        """Two runs for different tenants stay separate."""
        await _run(bridge, tenant_id=TENANT, execution_id="exec-a")
        await _run(bridge, tenant_id=OTHER_TENANT, execution_id="exec-b")

        first, second = bridge.service.runs
        assert first.context.tenant_id == TENANT
        assert second.context.tenant_id == OTHER_TENANT
        assert first.context.execution_id != second.context.execution_id

    async def test_the_correlation_id_travels_per_run(self, bridge: BridgeHarness) -> None:
        """Correlation is per-run, so two tenants never share one."""
        await _run(bridge, correlation_id="corr-a", request_id="req-a")
        await _run(bridge, correlation_id="corr-b", request_id="req-b")

        first, second = bridge.service.runs
        assert first.context.correlation_id == "corr-a"
        assert second.context.correlation_id == "corr-b"

    async def test_a_failure_reply_reports_the_run_identity(self, bridge: BridgeHarness) -> None:
        """A failed verdict names the correlation and request it belongs to."""
        bridge.service.error = _boom()
        reply = await _run(bridge, correlation_id="corr-42", request_id="req-42")

        assert reply.failed.correlation_id == "corr-42"
        assert reply.failed.request_id == "req-42"


class TestStageLevelRejection:
    """The stages abort directly, without a wire round trip."""

    async def test_authenticate_aborts_on_a_bad_credential(self) -> None:
        """``_authenticate`` aborts rather than returning a partial identity."""
        from mas_orchestrator.grpc.agent_bridge_servicer import (
            AgentBridgeServicer,
            AgentBridgeServicerConfig,
        )
        from mas_orchestrator.grpc.metadata import extract_call_metadata

        servicer = AgentBridgeServicer(
            execution_service=_no_dispatch_service(),
            identity_resolver=resolver(),
            config=AgentBridgeServicerConfig(require_scope=ServiceScope.EXECUTE_AGENT),
        )
        context = FakeContext(metadata(credential="wrong"))

        # The real context raises from `abort`; the fake does too, so the stage
        # cannot continue past it.
        with pytest.raises(Exception):  # noqa: B017 - raised by the fake context
            await servicer._authenticate(
                extract_call_metadata(tuple(context.invocation_metadata())), context
            )

        assert abort_code(context) is grpc.StatusCode.UNAUTHENTICATED
        assert abort_detail(context, ERROR_DETAILS_METADATA_KEY) is not None


def _boom() -> Exception:
    """A typed runtime failure.

    Returns:
        An :class:`OrchestratorError` the runtime could plausibly raise.
    """
    from mas_orchestrator.contracts.errors import OrchestratorError

    return OrchestratorError("the model call failed")


def _no_dispatch_service():
    """A double that records without answering, for rejection tests.

    Returns:
        The double.
    """
    from tests.bridge.conftest import RecordingExecutionService

    return RecordingExecutionService()


async def test_the_resolver_asserts_the_subject_the_body_declares() -> None:
    """Guards the fixture: the body subject must equal what the resolver asserts."""
    from mas_orchestrator.security.identity import ServiceIdentity

    resolved = resolver()
    identity = await resolved.resolve(f"Bearer {CREDENTIAL}")

    assert isinstance(identity, ServiceIdentity)
    assert identity.subject == SUBJECT
