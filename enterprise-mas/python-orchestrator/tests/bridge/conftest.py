# PURPOSE:
# Test fixtures for the agent bridge suite.
#
# Provides a real gRPC server on a real port, a real AgentBridgeServicer, and
# a controllable execution service double. The double exists only here: it
# stands in for the Agent Runtime so these tests exercise the bridge's own
# behaviour — identity, scope, deadlines, cancellation, error mapping —
# without depending on a live model provider.

"""Shared fixtures for the bridge tests.

The bridge is exercised over a **real** gRPC server bound to an ephemeral port
on loopback. Nothing about the transport is faked: an abort really aborts, a
cancellation really cancels, and a trailing-metadata detail really travels.
Faking those would let a bug in exactly the code under test pass unnoticed.

What is substituted is only the Agent Runtime behind the servicer. Talking to
a live model provider from a unit test would make the suite non-deterministic,
bill per run, and test the provider rather than the bridge.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest
import pytest_asyncio

from mas_orchestrator.agents.base import Termination
from mas_orchestrator.agents.runtime import ExecutionResult
from mas_orchestrator.contracts.errors import OrchestratorError
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.generated import mas_api_pb2, mas_api_pb2_grpc
from mas_orchestrator.grpc.metadata import (
    AUTHORIZATION_HEADER,
    CORRELATION_HEADER,
    INTERNAL_SERVICE_HEADER,
    INTERNAL_SIGNATURE_HEADER,
    ORGANIZATION_HEADER,
    PROTOCOL_VERSION_HEADER,
    REQUEST_ID_HEADER,
    TENANT_HEADER,
)
from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig
from mas_orchestrator.security.identity import (
    ServiceIdentity,
    ServiceScope,
    StaticTokenServiceIdentityResolver,
)
from mas_orchestrator.security.internal_service import (
    now_millis,
    signed_internal_signature,
)

TENANT = "tenant-alpha"
OTHER_TENANT = "tenant-beta"
ORGANIZATION = "org-1"
CREDENTIAL = "bridge-service-token"
SUBJECT = "rust-core"

# A literal secret is acceptable only in a test fixture: it stands in for the
# value the secret manager supplies, and every path that reads it is the path
# under test. Production reads it from `security.internal_service_secret_ref`.
SIGNING_SECRET = "internal-shared-secret"
#: The service id the Rust worker presents. It is on the default allowlist.
INTERNAL_SERVICE = "mas-worker"

ALL_SCOPES = frozenset(
    {
        ServiceScope.EXECUTE_AGENT,
        ServiceScope.INVOKE_API,
        ServiceScope.READ_HEALTH,
    }
)


@dataclass(slots=True)
class RecordedRun:
    """One execution the double was asked to perform.

    Attributes:
        request_text: The prompt.
        context: The execution context the servicer built.
        cancel_event: The cancellation event handed to the run, so a test can
            observe whether the bridge mirrored a wire cancellation onto it.
    """

    request_text: str
    context: Any
    cancel_event: asyncio.Event


@dataclass(slots=True)
class RecordingExecutionService:
    """Stands in for the Agent Runtime.

    Attributes:
        result: The result to return, when the behaviour is "return this".
        error: The error to raise, when the behaviour is "raise this".
        delay_seconds: How long to wait before answering, so a test can
            cancel while the run is in flight.
        honour_cancel: Whether to wait on the cancel event, so a test can
            prove the bridge sets it.
        runs: Every run the double was asked to perform.
    """

    result: ExecutionResult | None = None
    error: Exception | None = None
    delay_seconds: float = 0.0
    honour_cancel: bool = False
    runs: list[RecordedRun] = field(default_factory=list)

    async def execute(self, request: Any, *, cancel_event: Any = None) -> ExecutionResult:
        """Records the run and answers according to the configured behaviour.

        Args:
            request: The execution request.
            cancel_event: The bridge's cancellation signal.

        Returns:
            The configured result.

        Raises:
            Exception: The configured error, if one was set.
        """
        event = cancel_event if cancel_event is not None else asyncio.Event()
        self.runs.append(RecordedRun(request.request, request.context, event))

        if self.delay_seconds:
            if self.honour_cancel:
                await asyncio.wait_for(event.wait(), timeout=self.delay_seconds)
            else:
                await asyncio.sleep(self.delay_seconds)

        if self.error is not None:
            raise self.error
        if self.result is not None:
            return self.result
        return ExecutionResult(
            execution_id=request.context.execution_id,
            agent_id=request.context.agent_id,
            termination=Termination.COMPLETED,
            content="ok",
        )

    @property
    def call_count(self) -> int:
        """How many runs were dispatched."""
        return len(self.runs)


def make_service(**kwargs: object) -> RecordingExecutionService:
    """Builds a double.

    Args:
        **kwargs: Fields to set on the double.

    Returns:
        The double.
    """
    return RecordingExecutionService(**kwargs)  # type: ignore[arg-type]


def success_result(execution_id: str = "exec-1", content: str = "the answer") -> ExecutionResult:
    """A completed execution result.

    Args:
        execution_id: Execution id.
        content: The answer.

    Returns:
        The result.
    """
    return ExecutionResult(
        execution_id=execution_id,
        agent_id="platform.default",
        termination=Termination.COMPLETED,
        content=content,
    )


def failed_result(
    termination: Termination = Termination.FAILED,
    error: Exception | None = None,
) -> ExecutionResult:
    """A stopped execution result.

    Args:
        termination: Why it stopped.
        error: The error, if any.

    Returns:
        The result.
    """
    return ExecutionResult(
        execution_id="exec-1",
        agent_id="platform.default",
        termination=termination,
        content="",
        error=error,
    )


def request(
    *,
    tenant_id: str = TENANT,
    organization_id: str = ORGANIZATION,
    input_json: str = "hello",
    agent_id: str = "platform.default",
    correlation_id: str = "corr-1",
    request_id: str = "req-1",
    execution_id: str = "exec-1",
    deadline_rfc3339: str = "",
    identity_subject: str = SUBJECT,
    idempotency_key: str = "",
) -> mas_api_pb2.AgentRunRequest:
    """Builds a well-formed ``AgentRunRequest``.

    Args:
        tenant_id: Authoritative tenant.
        organization_id: Authoritative organization.
        input_json: The run input.
        agent_id: The agent to run.
        correlation_id: Correlation id.
        request_id: Request id.
        execution_id: Execution id.
        deadline_rfc3339: Deadline, or empty for none.
        identity_subject: The declared caller subject.
        idempotency_key: Idempotency key, or empty.

    Returns:
        The request.
    """
    message = mas_api_pb2.AgentRunRequest(
        agent_id=agent_id,
        input_json=input_json,
        correlation_id=correlation_id,
        request_id=request_id,
        execution_id=execution_id,
        tenant_id=tenant_id,
        organization_id=organization_id,
        protocol_version=str(CURRENT_PROTOCOL_VERSION),
        deadline_rfc3339=deadline_rfc3339,
        identity=mas_api_pb2.ServiceIdentityMsg(subject=identity_subject),
    )
    if idempotency_key:
        message.idempotency_key = idempotency_key
    return message


def metadata(
    *,
    credential: str | None = CREDENTIAL,
    tenant_id: str | None = TENANT,
    organization_id: str | None = ORGANIZATION,
    correlation_id: str | None = "corr-1",
    request_id: str | None = "req-1",
    protocol_version: str | None = str(CURRENT_PROTOCOL_VERSION),
) -> list[tuple[str, str]]:
    """Builds inbound gRPC metadata.

    Args:
        credential: The bearer token, or ``None`` to omit the header.
        tenant_id: Tenant header, or ``None`` to omit.
        organization_id: Organization header, or ``None`` to omit.
        correlation_id: Correlation header, or ``None`` to omit.
        request_id: Request header, or ``None`` to omit.
        protocol_version: Protocol header, or ``None`` to omit.

    Returns:
        Metadata pairs, omitting any whose value is ``None``.
    """
    pairs: list[tuple[str, str]] = []
    if credential is not None:
        pairs.append((AUTHORIZATION_HEADER, f"Bearer {credential}"))
    if tenant_id is not None:
        pairs.append((TENANT_HEADER, tenant_id))
    if organization_id is not None:
        pairs.append((ORGANIZATION_HEADER, organization_id))
    if correlation_id is not None:
        pairs.append((CORRELATION_HEADER, correlation_id))
    if request_id is not None:
        pairs.append((REQUEST_ID_HEADER, request_id))
    if protocol_version is not None:
        pairs.append((PROTOCOL_VERSION_HEADER, protocol_version))
    return pairs


def make_context(
    *,
    tenant_id: str = TENANT,
    organization_id: str = ORGANIZATION,
    execution_id: str = "exec-1",
    agent_id: str = "platform.default",
    correlation_id: str = "corr-1",
    request_id: str = "req-1",
):
    """Builds a real ``ExecutionContext``.

    Args:
        tenant_id: Tenant scope.
        organization_id: Organization scope.
        execution_id: Execution id.
        agent_id: Agent id.
        correlation_id: Correlation id.
        request_id: Request id.

    Returns:
        A context built by the production builder, so a test cannot drift from
        the real required-field set by constructing one by hand.
    """
    from mas_orchestrator.context.execution_context import build_execution_context
    from mas_orchestrator.security.identity import ServiceIdentity

    return build_execution_context(
        tenant_id=tenant_id,
        organization_id=organization_id,
        execution_id=execution_id,
        agent_id=agent_id,
        correlation_id=correlation_id,
        request_id=request_id,
        caller=ServiceIdentity(subject=SUBJECT, issuer="static", scopes=ALL_SCOPES),
        protocol_version=str(CURRENT_PROTOCOL_VERSION),
    )


def resolver(
    token: str = CREDENTIAL,
    scopes: frozenset[ServiceScope] = ALL_SCOPES,
) -> StaticTokenServiceIdentityResolver:
    """Builds the real static-token resolver.

    Args:
        token: The accepted token.
        scopes: The scopes to grant.

    Returns:
        The resolver. Tests use the real one so the constant-time comparison
        and the ``Bearer`` stripping are genuinely exercised.
    """
    return StaticTokenServiceIdentityResolver(token=token, subject=SUBJECT, scopes=scopes)


@pytest.fixture
def execution_service() -> RecordingExecutionService:
    """A fresh execution double per test."""
    return RecordingExecutionService()


@pytest_asyncio.fixture
async def bridge(execution_service: RecordingExecutionService):
    """Runs a real bridge server on an ephemeral port.

    Yields:
        A :class:`BridgeHarness` with the port and the double.
    """
    servicer = AgentBridgeServicer(
        execution_service=execution_service,  # type: ignore[arg-type]
        identity_resolver=resolver(),
        config=AgentBridgeServicerConfig(require_scope=ServiceScope.EXECUTE_AGENT),
    )
    server = GrpcServer(
        servicer=servicer,
        config=GrpcServerConfig(host="127.0.0.1", port=0),
    )
    port = await server.start()
    try:
        yield BridgeHarness(port=port, service=execution_service, server=server)
    finally:
        await server.stop()


@dataclass(frozen=True, slots=True)
class BridgeHarness:
    """Handle on a running bridge server.

    Attributes:
        port: The bound port.
        service: The execution double behind it.
        server: The server itself.
    """

    port: int
    service: RecordingExecutionService
    server: GrpcServer

    def stub(self):
        """Creates a client stub on a fresh channel.

        Returns:
            An ``AgentBridgeApiStub``. The caller owns closing the channel.
        """
        import grpc

        channel = grpc.aio.insecure_channel(f"127.0.0.1:{self.port}")
        return mas_api_pb2_grpc.AgentBridgeApiStub(channel), channel


class FakeContext:
    """A servicer context for tests that call servicer stages directly.

    Attributes:
        metadata: Invocation metadata.
        aborted: The status an abort produced, when one happened.
    """

    def __init__(self, metadata_pairs: list[tuple[str, str]] | None = None) -> None:
        self.metadata = metadata_pairs or []
        self.aborted: tuple[Any, str, Any] | None = None
        self._cancelled = False

    def invocation_metadata(self):
        """The metadata the servicer will parse."""
        return self.metadata

    async def abort(self, code: Any, message: str, trailing: Any = ()) -> None:
        """Records the abort and raises, as the real context does.

        Raises:
            grpc.aio.AbortError: The exception the real ``abort`` raises.
                Raising it matters: a stage that continued past its abort
                would go on to dispatch work that was already refused.
        """
        from grpc.aio import AbortError

        self.aborted = (code, message, trailing)
        raise AbortError(message)

    def cancelled(self) -> bool:
        """Whether the call was cancelled."""
        return self._cancelled

    def done(self) -> bool:
        """Whether the call is finished."""
        return self._cancelled


def abort_code(context: FakeContext) -> Any:
    """The status code an abort produced.

    Args:
        context: The fake context.

    Returns:
        The status code.

    Raises:
        AssertionError: If nothing was aborted.
    """
    assert context.aborted is not None, "expected the call to be aborted"
    return context.aborted[0]


def abort_detail(context: FakeContext, key: str) -> bytes | None:
    """Reads one trailing-metadata entry from an abort.

    Args:
        context: The fake context.
        key: The metadata key.

    Returns:
        The value, or ``None`` when absent.
    """
    assert context.aborted is not None, "expected the call to be aborted"
    for name, value in context.aborted[2] or ():
        if name == key:
            return value
    return None


@pytest.fixture
def any_error() -> type[OrchestratorError]:
    """The typed error base, for tests that construct one."""
    return OrchestratorError


@pytest.fixture
def identity() -> ServiceIdentity:
    """A fully-scoped service identity."""
    return ServiceIdentity(subject=SUBJECT, issuer="static", scopes=ALL_SCOPES)


def internal_signature_pairs(
    message: mas_api_pb2.AgentRunRequest,
    *,
    secret: str = SIGNING_SECRET,
    service: str = INTERNAL_SERVICE,
    at_ms: int | None = None,
) -> list[tuple[str, str]]:
    """Signs *message* and returns the two internal-call metadata pairs.

    Args:
        message: The request whose bytes are signed. The signature covers
            exactly these bytes, so a test that signs one message and sends
            another is the tamper case.
        secret: The shared secret to sign with.
        service: The service id to claim.
        at_ms: Signing time. Defaults to now, which is inside the skew window.

    Returns:
        The signature and service header pairs, ready to append to metadata.
    """
    signature = signed_internal_signature(
        secret, service, at_ms if at_ms is not None else now_millis(), _canonical(message)
    )
    return [(INTERNAL_SIGNATURE_HEADER, signature), (INTERNAL_SERVICE_HEADER, service)]


def _canonical(message: mas_api_pb2.AgentRunRequest) -> bytes:
    """The exact bytes the servicer will verify against.

    Args:
        message: The request.

    Returns:
        Its canonical serialization. Using the same encoding on both sides is
        what makes the check a check: a signature over *a* request is not a
        signature over *this* request.
    """
    encoded: bytes = message.SerializeToString(deterministic=True)
    return encoded


@pytest_asyncio.fixture
async def signed_bridge(execution_service: RecordingExecutionService):
    """Runs a bridge that requires a verified internal signature.

    This is the production posture: a secret is configured, so a call without
    a valid signature is refused no matter what bearer token it carries.

    Yields:
        A :class:`BridgeHarness` with the port and the double.
    """
    servicer = AgentBridgeServicer(
        execution_service=execution_service,  # type: ignore[arg-type]
        identity_resolver=resolver(),
        config=AgentBridgeServicerConfig(
            require_scope=ServiceScope.EXECUTE_AGENT,
            internal_service_secret=SIGNING_SECRET,
        ),
    )
    server = GrpcServer(servicer=servicer, config=GrpcServerConfig(host="127.0.0.1", port=0))
    port = await server.start()
    try:
        yield BridgeHarness(port=port, service=execution_service, server=server)
    finally:
        await server.stop()
