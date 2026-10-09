# PURPOSE:
# gRPC servicer implementing the Rust-owned AgentBridgeApi contract.
# This module is the inbound trust boundary: it authenticates the Rust Core
# service, reconciles the authoritative tenant scope, builds the execution
# context and dispatches to the real Agent Runtime. It never invents identity,
# never trusts a header for tenancy, and never converts a failure into a
# success.

"""Production ``AgentBridgeApi`` servicer.

This is the Python half of the Rust ↔ Python AgentBridge. Rust is the caller
and the sole authority on execution state; this servicer only runs AI work and
reports what happened.

The twelve steps the spec requires, in the order they happen:

1.  Accept the gRPC call.
2.  Validate metadata (``extract_call_metadata``).
3.  Authenticate the Rust service identity (``ServiceIdentityResolver``).
4.  Reconcile identity/tenant scope (``reconcile_tenant_scope``).
5.  Construct the execution context (``build_execution_context``).
6.  Validate the request body.
7.  Dispatch to the Agent Runtime (``ExecutionService``).
8.  Enforce the deadline.
9.  Support cancellation.
10. Map Agent Runtime errors.
11. Return the structured result.
12. Emit safe telemetry.

**The error channel is a deliberate design decision, not an implementation
detail.** There are exactly two ways a call can end, and choosing between them
is what keeps Rust's retry classification correct:

* :class:`AgentFailed` **verdict** — the run was accepted and *then* failed
  (model timeout, tool denial, policy denial, cancellation). Rust records this
  as a failed execution step carrying the retryable flag Python supplied, and
  charges the reported usage.
* **gRPC status abort** — the call was rejected before any AI work (bad
  credential, unsupported protocol, tenant mismatch, malformed request). Rust
  maps the status onto its own taxonomy via
  :data:`~mas_orchestrator.grpc.errors.CODE_TO_STATUS`.

Returning a validation failure as a verdict would make it look like a failed
*run*, and Rust would dutifully record an execution step that never happened.
Aborting a genuine model failure would discard the usage accounting and the
retryable classification the runtime actually computed. Neither is acceptable,
so the split is enforced here.

Every validation stage is a coroutine for one reason: ``abort`` on the asyncio
server API is a coroutine, and a synchronous stage cannot terminate an RPC.
Rather than schedule the abort and race it, the stages are awaited in order.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.agents.base import Termination
from mas_orchestrator.agents.runtime import ExecutionResult
from mas_orchestrator.context.execution_context import build_execution_context
from mas_orchestrator.contracts.errors import (
    AuthenticationError,
    AuthorizationError,
    ConfigurationError,
    InvalidRequestError,
    OrchestratorError,
    TenantContextError,
)
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.grpc.agent_snapshot import parse_agent_snapshot
from mas_orchestrator.grpc.errors import abort_with_error
from mas_orchestrator.grpc.generated import mas_api_pb2, mas_api_pb2_grpc
from mas_orchestrator.grpc.metadata import (
    RustCallMetadata,
    extract_call_metadata,
    reconcile_tenant_scope,
)
from mas_orchestrator.observability.bridge_metrics import (
    BridgeTelemetry,
    NoopTelemetry,
)
from mas_orchestrator.security.internal_replay import InMemoryReplayGuard, InternalReplayGuard
from mas_orchestrator.security.internal_service import (
    DEFAULT_ALLOWED_CALLERS,
    now_millis,
    verify_internal_signature,
)
from mas_orchestrator.services.execution_service import ExecutionRequest

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    import grpc

    from mas_orchestrator.agents.base import AgentSpec
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.security.identity import (
        ServiceIdentity,
        ServiceIdentityResolver,
        ServiceScope,
    )
    from mas_orchestrator.services.execution_service import ExecutionService

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "CANCELLATION_POLL_SECONDS",
    "DEFAULT_MAX_INPUT_BYTES",
    "HEALTHY_DETAIL",
    "AgentBridgeServicer",
    "AgentBridgeServicerConfig",
    "finish_reason_for",
    "model_key_from_result",
    "parse_deadline",
    "usage_from_result",
]

#: Detail reported on a healthy probe. Static on purpose: a readiness reply is
#: not a place to echo configuration.
HEALTHY_DETAIL: Final[str] = "ai runtime ready"

#: Ceiling on the decoded run input, matching the platform's prompt bound.
DEFAULT_MAX_INPUT_BYTES: Final[int] = 1_048_576

#: How often the cancellation watcher re-checks the wire. Small enough that a
#: cancelled model call stops promptly, large enough not to spin.
CANCELLATION_POLL_SECONDS: Final[float] = 0.05

#: Terminal terminations mapped onto the wire ``finish_reason`` string. These
#: values must match the comment in ``mas.proto`` exactly.
_FINISH_REASONS: Final[Mapping[Termination, str]] = {
    Termination.COMPLETED: "completed",
    Termination.STEP_LIMIT: "step_limit",
    Termination.TOOL_CALL_LIMIT: "tool_call_limit",
    Termination.DEADLINE: "deadline",
    Termination.CANCELLED: "cancelled",
    Termination.BUDGET_EXHAUSTED: "budget_exhausted",
    Termination.FAILED: "failed",
}


def finish_reason_for(termination: Termination) -> str:
    """Renders a :class:`Termination` as the wire ``finish_reason`` string.

    Args:
        termination: Why the run stopped.

    Returns:
        The wire value. An unrecognised termination renders as ``"failed"``
        rather than raising: a report about a stopped run must not itself fail.
    """
    return _FINISH_REASONS.get(termination, "failed")


def parse_deadline(raw: str) -> int | None:
    """Parses an RFC-3339 deadline into epoch milliseconds.

    Args:
        raw: The deadline string, or empty.

    Returns:
        Epoch milliseconds, or ``None`` when absent.

    Raises:
        InvalidRequestError: If the value is present but unparseable. A caller
            that supplies a deadline it cannot express has made a request that
            cannot be bounded, and silently dropping the bound would let the
            run outlive the caller's own timeout.
    """
    if not raw or not raw.strip():
        return None

    candidate = raw.strip()
    # `fromisoformat` rejects a trailing "Z" on older Pythons; normalise it
    # rather than depending on the minor version.
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as error:
        raise InvalidRequestError(
            "agent run deadline is not a valid RFC-3339 timestamp",
            details={"field": "deadline_rfc3339"},
        ) from error

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return int(parsed.timestamp() * 1000)


def model_key_from_result(result: ExecutionResult) -> str:
    """Reports which model produced the answer.

    Args:
        result: The execution result.

    Returns:
        The ``provider/model`` key, or ``""`` when it cannot be named
        unambiguously.

    The key is read from the cost ledger, which records one entry per model
    actually called. A run that fanned out across models has several, and
    reporting one of them would attribute the answer to a model that may not
    have produced it — so it is left empty rather than guessed. Empty means
    "not reported", which Rust treats as unknown, not as a specific model.
    """
    cost = result.cost
    if cost is None:
        return ""
    by_model = getattr(cost, "cost_by_model", None)
    if by_model is None:
        return ""
    try:
        recorded: dict[str, float] = dict(by_model())
    except Exception:
        # A ledger that cannot summarise reports nothing. Reporting the model
        # is a diagnostics-only concern, and letting a broken ledger raise here
        # would turn a successful run into a failed reply.
        return ""
    if len(recorded) != 1:
        return ""
    return str(next(iter(recorded)))


def usage_from_result(result: ExecutionResult) -> mas_api_pb2.AgentUsageMsg | None:
    """Projects a result's cost ledger onto the wire usage message.

    Args:
        result: The execution result.

    Returns:
        The usage message, or ``None`` when the runtime recorded no ledger.
        Absent usage means "not reported", which the platform deliberately
        distinguishes from "zero consumed" — so no zero-filled message is
        fabricated here.
    """
    cost = result.cost
    if cost is None:
        return None
    totals = cost.totals()
    return mas_api_pb2.AgentUsageMsg(
        tokens_input=totals.input_tokens,
        tokens_output=totals.output_tokens,
        tool_calls=0,
        steps=0,
        active_time_ms=max(0, result.duration_ms),
    )


#: Services permitted to call the bridge. Re-exported from the security
#: package under a bridge-specific name so the servicer's configuration reads
#: as a statement about *this* edge rather than about a generic default.
DEFAULT_ALLOWED_INTERNAL_CALLERS: Final[frozenset[str]] = DEFAULT_ALLOWED_CALLERS


def _canonical_request_bytes(request: Any | None) -> bytes:
    """Serializes the request exactly as it will be verified.

    The signature is bound to the bytes that were sent, so the bytes verified
    here must be the ones received. ``deterministic=True`` fixes field
    ordering, which is what makes the encoding reproducible across the
    Rust (prost) and Python (protobuf) implementations.

    Args:
        request: The wire request, or ``None`` for a call with no body.

    Returns:
        The canonical encoding. An absent request yields ``b""``, which can
        only verify against a signature over an empty body — the caller's
        problem, not ours, and not a reason to skip the check.
    """
    if request is None:
        return b""
    # `SerializeToString` is typed `Any` by the generated stubs; the result is
    # always `bytes`, so the annotation is asserted once here rather than
    # letting `Any` leak into the signature-verification path.
    encoded: bytes = request.SerializeToString(deterministic=True)
    return encoded


@dataclass(frozen=True, slots=True)
class AgentBridgeServicerConfig:
    """Settings for the bridge servicer.

    Attributes:
        require_scope: Scope a caller must hold to run an agent. ``None``
            disables the check, which is only appropriate for tests: the
            production composition root always sets it.
        protocol_version: Version Python declares on its own behalf when the
            caller sends none.
        max_input_bytes: Ceiling on the decoded input payload. Refusing an
            oversized prompt here keeps it from reaching a provider, which
            would otherwise bill for it and then fail.
    internal_service_secret: Shared secret used to verify the caller's
        body-bound signature. ``None`` disables verification, which is only
        appropriate for tests — a bridge that accepts unsigned calls is
        trusting the network for identity.
    allowed_internal_callers: Service ids permitted to call this process.
        A syntactically perfect signature from a service that is not on this
        list is still a refusal.
    require_internal_signature: When ``True`` an unconfigured secret is a
        boot-time refusal rather than a silent skip. The production
        composition root sets it; tests leave it off. Note that a
        *configured* secret makes the signature mandatory regardless of this
        flag — it cannot be used to accept unsigned calls in production.
    """

    require_scope: ServiceScope | None = None
    protocol_version: str = str(CURRENT_PROTOCOL_VERSION)
    max_input_bytes: int = DEFAULT_MAX_INPUT_BYTES
    internal_service_secret: str | None = None
    allowed_internal_callers: frozenset[str] = DEFAULT_ALLOWED_INTERNAL_CALLERS
    require_internal_signature: bool = False
    #: Shared durable nonce consumer. Production composition supplies a
    #: PostgreSQL-backed guard; a process-local guard is allowed only for
    #: isolated unit/development use when strict production auth is off.
    replay_guard: InternalReplayGuard | None = None

    def __post_init__(self) -> None:
        if self.max_input_bytes < 1:
            raise InvalidRequestError(
                "max_input_bytes must be positive",
                details={"field": "max_input_bytes"},
            )


class AgentBridgeServicer(mas_api_pb2_grpc.AgentBridgeApiServicer):
    """Serves ``AgentBridgeApi`` for the Rust execution runtime.

    Args:
        execution_service: The canonical Agent Runtime entrypoint.
        identity_resolver: Authenticates the caller's bearer credential.
        config: Servicer settings.
        logger: Logger override, for tests.
    """

    def __init__(
        self,
        *,
        execution_service: ExecutionService,
        identity_resolver: ServiceIdentityResolver,
        config: AgentBridgeServicerConfig | None = None,
        telemetry: BridgeTelemetry | NoopTelemetry | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._service = execution_service
        self._resolver = identity_resolver
        self._config = config or AgentBridgeServicerConfig()
        self._replay_guard = self._config.replay_guard
        if (
            self._replay_guard is None
            and self._config.internal_service_secret
            and not self._config.require_internal_signature
        ):
            # Test/development fallback only. Production composition sets
            # require_internal_signature and supplies PostgreSQL explicitly.
            self._replay_guard = InMemoryReplayGuard()
        self._telemetry: BridgeTelemetry | NoopTelemetry = telemetry or NoopTelemetry()
        self._logger = logger or _LOGGER

    @property
    def config(self) -> AgentBridgeServicerConfig:
        """The servicer configuration in force."""
        return self._config

    @property
    def telemetry(self) -> BridgeTelemetry | NoopTelemetry:
        """Where this servicer records its measurements."""
        return self._telemetry

    # --- AgentBridgeApi -----------------------------------------------------

    async def RunAgent(
        self,
        request: mas_api_pb2.AgentRunRequest,
        context: grpc.aio.ServicerContext,
    ) -> mas_api_pb2.AgentRunReply:
        """Runs one agent and returns the structured verdict.

        Args:
            request: The wire request.
            context: The gRPC servicer context.

        Returns:
            An ``AgentRunReply`` carrying either a ``succeeded`` or a
            ``failed`` verdict.
        """
        started = time.monotonic()

        # --- steps 1-4: authenticate and reconcile the shared bridge context --
        _metadata, identity = await self.validate_call_context(request, context)

        # --- 5. build the execution context -----------------------------------
        execution_context = await self._build_context(request, identity, context)

        self._telemetry.run_started(execution_context)

        # --- 6. validate the request body -------------------------------------
        prompt = await self._prompt_from(request, context)
        try:
            runtime_snapshot = parse_agent_snapshot(
                request.agent_snapshot_json,
                expected_agent_id=request.agent_id,
                expected_tenant_id=execution_context.tenant_id,
                expected_organization_id=execution_context.organization_id,
                expected_project_id=execution_context.project_id or "",
                expected_agent_version_id=execution_context.agent_version_id or "",
            )
        except OrchestratorError as error:
            await abort_with_error(context, error)
            raise AssertionError("abort_with_error never returns") from None

        # --- 7, 8, 9. dispatch, bounded by the context, cancellable -----------
        # The clock for the runtime excludes the work done above: the operator
        # question "is the model slow, or is our own intake slow?" is answered
        # by the difference between the two.
        runtime_started = time.monotonic()
        cancel_event = asyncio.Event()
        disarm = self._arm_cancellation(context, cancel_event)
        try:
            result = await self._dispatch(
                prompt,
                request,
                execution_context,
                cancel_event,
                context,
                runtime_snapshot.spec,
            )
        except asyncio.CancelledError:
            self._telemetry.cancellation(execution_context)
            raise
        finally:
            disarm()

        # --- 11. structured result --------------------------------------------
        elapsed_ms = int((time.monotonic() - started) * 1000)
        grpc_overhead_ms = int((runtime_started - started) * 1000)
        self._telemetry.run_finished(
            execution_context,
            termination=result.termination,
            elapsed_ms=elapsed_ms,
            grpc_overhead_ms=grpc_overhead_ms,
        )
        if result.termination is Termination.DEADLINE:
            self._telemetry.timeout(execution_context)
        reply = self._reply_for(result, execution_context)

        # --- 12. safe telemetry -----------------------------------------------
        self._logger.info(
            "agent run completed",
            extra={
                "event": "bridge.run_completed",
                "correlation_id": execution_context.correlation_id,
                "execution_id": execution_context.execution_id,
                "tenant_id": execution_context.tenant_id,
                "termination": result.termination.value,
                "duration_ms": elapsed_ms,
                "succeeded": result.succeeded,
            },
        )
        return reply

    async def validate_call_context(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
        *,
        required_scope: ServiceScope | None = None,
    ) -> tuple[RustCallMetadata, ServiceIdentity]:
        """Authenticates a Rust-originated protobuf call using the AgentBridge boundary.

        Data Hub RPCs deliberately call this method instead of implementing
        their own metadata parsing, bearer-token validation, signature rules,
        identity resolver, or tenant-header reconciliation. The method
        validates the complete serialized request against the configured
        body-bound HMAC, resolves the same ``ServiceIdentity``, enforces the
        operation-specific service scope, and cross-checks the signed body
        tenant/organization against any metadata headers. Callers must then
        use tenant/project values from the signed request body, never headers.

        Args:
            request: A protobuf message with ``tenant_id``,
                ``organization_id`` and an optional ``identity.subject`` field.
                Its deterministic protobuf serialization is the HMAC input.
            context: The inbound gRPC context. Authentication or scope errors
                abort this RPC before the method returns.
            required_scope: Operation scope. When omitted, the configured
                AgentBridge scope is applied.

        Returns:
            Parsed call metadata and the authenticated identity resolved from
            the bearer credential. No tenant value is inferred from metadata.
        """
        metadata = extract_call_metadata(tuple(context.invocation_metadata() or ()))
        identity = await self._authenticate(
            metadata,
            context,
            request,
            required_scope=required_scope,
        )
        await self._reconcile_scope(request, metadata, context, identity)
        return metadata, identity

    async def Health(
        self,
        request: mas_api_pb2.Empty,
        context: grpc.aio.ServicerContext,
    ) -> mas_api_pb2.BridgeHealthReply:
        """Readiness of the AI runtime behind the bridge.

        Health is unauthenticated by design: a supervisor probing readiness has
        no execution to authorise, and the reply carries no tenant data — only
        a boolean and a static string.

        Args:
            request: Ignored.
            context: The gRPC servicer context. Ignored.

        Returns:
            A ``BridgeHealthReply``.
        """
        del request, context
        return mas_api_pb2.BridgeHealthReply(healthy=True, detail=HEALTHY_DETAIL)

    # --- pipeline stages -----------------------------------------------------

    # --- authentication ------------------------------------------------------

    async def _verify_internal_signature(
        self,
        metadata: RustCallMetadata,
        context: grpc.aio.ServicerContext,
        request: Any | None,
    ) -> None:
        """Verifies the caller's body-bound signature, or refuses the call.

        The v2 signature is ``v2:<service>:<unix_millis>:<nonce>:<hmac>``;
        its HMAC binds the service, timestamp, nonce, and SHA-256 body digest.
        A verified nonce is then atomically consumed before work begins. The
        body is re-serialized here from the message Python actually received,
        which is what makes the check meaningful: a signature over *a* request
        is not a signature over *this* request.

        Two rules decide whether the check runs:

        * A **configured secret makes the signature mandatory.** There is no
          "signature absent but the bearer token looked fine" path: once the
          process has been provisioned to expect signed calls, an unsigned
          call is a refusal.
        * With **no secret configured**, the check is skipped unless
          ``require_internal_signature`` is set. That is the development and
          unit-test posture, and it is deliberately the *only* way to get it —
          the composition root has to ask for strictness, it does not have to
          remember to switch it on.

        Args:
            metadata: Parsed inbound metadata.
            context: The servicer context, aborted on failure.
            request: The wire request whose bytes were signed.
        """
        secret = self._config.internal_service_secret

        if not secret:
            if self._config.require_internal_signature:
                await self._refuse_signature(context, "no internal service secret is configured")
            return

        # A secret is configured, so a signature is required. Passing on an
        # absent one would mean the deployment had two authentication
        # postures and the weaker one was reachable by simply not sending a
        # header.
        try:
            verified = verify_internal_signature(
                secret,
                metadata.internal_signature,
                metadata.internal_service,
                now_millis(),
                _canonical_request_bytes(request),
                self._config.allowed_internal_callers,
            )
            # Authentication is not complete until the signed nonce has been
            # atomically consumed in a store shared by replicas and restarts.
            # A missing/unavailable store fails closed rather than reopening the
            # one-minute replay window v2 is designed to close.
            if self._replay_guard is None or not await self._replay_guard.consume(verified):
                raise AuthenticationError(
                    "invalid internal service signature",
                    details={"reason": "unauthenticated"},
                )
        except (AuthenticationError, ConfigurationError):
            await self._refuse_signature(context, "invalid internal service signature")

        # The proven service id is recorded for the audit line. It is proof of
        # *which process* called, not of which tenant the work belongs to —
        # tenancy stays authoritative in the request body and is reconciled
        # separately.
        self._logger.debug(
            "internal call verified",
            extra={
                "event": "bridge.internal_verified",
                "internal_service": verified.service,
                "correlation_id": metadata.correlation_id,
            },
        )

    async def _refuse_signature(
        self,
        context: grpc.aio.ServicerContext,
        detail: str,
    ) -> None:
        """Refuses a call whose internal signature does not verify.

        Every failure produces the same
        :class:`AuthenticationError`, so a prober cannot learn whether the
        secret was missing, the service was unknown, the clock had drifted, or
        the MAC was wrong.

        Args:
            context: The servicer context, aborted on failure.
            detail: Operator-facing reason, recorded in telemetry only. Never
                returned to the caller: it is the difference between a log an
                operator can act on and an oracle a caller can probe.

        Raises:
            AssertionError: Always, because ``abort_with_error`` never
                returns. The raise exists so static analysis and a future
                reader both know control cannot fall through into the
                authenticated path.
        """
        refusal = AuthenticationError(
            "invalid internal service signature",
            details={"reason": "unauthenticated"},
        )
        self._telemetry.authorization_failure(None, error=refusal)
        self._logger.warning(
            "internal signature verification failed",
            extra={"event": "bridge.internal_refused", "detail": detail},
        )
        await abort_with_error(context, refusal)
        raise AssertionError("abort_with_error never returns") from None

    async def _authenticate(
        self,
        metadata: RustCallMetadata,
        context: grpc.aio.ServicerContext,
        request: object | None = None,
        *,
        required_scope: ServiceScope | None = None,
    ) -> ServiceIdentity:
        """Resolves the caller's credential and authorises it.

        Two independent proofs are required in production:

        1. **Proof of possession** — the bearer credential, resolved by the
           configured :class:`ServiceIdentityResolver`. This says which
           subject the caller claims.
        2. **Proof of this message** — the body-bound, time-bounded HMAC in
           ``x-mas-internal-signature``, verified against the exact bytes of
           the request. This says the holder of the shared secret actually
           sent *this* call, just now, and that nothing altered it in flight.

        The first alone is not enough: a static bearer token is a bearer
        credential, so any process that can read it can replay it, and a
        token intercepted on the wire is usable forever. The second alone is
        not enough either, because it identifies a service rather than a
        subject. Together they bind identity to a single message.

        Args:
            metadata: Parsed inbound metadata.
            context: The gRPC servicer context, aborted on failure.
            request: The wire request, when the caller's signature is to be
                verified over its bytes. ``None`` only for paths that carry
                no body (there are none today; the parameter exists so the
                verification is not silently skipped by a future caller).

        Returns:
            The authenticated service identity.
        """
        await self._verify_internal_signature(metadata, context, request)

        try:
            identity = await self._resolver.resolve(metadata.credential)
        except OrchestratorError as error:
            self._telemetry.authorization_failure(None, error=error)
            await abort_with_error(context, error)
            raise

        required = required_scope if required_scope is not None else self._config.require_scope
        if required is not None and not identity.has_scope(required):
            refusal = AuthorizationError(
                "caller is not permitted to execute agents",
                details={"subject": identity.subject},
            )
            self._telemetry.authorization_failure(None, error=refusal)
            await abort_with_error(context, refusal)
            raise AssertionError("abort_with_error never returns") from None

        return identity

    async def _reconcile_scope(
        self,
        request: Any,
        metadata: RustCallMetadata,
        context: grpc.aio.ServicerContext,
        identity: ServiceIdentity,
    ) -> None:
        """Cross-checks body tenancy, headers and the caller's declared identity.

        The body is authoritative because Rust fills it from its own
        ``RuntimeContext``. Headers are checked for *agreement*, so a spliced or
        misrouted call is caught instead of executing under two scopes at once.

        Args:
            request: The wire request.
            metadata: Parsed inbound metadata.
            context: The gRPC servicer context, aborted on mismatch.
            identity: The authenticated caller.
        """
        # An empty tenant is refused outright. Tenancy is never defaulted: an
        # unscoped run would execute outside every tenant's isolation boundary.
        if not request.tenant_id.strip():
            await abort_with_error(
                context,
                TenantContextError(
                    "agent run request is missing the authoritative tenant scope",
                    details={"field": "tenant_id"},
                ),
            )
            raise AssertionError("abort_with_error never returns") from None

        if not request.organization_id.strip():
            await abort_with_error(
                context,
                TenantContextError(
                    "agent run request is missing the authoritative organization scope",
                    details={"field": "organization_id"},
                ),
            )
            raise AssertionError("abort_with_error never returns") from None

        try:
            reconcile_tenant_scope(
                body_tenant_id=request.tenant_id,
                body_organization_id=request.organization_id,
                metadata=metadata,
            )
        except TenantContextError as error:
            # The most important security counter on this path: it is expected
            # to read zero, and a sustained non-zero value means something is
            # splicing headers rather than that a caller made a mistake.
            self._telemetry.tenant_mismatch(
                correlation_id=request.correlation_id,
                reason=str(error.details.get("reason", "tenant_scope")),
            )
            await abort_with_error(context, error)
            raise

        # The body may declare which service is calling. It must agree with the
        # credential the resolver authenticated: a mismatch means one side was
        # spoofed, and running anyway would attribute AI spend to the wrong
        # principal.
        declared = request.identity.subject.strip() if request.HasField("identity") else ""
        if declared and declared != identity.subject:
            await abort_with_error(
                context,
                AuthenticationError(
                    "declared service identity disagrees with the authenticated caller",
                    details={"reason": "subject_mismatch"},
                ),
            )
            raise AssertionError("abort_with_error never returns") from None

    async def _build_context(
        self,
        request: mas_api_pb2.AgentRunRequest,
        identity: ServiceIdentity,
        context: grpc.aio.ServicerContext,
    ) -> ExecutionContext:
        """Builds the authoritative execution context.

        Args:
            request: The wire request.
            identity: The authenticated caller.
            context: The gRPC servicer context, aborted on a bad request.

        Returns:
            A validated execution context.
        """
        try:
            deadline_epoch_ms = parse_deadline(request.deadline_rfc3339)
        except InvalidRequestError as error:
            await abort_with_error(context, error)
            raise

        try:
            return build_execution_context(
                tenant_id=request.tenant_id,
                organization_id=request.organization_id,
                execution_id=request.execution_id or request.correlation_id,
                agent_id=request.agent_id,
                correlation_id=request.correlation_id,
                request_id=request.request_id or request.correlation_id,
                caller=identity,
                protocol_version=request.protocol_version or self._config.protocol_version,
                project_id=request.project_id or None,
                task_id=request.task_id or None,
                task_attempt_id=request.task_attempt_id or None,
                agent_version_id=request.agent_version or None,
                deadline_epoch_ms=deadline_epoch_ms,
                attributes=self._attributes(request),
            )
        except OrchestratorError as error:
            await abort_with_error(context, error)
            raise

    async def _prompt_from(
        self,
        request: mas_api_pb2.AgentRunRequest,
        context: grpc.aio.ServicerContext,
    ) -> str:
        """Extracts and bounds the run input.

        Args:
            request: The wire request.
            context: The gRPC servicer context, aborted on an oversized payload.

        Returns:
            The decoded input text.
        """
        raw = request.input_json
        if not raw.strip():
            await abort_with_error(
                context,
                InvalidRequestError(
                    "agent run request carries no input",
                    details={"field": "input_json"},
                ),
            )
            raise AssertionError("abort_with_error never returns") from None

        size = len(raw.encode("utf-8"))
        if size > self._config.max_input_bytes:
            await abort_with_error(
                context,
                InvalidRequestError(
                    "agent run input exceeds the maximum accepted size",
                    details={"field": "input_json", "limit": self._config.max_input_bytes},
                ),
            )
            raise AssertionError("abort_with_error never returns") from None

        return raw

    async def _dispatch(
        self,
        prompt: str,
        request: mas_api_pb2.AgentRunRequest,
        execution_context: ExecutionContext,
        cancel_event: asyncio.Event,
        context: grpc.aio.ServicerContext,
        agent_spec: AgentSpec,
    ) -> ExecutionResult:
        """Runs the execution and maps failures onto the right channel.

        Args:
            prompt: The validated input.
            request: The wire request.
            execution_context: The authoritative context.
            cancel_event: Cancellation signal mirrored from the wire.
            context: The gRPC servicer context, aborted on an unmodelled fault.
            agent_spec: Rust-resolved immutable snapshot validated for this call.

        Returns:
            The execution result.
        """
        try:
            return await self._service.execute(
                ExecutionRequest(
                    request=prompt,
                    context=execution_context,
                    agent_id=request.agent_id or None,
                    agent_spec=agent_spec,
                    protocol_version=(request.protocol_version or self._config.protocol_version),
                ),
                cancel_event=cancel_event,
            )
        except OrchestratorError as error:
            # A typed failure from the runtime: the run was accepted and then
            # failed, so it becomes a verdict rather than a transport error.
            self._logger.warning(
                "agent run failed",
                extra={
                    "event": "bridge.run_failed",
                    "correlation_id": execution_context.correlation_id,
                    "execution_id": execution_context.execution_id,
                    "error_code": str(error.code),
                    "retryable": error.retryable,
                },
            )
            return ExecutionResult(
                execution_id=execution_context.execution_id,
                agent_id=execution_context.agent_id,
                termination=Termination.FAILED,
                error=error,
            )
        except asyncio.CancelledError:
            # A cancelled RPC cannot be answered — gRPC will not send a reply
            # on a cancelled call — so the only correct action is to stop the
            # runtime and let the cancellation stand. Turning it into a
            # verdict here would risk reporting a stopped run as finished.
            cancel_event.set()
            raise
        except Exception as error:
            # Unmodelled: reporting it as a business failure would hide a bug
            # behind a retryable classification the runtime never made.
            self._logger.exception(
                "agent run raised an unmodelled error",
                extra={
                    "event": "bridge.run_error",
                    "correlation_id": execution_context.correlation_id,
                    "execution_id": execution_context.execution_id,
                },
            )
            await abort_with_error(context, OrchestratorError(type(error).__name__))
            raise AssertionError("abort_with_error never returns") from None

    def _arm_cancellation(
        self,
        context: grpc.aio.ServicerContext,
        cancel_event: asyncio.Event,
    ) -> Callable[[], None]:
        """Mirrors a cancelled RPC onto the runtime's cancel event.

        Rust cancelling the RPC must stop the Python work — otherwise the
        model keeps generating and the provider keeps billing for an answer
        nobody will read.

        How the cancellation arrives matters, and it is not the obvious thing.
        ``grpc.aio`` does **not** flip :meth:`ServicerContext.cancelled` when
        the client cancels: it cancels the servicer coroutine, so the
        cancellation surfaces as a ``CancelledError`` raised at the servicer's
        next await. A watcher that polled ``context.cancelled()`` would never
        fire, and the run would continue to completion after Rust had given
        up — which is both a billing bug and a correctness bug.

        So both channels are observed:

        * a **done-callback on the current task**, which fires however the
          cancellation was delivered; and
        * :meth:`cancelled` itself, polled cheaply, for the paths and gRPC
          versions that do set it (for example a server-side deadline).

        Args:
            context: The gRPC servicer context.
            cancel_event: The event the Agent Runtime observes.

        Returns:
            A callable that removes the callback. Callers must invoke it in a
            ``finally``: a callback left attached would fire on a later,
            unrelated completion of the same task object.
        """
        task = asyncio.current_task()
        if task is None:
            # No current task means no cancellation can be delivered here.
            return lambda: None

        def on_done(finished: asyncio.Task[object]) -> None:
            if finished.cancelled():
                cancel_event.set()
                self._logger.info(
                    "cancellation observed from the wire",
                    extra={"event": "bridge.cancelled", "source": "task"},
                )

        task.add_done_callback(on_done)

        watcher = asyncio.ensure_future(self._poll_wire_cancellation(context, cancel_event))

        def disarm() -> None:
            task.remove_done_callback(on_done)
            watcher.cancel()

        return disarm

    async def _poll_wire_cancellation(
        self,
        context: grpc.aio.ServicerContext,
        cancel_event: asyncio.Event,
    ) -> None:
        """Polls :meth:`ServicerContext.cancelled` as a secondary channel.

        Args:
            context: The gRPC servicer context.
            cancel_event: The event the Agent Runtime observes.

        Kept as a secondary channel only. It cannot be the primary one,
        because ``grpc.aio`` delivers a client cancellation by cancelling the
        servicer coroutine rather than by setting this flag.
        """
        try:
            while not cancel_event.is_set():
                if context.cancelled():
                    cancel_event.set()
                    self._logger.info(
                        "cancellation observed from the wire",
                        extra={"event": "bridge.cancelled", "source": "context"},
                    )
                    return
                await asyncio.sleep(CANCELLATION_POLL_SECONDS)
        except asyncio.CancelledError:
            # Cancelled when the run finishes normally: expected, not an error.
            return

    # --- reply construction ---------------------------------------------------

    def _reply_for(
        self,
        result: ExecutionResult,
        context: ExecutionContext,
    ) -> mas_api_pb2.AgentRunReply:
        """Turns an execution result into the wire verdict.

        Args:
            result: The execution result.
            context: The execution context, for correlation identity.

        Returns:
            An ``AgentRunReply``.
        """
        if result.succeeded:
            return mas_api_pb2.AgentRunReply(
                succeeded=mas_api_pb2.AgentSucceeded(
                    output_json=self._encode_output(result.content),
                    usage=usage_from_result(result),
                    finish_reason=finish_reason_for(result.termination),
                    model_key=model_key_from_result(result),
                )
            )

        error = result.error
        return mas_api_pb2.AgentRunReply(
            failed=mas_api_pb2.AgentFailed(
                code=self._failure_code(result, error),
                message=self._failure_message(result, error),
                retryable=self._failure_retryable(error),
                usage=usage_from_result(result),
                correlation_id=context.correlation_id,
                request_id=context.request_id,
            )
        )

    @staticmethod
    def _encode_output(content: str) -> str:
        """Encodes the final answer as the wire ``output_json`` document.

        ``output_json`` names a JSON document, so a bare answer is re-encoded
        as a JSON string: a consumer that parses it gets a string rather than
        a parse error on prose.

        Args:
            content: The answer text.

        Returns:
            A JSON document.
        """
        return json.dumps(content, ensure_ascii=False)

    @staticmethod
    def _failure_code(result: ExecutionResult, error: BaseException | None) -> str:
        """Derives the machine-readable failure code.

        Args:
            result: The execution result.
            error: The error that stopped it, when there is one.

        Returns:
            The typed error code when one exists, otherwise the termination.
            Both are stable strings Rust can branch on.
        """
        if error is not None and isinstance(error, OrchestratorError):
            return str(error.code)
        return result.termination.value

    @staticmethod
    def _failure_message(result: ExecutionResult, error: BaseException | None) -> str:
        """Derives a safe human-readable failure message.

        Args:
            result: The execution result.
            error: The error that stopped it, when there is one.

        Returns:
            A redacted message. Never includes a credential, a raw provider
            payload, or the prompt that failed.

        The message is scrubbed here rather than trusted, in two passes:

        * :func:`redact_text` removes credential-shaped values — keys, bearer
          tokens, PEM blocks — that leaked into prose.
        * The PII scanner with :class:`RedactAll` removes detected personal
          data. A failure message leaving the process is not a place to echo
          a prompt that happened to contain a card number, and the default
          policy deliberately allows personal data through because a user may
          have asked to see it in their own answer. That reasoning does not
          apply to an error report the user never asked for.

        Scrubbing is done here rather than assumed done upstream. A failure is
        built at the exact moment something went wrong — often while a
        credential is in scope — so an error raised deep in a provider adapter
        can carry the key it was using into its own text. Depending on every
        call site to be careful is not a property that survives an incident.
        """
        from mas_orchestrator.guardrails.pii import RedactAll, redact
        from mas_orchestrator.security.redaction import redact_text

        if error is not None and isinstance(error, OrchestratorError):
            raw = error.message
        else:
            raw = f"agent run did not complete: {result.termination.value}"

        scrubbed = redact_text(raw)
        # A PII policy that blocks cannot be honoured here: refusing to report
        # a failure would be worse than reporting a redacted one, and
        # `redact_text` has already removed credential-shaped values.
        with contextlib.suppress(Exception):
            scrubbed, _ = redact(scrubbed, policy=RedactAll())
        return scrubbed

    @staticmethod
    def _failure_retryable(error: BaseException | None) -> bool:
        """Whether the caller may retry this failure.

        Args:
            error: The error that stopped the run, when there is one.

        Returns:
            The typed retryable flag. A run stopped by a *bound* (deadline,
            step limit, cancellation) is terminal: re-running it unchanged
            would hit the same bound, so it is never reported as retryable.
        """
        if isinstance(error, OrchestratorError):
            return error.retryable
        return False

    @staticmethod
    def _attributes(request: mas_api_pb2.AgentRunRequest) -> dict[str, object]:
        """Non-authorization metadata carried for telemetry.

        Args:
            request: The wire request.

        Returns:
            Safe, non-identity attributes. The idempotency key is included so a
            duplicate can be traced; it never decides anything.
        """
        attributes: dict[str, object] = {}
        if request.idempotency_key:
            attributes["idempotency_key"] = request.idempotency_key
        if request.token_budget_max:
            attributes["token_budget_max"] = request.token_budget_max
        return attributes
