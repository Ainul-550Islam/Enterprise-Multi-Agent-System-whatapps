"""The canonical execution service.

This is the single entry point for running an execution, and the place where
the spec §6 flow becomes code:

    validate protocol version
    → validate request identity/context
    → resolve agent/version
    → apply input guardrails
    → plan
    → model call
    → tool calls
    → memory/RAG
    → output validation
    → usage/cost metadata
    → publish result/event
    → Rust canonical state transition

Every stage emits a typed event, so an execution can be reconstructed from
telemetry without reproducing it. The service also enforces the idempotency
rule from §16: it asks the authoritative Rust state what to do with a duplicate
delivery rather than deciding locally.

The service owns orchestration only. It does not own execution state — Rust
does — and it reports the terminal outcome to Rust rather than recording it
itself.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from mas_orchestrator.agents.base import AgentState, Termination
from mas_orchestrator.agents.runtime import AgentRuntime, ExecutionResult
from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    ProtocolError,
)
from mas_orchestrator.contracts.protocol_version import (
    CURRENT_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    negotiate_protocol_version,
)
from mas_orchestrator.events.emitter import EventEmitter
from mas_orchestrator.events.types import (
    cancellation,
    final_result,
    model_cost,
    model_tokens,
    request_received,
)
from mas_orchestrator.services.idempotency import (
    DuplicateAction,
    ExecutionStateReader,
    UnknownStateReader,
    evaluate,
)

if TYPE_CHECKING:
    from mas_orchestrator.agents.base import AgentSpec
    from mas_orchestrator.agents.router import AgentRoutingRequest
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.guardrails.manager import GuardrailManager

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@runtime_checkable
class StateTransitionReporter(Protocol):
    """Reports a terminal outcome back to Rust.

    Rust owns the canonical state transition; Python only reports what
    happened. A reporter that cannot deliver must raise, so the mismatch is
    visible rather than silent.
    """

    async def report(
        self,
        execution_id: str,
        *,
        termination: str,
        duration_ms: int,
        error_code: str | None = None,
    ) -> None:
        """Reports the outcome of an execution."""
        ...


class LoggingStateReporter:
    """A reporter that only logs.

    For development. It is explicit that nothing was reported to Rust, so a
    deployment running with this reporter cannot mistake itself for one that
    is integrated.
    """

    async def report(
        self,
        execution_id: str,
        *,
        termination: str,
        duration_ms: int,
        error_code: str | None = None,
    ) -> None:
        """Logs rather than reporting."""
        _LOGGER.info(
            "state transition (not reported to Rust)",
            extra={
                "event": "execution.state_transition_local",
                "execution_id": execution_id,
                "termination": termination,
                "duration_ms": duration_ms,
                "error_code": error_code,
            },
        )


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    """What the caller asked for.

    Attributes:
        request: The user's request text.
        context: Authoritative execution context.
        routing: Optional agent selection constraints.
        agent_id: Run a specific agent rather than selecting one.
        agent_spec: Optional immutable Rust-resolved definition for this run.
        state: Optional starting state.
        protocol_version: The protocol version claimed by the caller.
    """

    request: str
    context: ExecutionContext
    routing: AgentRoutingRequest | None = None
    agent_id: str | None = None
    agent_spec: AgentSpec | None = None
    state: AgentState | None = None
    protocol_version: str = str(CURRENT_PROTOCOL_VERSION)

    def __post_init__(self) -> None:
        if not self.request.strip():
            raise InvalidRequestError("request must not be empty", details={"field": "request"})
        if self.agent_spec is not None:
            selected_agent_id = self.agent_id or self.context.agent_id
            if self.agent_spec.agent_id != selected_agent_id:
                raise InvalidRequestError(
                    "Rust-resolved agent snapshot does not match the requested agent",
                    details={"field": "agent_id"},
                )
            if self.agent_spec.tenant_id != self.context.tenant_id:
                raise InvalidRequestError(
                    "Rust-resolved agent snapshot does not match the execution tenant",
                    details={"field": "tenant_id"},
                )
            if (
                self.context.agent_version_id is not None
                and self.agent_spec.agent_version_id != self.context.agent_version_id
            ):
                raise InvalidRequestError(
                    "Rust-resolved agent snapshot does not match the pinned version",
                    details={"field": "agent_version_id"},
                )
            if not self.agent_spec.is_runnable:
                raise InvalidRequestError(
                    "Rust-resolved agent snapshot is not runnable",
                    details={"field": "agent_spec.status"},
                )


class ExecutionService:
    """The canonical way to run an execution."""

    def __init__(
        self,
        runtime: AgentRuntime,
        *,
        guardrails: GuardrailManager | None = None,
        emitter: EventEmitter | None = None,
        state_reader: ExecutionStateReader | None = None,
        reporter: StateTransitionReporter | None = None,
    ) -> None:
        self._runtime = runtime
        self._guardrails = guardrails
        self._emitter = emitter or EventEmitter()
        self._state_reader = state_reader or UnknownStateReader()
        self._reporter = reporter or LoggingStateReporter()

    async def execute(
        self,
        request: ExecutionRequest,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> ExecutionResult:
        """Runs one execution.

        Args:
            request: The validated request.
            cancel_event: Optional cancellation signal.

        Returns:
            An :class:`ExecutionResult`.

        Raises:
            ProtocolError: If the protocol version is unsupported.
            InvalidRequestError: If mandatory identity fields are missing.
        """
        started = time.monotonic()
        context = request.context

        # --- stage 1: identity and protocol ---
        self._validate_protocol(request)
        self._validate_identity(context)

        await self._emit(
            request_received(
                tenant_id=context.tenant_id,
                execution_id=context.execution_id,
                correlation_id=context.correlation_id,
                agent_id=context.agent_id,
                protocol_version=request.protocol_version,
            )
        )

        # --- stage 2: idempotency, decided by authoritative state ---
        cancel_event = cancel_event or asyncio.Event()
        decision = await evaluate(context, self._state_reader)
        if decision.action is not DuplicateAction.PROCEED:
            duration_ms = int((time.monotonic() - started) * 1000)
            result = ExecutionResult(
                execution_id=context.execution_id,
                agent_id=context.agent_id,
                termination=Termination.COMPLETED,
                content="",
                duration_ms=duration_ms,
            )
            await self._emit(
                final_result(
                    tenant_id=context.tenant_id,
                    execution_id=context.execution_id,
                    correlation_id=context.correlation_id,
                    agent_id=context.agent_id,
                    termination=f"skipped:{decision.action.value}",
                    duration_ms=duration_ms,
                )
            )
            _LOGGER.info(
                "execution skipped as a duplicate",
                extra={
                    "event": "execution.skipped",
                    "execution_id": context.execution_id,
                    "action": decision.action.value,
                },
            )
            return result

        # --- stage 3: input guardrails ---
        guarded = request.request
        if self._guardrails is not None:
            before = await self._guardrails.before_model(request.request, context)
            guarded = before.text

        # --- stage 4: run the agent ---
        try:
            result = await self._runtime.execute(
                guarded,
                context,
                routing=request.routing,
                state=request.state,
                cancel_event=cancel_event,
                agent_id=request.agent_id,
                agent_spec=request.agent_spec,
            )
        except asyncio.CancelledError as error:
            result = ExecutionResult(
                execution_id=context.execution_id,
                agent_id=context.agent_id,
                termination=Termination.CANCELLED,
                error=error,
                duration_ms=int((time.monotonic() - started) * 1000),
            )
            await self._emit(
                cancellation(
                    tenant_id=context.tenant_id,
                    execution_id=context.execution_id,
                    correlation_id=context.correlation_id,
                    reason="execution was cancelled",
                )
            )

        # --- stage 5: output guardrails ---
        if self._guardrails is not None and result.succeeded and result.content:
            result = await self._apply_output_guardrails(result, context)

        # --- stage 6: usage and cost metadata ---
        await self._emit_usage(context, result)

        # --- stage 7: publish the result ---
        duration_ms = int((time.monotonic() - started) * 1000)
        await self._emit(
            final_result(
                tenant_id=context.tenant_id,
                execution_id=context.execution_id,
                correlation_id=context.correlation_id,
                agent_id=result.agent_id,
                termination=result.termination.value,
                duration_ms=duration_ms,
            )
        )

        # --- stage 8: report the canonical state transition to Rust ---
        await self._report(context.execution_id, result, duration_ms)

        return result

    # --- internals ---------------------------------------------------------

    def _validate_protocol(self, request: ExecutionRequest) -> None:
        """Rejects an unsupported protocol version."""
        # negotiate_protocol_version rejects an absent or unparseable version
        # rather than defaulting it, which is the whole point: a caller that
        # does not declare its contract cannot be assumed compatible.
        version = negotiate_protocol_version(request.protocol_version)
        if version not in SUPPORTED_PROTOCOL_VERSIONS:
            raise ProtocolError(
                f"unsupported protocol version: {version}",
                details={"supported": [str(v) for v in SUPPORTED_PROTOCOL_VERSIONS]},
            )

    def _validate_identity(self, context: ExecutionContext) -> None:
        """Rejects a request missing mandatory identity fields.

        Spec §6 requires these and forbids deriving tenancy from arbitrary
        headers, so a missing field is refused rather than defaulted.
        """
        required = {
            "tenant_id": context.tenant_id,
            "organization_id": context.organization_id,
            "execution_id": context.execution_id,
            "agent_id": context.agent_id,
            "correlation_id": context.correlation_id,
            "request_id": context.request_id,
        }
        missing = [name for name, value in required.items() if not str(value or "").strip()]
        if missing:
            raise InvalidRequestError(
                "request is missing mandatory identity fields",
                details={"missing": sorted(missing)},
            )
        if context.caller is None:
            raise InvalidRequestError(
                "request must carry an authenticated caller identity",
                details={"field": "caller"},
            )

    async def _apply_output_guardrails(
        self,
        result: ExecutionResult,
        context: ExecutionContext,
    ) -> ExecutionResult:
        """Runs output guardrails, converting a failure into a failed result.

        A guardrail failure must not become a successful execution (spec §13),
        so the termination is rewritten to ``FAILED`` and the original success
        is not reported.
        """
        from dataclasses import replace

        guardrails = self._guardrails
        if guardrails is None:
            return result

        try:
            validated = await guardrails.after_model(result.content, context)
        except Exception as error:
            _LOGGER.warning(
                "output guardrail rejected the result",
                extra={
                    "event": "execution.output_rejected",
                    "execution_id": context.execution_id,
                    "error": type(error).__name__,
                },
            )
            return replace(
                result,
                termination=Termination.FAILED,
                content="",
                error=error,
            )
        return replace(result, content=validated)

    async def _emit_usage(
        self,
        context: ExecutionContext,
        result: ExecutionResult,
    ) -> None:
        """Emits cost and token events when the ledger knows them.

        Nothing is emitted for an absent ledger: reporting a cost of zero for
        an unmeasured execution would be a measurement that was never taken.
        """
        totals = result.cost
        if totals is None:
            return

        await self._emit(
            model_tokens(
                tenant_id=context.tenant_id,
                execution_id=context.execution_id,
                correlation_id=context.correlation_id,
                agent_id=result.agent_id,
                model="aggregate",
                input_tokens=totals.input_tokens,
                output_tokens=totals.output_tokens,
            )
        )
        await self._emit(
            model_cost(
                tenant_id=context.tenant_id,
                execution_id=context.execution_id,
                correlation_id=context.correlation_id,
                agent_id=result.agent_id,
                model="aggregate",
                currency=totals.currency,
                total_cost=totals.total_cost,
                is_complete=totals.is_complete,
            )
        )

    async def _report(
        self,
        execution_id: str,
        result: ExecutionResult,
        duration_ms: int,
    ) -> None:
        """Reports the terminal state to Rust."""
        error_code = None
        if result.error is not None:
            error_code = getattr(getattr(result.error, "code", None), "value", None)
        await self._reporter.report(
            execution_id,
            termination=result.termination.value,
            duration_ms=duration_ms,
            error_code=error_code,
        )

    async def _emit(self, event: Any) -> None:
        """Emits one event, never letting telemetry fail an execution."""
        try:
            await self._emitter.emit(event)
        except InvalidRequestError:
            raise
        except Exception:
            _LOGGER.warning("event emission failed", extra={"event": "execution.emit_failed"})


def build_execution_service(
    runtime: AgentRuntime,
    *,
    guardrails: GuardrailManager | None = None,
    emitter: EventEmitter | None = None,
    state_reader: ExecutionStateReader | None = None,
    reporter: StateTransitionReporter | None = None,
) -> ExecutionService:
    """Constructs a service.

    Kept separate from the constructor so the wiring order is visible in one
    place and a caller cannot half-build it.
    """
    if runtime is None:
        raise ConfigurationError(
            "an execution service requires a runtime", details={"field": "runtime"}
        )
    return ExecutionService(
        runtime,
        guardrails=guardrails,
        emitter=emitter,
        state_reader=state_reader,
        reporter=reporter,
    )
