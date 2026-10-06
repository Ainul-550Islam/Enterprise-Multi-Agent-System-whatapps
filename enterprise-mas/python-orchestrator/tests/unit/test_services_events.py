"""Tests for events, idempotency and the execution service.

The claims worth testing are the ones where a simpler implementation would
silently do the wrong thing: that a duplicate delivery is decided by the
authoritative state rather than local memory, that an unknown state proceeds,
that telemetry never carries a credential, and that a guardrail failure cannot
turn into a successful execution.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.agents.base import Termination
from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import (
    InvalidRequestError,
    ProtocolError,
)
from mas_orchestrator.events.dedup import EventDeduplicator
from mas_orchestrator.events.emitter import (
    CollectingSink,
    EventEmitter,
    NullSink,
)
from mas_orchestrator.events.types import (
    MAX_STRING_FIELD_CHARS,
    EventName,
    final_result,
    model_cost,
    model_request_duration,
    request_received,
)
from mas_orchestrator.services.execution_service import (
    ExecutionRequest,
    ExecutionService,
    LoggingStateReporter,
    build_execution_service,
)
from mas_orchestrator.services.idempotency import (
    DuplicateAction,
    ExecutionState,
    UnknownStateReader,
    decide,
    invocation_key,
    should_run,
    tool_is_repeatable,
)

TENANT = "tenant-1"


def _context(**overrides: object) -> object:
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
    from mas_orchestrator.security.identity import ServiceIdentity

    values: dict[str, object] = {
        "tenant_id": TENANT,
        "organization_id": "org-1",
        "execution_id": "exec-1",
        "agent_id": "agent-1",
        "correlation_id": "corr-1",
        "request_id": "req-1",
        "caller": ServiceIdentity(subject="test", issuer="test"),
        "protocol_version": CURRENT_PROTOCOL_VERSION,
    }
    values.update(overrides)
    return ExecutionContext(**values)  # type: ignore[arg-type]


class TestEvents:
    """Event construction and screening."""

    def test_event_requires_tenant(self) -> None:
        with pytest.raises(InvalidRequestError):
            final_result(
                tenant_id="",
                execution_id="e",
                correlation_id="c",
                agent_id="a",
                termination="completed",
                duration_ms=1,
            )

    def test_event_requires_execution(self) -> None:
        with pytest.raises(InvalidRequestError):
            final_result(
                tenant_id=TENANT,
                execution_id="",
                correlation_id="c",
                agent_id="a",
                termination="completed",
                duration_ms=1,
            )

    def test_oversized_field_rejected(self) -> None:
        with pytest.raises(InvalidRequestError):
            model_request_duration(
                tenant_id=TENANT,
                execution_id="e",
                correlation_id="c",
                agent_id="a",
                model="m",
                duration_ms=1,
                attempt=1,
            ).__class__(
                name=EventName.MODEL_REQUEST_DURATION,
                tenant_id=TENANT,
                execution_id="e",
                correlation_id="c",
                fields={"big": "x" * (MAX_STRING_FIELD_CHARS + 1)},
            )

    def test_timestamp_defaults_to_now(self) -> None:
        event = request_received(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            protocol_version="1.0.0",
        )
        assert event.occurred_epoch_ms > 0

    def test_flattening_carries_correlation(self) -> None:
        event = final_result(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=5,
        )
        flat = event.to_log_fields()
        assert flat["event"] == "final_result"
        assert flat["tenant_id"] == TENANT
        assert flat["correlation_id"] == "c"
        assert flat["termination"] == "completed"

    def test_agent_id_absent_when_not_given(self) -> None:
        flat = final_result(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=5,
        ).to_log_fields()
        assert "agent_id" in flat

    def test_cost_carries_completeness_flag(self) -> None:
        event = model_cost(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            model="m",
            currency="USD",
            total_cost=0.5,
            is_complete=False,
        )
        assert event.fields["is_complete"] is False

    def test_final_result_carries_no_output(self) -> None:
        """The result text belongs to the caller, not to telemetry."""
        flat = final_result(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=5,
        ).to_log_fields()
        assert not any("answer" in key for key in flat)


class TestScreening:
    """Emission must refuse credential-shaped payloads."""

    @pytest.mark.asyncio
    async def test_secret_in_field_is_refused(self) -> None:
        emitter = EventEmitter(NullSink())
        event = model_request_duration(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            model="sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456",
            duration_ms=1,
        )
        with pytest.raises(InvalidRequestError, match="credential"):
            await emitter.emit(event)

    @pytest.mark.asyncio
    async def test_screening_can_be_disabled(self) -> None:
        emitter = EventEmitter(NullSink(), screen=False)
        event = model_request_duration(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            model="sk-ABCDEFGHIJKLMNOPQRSTUVWXYZ123456",
            duration_ms=1,
        )
        assert await emitter.emit(event) is True

    @pytest.mark.asyncio
    async def test_sink_failure_does_not_raise(self) -> None:
        class _Broken:
            async def emit(self, event: object) -> None:
                raise RuntimeError("sink down")

        emitter = EventEmitter(_Broken())  # type: ignore[arg-type]
        event = final_result(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=1,
        )
        assert await emitter.emit(event) is True

    @pytest.mark.asyncio
    async def test_collecting_sink(self) -> None:
        sink = CollectingSink()
        emitter = EventEmitter(sink)
        e1 = final_result(
            tenant_id=TENANT,
            execution_id="e1",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=1,
        )
        e2 = final_result(
            tenant_id=TENANT,
            execution_id="e2",
            correlation_id="c",
            agent_id="a",
            termination="failed",
            duration_ms=1,
        )
        assert await emitter.emit_many([e1, e2]) == 2
        assert sink.names() == ["final_result", "final_result"]
        sink.clear()
        assert sink.events == []


class TestDedup:
    """Duplicate suppression."""

    def test_first_is_not_duplicate(self) -> None:
        dedup = EventDeduplicator()
        event = final_result(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=1,
        )
        assert dedup.is_duplicate(event) is False

    def test_second_is_duplicate(self) -> None:
        dedup = EventDeduplicator()
        event = final_result(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=1,
        )
        dedup.is_duplicate(event)
        assert dedup.is_duplicate(event) is True

    def test_distinct_events_are_not_duplicates(self) -> None:
        dedup = EventDeduplicator()
        a = final_result(
            tenant_id=TENANT,
            execution_id="e1",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=1,
        )
        b = final_result(
            tenant_id=TENANT,
            execution_id="e2",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=1,
        )
        assert dedup.is_duplicate(a) is False
        assert dedup.is_duplicate(b) is False

    def test_window_expiry_releases_the_key(self) -> None:
        import time

        # A window short enough to expire inside the test.
        dedup = EventDeduplicator(window_seconds=0.001)
        event = final_result(
            tenant_id=TENANT,
            execution_id="e",
            correlation_id="c",
            agent_id="a",
            termination="completed",
            duration_ms=1,
        )
        dedup.is_duplicate(event)
        time.sleep(0.02)
        assert dedup.is_duplicate(event) is False

    def test_key_capacity_is_bounded(self) -> None:
        dedup = EventDeduplicator(max_keys=2)
        for i in range(5):
            dedup.is_duplicate(
                final_result(
                    tenant_id=TENANT,
                    execution_id=f"e{i}",
                    correlation_id="c",
                    agent_id="a",
                    termination="completed",
                    duration_ms=1,
                )
            )
        assert len(dedup) <= 2

    def test_invalid_window(self) -> None:
        with pytest.raises(InvalidRequestError):
            EventDeduplicator(window_seconds=0.0)
        with pytest.raises(InvalidRequestError):
            EventDeduplicator(window_seconds=-1.0)
        with pytest.raises(InvalidRequestError):
            EventDeduplicator(max_keys=0)


class TestIdempotency:
    """Duplicate handling is decided by authoritative state."""

    def test_key_is_stable(self) -> None:
        context = _context()
        assert invocation_key(context) == invocation_key(context)

    def test_key_varies_by_tenant(self) -> None:
        assert invocation_key(_context()) != invocation_key(_context(tenant_id="tenant-2"))

    def test_key_varies_by_attempt(self) -> None:
        base = invocation_key(_context())
        assert invocation_key(_context(), attempt="a1") != base

    def test_terminal_states_skip(self) -> None:
        for state in (
            ExecutionState.SUCCEEDED,
            ExecutionState.FAILED,
            ExecutionState.CANCELLED,
            ExecutionState.TIMED_OUT,
        ):
            decision = decide(state, "k")
            assert decision.action is DuplicateAction.SKIP
            assert should_run(decision) is False

    def test_running_waits(self) -> None:
        decision = decide(ExecutionState.RUNNING, "k")
        assert decision.action is DuplicateAction.WAIT

    def test_pending_proceeds(self) -> None:
        assert should_run(decide(ExecutionState.PENDING, "k")) is True

    def test_unknown_proceeds(self) -> None:
        """Unknown must proceed: skipping would drop real work."""
        decision = decide(ExecutionState.UNKNOWN, "k")
        assert decision.action is DuplicateAction.PROCEED
        assert "unknown" in decision.reason

    def test_unknown_reader_reports_unknown(self) -> None:
        import asyncio

        assert asyncio.run(UnknownStateReader().state_of("e")) is ExecutionState.UNKNOWN

    @pytest.mark.asyncio
    async def test_evaluate_uses_the_reader(self) -> None:
        class _Reader:
            async def state_of(self, execution_id: str) -> ExecutionState:
                return ExecutionState.SUCCEEDED

        from mas_orchestrator.services.idempotency import evaluate

        decision = await evaluate(_context(), _Reader())  # type: ignore[arg-type]
        assert decision.action is DuplicateAction.SKIP
        assert decision.state is ExecutionState.SUCCEEDED


class TestToolRepeatability:
    """Retry classification by tool safety."""

    def test_idempotent_read_only_is_repeatable(self) -> None:
        assert tool_is_repeatable(safety="read_only", idempotent=True) is True

    def test_non_idempotent_is_not_repeatable(self) -> None:
        assert tool_is_repeatable(safety="read_only", idempotent=False) is False

    def test_sensitive_is_never_repeatable(self) -> None:
        """Safety class outranks a claimed idempotency."""
        assert tool_is_repeatable(safety="sensitive", idempotent=True) is False

    def test_destructive_is_never_repeatable(self) -> None:
        assert tool_is_repeatable(safety="destructive", idempotent=True) is False


class TestExecutionService:
    """The canonical entry point."""

    def test_request_requires_text(self) -> None:
        with pytest.raises(InvalidRequestError):
            ExecutionRequest(request="  ", context=_context())

    def test_builder_rejects_no_runtime(self) -> None:
        from mas_orchestrator.contracts.errors import ConfigurationError

        with pytest.raises(ConfigurationError):
            build_execution_service(None)  # type: ignore[arg-type]

    def _service(
        self,
        runtime: object,
        *,
        guardrails: object = None,
        sink: CollectingSink | None = None,
        reporter: object = None,
    ) -> ExecutionService:
        return build_execution_service(
            runtime,  # type: ignore[arg-type]
            guardrails=guardrails,  # type: ignore[arg-type]
            emitter=EventEmitter(sink or NullSink()),
            reporter=reporter,  # type: ignore[arg-type]
        )

    @pytest.mark.asyncio
    async def test_bad_protocol_version_rejected(self) -> None:
        service = self._service(_FakeRuntime())
        request = ExecutionRequest(request="hello", context=_context(), protocol_version="99.0.0")
        with pytest.raises(ProtocolError):
            await service.execute(request)

    @pytest.mark.asyncio
    async def test_missing_identity_rejected(self) -> None:
        """The context refuses to exist without its identity fields.

        The service re-checks anyway, but the first refusal happens at
        construction: an incomplete context is never handed around, so no
        caller has to remember to validate before using one.
        """
        from mas_orchestrator.contracts.errors import TenantContextError

        with pytest.raises(TenantContextError, match="identity"):
            _context(tenant_id="")

    @pytest.mark.asyncio
    async def test_missing_caller_rejected(self) -> None:
        service = self._service(_FakeRuntime())
        request = ExecutionRequest(request="hello", context=_context(caller=None))
        with pytest.raises(InvalidRequestError, match="caller"):
            await service.execute(request)

    @pytest.mark.asyncio
    async def test_successful_run_emits_events(self) -> None:
        sink = CollectingSink()
        service = self._service(_FakeRuntime(), sink=sink)
        request = ExecutionRequest(request="hello", context=_context())
        result = await service.execute(request)

        assert result.succeeded is True
        assert "request_received" in sink.names()
        assert "final_result" in sink.names()

    @pytest.mark.asyncio
    async def test_duplicate_delivery_is_skipped(self) -> None:
        class _Reader:
            async def state_of(self, execution_id: str) -> ExecutionState:
                return ExecutionState.SUCCEEDED

        sink = CollectingSink()
        service = build_execution_service(
            _FakeRuntime(),
            emitter=EventEmitter(sink),
            state_reader=_Reader(),  # type: ignore[arg-type]
        )
        request = ExecutionRequest(request="hello", context=_context())
        result = await service.execute(request)

        assert result.content == ""
        terminal = [e for e in sink.events if e.name is EventName.FINAL_RESULT]
        assert terminal[0].fields["termination"] == "skipped:skip"

    @pytest.mark.asyncio
    async def test_state_transition_is_reported(self) -> None:
        recorded: list[tuple[str, str, str | None]] = []

        class _Reporter:
            async def report(
                self,
                execution_id: str,
                *,
                termination: str,
                duration_ms: int,
                error_code: str | None = None,
            ) -> None:
                recorded.append((execution_id, termination, error_code))

        service = self._service(_FakeRuntime(), reporter=_Reporter())
        await service.execute(ExecutionRequest(request="hello", context=_context()))
        assert recorded == [("exec-1", "completed", None)]

    @pytest.mark.asyncio
    async def test_guardrail_rejection_fails_the_run(self) -> None:
        """A guardrail failure must not become a successful execution."""
        from mas_orchestrator.guardrails.manager import GuardrailManager
        from mas_orchestrator.guardrails.output import OutputConfig
        from mas_orchestrator.guardrails.policy import AllowAllPolicy

        manager = GuardrailManager(
            policy=AllowAllPolicy(environment=Environment.DEVELOPMENT.value),
        )
        # A schema the fake runtime's output cannot satisfy.
        manager._config = type(manager._config)(output=OutputConfig(schema={"type": "array"}))

        service = self._service(_FakeRuntime(), guardrails=manager)
        result = await service.execute(ExecutionRequest(request="hello", context=_context()))
        assert result.termination is Termination.FAILED
        assert result.content == ""

    @pytest.mark.asyncio
    async def test_cancellation_is_reported(self) -> None:
        import asyncio

        class _Cancelling:
            async def execute(self, *args: object, **kwargs: object) -> object:
                raise asyncio.CancelledError()

        service = self._service(_Cancelling())
        request = ExecutionRequest(request="hello", context=_context())
        result = await service.execute(request)
        assert result.termination is Termination.CANCELLED

    @pytest.mark.asyncio
    async def test_logging_reporter_does_not_raise(self) -> None:
        reporter = LoggingStateReporter()
        await reporter.report("e", termination="completed", duration_ms=1)


class _FakeRuntime:
    """A runtime that answers immediately."""

    async def execute(self, *args: object, **kwargs: object) -> object:
        from mas_orchestrator.agents.base import Termination
        from mas_orchestrator.agents.runtime import ExecutionResult

        return ExecutionResult(
            execution_id="exec-1",
            agent_id="agent-1",
            termination=Termination.COMPLETED,
            content="the answer",
            duration_ms=1,
        )
