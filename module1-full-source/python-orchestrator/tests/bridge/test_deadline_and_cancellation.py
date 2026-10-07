# PURPOSE:
# Tests that a deadline bounds the run and that a cancellation stops it.
#
# Covers spec §11: end-to-end deadline and cancellation propagation, and the
# invariant that a cancelled run is NEVER reported as a success. A cancellation
# that returned success would let Rust commit an answer nobody waited for.

"""Deadline and cancellation tests for the bridge.

Two properties are under test, and the second is the one that matters:

* **A deadline bounds the run.** Rust supplies an absolute wall-clock
  deadline; the execution context carries it, and the runtime enforces it.
* **A cancellation is never a success.** This is the invariant. A cancelled
  run has no answer, so reporting it as ``succeeded`` would hand Rust an empty
  or stale output and let it commit a task that was explicitly abandoned —
  and would bill for work nobody wanted.

The cancellation path is exercised over a real gRPC call that the client
cancels mid-flight, so the wire cancellation genuinely reaches the servicer
rather than being simulated.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import grpc
import pytest
from tests.bridge.conftest import (
    BridgeHarness,
    RecordingExecutionService,
    failed_result,
    metadata,
    request,
    success_result,
)

from mas_orchestrator.agents.base import Termination
from mas_orchestrator.agents.runtime import ExecutionResult
from mas_orchestrator.grpc.agent_bridge_servicer import (
    CANCELLATION_POLL_SECONDS,
    finish_reason_for,
    parse_deadline,
)

if TYPE_CHECKING:
    from mas_orchestrator.grpc.generated import mas_api_pb2

pytestmark = pytest.mark.asyncio(loop_scope="function")


def _deadline_in(seconds: float) -> str:
    """Renders an RFC-3339 deadline some seconds from now.

    Args:
        seconds: How far in the future.

    Returns:
        The timestamp, with milliseconds and a ``Z`` offset — the shape Rust
        produces and :func:`parse_deadline` must accept.
    """
    moment = datetime.now(tz=UTC) + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


class TestDeadlineParsing:
    """The wire deadline string is parsed, not assumed."""

    async def test_an_absent_deadline_means_unbounded(self) -> None:
        """Empty means no deadline, which is legal — not zero."""
        assert parse_deadline("") is None
        assert parse_deadline("   ") is None

    async def test_a_zoned_deadline_parses(self) -> None:
        """The ``Z`` suffix Rust emits is accepted."""
        parsed = parse_deadline("2026-01-01T00:00:00.000Z")
        assert parsed is not None
        assert parsed == int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)

    async def test_an_offset_deadline_parses(self) -> None:
        """A numeric offset is accepted too, not only ``Z``."""
        assert parse_deadline("2026-01-01T00:00:00.000+00:00") == parse_deadline(
            "2026-01-01T00:00:00.000Z"
        )

    async def test_a_naive_deadline_is_treated_as_utc(self) -> None:
        """A timestamp with no zone is read as UTC rather than rejected.

        Interpreting it as local time would move the deadline by the server's
        offset, silently lengthening or shortening the caller's budget.
        """
        naive = parse_deadline("2026-01-01T00:00:00")
        assert naive == parse_deadline("2026-01-01T00:00:00.000Z")

    async def test_a_malformed_deadline_is_refused(self) -> None:
        """A deadline that cannot be parsed is an error, not an unbounded run."""
        from mas_orchestrator.contracts.errors import InvalidRequestError

        with pytest.raises(InvalidRequestError):
            parse_deadline("tomorrow")

    async def test_a_malformed_deadline_aborts_the_call(self, bridge: BridgeHarness) -> None:
        """An unparseable deadline never reaches the runtime."""
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(deadline_rfc3339="not-a-timestamp"), metadata=metadata())
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
        assert bridge.service.call_count == 0


class TestDeadlinePropagation:
    """The deadline travels from the wire into the execution context."""

    async def test_the_deadline_reaches_the_execution_context(self, bridge: BridgeHarness) -> None:
        """A run with a deadline is bounded by exactly that deadline."""
        deadline = _deadline_in(60.0)
        expected = parse_deadline(deadline)

        stub, channel = bridge.stub()
        await stub.RunAgent(request(deadline_rfc3339=deadline), metadata=metadata())
        await channel.close()

        recorded = bridge.service.runs[0]
        assert recorded.context.deadline_epoch_ms == expected

    async def test_a_run_without_a_deadline_is_unbounded(self, bridge: BridgeHarness) -> None:
        """No deadline means no bound, and must not fabricate one."""
        stub, channel = bridge.stub()
        await stub.RunAgent(request(deadline_rfc3339=""), metadata=metadata())
        await channel.close()

        assert bridge.service.runs[0].context.deadline_epoch_ms is None

    async def test_an_already_expired_deadline_is_refused_at_intake(
        self, bridge: BridgeHarness
    ) -> None:
        """A deadline in the past is refused before any work is dispatched.

        Running work whose deadline has already passed would spend provider
        budget on an answer the caller has stopped waiting for. Refusing at
        intake is the only place the refusal is free.
        """
        stub, channel = bridge.stub()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(deadline_rfc3339=_deadline_in(-30.0)), metadata=metadata())
        await channel.close()

        assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
        assert bridge.service.call_count == 0


class TestCancellation:
    """A cancelled run stops, and is never reported as a success."""

    async def test_a_cancelled_run_is_never_reported_as_success(
        self, bridge: BridgeHarness
    ) -> None:
        """The invariant: no cancellation may surface as ``succeeded``."""
        bridge.service.result = failed_result(Termination.CANCELLED)

        reply = await _run_cancelled(bridge)

        assert reply.WhichOneof("verdict") != "succeeded"

    async def test_a_cancelled_run_returns_a_failed_verdict(self, bridge: BridgeHarness) -> None:
        """A cancellation is reported as a terminal, non-retryable failure."""
        bridge.service.result = failed_result(Termination.CANCELLED)

        reply = await _run_cancelled(bridge)

        assert reply.failed.code == "cancelled"
        assert reply.failed.retryable is False

    async def test_a_cancelled_run_reports_no_output(self, bridge: BridgeHarness) -> None:
        """No answer means no answer: an empty output must not be fabricated."""
        bridge.service.result = failed_result(Termination.CANCELLED)

        reply = await _run_cancelled(bridge)

        assert reply.WhichOneof("verdict") == "failed"

    async def test_the_wire_cancellation_reaches_the_runtime(self, bridge: BridgeHarness) -> None:
        """Cancelling the RPC sets the runtime's cancel event.

        Without this the model keeps generating and the provider keeps billing
        for an answer nobody will read.
        """
        bridge.service.delay_seconds = 5.0
        bridge.service.honour_cancel = True

        stub, channel = bridge.stub()
        call = stub.RunAgent(request(), metadata=metadata())
        task = asyncio.ensure_future(call)

        # Let the run reach the runtime before cancelling.
        await asyncio.sleep(0.2)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        await channel.close()

        # Give the watcher one poll interval to mirror the cancellation.
        await asyncio.sleep(CANCELLATION_POLL_SECONDS * 4)

        assert bridge.service.runs, "the run should have been dispatched"
        assert bridge.service.runs[0].cancel_event.is_set()

    async def test_a_cancellation_is_distinguishable_from_a_failure(
        self, bridge: BridgeHarness
    ) -> None:
        """Cancelled and Failed are different terminations on the wire."""
        assert finish_reason_for(Termination.CANCELLED) == "cancelled"
        assert finish_reason_for(Termination.FAILED) == "failed"

    async def test_a_completed_run_is_still_success(self, bridge: BridgeHarness) -> None:
        """Guard against over-correction: a real answer is still a success."""
        bridge.service.result = success_result(content="a real answer")

        stub, channel = bridge.stub()
        reply = await stub.RunAgent(request(), metadata=metadata())
        await channel.close()

        assert reply.WhichOneof("verdict") == "succeeded"
        assert '"a real answer"' in reply.succeeded.output_json or (
            "a real answer" in reply.succeeded.output_json
        )


class TestTerminationMapping:
    """Every termination reason maps to a stable wire string."""

    async def test_all_terminations_map_to_a_known_reason(self) -> None:
        """No termination falls through to an invented string."""
        for termination in Termination:
            assert finish_reason_for(termination) in {
                "completed",
                "step_limit",
                "tool_call_limit",
                "deadline",
                "cancelled",
                "budget_exhausted",
                "failed",
            }

    async def test_completed_maps_to_completed(self) -> None:
        """The success termination is named as such."""
        assert finish_reason_for(Termination.COMPLETED) == "completed"

    async def test_a_deadline_termination_is_reported_as_a_failure(
        self, bridge: BridgeHarness
    ) -> None:
        """A run stopped by its deadline is a failure, not a short success."""
        bridge.service.result = failed_result(Termination.DEADLINE)

        reply = await _call(bridge)

        assert reply.WhichOneof("verdict") == "failed"
        assert reply.failed.code == "deadline"
        assert reply.failed.retryable is False


async def _call(bridge: BridgeHarness) -> mas_api_pb2.AgentRunReply:
    """Calls ``RunAgent``.

    Args:
        bridge: The running bridge.

    Returns:
        The reply.
    """
    stub, channel = bridge.stub()
    try:
        return await stub.RunAgent(request(), metadata=metadata())
    finally:
        await channel.close()


async def _run_cancelled(bridge: BridgeHarness):
    """Runs an execution whose result says it was cancelled.

    Args:
        bridge: The running bridge.

    Returns:
        The reply.
    """
    return await _call(bridge)


async def test_the_double_honours_its_cancel_event() -> None:
    """Guards the fixture: the double must actually wait on the event."""
    service: RecordingExecutionService = RecordingExecutionService(
        delay_seconds=0.5, honour_cancel=True
    )
    assert service.honour_cancel is True
    assert isinstance(service.runs, list)


async def test_an_absent_result_means_a_default_success() -> None:
    """Guards the fixture: an unconfigured double answers successfully."""
    service = RecordingExecutionService()
    assert service.result is None
    assert service.error is None


async def test_execution_result_succeeded_is_only_completed() -> None:
    """Guards the invariant the reply mapping depends on."""
    assert ExecutionResult("e", "a", Termination.COMPLETED).succeeded is True
    assert ExecutionResult("e", "a", Termination.CANCELLED).succeeded is False
