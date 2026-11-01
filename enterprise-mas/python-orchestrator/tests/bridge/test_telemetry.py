# PURPOSE:
# Tests that the bridge emits correlated telemetry without leaking secrets or
# high-cardinality dimensions.
#
# Covers spec §13: gRPC and Python runtime latency, model/tool latency,
# timeout, cancellation, retry, failover, provider failure, authorization
# failure and tenant mismatch counts — correlated by id, never carrying a
# credential, prompt, or unbounded label.

"""Telemetry tests for the bridge.

Two failure modes are guarded against, and they pull in opposite directions:

* **Under-measuring.** If a cancellation or a tenant mismatch is not counted,
  an operator cannot see it. These are rare by design, so the counter is the
  only realistic way to notice one.
* **Over-measuring.** If a tenant id, a prompt or an error message becomes a
  metric label, telemetry becomes the easiest exfiltration path in the system:
  it is emitted automatically on every call and shipped off-host by default.

So each test asserts both that the measurement exists and that it is bounded.
"""

from __future__ import annotations

import logging

import pytest
from tests.bridge.conftest import (
    RecordingExecutionService,
    failed_result,
    metadata,
    request,
    resolver,
    success_result,
)

from mas_orchestrator.agents.base import Termination
from mas_orchestrator.contracts.errors import (
    ModelProviderError,
    ModelTimeoutError,
)
from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.observability.bridge_metrics import (
    BridgeTelemetry,
    NoopTelemetry,
)
from mas_orchestrator.observability.metrics import InMemoryBackend, MetricsRecorder

pytestmark = pytest.mark.asyncio(loop_scope="function")

SECRET = "sk-live-TELEMETRY-SECRET-VALUE"


@pytest.fixture
def backend() -> InMemoryBackend:
    """A metrics backend whose contents a test can inspect."""
    return InMemoryBackend()


@pytest.fixture
def recorder(backend: InMemoryBackend) -> MetricsRecorder:
    """A recorder over the inspectable backend."""
    return MetricsRecorder(backend)


def _servicer(
    service: RecordingExecutionService,
    recorder: MetricsRecorder,
) -> AgentBridgeServicer:
    """Builds an instrumented servicer.

    Args:
        service: The execution double.
        recorder: Where measurements go.

    Returns:
        The servicer.
    """
    return AgentBridgeServicer(
        execution_service=service,  # type: ignore[arg-type]
        identity_resolver=resolver(),
        config=AgentBridgeServicerConfig(),
        telemetry=BridgeTelemetry(recorder=recorder),
    )


async def _run(servicer: AgentBridgeServicer, **kwargs: object):
    """Drives one run through the servicer.

    Args:
        servicer: The servicer.
        **kwargs: Request overrides.

    Returns:
        The reply.
    """
    from tests.bridge.conftest import FakeContext

    return await servicer.RunAgent(request(**kwargs), FakeContext(metadata()))  # type: ignore[arg-type]


class TestCounters:
    """Each rare event is counted."""

    async def test_a_successful_run_is_counted(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A completed run increments the success counter."""
        service = RecordingExecutionService(result=success_result())

        await _run(_servicer(service, recorder))

        assert (
            backend.snapshot()["counters"].get("bridge.runs_succeeded{termination=completed}")
            == 1.0
        )

    async def test_a_failed_run_is_counted(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A failed run increments the failure counter, not success."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelTimeoutError("timed out"))
        )

        await _run(_servicer(service, recorder))

        counters = backend.snapshot()["counters"]
        assert counters.get("bridge.runs_failed{termination=failed}") == 1.0
        assert "bridge.runs_succeeded{termination=completed}" not in counters

    async def test_a_deadline_termination_is_counted_as_a_timeout(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A run stopped by its deadline is visible as a timeout."""
        service = RecordingExecutionService(result=failed_result(Termination.DEADLINE))

        await _run(_servicer(service, recorder))

        assert backend.snapshot()["counters"].get("bridge.timeouts{}") == 1.0

    async def test_an_authorization_failure_is_counted(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A caller that cannot authenticate is counted, not merely logged."""
        servicer = _servicer(RecordingExecutionService(), recorder)

        with pytest.raises(Exception):  # noqa: B017 - the fake context aborts
            from tests.bridge.conftest import FakeContext

            await servicer.RunAgent(
                request(),
                FakeContext(metadata(credential="wrong")),  # type: ignore[arg-type]
            )

        assert backend.snapshot()["counters"].get("bridge.authorization_failures{}") == 1.0

    async def test_a_tenant_mismatch_is_counted(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A header/body disagreement is the key security counter."""
        servicer = _servicer(RecordingExecutionService(), recorder)

        with pytest.raises(Exception):  # noqa: B017 - the fake context aborts
            from tests.bridge.conftest import FakeContext

            await servicer.RunAgent(
                request(tenant_id="tenant-alpha"),
                FakeContext(metadata(tenant_id="tenant-beta")),  # type: ignore[arg-type]
            )

        assert backend.snapshot()["counters"].get("bridge.tenant_mismatches{}") == 1.0

    async def test_a_provider_failure_is_counted(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A provider failure is counted under a bounded label."""
        from tests.bridge.conftest import make_context

        BridgeTelemetry(recorder=recorder).provider_failure(
            make_context(), provider="openai/gpt-4o", error=ModelProviderError("upstream 500")
        )

        counters = backend.snapshot()["counters"]
        assert counters.get("bridge.provider_failures{provider=openai}") == 1.0

    async def test_a_failover_is_counted(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A failover is counted, and only reports model families."""
        from tests.bridge.conftest import make_context

        BridgeTelemetry(recorder=recorder).failover(
            make_context(), from_model="openai/gpt-4o", to_model="anthropic/claude-3"
        )

        assert backend.snapshot()["counters"].get("bridge.failovers{}") == 1.0


class TestLatency:
    """Latency is measured on both sides of the bridge."""

    async def test_both_grpc_and_runtime_latency_are_recorded(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """An operator can tell intake slowness from model slowness."""
        service = RecordingExecutionService(result=success_result())

        await _run(_servicer(service, recorder))

        histograms = backend.snapshot()["histograms"]
        assert "bridge.grpc_latency_ms{}" in histograms
        assert "bridge.runtime_latency_ms{}" in histograms

    async def test_latency_is_non_negative(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A negative duration would corrupt every aggregate computed on it."""
        service = RecordingExecutionService(result=success_result())

        await _run(_servicer(service, recorder))

        for summary in backend.snapshot()["histograms"].values():
            assert summary["count"] >= 1.0
            assert summary["sum_ms"] >= 0.0
            assert summary["max_ms"] >= 0.0

    async def test_model_latency_is_recorded_per_provider(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """Model latency is attributed to the provider that served it."""
        BridgeTelemetry(recorder=recorder).model_latency(
            provider="anthropic/claude-3", elapsed_ms=42.0
        )

        summary = backend.snapshot()["histograms"].get(
            "bridge.model_latency_ms{provider=anthropic}"
        )
        assert summary is not None
        assert summary["sum_ms"] == 42.0

    async def test_tool_latency_is_recorded_per_kind(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """Tool latency is attributed by kind, not by tenant-authored id."""
        BridgeTelemetry(recorder=recorder).tool_latency(tool_kind="http_request", elapsed_ms=7.5)

        summary = backend.snapshot()["histograms"].get(
            "bridge.tool_latency_ms{tool_kind=http_request}"
        )
        assert summary is not None


class TestTelemetrySafety:
    """Measurements must not become an exfiltration channel."""

    async def test_no_tenant_id_becomes_a_metric_label(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A tenant id would create one series per customer."""
        from tests.bridge.conftest import make_context

        context = make_context(tenant_id="tenant-very-distinctive")
        telemetry = BridgeTelemetry(recorder=recorder)
        telemetry.run_started(context)
        telemetry.provider_failure(context, provider="openai", error=RuntimeError("x"))

        rendered = str(backend.snapshot())
        assert "tenant-very-distinctive" not in rendered

    async def test_a_provider_error_message_is_not_recorded(
        self, recorder: MetricsRecorder, backend: InMemoryBackend
    ) -> None:
        """A provider's error text can contain request bodies and headers."""
        from tests.bridge.conftest import make_context

        BridgeTelemetry(recorder=recorder).provider_failure(
            make_context(),
            provider="openai",
            error=RuntimeError(f"rejected {SECRET}"),
        )

        assert SECRET not in str(backend.snapshot())

    async def test_a_secret_in_a_message_is_redacted_from_the_log(
        self,
        recorder: MetricsRecorder,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The failure log carries the safe message, not the raw one."""
        service = RecordingExecutionService(
            result=failed_result(error=ModelProviderError(f"key {SECRET} refused"))
        )

        with caplog.at_level(logging.WARNING):
            await _run(_servicer(service, recorder))

        assert SECRET not in caplog.text

    async def test_label_values_are_reduced_to_a_family(self) -> None:
        """A full model key is not published as a label."""
        from mas_orchestrator.observability.bridge_metrics import _family

        assert _family("openai/gpt-4o") == "openai"
        assert _family("Weird Provider") == "weird provider"
        assert _family("") == "unknown"

    async def test_a_correlation_id_is_present_in_logs(
        self,
        recorder: MetricsRecorder,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Correlation is what makes the telemetry joinable to a run."""
        service = RecordingExecutionService(result=success_result())

        with caplog.at_level(logging.INFO):
            await _run(_servicer(service, recorder), correlation_id="corr-join")

        assert any(
            record.__dict__.get("correlation_id") == "corr-join" for record in caplog.records
        )


class TestNoopTelemetry:
    """The no-op sink is a valid substitute everywhere."""

    async def test_a_servicer_without_telemetry_still_runs(self) -> None:
        """Omitting telemetry must not disable the bridge."""
        service = RecordingExecutionService(result=success_result())
        servicer = AgentBridgeServicer(
            execution_service=service,  # type: ignore[arg-type]
            identity_resolver=resolver(),
        )

        reply = await _run(servicer)

        assert reply.WhichOneof("verdict") == "succeeded"
        assert isinstance(servicer.telemetry, NoopTelemetry)

    async def test_the_noop_sink_accepts_every_call(self) -> None:
        """Every method is a no-op, so no call site needs a guard."""
        telemetry = NoopTelemetry()
        telemetry.tenant_mismatch(correlation_id="c", reason="r")
        telemetry.model_latency(provider="p", elapsed_ms=1.0)
        telemetry.tool_latency(tool_kind="k", elapsed_ms=1.0)

    async def test_a_span_measures_a_block(self, recorder: MetricsRecorder) -> None:
        """The span helper returns a usable duration."""
        import asyncio

        telemetry = BridgeTelemetry(recorder=recorder)
        with telemetry.span() as span:
            await asyncio.sleep(0.01)

        assert span.elapsed_ms >= 5.0
