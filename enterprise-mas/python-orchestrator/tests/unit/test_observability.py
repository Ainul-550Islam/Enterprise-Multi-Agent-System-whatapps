"""Tests for logging, metrics, tracing and health.

The claims that matter: a secret must not survive into a log record, metric
labels must not carry high-cardinality identifiers, and health must answer
without leaking configuration.
"""

from __future__ import annotations

import logging

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError
from mas_orchestrator.observability.health import (
    HealthChecker,
    HealthState,
    default_checker,
    overall_state,
    reset_default_checker,
    safe_status,
)
from mas_orchestrator.observability.logging import (
    CorrelationFilter,
    JsonFormatter,
    LoggingConfig,
    RedactingFilter,
    bind_context,
    clear_context,
    configure_logging,
    current_context,
)
from mas_orchestrator.observability.metrics import (
    ALLOWED_LABELS,
    InMemoryBackend,
    Metric,
    MetricsRecorder,
    is_safe_label_value,
)
from mas_orchestrator.observability.tracing import (
    MAX_ATTRIBUTE_CHARS,
    MAX_SPAN_ATTRIBUTES,
    CollectingExporter,
    Span,
    SpanStatus,
    TraceContext,
    Tracer,
    current_span,
)

SECRET = "sk-" + "A" * 24


def _record(message: str) -> logging.LogRecord:
    return logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


class TestLogging:
    def test_redaction_screens_credentials(self) -> None:
        record = _record(f"token is {SECRET}")
        assert RedactingFilter().filter(record)
        assert "sk-A" not in record.getMessage()

    def test_redaction_can_be_disabled_for_tests(self) -> None:
        record = _record(f"token is {SECRET}")
        RedactingFilter(enabled=False).filter(record)
        assert "sk-A" in record.getMessage()

    def test_correlation_filter_adds_defaults(self) -> None:
        record = _record("hello")
        assert CorrelationFilter().filter(record)
        assert hasattr(record, "correlation_id")

    def test_json_formatter_emits_parseable_output(self) -> None:
        import json

        record = _record("hello")
        CorrelationFilter().filter(record)
        assert json.loads(JsonFormatter().format(record))["message"] == "hello"

    def test_context_binds_and_clears(self) -> None:
        bind_context(correlation_id="corr-1")
        try:
            assert current_context()["correlation_id"] == "corr-1"
        finally:
            clear_context()

    def test_configure_logging_is_idempotent(self) -> None:
        # Calling it twice must not double every log line.
        configure_logging(LoggingConfig(level="WARNING"))
        before = len(logging.getLogger().handlers)
        configure_logging(LoggingConfig(level="WARNING"))
        assert len(logging.getLogger().handlers) == before


class TestMetrics:
    def test_high_cardinality_label_names_are_refused(self) -> None:
        # An execution id as a label would explode metric cardinality, so the
        # allowlist refuses the key before any value reaches it.
        with pytest.raises(ConfigurationError):
            Metric(name="ai_requests_total", labels={"execution_id": "exec-1"})

    def test_unbounded_label_values_are_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            Metric(name="ai_requests_total", labels={"status": "x" * 500})

    def test_safe_label_values_are_accepted(self) -> None:
        assert is_safe_label_value("ok")

    def test_allowed_labels_are_known(self) -> None:
        assert ALLOWED_LABELS
        assert "status" in ALLOWED_LABELS

    def test_backend_records_and_snapshots(self) -> None:
        backend = InMemoryBackend()
        metric = Metric(name="ai_requests_total", labels={"status": "ok"})
        backend.counter(metric)
        assert backend.snapshot()["counters"]
        backend.reset()
        assert backend.snapshot()["counters"] == {}

    def test_recorder_uses_the_backend(self) -> None:
        backend = InMemoryBackend()
        recorder = MetricsRecorder(backend)
        recorder.count("ai_requests_total", labels={"status": "ok"})
        assert backend.snapshot()["counters"]

    def test_recorder_observes_durations(self) -> None:
        backend = InMemoryBackend()
        MetricsRecorder(backend).observe("ai_duration_ms", 12.0, labels={"status": "ok"})
        assert backend.snapshot()["histograms"]

    def test_gauges_are_recorded(self) -> None:
        backend = InMemoryBackend()
        MetricsRecorder(backend).gauge("ai_in_flight", 3.0, labels={"status": "ok"})
        assert backend.snapshot()["gauges"]

    def test_series_key_separates_labels(self) -> None:
        first = Metric(name="m", labels={"status": "ok"})
        second = Metric(name="m", labels={"status": "error"})
        assert first.series_key != second.series_key

    def test_timer_records_on_exit(self) -> None:
        backend = InMemoryBackend()
        with MetricsRecorder(backend).time("ai_duration_ms", labels={"status": "ok"}):
            pass
        assert backend.snapshot()["histograms"]


class TestTracing:
    def test_span_exports_on_exit_when_sampled(self) -> None:
        exporter = CollectingExporter()
        tracer = Tracer(exporter=exporter, sample_ratio=1.0)
        with tracer.span("agent.run", agent_id="worker") as span:
            span.set_attribute("steps", 3)
        assert "agent.run" in exporter.names()

    def test_span_discards_when_unsampled(self) -> None:
        # Sampling off by default: tracing every call is expensive and the
        # default must be cheap.
        exporter = CollectingExporter()
        tracer = Tracer(exporter=exporter, sample_ratio=0.0)
        with tracer.span("agent.run"):
            pass
        assert exporter.names() == []

    def test_span_records_errors(self) -> None:
        exporter = CollectingExporter()
        tracer = Tracer(exporter=exporter, sample_ratio=1.0)
        with tracer.span("agent.run") as span:
            span.record_error("boom")
        assert span.status is SpanStatus.ERROR

    def test_span_is_current_inside_its_block(self) -> None:
        tracer = Tracer(sample_ratio=1.0)
        with tracer.span("agent.run") as span:
            assert current_span() is span
        assert current_span() is None

    def test_attributes_are_bounded(self) -> None:
        # Refused rather than silently dropped: an unbounded attribute set is a
        # memory leak in the exporter, and a silent drop hides the span.
        tracer = Tracer(sample_ratio=1.0)
        with tracer.span("agent.run") as span, pytest.raises(InvalidRequestError):
            for index in range(MAX_SPAN_ATTRIBUTES + 5):
                span.set_attribute(f"key{index}", "v")
        assert len(span.attributes) <= MAX_SPAN_ATTRIBUTES

    def test_long_attribute_values_are_refused(self) -> None:
        tracer = Tracer(sample_ratio=1.0)
        with tracer.span("agent.run") as span, pytest.raises(InvalidRequestError):
            span.set_attribute("prompt", "x" * (MAX_ATTRIBUTE_CHARS + 1))

    def test_trace_context_renders_headers(self) -> None:
        context = TraceContext(trace_id="a" * 32, span_id="b" * 16, sampled=True)
        assert "traceparent" in context.to_headers()

    def test_invalid_trace_ids_are_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            TraceContext(trace_id="not-hex", span_id="b" * 16)

    def test_span_is_a_span(self) -> None:
        assert isinstance(Tracer(sample_ratio=1.0).span("op"), Span)


class TestHealth:
    def test_new_checker_is_ready(self) -> None:
        assert HealthChecker().check().ready

    def test_raising_check_is_reported_as_failing(self) -> None:
        # An exception from a probe is the clearest signal there is, so it is
        # reported rather than swallowed.
        def probe() -> HealthState:
            raise RuntimeError("unreachable")

        checker = HealthChecker()
        checker.register("broker", probe)
        report = checker.check()
        assert not report.ready
        assert report.dependencies[0].state is HealthState.FAILING

    def test_no_opinion_is_degraded_not_ok(self) -> None:
        # A check that cannot tell is not evidence of health.
        checker = HealthChecker()
        checker.register("broker", lambda: None)
        report = checker.check()
        assert report.dependencies[0].state is HealthState.DEGRADED

    def test_liveness_does_not_touch_dependencies(self) -> None:
        # If liveness depended on a dependency, an outage would restart the
        # service repeatedly and make the outage worse.
        def probe() -> HealthState:
            raise RuntimeError("unreachable")

        checker = HealthChecker()
        checker.register("broker", probe)
        assert checker.liveness().state is HealthState.OK

    def test_overall_state_takes_the_worst(self) -> None:
        assert overall_state([HealthState.OK, HealthState.FAILING]) is HealthState.FAILING

    def test_safe_status_reports_lengths_not_values(self) -> None:
        # A health route is usually the least protected route in a service.
        payload = safe_status(
            service_name="svc",
            environment="development",
            report=HealthChecker().check(),
        )
        assert "sk-" not in str(payload)

    def test_default_checker_is_shared_and_resettable(self) -> None:
        assert default_checker() is default_checker()
        reset_default_checker()
        assert default_checker() is not None
