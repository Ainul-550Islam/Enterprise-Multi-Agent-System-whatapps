# PURPOSE:
# Public surface of the observability layer.
#
# Exports logging, metrics, tracing, health and bridge telemetry. The never-log
# list is enforced by construction rather than by review: log records are
# screened for credential shapes, metric labels are restricted to a fixed
# allowlist of bounded identifiers, and span attributes reject content-bearing
# values.

"""Observability.

Implements spec §17. Every entry point emits structured telemetry carrying
correlation context, and the never-log list is enforced by construction rather
than by review:

* :mod:`.logging` installs a :class:`~.logging.RedactingFilter` that screens
  every record for credential shapes before it reaches a handler.
* :mod:`.metrics` refuses labels that are not on a fixed allowlist, and refuses
  values that are not bounded identifiers — so a prompt, a URL or an execution
  id cannot become a metric dimension.
* :mod:`.tracing` caps span attributes and rejects content-bearing values.
* :mod:`.health` separates liveness from readiness, and reports environment
  configuration by name and length only.
* :mod:`.bridge_metrics` measures the Rust → Python bridge. It correlates by
  id in *logs* and keeps *metric labels* bounded, so no tenant, prompt or
  credential can become a series dimension.

The module named ``logging`` shadows the standard library *within this package
only*; it imports the standard library as ``logging as _logging`` and every
other module in the codebase continues to import the standard library directly.
"""

from __future__ import annotations

from mas_orchestrator.observability.bridge_metrics import (
    BridgeTelemetry,
    NoopTelemetry,
)
from mas_orchestrator.observability.health import (
    CHECK_TIMEOUT_SECONDS,
    DependencyStatus,
    HealthChecker,
    HealthReport,
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
    TextFormatter,
    bind_context,
    clear_context,
    configure_logging,
    current_context,
    get_logger,
)
from mas_orchestrator.observability.metrics import (
    ALLOWED_LABELS,
    MAX_LABEL_VALUE_CHARS,
    InMemoryBackend,
    Metric,
    MetricsBackend,
    MetricsRecorder,
    is_safe_label_value,
)
from mas_orchestrator.observability.tracing import (
    MAX_ATTRIBUTE_CHARS,
    MAX_SPAN_ATTRIBUTES,
    CollectingExporter,
    DiscardingExporter,
    Span,
    SpanExporter,
    SpanStatus,
    TraceContext,
    Tracer,
    current_span,
)

__all__ = [
    "ALLOWED_LABELS",
    "CHECK_TIMEOUT_SECONDS",
    "MAX_ATTRIBUTE_CHARS",
    "MAX_LABEL_VALUE_CHARS",
    "MAX_SPAN_ATTRIBUTES",
    "BridgeTelemetry",
    "CollectingExporter",
    "CorrelationFilter",
    "DependencyStatus",
    "DiscardingExporter",
    "HealthChecker",
    "HealthReport",
    "HealthState",
    "InMemoryBackend",
    "JsonFormatter",
    "LoggingConfig",
    "Metric",
    "MetricsBackend",
    "MetricsRecorder",
    "NoopTelemetry",
    "RedactingFilter",
    "Span",
    "SpanExporter",
    "SpanStatus",
    "TextFormatter",
    "TraceContext",
    "Tracer",
    "bind_context",
    "clear_context",
    "configure_logging",
    "current_context",
    "current_span",
    "default_checker",
    "get_logger",
    "is_safe_label_value",
    "overall_state",
    "reset_default_checker",
    "safe_status",
]
