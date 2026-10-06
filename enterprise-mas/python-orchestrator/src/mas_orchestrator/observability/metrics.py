"""Metrics.

Spec §17's constraint on this module is the interesting one: *avoid
high-cardinality metric labels containing arbitrary user text, prompts, IDs,
URLs, or document content.*

That is enforced by :func:`is_safe_label_value` and by the label allowlist on
:class:`Metric`. A label whose value would multiply the series count without
bound is refused rather than truncated, because truncation produces a
half-identifier that looks safe and is not.

The default backend accumulates counters in memory. This is not a substitute
for a real metrics pipeline: it exists so behaviour is observable without a
collector, and it says so rather than pretending otherwise.
"""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Final, Protocol

from mas_orchestrator.contracts.errors import ConfigurationError

#: Label values shorter than this are almost certainly identifiers rather than
#: descriptions.
MAX_LABEL_VALUE_CHARS: Final[int] = 64

#: Characters allowed in a label value. Prometheus-compatible, which also
#: happens to exclude the free text we do not want.
_SAFE_LABEL_RE: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z0-9_.:-]*$")

#: Labels that are permitted at all. Anything else is refused, so a new call
#: site cannot quietly introduce a high-cardinality dimension.
ALLOWED_LABELS: Final[frozenset[str]] = frozenset(
    {
        "tenant_tier",
        "provider",
        "model_family",
        "agent_kind",
        "tool_kind",
        "termination",
        "status",
        "environment",
    }
)


def is_safe_label_value(value: str) -> bool:
    """Whether a label value is bounded and content-free."""
    return bool(value) and len(value) <= MAX_LABEL_VALUE_CHARS and bool(_SAFE_LABEL_RE.match(value))


@dataclass(frozen=True, slots=True)
class Metric:
    """A named measurement with bounded labels.

    Attributes:
        name: Metric name.
        labels: Dimensions. Keys must be in :data:`ALLOWED_LABELS` and values
            must pass :func:`is_safe_label_value`.
    """

    name: str
    labels: dict[str, str]

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ConfigurationError("a metric requires a name", details={"field": "name"})
        for key, value in self.labels.items():
            if key not in ALLOWED_LABELS:
                raise ConfigurationError(
                    "metric label is not on the allowlist: high-cardinality "
                    "and content-bearing labels are refused by design",
                    details={"label": key, "metric": self.name},
                )
            if not is_safe_label_value(value):
                raise ConfigurationError(
                    "metric label value is not a bounded identifier",
                    details={"label": key, "metric": self.name},
                )

    @property
    def series_key(self) -> str:
        """A stable key identifying this series."""
        rendered = ",".join(f"{k}={self.labels[k]}" for k in sorted(self.labels))
        return f"{self.name}{{{rendered}}}"


class MetricsBackend(Protocol):
    """Where measurements go."""

    def counter(self, metric: Metric, value: float = 1.0) -> None: ...
    def gauge(self, metric: Metric, value: float) -> None: ...
    def observe(self, metric: Metric, value_ms: float) -> None: ...


class InMemoryBackend:
    """Accumulates measurements in a dictionary.

    Intended for tests and for deployments without a collector. It is not
    durable and is not thread-safe across processes; it says so rather than
    being mistaken for a real backend.
    """

    def __init__(self) -> None:
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def counter(self, metric: Metric, value: float = 1.0) -> None:
        """Adds to a counter."""
        with self._lock:
            key = metric.series_key
            self._counters[key] = self._counters.get(key, 0.0) + value

    def gauge(self, metric: Metric, value: float) -> None:
        """Sets a gauge."""
        with self._lock:
            self._gauges[metric.series_key] = value

    def observe(self, metric: Metric, value_ms: float) -> None:
        """Records one observation."""
        with self._lock:
            self._histograms.setdefault(metric.series_key, []).append(value_ms)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Everything recorded, for assertions and debugging."""
        with self._lock:
            histograms = {
                key: {
                    "count": float(len(values)),
                    "sum_ms": float(sum(values)),
                    "max_ms": float(max(values)) if values else 0.0,
                }
                for key, values in self._histograms.items()
            }
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": histograms,
            }

    def reset(self) -> None:
        """Forgets everything."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()


class MetricsRecorder:
    """Records measurements against one backend."""

    def __init__(self, backend: MetricsBackend | None = None) -> None:
        self._backend = backend or InMemoryBackend()

    @property
    def backend(self) -> MetricsBackend:
        """The configured backend."""
        return self._backend

    def count(
        self,
        name: str,
        *,
        labels: dict[str, str] | None = None,
        value: float = 1.0,
    ) -> None:
        """Increments a counter."""
        self._backend.counter(Metric(name, dict(labels or {})), value)

    def gauge(
        self,
        name: str,
        value: float,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Sets a gauge."""
        self._backend.gauge(Metric(name, dict(labels or {})), value)

    def observe(
        self,
        name: str,
        value_ms: float,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Records a duration in milliseconds."""
        self._backend.observe(Metric(name, dict(labels or {})), value_ms)

    def time(self, name: str, *, labels: dict[str, str] | None = None) -> _Timer:
        """Returns a context manager that observes elapsed time."""
        return _Timer(self, name, labels or {})


class _Timer:
    """Measures a block and records the duration on exit."""

    def __init__(self, recorder: MetricsRecorder, name: str, labels: dict[str, str]) -> None:
        self._recorder = recorder
        self._metric = Metric(name, labels)
        self._started = 0.0

    def __enter__(self) -> _Timer:
        self._started = time.monotonic()
        return self

    def __exit__(self, *exc: object) -> None:
        elapsed_ms = (time.monotonic() - self._started) * 1000
        self._recorder.observe(self._metric.name, elapsed_ms, labels=self._metric.labels)
