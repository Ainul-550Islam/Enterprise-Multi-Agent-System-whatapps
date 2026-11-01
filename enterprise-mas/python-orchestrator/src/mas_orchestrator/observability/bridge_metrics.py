# PURPOSE:
# Telemetry for the Rust -> Python agent bridge.
#
# Measures gRPC and Python runtime latency, model/tool latency, and counts of
# timeout, cancellation, retry, failover, provider failure, authorization
# failure and tenant mismatch — correlated by correlation id, without secrets
# and without high-cardinality labels.

"""Correlated telemetry for the agent bridge.

Spec §13 asks for measurements that can be *joined* to a run. Two channels are
used, and the split is deliberate:

* **Logs carry the correlation.** Every event below includes
  ``correlation_id``, ``execution_id`` and ``request_id``, so a single run can
  be traced end to end. These are unbounded, high-cardinality values, which is
  exactly what a *log* is for.
* **Metrics carry the shape.** Counters and histograms are labelled only with
  the bounded dimensions the metrics layer allows — ``provider``,
  ``agent_kind``, ``termination``, ``status``. A tenant id is never a metric
  label: it would create a series per customer, which is both a cost problem
  and a data-leak problem, since a label set is far more likely to be exported
  to a third party than a log line is.

Nothing here records a prompt, an output, a credential or a header. The
bridge sits on the most sensitive path in the system, and telemetry is the
easiest place for that data to escape — it is emitted automatically, on every
call, and shipped off-host by default.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from types import TracebackType

    from mas_orchestrator.agents.base import Termination
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.contracts.errors import OrchestratorError
    from mas_orchestrator.observability.metrics import MetricsRecorder

_LOGGER: Final[logging.Logger] = logging.getLogger("mas_orchestrator.bridge")

__all__ = [
    "BRIDGE_METRIC_PREFIX",
    "BridgeTelemetry",
    "NoopTelemetry",
]

#: Prefix for every series emitted here, so the bridge's measurements can be
#: selected without enumerating them.
BRIDGE_METRIC_PREFIX: Final[str] = "bridge"

#: Series names, kept as constants so a rename is one edit rather than a
#: silent divergence between the emitter and the dashboard that reads it.
GRPC_LATENCY: Final[str] = f"{BRIDGE_METRIC_PREFIX}.grpc_latency_ms"
RUNTIME_LATENCY: Final[str] = f"{BRIDGE_METRIC_PREFIX}.runtime_latency_ms"
MODEL_LATENCY: Final[str] = f"{BRIDGE_METRIC_PREFIX}.model_latency_ms"
TOOL_LATENCY: Final[str] = f"{BRIDGE_METRIC_PREFIX}.tool_latency_ms"

#: Counter suffixes. Each is one observation of a thing that should be rare,
#: so a non-zero value is a signal rather than noise.
COUNTER_SUFFIXES: Final[frozenset[str]] = frozenset(
    {
        "runs_started",
        "runs_succeeded",
        "runs_failed",
        "timeouts",
        "cancellations",
        "retries",
        "failovers",
        "provider_failures",
        "authorization_failures",
        "tenant_mismatches",
    }
)


@dataclass(slots=True)
class BridgeTelemetry:
    """Records bridge measurements for one process.

    Args:
        recorder: Where measurements go.
        logger: Logger override, for tests.

    The counters are cumulative for the process. They are not reset per run:
    a bridge-wide total is what an operator wants, and per-run detail already
    lives in the logs under the correlation id.
    """

    recorder: MetricsRecorder
    logger: logging.Logger = field(default=_LOGGER)

    def _count(self, name: str, **labels: str) -> None:
        """Increments one counter.

        Args:
            name: Series name, already prefixed.
            **labels: Bounded label values. Empty and ``None`` values are
                dropped: an absent label is not a label with an empty value,
                and the metrics layer refuses the latter.
        """
        self.recorder.count(name, labels=_labels(**labels))

    # --- lifecycle ----------------------------------------------------------

    def run_started(self, context: ExecutionContext) -> None:
        """Records that a run was accepted.

        Args:
            context: The execution context, for correlation.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.runs_started")
        self.logger.info(
            "bridge run started",
            extra={
                "event": "bridge.run_started",
                "correlation_id": context.correlation_id,
                "execution_id": context.execution_id,
                "request_id": context.request_id,
            },
        )

    def run_finished(
        self,
        context: ExecutionContext,
        *,
        termination: Termination,
        elapsed_ms: int,
        grpc_overhead_ms: int,
    ) -> None:
        """Records how a run ended and how long each side took.

        Args:
            context: The execution context, for correlation.
            termination: Why the run stopped.
            elapsed_ms: Total time inside the servicer.
            grpc_overhead_ms: Time spent outside the runtime — decoding,
                authentication, scope reconciliation, reply encoding.
        """
        name = "runs_succeeded" if termination.value == "completed" else "runs_failed"
        self._count(
            f"{BRIDGE_METRIC_PREFIX}.{name}",
            termination=termination.value,
        )
        self.recorder.observe(GRPC_LATENCY, float(grpc_overhead_ms))
        self.recorder.observe(RUNTIME_LATENCY, float(max(0, elapsed_ms - grpc_overhead_ms)))

        self.logger.info(
            "bridge run finished",
            extra={
                "event": "bridge.run_finished",
                "correlation_id": context.correlation_id,
                "execution_id": context.execution_id,
                "termination": termination.value,
                "elapsed_ms": elapsed_ms,
                "grpc_overhead_ms": grpc_overhead_ms,
                "runtime_ms": max(0, elapsed_ms - grpc_overhead_ms),
            },
        )

    # --- failure classes ----------------------------------------------------

    def timeout(self, context: ExecutionContext) -> None:
        """Records a run stopped by its deadline.

        Args:
            context: The execution context, for correlation.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.timeouts")
        self.logger.warning(
            "bridge run timed out",
            extra={
                "event": "bridge.timeout",
                "correlation_id": context.correlation_id,
                "execution_id": context.execution_id,
            },
        )

    def cancellation(self, context: ExecutionContext) -> None:
        """Records a run cancelled from the wire.

        Args:
            context: The execution context, for correlation.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.cancellations")
        self.logger.info(
            "bridge run cancelled",
            extra={
                "event": "bridge.cancelled",
                "correlation_id": context.correlation_id,
                "execution_id": context.execution_id,
            },
        )

    def retry(self, context: ExecutionContext, *, attempt: int) -> None:
        """Records one retry of a model call.

        Args:
            context: The execution context, for correlation.
            attempt: Which attempt this is, starting at 1.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.retries")

    def failover(self, context: ExecutionContext, *, from_model: str, to_model: str) -> None:
        """Records a failover between providers or models.

        Args:
            context: The execution context, for correlation.
            from_model: The model that failed.
            to_model: The model taking over.

        Only the model *family* is recorded. A model key is a configured
        identifier rather than a secret, but recording the full key per run
        would let a dashboard reveal which models a tenant uses — which is
        commercial information the telemetry layer has no call to publish.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.failovers")
        self.logger.warning(
            "bridge model failover",
            extra={
                "event": "bridge.failover",
                "correlation_id": context.correlation_id,
                "execution_id": context.execution_id,
                "from_family": _family(from_model),
                "to_family": _family(to_model),
            },
        )

    def provider_failure(
        self, context: ExecutionContext, *, provider: str, error: BaseException
    ) -> None:
        """Records a provider call that failed.

        Args:
            context: The execution context, for correlation.
            provider: The provider id.
            error: What went wrong. Only its *type* is recorded: a provider's
                error text routinely contains request bodies and headers, and
                a telemetry field is shipped off-host by default.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.provider_failures", provider=_family(provider))
        self.logger.warning(
            "bridge provider failure",
            extra={
                "event": "bridge.provider_failure",
                "correlation_id": context.correlation_id,
                "execution_id": context.execution_id,
                "provider": _family(provider),
                "error_type": type(error).__name__,
            },
        )

    def authorization_failure(
        self, context: ExecutionContext | None, *, error: OrchestratorError
    ) -> None:
        """Records a caller that was refused.

        Args:
            context: The execution context, when one was built. A caller
                refused during authentication has none, so this is optional.
            error: The refusal. Only its code is recorded, never its details:
                refusal details name the subject and the reason, which is
                useful to the caller but not to a metrics pipeline.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.authorization_failures")
        self.logger.warning(
            "bridge authorization failure",
            extra={
                "event": "bridge.authorization_failure",
                "correlation_id": getattr(context, "correlation_id", "") or "",
                "error_code": str(error.code),
            },
        )

    def tenant_mismatch(self, *, correlation_id: str, reason: str) -> None:
        """Records a request whose header and body disagreed.

        Args:
            correlation_id: Correlation id, when one was available.
            reason: Which field disagreed.

        A tenant mismatch is the single most important security counter here. It
        is expected to be zero; a sustained non-zero value means something is
        splicing headers, and it must be visible without reading logs.
        """
        self._count(f"{BRIDGE_METRIC_PREFIX}.tenant_mismatches")
        self.logger.warning(
            "bridge tenant mismatch",
            extra={
                "event": "bridge.tenant_mismatch",
                "correlation_id": correlation_id,
                "reason": reason,
            },
        )

    # --- latency ------------------------------------------------------------

    def model_latency(self, *, provider: str, elapsed_ms: float) -> None:
        """Records one model call.

        Args:
            provider: The provider id.
            elapsed_ms: How long the call took.
        """
        self.recorder.observe(MODEL_LATENCY, elapsed_ms, labels={"provider": _family(provider)})

    def tool_latency(self, *, tool_kind: str, elapsed_ms: float) -> None:
        """Records one tool invocation.

        Args:
            tool_kind: The kind of tool, not its id: a tool id is
                tenant-authored and unbounded.
            elapsed_ms: How long the call took.
        """
        self.recorder.observe(TOOL_LATENCY, elapsed_ms, labels={"tool_kind": _family(tool_kind)})

    def span(self) -> _Span:
        """Returns a context manager measuring a block.

        Returns:
            A :class:`_Span` recording elapsed milliseconds.
        """
        return _Span()


@dataclass(frozen=True, slots=True)
class NoopTelemetry:
    """A telemetry sink that records nothing.

    Used by tests and by callers that have not configured a metrics backend.
    It exists so the bridge can require a telemetry collaborator without
    forcing every construction site to build a real one — and, unlike a bare
    ``None``, it cannot be forgotten and dereferenced.
    """

    def run_started(self, context: ExecutionContext) -> None:
        """Records nothing.

        Args:
            context: Ignored.
        """

    def run_finished(
        self,
        context: ExecutionContext,
        *,
        termination: Termination,
        elapsed_ms: int,
        grpc_overhead_ms: int,
    ) -> None:
        """Records nothing.

        Args:
            context: Ignored.
            termination: Ignored.
            elapsed_ms: Ignored.
            grpc_overhead_ms: Ignored.
        """

    def timeout(self, context: ExecutionContext) -> None:
        """Records nothing.

        Args:
            context: Ignored.
        """

    def cancellation(self, context: ExecutionContext) -> None:
        """Records nothing.

        Args:
            context: Ignored.
        """

    def retry(self, context: ExecutionContext, *, attempt: int) -> None:
        """Records nothing.

        Args:
            context: Ignored.
            attempt: Ignored.
        """

    def failover(self, context: ExecutionContext, *, from_model: str, to_model: str) -> None:
        """Records nothing.

        Args:
            context: Ignored.
            from_model: Ignored.
            to_model: Ignored.
        """

    def provider_failure(
        self, context: ExecutionContext, *, provider: str, error: BaseException
    ) -> None:
        """Records nothing.

        Args:
            context: Ignored.
            provider: Ignored.
            error: Ignored.
        """

    def authorization_failure(
        self, context: ExecutionContext | None, *, error: OrchestratorError
    ) -> None:
        """Records nothing.

        Args:
            context: Ignored.
            error: Ignored.
        """

    def tenant_mismatch(self, *, correlation_id: str, reason: str) -> None:
        """Records nothing.

        Args:
            correlation_id: Ignored.
            reason: Ignored.
        """

    def model_latency(self, *, provider: str, elapsed_ms: float) -> None:
        """Records nothing.

        Args:
            provider: Ignored.
            elapsed_ms: Ignored.
        """

    def tool_latency(self, *, tool_kind: str, elapsed_ms: float) -> None:
        """Records nothing.

        Args:
            tool_kind: Ignored.
            elapsed_ms: Ignored.
        """


@dataclass(slots=True)
class _Span:
    """Measures a block.

    Attributes:
        elapsed_ms: Milliseconds measured, valid after the block exits.
    """

    elapsed_ms: float = 0.0
    _started: float = field(default=0.0, repr=False)

    def __enter__(self) -> _Span:
        """Starts the clock.

        Returns:
            Itself.
        """
        self._started = time.monotonic()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Stops the clock.

        Args:
            exc_type: Ignored.
            exc: Ignored.
            tb: Ignored.
        """
        self.elapsed_ms = (time.monotonic() - self._started) * 1000


def _labels(**labels: str) -> dict[str, str]:
    """Drops empty labels.

    Args:
        **labels: Candidate labels.

    Returns:
        Only the non-empty ones. An absent dimension is not a dimension with an
        empty value, and the metrics layer refuses the latter.
    """
    return {key: value for key, value in labels.items() if value}


def _family(value: str) -> str:
    """Reduces an identifier to a bounded, safe label value.

    Args:
        value: A provider, model or tool identifier.

    Returns:
        The part before the first ``/`` — the provider or family — lowercased
        and clipped. Never empty: an empty label value is refused by the
        metrics layer, and a metric that fails to record would be worse than
        one recorded coarsely.
    """
    head = value.split("/", 1)[0].strip().lower()
    return head[:32] or "unknown"
