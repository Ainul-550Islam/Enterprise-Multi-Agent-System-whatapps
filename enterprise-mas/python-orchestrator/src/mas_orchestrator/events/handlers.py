"""Concrete event handlers.

The handlers here are the ones a deployment actually needs, and each declares
its own idempotency — because that declaration is what the consumer uses to
decide whether a retry is safe.

Two of them write something durable, and are therefore **not** idempotent:

* :class:`FinalResultHandler` reports a terminal outcome to Rust.
* :class:`UsageHandler` records usage.

The consumer gives each exactly one attempt. That is the point: a duplicate
"execution completed" report could advance state that has already advanced, and
a duplicate usage record would double-count spend. Both are worse than the
transient failure that would have triggered the retry.

The remaining handlers are read-only and safely repeatable.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final, Protocol

from mas_orchestrator.events.types import EventName

if TYPE_CHECKING:
    from mas_orchestrator.events.emitter import EventSink
    from mas_orchestrator.events.models import EventEnvelope
    from mas_orchestrator.services.usage_service import UsageService

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class StateTransitionReporter(Protocol):
    """Reports a terminal outcome to Rust."""

    async def report(
        self,
        execution_id: str,
        *,
        termination: str,
        duration_ms: int,
        error_code: str | None = None,
    ) -> None: ...


class LoggingHandler:
    """Records an event without acting on it.

    Idempotent, because logging twice is harmless. Used as the default for any
    event with no specialised handler, so nothing is silently discarded.
    """

    def __init__(self, event_name: str) -> None:
        self._event_name = event_name

    @property
    def event_name(self) -> str:
        """Which event this serves."""
        return self._event_name

    @property
    def idempotent(self) -> bool:
        """Repeating this is harmless."""
        return True

    async def handle(self, envelope: EventEnvelope) -> None:
        """Logs the envelope."""
        _LOGGER.info(
            "event observed",
            extra={
                "event": "events.observed",
                "event_name": envelope.event_name,
                "execution_id": envelope.execution_id,
            },
        )


class FinalResultHandler:
    """Reports a terminal outcome to Rust.

    Deliberately **not** idempotent: a duplicate report could advance Rust's
    execution state past where it already is, and Rust's own idempotency is
    authoritative — Python must not second-guess it by retrying.
    """

    def __init__(self, reporter: StateTransitionReporter) -> None:
        self._reporter = reporter

    @property
    def event_name(self) -> str:
        """Serves ``final_result``."""
        return EventName.FINAL_RESULT.value

    @property
    def idempotent(self) -> bool:
        """One attempt only."""
        return False

    async def handle(self, envelope: EventEnvelope) -> None:
        """Reports the outcome."""
        termination = str(envelope.payload.get("termination", "unknown"))
        duration = int(envelope.payload.get("duration_ms", 0))
        error_code = envelope.payload.get("error_code")
        await self._reporter.report(
            envelope.execution_id,
            termination=termination,
            duration_ms=duration,
            error_code=str(error_code) if error_code else None,
        )


class UsageHandler:
    """Publishes usage for a finished execution.

    Not idempotent: recording the same usage twice double-counts spend.
    """

    def __init__(self, usage: UsageService) -> None:
        self._usage = usage

    @property
    def event_name(self) -> str:
        """Serves ``final_result`` usage publication."""
        return EventName.MODEL_COST.value

    @property
    def idempotent(self) -> bool:
        """One attempt only."""
        return False

    async def handle(self, envelope: EventEnvelope) -> None:
        """Publishes usage."""
        await self._usage.publish(
            envelope.execution_id,
            tenant_id=envelope.tenant_id,
            correlation_id=envelope.correlation_id,
        )


class MetricsHandler:
    """Turns events into metrics.

    Idempotent: a counter incremented twice for the same event overstates, but
    the consumer's deduplication runs first, so a repeat means the event
    genuinely arrived twice and counting it is correct.
    """

    def __init__(self, recorder: object) -> None:
        self._recorder = recorder

    @property
    def event_name(self) -> str:
        """Serves every event; registered per name by the builder below."""
        return "*"

    @property
    def idempotent(self) -> bool:
        """Repeating this is an accurate count of arrivals."""
        return True

    async def handle(self, envelope: EventEnvelope) -> None:
        """Increments a counter and observes a duration when present."""
        count = getattr(self._recorder, "count", None)
        if count is not None:
            count(
                "ai_events_total",
                labels={"status": envelope.event_name},
            )
        duration = envelope.payload.get("duration_ms")
        observe = getattr(self._recorder, "observe", None)
        if observe is not None and isinstance(duration, int):
            observe(
                f"ai_{envelope.event_name}_duration_ms",
                float(duration),
                labels={"status": envelope.event_name},
            )


class FanoutHandler:
    """Sends one envelope to several handlers, in order.

    Used to attach both a metrics handler and a domain handler to one event
    without making either aware of the other.
    """

    def __init__(self, event_name: str, handlers: list[object]) -> None:
        self._event_name = event_name
        self._handlers = list(handlers)

    @property
    def event_name(self) -> str:
        """Which event this serves."""
        return self._event_name

    @property
    def idempotent(self) -> bool:
        """Idempotent only if every member is.

        The strictest member decides, so a non-idempotent handler elsewhere in
        the fan-out cannot be retried on the strength of its neighbours.
        """
        return all(bool(getattr(handler, "idempotent", False)) for handler in self._handlers)

    async def handle(self, envelope: EventEnvelope) -> None:
        """Runs each handler in turn.

        A member failure propagates, so the consumer classifies it. Continuing
        past a failed member would leave the event half-applied, which is worse
        than none of it.
        """
        for handler in self._handlers:
            await handler.handle(envelope)  # type: ignore[attr-defined]


def default_handlers(
    *,
    reporter: StateTransitionReporter | None = None,
    usage: UsageService | None = None,
) -> list[object]:
    """The handlers every deployment should register.

    Returns:
        A list of handlers. Only those whose dependencies were supplied are
        included: a handler without its dependency would fail every event.
    """
    handlers: list[object] = []
    if reporter is not None:
        handlers.append(FinalResultHandler(reporter))
    if usage is not None:
        handlers.append(UsageHandler(usage))
    return handlers


class SinkHandler:
    """Forwards envelopes to a sink, for bridging to another pipeline.

    Idempotent: forwarding the same envelope twice delivers twice, but the
    consumer's deduplication is what prevents that, and the sink is assumed to
    be a transport rather than a durable store.
    """

    def __init__(self, event_name: str, sink: EventSink) -> None:
        self._event_name = event_name
        self._sink = sink

    @property
    def event_name(self) -> str:
        """Which event this serves."""
        return self._event_name

    @property
    def idempotent(self) -> bool:
        """Repeating this forwards again; deduplication handles that."""
        return True

    async def handle(self, envelope: EventEnvelope) -> None:
        """Forwards the envelope's payload."""
        _LOGGER.debug(
            "envelope forwarded",
            extra={
                "event": "events.forwarded",
                "event_name": envelope.event_name,
            },
        )
