"""Event emission.

Events go to a sink. The default sink writes structured log lines through
``logging``, which keeps the common case dependency-free; a deployment that
needs a broker injects a different sink.

The emitter's second job is to make sure the never-log rule (spec §17) holds at
the point of emission, not merely at the point of construction. It re-checks
the payload for credential shapes before anything is written, because the cost
of one more scan is far below the cost of a key in a log index.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import InvalidRequestError

if TYPE_CHECKING:
    from mas_orchestrator.events.types import ExecutionEvent

_LOGGER: Final[logging.Logger] = logging.getLogger("mas_orchestrator.events")


@runtime_checkable
class EventSink(Protocol):
    """Where events go."""

    async def emit(self, event: ExecutionEvent) -> None:
        """Delivers one event.

        Implementations must not raise for an undeliverable event: telemetry
        failure must never fail an execution.
        """
        ...


class LoggingSink:
    """Writes events as structured log records."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or _LOGGER

    async def emit(self, event: ExecutionEvent) -> None:
        """Writes one event."""
        self._logger.info("%s", event.name.value, extra=event.to_log_fields())


class NullSink:
    """Discards events.

    Used when telemetry is deliberately off. Swallowing rather than failing is
    the right default: an execution that cannot report itself should still
    complete.
    """

    async def emit(self, event: ExecutionEvent) -> None:
        """Discards the event."""
        return None


class CollectingSink:
    """Retains events in memory.

    Intended for tests and local inspection. It is not a durable buffer and
    must not be used as one: everything it holds is lost on restart.
    """

    def __init__(self) -> None:
        self.events: list[ExecutionEvent] = []

    async def emit(self, event: ExecutionEvent) -> None:
        """Records the event."""
        self.events.append(event)

    def names(self) -> list[str]:
        """Names of collected events, in order."""
        return [event.name.value for event in self.events]

    def clear(self) -> None:
        """Forgets all collected events."""
        self.events.clear()


class EventEmitter:
    """Emits events, screening them and suppressing duplicates."""

    def __init__(
        self,
        sink: EventSink | None = None,
        *,
        deduplicator: object = None,
        screen: bool = True,
    ) -> None:
        self._sink = sink or LoggingSink()
        self._dedup = deduplicator
        self._screen = screen

    async def emit(self, event: ExecutionEvent) -> bool:
        """Emits one event if it is not a duplicate.

        Returns:
            Whether the event was emitted. ``False`` means it was suppressed
            as a duplicate.

        Raises:
            InvalidRequestError: If screening finds credential material. This
                is raised rather than swallowed, because the alternative is
                writing the event with the secret redacted and no signal that
                a secret was ever near the telemetry path.
        """
        if self._dedup is not None and self._dedup.is_duplicate(event):  # type: ignore[attr-defined]
            return False

        if self._screen:
            _screen(event)

        try:
            await self._sink.emit(event)
        except Exception:
            _LOGGER.warning("event sink failed", extra={"event": "events.sink_failed"})
        return True

    async def emit_many(self, events: list[ExecutionEvent]) -> int:
        """Emits several events, returning how many were sent."""
        sent = 0
        for event in events:
            if await self.emit(event):
                sent += 1
        return sent


def _screen(event: ExecutionEvent) -> None:
    """Refuses an event whose payload looks like it holds a credential.

    Raises:
        InvalidRequestError: If a secret shape is detected in any field.
    """
    from mas_orchestrator.guardrails.pii import scan

    for key, value in event.fields.items():
        if not isinstance(value, str):
            continue
        report = scan(value)
        if not report.detected:
            continue
        kinds = {finding.kind for finding in report.findings}
        from mas_orchestrator.guardrails.pii import RedactSecretsOnly

        if kinds & RedactSecretsOnly.SECRET_KINDS:
            raise InvalidRequestError(
                "event payload contains what looks like a credential",
                details={
                    "event": event.name.value,
                    "field": key,
                    "kinds": sorted(kind.value for kind in kinds),
                },
            )
