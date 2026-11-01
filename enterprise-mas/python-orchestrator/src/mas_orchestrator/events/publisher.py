"""Event publishing.

Publishes typed events to a broker through an injected transport. Two rules
govern it.

**Publishing must not fail an execution.** A broker outage is not an execution
failure, and raising here would turn one into the other. A failed publish is
recorded and reported; the execution carries on.

**A publish that cannot be retried is dead-lettered, not dropped.** Dropping
means the event is gone with no record that it existed. Dead-lettering leaves
something to reconcile later, which is the difference between a gap and a
known gap.

The publisher screens every payload before it leaves the process — the same
check the emitter applies — because this is the last point at which a
credential can still be caught.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Final, Protocol

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.events.models import (
    DeliveryRecord,
    DeliveryStatus,
    EventEnvelope,
    envelope_for,
)

if TYPE_CHECKING:
    from mas_orchestrator.events.dedup import EventDeduplicator
    from mas_orchestrator.events.types import ExecutionEvent

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Attempts before an event is dead-lettered.
MAX_PUBLISH_ATTEMPTS: Final[int] = 3


class EventTransport(Protocol):
    """Delivers an envelope to a broker."""

    async def publish(self, envelope: EventEnvelope) -> None:
        """Sends one envelope.

        Raises:
            Exception: On failure. The publisher classifies and retries.
        """
        ...


class LoggingTransport:
    """Writes envelopes to the log rather than a broker.

    For development. It states plainly that nothing left the process, so a
    deployment running this cannot mistake itself for one that publishes.
    """

    async def publish(self, envelope: EventEnvelope) -> None:
        """Logs the envelope."""
        _LOGGER.info(
            "event published (local only)",
            extra={
                "event": "events.published_local",
                "event_name": envelope.event_name,
                "routing_key": envelope.routing_key,
            },
        )


class CollectingTransport:
    """Retains envelopes in memory, for tests."""

    def __init__(self) -> None:
        self.envelopes: list[EventEnvelope] = []

    async def publish(self, envelope: EventEnvelope) -> None:
        """Records the envelope."""
        self.envelopes.append(envelope)


class FailingTransport:
    """Always fails. For resilience tests."""

    def __init__(self, *, failures_before_success: int = 0) -> None:
        self._remaining = failures_before_success
        self.attempts = 0

    async def publish(self, envelope: EventEnvelope) -> None:
        """Fails a configured number of times, then succeeds."""
        self.attempts += 1
        if self._remaining > 0:
            self._remaining -= 1
            raise RuntimeError("broker unavailable")
        return None


class EventPublisher:
    """Publishes events, screening and deduplicating them."""

    def __init__(
        self,
        transport: EventTransport | None = None,
        *,
        deduplicator: EventDeduplicator | None = None,
        max_attempts: int = MAX_PUBLISH_ATTEMPTS,
    ) -> None:
        if max_attempts < 1:
            raise InvalidRequestError(
                "max_attempts must be at least 1", details={"max_attempts": max_attempts}
            )
        self._transport = transport or LoggingTransport()
        self._dedup = deduplicator
        self._max_attempts = max_attempts
        self._records: dict[str, DeliveryRecord] = {}

    @property
    def records(self) -> dict[str, DeliveryRecord]:
        """What happened to each published event."""
        return dict(self._records)

    async def publish(self, event: ExecutionEvent) -> DeliveryRecord:
        """Publishes one event.

        Returns:
            A :class:`DeliveryRecord`. Never raises for a transport failure:
            the outcome is in the record.

        Raises:
            InvalidRequestError: If screening finds credential material in the
                payload. That is raised, not swallowed, because writing the
                event anywhere — even to a dead-letter queue — would spread the
                secret.
        """
        event_id = _event_id(event)

        if self._dedup is not None and self._dedup.is_duplicate(event):
            record = DeliveryRecord(event_id=event_id, status=DeliveryStatus.SKIPPED_DUPLICATE)
            self._records[event_id] = record
            return record

        _screen(event)
        envelope = envelope_for(event, event_id=event_id)

        last_error: str | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                await self._transport.publish(envelope)
            except Exception as error:
                last_error = type(error).__name__
                _LOGGER.warning(
                    "event publish failed",
                    extra={
                        "event": "events.publish_failed",
                        "event_name": envelope.event_name,
                        "attempt": attempt,
                        "error": last_error,
                    },
                )
                continue

            record = DeliveryRecord(
                event_id=event_id,
                status=DeliveryStatus.PUBLISHED,
                attempts=attempt,
                published_epoch_ms=int(time.time() * 1000),
            )
            self._records[event_id] = record
            return record

        record = DeliveryRecord(
            event_id=event_id,
            status=DeliveryStatus.DEAD_LETTERED,
            attempts=self._max_attempts,
            last_error=last_error,
        )
        self._records[event_id] = record
        _LOGGER.error(
            "event dead-lettered",
            extra={
                "event": "events.dead_lettered",
                "event_name": envelope.event_name,
                "attempts": self._max_attempts,
            },
        )
        return record

    async def publish_many(self, events: list[ExecutionEvent]) -> list[DeliveryRecord]:
        """Publishes several events, returning one record each."""
        return [await self.publish(event) for event in events]


def _event_id(event: ExecutionEvent) -> str:
    """A stable id for an event, derived from its identity.

    Derived rather than generated, so a redelivery of the same event produces
    the same id and a consumer can deduplicate without being given one.
    """
    import hashlib

    material = (
        f"{event.name.value}|{event.tenant_id}|{event.execution_id}|{event.occurred_epoch_ms}"
    )
    return f"evt-{hashlib.sha256(material.encode()).hexdigest()[:24]}"


def _screen(event: ExecutionEvent) -> None:
    """Refuses an event whose payload matches a credential pattern."""
    from mas_orchestrator.guardrails.pii import RedactSecretsOnly, scan

    for key, value in event.fields.items():
        if not isinstance(value, str):
            continue
        report = scan(value)
        kinds = {finding.kind for finding in report.findings}
        if kinds & RedactSecretsOnly.SECRET_KINDS:
            raise InvalidRequestError(
                "event payload contains what looks like a credential",
                details={
                    "event_name": event.name.value,
                    "field": key,
                    "kinds": sorted(kind.value for kind in kinds),
                },
            )
