"""Event consumption.

Spec §16 requires that event handling be deduplicated and that retries must not
manufacture duplicate durable outcomes. Both are decided by the *handler*, not
by the consumer, because only the handler knows whether repeating its work is
safe. So the consumer's job is narrow and mechanical:

* hand each envelope to the handler registered for its event name;
* deduplicate by envelope id before doing so;
* classify a failure and decide whether to retry, using the handler's declared
  idempotency rather than a blanket policy;
* refuse to retry a handler that is not idempotent.

That last rule is the important one. Retrying a non-idempotent handler is how a
duplicate durable outcome gets manufactured, and the consumer will not do it —
it reports the failure instead, so the decision surfaces rather than being made
by default.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import InvalidRequestError

if TYPE_CHECKING:
    from mas_orchestrator.events.dedup import EventDeduplicator
    from mas_orchestrator.events.models import EventEnvelope

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class ConsumptionOutcome(StrEnum):
    """What happened to one envelope."""

    HANDLED = "handled"
    DUPLICATE = "duplicate"
    NO_HANDLER = "no_handler"
    FAILED = "failed"
    FAILED_NOT_RETRYABLE = "failed_not_retryable"


@dataclass(frozen=True, slots=True)
class ConsumptionRecord:
    """The outcome of consuming one envelope.

    Attributes:
        event_id: The envelope's event id.
        event_name: The event name.
        outcome: What happened.
        attempts: How many tries were made.
        error: Type name of the failure, when it failed. A type name rather
            than a message, which can echo content.
    """

    event_id: str
    event_name: str
    outcome: ConsumptionOutcome
    attempts: int = 1
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        """Whether the work is done.

        A duplicate counts as success: the work was already done, and doing it
        again is precisely what must not happen.
        """
        return self.outcome in (
            ConsumptionOutcome.HANDLED,
            ConsumptionOutcome.DUPLICATE,
        )


@runtime_checkable
class EventHandler(Protocol):
    """Handles one kind of event."""

    @property
    def event_name(self) -> str:
        """Which event this handler serves."""
        ...

    @property
    def idempotent(self) -> bool:
        """Whether repeating this handler is safe.

        A handler that writes a durable outcome must report ``False`` unless it
        is genuinely idempotent, because the consumer refuses to retry
        otherwise.
        """
        ...

    async def handle(self, envelope: EventEnvelope) -> None:
        """Processes one envelope.

        Raises:
            Exception: On failure. The consumer classifies it.
        """
        ...


class EventConsumer:
    """Dispatches envelopes to handlers."""

    def __init__(
        self,
        *,
        deduplicator: EventDeduplicator | None = None,
        max_attempts: int = 3,
    ) -> None:
        if max_attempts < 1:
            raise InvalidRequestError(
                "max_attempts must be at least 1", details={"max_attempts": max_attempts}
            )
        self._dedup = deduplicator
        self._max_attempts = max_attempts
        self._handlers: dict[str, EventHandler] = {}

    def register(self, handler: EventHandler) -> None:
        """Registers a handler for its event name."""
        self._handlers[handler.event_name] = handler

    def handler_for(self, event_name: str) -> EventHandler | None:
        """The handler for an event name, if one is registered."""
        return self._handlers.get(event_name)

    async def consume(self, envelope: EventEnvelope) -> ConsumptionRecord:
        """Handles one envelope.

        Returns:
            A :class:`ConsumptionRecord`. Never raises for a handler failure:
            the outcome is in the record, so one bad event cannot stop a batch.

        Raises:
            InvalidRequestError: If the envelope is malformed.
        """
        if not envelope.event_id.strip():
            raise InvalidRequestError(
                "an envelope requires an event id", details={"field": "event_id"}
            )

        if self._dedup is not None and self._dedup.is_duplicate_envelope(envelope):
            return ConsumptionRecord(
                event_id=envelope.event_id,
                event_name=envelope.event_name,
                outcome=ConsumptionOutcome.DUPLICATE,
            )

        handler = self._handlers.get(envelope.event_name)
        if handler is None:
            # Not an error: a consumer that subscribes to a topic will see
            # events it has no interest in, and failing on those would be noise.
            _LOGGER.debug(
                "no handler for event",
                extra={
                    "event": "events.no_handler",
                    "event_name": envelope.event_name,
                },
            )
            return ConsumptionRecord(
                event_id=envelope.event_id,
                event_name=envelope.event_name,
                outcome=ConsumptionOutcome.NO_HANDLER,
            )

        # A non-idempotent handler gets exactly one attempt. Retrying it could
        # manufacture a duplicate durable outcome, which is worse than the
        # failure it was meant to recover from.
        attempts_allowed = self._max_attempts if handler.idempotent else 1

        last_error: str | None = None
        for attempt in range(1, attempts_allowed + 1):
            started = time.monotonic()
            try:
                await handler.handle(envelope)
            except Exception as error:
                last_error = type(error).__name__
                _LOGGER.warning(
                    "event handling failed",
                    extra={
                        "event": "events.handle_failed",
                        "event_name": envelope.event_name,
                        "attempt": attempt,
                        "error": last_error,
                    },
                )
                continue

            duration = int((time.monotonic() - started) * 1000)
            _LOGGER.debug(
                "event handled",
                extra={
                    "event": "events.handled",
                    "event_name": envelope.event_name,
                    "duration_ms": duration,
                },
            )
            return ConsumptionRecord(
                event_id=envelope.event_id,
                event_name=envelope.event_name,
                outcome=ConsumptionOutcome.HANDLED,
                attempts=attempt,
            )

        not_retryable = not handler.idempotent
        return ConsumptionRecord(
            event_id=envelope.event_id,
            event_name=envelope.event_name,
            outcome=(
                ConsumptionOutcome.FAILED_NOT_RETRYABLE
                if not_retryable
                else ConsumptionOutcome.FAILED
            ),
            attempts=attempts_allowed,
            error=last_error,
        )

    async def consume_many(
        self,
        envelopes: list[EventEnvelope],
    ) -> list[ConsumptionRecord]:
        """Consumes several envelopes in order.

        Order is preserved. A batch that reorders events would make a
        consumer's view of an execution depend on arrival order.
        """
        return [await self.consume(envelope) for envelope in envelopes]
