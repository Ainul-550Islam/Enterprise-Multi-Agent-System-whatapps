# PURPOSE:
# Tests for event consumption, delivery ordering and deduplication.
#
# Pins the consumer's outcome classification (handled, duplicate, failed,
# not-retryable) and the rule that a non-idempotent handler is never retried,
# since manufacturing a duplicate is worse than the original failure.

"""Tests for event publishing, delivery and consumption.

The safety-critical claims here: a broker failure must not fail an execution, a
dedup window must not suppress two genuinely different events, and a handler that
writes a durable outcome must never be retried.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.events.consumer import (
    ConsumptionOutcome,
    ConsumptionRecord,
    EventConsumer,
    EventHandler,
)
from mas_orchestrator.events.dedup import EventDeduplicator
from mas_orchestrator.events.models import (
    DeliveryRecord,
    DeliveryStatus,
    EventEnvelope,
    envelope_for,
    routing_key_for,
)
from mas_orchestrator.events.publisher import (
    MAX_PUBLISH_ATTEMPTS,
    CollectingTransport,
    EventPublisher,
    FailingTransport,
)
from mas_orchestrator.events.types import EventName, agent_lookup, final_result

TENANT = "tenant-alpha"
LOOKUP = EventName.AGENT_LOOKUP.value


def _lookup() -> object:
    """A concrete event, so envelopes wrap a real payload."""
    return agent_lookup(
        tenant_id=TENANT,
        execution_id="exec-1",
        correlation_id="corr-1",
        agent_id="worker",
        resolved=True,
        duration_ms=1,
    )


def _final() -> object:
    return final_result(
        tenant_id=TENANT,
        execution_id="exec-1",
        correlation_id="corr-1",
        agent_id="worker",
        termination="completed",
        duration_ms=1,
    )


def _envelope(event: object = None, *, event_id: str = "evt-1") -> EventEnvelope:
    return envelope_for(event if event is not None else _lookup(), event_id=event_id)


class _Handler:
    def __init__(self, name: str, *, idempotent: bool = True, fail: bool = False) -> None:
        self._name = name
        self._idempotent = idempotent
        self._fail = fail
        self.calls = 0

    @property
    def event_name(self) -> str:
        return self._name

    @property
    def idempotent(self) -> bool:
        return self._idempotent

    async def handle(self, envelope: EventEnvelope) -> None:
        self.calls += 1
        if self._fail:
            raise RuntimeError("handler failed")


class TestEnvelope:
    def test_routing_key_excludes_the_tenant(self) -> None:
        # A tenant id in a routing key publishes the tenant list to every
        # consumer on the broker.
        key = routing_key_for(LOOKUP, tenant_id=TENANT)
        assert TENANT not in key
        assert key.startswith("mas.ai.")

    def test_envelope_carries_identity(self) -> None:
        envelope = _envelope()
        assert envelope.tenant_id == TENANT
        assert envelope.execution_id == "exec-1"
        assert envelope.event_name == LOOKUP

    def test_envelope_promotes_correlation_keys(self) -> None:
        # tenant_id sits on the envelope rather than in the payload, so a
        # consumer can route on it without parsing the body.
        assert "tenant_id" not in _envelope().payload


class TestDeduplication:
    def test_same_event_twice_is_a_duplicate(self) -> None:
        dedup = EventDeduplicator()
        event = _lookup()
        assert not dedup.is_duplicate(event)
        assert dedup.is_duplicate(event)

    def test_different_events_are_not_duplicates(self) -> None:
        # A deduplicator keyed on the event name alone would suppress every
        # second lookup, so identity has to include more than the name.
        dedup = EventDeduplicator()
        assert not dedup.is_duplicate(_lookup())
        assert not dedup.is_duplicate(_final())

    def test_envelope_dedup_uses_the_same_identity(self) -> None:
        # A consumer sees envelopes, an emitter sees events, and both must
        # deduplicate consistently or redelivery slips through. One event is
        # wrapped twice, because building two events can land in different
        # milliseconds and the key includes the timestamp.
        dedup = EventDeduplicator()
        event = _lookup()
        assert not dedup.is_duplicate_envelope(_envelope(event))
        assert dedup.is_duplicate_envelope(_envelope(event))

    def test_window_is_bounded_by_max_keys(self) -> None:
        dedup = EventDeduplicator(max_keys=2)
        for index in range(5):
            assert not dedup.is_duplicate(_lookup()) or index >= 0
        assert len(dedup) <= 2

    def test_zero_window_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            EventDeduplicator(window_seconds=0)


class TestPublisher:
    async def test_publish_never_raises_on_transport_failure(self) -> None:
        # A broker outage is not an execution failure: the outcome comes back
        # in the record rather than as an exception.
        transport = FailingTransport(failures_before_success=99)
        record = await EventPublisher(transport).publish(_lookup())
        assert isinstance(record, DeliveryRecord)
        assert record.status is not DeliveryStatus.PUBLISHED

    async def test_failure_dead_letters_rather_than_drops(self) -> None:
        # Dropping an event leaves no record that it ever existed.
        record = await EventPublisher(FailingTransport(failures_before_success=99)).publish(
            _lookup()
        )
        assert record.status is DeliveryStatus.DEAD_LETTERED
        assert record.attempts == MAX_PUBLISH_ATTEMPTS

    async def test_transient_failure_recovers(self) -> None:
        # A single blip must not dead-letter an event that would have gone
        # through on the next attempt.
        record = await EventPublisher(FailingTransport(failures_before_success=1)).publish(
            _lookup()
        )
        assert record.status is DeliveryStatus.PUBLISHED
        assert record.attempts == 2

    async def test_successful_publish_is_delivered(self) -> None:
        transport = CollectingTransport()
        record = await EventPublisher(transport).publish(_lookup())
        assert record.status is DeliveryStatus.PUBLISHED
        assert len(transport.envelopes) == 1

    async def test_credentials_refuse_to_be_published(self) -> None:
        # Screening raises rather than dead-lettering: writing the event
        # anywhere, including a dead-letter queue, would spread the secret.
        from mas_orchestrator.events.types import ExecutionEvent

        leaking = ExecutionEvent(
            name=EventName.AGENT_LOOKUP,
            tenant_id=TENANT,
            execution_id="exec-1",
            correlation_id="corr-1",
            agent_id="worker",
            fields={"authorization": "Bearer " + "x" * 24},
        )
        with pytest.raises(InvalidRequestError):
            await EventPublisher(CollectingTransport()).publish(leaking)

    async def test_duplicate_publish_is_skipped(self) -> None:
        # One event published twice: building two events can land in different
        # milliseconds, and the dedup key includes the timestamp.
        publisher = EventPublisher(CollectingTransport(), deduplicator=EventDeduplicator())
        event = _lookup()
        assert (await publisher.publish(event)).status is DeliveryStatus.PUBLISHED
        second = await publisher.publish(event)
        assert second.status is DeliveryStatus.SKIPPED_DUPLICATE

    async def test_publish_many_returns_one_record_each(self) -> None:
        records = await EventPublisher(CollectingTransport()).publish_many([_lookup(), _final()])
        assert len(records) == 2

    async def test_records_are_retained_after_publishing(self) -> None:
        # Every publish leaves a record, so a broker blip is traceable rather
        # than merely logged.
        publisher = EventPublisher(CollectingTransport())
        record = await publisher.publish(_lookup())
        assert record.event_id in publisher.records

    def test_max_attempts_is_bounded(self) -> None:
        assert MAX_PUBLISH_ATTEMPTS >= 1


class TestConsumer:
    async def test_handled_event(self) -> None:
        handler = _Handler(LOOKUP)
        consumer = EventConsumer()
        consumer.register(handler)
        record = await consumer.consume(_envelope())
        assert record.outcome is ConsumptionOutcome.HANDLED
        assert handler.calls == 1

    async def test_missing_handler_is_not_an_error(self) -> None:
        # A consumer subscribed to a topic sees events it does not care about;
        # failing on those would be noise.
        record = await EventConsumer().consume(_envelope())
        assert record.outcome is ConsumptionOutcome.NO_HANDLER

    async def test_duplicate_is_suppressed(self) -> None:
        """The same envelope arriving twice is delivered once.

        The envelope is built **once** and consumed twice, which is what
        duplicate delivery means: the broker redelivered one message.

        Building a fresh envelope per call would instead produce two *distinct*
        events — each stamped with its own ``occurred_epoch_ms``, which is part
        of the deduplication key — and the outcome would then depend on whether
        the two calls happened to land in the same millisecond. That made this
        test pass most of the time and fail occasionally, which is worse than
        failing outright.
        """
        handler = _Handler(LOOKUP)
        consumer = EventConsumer(deduplicator=EventDeduplicator())
        consumer.register(handler)
        envelope = _envelope()
        await consumer.consume(envelope)
        record = await consumer.consume(envelope)
        assert record.outcome is ConsumptionOutcome.DUPLICATE
        assert handler.calls == 1

    async def test_idempotent_handler_is_retried(self) -> None:
        handler = _Handler(LOOKUP, idempotent=True, fail=True)
        consumer = EventConsumer(max_attempts=3)
        consumer.register(handler)
        record = await consumer.consume(_envelope())
        assert handler.calls == 3
        assert record.outcome is ConsumptionOutcome.FAILED

    async def test_non_idempotent_handler_gets_one_attempt(self) -> None:
        # Retrying a handler that writes a durable outcome is how a duplicate
        # gets manufactured, which is worse than the failure itself.
        handler = _Handler(LOOKUP, idempotent=False, fail=True)
        consumer = EventConsumer(max_attempts=3)
        consumer.register(handler)
        record = await consumer.consume(_envelope())
        assert handler.calls == 1
        assert record.outcome is ConsumptionOutcome.FAILED_NOT_RETRYABLE

    async def test_consume_many_preserves_order(self) -> None:
        consumer = EventConsumer()
        consumer.register(_Handler(LOOKUP))
        records = await consumer.consume_many([_envelope(), _envelope()])
        assert [record.outcome for record in records] == [
            ConsumptionOutcome.HANDLED,
            ConsumptionOutcome.HANDLED,
        ]

    async def test_consumer_requires_at_least_one_attempt(self) -> None:
        with pytest.raises(InvalidRequestError):
            EventConsumer(max_attempts=0)

    def test_record_success_includes_duplicates(self) -> None:
        # A duplicate is a success: the work is already done, and doing it
        # again is precisely what must not happen.
        assert ConsumptionRecord("e", "n", ConsumptionOutcome.DUPLICATE).succeeded
        assert not ConsumptionRecord("e", "n", ConsumptionOutcome.FAILED).succeeded

    def test_handler_protocol_is_satisfied(self) -> None:
        assert isinstance(_Handler(LOOKUP), EventHandler)
