"""Event envelope models.

:mod:`~mas_orchestrator.events.types` defines what an *AI* event is. This
module defines how an event travels: the envelope that carries it to a broker
or a gRPC stream, and the record of what happened to it on the way.

The distinction matters because delivery is not part of the event. An event
describes something that happened inside an execution; the envelope describes
the attempt to tell someone about it. Conflating them makes a delivery failure
look like an execution failure, which is how a healthy system gets paged for a
broker blip.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from mas_orchestrator.contracts.errors import InvalidRequestError


class DeliveryStatus(StrEnum):
    """Where an event got to."""

    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"
    SKIPPED_DUPLICATE = "skipped_duplicate"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """One event, wrapped for transport.

    Attributes:
        event_id: Stable identifier for this delivery attempt's subject. Two
            envelopes for the same event carry the same id, which is what lets
            a consumer deduplicate without inspecting the payload.
        routing_key: Where it goes.
        event_name: The event's stable name.
        payload: The flattened, already-screened event fields.
        tenant_id: Owning tenant.
        execution_id: Execution that produced it.
        correlation_id: Correlation identifier.
        occurred_epoch_ms: When the event happened, not when it was sent.
        schema_version: Envelope format version.
    """

    event_id: str
    routing_key: str
    event_name: str
    payload: dict[str, Any] = field(default_factory=dict)
    tenant_id: str = ""
    execution_id: str = ""
    correlation_id: str = ""
    occurred_epoch_ms: int = 0
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise InvalidRequestError(
                "an envelope requires an event id", details={"field": "event_id"}
            )
        if not self.routing_key.strip():
            raise InvalidRequestError(
                "an envelope requires a routing key", details={"field": "routing_key"}
            )
        if not self.occurred_epoch_ms:
            object.__setattr__(self, "occurred_epoch_ms", int(time.time() * 1000))


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    """What happened to one delivery attempt.

    Attributes:
        event_id: The event.
        status: Where it got to.
        attempts: How many tries were made.
        last_error: Type name of the last failure, when it failed. A type name
            rather than a message: a message can echo content that must not
            reach a log.
        published_epoch_ms: When it landed, on success.
    """

    event_id: str
    status: DeliveryStatus
    attempts: int = 1
    last_error: str | None = None
    published_epoch_ms: int = 0

    @property
    def landed(self) -> bool:
        """Whether the event reached its destination."""
        return self.status is DeliveryStatus.PUBLISHED

    @property
    def gave_up(self) -> bool:
        """Whether no further attempt will be made."""
        return self.status is DeliveryStatus.DEAD_LETTERED


def routing_key_for(event_name: str, *, tenant_id: str = "") -> str:
    """Builds a routing key.

    The tenant is excluded by default. Putting a tenant id in a routing key
    publishes the tenant list to every broker consumer, and makes per-tenant
    routing decisions visible in broker metadata.
    """
    return f"mas.ai.{event_name}"


def envelope_for(event: Any, *, event_id: str) -> EventEnvelope:
    """Wraps an :class:`~mas_orchestrator.events.types.ExecutionEvent`."""
    fields = event.to_log_fields()
    return EventEnvelope(
        event_id=event_id,
        routing_key=routing_key_for(str(event.name.value)),
        event_name=str(event.name.value),
        payload={k: v for k, v in fields.items() if k not in _CORRELATION_KEYS},
        tenant_id=str(event.tenant_id),
        execution_id=str(event.execution_id),
        correlation_id=str(event.correlation_id),
        occurred_epoch_ms=int(event.occurred_epoch_ms),
    )


#: Fields promoted onto the envelope rather than left in the payload, so a
#: consumer can route on them without parsing.
_CORRELATION_KEYS: frozenset[str] = frozenset(
    {"event", "tenant_id", "execution_id", "correlation_id", "occurred_epoch_ms"}
)
