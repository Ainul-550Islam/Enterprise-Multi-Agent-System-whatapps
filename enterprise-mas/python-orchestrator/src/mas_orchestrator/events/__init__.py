"""Typed AI events.

Implements the observability half of spec §17. Every event is a frozen
dataclass with a stable :class:`~.types.EventName`, carrying identifiers,
counts and durations — and, structurally, nothing else. There is no field for a
prompt, a response body or a URL, so content cannot reach a log by accident.

The never-log rule is enforced twice: once by the event shapes, and again by
:func:`~.emitter._screen`, which refuses to emit an event whose payload matches
a credential pattern. Screening raises rather than silently redacting, because
a secret that nearly reached the telemetry path is worth an alarm.

Duplicate deliveries (spec §16) are suppressed by :class:`~.dedup.EventDeduplicator`
within a time window. That is best-effort by design and is not a competing
idempotency system: authoritative execution state belongs to Rust.

Publishing and consumption are separated from emission for the same reason. An
event describes something that happened; a :class:`~.models.DeliveryRecord`
describes the attempt to tell someone about it. Conflating them makes a broker
blip look like an execution failure.

:mod:`.consumer` refuses to retry a handler that declares itself
non-idempotent. Only the handler knows whether repeating its work is safe, and
retrying a handler that writes a durable outcome is how a duplicate gets
manufactured.
"""

from __future__ import annotations

from mas_orchestrator.events.consumer import (
    ConsumptionOutcome,
    ConsumptionRecord,
    EventConsumer,
    EventHandler,
)
from mas_orchestrator.events.dedup import (
    DEFAULT_MAX_KEYS,
    DEFAULT_WINDOW_SECONDS,
    EventDeduplicator,
)
from mas_orchestrator.events.emitter import (
    CollectingSink,
    EventEmitter,
    EventSink,
    LoggingSink,
    NullSink,
)
from mas_orchestrator.events.handlers import (
    FanoutHandler,
    FinalResultHandler,
    LoggingHandler,
    MetricsHandler,
    SinkHandler,
    UsageHandler,
    default_handlers,
)
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
    EventTransport,
    FailingTransport,
    LoggingTransport,
)
from mas_orchestrator.events.types import (
    MAX_EVENT_FIELDS,
    MAX_STRING_FIELD_CHARS,
    EventName,
    ExecutionEvent,
    agent_lookup,
    cancellation,
    duration_event,
    final_result,
    guardrail_decision,
    model_cost,
    model_request_duration,
    model_tokens,
    planning_duration,
    policy_denial,
    quota_denial,
    request_received,
    retry,
    tool_failure,
    tool_invocation_duration,
)

__all__ = [
    "DEFAULT_MAX_KEYS",
    "DEFAULT_WINDOW_SECONDS",
    "MAX_EVENT_FIELDS",
    "MAX_PUBLISH_ATTEMPTS",
    "MAX_STRING_FIELD_CHARS",
    "CollectingSink",
    "CollectingTransport",
    "ConsumptionOutcome",
    "ConsumptionRecord",
    "DeliveryRecord",
    "DeliveryStatus",
    "EventConsumer",
    "EventDeduplicator",
    "EventEmitter",
    "EventEnvelope",
    "EventHandler",
    "EventName",
    "EventPublisher",
    "EventSink",
    "EventTransport",
    "ExecutionEvent",
    "FailingTransport",
    "FanoutHandler",
    "FinalResultHandler",
    "LoggingHandler",
    "LoggingSink",
    "LoggingTransport",
    "MetricsHandler",
    "NullSink",
    "SinkHandler",
    "UsageHandler",
    "agent_lookup",
    "cancellation",
    "default_handlers",
    "duration_event",
    "envelope_for",
    "final_result",
    "guardrail_decision",
    "model_cost",
    "model_request_duration",
    "model_tokens",
    "planning_duration",
    "policy_denial",
    "quota_denial",
    "request_received",
    "retry",
    "routing_key_for",
    "tool_failure",
    "tool_invocation_duration",
]
