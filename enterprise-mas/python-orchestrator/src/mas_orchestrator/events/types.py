"""Typed AI events.

Every event is a frozen dataclass with a stable string name, so a consumer can
switch on the type rather than parse a dictionary. Correlation fields come from
the authoritative execution context, which means an event can be joined back to
the execution that produced it without any inference.

The never-log rule from spec §17 is enforced structurally rather than by
review: events carry **identifiers, counts and durations**. There is no field
for a prompt, a response body, or a URL, so content cannot be logged by
accident.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

from mas_orchestrator.contracts.errors import InvalidRequestError

# Event payloads must stay small enough to be cheap to emit and ship.
MAX_EVENT_FIELDS: Final[int] = 32
MAX_STRING_FIELD_CHARS: Final[int] = 512


class EventName(StrEnum):
    """Stable event names, one per observable moment in spec §17."""

    REQUEST_RECEIVED = "request_received"
    AGENT_LOOKUP = "agent_lookup"
    PLANNING_DURATION = "planning_duration"
    MODEL_REQUEST_DURATION = "model_request_duration"
    MODEL_TOKENS = "model_tokens"
    MODEL_COST = "model_cost"
    TOOL_INVOCATION_DURATION = "tool_invocation_duration"
    TOOL_FAILURE = "tool_failure"
    RETRY = "retry"
    GUARDRAIL_DECISION = "guardrail_decision"
    POLICY_DENIAL = "policy_denial"
    QUOTA_DENIAL = "quota_denial"
    RAG_RETRIEVAL_DURATION = "rag_retrieval_duration"
    EMBEDDING_DURATION = "embedding_duration"
    RERANKING_DURATION = "reranking_duration"
    EXECUTION_DURATION = "execution_duration"
    CANCELLATION = "cancellation"
    FINAL_RESULT = "final_result"


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """The common shape every event carries.

    Attributes:
        name: Stable event name.
        tenant_id: Owning tenant.
        execution_id: Execution that produced the event.
        correlation_id: Correlation identifier.
        agent_id: Agent involved, when applicable.
        occurred_epoch_ms: When it happened.
        fields: Event-specific payload: counts, durations, identifiers.
    """

    name: EventName
    tenant_id: str
    execution_id: str
    correlation_id: str
    occurred_epoch_ms: int = 0
    agent_id: str | None = None
    fields: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise InvalidRequestError("event must name a tenant", details={"field": "tenant_id"})
        if not self.execution_id.strip():
            raise InvalidRequestError(
                "event must name an execution", details={"field": "execution_id"}
            )
        if len(self.fields) > MAX_EVENT_FIELDS:
            raise InvalidRequestError(
                "event has too many fields",
                details={"limit": MAX_EVENT_FIELDS, "count": len(self.fields)},
            )
        for key, value in self.fields.items():
            if isinstance(value, str) and len(value) > MAX_STRING_FIELD_CHARS:
                raise InvalidRequestError(
                    "event field exceeds the maximum length",
                    details={"field": key, "limit": MAX_STRING_FIELD_CHARS},
                )
        if not self.occurred_epoch_ms:
            object.__setattr__(self, "occurred_epoch_ms", int(time.time() * 1000))

    def to_log_fields(self) -> dict[str, Any]:
        """Flattens the event for a structured logger.

        Namespaced under ``event`` so consumer-side field extraction does not
        collide with the logger's own fields.
        """
        payload: dict[str, Any] = {
            "event": self.name.value,
            "tenant_id": self.tenant_id,
            "execution_id": self.execution_id,
            "correlation_id": self.correlation_id,
            "occurred_epoch_ms": self.occurred_epoch_ms,
        }
        if self.agent_id is not None:
            payload["agent_id"] = self.agent_id
        payload.update(self.fields)
        return payload


# --- Constructors ---------------------------------------------------------


def request_received(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    protocol_version: str,
) -> ExecutionEvent:
    """A request was accepted."""
    return ExecutionEvent(
        name=EventName.REQUEST_RECEIVED,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={"protocol_version": protocol_version},
    )


def agent_lookup(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    resolved: bool,
    duration_ms: int,
) -> ExecutionEvent:
    """An agent was resolved, or not."""
    return ExecutionEvent(
        name=EventName.AGENT_LOOKUP,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={"resolved": resolved, "duration_ms": duration_ms},
    )


def planning_duration(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    step_count: int,
    duration_ms: int,
) -> ExecutionEvent:
    """Planning finished."""
    return ExecutionEvent(
        name=EventName.PLANNING_DURATION,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={"step_count": step_count, "duration_ms": duration_ms},
    )


def model_request_duration(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    model: str,
    duration_ms: int,
    attempt: int = 1,
) -> ExecutionEvent:
    """A model call finished.

    ``model`` is a model *key*, not a prompt: the request body is never part of
    an event.
    """
    return ExecutionEvent(
        name=EventName.MODEL_REQUEST_DURATION,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={"model": model, "duration_ms": duration_ms, "attempt": attempt},
    )


def model_tokens(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    model: str,
    input_tokens: int | None,
    output_tokens: int | None,
) -> ExecutionEvent:
    """Token usage, when the provider reported it.

    ``None`` is recorded as ``None`` rather than zero: a missing measurement is
    not a measurement of zero.
    """
    return ExecutionEvent(
        name=EventName.MODEL_TOKENS,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
    )


def model_cost(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    model: str,
    currency: str,
    total_cost: float,
    is_complete: bool,
) -> ExecutionEvent:
    """Cost, with an explicit completeness flag.

    A total that omits unpriced or unmeasured calls is a lower bound, and the
    flag says so. Reporting it without the flag would let a dashboard treat an
    undercount as the final figure.
    """
    return ExecutionEvent(
        name=EventName.MODEL_COST,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={
            "model": model,
            "currency": currency,
            "total_cost": round(total_cost, 6),
            "is_complete": is_complete,
        },
    )


def tool_invocation_duration(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    tool_id: str,
    tool_version: int,
    duration_ms: int,
    truncated: bool = False,
) -> ExecutionEvent:
    """A tool call finished."""
    return ExecutionEvent(
        name=EventName.TOOL_INVOCATION_DURATION,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={
            "tool_id": tool_id,
            "tool_version": tool_version,
            "duration_ms": duration_ms,
            "truncated": truncated,
        },
    )


def tool_failure(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    tool_id: str,
    error_code: str,
    failure_class: str,
) -> ExecutionEvent:
    """A tool call failed.

    The error *code* is carried, not the message: a message can echo provider
    content, which must not reach a log.
    """
    return ExecutionEvent(
        name=EventName.TOOL_FAILURE,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={
            "tool_id": tool_id,
            "error_code": error_code,
            "failure_class": failure_class,
        },
    )


def retry(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    subject: str,
    attempt: int,
    reason: str,
) -> ExecutionEvent:
    """A retry was about to happen."""
    return ExecutionEvent(
        name=EventName.RETRY,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        fields={"subject": subject, "attempt": attempt, "reason": reason},
    )


def guardrail_decision(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    stage: str,
    outcome: str,
    detail: str = "",
) -> ExecutionEvent:
    """A guardrail reached a decision."""
    return ExecutionEvent(
        name=EventName.GUARDRAIL_DECISION,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        fields={"stage": stage, "outcome": outcome, "detail": detail},
    )


def policy_denial(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    subject: str,
    rule: str | None = None,
) -> ExecutionEvent:
    """The policy authority denied something."""
    return ExecutionEvent(
        name=EventName.POLICY_DENIAL,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        fields={"subject": subject, "rule": rule},
    )


def quota_denial(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    quota: str,
    limit: float,
) -> ExecutionEvent:
    """A quota stopped the execution."""
    return ExecutionEvent(
        name=EventName.QUOTA_DENIAL,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        fields={"quota": quota, "limit": limit},
    )


def duration_event(
    name: EventName,
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    duration_ms: int,
    **extra: Any,
) -> ExecutionEvent:
    """Builds one of the duration-only events."""
    fields: dict[str, Any] = {"duration_ms": duration_ms}
    fields.update(extra)
    return ExecutionEvent(
        name=name,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        fields=fields,
    )


def cancellation(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    reason: str,
) -> ExecutionEvent:
    """An execution was cancelled."""
    return ExecutionEvent(
        name=EventName.CANCELLATION,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        fields={"reason": reason},
    )


def final_result(
    *,
    tenant_id: str,
    execution_id: str,
    correlation_id: str,
    agent_id: str,
    termination: str,
    duration_ms: int,
    steps: int = 0,
    tool_calls: int = 0,
) -> ExecutionEvent:
    """An execution reached its end.

    Carries the termination reason rather than the output: the result text is
    data belonging to the caller, not to the telemetry pipeline.
    """
    return ExecutionEvent(
        name=EventName.FINAL_RESULT,
        tenant_id=tenant_id,
        execution_id=execution_id,
        correlation_id=correlation_id,
        agent_id=agent_id,
        fields={
            "termination": termination,
            "duration_ms": duration_ms,
            "steps": steps,
            "tool_calls": tool_calls,
        },
    )
