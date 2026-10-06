"""Distributed tracing.

Spans carry identifiers only. Prompts, responses, document content and URLs are
excluded by construction — the same rule spec §17 applies to metrics, and for
the same reason: a trace store is usually easier to reach than the data it
describes.

The default exporter discards. A deployment with an OTLP collector injects a
real exporter; nothing here pretends that discarding is equivalent.
"""

from __future__ import annotations

import contextvars
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final, Protocol

from mas_orchestrator.contracts.errors import InvalidRequestError

#: Span attributes are capped so a runaway caller cannot make a span unusable.
MAX_SPAN_ATTRIBUTES: Final[int] = 32
MAX_ATTRIBUTE_CHARS: Final[int] = 256

_current_span: contextvars.ContextVar[Span | None] = contextvars.ContextVar(
    "mas_current_span", default=None
)


class SpanStatus(StrEnum):
    """How a span ended."""

    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class TraceContext:
    """W3C trace context, propagated across the Rust/Python boundary.

    Attributes:
        trace_id: 32 lowercase hex characters.
        span_id: 16 lowercase hex characters.
        sampled: Whether this trace is being recorded.
    """

    trace_id: str
    span_id: str
    sampled: bool = False

    def __post_init__(self) -> None:
        if len(self.trace_id) != 32 or not _is_hex(self.trace_id):
            raise InvalidRequestError(
                "trace_id must be 32 lowercase hex characters",
                details={"field": "trace_id"},
            )
        if len(self.span_id) != 16 or not _is_hex(self.span_id):
            raise InvalidRequestError(
                "span_id must be 16 lowercase hex characters",
                details={"field": "span_id"},
            )

    def to_headers(self) -> dict[str, str]:
        """Renders the W3C ``traceparent`` header."""
        flags = "01" if self.sampled else "00"
        return {"traceparent": f"00-{self.trace_id}-{self.span_id}-{flags}"}


def _is_hex(value: str) -> bool:
    """Whether a string is lowercase hexadecimal."""
    if not value:
        return False
    allowed = set("0123456789abcdef")
    return all(char in allowed for char in value)


class Span:
    """One unit of work.

    Use as a context manager; the status and duration are recorded on exit.
    """

    def __init__(
        self,
        name: str,
        *,
        attributes: dict[str, Any] | None = None,
        exporter: SpanExporter | None = None,
    ) -> None:
        if not name.strip():
            raise InvalidRequestError("a span requires a name", details={"field": "name"})
        self.name = name
        self.attributes: dict[str, Any] = {}
        if attributes:
            for key, value in attributes.items():
                self.set_attribute(key, value)
        self.status: SpanStatus = SpanStatus.UNSET
        self.error: str | None = None
        self.start_epoch_ms: int = 0
        self.duration_ms: int = 0
        self.trace: TraceContext | None = None
        self._token: Any = None
        self._exporter = exporter

    def set_attribute(self, key: str, value: Any) -> None:
        """Adds an attribute.

        Raises:
            InvalidRequestError: If the attribute limit is exceeded or the
                value is a string longer than the cap. Content-bearing values
                are rejected rather than truncated, because a truncated
                prompt is still a prompt.
        """
        if len(self.attributes) >= MAX_SPAN_ATTRIBUTES and key not in self.attributes:
            raise InvalidRequestError(
                "span has too many attributes",
                details={"span": self.name, "limit": MAX_SPAN_ATTRIBUTES},
            )
        if isinstance(value, str) and len(value) > MAX_ATTRIBUTE_CHARS:
            raise InvalidRequestError(
                "span attribute value is too long: spans carry identifiers, not content",
                details={"span": self.name, "key": key, "limit": MAX_ATTRIBUTE_CHARS},
            )
        self.attributes[key] = value

    def record_error(self, message: str) -> None:
        """Marks the span as failed with a message."""
        self.status = SpanStatus.ERROR
        self.error = message

    def __enter__(self) -> Span:
        self.start_epoch_ms = int(time.time() * 1000)
        parent = _current_span.get()
        self.trace = TraceContext(
            trace_id=(parent.trace.trace_id if parent and parent.trace else _new_id(32)),
            span_id=_new_id(16),
            sampled=parent.trace.sampled if parent and parent.trace else False,
        )
        self._token = _current_span.set(self)
        return self

    def __exit__(self, *exc: object) -> None:
        if self.status is SpanStatus.UNSET:
            self.status = SpanStatus.ERROR if any(exc) else SpanStatus.OK
        self.duration_ms = max(0, int(time.time() * 1000) - self.start_epoch_ms)
        _current_span.reset(self._token)
        # Exporting on exit rather than on an explicit call means a caller that
        # forgets to export still produces a span.
        if self._exporter is not None:
            self._exporter.export(self)


def _new_id(length: int) -> str:
    """Generates a lowercase hex identifier."""
    import secrets

    return secrets.token_hex(length // 2)


def current_span() -> Span | None:
    """The active span, if any."""
    return _current_span.get()


class SpanExporter(Protocol):
    """Where finished spans go."""

    def export(self, span: Span) -> None: ...


class DiscardingExporter:
    """Discards spans.

    The safe default: tracing is opt-in, and a system that claims to trace
    while discarding is worse than one that admits it does not.
    """

    def export(self, span: Span) -> None:
        """Discards the span."""
        return None


class CollectingExporter:
    """Retains spans in memory, for tests and local debugging."""

    def __init__(self) -> None:
        self.spans: list[Span] = []

    def export(self, span: Span) -> None:
        """Records the span."""
        self.spans.append(span)

    def names(self) -> list[str]:
        """Names of exported spans, in order."""
        return [span.name for span in self.spans]


@dataclass(slots=True)
class Tracer:
    """Creates and exports spans."""

    exporter: SpanExporter = field(default_factory=DiscardingExporter)
    sample_ratio: float = 0.0

    def span(self, name: str, **attributes: Any) -> Span:
        """Creates a span that exports itself when the context exits."""
        created = Span(
            name,
            attributes=attributes or None,
            exporter=self.exporter if self.sample_ratio > 0.0 else None,
        )
        created.trace = TraceContext(
            trace_id=_new_id(32),
            span_id=_new_id(16),
            sampled=self.sample_ratio > 0.0,
        )
        return created
