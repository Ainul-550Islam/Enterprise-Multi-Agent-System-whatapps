"""Structured logging.

Spec §17 asks for correlation context on every log and forbids a list of
things that must never appear in one. This module makes both structural:

* :class:`CorrelationFilter` attaches the correlation context to every record,
  so a caller cannot forget it.
* :class:`RedactingFilter` screens the rendered message and every field for
  credential shapes before the record reaches a handler.

Redaction is applied at the *filter* layer rather than in each call site,
because a rule enforced by convention is a rule that holds until someone is in
a hurry.

Note the module is named ``logging`` and shadows the standard library inside
the package. Every internal import therefore uses absolute imports
(``import logging`` at module scope still resolves to the standard library
because Python 3 uses absolute imports by default), and callers outside the
package should import ``mas_orchestrator.observability.logging`` explicitly.
"""

from __future__ import annotations

import contextvars
import json
import logging as _logging
import sys
from dataclasses import dataclass
from typing import Any, Final

# --- correlation context ----------------------------------------------------

_correlation_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "mas_correlation_id", default=""
)
_execution_id: contextvars.ContextVar[str] = contextvars.ContextVar("mas_execution_id", default="")
_tenant_id: contextvars.ContextVar[str] = contextvars.ContextVar("mas_tenant_id", default="")


def bind_context(
    *,
    correlation_id: str = "",
    execution_id: str = "",
    tenant_id: str = "",
) -> None:
    """Binds correlation values to the current context."""
    _correlation_id.set(correlation_id)
    _execution_id.set(execution_id)
    _tenant_id.set(tenant_id)


def clear_context() -> None:
    """Unbinds the correlation values."""
    _correlation_id.set("")
    _execution_id.set("")
    _tenant_id.set("")


def current_context() -> dict[str, str]:
    """The bound context, for propagating into another task or thread."""
    return {
        "correlation_id": _correlation_id.get(),
        "execution_id": _execution_id.get(),
        "tenant_id": _tenant_id.get(),
    }


# --- filters ----------------------------------------------------------------


class CorrelationFilter(_logging.Filter):
    """Attaches the bound correlation context to every record."""

    def filter(self, record: _logging.LogRecord) -> bool:
        """Adds the context fields, without overwriting explicit ones.

        An explicit ``extra`` value wins: a log line that names a different
        execution than the bound one is making a deliberate statement, and
        silently overwriting it would hide a real mismatch.
        """
        record.correlation_id = getattr(record, "correlation_id", "") or _correlation_id.get()
        record.execution_id = getattr(record, "execution_id", "") or _execution_id.get()
        record.tenant_id = getattr(record, "tenant_id", "") or _tenant_id.get()
        return True


class RedactingFilter(_logging.Filter):
    """Screens a record for credential material before it is emitted.

    The record is *redacted in place* rather than dropped: losing the log line
    entirely would make an incident harder to diagnose, while removing the
    secret keeps the diagnostic and removes the harm.
    """

    def __init__(self, *, enabled: bool = True) -> None:
        super().__init__()
        self._enabled = enabled

    def filter(self, record: _logging.LogRecord) -> bool:
        """Screens and rewrites the record."""
        if not self._enabled:
            return True

        from mas_orchestrator.security.redaction import redact_text, redact_value

        record.msg = redact_text(str(record.msg))
        if record.args:
            if isinstance(record.args, dict):
                record.args = dict(redact_value(dict(record.args)))
            else:
                record.args = tuple(redact_value(arg) for arg in record.args)

        for key, value in list(record.__dict__.items()):
            if key.startswith("_") or key in _LOGGING_RESERVED:
                continue
            if isinstance(value, str):
                record.__dict__[key] = redact_text(value)
        return True


#: Attributes :class:`RedactingFilter` must not touch. These are set by the
#: logging machinery itself and rewriting them corrupts the record.
_LOGGING_RESERVED: Final[frozenset[str]] = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }
)


# --- formatters -------------------------------------------------------------


class JsonFormatter(_logging.Formatter):
    """Emits one JSON object per line.

    Structured output is what makes the logs queryable, and one line per record
    is what keeps them streamable.
    """

    def __init__(self) -> None:
        super().__init__()
        self._skip = _LOGGING_RESERVED | {"asctime", "message"}

    def format(self, record: _logging.LogRecord) -> str:
        """Renders the record as JSON."""
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, self.default_msec_format),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in self._skip or key.startswith("_"):
                continue
            if isinstance(value, (str, int, float, bool)) or value is None:
                payload[key] = value
            else:
                payload[key] = repr(value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=repr, ensure_ascii=False)


class TextFormatter(_logging.Formatter):
    """Human-readable output for local development."""

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)-8s %(name)s [%(correlation_id)s] %(message)s"
        )

    def format(self, record: _logging.LogRecord) -> str:
        """Renders the record, tolerating a missing correlation id."""
        if not hasattr(record, "correlation_id"):
            record.correlation_id = "-"
        return super().format(record)


# --- setup ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    """How logging should be configured.

    Attributes:
        level: Minimum level name.
        json_output: Whether to emit JSON rather than text.
        redact: Whether to screen records for secrets. Refusing to disable it
            in production is enforced at configuration validation, not here.
        stream: Where to write. Defaults to stderr.
    """

    level: str = "INFO"
    json_output: bool = True
    redact: bool = True
    stream: Any = None


def configure_logging(config: LoggingConfig | None = None) -> None:
    """Installs handlers, formatters and filters on the root logger.

    Idempotent: existing handlers are removed first, so calling this twice does
    not double every log line.
    """
    settings = config or LoggingConfig()
    root = _logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    stream = settings.stream if settings.stream is not None else sys.stderr
    handler = _logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter() if settings.json_output else TextFormatter())
    handler.addFilter(CorrelationFilter())
    handler.addFilter(RedactingFilter(enabled=settings.redact))

    root.addHandler(handler)
    root.setLevel(getattr(_logging, settings.level.upper(), _logging.INFO))


def get_logger(name: str) -> _logging.Logger:
    """Returns a namespaced logger."""
    return _logging.getLogger(name)
