"""API error mapping.

The rule this module exists to enforce: an HTTP response must preserve the
typed error's identity. Collapsing every failure into ``500`` loses the
distinction between "you may not" and "we broke", which is the distinction a
caller needs in order to behave correctly.

So each domain error maps to a specific status and a stable ``code`` drawn from
the error's own code, never invented here. An unmapped error becomes ``500``,
because an error the API does not understand is by definition unexpected.

Two things are never included in a response:

* an exception message, which can echo provider or tool content;
* any detail that looks like a credential.

Errors are logged with their type and correlation id; the body carries just
enough for the caller to act.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Final

from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    AuthenticationError,
    AuthorizationError,
    ConfigurationError,
    InvalidRequestError,
    ModelRateLimitedError,
    ModelTimeoutError,
    OrchestratorError,
    PolicyDeniedError,
    ProtocolError,
    QuotaExceededError,
    TenantContextError,
    ToolPermissionDeniedError,
    ToolTimeoutError,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Domain error type -> HTTP status.
STATUS_MAP: Final[dict[type[BaseException], int]] = {
    InvalidRequestError: 400,
    ProtocolError: 400,
    TenantContextError: 400,
    AuthenticationError: 401,
    AuthorizationError: 403,
    PolicyDeniedError: 403,
    ToolPermissionDeniedError: 403,
    AgentNotFoundError: 404,
    ModelTimeoutError: 504,
    ToolTimeoutError: 504,
    ModelRateLimitedError: 429,
    QuotaExceededError: 429,
    ConfigurationError: 500,
}

#: Errors that are safe to describe to a caller. Anything else gets a generic
#: message, because its text may contain content that should not be echoed.
SAFE_TO_DESCRIBE: Final[frozenset[type[BaseException]]] = frozenset(
    {
        InvalidRequestError,
        ProtocolError,
        AuthenticationError,
        PolicyDeniedError,
        ToolPermissionDeniedError,
        AgentNotFoundError,
        QuotaExceededError,
        ModelRateLimitedError,
    }
)


@dataclass(frozen=True, slots=True)
class ErrorBody:
    """An API error response body.

    Attributes:
        code: The domain error code, when the error has one.
        message: A caller-safe message.
        status: HTTP status.
        retryable: Whether retrying could succeed.
        correlation_id: Correlation identifier, so a caller can quote it.
    """

    code: str
    message: str
    status: int
    retryable: bool = False
    correlation_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        """A JSON-serialisable view."""
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "retryable": self.retryable,
                "correlation_id": self.correlation_id,
            }
        }


def status_for(error: BaseException) -> int:
    """Maps an error to an HTTP status.

    Subclass relationships are honoured, so a new error deriving from
    :class:`InvalidRequestError` maps to 400 without being listed.
    """
    for error_type, status in STATUS_MAP.items():
        if isinstance(error, error_type):
            return status
    return 500


def to_body(
    error: BaseException,
    *,
    correlation_id: str = "",
) -> ErrorBody:
    """Builds a response body for an error.

    Args:
        error: The error to describe.
        correlation_id: Correlation identifier for the caller to quote.

    Returns:
        An :class:`ErrorBody`.
    """
    status = status_for(error)
    code = _code_of(error)
    retryable = bool(getattr(getattr(error, "details", None), "get", lambda _k: None)("retryable"))

    if isinstance(error, tuple(SAFE_TO_DESCRIBE)):
        # str() of an OrchestratorError is its code-prefixed message, which is
        # written by us and safe to echo; only these types guarantee that.
        message = str(error)
    else:
        # Generic, because the real message may contain content that should
        # not leave the service.
        message = _GENERIC_MESSAGES.get(status, "the request could not be completed")

    return ErrorBody(
        code=code,
        message=message,
        status=status,
        retryable=retryable,
        correlation_id=correlation_id,
    )


#: Messages used when the real one must not be echoed.
_GENERIC_MESSAGES: Final[dict[int, str]] = {
    400: "the request was rejected",
    401: "authentication is required",
    403: "the request is not permitted",
    404: "the requested resource does not exist",
    429: "too many requests",
    500: "an internal error occurred",
    504: "the operation timed out",
}


def log_error(
    error: BaseException,
    *,
    correlation_id: str = "",
    route: str = "",
) -> None:
    """Logs an error with its type and correlation id, never its payload.

    A 5xx is logged at error level and a 4xx at warning: the first is our
    problem, the second is the caller's.
    """
    status = status_for(error)
    payload = {
        "event": "api.error",
        "status": status,
        "error_type": type(error).__name__,
        "error_code": _code_of(error),
        "correlation_id": correlation_id,
        "route": route,
    }
    if status >= 500:
        _LOGGER.error("request failed", extra=payload, exc_info=error)
    else:
        _LOGGER.warning("request rejected", extra=payload)


def _code_of(error: BaseException) -> str:
    """The domain error code, or a fallback."""
    code = getattr(error, "code", None)
    if code is not None:
        return str(getattr(code, "value", code))
    return error.__class__.__name__


def is_retryable_status(status: int) -> bool:
    """Whether an HTTP status is worth retrying."""
    return status in (429, 502, 503, 504)


__all__ = [
    "STATUS_MAP",
    "ErrorBody",
    "OrchestratorError",
    "is_retryable_status",
    "log_error",
    "status_for",
    "to_body",
]
