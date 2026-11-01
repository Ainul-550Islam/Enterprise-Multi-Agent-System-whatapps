"""Typed error model shared across the Python AI Orchestrator.

Every failure that crosses a boundary (gRPC, HTTP, provider, tool, internal
service) is represented as an :class:`OrchestratorError` carrying:

* a **stable machine-readable code** (:class:`ErrorCode`) — never a prose
  string, so callers can branch without parsing messages;
* a **safe message** — human readable, guaranteed free of secrets and of raw
  provider payloads;
* **retryability** — so the retry layer never has to guess;
* optional **correlation/request identifiers** for end-to-end tracing.

The code list mirrors the contract agreed with the Rust Core so that an error
produced here can be classified by the platform's canonical retry policy
without a translation table at the edge.

Design rules enforced here:

* Secrets are never accepted into error details as free-form text; anything
  interpolated into a message must be passed through
  :func:`mas_orchestrator.security.redaction.redact` by the caller.
* ``retryable`` is explicit per code; there is no default of ``True``.
* Errors are immutable value objects: they may be logged, serialised and
  re-raised without mutation.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar, Final

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "AUTHENTICATION_ERRORS",
    "RETRYABLE_CODES",
    "TERMINAL_CODES",
    "AgentNotFoundError",
    "AgentVersionNotFoundError",
    "AuthenticationError",
    "AuthorizationError",
    "CancellationRequestedError",
    "CitationValidationError",
    "ConfigurationError",
    "DependencyUnavailableError",
    "DuplicateRequestError",
    "ErrorCode",
    "ExecutionTimeoutError",
    "InternalError",
    "InvalidRequestError",
    "MemoryBackendError",
    "ModelNotAvailableError",
    "ModelProviderError",
    "ModelRateLimitedError",
    "ModelTimeoutError",
    "OrchestratorError",
    "OutputValidationFailedError",
    "PiiPolicyDeniedError",
    "PolicyDeniedError",
    "PromptInjectionDetectedError",
    "PromptRenderError",
    "ProtocolError",
    "QuotaExceededError",
    "RetrievalError",
    "TenantContextError",
    "ToolFailedError",
    "ToolNotFoundError",
    "ToolPermissionDeniedError",
    "ToolTimeoutError",
    "error_from_code",
    "is_retryable",
]


class ErrorCode(StrEnum):
    """Stable, machine-readable error codes.

    The string values are part of the Python<->Rust contract: they are emitted
    in gRPC status details and in AI event payloads, and Rust branches on them.
    Renaming a member without updating the Rust side is a breaking change.
    """

    # -- configuration / startup --------------------------------------------
    CONFIGURATION_ERROR = "configuration_error"
    INTERNAL_ERROR = "internal_error"
    DEPENDENCY_UNAVAILABLE = "dependency_unavailable"

    # -- identity / authorization -------------------------------------------
    AUTHENTICATION_ERROR = "authentication_error"
    AUTHORIZATION_ERROR = "authorization_error"
    TENANT_CONTEXT_ERROR = "tenant_context_error"
    POLICY_DENIED = "policy_denied"

    # -- protocol / request shape -------------------------------------------
    PROTOCOL_ERROR = "protocol_error"
    INVALID_REQUEST = "invalid_request"
    DUPLICATE_REQUEST = "duplicate_request"

    # -- agent resolution ----------------------------------------------------
    AGENT_NOT_FOUND = "agent_not_found"
    AGENT_VERSION_NOT_FOUND = "agent_version_not_found"

    # -- model provider ------------------------------------------------------
    MODEL_NOT_AVAILABLE = "model_not_available"
    MODEL_RATE_LIMITED = "model_rate_limited"
    MODEL_TIMEOUT = "model_timeout"
    MODEL_PROVIDER_ERROR = "model_provider_error"

    # -- tools ---------------------------------------------------------------
    TOOL_NOT_FOUND = "tool_not_found"
    TOOL_PERMISSION_DENIED = "tool_permission_denied"
    TOOL_TIMEOUT = "tool_timeout"
    TOOL_FAILED = "tool_failed"

    # -- prompts / outputs ---------------------------------------------------
    PROMPT_RENDER_ERROR = "prompt_render_error"
    PROMPT_INJECTION_DETECTED = "prompt_injection_detected"
    OUTPUT_VALIDATION_FAILED = "output_validation_failed"
    PII_POLICY_DENIED = "pii_policy_denied"
    CITATION_VALIDATION_ERROR = "citation_validation_error"

    # -- memory / retrieval --------------------------------------------------
    MEMORY_ERROR = "memory_error"
    RETRIEVAL_ERROR = "retrieval_error"

    # -- lifecycle / resources -----------------------------------------------
    QUOTA_EXCEEDED = "quota_exceeded"
    CANCELLATION_REQUESTED = "cancellation_requested"
    EXECUTION_TIMEOUT = "execution_timeout"


#: Codes whose failures may succeed on a later attempt without any change to
#: the request. Everything outside this set is treated as terminal: retrying it
#: would either reproduce the failure or duplicate a durable side effect.
RETRYABLE_CODES: Final[frozenset[ErrorCode]] = frozenset(
    {
        ErrorCode.MODEL_RATE_LIMITED,
        ErrorCode.MODEL_TIMEOUT,
        ErrorCode.MODEL_PROVIDER_ERROR,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        ErrorCode.MODEL_NOT_AVAILABLE,
        ErrorCode.TOOL_TIMEOUT,
        ErrorCode.RETRIEVAL_ERROR,
        ErrorCode.MEMORY_ERROR,
        ErrorCode.INTERNAL_ERROR,
    }
)

#: Codes that must never be retried because the request itself is rejected;
#: retrying burns quota and delays the error reaching the caller.
TERMINAL_CODES: Final[frozenset[ErrorCode]] = frozenset(
    code for code in ErrorCode if code not in RETRYABLE_CODES
)

#: Codes indicating the caller's identity could not be established. These are
#: surfaced without echoing the credential material that failed.
AUTHENTICATION_ERRORS: Final[frozenset[ErrorCode]] = frozenset(
    {
        ErrorCode.AUTHENTICATION_ERROR,
        ErrorCode.AUTHORIZATION_ERROR,
        ErrorCode.TENANT_CONTEXT_ERROR,
    }
)


class OrchestratorError(Exception):
    """Base class for every typed error raised by the orchestrator.

    Args:
        message: Human-readable, secret-free description. Callers are
            responsible for redacting any value interpolated into it.
        code: Stable machine-readable code. Defaults per subclass.
        retryable: Whether an identical retry could plausibly succeed. When
            omitted it is derived from :data:`RETRYABLE_CODES` so a subclass
            cannot accidentally opt into endless retries.
        correlation_id: End-to-end correlation identifier supplied by Rust.
        request_id: Platform request identifier supplied by Rust.
        details: Structured, already-redacted context. Values must be JSON
            primitives; never raw provider responses or credentials.

    The rendered ``str()`` never includes ``details``, so a stray log of an
    exception cannot leak structured payloads an operator did not ask for.
    """

    #: Default code for the subclass; overridden per concrete error type.
    default_code: ClassVar[ErrorCode] = ErrorCode.INTERNAL_ERROR

    def __init__(
        self,
        message: str,
        *,
        code: ErrorCode | None = None,
        retryable: bool | None = None,
        correlation_id: str | None = None,
        request_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        resolved_code = code if code is not None else type(self).default_code
        self.code: ErrorCode = resolved_code
        self.message: str = message
        self.retryable: bool = resolved_code in RETRYABLE_CODES if retryable is None else retryable
        self.correlation_id: str | None = correlation_id
        self.request_id: str | None = request_id
        self.details: dict[str, Any] = dict(details) if details else {}

    def __str__(self) -> str:
        """Renders code, message and — when present — correlation context."""
        suffix = ""
        if self.correlation_id or self.request_id:
            identifiers = [
                part
                for part in (
                    f"correlation_id={self.correlation_id}" if self.correlation_id else "",
                    f"request_id={self.request_id}" if self.request_id else "",
                )
                if part
            ]
            suffix = f" [{', '.join(identifiers)}]"
        return f"{self.code}: {self.message}{suffix}"

    def __repr__(self) -> str:
        """Debug rendering that includes retryability and detail keys."""
        return (
            f"{type(self).__name__}(code={self.code!r}, message={self.message!r}, "
            f"retryable={self.retryable!r}, details={sorted(self.details)!r})"
        )

    def with_context(
        self,
        *,
        correlation_id: str | None = None,
        request_id: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> OrchestratorError:
        """Returns a copy carrying additional identifiers and details.

        Used when an error crosses a boundary into a layer that knows more
        about the request (for example the gRPC servicer attaching the
        correlation id before serialising the status). The original exception
        is never mutated, so it stays safe to re-raise.
        """
        merged: dict[str, Any] = dict(self.details)
        if details:
            merged.update(details)
        return type(self)(
            self.message,
            code=self.code,
            retryable=self.retryable,
            correlation_id=correlation_id or self.correlation_id,
            request_id=request_id or self.request_id,
            details=merged,
        )

    def to_payload(self) -> dict[str, Any]:
        """Serialises the error for gRPC status details and event payloads.

        ``details`` is included verbatim: it is the caller's responsibility to
        populate it only with redacted, JSON-primitive values.
        """
        payload: dict[str, Any] = {
            "code": str(self.code),
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.correlation_id:
            payload["correlation_id"] = self.correlation_id
        if self.request_id:
            payload["request_id"] = self.request_id
        if self.details:
            payload["details"] = dict(self.details)
        return payload


def is_retryable(error: BaseException) -> bool:
    """Returns whether *error* may be retried.

    Non-orchestrator exceptions (``asyncio.CancelledError``, transport
    internals) are **not** treated as retryable: classifying them would require
    guessing, and the spec forbids retrying anything not explicitly classified.
    Callers must wrap such failures into a typed error first.
    """
    if isinstance(error, OrchestratorError):
        return error.retryable
    return False


# ---------------------------------------------------------------------------
# Configuration / startup
# ---------------------------------------------------------------------------


class ConfigurationError(OrchestratorError):
    """Settings are invalid, incomplete, or unsafe for the target environment."""

    default_code = ErrorCode.CONFIGURATION_ERROR


class InternalError(OrchestratorError):
    """An invariant was violated inside the orchestrator.

    Always retryable-safe to surface but never expected; presence in
    :data:`RETRYABLE_CODES` reflects that a transient internal fault may clear,
    not that retrying is generally useful.
    """

    default_code = ErrorCode.INTERNAL_ERROR


class DependencyUnavailableError(OrchestratorError):
    """A required downstream (Rust Core, broker, vector store) is unreachable."""

    default_code = ErrorCode.DEPENDENCY_UNAVAILABLE


# ---------------------------------------------------------------------------
# Identity / authorization
# ---------------------------------------------------------------------------


class AuthenticationError(OrchestratorError):
    """The caller's service identity could not be established."""

    default_code = ErrorCode.AUTHENTICATION_ERROR


class AuthorizationError(OrchestratorError):
    """The caller is authenticated but not permitted for this operation."""

    default_code = ErrorCode.AUTHORIZATION_ERROR


class TenantContextError(OrchestratorError):
    """Authoritative tenant/organization context is missing or inconsistent.

    Raised when a request arrives without the identity Rust is required to
    supply, or when the caller attempts to override it. This is the guard that
    stops the orchestrator from trusting a user-supplied ``tenant_id`` header.
    """

    default_code = ErrorCode.TENANT_CONTEXT_ERROR


class PolicyDeniedError(OrchestratorError):
    """The authoritative Rust policy service denied the operation."""

    default_code = ErrorCode.POLICY_DENIED


# ---------------------------------------------------------------------------
# Protocol / request shape
# ---------------------------------------------------------------------------


class ProtocolError(OrchestratorError):
    """Wire-level contract violation: unsupported or malformed protocol version."""

    default_code = ErrorCode.PROTOCOL_ERROR


class InvalidRequestError(OrchestratorError):
    """Request failed schema or cross-field validation."""

    default_code = ErrorCode.INVALID_REQUEST


class DuplicateRequestError(OrchestratorError):
    """A request with the same invocation identity was already observed."""

    default_code = ErrorCode.DUPLICATE_REQUEST


# ---------------------------------------------------------------------------
# Agent resolution
# ---------------------------------------------------------------------------


class AgentNotFoundError(OrchestratorError):
    """No agent matches the requested identifier."""

    default_code = ErrorCode.AGENT_NOT_FOUND


class AgentVersionNotFoundError(OrchestratorError):
    """The agent exists but the requested version does not."""

    default_code = ErrorCode.AGENT_VERSION_NOT_FOUND


# ---------------------------------------------------------------------------
# Model provider
# ---------------------------------------------------------------------------


class ModelNotAvailableError(OrchestratorError):
    """The requested model is not enabled, not allowlisted, or not routable."""

    default_code = ErrorCode.MODEL_NOT_AVAILABLE


class ModelRateLimitedError(OrchestratorError):
    """The provider returned a rate-limit signal (HTTP 429 or equivalent)."""

    default_code = ErrorCode.MODEL_RATE_LIMITED

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, **kwargs)
        #: Provider-supplied retry hint; honoured by the retry layer when the
        #: configuration enables it. ``None`` means "no hint supplied".
        self.retry_after_seconds: float | None = retry_after_seconds

    def to_payload(self) -> dict[str, Any]:
        """Includes the retry hint so callers can back off accurately."""
        payload = super().to_payload()
        if self.retry_after_seconds is not None:
            payload["retry_after_seconds"] = self.retry_after_seconds
        return payload


class ModelTimeoutError(OrchestratorError):
    """The provider call exceeded its deadline."""

    default_code = ErrorCode.MODEL_TIMEOUT


class ModelProviderError(OrchestratorError):
    """The provider failed in a way not covered by a more specific code.

    Covers 5xx responses, malformed payloads and transport failures. The raw
    provider body is never attached; only a redacted summary may be.
    """

    default_code = ErrorCode.MODEL_PROVIDER_ERROR

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(message, **kwargs)
        #: HTTP status when the failure originated from an HTTP provider.
        self.status_code: int | None = status_code

    def to_payload(self) -> dict[str, Any]:
        """Includes the HTTP status so the caller can classify the failure."""
        payload = super().to_payload()
        if self.status_code is not None:
            payload["status_code"] = self.status_code
        return payload


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


class ToolNotFoundError(OrchestratorError):
    """The requested tool is not registered or not enabled for this agent."""

    default_code = ErrorCode.TOOL_NOT_FOUND


class ToolPermissionDeniedError(OrchestratorError):
    """The tool exists but policy/permissions refuse this invocation."""

    default_code = ErrorCode.TOOL_PERMISSION_DENIED


class ToolTimeoutError(OrchestratorError):
    """The tool invocation exceeded its timeout."""

    default_code = ErrorCode.TOOL_TIMEOUT


class ToolFailedError(OrchestratorError):
    """The tool ran and returned a failure result."""

    default_code = ErrorCode.TOOL_FAILED


# ---------------------------------------------------------------------------
# Prompts / outputs
# ---------------------------------------------------------------------------


class PromptRenderError(OrchestratorError):
    """Template rendering failed: missing variable, unsafe value, size breach."""

    default_code = ErrorCode.PROMPT_RENDER_ERROR


class PromptInjectionDetectedError(OrchestratorError):
    """Input or tool output matched an injection heuristic and was blocked."""

    default_code = ErrorCode.PROMPT_INJECTION_DETECTED


class OutputValidationFailedError(OrchestratorError):
    """Model output failed schema validation or output guardrails."""

    default_code = ErrorCode.OUTPUT_VALIDATION_FAILED


class PiiPolicyDeniedError(OrchestratorError):
    """Content violated the PII policy under a fail-closed configuration."""

    default_code = ErrorCode.PII_POLICY_DENIED


class CitationValidationError(OrchestratorError):
    """Required provenance/citations were missing, unverifiable, or mismatched."""

    default_code = ErrorCode.CITATION_VALIDATION_ERROR


# ---------------------------------------------------------------------------
# Memory / retrieval
# ---------------------------------------------------------------------------


class MemoryBackendError(OrchestratorError):
    """Memory backend read/write failed.

    Trailing underscore avoids colliding with the builtin ``MemoryError``.
    """

    default_code = ErrorCode.MEMORY_ERROR


class RetrievalError(OrchestratorError):
    """RAG retrieval failed: index unreachable, query invalid, filter rejected."""

    default_code = ErrorCode.RETRIEVAL_ERROR


# ---------------------------------------------------------------------------
# Lifecycle / resources
# ---------------------------------------------------------------------------


class QuotaExceededError(OrchestratorError):
    """A configured budget (tokens, tool calls, steps, cost) was exhausted."""

    default_code = ErrorCode.QUOTA_EXCEEDED


class CancellationRequestedError(OrchestratorError):
    """Cancellation was authoritatively requested; work must stop.

    Never retryable: the spec forbids continuing work after cancellation has
    been received, and retrying would resurrect cancelled work.
    """

    default_code = ErrorCode.CANCELLATION_REQUESTED


class ExecutionTimeoutError(OrchestratorError):
    """The execution exceeded its overall wall-clock budget."""

    default_code = ErrorCode.EXECUTION_TIMEOUT


#: Maps every code to the concrete exception class representing it, so an error
#: arriving over the wire (or from Rust) can be reconstructed as a typed object.
_CODE_TO_TYPE: Final[Mapping[ErrorCode, type[OrchestratorError]]] = {
    ErrorCode.CONFIGURATION_ERROR: ConfigurationError,
    ErrorCode.INTERNAL_ERROR: InternalError,
    ErrorCode.DEPENDENCY_UNAVAILABLE: DependencyUnavailableError,
    ErrorCode.AUTHENTICATION_ERROR: AuthenticationError,
    ErrorCode.AUTHORIZATION_ERROR: AuthorizationError,
    ErrorCode.TENANT_CONTEXT_ERROR: TenantContextError,
    ErrorCode.POLICY_DENIED: PolicyDeniedError,
    ErrorCode.PROTOCOL_ERROR: ProtocolError,
    ErrorCode.INVALID_REQUEST: InvalidRequestError,
    ErrorCode.DUPLICATE_REQUEST: DuplicateRequestError,
    ErrorCode.AGENT_NOT_FOUND: AgentNotFoundError,
    ErrorCode.AGENT_VERSION_NOT_FOUND: AgentVersionNotFoundError,
    ErrorCode.MODEL_NOT_AVAILABLE: ModelNotAvailableError,
    ErrorCode.MODEL_RATE_LIMITED: ModelRateLimitedError,
    ErrorCode.MODEL_TIMEOUT: ModelTimeoutError,
    ErrorCode.MODEL_PROVIDER_ERROR: ModelProviderError,
    ErrorCode.TOOL_NOT_FOUND: ToolNotFoundError,
    ErrorCode.TOOL_PERMISSION_DENIED: ToolPermissionDeniedError,
    ErrorCode.TOOL_TIMEOUT: ToolTimeoutError,
    ErrorCode.TOOL_FAILED: ToolFailedError,
    ErrorCode.PROMPT_RENDER_ERROR: PromptRenderError,
    ErrorCode.PROMPT_INJECTION_DETECTED: PromptInjectionDetectedError,
    ErrorCode.OUTPUT_VALIDATION_FAILED: OutputValidationFailedError,
    ErrorCode.PII_POLICY_DENIED: PiiPolicyDeniedError,
    ErrorCode.CITATION_VALIDATION_ERROR: CitationValidationError,
    ErrorCode.MEMORY_ERROR: MemoryBackendError,
    ErrorCode.RETRIEVAL_ERROR: RetrievalError,
    ErrorCode.QUOTA_EXCEEDED: QuotaExceededError,
    ErrorCode.CANCELLATION_REQUESTED: CancellationRequestedError,
    ErrorCode.EXECUTION_TIMEOUT: ExecutionTimeoutError,
}


def error_from_code(
    code: ErrorCode | str,
    message: str,
    **kwargs: Any,
) -> OrchestratorError:
    """Reconstructs the concrete error type for *code*.

    Unknown codes (for example a newer Rust build emitting a code this process
    does not know) fall back to :class:`InternalError` rather than raising
    during error handling: a failure to classify must never mask the original
    failure, and must never crash the servicer.
    """
    try:
        resolved = ErrorCode(code)
    except ValueError:
        return InternalError(
            message,
            details={"unknown_code": str(code)},
            **kwargs,
        )
    error_type = _CODE_TO_TYPE[resolved]
    return error_type(message, code=resolved, **kwargs)
