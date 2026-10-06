# PURPOSE:
# Typed-error <-> gRPC status translation for the orchestrator's gRPC surface.
#
# Maps each domain error code onto the status Rust should observe, mirrors the
# Rust retry classification so moving a workload between lanes does not change
# retry behaviour, and carries structured details as trailing metadata rather
# than in the status message.
#
# Provides both directions: `abort_with_error` for a servicer terminating a
# call, and `error_from_grpc_status` for a client classifying a failure it
# received.

"""Bidirectional mapping between gRPC statuses and typed domain errors.

Two directions matter, and keeping them in one module is deliberate: a change
to the outbound mapping that forgets the inbound mapping produces a service
that cannot understand its own errors.

**Outbound (Python -> Rust)**

A :class:`~mas_orchestrator.contracts.errors.OrchestratorError` becomes a gRPC
status whose code Rust can classify with its canonical retry policy, plus a
structured payload (code, retryable, identifiers) in the status ``details``.

.. important::

   **``details.retryable`` is authoritative; the status code is a hint.**

   gRPC has no status meaning "the server is misconfigured" or "the model
   output failed validation" — both collapse to ``INTERNAL``, which most
   clients treat as transient. Retrying either is wrong, and no choice of
   status code can fix that. The retry decision must therefore be read from
   the ``details`` payload, which carries the domain code and its explicit
   ``retryable`` flag. A peer that ignores ``details`` and infers retryability
   from the status code alone will retry some terminal failures; this is a
   documented limitation of the status-code channel, not of the payload.

**Inbound (Rust -> Python)**

A gRPC status becomes a typed error so the retry layer can classify it. An
unrecognised status maps to
:class:`~mas_orchestrator.contracts.errors.DependencyUnavailableError` —
never to a generic success and never to a silent ``None``.

Security rules:

* The status message is always the error's **safe message**. Structured
  ``details`` are only attached when they were populated with redacted values.
* Credentials and provider payloads never reach a status: the redaction layer
  runs before this module is reached, and this module does not re-serialise
  arbitrary exception text.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final, NoReturn

import grpc

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any

from mas_orchestrator.contracts.errors import (
    RETRYABLE_CODES,
    DependencyUnavailableError,
    ErrorCode,
    OrchestratorError,
    error_from_code,
)

__all__ = [
    "CODE_TO_STATUS",
    "ERROR_DETAILS_METADATA_KEY",
    "RETRYABLE_STATUSES",
    "STATUS_TO_CODE",
    "abort_with_error",
    "details_from_abort",
    "error_from_code",
    "error_from_grpc_status",
    "is_retryable_status",
    "to_grpc_status",
    "unavailable",
    "unwrap_status_details",
]


#: Maps each domain error code onto the gRPC status Rust should observe.
#:
#: The classification mirrors the Rust side's canonical mapping
#: Trailing-metadata key carrying the JSON error payload. The status *message*
#: stays a short human-readable string; structured fields travel here so Rust
#: can log the machine-readable code without parsing prose.
ERROR_DETAILS_METADATA_KEY: Final[str] = "mas-error-details-bin"

#: (``mas_messaging::grpc::GrpcCode::is_retryable``) so that moving a workload
#: between the broker and the gRPC lane does not change retry behaviour.
CODE_TO_STATUS: Final[Mapping[ErrorCode, grpc.StatusCode]] = {
    # Configuration / startup — the caller cannot fix this by retrying.
    ErrorCode.CONFIGURATION_ERROR: grpc.StatusCode.INTERNAL,
    ErrorCode.INTERNAL_ERROR: grpc.StatusCode.INTERNAL,
    ErrorCode.DEPENDENCY_UNAVAILABLE: grpc.StatusCode.UNAVAILABLE,
    # Identity — retrying reproduces the rejection.
    ErrorCode.AUTHENTICATION_ERROR: grpc.StatusCode.UNAUTHENTICATED,
    ErrorCode.AUTHORIZATION_ERROR: grpc.StatusCode.PERMISSION_DENIED,
    ErrorCode.TENANT_CONTEXT_ERROR: grpc.StatusCode.FAILED_PRECONDITION,
    ErrorCode.POLICY_DENIED: grpc.StatusCode.PERMISSION_DENIED,
    # Protocol / request shape.
    ErrorCode.PROTOCOL_ERROR: grpc.StatusCode.FAILED_PRECONDITION,
    ErrorCode.INVALID_REQUEST: grpc.StatusCode.INVALID_ARGUMENT,
    ErrorCode.DUPLICATE_REQUEST: grpc.StatusCode.ALREADY_EXISTS,
    # Agent resolution.
    ErrorCode.AGENT_NOT_FOUND: grpc.StatusCode.NOT_FOUND,
    ErrorCode.AGENT_VERSION_NOT_FOUND: grpc.StatusCode.NOT_FOUND,
    # Model provider.
    ErrorCode.MODEL_NOT_AVAILABLE: grpc.StatusCode.UNAVAILABLE,
    ErrorCode.MODEL_RATE_LIMITED: grpc.StatusCode.RESOURCE_EXHAUSTED,
    ErrorCode.MODEL_TIMEOUT: grpc.StatusCode.DEADLINE_EXCEEDED,
    ErrorCode.MODEL_PROVIDER_ERROR: grpc.StatusCode.UNAVAILABLE,
    # Tools.
    ErrorCode.TOOL_NOT_FOUND: grpc.StatusCode.NOT_FOUND,
    ErrorCode.TOOL_PERMISSION_DENIED: grpc.StatusCode.PERMISSION_DENIED,
    ErrorCode.TOOL_TIMEOUT: grpc.StatusCode.DEADLINE_EXCEEDED,
    ErrorCode.TOOL_FAILED: grpc.StatusCode.INTERNAL,
    # Prompts / outputs — terminal; the request or the model output is at fault.
    ErrorCode.PROMPT_RENDER_ERROR: grpc.StatusCode.INTERNAL,
    ErrorCode.PROMPT_INJECTION_DETECTED: grpc.StatusCode.INVALID_ARGUMENT,
    ErrorCode.OUTPUT_VALIDATION_FAILED: grpc.StatusCode.INTERNAL,
    ErrorCode.PII_POLICY_DENIED: grpc.StatusCode.PERMISSION_DENIED,
    ErrorCode.CITATION_VALIDATION_ERROR: grpc.StatusCode.INTERNAL,
    # Memory / retrieval — transient infrastructure.
    ErrorCode.MEMORY_ERROR: grpc.StatusCode.UNAVAILABLE,
    ErrorCode.RETRIEVAL_ERROR: grpc.StatusCode.UNAVAILABLE,
    # Lifecycle.
    ErrorCode.QUOTA_EXCEEDED: grpc.StatusCode.RESOURCE_EXHAUSTED,
    ErrorCode.CANCELLATION_REQUESTED: grpc.StatusCode.CANCELLED,
    ErrorCode.EXECUTION_TIMEOUT: grpc.StatusCode.DEADLINE_EXCEEDED,
}

#: Statuses worth retrying as a *transport* signal.
#:
#: This mirrors `mas_messaging::grpc::GrpcCode::is_retryable` exactly, and it
#: is deliberately separate from the domain
#: :data:`mas_orchestrator.contracts.errors.RETRYABLE_CODES`.
#:
#: The two answer different questions. Retrying a **domain** failure is about
#: whether the request was rejected outright — a quota that is spent must not
#: be retried, because the retry burns more of the very thing that ran out.
#: Retrying a **transport** status is about whether the channel might work next
#: time — and for a bare status that is all the information there is.
#:
#: Conflating the two made Python disagree with Rust about
#: ``RESOURCE_EXHAUSTED``: Rust treats it as retryable, while Python's inverse
#: map resolved it to ``quota_exceeded``, which is terminal. A workload moved
#: between the broker and the gRPC lane would therefore have changed behaviour
#: for no reason a caller could see.
RETRYABLE_STATUSES: Final[frozenset[grpc.StatusCode]] = frozenset(
    {
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
        grpc.StatusCode.ABORTED,
        grpc.StatusCode.UNAVAILABLE,
    }
)

#: Documented, not enforced: Python deliberately classifies a deadline and an
#: exhausted quota as terminal at the domain level even though Rust's transport
#: rule would retry them. See :func:`is_retryable_status`.

#: The inverse mapping, used when Python calls a Rust service and needs to
#: classify the failure it received.
STATUS_TO_CODE: Final[Mapping[grpc.StatusCode, ErrorCode]] = {
    grpc.StatusCode.CANCELLED: ErrorCode.CANCELLATION_REQUESTED,
    grpc.StatusCode.DEADLINE_EXCEEDED: ErrorCode.EXECUTION_TIMEOUT,
    grpc.StatusCode.RESOURCE_EXHAUSTED: ErrorCode.QUOTA_EXCEEDED,
    grpc.StatusCode.UNAVAILABLE: ErrorCode.DEPENDENCY_UNAVAILABLE,
    grpc.StatusCode.UNAUTHENTICATED: ErrorCode.AUTHENTICATION_ERROR,
    grpc.StatusCode.PERMISSION_DENIED: ErrorCode.AUTHORIZATION_ERROR,
    grpc.StatusCode.INVALID_ARGUMENT: ErrorCode.INVALID_REQUEST,
    grpc.StatusCode.NOT_FOUND: ErrorCode.AGENT_NOT_FOUND,
    grpc.StatusCode.ALREADY_EXISTS: ErrorCode.DUPLICATE_REQUEST,
    grpc.StatusCode.FAILED_PRECONDITION: ErrorCode.TENANT_CONTEXT_ERROR,
    grpc.StatusCode.INTERNAL: ErrorCode.INTERNAL_ERROR,
    grpc.StatusCode.UNIMPLEMENTED: ErrorCode.PROTOCOL_ERROR,
    grpc.StatusCode.UNKNOWN: ErrorCode.INTERNAL_ERROR,
    grpc.StatusCode.DATA_LOSS: ErrorCode.INTERNAL_ERROR,
    grpc.StatusCode.OUT_OF_RANGE: ErrorCode.INVALID_REQUEST,
    grpc.StatusCode.ABORTED: ErrorCode.DEPENDENCY_UNAVAILABLE,
}


def to_grpc_status(error: OrchestratorError) -> tuple[grpc.StatusCode, str, bytes]:
    """Converts a typed error into the gRPC status triple Rust will receive.

    Args:
        error: The typed error raised inside the orchestrator.

    Returns:
        A ``(status_code, safe_message, serialised_details)`` triple. The
        message is the error's safe message; the details blob is the JSON
        encoding of :meth:`OrchestratorError.to_payload`.

    The details blob is produced with ``sort_keys`` so identical failures
    produce byte-identical statuses, which keeps logs and traces diffable.
    """
    payload = error.to_payload()
    status_code = CODE_TO_STATUS.get(error.code, grpc.StatusCode.INTERNAL)
    serialised = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return status_code, error.message, serialised


async def abort_with_error(
    context: grpc.aio.ServicerContext,
    error: OrchestratorError,
) -> NoReturn:
    """Aborts an in-flight RPC with the status implied by *error*.

    Args:
        context: The gRPC servicer context for the current call.
        error: The typed error to report.

    Because the orchestrator returns :class:`AgentRunReply` with a ``Failed``
    verdict for *expected* AI failures, this helper is reserved for failures
    that mean the RPC itself could not be served: authentication, protocol
    negotiation, and unrecoverable internal faults. Sending a modelled business
    failure as a transport error would bypass Rust's verdict handling.

    ``abort`` is a coroutine on the asyncio server API and never returns
    normally, so this helper is ``NoReturn``: the statement after an ``await
    abort_with_error(...)`` is unreachable by construction.

    Raises:
        Exception: Whatever :meth:`grpc.aio.ServicerContext.abort` raises to
            terminate the call.
    """
    status_code, message, details = to_grpc_status(error)
    # ``details`` is a JSON blob, so it travels as trailing metadata rather
    # than as the status message. Passing it as the third positional argument
    # would land it in ``trailing_metadata``, which gRPC rejects — and the
    # rejection replaces the intended status with UNKNOWN. That matters more
    # than a cosmetic detail: Rust maps UNKNOWN to a *retryable* external
    # failure, so a misrouted authentication error would have been retried
    # instead of reported.
    await context.abort(status_code, message, ((ERROR_DETAILS_METADATA_KEY, details),))
    # Unreachable: ``abort`` terminates the RPC. The raise exists so this
    # helper is genuinely NoReturn rather than merely annotated as such —
    # otherwise a caller that forgot to await would silently continue.
    raise AssertionError("abort terminates the RPC and never returns")


def error_from_grpc_status(
    status: grpc.aio.AioRpcError | grpc.RpcError,
    *,
    default_code: ErrorCode | None = None,
    correlation_id: str | None = None,
    request_id: str | None = None,
) -> OrchestratorError:
    """Converts a failed RPC into a typed error.

    Args:
        status: The RPC error raised by the gRPC runtime.
        default_code: Overrides the code chosen for an unrecognised status.
        correlation_id: Correlation id to attach, for end-to-end tracing.
        request_id: Request id to attach.

    Returns:
        A typed :class:`OrchestratorError`. An unrecognised status code becomes
        :class:`DependencyUnavailableError` (or ``default_code``), because a
        status we do not understand most conservatively means "the other side
        is not behaving as contracted" — which is transient from our
        perspective, not a permanent rejection of the request.

    Note:
        The remote status message is **not** propagated verbatim. It is
        attacker-influenced data from another service and may contain text we
        do not control; only its presence is recorded.
    """
    code = status.code() if hasattr(status, "code") else None

    # The peer's structured payload is the authoritative classification. The
    # sender knew why the call failed; from a bare status the receiver can only
    # guess. Guessing when the answer is available is how a terminal quota
    # rejection gets retried, or a transient rate limit gets given up on.
    payload = details_from_abort(status) if hasattr(status, "trailing_metadata") else {}
    payload_code = payload.get("code")
    payload_retryable = payload.get("retryable")

    mapped: ErrorCode
    if isinstance(payload_code, str):
        try:
            mapped = ErrorCode(payload_code)
        except ValueError:
            # A newer peer emitted a code this process does not know. Falling
            # back to the status is right; refusing to classify would mask the
            # original failure.
            mapped = STATUS_TO_CODE.get(code) or default_code or ErrorCode.DEPENDENCY_UNAVAILABLE
    else:
        mapped = (
            (STATUS_TO_CODE.get(code) if code is not None else None)
            or default_code
            or ErrorCode.DEPENDENCY_UNAVAILABLE
        )

    # Only an explicit payload overrides the domain's own answer. Without one,
    # the domain table decides, so this agrees with `is_retryable_status`
    # rather than contradicting it.
    retryable: bool | None = payload_retryable if isinstance(payload_retryable, bool) else None

    remote_message = status.details() if hasattr(status, "details") else None
    message = f"rust core call failed with {getattr(code, 'name', 'UNKNOWN')}"

    error = error_from_code(
        mapped,
        message,
        retryable=retryable,
        correlation_id=correlation_id,
        request_id=request_id,
        details={
            "grpc_status": getattr(code, "name", "UNKNOWN"),
            "remote_message_present": bool(remote_message),
        },
    )
    return error


def is_retryable_status(
    status: grpc.aio.AioRpcError | grpc.RpcError | grpc.StatusCode,
) -> bool:
    """Returns whether a failed RPC may be retried.

    Args:
        status: An RPC error, or a bare :class:`grpc.StatusCode`.

    Returns:
        Whether a retry could plausibly succeed.

    Retryability is derived from the mapped domain code, so the answer agrees
    with :data:`mas_orchestrator.contracts.errors.RETRYABLE_CODES`.

    This answers the **domain** question — "should this operation be retried?"
    — not the transport one. The distinction matters: a deadline means the
    execution is over, so retrying would resurrect work the platform has
    already given up on, even though the underlying RPC might well succeed on a
    second attempt. Rust's ``GrpcCode::is_retryable`` answers the transport
    question and therefore disagrees here; that is intentional on both sides,
    and the two are reconciled by :func:`error_from_grpc_status`, which prefers
    the peer's explicit classification over either guess.

    A bare :class:`~grpc.StatusCode` is accepted as well as an RPC error.
    Previously only an error object was understood and anything else fell
    through to the "unknown" branch, which answers *retryable* — so a caller
    that passed ``error.code()`` by mistake was told to retry an
    authentication rejection forever. Accepting both makes the function total
    and removes that failure mode.
    """
    code = status if isinstance(status, grpc.StatusCode) else _status_code_of(status)
    mapped = STATUS_TO_CODE.get(code) if code is not None else None
    if mapped is None:
        # Unknown status: treat as an unavailable dependency (retryable) which
        # is the same conservative default as `error_from_grpc_status`.
        return ErrorCode.DEPENDENCY_UNAVAILABLE in RETRYABLE_CODES
    return mapped in RETRYABLE_CODES


def _status_code_of(status: grpc.aio.AioRpcError | grpc.RpcError) -> grpc.StatusCode | None:
    """Extracts the status code from an RPC error.

    Args:
        status: The RPC error.

    Returns:
        Its status code, or ``None`` when the object does not carry one.
    """
    getter = getattr(status, "code", None)
    if getter is None or not callable(getter):
        return None
    code = getter()
    return code if isinstance(code, grpc.StatusCode) else None


def unwrap_status_details(details: bytes | str | None) -> dict[str, object]:
    """Decodes a status details blob produced by :func:`to_grpc_status`.

    Args:
        details: The raw details bytes (or str) from a failed RPC.

    Returns:
        The decoded payload, or an empty dict when absent or malformed. A
        malformed blob must never prevent the caller from handling the error,
        so this returns ``{}`` instead of raising.
    """
    if not details:
        return {}
    raw = details.encode("utf-8") if isinstance(details, str) else details
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def unavailable(operation: str) -> DependencyUnavailableError:
    """Builds the canonical 'dependency not reachable' error.

    Used by the client layer when the channel itself is not ready, so every
    such failure is classified identically regardless of where it is raised.
    """
    return DependencyUnavailableError(
        f"rust core is unavailable for {operation}",
        details={"operation": operation},
    )


def details_from_abort(
    error: grpc.aio.AioRpcError | grpc.RpcError,
) -> dict[str, Any]:
    """Reads the structured error payload Python attached when it aborted.

    This is the inbound mirror of :func:`abort_with_error`: when the
    orchestrator calls a Rust service that aborts with the same convention, the
    JSON payload can be recovered here instead of being scraped out of a
    human-readable message.

    Args:
        error: The RPC error raised by the gRPC runtime.

    Returns:
        The decoded payload, or an empty mapping when no parsable payload is
        present. Malformed metadata yields an empty mapping rather than an
        exception: a diagnostics-only field must never turn one failure into
        two.
    """
    for key, value in tuple(error.trailing_metadata() or ()):
        if key != ERROR_DETAILS_METADATA_KEY:
            continue
        raw = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}
