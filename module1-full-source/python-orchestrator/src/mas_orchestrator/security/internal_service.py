# PURPOSE: Body-bound, time-bounded internal service authentication for the
# Rust Core -> Python orchestrator bridge.
#
# Owning layer: `mas_orchestrator.security` (the Python process edge).
#
# Production responsibility: verify that an inbound bridge call was made by a
# platform service that holds the shared secret, that the call was made in the
# last `MAX_SKEW_SECONDS`, and that the exact bytes now being processed are the
# bytes that were signed.
#
# Security boundary: this scheme proves *which service* called and that the
# message was not tampered with in flight. It does NOT establish tenancy —
# the tenant is authoritative data supplied by Rust in the request body, and
# Python validates it against the resolved scope. Network reachability is
# never an identity.
#
# It must NOT:
#   • accept a signature over anything other than the exact request bytes;
#   • accept a timestamp outside the skew window (replay defence);
#   • accept a caller that is not on the allowlist;
#   • distinguish "missing header" from "wrong secret" in the error it raises —
#     every failure is the same `AuthenticationError`, so a prober learns
#     nothing;
#   • log, render, or include the secret in any exception detail.
#
# The scheme is byte-for-byte compatible with
# `mas-security::internal_service` (Rust):
#
#     body_digest = hex(SHA256(body))
#     signed_message = "v1:" + service + ":" + unix_millis + ":" + body_digest
#     signature = signed_message prefix + hex(HMAC_SHA256(secret, signed_message))

from __future__ import annotations

import hashlib
import hmac
import re
import time
from dataclasses import dataclass
from typing import Final

from mas_orchestrator.contracts.errors import AuthenticationError

__all__ = [
    "MAX_SKEW_SECONDS",
    "SERVICE_HEADER",
    "SIGNATURE_HEADER",
    "InternalServiceIdentity",
    "signed_internal_signature",
    "verify_internal_signature",
]

#: Metadata key carrying the signature.
SIGNATURE_HEADER: Final[str] = "x-mas-internal-signature"

#: Metadata key carrying the calling service's claimed id.
SERVICE_HEADER: Final[str] = "x-mas-internal-service"

#: Largest acceptable clock skew, in seconds.
#:
#: 60 seconds is deliberately tight: it bounds the window in which a captured
#: request can be replayed, while tolerating ordinary clock drift between two
#: hosts in the same cluster.
MAX_SKEW_SECONDS: Final[int] = 60

#: Only these services may call the orchestrator's internal endpoints.
DEFAULT_ALLOWED_CALLERS: Final[frozenset[str]] = frozenset({"mas-api", "mas-worker"})

_SIGNATURE_RE: Final[re.Pattern[str]] = re.compile(
    r"\Av1:([A-Za-z0-9_-]{1,64}):(\d{1,20}):([0-9a-f]{64})\Z"
)


@dataclass(frozen=True, slots=True)
class InternalServiceIdentity:
    """The result of a successful verification.

    Attributes:
        service: The calling service id, proven against the allowlist.
        signed_at_ms: The timestamp the caller signed, in epoch milliseconds.
    """

    service: str
    signed_at_ms: int

    def __str__(self) -> str:
        """Renders the service and timestamp — never key material."""
        return f"InternalServiceIdentity(service={self.service}, signed_at_ms={self.signed_at_ms})"


def _body_digest(body: bytes) -> str:
    """Hex SHA-256 of *body*.

    The digest stands in for the body inside the MAC so that arbitrarily large
    payloads cost one hash rather than being buffered twice, while still
    binding the signature to the exact message.
    """
    return hashlib.sha256(body).hexdigest()


def _mac(secret: bytes, service: str, signed_at_ms: int, body: bytes) -> str:
    """Computes the hex HMAC-SHA256 for the message.

    The signed message is ``v1:<service>:<millis>:<hex sha256(body)>`` — the
    same string the signature header begins with, so a reader comparing the
    two can see at a glance that the version, service and timestamp are all
    inside the MAC and not merely adjacent to it.
    """
    message = f"v1:{service}:{signed_at_ms}:{_body_digest(body)}".encode()
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def signed_internal_signature(
    secret: str | bytes,
    service: str,
    signed_at_ms: int,
    body: bytes,
) -> str:
    """Builds a signature header value.

    Args:
        secret: The shared secret.
        service: The calling service id.
        signed_at_ms: Signing time, in epoch milliseconds.
        body: The exact bytes that will be transmitted.

    Returns:
        A header value of the form ``v1:<service>:<millis>:<hex mac>``.
    """
    raw = secret.encode("utf-8") if isinstance(secret, str) else secret
    return f"v1:{service}:{signed_at_ms}:{_mac(raw, service, signed_at_ms, body)}"


def verify_internal_signature(
    secret: str | bytes,
    signature: str | None,
    service: str | None,
    now_ms: int,
    body: bytes,
    allowed_callers: frozenset[str] | set[str] = DEFAULT_ALLOWED_CALLERS,
) -> InternalServiceIdentity:
    """Verifies an inbound internal call.

    Args:
        secret: The shared secret. An empty secret fails closed: no call can
            ever be verified without one.
        signature: The presented ``x-mas-internal-signature`` value.
        service: The presented ``x-mas-internal-service`` value.
        now_ms: Current time in epoch milliseconds, supplied by the caller so
            the verifier stays pure and testable.
        body: The exact bytes received.
        allowed_callers: Service ids permitted to call this process.

    Returns:
        The proven identity.

    Raises:
        AuthenticationError: For every failure mode — missing header,
            malformed signature, unknown service, stale timestamp, or MAC
            mismatch. The message is identical in all cases, so the response
            is not an oracle for which check failed.
    """
    # One message for every failure. A caller must not be able to learn
    # whether the header was absent, the service was unknown, the clock had
    # drifted, or the key was wrong.
    refusal = AuthenticationError(
        "invalid internal service signature",
        details={"reason": "unauthenticated"},
    )

    if not secret:
        raise refusal
    if not signature or not service:
        raise refusal

    match = _SIGNATURE_RE.match(signature.strip())
    if match is None:
        raise refusal

    claimed_service, millis_text, presented_mac = match.groups()

    # The service id in the signature and in the header must agree. If they
    # do not, one of them was spliced in transit.
    if not hmac.compare_digest(claimed_service, service):
        raise refusal

    if claimed_service not in frozenset(allowed_callers):
        raise refusal

    try:
        signed_at_ms = int(millis_text)
    except ValueError:
        raise refusal from None

    if abs(now_ms - signed_at_ms) > MAX_SKEW_SECONDS * 1000:
        raise refusal

    raw = secret.encode("utf-8") if isinstance(secret, str) else secret
    expected = _mac(raw, claimed_service, signed_at_ms, body)
    if not hmac.compare_digest(presented_mac, expected):
        raise refusal

    return InternalServiceIdentity(service=claimed_service, signed_at_ms=signed_at_ms)


def now_millis() -> int:
    """Current time in epoch milliseconds.

    Exists as a function so tests can pin the clock and so no call site has
    to remember the millisecond conversion.
    """
    return int(time.time() * 1000)
