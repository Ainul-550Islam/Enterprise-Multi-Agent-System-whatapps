# PURPOSE: Body-bound, time-bounded, replay-safe internal service
# authentication for the Rust Core -> Python orchestrator bridge (scheme v2).
#
# Owning layer: `mas_orchestrator.security` (the Python process edge).
#
# Production responsibility: verify that an inbound bridge call was made by a
# platform service that holds the shared secret, that the call was made in the
# last `MAX_SKEW_SECONDS`, that the exact bytes now being processed are the
# bytes that were signed, and — through the returned nonce — that this exact
# request has not already been consumed by a durable replay receipt store.
#
# Security boundary: this scheme proves *which service* called, that the
# message was not tampered with in flight, and (with the replay guard) that it
# is not a replay. It does NOT establish tenancy — the tenant is authoritative
# data supplied by Rust in the request body, and Python validates it against
# the resolved scope. Network reachability is never an identity.
#
# It must NOT:
#   • accept a signature over anything other than the exact request bytes;
#   • accept a timestamp outside the skew window;
#   • accept a caller that is not on the allowlist;
#   • accept a v1 (timestamp-only, nonce-free) signature — there is no
#     downgrade path, because v1 has no replay defence at all;
#   • distinguish "missing header" from "wrong secret" in the error it raises —
#     every failure is the same `AuthenticationError`, so a prober learns
#     nothing;
#   • log, render, or include the secret in any exception detail.
#
# The scheme is byte-for-byte compatible with
# `mas-security::internal_service` (Rust) and with
# `rust-core/docs/internal-service-authentication-v2.md`:
#
#     body_digest    = hex(SHA256(body))
#     signed_message = "v2:" + service + ":" + unix_millis + ":" + nonce + ":" + body_digest
#     signature      = signed_message + ":" + hex(HMAC_SHA256(secret, signed_message))
#
# Deterministic cross-language vector (asserted by both sides' test suites):
#
#     secret:    s3cr3t
#     service:   mas-api
#     millis:    1700000000000
#     nonce:     0123456789abcdefghijklmnopqrstuv
#     body:      {"a":1}
#     signature: v2:mas-api:1700000000000:0123456789abcdefghijklmnopqrstuv:
#                ad29671c18b6926ff5c86fa205004b7b9e8675b6324bef6459aedead1e775766

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
import time
from dataclasses import dataclass
from typing import Final

from mas_orchestrator.contracts.errors import AuthenticationError

__all__ = [
    "DEFAULT_ALLOWED_CALLERS",
    "MAX_NONCE_LENGTH",
    "MAX_SKEW_SECONDS",
    "MIN_NONCE_LENGTH",
    "SERVICE_HEADER",
    "SIGNATURE_HEADER",
    "SIGNATURE_VERSION",
    "InternalServiceIdentity",
    "new_nonce",
    "now_millis",
    "signed_internal_signature",
    "verify_internal_signature",
]

#: Metadata key carrying the signature.
SIGNATURE_HEADER: Final[str] = "x-mas-internal-signature"

#: Metadata key carrying the calling service's claimed id.
SERVICE_HEADER: Final[str] = "x-mas-internal-service"

#: The only accepted signature version. v1 is refused, deliberately.
SIGNATURE_VERSION: Final[str] = "v2"

#: Largest acceptable clock skew, in seconds.
#:
#: 60 seconds is deliberately tight: it bounds the window in which a captured
#: request can be replayed, while tolerating ordinary clock drift between two
#: hosts in the same cluster. A receipt store therefore has to remember a
#: nonce only until ``signed_at + MAX_SKEW``; after that the freshness check
#: rejects every use anyway.
MAX_SKEW_SECONDS: Final[int] = 60

#: Accepted nonce length band. Mirrors Rust's `MIN_NONCE_LEN`/`MAX_NONCE_LEN`
#: and accommodates a 24-byte URL-safe base64 value (32 characters, no
#: padding) as well as any shorter test vector down to 22 characters.
MIN_NONCE_LENGTH: Final[int] = 22
MAX_NONCE_LENGTH: Final[int] = 128

#: Only these services may call the orchestrator's internal endpoints.
DEFAULT_ALLOWED_CALLERS: Final[frozenset[str]] = frozenset({"mas-api", "mas-worker"})

#: `v2:<service>:<millis>:<nonce>:<64 hex chars>`. Every group is bounded so a
#: hostile header cannot make the regex do work proportional to its length.
_SIGNATURE_RE: Final[re.Pattern[str]] = re.compile(
    r"\Av2:([A-Za-z0-9_-]{1,64}):(\d{1,20}):([A-Za-z0-9_-]{22,128}):([0-9a-f]{64})\Z"
)

#: Alphabet a nonce may use. Mirrors Rust's `valid_nonce`.
_NONCE_CHARSET: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z0-9_-]+\Z")


@dataclass(frozen=True, slots=True)
class InternalServiceIdentity:
    """The result of a successful verification.

    Attributes:
        service: The calling service id, proven against the allowlist.
        signed_at_ms: The timestamp the caller signed, in epoch milliseconds.
        nonce: The signed random request identity. It must be consumed exactly
            once by a durable replay guard before application work begins.
    """

    service: str
    signed_at_ms: int
    nonce: str

    @property
    def expires_at_ms(self) -> int:
        """When this nonce stops being acceptable at all.

        The freshness window already rejects anything older, so a receipt
        store only has to remember the nonce until then. Keeping a receipt
        for a nonce whose every use would be refused is pure garbage.
        """
        return self.signed_at_ms + MAX_SKEW_SECONDS * 1000

    def __str__(self) -> str:
        """Renders the service, timestamp and nonce — never key material."""
        return (
            f"InternalServiceIdentity(service={self.service}, "
            f"signed_at_ms={self.signed_at_ms}, nonce={self.nonce})"
        )


def _body_digest(body: bytes) -> str:
    """Hex SHA-256 of *body*.

    The digest stands in for the body inside the MAC so that arbitrarily large
    payloads cost one hash rather than being buffered twice, while still
    binding the signature to the exact message.
    """
    return hashlib.sha256(body).hexdigest()


def _mac(secret: bytes, service: str, signed_at_ms: int, nonce: str, body: bytes) -> str:
    """Computes the hex HMAC-SHA256 for the message.

    The signed message is
    ``v2:<service>:<millis>:<nonce>:<hex sha256(body)>`` — the same string the
    signature header begins with, so a reader comparing the two can see at a
    glance that the version, service, timestamp, nonce and body digest are all
    inside the MAC and not merely adjacent to it.
    """
    message = f"v2:{service}:{signed_at_ms}:{nonce}:{_body_digest(body)}".encode()
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def new_nonce() -> str:
    """Returns a fresh 24-byte URL-safe nonce without padding (32 characters).

    Uses :mod:`secrets`, not :mod:`random`: a predictable nonce would let an
    attacker pre-compute a receipt collision and replay a captured request
    inside the freshness window.
    """
    return base64.urlsafe_b64encode(secrets.token_bytes(24)).rstrip(b"=").decode("ascii")


def signed_internal_signature(
    secret: str | bytes,
    service: str,
    signed_at_ms: int,
    body: bytes,
    *,
    nonce: str | None = None,
) -> str:
    """Builds a signature header value.

    Args:
        secret: The shared secret.
        service: The calling service id.
        signed_at_ms: Signing time, in epoch milliseconds.
        body: The exact bytes that will be transmitted.
        nonce: The request nonce. Omitted by production call sites so a fresh
            random one is generated per call; supplied only to reproduce a
            deterministic cross-language vector.

    Returns:
        A header value of the form ``v2:<service>:<millis>:<nonce>:<hex mac>``.

    Raises:
        AuthenticationError: If a supplied nonce is outside the accepted band
            or charset. A caller-chosen nonce is exactly the replay vector
            this scheme exists to close, so a malformed one fails closed
            rather than being silently replaced.
    """
    raw = secret.encode("utf-8") if isinstance(secret, str) else secret
    request_nonce = new_nonce() if nonce is None else nonce
    if not MIN_NONCE_LENGTH <= len(request_nonce) <= MAX_NONCE_LENGTH:
        raise AuthenticationError(
            "internal service nonce is outside the accepted length band",
            details={"reason": "invalid_nonce"},
        )
    if not _NONCE_CHARSET.fullmatch(request_nonce):
        raise AuthenticationError(
            "internal service nonce contains characters outside the accepted alphabet",
            details={"reason": "invalid_nonce"},
        )
    return (
        f"v2:{service}:{signed_at_ms}:{request_nonce}:"
        f"{_mac(raw, service, signed_at_ms, request_nonce, body)}"
    )


def verify_internal_signature(
    secret: str | bytes,
    signature: str | None,
    service: str | None,
    now_ms: int,
    body: bytes,
    allowed_callers: frozenset[str] | set[str] = DEFAULT_ALLOWED_CALLERS,
) -> InternalServiceIdentity:
    """Verifies an inbound internal call's cryptographic facts.

    This function proves *authenticity and freshness*. It does not prove
    single use: the caller must atomically consume
    :attr:`InternalServiceIdentity.nonce` in a durable receipt store before
    doing any work. Accepting a request without that consumption is the replay
    hole v2 exists to close.

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
        The proven identity, including the nonce to consume.

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

    claimed_service, millis_text, nonce, presented_mac = match.groups()

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
    expected = _mac(raw, claimed_service, signed_at_ms, nonce, body)
    if not hmac.compare_digest(presented_mac, expected):
        raise refusal

    return InternalServiceIdentity(
        service=claimed_service,
        signed_at_ms=signed_at_ms,
        nonce=nonce,
    )


def now_millis() -> int:
    """Current time in epoch milliseconds.

    Exists as a function so tests can pin the clock and so no call site has
    to remember the millisecond conversion.
    """
    return int(time.time() * 1000)
