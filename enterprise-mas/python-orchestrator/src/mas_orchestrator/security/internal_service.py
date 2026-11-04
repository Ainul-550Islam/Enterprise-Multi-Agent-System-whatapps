"""Body-bound, time-bounded internal service authentication.

Version two binds a freshly generated nonce, service identity, timestamp and
SHA-256 digest of the exact request body to an HMAC. A verified identity is not
sufficient to authorize work: the receiver must atomically consume its nonce in
a shared replay receipt store before accepting the request.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import time
from dataclasses import dataclass
from typing import Final

from mas_orchestrator.contracts.errors import AuthenticationError

__all__ = [
    "MAX_SKEW_SECONDS",
    "SERVICE_HEADER",
    "SIGNATURE_HEADER",
    "InternalServiceIdentity",
    "now_millis",
    "signed_internal_signature",
    "verify_internal_signature",
]

SIGNATURE_HEADER: Final[str] = "x-mas-internal-signature"
SERVICE_HEADER: Final[str] = "x-mas-internal-service"
MAX_SKEW_SECONDS: Final[int] = 60
DEFAULT_ALLOWED_CALLERS: Final[frozenset[str]] = frozenset({"mas-api", "mas-worker"})
_SIGNATURE_RE: Final[re.Pattern[str]] = re.compile(
    r"\Av2:([A-Za-z0-9_-]{1,64}):([0-9]{1,19}):([A-Za-z0-9_-]{22,128}):([0-9a-f]{64})\Z"
)
_SERVICE_RE: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z0-9_-]{1,64}\Z")
_NONCE_RE: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z0-9_-]{22,128}\Z")
_MAX_SIGNED_MILLIS: Final[int] = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class InternalServiceIdentity:
    """Authenticated service and nonce awaiting durable replay consumption."""

    service: str
    signed_at_ms: int
    nonce: str
    expires_at_ms: int

    def __str__(self) -> str:
        """Render the identity without key material or a signed nonce."""
        return f"InternalServiceIdentity(service={self.service}, signed_at_ms={self.signed_at_ms})"


def _body_digest(body: bytes) -> str:
    """Return the SHA-256 digest of the exact transmitted body."""
    return hashlib.sha256(body).hexdigest()


def _mac(secret: bytes, service: str, signed_at_ms: int, nonce: str, body: bytes) -> str:
    """Compute the Rust-compatible v2 HMAC over the canonical message."""
    message = f"v2:{service}:{signed_at_ms}:{nonce}:{_body_digest(body)}".encode("ascii")
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def signed_internal_signature(
    secret: str | bytes,
    service: str,
    signed_at_ms: int,
    body: bytes,
    *,
    nonce: str | None = None,
) -> str:
    """Sign a request with a fresh nonce, or a supplied deterministic test nonce."""
    raw = secret.encode("utf-8") if isinstance(secret, str) else secret
    selected_nonce = secrets.token_urlsafe(24) if nonce is None else nonce
    if not raw or not _SERVICE_RE.fullmatch(service):
        raise ValueError("invalid internal signature configuration")
    if not _NONCE_RE.fullmatch(selected_nonce):
        raise ValueError("invalid internal signature nonce")
    if not 0 <= signed_at_ms <= _MAX_SIGNED_MILLIS:
        raise ValueError("invalid internal signature timestamp")
    return (
        f"v2:{service}:{signed_at_ms}:{selected_nonce}:"
        f"{_mac(raw, service, signed_at_ms, selected_nonce, body)}"
    )


def verify_internal_signature(
    secret: str | bytes,
    signature: str | None,
    service: str | None,
    now_ms: int,
    body: bytes,
    allowed_callers: frozenset[str] | set[str] = DEFAULT_ALLOWED_CALLERS,
) -> InternalServiceIdentity:
    """Verify a v2 signature; the caller must consume its nonce before work.

    All invalid requests produce the same authentication error. Version one is
    rejected to prevent downgrade to the timestamp-only replayable scheme.
    """
    refusal = AuthenticationError(
        "invalid internal service signature",
        details={"reason": "unauthenticated"},
    )
    if not secret or not signature or not service or not _SERVICE_RE.fullmatch(service):
        raise refusal

    match = _SIGNATURE_RE.fullmatch(signature.strip())
    if match is None:
        raise refusal
    claimed_service, millis_text, nonce, presented_mac = match.groups()
    if not hmac.compare_digest(claimed_service, service):
        raise refusal
    if claimed_service not in allowed_callers:
        raise refusal
    signed_at_ms = int(millis_text)
    if signed_at_ms > _MAX_SIGNED_MILLIS:
        raise refusal
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
        expires_at_ms=min(_MAX_SIGNED_MILLIS, signed_at_ms + MAX_SKEW_SECONDS * 1000),
    )


def now_millis() -> int:
    """Return the current Unix time in milliseconds."""
    return int(time.time() * 1000)
