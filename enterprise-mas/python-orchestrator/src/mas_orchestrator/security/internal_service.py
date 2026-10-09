"""Versioned, body-bound internal service authentication.

Version 2 signs a cryptographically random nonce as well as the service,
timestamp, and SHA-256 body digest.  Verifiers must pair this module with a
durable replay receipt store and atomically consume that nonce before accepting
work; freshness alone cannot stop a captured request being replayed inside its
clock-skew window.

Wire format, shared byte-for-byte with ``mas-security``:

``v2:<service>:<unix_millis>:<nonce>:<hmac-sha256-hex>``

The HMAC input is the prefix through ``nonce`` plus the body digest.  The
nonce is contained in the MAC, not merely placed beside it in metadata.
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
    "DEFAULT_ALLOWED_CALLERS",
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
_VERSION: Final[str] = "v2"
# 24 random bytes URL-safe encoded without padding produce 32 portable ASCII
# characters. The range allows future nonce generators while keeping metadata
# and database index size bounded.
_NONCE_RE: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9_-]{22,128}")
_SIGNATURE_RE: Final[re.Pattern[str]] = re.compile(
    r"\Av2:([A-Za-z0-9_-]{1,64}):(\d{1,20}):([A-Za-z0-9_-]{22,128}):([0-9a-f]{64})\Z"
)


@dataclass(frozen=True, slots=True)
class InternalServiceIdentity:
    """A cryptographically verified request identity awaiting nonce consumption."""

    service: str
    signed_at_ms: int
    nonce: str
    expires_at_ms: int

    def __str__(self) -> str:
        """Renders safe audit fields, never shared key material."""
        return (
            "InternalServiceIdentity("
            f"service={self.service}, signed_at_ms={self.signed_at_ms}, nonce=<present>)"
        )


def _body_digest(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _mac(secret: bytes, service: str, signed_at_ms: int, nonce: str, body: bytes) -> str:
    message = f"{_VERSION}:{service}:{signed_at_ms}:{nonce}:{_body_digest(body)}".encode("ascii")
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


def _new_nonce() -> str:
    return secrets.token_urlsafe(24).rstrip("=")


def signed_internal_signature(
    secret: str | bytes,
    service: str,
    signed_at_ms: int,
    body: bytes,
    *,
    nonce: str | None = None,
) -> str:
    """Builds a v2 signature; tests may provide ``nonce`` for stable vectors."""
    if nonce is not None and not _NONCE_RE.fullmatch(nonce):
        raise ValueError("internal signature nonce is malformed")
    selected_nonce = nonce or _new_nonce()
    raw = secret.encode("utf-8") if isinstance(secret, str) else secret
    return (
        f"{_VERSION}:{service}:{signed_at_ms}:{selected_nonce}:"
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
    """Verifies v2 authenticity/freshness; caller must consume its nonce once.

    All invalid forms produce the same :class:`AuthenticationError`, avoiding
    an authentication oracle.  Receipt-store errors are intentionally handled
    by the process edge, where a durable backend can fail closed.
    """
    refusal = AuthenticationError(
        "invalid internal service signature",
        details={"reason": "unauthenticated"},
    )
    if not secret or not signature or not service:
        raise refusal
    claimed_header_service = service.strip()
    if not claimed_header_service:
        raise refusal
    match = _SIGNATURE_RE.match(signature.strip())
    if match is None:
        raise refusal
    claimed_service, millis_text, nonce, presented_mac = match.groups()
    if not claimed_header_service.isascii() or not hmac.compare_digest(
        claimed_service, claimed_header_service
    ):
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
        expires_at_ms=signed_at_ms + MAX_SKEW_SECONDS * 1000,
    )


def now_millis() -> int:
    """Current epoch time in milliseconds."""
    return int(time.time() * 1000)
