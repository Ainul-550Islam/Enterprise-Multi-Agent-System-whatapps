# PURPOSE: Tests for the Rust<->Python internal service authentication scheme.
#
# These tests pin the wire format. If either implementation changes its signed
# message, the interop vectors here and in
# `crates/security/src/internal_service.rs` are the single place where that
# divergence is caught before production.

from __future__ import annotations

import hashlib
import hmac
import time

import pytest

from mas_orchestrator.contracts.errors import AuthenticationError
from mas_orchestrator.security.internal_service import (
    MAX_SKEW_SECONDS,
    SERVICE_HEADER,
    SIGNATURE_HEADER,
    InternalServiceIdentity,
    now_millis,
    signed_internal_signature,
    verify_internal_signature,
)

SECRET = "s3cr3t"
BODY = b'{"tenant_id": "t-1", "operation": "run"}'

# The vector produced by the Rust implementation for
# secret="s3cr3t", service="mas-api", millis=1700000000000, body={"a":1},
# nonce="0123456789abcdefghijklmnopqrstuv". Fixed nonce input is test-only;
# real requests must use freshly generated randomness.
RUST_VECTOR_NONCE = "0123456789abcdefghijklmnopqrstuv"
RUST_VECTOR = (
    "v2:mas-api:1700000000000:0123456789abcdefghijklmnopqrstuv:"
    "ad29671c18b6926ff5c86fa205004b7b9e8675b6324bef6459aedead1e775766"
)
RUST_VECTOR_BODY = b'{"a":1}'
RUST_VECTOR_MILLIS = 1_700_000_000_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _sign(body: bytes = BODY, service: str = "mas-api", at_ms: int | None = None) -> str:
    return signed_internal_signature(
        SECRET, service, at_ms if at_ms is not None else _now_ms(), body
    )


# ---------------------------------------------------------------------------
# Header names
# ---------------------------------------------------------------------------


def test_header_names_match_the_rust_constants() -> None:
    """The metadata keys must agree with `mas-security`'s constants."""
    assert SIGNATURE_HEADER == "x-mas-internal-signature"
    assert SERVICE_HEADER == "x-mas-internal-service"


# ---------------------------------------------------------------------------
# Round trip
# ---------------------------------------------------------------------------


def test_a_fresh_signature_verifies() -> None:
    identity = verify_internal_signature(SECRET, _sign(), "mas-api", _now_ms(), BODY)
    assert isinstance(identity, InternalServiceIdentity)
    assert identity.service == "mas-api"


def test_the_mac_is_independent_of_the_transport_encoding() -> None:
    """Given the same nonce, byte-equivalent payloads produce the same MAC."""
    nonce = "0123456789abcdefghijklmnopqrstuv"
    first = signed_internal_signature(SECRET, "mas-api", 1_700_000_000_000, BODY, nonce=nonce)
    again = signed_internal_signature(
        SECRET, "mas-api", 1_700_000_000_000, bytes(BODY), nonce=nonce
    )
    assert first == again


def test_fresh_signatures_have_distinct_nonces() -> None:
    first = signed_internal_signature(SECRET, "mas-api", 1_700_000_000_000, BODY)
    second = signed_internal_signature(SECRET, "mas-api", 1_700_000_000_000, BODY)
    assert first != second


# ---------------------------------------------------------------------------
# Body binding
# ---------------------------------------------------------------------------


def test_a_signature_over_a_different_body_is_refused() -> None:
    """Signing one message and sending another is the tamper case."""
    with pytest.raises(AuthenticationError):
        verify_internal_signature(
            SECRET, _sign(BODY), "mas-api", _now_ms(), b'{"tenant_id": "t-2"}'
        )


def test_a_one_byte_body_change_is_detected() -> None:
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, _sign(BODY), "mas-api", _now_ms(), BODY + b" ")


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------


def test_a_signature_outside_the_skew_window_is_refused() -> None:
    old = _now_ms() - (MAX_SKEW_SECONDS * 1000) - 1
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, _sign(at_ms=old), "mas-api", _now_ms(), BODY)


def test_a_signature_at_the_edge_of_the_window_is_accepted() -> None:
    now = _now_ms()
    edge = now - (MAX_SKEW_SECONDS * 1000)
    identity = verify_internal_signature(SECRET, _sign(at_ms=edge), "mas-api", now, BODY)
    assert identity.service == "mas-api"


def test_a_signature_from_the_future_is_refused() -> None:
    """Clock skew is symmetric: a caller whose clock is ahead is rejected too."""
    future = _now_ms() + (MAX_SKEW_SECONDS * 1000) + 1
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, _sign(at_ms=future), "mas-api", _now_ms(), BODY)


# ---------------------------------------------------------------------------
# Secret and service
# ---------------------------------------------------------------------------


def test_a_signature_from_the_wrong_secret_is_refused() -> None:
    with pytest.raises(AuthenticationError):
        verify_internal_signature("different", _sign(), "mas-api", _now_ms(), BODY)


def test_an_empty_secret_refuses_everything() -> None:
    """Fail closed: an unconfigured secret is not an open door."""
    with pytest.raises(AuthenticationError):
        verify_internal_signature("", _sign(), "mas-api", _now_ms(), BODY)


def test_a_service_outside_the_allowlist_is_refused() -> None:
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, _sign(service="mas-rogue"), "mas-rogue", _now_ms(), BODY)


def test_a_custom_allowlist_narrows_who_may_call() -> None:
    signature = _sign(service="mas-api")
    with pytest.raises(AuthenticationError):
        verify_internal_signature(
            SECRET, signature, "mas-api", _now_ms(), BODY, allowed_callers={"mas-worker"}
        )


def test_a_spliced_service_header_is_refused() -> None:
    """Signature says one service, header says another: reject."""
    signature = _sign(service="mas-api")
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, signature, "mas-worker", _now_ms(), BODY)


# ---------------------------------------------------------------------------
# Malformed input
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "signature",
    [
        None,
        "",
        "   ",
        "v1",
        "v1:mas-api",
        "v1:mas-api:1700000000000",
        "v2:mas-api:1700000000000:" + "a" * 64,
        "v2:mas-api:notanumber:0123456789abcdefghijklmnopqrstuv:" + "a" * 64,
        "v2:mas-api:1700000000000:too-short:zzzz",
        "v2:mas-api:1700000000000:0123456789abcdefghijklmnopqrstuv:" + "a" * 63,
        "v2:mas-api:1700000000000:0123456789abcdefghijklmnopqrstuv:" + "a" * 65,
        "v2::1700000000000:0123456789abcdefghijklmnopqrstuv:" + "a" * 64,
    ],
)
def test_malformed_signatures_are_refused(signature: str | None) -> None:
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, signature, "mas-api", _now_ms(), BODY)


def test_a_missing_service_header_is_refused() -> None:
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, _sign(), None, _now_ms(), BODY)


def test_every_failure_produces_the_same_message() -> None:
    """A prober must not be able to tell which check rejected the call."""
    messages = set()
    for signature, service, secret, body in (
        (None, "mas-api", SECRET, BODY),
        (_sign(), "mas-api", "wrong", BODY),
        (_sign(), "mas-rogue", SECRET, BODY),
        (_sign(at_ms=_now_ms() - 10_000_000), "mas-api", SECRET, BODY),
        (_sign(), "mas-api", SECRET, b"other"),
        ("garbage", "mas-api", SECRET, BODY),
    ):
        with pytest.raises(AuthenticationError) as raised:
            verify_internal_signature(secret, signature, service, _now_ms(), body)
        messages.add(str(raised.value))
    assert len(messages) == 1, f"failure messages differ: {messages}"


# ---------------------------------------------------------------------------
# Cross-language interop
# ---------------------------------------------------------------------------


def test_the_signed_message_format_matches_the_documented_layout() -> None:
    """Pins the exact bytes under the MAC, so a change to the format on either
    side is caught here rather than in production."""
    digest = hashlib.sha256(RUST_VECTOR_BODY).hexdigest()
    message = (f"v2:mas-api:{RUST_VECTOR_MILLIS}:{RUST_VECTOR_NONCE}:{digest}").encode()
    expected = hmac.new(SECRET.encode(), message, hashlib.sha256).hexdigest()
    assert RUST_VECTOR.endswith(expected)


def test_python_produces_the_rust_vector() -> None:
    produced = signed_internal_signature(
        SECRET,
        "mas-api",
        RUST_VECTOR_MILLIS,
        RUST_VECTOR_BODY,
        nonce=RUST_VECTOR_NONCE,
    )
    assert produced == RUST_VECTOR


def test_python_accepts_the_rust_vector() -> None:
    identity = verify_internal_signature(
        SECRET, RUST_VECTOR, "mas-api", RUST_VECTOR_MILLIS, RUST_VECTOR_BODY
    )
    assert identity.service == "mas-api"
    assert identity.signed_at_ms == RUST_VECTOR_MILLIS
    assert identity.nonce == RUST_VECTOR_NONCE


def test_python_rejects_the_rust_vector_over_a_tampered_body() -> None:
    with pytest.raises(AuthenticationError):
        verify_internal_signature(SECRET, RUST_VECTOR, "mas-api", RUST_VECTOR_MILLIS, b'{"a":2}')


# ---------------------------------------------------------------------------
# Non-disclosure
# ---------------------------------------------------------------------------


def test_the_secret_never_appears_in_a_failure() -> None:
    with pytest.raises(AuthenticationError) as raised:
        verify_internal_signature(SECRET, "garbage", "mas-api", _now_ms(), BODY)
    assert SECRET not in str(raised.value)
    assert SECRET not in repr(raised.value)


def test_the_identity_never_appears_with_a_secret() -> None:
    identity = verify_internal_signature(SECRET, _sign(), "mas-api", _now_ms(), BODY)
    assert SECRET not in str(identity)
    assert SECRET not in repr(identity)


# ---------------------------------------------------------------------------
# Clock helper
# ---------------------------------------------------------------------------


def test_now_millis_is_milliseconds_not_seconds() -> None:
    assert now_millis() > 1_600_000_000_000
