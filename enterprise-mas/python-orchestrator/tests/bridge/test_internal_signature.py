# PURPOSE:
# Tests that the bridge verifies the body-bound, time-bounded signature on
# every inbound internal call, and that the verification is a real check and
# not a header presence test.
#
# Covers spec §9 and §10: internal service authentication. Being able to reach
# the port is not an identity, and holding a bearer token is not proof that
# *this* message was sent by its holder. Only an HMAC over the exact request
# bytes, bound to the calling service and to the moment it was made, establishes
# that. Every rejection must be indistinguishable from every other, so the
# response is not an oracle for which check failed.

"""Body-bound internal signature tests for the Rust -> Python bridge.

The threat these tests encode is a caller that holds (or has captured) a valid
bearer credential and can therefore reach the port, but is not the service it
claims to be — or is replaying a call it captured earlier, or has altered the
body in flight.

Four variants matter and are tested separately:

* **Unsigned** — a valid credential and no signature. Refused: a configured
  secret makes the signature mandatory.
* **Tampered** — a valid signature over a *different* body. Refused: a
  signature over one request is not a signature over another.
* **Unknown service** — a syntactically perfect signature from a service that
  is not on the allowlist. Refused.
* **Stale** — a valid signature whose timestamp is outside the skew window.
  Refused: freshness bounds nonce lifetime.
* **Replay** — the exact valid signature sent a second time. Refused because
  its nonce was atomically consumed before the first execution.

The positive case is tested too, because a verification that refuses
everything is not a working control — it is an outage.
"""

from __future__ import annotations

import grpc
import pytest
from tests.bridge.conftest import (
    INTERNAL_SERVICE,
    SIGNING_SECRET,
    BridgeHarness,
    internal_signature_pairs,
    metadata,
    request,
)

from mas_orchestrator.grpc.errors import is_retryable_status
from mas_orchestrator.security.internal_service import (
    MAX_SKEW_SECONDS,
    now_millis,
)

pytestmark = pytest.mark.asyncio(loop_scope="function")


async def _run(
    harness: BridgeHarness,
    message=None,
    extra_pairs: list[tuple[str, str]] | None = None,
):
    """Calls ``RunAgent`` with the given metadata appended to the standard set.

    Args:
        harness: The running bridge.
        message: The request to send. Built fresh when omitted.
        extra_pairs: Additional metadata pairs (typically the signature).

    Returns:
        The reply.

    Raises:
        grpc.aio.AioRpcError: When the call is aborted.
    """
    message = message if message is not None else request()
    pairs = metadata(tenant_id=message.tenant_id, organization_id=message.organization_id) + list(
        extra_pairs or ()
    )
    stub, channel = harness.stub()
    try:
        return await stub.RunAgent(message, metadata=pairs)
    finally:
        await channel.close()


class TestSignedCallIsAccepted:
    """A correctly signed call must reach the runtime."""

    async def test_a_signed_call_dispatches(self, signed_bridge: BridgeHarness) -> None:
        """A valid signature over the exact body is accepted and dispatched."""
        message = request()
        await _run(signed_bridge, message, internal_signature_pairs(message))

        assert signed_bridge.service.call_count == 1

    async def test_an_unsigned_bridge_still_accepts_unsigned_calls(
        self, bridge: BridgeHarness
    ) -> None:
        """With no secret configured the check is skipped, not failed.

        This is the development posture. It is asserted explicitly so a change
        that accidentally makes an unprovisioned bridge refuse all traffic is
        caught here rather than in an environment.
        """
        await _run(bridge, request(), None)

        assert bridge.service.call_count == 1


class TestUnsignedCallIsRefused:
    """A configured secret makes the signature mandatory."""

    async def test_a_valid_credential_without_a_signature_is_refused(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """The bearer token alone is not enough once signing is configured."""
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(signed_bridge, request(), None)

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 0

    async def test_a_refused_signature_is_never_retryable(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """Authentication failures are permanent; retrying them is an outage."""
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(signed_bridge, request(), None)

        assert is_retryable_status(raised.value) is False


class TestTamperedBodyIsRefused:
    """A signature over one message is not a signature over another."""

    async def test_a_signature_over_a_different_body_is_refused(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """Sign one request, send another: the MAC cannot match."""
        signed_for = request(input_json="hello")
        sent = request(input_json="hello but different")

        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(signed_bridge, sent, internal_signature_pairs(signed_for))

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 0

    async def test_a_signature_over_a_different_tenant_is_refused(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """Retargeting a captured call at another tenant fails the MAC.

        This is the cross-tenant case for the bridge: an attacker who captures
        tenant A's signed call cannot replay it against tenant B, because the
        tenant is inside the signed bytes.
        """
        signed_for = request(tenant_id="tenant-alpha")
        retargeted = request(tenant_id="tenant-beta")

        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(signed_bridge, retargeted, internal_signature_pairs(signed_for))

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 0

    async def test_a_signature_over_a_different_service_is_refused(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """Signing as an unpermitted service is refused even with the right key."""
        message = request()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(
                signed_bridge,
                message,
                internal_signature_pairs(message, service="mas-attacker"),
            )

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 0


class TestReplayIsRefused:
    """A fresh nonce is consumed before bridge work begins."""

    async def test_same_valid_signature_cannot_run_twice(
        self, signed_bridge: BridgeHarness
    ) -> None:
        message = request()
        signed_headers = internal_signature_pairs(message)

        await _run(signed_bridge, message, signed_headers)
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(signed_bridge, message, signed_headers)

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 1


class TestStaleSignatureIsRefused:
    """Timestamp freshness bounds the useful life of a signed nonce."""

    async def test_a_signature_outside_the_skew_window_is_refused(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """A captured call replayed an hour later does not verify."""
        message = request()
        stale = now_millis() - (MAX_SKEW_SECONDS * 1000) - 60_000
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(signed_bridge, message, internal_signature_pairs(message, at_ms=stale))

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 0

    async def test_a_signature_from_the_future_is_refused(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """Skew is measured in both directions, not only backwards."""
        message = request()
        future = now_millis() + (MAX_SKEW_SECONDS * 1000) + 60_000
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(signed_bridge, message, internal_signature_pairs(message, at_ms=future))

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 0


class TestWrongSecretIsRefused:
    """A signature under a different secret proves nothing."""

    async def test_a_signature_under_another_secret_is_refused(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """The MAC is keyed; a correct structure with the wrong key fails."""
        message = request()
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await _run(
                signed_bridge,
                message,
                internal_signature_pairs(message, secret="not-the-secret"),
            )

        assert raised.value.code() is grpc.StatusCode.UNAUTHENTICATED
        assert signed_bridge.service.call_count == 0

    async def test_every_refusal_is_indistinguishable(self, signed_bridge: BridgeHarness) -> None:
        """No failure mode may be told apart from another by its message.

        If "wrong key" and "stale timestamp" produced different responses, the
        bridge would be an oracle that lets a prober confirm which secret it
        guessed correctly.
        """
        message = request()
        messages = []

        variants: list[list[tuple[str, str]] | None] = [
            None,
            internal_signature_pairs(request(input_json="other")),
            internal_signature_pairs(message, secret="not-the-secret"),
            internal_signature_pairs(message, service="mas-attacker"),
            internal_signature_pairs(message, at_ms=now_millis() - 600_000),
        ]
        for pairs in variants:
            with pytest.raises(grpc.aio.AioRpcError) as raised:
                await _run(signed_bridge, message, pairs)
            messages.append(raised.value.details())

        assert len(set(messages)) == 1, f"refusals differ: {messages}"
        assert signed_bridge.service.call_count == 0


class TestServiceAllowlist:
    """The allowlist is enforced even for a cryptographically valid signature."""

    async def test_the_configured_worker_service_is_accepted(
        self, signed_bridge: BridgeHarness
    ) -> None:
        """The id the Rust worker presents is on the default allowlist."""
        message = request()
        await _run(signed_bridge, message, internal_signature_pairs(message))

        assert INTERNAL_SERVICE == "mas-worker"
        assert SIGNING_SECRET
        assert signed_bridge.service.call_count == 1
