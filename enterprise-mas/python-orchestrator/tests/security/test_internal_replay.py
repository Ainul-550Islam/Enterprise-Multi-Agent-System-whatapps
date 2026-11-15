"""Tests for one-time nonce consumption used by internal service auth v2."""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.internal_replay import InMemoryReplayGuard, PostgresReplayGuard
from mas_orchestrator.security.internal_service import InternalServiceIdentity

pytestmark = pytest.mark.asyncio(loop_scope="function")


def _identity(nonce: str = "0123456789abcdefghijklmnopqrstuv") -> InternalServiceIdentity:
    return InternalServiceIdentity(
        service="mas-worker",
        signed_at_ms=1_700_000_000_000,
        nonce=nonce,
        expires_at_ms=4_102_444_800_000,
    )


async def test_in_memory_guard_accepts_exactly_one_use_of_a_nonce() -> None:
    guard = InMemoryReplayGuard()
    identity = _identity()

    assert await guard.consume(identity) is True
    assert await guard.consume(identity) is False
    assert await guard.consume(_identity("abcdefghijklmnopqrstuvwxyz012345")) is True


async def test_postgres_guard_fails_closed_when_the_receipt_store_is_unavailable() -> None:
    guard = PostgresReplayGuard("postgresql://127.0.0.1:1/mas")

    with pytest.raises(ConfigurationError) as raised:
        await guard.consume(_identity())

    assert "internal replay receipt store is unavailable" in str(raised.value)
