"""Tests for one-time nonce consumption used by internal service auth v2."""

from __future__ import annotations

import asyncio

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


class _SharedReceiptAuthority:
    """One durable receipt table shared by several guard "processes".

    Implements the exact semantics of the production ``INSERT … ON CONFLICT
    (service_name, nonce) DO NOTHING RETURNING nonce``: the first insert wins
    atomically and later inserts report a replay.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._rows: set[tuple[str, str]] = set()

    async def insert_ignoring_conflict(self, service: str, nonce: str) -> bool:
        async with self._lock:
            key = (service, nonce)
            if key in self._rows:
                return False
            self._rows.add(key)
            return True


class _FakeCursor:
    def __init__(self, authority: _SharedReceiptAuthority) -> None:
        self._authority = authority
        self._inserted = False

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def execute(self, sql: str, params: object) -> None:
        service, nonce, _signed_at_ms, _expires_at = params  # type: ignore[misc]
        self._inserted = await self._authority.insert_ignoring_conflict(service, nonce)

    async def fetchone(self) -> object:
        return (self._inserted,) if self._inserted else None


class _FakeConnection:
    def __init__(self, authority: _SharedReceiptAuthority) -> None:
        self._authority = authority

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._authority)

    async def __aenter__(self) -> _FakeConnection:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None


def _share_receipt_authority(
    monkeypatch: pytest.MonkeyPatch, authority: _SharedReceiptAuthority
) -> None:
    """Routes every guard instance to one shared receipt authority."""

    async def _connect(_url: str) -> _FakeConnection:
        return _FakeConnection(authority)

    import psycopg

    monkeypatch.setattr(psycopg.AsyncConnection, "connect", _connect)


async def test_two_racers_consume_a_fresh_nonce_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two replicas of one signed request race: one accepted, one UNAUTHENTICATED."""
    authority = _SharedReceiptAuthority()
    _share_receipt_authority(monkeypatch, authority)
    replica_a = PostgresReplayGuard("postgresql://replica-a/mas")
    replica_b = PostgresReplayGuard("postgresql://replica-b/mas")

    results = await asyncio.gather(
        replica_a.consume(_identity()),
        replica_b.consume(_identity()),
    )

    assert sorted(results) == [False, True]


async def test_a_captured_nonce_stays_consumed_across_a_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The receipt survives a guard restart: a captured request stays dead."""
    authority = _SharedReceiptAuthority()
    _share_receipt_authority(monkeypatch, authority)
    before_restart = PostgresReplayGuard("postgresql://replica-a/mas")
    after_restart = PostgresReplayGuard("postgresql://replica-a/mas")

    assert await before_restart.consume(_identity()) is True
    assert await after_restart.consume(_identity()) is False
