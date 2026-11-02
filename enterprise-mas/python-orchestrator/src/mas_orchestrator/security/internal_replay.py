"""Durable replay receipts for internal-service request signatures.

A signed timestamp limits how long a captured request is useful, but it does
not prevent the same fresh request from being sent twice.  The receipt store is
therefore part of authentication: it atomically consumes the signed nonce
before an authenticated request is allowed to enter application work.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.internal_service import MAX_SKEW_SECONDS

if TYPE_CHECKING:
    from mas_orchestrator.security.internal_service import InternalServiceIdentity


@runtime_checkable
class InternalReplayGuard(Protocol):
    """Atomically consumes a nonce exactly once until its signed expiry."""

    async def consume(self, identity: InternalServiceIdentity) -> bool:
        """Returns ``True`` only for the first accepted use of the nonce."""
        raise NotImplementedError


class InMemoryReplayGuard:
    """Process-local guard for unit tests and explicitly non-production runs.

    It is intentionally not composed in production: replicas and restarts do
    not share this map, so it cannot be the security boundary there.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._receipts: dict[tuple[str, str], int] = {}

    async def consume(self, identity: InternalServiceIdentity) -> bool:
        """Records the nonce, returning ``False`` when it was already seen.

        The receipt is retained for the full skew window measured from the
        moment it is first seen — ``max(signed_at, now) + MAX_SKEW_SECONDS`` —
        rather than only until the signature's own expiry.

        That distinction matters. A signer whose clock runs slightly ahead of
        this process produces an identity that is already past its nominal
        expiry here, and retaining it "until its expiry" would insert an
        already-stale row that the next call garbage-collects before it can
        detect the replay. Anchoring retention to the later of the two clocks
        keeps the nonce remembered for at least one full window no matter
        which side is ahead, and never for longer than that.
        """
        now_ms = int(time.time() * 1000)
        retain_until_ms = max(identity.signed_at_ms, now_ms) + MAX_SKEW_SECONDS * 1000
        key = (identity.service, identity.nonce)
        async with self._lock:
            expired = [entry for entry, expiry in self._receipts.items() if expiry <= now_ms]
            for expired_key in expired:
                del self._receipts[expired_key]
            if key in self._receipts:
                return False
            self._receipts[key] = retain_until_ms
            return True


@dataclass(frozen=True, slots=True)
class PostgresReplayGuard:
    """Cross-replica PostgreSQL nonce consumer used by production composition."""

    database_url: str

    async def consume(self, identity: InternalServiceIdentity) -> bool:
        """Inserts one receipt with conflict-as-replay semantics.

        Returns ``True`` only when this call inserted the receipt, i.e. only
        for the first use of the nonce. Every other outcome — a duplicate, an
        unreachable database, a missing driver — fails the request closed.

        Raises:
            ConfigurationError: If the receipt store cannot be reached or its
                driver is absent. Authentication must never succeed because
                the authority that decides "seen this already" was missing;
                that is the entire difference between v2 and the timestamp-only
                v1 scheme this replaces.
        """
        try:
            # Imported inside the guarded block, not at module scope: psycopg
            # is an optional extra, and a missing driver has to be a
            # fail-closed *authentication* error rather than an import-time
            # crash of the whole service in deployments that never configured
            # durable receipts.
            import psycopg

            async with (
                await psycopg.AsyncConnection.connect(self.database_url) as connection,
                connection.cursor() as cursor,
            ):
                await cursor.execute(
                    """
                    INSERT INTO internal_request_receipts
                        (service_name, nonce, signed_at_ms, expires_at)
                    VALUES (%s, %s, %s, to_timestamp(%s / 1000.0))
                    ON CONFLICT (service_name, nonce) DO NOTHING
                    RETURNING nonce
                    """,
                    (
                        identity.service,
                        identity.nonce,
                        identity.signed_at_ms,
                        identity.expires_at_ms,
                    ),
                )
                return await cursor.fetchone() is not None
        except Exception as error:
            # Authentication must fail closed when the shared receipt authority
            # is unavailable.  Do not expose driver/database detail to callers.
            raise ConfigurationError("internal replay receipt store is unavailable") from error
