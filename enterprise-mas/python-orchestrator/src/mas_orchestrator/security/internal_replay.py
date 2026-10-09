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
        now_ms = int(time.time() * 1000)
        key = (identity.service, identity.nonce)
        async with self._lock:
            expired = [key for key, expiry in self._receipts.items() if expiry <= now_ms]
            for expired_key in expired:
                del self._receipts[expired_key]
            if key in self._receipts:
                return False
            self._receipts[key] = identity.expires_at_ms
            return True


@dataclass(frozen=True, slots=True)
class PostgresReplayGuard:
    """Cross-replica PostgreSQL nonce consumer used by production composition."""

    database_url: str

    async def consume(self, identity: InternalServiceIdentity) -> bool:
        """Inserts one receipt with conflict-as-replay semantics."""
        try:
            import psycopg
        except ImportError as error:
            raise ConfigurationError(
                "durable internal replay protection requires psycopg",
                details={"extra": "postgres"},
            ) from error

        try:
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
