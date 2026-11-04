"""Live replay-receipt tests against a migrated PostgreSQL database.

MAS_TEST_REPLAY_DATABASE_URL must point to the non-privileged mas_app role on
an isolated migrated test database. The role deliberately has INSERT but not
SELECT on internal_request_receipts, as required by migration 0034.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import time

import psycopg
import pytest

from mas_orchestrator.security.internal_replay import PostgresReplayGuard
from mas_orchestrator.security.internal_service import InternalServiceIdentity

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="function")]


async def test_insert_only_role_consumes_nonce_once_across_instances() -> None:
    """Concurrent replicas accept only one request without SELECT privileges."""
    url = os.environ["MAS_TEST_REPLAY_DATABASE_URL"]
    async with (
        await psycopg.AsyncConnection.connect(url) as connection,
        connection.cursor() as cursor,
    ):
        await cursor.execute(
            "SELECT current_user, "
            "has_table_privilege(current_user, 'internal_request_receipts', 'INSERT'), "
            "has_table_privilege(current_user, 'internal_request_receipts', 'SELECT')"
        )
        assert await cursor.fetchone() == ("mas_app", True, False)

    now_ms = int(time.time() * 1000)
    identity = InternalServiceIdentity(
        service="mas-worker",
        signed_at_ms=now_ms,
        nonce=secrets.token_urlsafe(32),
        expires_at_ms=now_ms + 60_000,
    )
    first, second = PostgresReplayGuard(url), PostgresReplayGuard(url)
    results = await asyncio.gather(*(guard.consume(identity) for guard in (first, second) * 4))
    assert sum(results) == 1
    assert not await PostgresReplayGuard(url).consume(identity)
