"""Live PostgreSQL regression tests for the pgvector RAG role boundary.

Run these tests against a database that has the Rust migrations applied,
including ``0028_rag_pgvector_plane``. Set ``MAS_TEST_RAG_DATABASE_URL`` to a
non-superuser login granted ``SET ROLE mas_rag`` and, for the rejection test,
set ``MAS_TEST_RAG_ELEVATED_DATABASE_URL`` to a superuser, a ``BYPASSRLS``
login, or a login that can ``SET ROLE`` to either for the same database.
"""

from __future__ import annotations

import os

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.rag.pgvector_index import PgVectorIndex

pytestmark = pytest.mark.integration


def _required_database_url(name: str) -> str:
    """Returns an explicitly configured live-test URL or skips safely."""
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} is required for the live pgvector role-boundary test")
    return value


@pytest.mark.asyncio
async def test_member_login_opens_a_clean_pool_and_can_probe_the_rag_table() -> None:
    """A documented member-login DSN works with pool configure hooks enabled."""
    index = await PgVectorIndex.connect(
        _required_database_url("MAS_TEST_RAG_DATABASE_URL"),
        dimensions=1536,
        database_role="mas_rag",
        min_size=1,
        max_size=1,
    )
    try:
        assert await index.health_check() is True
    finally:
        await index.aclose()


@pytest.mark.asyncio
async def test_elevated_login_is_rejected_before_the_pool_is_opened() -> None:
    """A textual ``database_role`` setting cannot launder an elevated DSN."""
    with pytest.raises(
        ConfigurationError,
        match="non-privileged and able to assume mas_rag",
    ):
        await PgVectorIndex.connect(
            _required_database_url("MAS_TEST_RAG_ELEVATED_DATABASE_URL"),
            dimensions=1536,
            database_role="mas_rag",
            min_size=1,
            max_size=1,
        )
