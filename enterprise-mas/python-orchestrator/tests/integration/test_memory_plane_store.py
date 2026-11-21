"""Live PostgreSQL regression tests for the memory-plane store boundary.

Run against a database with the Rust migrations applied, including
``0054_memory_plane_role``. Set ``MAS_TEST_MEMORY_DATABASE_URL`` to a
non-superuser login granted ``SET ROLE mas_memory`` and, for the rejection test,
set ``MAS_TEST_MEMORY_ELEVATED_DATABASE_URL`` to a superuser, a ``BYPASSRLS``
login, or a login that can ``SET ROLE`` to either for the same database.
"""

from __future__ import annotations

import os
import uuid

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.memory.models import (
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Sensitivity,
)
from mas_orchestrator.memory.postgres_store import PostgresMemoryStore

pytestmark = pytest.mark.integration


def _required_database_url(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} is required for the live memory-plane test")
    return value


def _record(tenant_id: str, *, record_id: str, content: str) -> MemoryRecord:
    return MemoryRecord(
        record_id=record_id,
        tenant_id=tenant_id,
        scope=MemoryScope.DURABLE,
        content=content,
        sensitivity=Sensitivity.INTERNAL,
        agent_id="agent-live",
        execution_id="exec-live",
    )


async def test_put_get_query_delete_round_trip() -> None:
    tenant_id = str(uuid.uuid4())
    record = _record(tenant_id, record_id=f"mem-{uuid.uuid4().hex}", content="alpha beta")
    store = await PostgresMemoryStore.connect(
        _required_database_url("MAS_TEST_MEMORY_DATABASE_URL"),
        min_size=1,
        max_size=2,
    )
    try:
        await store.put(record)

        loaded = await store.get(tenant_id, record.record_id)
        assert loaded is not None
        assert loaded.content == "alpha beta"
        assert loaded.agent_id == "agent-live"

        hits = await store.query(
            MemoryQuery(tenant_id=tenant_id, scopes=(MemoryScope.DURABLE,), text="alpha")
        )
        assert [hit.record.record_id for hit in hits] == [record.record_id]

        assert await store.delete(tenant_id, record.record_id) is True
        assert await store.get(tenant_id, record.record_id) is None
        assert await store.delete(tenant_id, record.record_id) is False
    finally:
        await store.aclose()


async def test_query_is_scoped_to_the_tenant() -> None:
    tenant_a = str(uuid.uuid4())
    tenant_b = str(uuid.uuid4())
    record = _record(tenant_a, record_id=f"mem-{uuid.uuid4().hex}", content="shared needle")
    store = await PostgresMemoryStore.connect(
        _required_database_url("MAS_TEST_MEMORY_DATABASE_URL"),
        min_size=1,
        max_size=2,
    )
    try:
        await store.put(record)
        other = await store.query(
            MemoryQuery(tenant_id=tenant_b, scopes=(MemoryScope.DURABLE,), text="needle")
        )
        assert other == []
        assert await store.get(tenant_b, record.record_id) is None
        assert await store.purge_tenant(tenant_a) == 1
    finally:
        await store.aclose()


async def test_sensitive_records_are_hidden_unless_requested() -> None:
    tenant_id = str(uuid.uuid4())
    record = MemoryRecord(
        record_id=f"mem-{uuid.uuid4().hex}",
        tenant_id=tenant_id,
        scope=MemoryScope.SEMANTIC,
        content="sensitive needle",
        sensitivity=Sensitivity.RESTRICTED,
        agent_id="agent-live",
    )
    store = await PostgresMemoryStore.connect(
        _required_database_url("MAS_TEST_MEMORY_DATABASE_URL"),
        min_size=1,
        max_size=2,
    )
    try:
        await store.put(record)
        assert (
            await store.query(MemoryQuery(tenant_id=tenant_id, scopes=(MemoryScope.SEMANTIC,)))
            == []
        )
        hits = await store.query(
            MemoryQuery(
                tenant_id=tenant_id,
                scopes=(MemoryScope.SEMANTIC,),
                include_sensitive=True,
            )
        )
        assert [hit.record.record_id for hit in hits] == [record.record_id]
        assert await store.purge_tenant(tenant_id) == 1
    finally:
        await store.aclose()


async def test_elevated_login_is_rejected_before_the_pool_is_opened() -> None:
    with pytest.raises(
        ConfigurationError,
        match="non-privileged and able to assume mas_memory",
    ):
        await PostgresMemoryStore.connect(
            _required_database_url("MAS_TEST_MEMORY_ELEVATED_DATABASE_URL"),
            min_size=1,
            max_size=1,
        )
