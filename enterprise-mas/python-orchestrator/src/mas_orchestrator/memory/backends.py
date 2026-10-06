"""Memory storage backends.

The production store is a database owned by Rust. Python must not write
Rust-owned tables (spec §5), so the only backend that ships here is an
**in-process** one, and it refuses to be selected in production.

That is not a stub waiting to be replaced — it is a deliberate boundary. The
alternative would be a "temporary" SQL implementation in Python that quietly
becomes the system of record, at which point two services own the same data and
neither can enforce the other's invariants.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.memory.models import MemoryHit, MemoryQuery, MemoryRecord

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_RECORDS_PER_TENANT: Final[int] = 10_000


@runtime_checkable
class MemoryStore(Protocol):
    """What a memory backend must do."""

    async def put(self, record: MemoryRecord) -> None:
        """Stores a record, overwriting any record with the same id."""
        ...

    async def get(
        self,
        tenant_id: str,
        record_id: str,
    ) -> MemoryRecord | None:
        """Returns a record, or ``None``. Never another tenant's record."""
        ...

    async def query(self, query: MemoryQuery) -> list[MemoryHit]:
        """Searches within one tenant only."""
        ...

    async def delete(self, tenant_id: str, record_id: str) -> bool:
        """Deletes a record. Returns whether it existed."""
        ...

    async def purge_tenant(self, tenant_id: str) -> int:
        """Deletes every record for a tenant. Returns the count."""
        ...


class InProcessMemoryStore:
    """An in-process store for development and tests.

    Refuses to be constructed in production: in-process state is per-worker,
    so behind more than one replica every execution would see a different
    memory, and durable records would vanish on restart.

    Tenant isolation is enforced on read as well as write, so a query cannot
    reach across tenants even if the caller built the query wrongly.
    """

    def __init__(self, *, environment: Environment = Environment.DEVELOPMENT) -> None:
        if environment is Environment.PRODUCTION:
            raise ConfigurationError(
                "InProcessMemoryStore must not be used in production; "
                "configure the durable store owned by Rust instead",
                details={"environment": environment.value},
            )
        self._environment = environment
        self._records: dict[tuple[str, str], MemoryRecord] = {}

    async def put(self, record: MemoryRecord) -> None:
        """Stores a record."""
        key = (record.tenant_id, record.record_id)
        if len(self._records) >= MAX_RECORDS_PER_TENANT and key not in self._records:
            raise ConfigurationError(
                "in-process memory store is full",
                details={"limit": MAX_RECORDS_PER_TENANT},
            )
        self._records[key] = record

    async def get(self, tenant_id: str, record_id: str) -> MemoryRecord | None:
        """Returns a record scoped to the tenant."""
        return self._records.get((tenant_id, record_id))

    async def query(self, query: MemoryQuery) -> list[MemoryHit]:
        """Searches records for one tenant.

        Matching is a plain case-insensitive substring test. That is honest
        for a development store; semantic retrieval belongs to the RAG layer,
        not here, and pretending otherwise would make this look complete.
        """
        needle = query.text.strip().lower()
        results: list[MemoryHit] = []

        for (owner, _), record in self._records.items():
            if owner != query.tenant_id:
                continue
            if record.scope not in query.scopes:
                continue
            if query.agent_id is not None and record.agent_id != query.agent_id:
                continue
            if query.execution_id is not None and record.execution_id != query.execution_id:
                continue
            if not query.include_sensitive and record.needs_redaction():
                # Skipped rather than returned unredacted: leaking an
                # unredacted sensitive record is worse than omitting it.
                continue
            if needle and needle not in record.content.lower():
                continue

            results.append(MemoryHit(record=record, source="in_process"))
            if len(results) >= query.limit:
                break

        return results

    async def delete(self, tenant_id: str, record_id: str) -> bool:
        """Deletes a record."""
        return self._records.pop((tenant_id, record_id), None) is not None

    async def purge_tenant(self, tenant_id: str) -> int:
        """Deletes all records for a tenant."""
        doomed = [key for key in self._records if key[0] == tenant_id]
        for key in doomed:
            del self._records[key]
        return len(doomed)

    def count(self, tenant_id: str | None = None) -> int:
        """Number of stored records, for tests."""
        if tenant_id is None:
            return len(self._records)
        return sum(1 for (owner, _) in self._records if owner == tenant_id)


async def evict_expired(
    store: MemoryStore,
    *,
    now_epoch_ms: int | None = None,
) -> int:
    """Deletes records past their retention deadline.

    Returns the number removed. Retention that is never enforced is not a
    retention policy, so this is called on a schedule rather than relying on
    readers to filter.

    Note that this requires enumerating the store; a durable backend would
    push expiry into the database. The interface is the same either way.
    """
    removed = 0
    if not isinstance(store, InProcessMemoryStore):
        raise ConfigurationError(
            "expiry sweeping is only implemented for the in-process store",
            details={"store": type(store).__name__},
        )

    expired: list[tuple[str, str]] = []
    for (tenant_id, record_id), record in store._records.items():
        if record.is_expired(now_epoch_ms=now_epoch_ms):
            expired.append((tenant_id, record_id))

    for tenant_id, record_id in expired:
        if await store.delete(tenant_id, record_id):
            removed += 1

    if removed:
        _LOGGER.info(
            "memory records expired",
            extra={"event": "memory.expired", "count": removed},
        )
    return removed


def assert_tenant_scoped(records: Sequence[MemoryRecord], tenant_id: str) -> None:
    """Raises if any record belongs to another tenant.

    A defensive check at the boundary: the store already scopes by tenant, but
    a caller that assembles records itself should be able to assert the
    invariant rather than trust its own filtering.
    """
    foreign = [item.record_id for item in records if item.tenant_id != tenant_id]
    if foreign:
        raise ConfigurationError(
            "memory results contain records from another tenant",
            details={"tenant_id": tenant_id, "foreign": foreign},
        )
