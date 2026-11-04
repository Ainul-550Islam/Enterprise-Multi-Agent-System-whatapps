"""Execution and agent isolation for the execution-facing memory manager."""

from dataclasses import replace

import pytest

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.context.execution_context import ExecutionContext
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.memory.backends import InProcessMemoryStore
from mas_orchestrator.memory.manager import MemoryManager
from mas_orchestrator.memory.models import MemoryQuery, MemoryScope, Retention, Sensitivity
from mas_orchestrator.security.identity import ServiceIdentity


def context(execution: str, agent: str = "agent-1", tenant: str = "tenant") -> ExecutionContext:
    return ExecutionContext(
        tenant_id=tenant,
        organization_id="organization",
        execution_id=execution,
        agent_id=agent,
        correlation_id="correlation",
        request_id="request",
        protocol_version=CURRENT_PROTOCOL_VERSION,
        caller=ServiceIdentity(subject="rust-core", issuer="static"),
    )


@pytest.mark.asyncio
async def test_mixed_recall_enforces_task_execution_and_episodic_agent() -> None:
    store = InProcessMemoryStore(environment=Environment.DEVELOPMENT)
    manager = MemoryManager(store)
    own = context("exec-1")
    await manager.remember("other task", context("exec-2"), scope=MemoryScope.TASK)
    await manager.remember("own task", own, scope=MemoryScope.TASK)
    await manager.remember("other agent", context("exec-2", "agent-2"), scope=MemoryScope.EPISODIC)
    await manager.remember("own agent", context("exec-2"), scope=MemoryScope.EPISODIC)
    await manager.remember(
        "tenant durable", context("exec-2", "agent-2"), scope=MemoryScope.DURABLE
    )
    hits = await manager.recall(
        own, scopes=(MemoryScope.TASK, MemoryScope.EPISODIC, MemoryScope.DURABLE)
    )
    assert [hit.content for hit in hits] == ["own task", "own agent", "tenant durable"]


@pytest.mark.asyncio
async def test_unconfigured_redaction_never_marks_sensitive_raw_content_as_safe() -> None:
    store = InProcessMemoryStore()
    manager = MemoryManager(store)
    with pytest.raises(ConfigurationError, match="redaction hook"):
        await manager.remember(
            "password supersecret", context("exec-1"), sensitivity=Sensitivity.SENSITIVE
        )
    assert store.count() == 0


@pytest.mark.asyncio
async def test_expired_records_are_not_returned_even_before_sweep() -> None:
    store = InProcessMemoryStore()
    manager = MemoryManager(store)
    record = await manager.remember(
        "expired",
        context("exec-1"),
        scope=MemoryScope.DURABLE,
        retention=Retention(retain_until_epoch_ms=1),
    )
    assert await store.get("tenant", record.record_id) is None
    assert await store.query(MemoryQuery(tenant_id="tenant", scopes=(MemoryScope.DURABLE,))) == []
    assert await manager.recall(context("exec-1"), scopes=(MemoryScope.DURABLE,)) == []
    assert store.count() == 1  # physical deletion still belongs to the sweep


@pytest.mark.asyncio
async def test_manager_refuses_wrong_scope_results_from_misbehaving_store() -> None:
    store = InProcessMemoryStore()
    manager = MemoryManager(store)
    source = await manager.remember("wrong", context("exec-2"))
    from mas_orchestrator.memory.models import MemoryHit

    async def unsafe_query(query: MemoryQuery) -> list[MemoryHit]:
        return [MemoryHit(record=source), MemoryHit(record=replace(source, tenant_id="other"))]

    store.query = unsafe_query  # type: ignore[method-assign]
    assert await manager.recall(context("exec-1")) == []


class _Redactor:
    def redact(self, content: str) -> str:
        return content.replace("secret", "[redacted]")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["task", "fact"])
async def test_memory_service_returns_the_persisted_redacted_record(kind: str) -> None:
    from mas_orchestrator.services.memory_service import MemoryService

    store = InProcessMemoryStore()
    service = MemoryService(MemoryManager(store, redaction=_Redactor()))
    method = service.remember_task if kind == "task" else service.remember_fact
    record = await method(
        "  a secret note  ",
        context("exec-1"),
        record_id="provided-id",
        sensitivity=Sensitivity.SENSITIVE,
    )
    assert record.record_id == "provided-id"
    assert record.redacted and record.content == "a [redacted] note"
    assert await store.get("tenant", "provided-id") == record
    if kind == "fact":
        assert record.retention.retain_until_epoch_ms is not None
    else:
        assert record.retention.delete_on_execution_end


@pytest.mark.asyncio
async def test_user_selected_memory_id_cannot_overwrite_another_execution() -> None:
    from mas_orchestrator.contracts.errors import MemoryBackendError
    from mas_orchestrator.services.memory_service import MemoryService

    store = InProcessMemoryStore()
    service = MemoryService(MemoryManager(store))
    initial = await service.remember_task("original", context("exec-1"), record_id="same")
    with pytest.raises(MemoryBackendError):
        await service.remember_task("replacement", context("exec-2"), record_id="same")
    assert await store.get("tenant", "same") == initial


@pytest.mark.asyncio
async def test_simultaneous_create_claims_do_not_overwrite() -> None:
    import asyncio

    from mas_orchestrator.contracts.errors import MemoryBackendError
    from mas_orchestrator.services.memory_service import MemoryService

    store = InProcessMemoryStore()
    service = MemoryService(MemoryManager(store))
    results = await asyncio.gather(
        service.remember_task("first", context("exec-1"), record_id="same"),
        service.remember_task("second", context("exec-2"), record_id="same"),
        return_exceptions=True,
    )
    assert sum(isinstance(result, MemoryBackendError) for result in results) == 1
    assert store.count() == 1


@pytest.mark.asyncio
async def test_forget_cannot_delete_another_execution_or_agents_private_memory() -> None:
    store = InProcessMemoryStore()
    manager = MemoryManager(store)
    foreign_task = await manager.remember("other task", context("exec-2"))
    foreign_episode = await manager.remember(
        "other agent", context("exec-2", "agent-2"), scope=MemoryScope.EPISODIC
    )
    assert not await manager.forget(context("exec-1"), foreign_task.record_id)
    assert not await manager.forget(context("exec-1"), foreign_episode.record_id)
    assert await store.get("tenant", foreign_task.record_id) == foreign_task
    assert await store.get("tenant", foreign_episode.record_id) == foreign_episode
    assert await manager.forget(context("exec-2"), foreign_task.record_id)
    assert await manager.forget(context("exec-3", "agent-2"), foreign_episode.record_id)


@pytest.mark.asyncio
async def test_close_execution_purges_all_records_and_fences_late_writes() -> None:
    from mas_orchestrator.contracts.errors import MemoryBackendError

    store = InProcessMemoryStore()
    manager = MemoryManager(store)
    own = context("exec-1")
    for number in range(1050):
        await manager.remember(f"record {number}", own)
    other = await manager.remember("other execution", context("exec-2"))
    durable = await manager.remember("durable", own, scope=MemoryScope.DURABLE)
    assert await manager.forget_execution(own) == 1050
    assert await manager.forget_execution(own) == 0
    assert await store.get("tenant", other.record_id) == other
    assert await store.get("tenant", durable.record_id) == durable
    with pytest.raises(MemoryBackendError):
        await manager.remember("late write", own)
    await manager.remember("another tenant", context("exec-1", tenant="other"))


@pytest.mark.asyncio
async def test_closed_execution_capacity_failure_does_not_claim_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import mas_orchestrator.memory.backends as backends
    from mas_orchestrator.contracts.errors import MemoryBackendError

    monkeypatch.setattr(backends, "MAX_CLOSED_EXECUTIONS", 1)
    store = InProcessMemoryStore()
    manager = MemoryManager(store)
    await manager.forget_execution(context("already-closed"))
    retained = await manager.remember("keep until cleanup succeeds", context("other"))
    with pytest.raises(MemoryBackendError, match="cleanup failed"):
        await manager.forget_execution(context("other"))
    assert await store.get("tenant", retained.record_id) == retained
    assert await manager.forget_execution(context("already-closed")) == 0
