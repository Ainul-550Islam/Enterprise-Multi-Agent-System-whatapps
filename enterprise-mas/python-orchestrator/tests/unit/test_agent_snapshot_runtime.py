"""Snapshot-backed runtime selection must bypass the static catalog."""

from __future__ import annotations

import asyncio
from typing import Any

from mas_orchestrator.agents.base import (
    AgentKind,
    AgentLimits,
    AgentOutcome,
    AgentSpec,
    AgentStatus,
    Termination,
)
from mas_orchestrator.agents.runtime import AgentRuntime
from mas_orchestrator.context.execution_context import build_execution_context
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.security.identity import ServiceIdentity


class RejectingRegistry:
    def spec(self, tenant_id: str, agent_id: str) -> AgentSpec:
        raise AssertionError("snapshot dispatch must not query the static agent registry")


class AllowRouter:
    def evaluate(self, spec: AgentSpec, routing: Any) -> None:
        return None

    def select(self, tenant_id: str, routing: Any) -> AgentSpec:
        raise AssertionError("explicit snapshot execution must not select a catalog default")


class RecordingFactory:
    def __init__(self) -> None:
        self.spec: AgentSpec | None = None

    def build(self, spec: AgentSpec, **_: Any) -> Any:
        self.spec = spec
        return SnapshotAgent(spec)


class SnapshotAgent:
    def __init__(self, spec: AgentSpec) -> None:
        self.spec = spec

    async def run(self, state: Any, context: Any, *, cancel_event: asyncio.Event) -> AgentOutcome:
        return AgentOutcome(
            agent_id=self.spec.agent_id,
            termination=Termination.COMPLETED,
            content=f"ran {self.spec.agent_version_id}: {self.spec.system_prompt}",
        )


def test_agent_runtime_executes_the_supplied_snapshot_without_registry_lookup() -> None:
    tenant_id = "11111111-1111-4111-8111-111111111111"
    organization_id = "33333333-3333-4333-8333-333333333333"
    agent_id = "55555555-5555-4555-8555-555555555555"
    version_id = "77777777-7777-4777-8777-777777777777"
    spec = AgentSpec(
        agent_id=agent_id,
        agent_version_id=version_id,
        name="Published Agent",
        slug="published-agent",
        kind=AgentKind.STANDARD,
        tenant_id=tenant_id,
        status=AgentStatus.PUBLISHED,
        model_hint="provider/model-v7",
        system_prompt="instructions from the pinned version",
        limits=AgentLimits(max_steps=17, max_tool_calls=9),
    )
    factory = RecordingFactory()
    runtime = AgentRuntime(
        registry=RejectingRegistry(),  # type: ignore[arg-type]
        router=AllowRouter(),  # type: ignore[arg-type]
        factory=factory,  # type: ignore[arg-type]
    )
    context = build_execution_context(
        tenant_id=tenant_id,
        organization_id=organization_id,
        execution_id="execution-7",
        agent_id=agent_id,
        correlation_id="correlation-7",
        request_id="request-7",
        caller=ServiceIdentity(subject="rust-core", issuer="platform", scopes=frozenset()),
        protocol_version=str(CURRENT_PROTOCOL_VERSION),
        agent_version_id=version_id,
    )

    result = asyncio.run(
        runtime.execute(
            "question",
            context,
            agent_id=agent_id,
            agent_spec=spec,
        )
    )

    assert factory.spec is spec
    assert result.agent_id == agent_id
    assert result.content == f"ran {version_id}: instructions from the pinned version"
    assert result.succeeded
