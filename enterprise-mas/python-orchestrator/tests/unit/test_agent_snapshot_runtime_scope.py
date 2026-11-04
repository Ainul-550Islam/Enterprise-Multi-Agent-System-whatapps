"""Snapshot dispatch fails closed on identity or policy mismatches."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from mas_orchestrator.agents.base import AgentKind, AgentSpec
from mas_orchestrator.agents.runtime import AgentRuntime
from mas_orchestrator.context.execution_context import build_execution_context
from mas_orchestrator.contracts.errors import AgentNotFoundError, InvalidRequestError
from mas_orchestrator.security.identity import ServiceIdentity


class NoRegistry:
    def spec(self, *args: Any) -> AgentSpec:
        raise AssertionError("snapshot must not fall back to catalog")


class Router:
    def __init__(self, denied: bool = False) -> None:
        self.denied = denied

    def evaluate_pinned(self, *args: Any) -> str | None:
        return "denied" if self.denied else None


class NoFactory:
    def build(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("rejected snapshot must not reach factory")


def context() -> Any:
    return build_execution_context(
        tenant_id="tenant-a",
        organization_id="organization-a",
        execution_id="execution-a",
        agent_id="agent-a",
        agent_version_id="version-a",
        correlation_id="correlation-a",
        request_id="request-a",
        caller=ServiceIdentity(subject="worker", issuer="test", scopes=frozenset()),
        protocol_version="1.1.0",
    )


def spec() -> AgentSpec:
    return AgentSpec(
        agent_id="agent-a",
        agent_version_id="version-a",
        name="Agent",
        slug="agent",
        kind=AgentKind.STANDARD,
        tenant_id="tenant-a",
    )


@pytest.mark.parametrize(
    "changed",
    [
        {"tenant_id": "tenant-b"},
        {"agent_id": "agent-b"},
        {"agent_version_id": "version-b"},
    ],
)
def test_snapshot_scope_mismatch_never_reaches_factory(changed: dict[str, str]) -> None:
    runtime = AgentRuntime(
        registry=NoRegistry(),
        router=Router(),
        factory=NoFactory(),  # type: ignore[arg-type]
    )
    with pytest.raises(InvalidRequestError):
        asyncio.run(
            runtime.execute(
                "input", context(), agent_id="agent-a", agent_spec=replace(spec(), **changed)
            )
        )


def test_snapshot_still_obeys_routing_policy() -> None:
    runtime = AgentRuntime(
        registry=NoRegistry(),
        router=Router(denied=True),
        factory=NoFactory(),  # type: ignore[arg-type]
    )
    with pytest.raises(AgentNotFoundError):
        asyncio.run(runtime.execute("input", context(), agent_id="agent-a", agent_spec=spec()))
