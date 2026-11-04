"""Rust-selected versions remain subject to runtime eligibility checks."""

from dataclasses import replace

from mas_orchestrator.agents.base import AgentKind, AgentSpec, AgentStatus
from mas_orchestrator.agents.router import AgentRouter, AgentRoutingRequest


def test_pinned_snapshot_does_not_use_unrelated_python_catalog_allowlist() -> None:
    router = AgentRouter(registry=object())  # type: ignore[arg-type]
    spec = AgentSpec(
        agent_id="rust-selected-agent",
        agent_version_id="published-version",
        name="Agent",
        slug="agent",
        kind=AgentKind.STANDARD,
        tenant_id="tenant",
    )
    assert router.evaluate(spec, AgentRoutingRequest()).reason == "not_allowlisted"
    assert router.evaluate_pinned(spec, AgentRoutingRequest()) is None
    assert (
        router.evaluate_pinned(
            replace(spec, status=AgentStatus.DISABLED), AgentRoutingRequest()
        ).reason
        == "agent_disabled"
    )
    assert (
        router.evaluate_pinned(spec, AgentRoutingRequest(require_tools=True)).reason
        == "no_tool_support"
    )
