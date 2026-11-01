"""Tests for handoff chains, the concrete agent set, and the entrypoint.

Handoff is the dangerous kind of delegation: the agent that started the chain is
no longer in the loop to notice anything going wrong. So the tests here are about
the three limits that stop a chain, and about the fact that they are enforced
rather than advisory.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.agents.agent import (
    WORKER_TOOLS,
    AgentBundle,
    DefaultAgent,
    build_agents,
    refused_outcome,
)
from mas_orchestrator.agents.base import AgentKind, AgentSpec, Termination
from mas_orchestrator.agents.handoff import (
    DEFAULT_BUDGET,
    MAX_HANDOFF_DEPTH,
    MAX_REPEATS_PER_AGENT,
    HandoffChain,
    assert_same_tenant,
    describe,
    evaluate_handoff,
    extend,
    start_chain,
)
from mas_orchestrator.contracts.errors import InvalidRequestError

TENANT = "tenant-alpha"
OTHER = "tenant-beta"


class TestHandoffChain:
    def test_start_records_the_first_agent(self) -> None:
        chain = start_chain(tenant_id=TENANT, agent_id="a")
        assert chain.current == "a"
        assert chain.depth == 0

    def test_chain_requires_a_tenant(self) -> None:
        with pytest.raises(InvalidRequestError):
            HandoffChain(tenant_id=" ")

    def test_negative_budget_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            HandoffChain(tenant_id=TENANT, budget=-1)

    def test_extend_returns_a_new_chain(self) -> None:
        # Immutable, so a branch cannot corrupt the record of how control
        # arrived — which is what an audit needs.
        first = start_chain(tenant_id=TENANT, agent_id="a")
        second = extend(first, to_agent_id="b")
        assert first.current == "a"
        assert second.current == "b"

    def test_depth_ceiling_stops_the_chain(self) -> None:
        chain = start_chain(tenant_id=TENANT, agent_id="a")
        for index in range(MAX_HANDOFF_DEPTH):
            chain = extend(chain, to_agent_id=f"n{index}")
        assert chain.depth == MAX_HANDOFF_DEPTH
        # Depth limits total work, not just cycles.
        with pytest.raises(InvalidRequestError, match="depth ceiling"):
            extend(chain, to_agent_id="f")

    def test_a_cycle_within_the_depth_limit_is_still_refused(self) -> None:
        # A→B→A→B stays within the depth ceiling and is nonetheless a loop,
        # and an expensive one because every hop is a full agent run.
        chain = start_chain(tenant_id=TENANT, agent_id="a")
        chain = extend(chain, to_agent_id="b")
        chain = extend(chain, to_agent_id="a")
        with pytest.raises(InvalidRequestError, match="already appeared"):
            extend(chain, to_agent_id="a")

    def test_budget_stops_a_legal_chain(self) -> None:
        # Structurally legal but doing far more work than intended.
        chain = start_chain(tenant_id=TENANT, agent_id="a", budget=2)
        chain = extend(chain, to_agent_id="b", cost=2)
        assert chain.exhausted
        with pytest.raises(InvalidRequestError, match="budget"):
            extend(chain, to_agent_id="c", cost=1)

    def test_evaluate_does_not_raise(self) -> None:
        # A query, so a planner can ask before committing to a hop.
        chain = start_chain(tenant_id=TENANT, agent_id="a")
        decision = evaluate_handoff(chain, to_agent_id="b")
        assert decision.allowed

    def test_evaluate_refuses_a_blank_target(self) -> None:
        decision = evaluate_handoff(start_chain(tenant_id=TENANT, agent_id="a"), to_agent_id=" ")
        assert decision.refused
        assert decision.code == "missing_target"

    def test_evaluate_refuses_negative_cost(self) -> None:
        decision = evaluate_handoff(
            start_chain(tenant_id=TENANT, agent_id="a"), to_agent_id="b", cost=-1
        )
        assert decision.code == "invalid_cost"

    def test_describe_names_agents_only(self) -> None:
        # A reason could contain user text, which must not reach a log.
        chain = extend(start_chain(tenant_id=TENANT, agent_id="a"), to_agent_id="b")
        assert "b" in describe(chain)

    def test_cross_tenant_chain_is_refused(self) -> None:
        chain = start_chain(tenant_id=TENANT, agent_id="a")
        with pytest.raises(InvalidRequestError, match="different tenant"):
            assert_same_tenant(chain, OTHER)

    def test_repeats_are_counted(self) -> None:
        chain = start_chain(tenant_id=TENANT, agent_id="a")
        assert chain.repeats_of("a") == 1

    def test_default_budget_is_positive(self) -> None:
        assert DEFAULT_BUDGET > 0
        assert MAX_REPEATS_PER_AGENT >= 1


class _Registry:
    def __init__(self) -> None:
        self.items: dict[str, object] = {}

    def register(self, spec: AgentSpec, factory: object) -> None:
        self.items[spec.agent_id] = factory

    def __len__(self) -> int:
        return len(self.items)


class TestDefaultAgent:
    def test_tool_agent_requires_an_executor(self) -> None:
        # Refused at construction: an agent that may call tools but cannot
        # execute them would fail mid-run, after spending tokens.
        spec = AgentSpec(
            agent_id="a",
            agent_version_id="v1",
            name="a",
            slug="a",
            kind=AgentKind.TOOL_USER,
            tenant_id=TENANT,
            allowed_tools=frozenset({"builtin.http"}),
        )
        with pytest.raises(InvalidRequestError, match="tool executor"):
            DefaultAgent(spec, router=object(), provider_for=lambda ref: None)  # type: ignore[arg-type]

    def test_a_toolless_agent_needs_no_executor(self) -> None:
        spec = AgentSpec(
            agent_id="a",
            agent_version_id="v1",
            name="a",
            slug="a",
            kind=AgentKind.STANDARD,
            tenant_id=TENANT,
        )
        agent = DefaultAgent(spec, router=object(), provider_for=lambda ref: None)  # type: ignore[arg-type]
        assert agent.spec.agent_id == "a"


def _bundle() -> AgentBundle:
    """Builds the standard set with minimal tool plumbing.

    The tool layer needs a registry and an executor for the tool-using agent;
    neither is exercised here, so small stand-ins are enough.
    """
    from mas_orchestrator.tools.executor import ToolExecutor
    from mas_orchestrator.tools.permissions import DenyAllPermissionBridge
    from mas_orchestrator.tools.registry import ToolRegistry

    return build_agents(
        tenant_id=TENANT,
        router=object(),  # type: ignore[arg-type]
        provider_for=lambda ref: None,
        tool_executor=ToolExecutor(ToolRegistry(), DenyAllPermissionBridge()),
        tool_registry=ToolRegistry(),
    )


class TestBuildAgents:
    def test_tenant_is_required(self) -> None:
        # Identity is owned by Rust, so it must be supplied rather than
        # invented.
        with pytest.raises(InvalidRequestError, match="tenant"):
            build_agents(
                tenant_id=" ",
                router=object(),  # type: ignore[arg-type]
                provider_for=lambda ref: None,
            )

    def test_builds_four_agents(self) -> None:
        bundle = _bundle()
        assert isinstance(bundle, AgentBundle)
        assert len(bundle.agents()) == 3
        # The supervisor is registered too, so it can be resolved by id.
        assert len(bundle.registry) == 4

    def test_summariser_has_no_tools(self) -> None:
        # Declared twice — by kind and by allow-list — not merely intended.
        summariser = _bundle().by_id()["summariser"]
        assert summariser.spec.allowed_tools == frozenset()
        assert summariser.spec.kind is not AgentKind.TOOL_USER

    def test_supervisor_allowlist_names_its_delegates(self) -> None:
        # A supervisor's tools *are* its delegates, so delegation is checked by
        # the same mechanism as a tool call.
        assert _bundle().supervisor.spec.allowed_tools

    def test_worker_tools_are_explicit(self) -> None:
        # An allow-list of ("*",) would make the tool layer's own check the only
        # gate, which is one gate too few.
        assert "*" not in WORKER_TOOLS


class TestRefusedOutcome:
    def test_refusal_is_not_recorded_as_failure(self) -> None:
        # An agent refused before starting must not be reported as having failed
        # to answer a question it was never permitted to attempt.
        spec = AgentSpec(
            agent_id="a",
            agent_version_id="v1",
            name="a",
            slug="a",
            kind=AgentKind.STANDARD,
            tenant_id=TENANT,
        )
        outcome = refused_outcome(spec, reason="policy")
        assert outcome.termination is not Termination.FAILED
        assert outcome.content == ""
