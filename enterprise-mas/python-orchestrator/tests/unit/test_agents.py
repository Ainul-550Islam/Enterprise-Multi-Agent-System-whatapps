"""Unit tests for the agent runtime.

The properties that matter are the bounds. An agent that does not stop on its
own must be stopped, and the record must say which bound stopped it rather
than reporting that it finished.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.agents.base import (
    MAX_STEPS_PER_EXECUTION,
    AgentKind,
    AgentLimits,
    AgentOutcome,
    AgentSpec,
    AgentState,
    AgentStatus,
    Termination,
    budget_exhausted,
    new_state,
)
from mas_orchestrator.agents.delegation import (
    DelegationRequest,
    check_delegation,
    state_for_delegate,
    summarise_for_caller,
    wrap_delegation_failure,
)
from mas_orchestrator.agents.planner import Plan, PlanStep, parse_plan
from mas_orchestrator.agents.router import (
    AgentRouter,
    AgentRoutingRequest,
    build_policy,
)
from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    ConfigurationError,
    InvalidRequestError,
)
from mas_orchestrator.models.types import Message, ModelRef, ModelResponse, Usage

TENANT = "tenant-1"


def _spec(
    *,
    agent_id: str = "agent-1",
    kind: AgentKind = AgentKind.STANDARD,
    status: AgentStatus = AgentStatus.PUBLISHED,
    limits: AgentLimits | None = None,
    tenant_id: str = TENANT,
    allowed_tools: frozenset[str] = frozenset(),
    tags: tuple[str, ...] = (),
) -> AgentSpec:
    return AgentSpec(
        agent_id=agent_id,
        agent_version_id="v1",
        name="Test Agent",
        slug="test-agent",
        kind=kind,
        tenant_id=tenant_id,
        status=status,
        limits=limits or AgentLimits(),
        allowed_tools=allowed_tools,
        tags=tags,
    )


class TestSpec:
    """Agent definitions."""

    def test_validation(self) -> None:
        with pytest.raises(InvalidRequestError):
            AgentSpec(
                agent_id="",
                agent_version_id="v",
                name="n",
                slug="s",
                kind=AgentKind.STANDARD,
                tenant_id=TENANT,
            )
        with pytest.raises(InvalidRequestError):
            AgentSpec(
                agent_id="a",
                agent_version_id="v",
                name="n",
                slug="s",
                kind=AgentKind.STANDARD,
                tenant_id=" ",
            )

    def test_kind_governs_tool_use(self) -> None:
        """A standard agent cannot use tools even if granted some."""
        spec = _spec(kind=AgentKind.STANDARD, allowed_tools=frozenset({"t"}))
        assert spec.may_use_tools is False
        assert spec.permits_tool("t") is False

    def test_tool_user_requires_the_tool_to_be_listed(self) -> None:
        spec = _spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"t"}))
        assert spec.permits_tool("t") is True
        assert spec.permits_tool("other") is False

    def test_unrunnable_statuses(self) -> None:
        assert _spec(status=AgentStatus.DRAFT).is_runnable is False
        assert _spec(status=AgentStatus.DISABLED).is_runnable is False
        assert _spec(status=AgentStatus.DEPRECATED).is_runnable is True


class TestLimits:
    """Limit clamping."""

    def test_values_are_clamped_to_platform_maxima(self) -> None:
        """No tenant configuration may exceed the platform guarantee."""
        limits = AgentLimits(max_steps=10_000).clamped()
        assert limits.max_steps == MAX_STEPS_PER_EXECUTION

    def test_invalid_limits_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            AgentLimits(max_steps=0)
        with pytest.raises(InvalidRequestError):
            AgentLimits(max_tool_calls=-1)
        with pytest.raises(InvalidRequestError):
            AgentLimits(timeout_seconds=0)


class TestOutcome:
    """Outcome invariants."""

    def test_failed_outcome_must_carry_an_error(self) -> None:
        with pytest.raises(ConfigurationError):
            AgentOutcome(agent_id="a", termination=Termination.FAILED)

    def test_completed_is_success(self) -> None:
        outcome = AgentOutcome(agent_id="a", termination=Termination.COMPLETED)
        assert outcome.succeeded is True
        assert outcome.content == ""

    def test_bound_is_not_success(self) -> None:
        """Hitting a bound is not the same as finishing."""
        for termination in (
            Termination.STEP_LIMIT,
            Termination.TOOL_CALL_LIMIT,
            Termination.DEADLINE,
            Termination.CANCELLED,
            Termination.BUDGET_EXHAUSTED,
        ):
            outcome = AgentOutcome(agent_id="a", termination=termination)
            assert outcome.succeeded is False


class TestBudget:
    """Spend ceilings."""

    def test_no_ledger_means_no_ceiling(self) -> None:
        assert budget_exhausted(None, 1.0) is False

    def test_no_ceiling_means_unbounded(self) -> None:
        from mas_orchestrator.models.cost import CostLedger, PricingTable

        ledger = CostLedger(PricingTable(rates={}))
        assert budget_exhausted(ledger, None) is False

    def test_incomplete_totals_never_stop_a_run(self) -> None:
        """Stopping on a lower bound would be a guess, not a decision."""
        from mas_orchestrator.models.cost import CostLedger, PricingTable

        ledger = CostLedger(PricingTable(rates={}), emit_unpriced_warning=False)
        ledger.record(ModelRef("openai", "m"), None)
        assert ledger.totals().is_complete is False
        assert budget_exhausted(ledger, 0.0001) is False


class TestDelegation:
    """Delegation rules."""

    def test_request_validation(self) -> None:
        with pytest.raises(InvalidRequestError):
            DelegationRequest(
                delegation_id="d",
                from_agent_id="a",
                to_agent_id=" ",
                task="t",
                depth=0,
            )
        with pytest.raises(InvalidRequestError):
            DelegationRequest(
                delegation_id="d",
                from_agent_id="a",
                to_agent_id="b",
                task=" ",
                depth=0,
            )
        with pytest.raises(InvalidRequestError):
            DelegationRequest(
                delegation_id="d",
                from_agent_id="a",
                to_agent_id="b",
                task="t",
                depth=-1,
            )

    def test_depth_ceiling(self) -> None:
        assert check_delegation(_spec(), 1, max_depth=3) is None
        assert check_delegation(_spec(), 4, max_depth=3) is not None

    def test_unknown_target_refused(self) -> None:
        assert check_delegation(None, 1) is not None

    def test_unrunnable_target_refused(self) -> None:
        draft = _spec(status=AgentStatus.DRAFT)
        assert check_delegation(draft, 1) is not None

    def test_delegate_state_is_isolated(self) -> None:
        """A delegate must not inherit the parent's transcript."""
        parent = AgentState(messages=(Message.user("secret"), Message.assistant("x")))
        child = state_for_delegate("do the thing", parent)
        assert len(child.messages) == 1
        assert "secret" not in str(child.messages[0].content)

    def test_refusal_result_has_no_outcome(self) -> None:
        request = DelegationRequest(
            delegation_id="d",
            from_agent_id="a",
            to_agent_id="b",
            task="t",
            depth=0,
        )
        result = wrap_delegation_failure(request, "too deep")
        assert result.refused is True
        assert result.outcome is None
        assert result.content == ""

    def test_summary_marks_provenance(self) -> None:
        request = DelegationRequest(
            delegation_id="d",
            from_agent_id="a",
            to_agent_id="b",
            task="t",
            depth=0,
        )
        rendered = summarise_for_caller(wrap_delegation_failure(request, "nope"))
        assert 'status="refused"' in rendered

    def test_summary_labels_delegate_content(self) -> None:
        from mas_orchestrator.agents.delegation import DelegationResult

        outcome = AgentOutcome(
            agent_id="b", termination=Termination.COMPLETED, content="the answer"
        )
        result = DelegationResult(delegation_id="d", outcome=outcome)
        rendered = summarise_for_caller(result)
        assert "the answer" in rendered
        assert "not instructions" in rendered


class TestPlanner:
    """Plan parsing."""

    def test_valid_plan(self) -> None:
        plan = parse_plan('{"goal":"g","steps":[{"id":"1","description":"do it"}]}')
        assert plan.goal == "g"
        assert plan.steps[0].step_id == "1"

    def test_empty_output_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            parse_plan("")

    def test_malformed_json_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            parse_plan("{not json")

    def test_missing_goal_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            parse_plan('{"steps":[]}')

    def test_unknown_tool_refused(self) -> None:
        """A plan must not schedule work the agent cannot do."""
        payload = '{"goal":"g","steps":[{"id":"1","description":"d","tool":"rm"}]}'
        with pytest.raises(InvalidRequestError):
            parse_plan(payload, allowed_tools=frozenset({"ls"}))

    def test_known_tool_allowed(self) -> None:
        payload = '{"goal":"g","steps":[{"id":"1","description":"d","tool":"ls"}]}'
        plan = parse_plan(payload, allowed_tools=frozenset({"ls"}))
        assert plan.tool_names == frozenset({"ls"})

    def test_dependency_cycle_refused(self) -> None:
        payload = (
            '{"goal":"g","steps":['
            '{"id":"a","description":"d","depends_on":["b"]},'
            '{"id":"b","description":"d","depends_on":["a"]}]}'
        )
        with pytest.raises(InvalidRequestError):
            parse_plan(payload)

    def test_ready_steps_respect_dependencies(self) -> None:
        plan = Plan(
            goal="g",
            steps=(
                PlanStep("a", "first"),
                PlanStep("b", "second", depends_on=("a",)),
            ),
        )
        assert [s.step_id for s in plan.ready_steps(frozenset())] == ["a"]
        assert [s.step_id for s in plan.ready_steps(frozenset({"a"}))] == ["b"]

    def test_acyclic_plan_is_detected(self) -> None:
        plan = Plan(goal="g", steps=(PlanStep("a", "d"), PlanStep("b", "d", depends_on=("a",))))
        assert plan.is_acyclic() is True


class TestAgentRouter:
    """Policy-driven agent selection."""

    def _registry(self) -> object:
        from mas_orchestrator.agents.registry import AgentRegistry

        registry = AgentRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_spec(agent_id="standard"), lambda: None)
        registry.register(
            _spec(
                agent_id="tools",
                kind=AgentKind.TOOL_USER,
                allowed_tools=frozenset({"search"}),
            ),
            lambda: None,
        )
        registry.register(
            _spec(agent_id="boss", kind=AgentKind.SUPERVISOR, tags=("v2",)),
            lambda: None,
        )
        return registry

    def test_allowlist_filters(self) -> None:
        router = AgentRouter(self._registry(), build_policy(["standard"]))
        assert router.select(TENANT).agent_id == "standard"

    def test_no_eligible_agent_raises_with_reasons(self) -> None:
        router = AgentRouter(self._registry(), build_policy([]))
        with pytest.raises(AgentNotFoundError) as excinfo:
            router.select(TENANT)
        assert len(excinfo.value.details["rejections"]) == 3

    def test_tool_requirement_filters(self) -> None:
        router = AgentRouter(self._registry(), build_policy(["standard", "tools", "boss"]))
        selected = router.select(TENANT, AgentRoutingRequest(require_tools=True))
        assert selected.agent_id == "tools"

    def test_delegation_requires_a_supervisor(self) -> None:
        router = AgentRouter(self._registry(), build_policy(["standard", "tools", "boss"]))
        selected = router.select(TENANT, AgentRoutingRequest(require_delegation=True))
        assert selected.agent_id == "boss"

    def test_kind_filter(self) -> None:
        router = AgentRouter(self._registry(), build_policy(["standard", "tools", "boss"]))
        selected = router.select(TENANT, AgentRoutingRequest(kind=AgentKind.SUPERVISOR))
        assert selected.agent_id == "boss"

    def test_tag_filter(self) -> None:
        router = AgentRouter(self._registry(), build_policy(["boss"]))
        assert router.select(TENANT, AgentRoutingRequest(tags=("v2",))).agent_id == "boss"
        with pytest.raises(AgentNotFoundError):
            router.select(TENANT, AgentRoutingRequest(tags=("v9",)))

    def test_preference_order(self) -> None:
        router = AgentRouter(
            self._registry(),
            build_policy(["standard", "tools"], preferred=["tools", "standard"]),
        )
        assert router.select(TENANT).agent_id == "tools"

    def test_preference_must_be_on_the_allowlist(self) -> None:
        with pytest.raises(ConfigurationError):
            build_policy(["standard"], preferred=["boss"])

    def test_deprecated_can_be_excluded(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry

        registry = AgentRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_spec(agent_id="old", status=AgentStatus.DEPRECATED), lambda: None)
        router = AgentRouter(registry, build_policy(["old"], allow_deprecated=False))
        with pytest.raises(AgentNotFoundError):
            router.select(TENANT)

    def test_selection_is_deterministic(self) -> None:
        router = AgentRouter(self._registry(), build_policy(["standard", "tools", "boss"]))
        first = router.select(TENANT)
        assert all(router.select(TENANT) == first for _ in range(5))

    def test_explain_reports_rejections(self) -> None:
        router = AgentRouter(self._registry(), build_policy(["standard"]))
        reasons = {item.agent_id: item.reason for item in router.explain(TENANT)}
        assert reasons["tools"] == "not_allowlisted"
        assert reasons["boss"] == "not_allowlisted"


class TestAgentRegistry:
    """Registration rules."""

    def test_duplicate_registration_refused(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry

        registry = AgentRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_spec(), lambda: None)
        with pytest.raises(ConfigurationError):
            registry.register(_spec(), lambda: None)

    def test_other_tenants_agents_are_invisible(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry

        registry = AgentRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_spec(agent_id="a", tenant_id="tenant-2"), lambda: None)
        assert registry.list_agents(TENANT) == ()
        assert registry.has(TENANT, "a") is False

    def test_unknown_agent_raises(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry

        registry = AgentRegistry(environment=Environment.DEVELOPMENT)
        with pytest.raises(AgentNotFoundError):
            registry.spec(TENANT, "nope")
        with pytest.raises(AgentNotFoundError):
            registry.build(TENANT, "nope")

    def test_factory_is_called_per_build(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry

        built: list[int] = []

        def factory() -> object:
            built.append(1)
            return object()

        registry = AgentRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_spec(), factory)
        registry.build(TENANT, "agent-1")
        registry.build(TENANT, "agent-1")
        assert len(built) == 2

    def test_dev_only_refused_in_production(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry

        registry = AgentRegistry(environment=Environment.PRODUCTION)
        with pytest.raises(ConfigurationError):
            registry.register(_spec(), lambda: None, dev_only=True)

    def test_list_by_kind(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry

        registry = AgentRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_spec(agent_id="a"), lambda: None)
        registry.register(_spec(agent_id="b", kind=AgentKind.TOOL_USER), lambda: None)
        assert [s.agent_id for s in registry.list_by_kind(TENANT, AgentKind.TOOL_USER)] == ["b"]


class TestInitialState:
    """Seeding."""

    def test_system_prompt_precedes_the_request(self) -> None:
        state = new_state("be brief", "hello")
        assert [m.role for m in state.messages] == ["system", "user"]

    def test_blank_parts_are_omitted(self) -> None:
        assert new_state("", "").messages == ()
        assert [m.role for m in new_state("", "hi").messages] == ["user"]


class TestSupervisor:
    """Supervisor construction and delegation."""

    def test_requires_a_supervisor_spec(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry
        from mas_orchestrator.agents.supervisor import SupervisorAgent

        with pytest.raises(ConfigurationError):
            SupervisorAgent(_spec(), AgentRegistry(environment=Environment.DEVELOPMENT))

    def test_delegate_list_is_explicit(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry
        from mas_orchestrator.agents.supervisor import SupervisorAgent

        supervisor = SupervisorAgent(
            _spec(kind=AgentKind.SUPERVISOR),
            AgentRegistry(environment=Environment.DEVELOPMENT),
            delegates=("a", "b"),
        )
        assert supervisor.delegates == ("a", "b")

    @pytest.mark.asyncio
    async def test_no_delegates_fails_rather_than_answering_itself(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry
        from mas_orchestrator.agents.supervisor import SupervisorAgent

        supervisor = SupervisorAgent(
            _spec(kind=AgentKind.SUPERVISOR),
            AgentRegistry(environment=Environment.DEVELOPMENT),
        )
        outcome = await supervisor.supervise(AgentState(messages=(Message.user("hi"),)), _context())
        assert outcome.termination is Termination.FAILED
        assert outcome.content == ""

    @pytest.mark.asyncio
    async def test_unregistered_delegate_is_refused_not_failed(self) -> None:
        from mas_orchestrator.agents.registry import AgentRegistry
        from mas_orchestrator.agents.supervisor import SupervisorAgent

        supervisor = SupervisorAgent(
            _spec(kind=AgentKind.SUPERVISOR),
            AgentRegistry(environment=Environment.DEVELOPMENT),
            delegates=("ghost",),
        )
        outcome = await supervisor.supervise(AgentState(messages=(Message.user("hi"),)), _context())
        assert outcome.refusal_count == 1
        assert outcome.delegations[0].refused is True


def _context() -> object:
    """A minimal execution context for the supervisor tests."""
    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
    from mas_orchestrator.security.identity import ServiceIdentity

    return ExecutionContext(
        tenant_id=TENANT,
        organization_id="org-1",
        execution_id="exec-1",
        agent_id="agent-1",
        correlation_id="corr-1",
        request_id="req-1",
        caller=ServiceIdentity(subject="test", issuer="test"),
        protocol_version=CURRENT_PROTOCOL_VERSION,
    )


class TestRuntimeConfig:
    """Runtime configuration bounds."""

    def test_invalid_config_refused(self) -> None:
        from mas_orchestrator.agents.runtime import RuntimeConfig

        with pytest.raises(ConfigurationError):
            RuntimeConfig(default_timeout_seconds=0)
        with pytest.raises(ConfigurationError):
            RuntimeConfig(max_parallel_agents=0)
        with pytest.raises(ConfigurationError):
            RuntimeConfig(cost_ceiling=-1)


class TestDefaultLimits:
    """Per-kind defaults."""

    def test_standard_agent_gets_no_tools(self) -> None:
        from mas_orchestrator.agents.runtime import build_default_limits

        assert build_default_limits(AgentKind.STANDARD).max_tool_calls == 0

    def test_tool_user_gets_a_budget(self) -> None:
        from mas_orchestrator.agents.runtime import build_default_limits

        limits = build_default_limits(AgentKind.TOOL_USER)
        assert limits.max_tool_calls > 0
        assert limits.max_steps > 1

    def test_supervisor_expects_to_delegate(self) -> None:
        from mas_orchestrator.agents.runtime import build_default_limits

        limits = build_default_limits(AgentKind.SUPERVISOR)
        assert limits.max_delegation_depth > 0


class TestExecutorConfiguration:
    """AgentRunConfig validation."""

    def test_bad_schema_refused(self) -> None:
        from mas_orchestrator.agents.executor import AgentRunConfig

        with pytest.raises(InvalidRequestError):
            AgentRunConfig(response_json_schema=["not", "an", "object"])

    def test_non_positive_ceiling_refused(self) -> None:
        from mas_orchestrator.agents.executor import AgentRunConfig

        with pytest.raises(InvalidRequestError):
            AgentRunConfig(cost_ceiling=0)


class TestToolArgumentDecoding:
    """Malformed model output must not crash the loop."""

    def test_empty_arguments_become_an_empty_object(self) -> None:
        from mas_orchestrator.agents.executor import AgentExecutor
        from mas_orchestrator.models.types import ToolCall

        assert AgentExecutor._decode_arguments(ToolCall(id="1", name="t", arguments_json="")) == {}

    def test_malformed_json_is_a_typed_error(self) -> None:
        from mas_orchestrator.agents.executor import AgentExecutor
        from mas_orchestrator.models.types import ToolCall

        with pytest.raises(InvalidRequestError):
            AgentExecutor._decode_arguments(ToolCall(id="1", name="t", arguments_json="{oops"))

    def test_valid_json_parses(self) -> None:
        from mas_orchestrator.agents.executor import AgentExecutor
        from mas_orchestrator.models.types import ToolCall

        assert AgentExecutor._decode_arguments(
            ToolCall(id="1", name="t", arguments_json='{"a":1}')
        ) == {"a": 1}


class TestStreamHelpers:
    """Small helpers the loop relies on."""

    def test_terminal_from_finish_reason(self) -> None:
        from mas_orchestrator.agents.executor import terminal_from_finish_reason
        from mas_orchestrator.models.types import FinishReason

        assert terminal_from_finish_reason(FinishReason.STOP) is Termination.COMPLETED
        assert terminal_from_finish_reason(FinishReason.LENGTH) is Termination.COMPLETED
        assert terminal_from_finish_reason(FinishReason.TOOL_CALLS) is None

    def test_applies_to_stream(self) -> None:
        from mas_orchestrator.agents.executor import applies_to_stream
        from mas_orchestrator.models.types import ModelStreamEvent

        assert applies_to_stream(ModelStreamEvent(model=ModelRef("o", "m"), delta_text="x")) is True
        assert applies_to_stream(ModelStreamEvent(model=ModelRef("o", "m"))) is False


class TestUsageRecording:
    """The ledger records what the provider reported, not an estimate."""

    def test_unmeasured_usage_is_recorded_as_a_count(self) -> None:
        from mas_orchestrator.models.cost import CostLedger, PricingTable

        ledger = CostLedger(PricingTable(rates={}))
        response = ModelResponse(model=ModelRef("openai", "m"), text="hi", usage=None)
        executor_ledger = ledger
        executor_ledger.record(response.model, response.usage)
        assert ledger.totals().unmeasured_call_count == 1

    def test_reported_usage_accumulates(self) -> None:
        from mas_orchestrator.models.cost import CostLedger, PricingTable

        ledger = CostLedger(PricingTable(rates={}), emit_unpriced_warning=False)
        response = ModelResponse(
            model=ModelRef("openai", "m"),
            text="hi",
            usage=Usage(input_tokens=10, output_tokens=5),
        )
        ledger.record(response.model, response.usage)
        assert ledger.totals().input_tokens == 10
