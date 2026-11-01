"""Security and contract tests for the read-only platform assistant."""

from __future__ import annotations

import json
from typing import Any

import pytest

from mas_orchestrator.agents.base import (
    AgentKind,
    AgentLimits,
    AgentOutcome,
    AgentSpec,
    AgentState,
    Termination,
)
from mas_orchestrator.agents.builtin import register_builtin_agents
from mas_orchestrator.agents.builtin.platform_assistant import (
    PLATFORM_ASSISTANT_AGENT_ID,
    AgentDraftProposal,
    AssistantQuickAction,
    PlatformAssistantAgent,
    PlatformAssistantResponse,
)
from mas_orchestrator.agents.factory import AgentFactory
from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry
from mas_orchestrator.config.enums import Environment
from mas_orchestrator.context.execution_context import build_execution_context
from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.models.types import Message
from mas_orchestrator.prompts.builtin_assistant import register_builtin_assistant_prompt
from mas_orchestrator.prompts.registry import PromptRegistry
from mas_orchestrator.security.identity import ServiceIdentity, ServiceScope

TENANT_ID = "tenant-platform-assistant"
CALLER = ServiceIdentity(
    subject="rust-core",
    issuer="test",
    scopes=frozenset({ServiceScope.EXECUTE_AGENT}),
)


def _context(*, attributes: dict[str, object] | None = None):
    return build_execution_context(
        tenant_id=TENANT_ID,
        organization_id="org-platform-assistant",
        execution_id="exec-platform-assistant",
        agent_id=PLATFORM_ASSISTANT_AGENT_ID,
        correlation_id="corr-platform-assistant",
        request_id="req-platform-assistant",
        protocol_version=str(CURRENT_PROTOCOL_VERSION),
        caller=CALLER,
        attributes=attributes,
    )


def _spec(*, read_only: bool = True, allowed_tools: frozenset[str] = frozenset()) -> AgentSpec:
    return AgentSpec(
        agent_id=PLATFORM_ASSISTANT_AGENT_ID,
        agent_version_id="1",
        name="Platform Assistant",
        slug="platform-assistant",
        kind=AgentKind.STANDARD,
        tenant_id=TENANT_ID,
        description="Read-only assistant",
        allowed_tools=allowed_tools,
        limits=AgentLimits(max_steps=4, max_tool_calls=0, max_delegation_depth=0),
        read_only=read_only,
    )


class _Executor:
    def __init__(self, content: str, *, tool_calls_made: int = 0) -> None:
        self.content = content
        self.tool_calls_made = tool_calls_made
        self.received_state: AgentState | None = None
        self.received_context: Any = None

    async def run(
        self,
        state: AgentState,
        context: Any,
        *,
        cancel_event: Any = None,
    ) -> AgentOutcome:
        self.received_state = state
        self.received_context = context
        return AgentOutcome(
            agent_id=PLATFORM_ASSISTANT_AGENT_ID,
            termination=Termination.COMPLETED,
            content=self.content,
            messages=state.messages,
            steps_taken=1,
            tool_calls_made=self.tool_calls_made,
            duration_ms=12,
        )


def _model_json(
    action: AssistantQuickAction,
    *,
    evidence: list[dict[str, str]] | None = None,
    proposal: dict[str, object] | None = None,
) -> str:
    return json.dumps(
        {
            "action": action.value,
            "summary": "Supported summary.",
            "evidence": evidence or [],
            "proposal": proposal,
        }
    )


class TestRegistration:
    def test_builtin_is_registered_per_tenant_as_read_only_and_tool_free(self) -> None:
        registry = TenantScopedAgentRegistry(
            environment=Environment.TEST,
            catalog=(
                {
                    "agent_id": "platform.default",
                    "name": "Default",
                    "slug": "default",
                    "kind": AgentKind.STANDARD,
                },
            ),
            default_agent_id="platform.default",
        )
        prompts = PromptRegistry()

        register_builtin_agents(registry, prompts=prompts)
        spec = registry.spec(TENANT_ID, PLATFORM_ASSISTANT_AGENT_ID)

        assert spec.read_only is True
        assert registry.is_read_only(TENANT_ID, PLATFORM_ASSISTANT_AGENT_ID) is True
        assert registry.is_read_only(TENANT_ID, "missing-agent") is False
        assert spec.kind is AgentKind.STANDARD
        assert spec.allowed_tools == frozenset()
        assert spec.limits.max_tool_calls == 0
        assert prompts.current_version("platform_assistant.system") == 1
        assert spec.tags[3].startswith("prompt_fingerprint:sha256:")

    def test_builtin_registration_is_closed_after_factory_binding(self) -> None:
        registry = TenantScopedAgentRegistry(
            environment=Environment.TEST,
            catalog=(
                {
                    "agent_id": "platform.default",
                    "name": "Default",
                    "slug": "default",
                    "kind": AgentKind.STANDARD,
                },
            ),
            default_agent_id="platform.default",
        )
        register_builtin_agents(registry)
        registry.bind_factory(AgentFactory(router=object(), provider_for=lambda _model: object()))  # type: ignore[arg-type]

        with pytest.raises(ConfigurationError, match="before registry startup"):
            registry.register_builtin(
                {
                    "agent_id": "another-builtin",
                    "kind": AgentKind.STANDARD,
                    "name": "Another",
                    "slug": "another",
                }
            )

    def test_factory_dispatches_product_owned_assistant(self) -> None:
        factory = AgentFactory(router=object(), provider_for=lambda _model: object())  # type: ignore[arg-type]

        agent = factory.build(_spec())

        assert isinstance(agent, PlatformAssistantAgent)
        assert agent.spec.read_only is True

    def test_factory_refuses_misconfigured_assistant(self) -> None:
        factory = AgentFactory(router=object(), provider_for=lambda _model: object())  # type: ignore[arg-type]

        with pytest.raises(ConfigurationError, match="read-only standard agent"):
            factory.build(_spec(read_only=False))


class TestStructuredResponse:
    def test_metric_evidence_must_match_snapshot_reference(self) -> None:
        with pytest.raises(InvalidRequestError, match="absent from the supplied snapshot"):
            PlatformAssistantResponse.parse(
                _model_json(
                    AssistantQuickAction.PERFORMANCE_INSIGHTS,
                    evidence=[{"metric_ref": "forged", "claim": "Latency improved."}],
                ),
                expected_action=AssistantQuickAction.PERFORMANCE_INSIGHTS,
                allowed_metric_refs=frozenset({"metric:latency:p95"}),
            )

    def test_duplicate_json_keys_are_rejected(self) -> None:
        with pytest.raises(InvalidRequestError, match="valid JSON"):
            PlatformAssistantResponse.parse(
                '{"action":"view_recommendations","action":"create_new_agent",'
                '"summary":"x","evidence":[],"proposal":null}',
                expected_action=AssistantQuickAction.VIEW_RECOMMENDATIONS,
                allowed_metric_refs=frozenset(),
            )

    def test_create_agent_must_be_a_no_tool_standard_draft(self) -> None:
        proposal = {
            "name": "Analytics Helper",
            "slug": "analytics-helper",
            "description": "Summarizes analytics.",
            "objective": "Summarize approved analytics metrics.",
            "system_prompt": "Use only supplied analytics data.",
            "kind": "standard",
            "model_hint": None,
            "suggested_tools": [],
        }
        response = PlatformAssistantResponse.parse(
            _model_json(AssistantQuickAction.CREATE_NEW_AGENT, proposal=proposal),
            expected_action=AssistantQuickAction.CREATE_NEW_AGENT,
            allowed_metric_refs=frozenset(),
        )

        assert response.proposal is not None
        assert response.proposal.suggested_tools == ()
        assert response.proposal.kind == "standard"

    def test_create_agent_cannot_recommend_tool_capabilities(self) -> None:
        proposal = {
            "name": "Analytics Helper",
            "slug": "analytics-helper",
            "description": "Summarizes analytics.",
            "objective": "Summarize approved analytics metrics.",
            "system_prompt": "Use only supplied analytics data.",
            "kind": "standard",
            "model_hint": None,
            "suggested_tools": ["database.query"],
        }
        with pytest.raises(InvalidRequestError, match="cannot grant or recommend"):
            AgentDraftProposal.from_mapping(proposal)

    def test_create_agent_cannot_be_a_tool_user(self) -> None:
        proposal = {
            "name": "Analytics Helper",
            "slug": "analytics-helper",
            "description": "Summarizes analytics.",
            "objective": "Summarize approved analytics metrics.",
            "system_prompt": "Use only supplied analytics data.",
            "kind": "tool_user",
            "model_hint": None,
            "suggested_tools": [],
        }
        with pytest.raises(InvalidRequestError, match="standard no-tool agent"):
            AgentDraftProposal.from_mapping(proposal)


class TestAgentRun:
    @pytest.mark.asyncio
    async def test_prompt_uses_action_instructions_and_snapshot_evidence(self) -> None:
        action = AssistantQuickAction.PERFORMANCE_INSIGHTS
        content = _model_json(
            action,
            evidence=[{"metric_ref": "metric:latency:p95", "claim": "p95 latency is 120 ms."}],
        )
        executor = _Executor(content)
        prompts = PromptRegistry()
        register_builtin_assistant_prompt(prompts)
        agent = PlatformAssistantAgent(_spec(), executor, prompts=prompts)  # type: ignore[arg-type]
        snapshot = {
            "quick_action": action.value,
            "snapshot": {
                "analytics": {
                    "metrics": [{"metric_ref": "metric:latency:p95", "value": 120, "unit": "ms"}]
                }
            },
        }
        state = AgentState(messages=(Message.user(json.dumps(snapshot)),))
        context = _context()

        outcome = await agent.run(state, context)

        assert outcome.succeeded
        assert json.loads(outcome.content)["evidence"][0]["metric_ref"] == "metric:latency:p95"
        assert executor.received_state is not None
        system_prompt = executor.received_state.messages[0].content
        assert "Analyze the supplied performance snapshot" in system_prompt
        assert context.tenant_id == TENANT_ID
        assert executor.received_context is context

    @pytest.mark.asyncio
    async def test_numeric_claim_not_present_in_snapshot_becomes_typed_failure(self) -> None:
        action = AssistantQuickAction.PERFORMANCE_INSIGHTS
        executor = _Executor(
            _model_json(action, evidence=[{"metric_ref": "metric:latency:p95", "claim": "999 ms."}])
        )
        agent = PlatformAssistantAgent(_spec(), executor)  # type: ignore[arg-type]
        snapshot = {
            "quick_action": action.value,
            "snapshot": {
                "metrics": [{"metric_ref": "metric:latency:p95", "value": 120, "unit": "ms"}]
            },
        }
        state = AgentState(messages=(Message.user(json.dumps(snapshot)),))

        outcome = await agent.run(state, _context())

        assert outcome.termination is Termination.FAILED
        assert outcome.error is not None
        assert "numeric claims absent" in outcome.error.details["reason"]

    @pytest.mark.asyncio
    async def test_malformed_model_response_becomes_typed_failure(self) -> None:
        executor = _Executor("not JSON")
        agent = PlatformAssistantAgent(_spec(), executor)  # type: ignore[arg-type]
        state = AgentState(messages=(Message.user('{"snapshot": {}}'),))

        outcome = await agent.run(state, _context())

        assert outcome.termination is Termination.FAILED
        assert outcome.error is not None
        assert outcome.error.code == "output_validation_failed"

    @pytest.mark.asyncio
    async def test_tool_attempt_fails_without_having_tool_executor(self) -> None:
        executor = _Executor(
            _model_json(AssistantQuickAction.VIEW_RECOMMENDATIONS),
            tool_calls_made=1,
        )
        agent = PlatformAssistantAgent(_spec(), executor)  # type: ignore[arg-type]
        state = AgentState(messages=(Message.user('{"snapshot": {}}'),))

        outcome = await agent.run(state, _context())

        assert outcome.termination is Termination.FAILED
        assert outcome.tool_calls_made == 1
        assert outcome.error is not None
        assert "attempted to invoke a tool" in str(outcome.error)

    @pytest.mark.asyncio
    async def test_invalid_action_attribute_fails_closed(self) -> None:
        executor = _Executor(_model_json(AssistantQuickAction.VIEW_RECOMMENDATIONS))
        agent = PlatformAssistantAgent(_spec(), executor)  # type: ignore[arg-type]
        state = AgentState(messages=(Message.user('{"snapshot": {}}'),))

        outcome = await agent.run(state, _context(attributes={"assistant_action": "write_data"}))

        assert outcome.termination is Termination.FAILED
        assert executor.received_state is None
        assert outcome.error is not None
        assert "unsupported" in str(outcome.error)

    def test_constructor_refuses_tool_capability_or_mutable_spec(self) -> None:
        executor = _Executor(_model_json(AssistantQuickAction.VIEW_RECOMMENDATIONS))
        with pytest.raises(InvalidRequestError, match="read-only standard no-tool"):
            PlatformAssistantAgent(_spec(allowed_tools=frozenset({"database.query"})), executor)  # type: ignore[arg-type]
        with pytest.raises(InvalidRequestError, match="read-only standard no-tool"):
            PlatformAssistantAgent(_spec(read_only=False), executor)  # type: ignore[arg-type]
