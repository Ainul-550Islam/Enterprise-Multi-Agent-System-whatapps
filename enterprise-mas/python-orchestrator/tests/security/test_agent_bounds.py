"""Adversarial tests for agent bounds.

These run the real loop against a scripted model. The question each test asks
is "what stops this?" — an agent that never finishes, an agent that asks for
tools it was not granted, an agent that tries to escape its delegation depth.

A bound that is only checked after the work it limits is not a bound.
"""

from __future__ import annotations

import asyncio
from typing import ClassVar

import pytest

from mas_orchestrator.agents.base import (
    AgentKind,
    AgentLimits,
    AgentSpec,
    AgentState,
    Termination,
)
from mas_orchestrator.agents.executor import AgentExecutor, AgentRunConfig
from mas_orchestrator.config.enums import Environment
from mas_orchestrator.context import ExecutionContext
from mas_orchestrator.contracts.errors import (
    InvalidRequestError,
    ToolPermissionDeniedError,
)
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.models.registry import ModelRegistry
from mas_orchestrator.models.router import ModelRouter, RoutingPolicy
from mas_orchestrator.models.types import (
    FinishReason,
    Message,
    ModelCapabilities,
    ModelRef,
    ModelResponse,
    ToolCall,
    Usage,
)
from mas_orchestrator.security.identity import ServiceIdentity
from mas_orchestrator.tools.permissions import AllowListPermissionBridge
from mas_orchestrator.tools.registry import ToolRegistry
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolKind,
    ToolSafety,
    ToolVersion,
)

TENANT = "tenant-1"
MODEL = ModelRef("openai", "gpt-4o")


class ScriptedProvider:
    """A provider that returns a fixed sequence of responses.

    Scripted rather than mocked: it implements the real protocol, so the loop
    under test is the production loop.
    """

    def __init__(self, responses: list[ModelResponse]) -> None:
        self._responses = responses
        self.calls = 0

    @property
    def provider_id(self) -> str:
        """Identifier used by the registry."""
        return "scripted"

    def supports(self, model: ModelRef) -> bool:
        """Only the one model is served."""
        return model == MODEL

    def capabilities(self, model: ModelRef) -> ModelCapabilities:
        """Declared capabilities for the scripted model."""
        return ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=False,
            supports_system_prompt=True,
            max_input_tokens=100_000,
            max_output_tokens=8_192,
        )

    async def complete(
        self,
        request: object,
        *,
        timeout_seconds: float,
        cancel_event: object = None,
    ) -> ModelResponse:
        """Returns the next scripted response, repeating the last one."""
        self.calls += 1
        index = min(self.calls - 1, len(self._responses) - 1)
        return self._responses[index]

    async def aclose(self) -> None:
        """No resources to release."""
        return None


def _text(text: str) -> ModelResponse:
    return ModelResponse(model=MODEL, text=text, finish_reason=FinishReason.STOP, usage=Usage(1, 1))


def _tool_call(name: str = "demo", arguments: str = "{}") -> ModelResponse:
    return ModelResponse(
        model=MODEL,
        text="",
        finish_reason=FinishReason.TOOL_CALLS,
        tool_calls=(ToolCall(id="c1", name=name, arguments_json=arguments),),
        usage=Usage(1, 1),
    )


def _context(*, deadline_seconds: float | None = None) -> ExecutionContext:
    import time

    return ExecutionContext(
        tenant_id=TENANT,
        organization_id="org-1",
        execution_id="exec-1",
        agent_id="agent-1",
        correlation_id="corr-1",
        request_id="req-1",
        caller=ServiceIdentity(subject="test", issuer="test"),
        protocol_version=CURRENT_PROTOCOL_VERSION,
        deadline_epoch_ms=(
            int((time.time() + deadline_seconds) * 1000) if deadline_seconds is not None else None
        ),
    )


def _router(provider: ScriptedProvider) -> ModelRouter:
    registry = ModelRegistry()
    registry.register(provider)
    registry.register_model(MODEL, "scripted")
    return ModelRouter(registry, RoutingPolicy(allowed_models={MODEL.key}))


def _tool_registry() -> ToolRegistry:
    """Registry holding both a granted and an ungranted tool.

    "secret" is deliberately registered but never granted. That is the case
    worth testing: if it were absent from the registry the executor would
    report "not found" and the permission bridge would never be consulted,
    which says nothing about whether permissions hold.
    """
    registry = ToolRegistry(environment=Environment.DEVELOPMENT)
    for tool_id in ("demo", "secret"):
        registry.register(
            ToolDefinition(
                tool_id=tool_id,
                name=tool_id,
                version=ToolVersion(1),
                kind=ToolKind.INTERNAL,
                input_schema={"type": "object"},
                tenant_id=TENANT,
                idempotent=True,
            ),
            _OkTool(),
        )
    return registry


class _OkTool:
    async def invoke(self, arguments: object, context: object) -> object:
        return "tool ran"


def _tool_executor(registry: ToolRegistry, allowed: frozenset[str]) -> object:
    from mas_orchestrator.tools.executor import ToolExecutor, ToolExecutorConfig

    return ToolExecutor(
        registry,
        AllowListPermissionBridge(
            {(TENANT, name) for name in allowed},
            environment=Environment.DEVELOPMENT,
        ),
        config=ToolExecutorConfig(default_timeout_seconds=5.0),
    )


def _executor(
    responses: list[ModelResponse],
    *,
    kind: AgentKind = AgentKind.TOOL_USER,
    limits: AgentLimits | None = None,
    allowed_tools: frozenset[str] = frozenset({"demo"}),
    with_tools: bool = True,
) -> tuple[AgentExecutor, ScriptedProvider]:
    provider = ScriptedProvider(responses)
    router = _router(provider)
    spec = AgentSpec(
        agent_id="agent-1",
        agent_version_id="v1",
        name="Agent",
        slug="agent",
        kind=kind,
        tenant_id=TENANT,
        limits=limits or AgentLimits(max_steps=10, max_tool_calls=10),
        allowed_tools=allowed_tools,
        system_prompt="be brief",
    )
    executor = AgentExecutor(
        spec,
        router=router,
        provider_for=lambda model: provider,
        tool_executor=_tool_executor(_tool_registry(), allowed_tools) if with_tools else None,
        tool_registry=_tool_registry() if with_tools else None,
    )
    return executor, provider


class TestStepBound:
    """An agent that never finishes must be stopped."""

    @pytest.mark.asyncio
    async def test_step_limit_stops_an_endless_agent(self) -> None:
        """The model always asks for another tool, so it can never finish."""
        executor, provider = _executor(
            [_tool_call()], limits=AgentLimits(max_steps=3, max_tool_calls=100)
        )
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.STEP_LIMIT
        assert outcome.succeeded is False
        assert provider.calls == 3

    @pytest.mark.asyncio
    async def test_first_answer_finishes_immediately(self) -> None:
        executor, provider = _executor([_text("done")])
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.COMPLETED
        assert outcome.content == "done"
        assert provider.calls == 1


class TestToolCallBound:
    """Tool calls are bounded separately from steps."""

    @pytest.mark.asyncio
    async def test_tool_call_limit_stops_the_loop(self) -> None:
        executor, _ = _executor([_tool_call()], limits=AgentLimits(max_steps=100, max_tool_calls=2))
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.TOOL_CALL_LIMIT
        assert outcome.tool_calls_made == 2

    @pytest.mark.asyncio
    async def test_tool_results_reach_the_model(self) -> None:
        """The loop feeds results back; otherwise it would spin forever."""
        executor, provider = _executor([_tool_call(), _text("done")])
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.COMPLETED
        assert provider.calls == 2


class TestCancellation:
    """Cancellation is checked before work, not after."""

    @pytest.mark.asyncio
    async def test_pre_set_cancellation_stops_before_any_model_call(self) -> None:
        executor, provider = _executor([_text("done")])
        event = asyncio.Event()
        event.set()
        outcome = await executor.run(
            AgentState(messages=(Message.user("go"),)), _context(), cancel_event=event
        )
        assert outcome.termination is Termination.CANCELLED
        assert provider.calls == 0

    @pytest.mark.asyncio
    async def test_expired_deadline_stops_before_any_model_call(self) -> None:
        executor, provider = _executor([_text("done")])
        outcome = await executor.run(
            AgentState(messages=(Message.user("go"),)), _context(deadline_seconds=-1)
        )
        assert outcome.termination is Termination.DEADLINE
        assert provider.calls == 0


class TestUngrantedTools:
    """The model cannot widen its own permissions."""

    @pytest.mark.asyncio
    async def test_tool_outside_the_allowlist_is_denied(self) -> None:
        """The model asks for 'secret'; only 'demo' was granted."""
        executor, _ = _executor([_tool_call("secret")], allowed_tools=frozenset({"demo"}))
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        # The call is refused, the loop continues, and the step bound stops it.
        assert outcome.succeeded is False
        assert outcome.termination is Termination.STEP_LIMIT

    @pytest.mark.asyncio
    async def test_standard_agent_is_never_shown_tools(self) -> None:
        """A standard agent must not be advertised tool capability."""
        executor, _ = _executor(
            [_tool_call(), _text("done")],
            kind=AgentKind.STANDARD,
            allowed_tools=frozenset({"demo"}),
        )
        assert executor._tool_specs() == ()

    @pytest.mark.asyncio
    async def test_standard_agent_ignores_tool_requests(self) -> None:
        """When the model asks for tools anyway, the agent must not run them."""
        executor, provider = _executor(
            [_tool_call(), _text("done")],
            kind=AgentKind.STANDARD,
            allowed_tools=frozenset({"demo"}),
        )
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        # It stops with the first response rather than dispatching a tool.
        assert outcome.termination is Termination.COMPLETED
        assert outcome.tool_calls_made == 0
        assert provider.calls == 1

    @pytest.mark.asyncio
    async def test_denied_tool_reports_the_failure_to_the_model(self) -> None:
        """A denial must reach the transcript as a failure, not vanish."""
        provider = ScriptedProvider([_tool_call("secret"), _text("done")])
        spec = AgentSpec(
            agent_id="agent-1",
            agent_version_id="v1",
            name="Agent",
            slug="agent",
            kind=AgentKind.TOOL_USER,
            tenant_id=TENANT,
            limits=AgentLimits(max_steps=5, max_tool_calls=5),
            allowed_tools=frozenset({"demo"}),
        )
        registry = _tool_registry()
        executor = AgentExecutor(
            spec,
            router=_router(provider),
            provider_for=lambda model: provider,
            tool_executor=_tool_executor(registry, frozenset({"demo"})),
            tool_registry=registry,
        )
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.COMPLETED
        # The tool result message records the denial.
        tool_messages = [m for m in outcome.messages if m.role == "tool"]
        assert tool_messages
        assert "denied" in tool_messages[0].content.lower()


class TestMalformedModelOutput:
    """A model that emits broken tool arguments must not crash the loop."""

    @pytest.mark.asyncio
    async def test_malformed_arguments_are_reported_not_raised(self) -> None:
        executor, _ = _executor([_tool_call("demo", "{oops"), _text("done")])
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.COMPLETED
        tool_messages = [m for m in outcome.messages if m.role == "tool"]
        assert "malformed" in tool_messages[0].content.lower()


class TestUnrunnableAgents:
    """Lifecycle gating."""

    @pytest.mark.asyncio
    async def test_draft_agent_does_not_run(self) -> None:
        from mas_orchestrator.agents.base import AgentStatus

        provider = ScriptedProvider([_text("done")])
        spec = AgentSpec(
            agent_id="agent-1",
            agent_version_id="v1",
            name="Agent",
            slug="agent",
            kind=AgentKind.STANDARD,
            tenant_id=TENANT,
            status=AgentStatus.DRAFT,
        )
        executor = AgentExecutor(
            spec, router=_router(provider), provider_for=lambda model: provider
        )
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.FAILED
        assert provider.calls == 0


class TestToolBatchBound:
    """One model turn cannot schedule unbounded work."""

    def test_batch_limit_exists(self) -> None:
        from mas_orchestrator.agents.executor import MAX_TOOL_BATCH

        assert MAX_TOOL_BATCH > 0
        assert MAX_TOOL_BATCH <= 64

    @pytest.mark.asyncio
    async def test_oversized_batch_stops_the_loop(self) -> None:
        from mas_orchestrator.agents.executor import MAX_TOOL_BATCH

        many = tuple(
            ToolCall(id=f"c{i}", name="demo", arguments_json="{}")
            for i in range(MAX_TOOL_BATCH + 1)
        )
        responses = [
            ModelResponse(
                model=MODEL,
                finish_reason=FinishReason.TOOL_CALLS,
                tool_calls=many,
                usage=Usage(1, 1),
            )
        ]
        executor, _ = _executor(responses)
        outcome = await executor.run(AgentState(messages=(Message.user("go"),)), _context())
        assert outcome.termination is Termination.TOOL_CALL_LIMIT
        assert outcome.tool_calls_made == 0


class TestConfigValidation:
    """Executor configuration."""

    def test_bad_schema_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            AgentRunConfig(response_json_schema=["x"])

    def test_non_positive_ceiling_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            AgentRunConfig(cost_ceiling=0)


class TestSafetyClassification:
    """Tools with a blast radius need explicit approval."""

    def test_destructive_tool_needs_approval(self) -> None:
        definition = ToolDefinition(
            tool_id="wipe",
            name="wipe",
            version=ToolVersion(1),
            kind=ToolKind.INTERNAL,
            safety=ToolSafety.DESTRUCTIVE,
            tenant_id=TENANT,
        )
        from mas_orchestrator.tools.permissions import enforce_permission

        bridge = AllowListPermissionBridge({(TENANT, "wipe")}, environment=Environment.DEVELOPMENT)
        from mas_orchestrator.tools.types import ToolInvocationContext

        context = ToolInvocationContext(
            tenant_id=TENANT,
            execution_id="e",
            agent_id="a",
            invocation_id="i",
        )
        with pytest.raises(ToolPermissionDeniedError):
            enforce_permission(definition, context, bridge)


class TestProjectLayout:
    """Sanity checks on the agent package itself."""

    AGENTS_ROOT: ClassVar[str] = "src/mas_orchestrator/agents"

    def test_modules_exist(self) -> None:
        import pathlib

        root = pathlib.Path(self.AGENTS_ROOT)
        for name in (
            "base.py",
            "executor.py",
            "supervisor.py",
            "delegation.py",
            "planner.py",
            "registry.py",
            "factory.py",
            "router.py",
            "runtime.py",
        ):
            assert (root / name).is_file(), f"missing {name}"
