"""End-to-end execution of a single agent.

These tests run the real pipeline, not a sketch of it: a real
:class:`ModelRouter` over a real :class:`ModelRegistry`, a real
:class:`AgentExecutor` loop, a real :class:`ToolExecutor` in front of a real
:class:`ToolRegistry`, and a real :class:`CostLedger`. The only substituted
parts are the model itself and the permission authority, both of which are
external systems.

What is being proved is that the seams hold when the whole machine runs:

* **Routing happens before the call, and failover respects it.** The model
  that answers is one the policy allowed.
* **Tools are gated twice** — once by the agent's allowlist, once by the
  permission authority — and a denial is reported to the model as a denial
  rather than as data.
* **Tenancy is carried, not asked for.** The tenant in the tool invocation
  comes from the execution context; a tool belonging to another tenant is
  invisible.
* **Every bound terminates the loop with a named reason.** Reaching a limit
  produces a distinct :class:`Termination`, so "finished" and "stopped" are
  never the same outcome.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import pytest

from mas_orchestrator.agents.base import (
    AgentKind,
    AgentLimits,
    AgentSpec,
    AgentState,
    AgentStatus,
    Termination,
)
from mas_orchestrator.agents.executor import AgentExecutor, AgentRunConfig
from mas_orchestrator.context import ExecutionContext
from mas_orchestrator.contracts.errors import (
    AuthenticationError,
    InvalidRequestError,
    ModelRateLimitedError,
)
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.models.cost import CostLedger, PricingTable
from mas_orchestrator.models.registry import ModelRegistry
from mas_orchestrator.models.router import ModelRouter, RoutingPolicy
from mas_orchestrator.models.types import (
    FinishReason,
    Message,
    ModelRef,
    ModelRequest,
    ModelResponse,
    ToolCall,
    Usage,
)
from mas_orchestrator.security.identity import ServiceIdentity
from mas_orchestrator.tools.executor import ToolExecutor, ToolExecutorConfig
from mas_orchestrator.tools.permissions import PermissionDecision, PermissionResult
from mas_orchestrator.tools.registry import ToolRegistry
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolInvocationContext,
    ToolKind,
    ToolSafety,
    ToolStatus,
    ToolVersion,
)

TENANT: str = "tenant-a"
OTHER_TENANT: str = "tenant-b"
ORGANIZATION: str = "org-a"

PRIMARY: ModelRef = ModelRef(provider="openai", model="gpt-4o")
SECONDARY: ModelRef = ModelRef(provider="anthropic", model="claude-3-5-sonnet")


# --- the one external system that is substituted: the model ------------------


@dataclass(slots=True)
class ScriptedModel:
    """A model that answers from a scripted list of responses.

    This is the only substituted component in the pipeline, because it stands
    for a remote system. It still implements the whole
    :class:`~mas_orchestrator.models.base.ModelProvider` surface the executor
    uses, so the call path under test is the production one.
    """

    provider_id: str
    responses: list[ModelResponse]
    failures: dict[str, BaseException] = field(default_factory=dict)
    requests: list[ModelRequest] = field(default_factory=list)
    #: Called with each request before it is answered, so a test can change
    #: state mid-run (for example to request cancellation).
    on_request: Any = None

    def supports(self, model: ModelRef) -> bool:
        """Whether this provider owns *model*."""
        return model.provider == self.provider_id

    def capabilities(self, model: ModelRef) -> object:
        """Capabilities for *model*."""
        from mas_orchestrator.models.types import ModelCapabilities

        return ModelCapabilities(supports_tools=True, supports_structured_output=True)

    async def complete(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Returns the next scripted response, or raises the scripted error."""
        self.requests.append(request)
        if self.on_request is not None:
            self.on_request(request)
        key = request.model.key
        if key in self.failures:
            raise self.failures.pop(key)
        if not self.responses:
            raise AssertionError(f"scripted model {key} has no response left")
        return self.responses.pop(0)

    async def aclose(self) -> None:
        """Nothing to release."""
        return None


def _text(model: ModelRef, text: str, **kwargs: Any) -> ModelResponse:
    """Builds a text-only response."""
    return ModelResponse(
        model=model,
        text=text,
        finish_reason=FinishReason.STOP,
        usage=Usage(input_tokens=10, output_tokens=5),
        **kwargs,
    )


def _tool_call(model: ModelRef, name: str, arguments: dict[str, Any]) -> ModelResponse:
    """Builds a response that requests one tool call."""
    import json

    return ModelResponse(
        model=model,
        text="",
        finish_reason=FinishReason.TOOL_CALLS,
        tool_calls=(ToolCall(id="call-1", name=name, arguments_json=json.dumps(arguments)),),
        usage=Usage(input_tokens=10, output_tokens=5),
    )


# --- tools and permissions ---------------------------------------------------


@dataclass(slots=True)
class _EchoHandler:
    """Returns its arguments, and records what it was given."""

    seen: list[ToolInvocationContext] = field(default_factory=list)
    output: Any = None

    async def invoke(
        self,
        arguments: dict[str, Any],
        context: ToolInvocationContext,
    ) -> object:
        """Records the invocation and returns the configured output."""
        self.seen.append(context)
        return self.output if self.output is not None else arguments


@dataclass(slots=True)
class _RecordingBridge:
    """A permission authority that answers from a script."""

    decisions: dict[str, PermissionDecision] = field(default_factory=dict)
    default: PermissionDecision = PermissionDecision.ALLOW
    checked: list[str] = field(default_factory=list)

    def check(
        self,
        tool: ToolDefinition,
        context: ToolInvocationContext,
    ) -> PermissionResult:
        """Returns the scripted decision for *tool*."""
        self.checked.append(tool.tool_id)
        decision = self.decisions.get(tool.tool_id, self.default)
        return PermissionResult(
            decision=decision,
            tool_id=tool.tool_id,
            tool_version=tool.version.number,
            reason=f"scripted {decision.value}",
            decision_id="dec-1",
            source="test-bridge",
        )


@dataclass(slots=True)
class _Harness:
    """Everything needed to run one agent end to end."""

    executor: AgentExecutor
    model: ScriptedModel
    ledger: CostLedger
    permissions: _RecordingBridge


def _tool(
    tool_id: str = "echo",
    *,
    tenant_id: str = TENANT,
    safety: ToolSafety = ToolSafety.STANDARD,
) -> ToolDefinition:
    """Builds a published tool definition."""
    return ToolDefinition(
        tool_id=tool_id,
        name=tool_id,
        version=ToolVersion(number=1),
        kind=ToolKind.INTERNAL,
        safety=safety,
        status=ToolStatus.PUBLISHED,
        input_schema={"type": "object", "properties": {"value": {"type": "string"}}},
        description="Echoes its arguments back.",
        tenant_id=tenant_id,
        idempotent=True,
    )


def _harness(
    responses: list[ModelResponse],
    *,
    spec: AgentSpec | None = None,
    tools: tuple[ToolDefinition, ...] = (),
    handlers: dict[str, Any] | None = None,
    permissions: _RecordingBridge | None = None,
    ledger: CostLedger | None = None,
    config: AgentRunConfig | None = None,
    failures: dict[str, BaseException] | None = None,
    allowed_models: frozenset[str] = frozenset({PRIMARY.key, SECONDARY.key}),
    preferred: tuple[str, ...] = (PRIMARY.key, SECONDARY.key),
    registered: tuple[ModelRef, ...] = (PRIMARY, SECONDARY),
) -> _Harness:
    """Wires the whole pipeline for one agent run."""
    agent_spec = spec or _spec()

    registry = ModelRegistry()
    model = ScriptedModel(
        provider_id="openai",
        responses=responses,
        failures=dict(failures or {}),
    )
    registry.register(model)
    # Models are bound explicitly: registering a provider does not advertise
    # any model on its own.
    for ref in registered:
        registry.register_model(ref, "openai")

    # The preference order is explicit so a test can say which model it
    # expects to answer. Without it the chain is ordered by model key, which
    # would make "the primary answered" an assertion about alphabetical order
    # rather than about routing.
    router = ModelRouter(
        registry,
        RoutingPolicy(allowed_models=allowed_models, preferred_models=preferred),
    )

    tool_registry = ToolRegistry()
    handlers = handlers or {}
    for definition in tools:
        tool_registry.register(definition, handlers.get(definition.tool_id, _EchoHandler()))

    bridge = permissions or _RecordingBridge()
    tool_executor = ToolExecutor(
        tool_registry,
        bridge,
        config=ToolExecutorConfig(retry_policy=None),
    )

    executor = AgentExecutor(
        agent_spec,
        router=router,
        provider_for=lambda ref: model,
        tool_executor=tool_executor,
        tool_registry=tool_registry,
        ledger=ledger,
        config=config,
    )
    return _Harness(executor=executor, model=model, ledger=ledger, permissions=bridge)


def _spec(
    *,
    agent_id: str = "agent-1",
    kind: AgentKind = AgentKind.STANDARD,
    status: AgentStatus = AgentStatus.PUBLISHED,
    limits: AgentLimits | None = None,
    allowed_tools: frozenset[str] = frozenset(),
    system_prompt: str = "You are a test agent.",
    tenant_id: str = TENANT,
    model_hint: str | None = None,
) -> AgentSpec:
    """Builds an agent spec."""
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
        system_prompt=system_prompt,
        model_hint=model_hint,
    )


def _context(
    *,
    tenant_id: str = TENANT,
    deadline_epoch_ms: int | None = None,
) -> ExecutionContext:
    """Builds an execution context carrying authoritative identity."""
    return ExecutionContext(
        tenant_id=tenant_id,
        organization_id=ORGANIZATION,
        execution_id="exec-1",
        agent_id="agent-1",
        correlation_id="corr-1",
        request_id="req-1",
        caller=ServiceIdentity(subject="rust-bridge", issuer="rust"),
        protocol_version=CURRENT_PROTOCOL_VERSION,
        deadline_epoch_ms=deadline_epoch_ms,
    )


# --- the happy path ----------------------------------------------------------


async def test_agent_completes_with_a_text_answer() -> None:
    harness = _harness([_text(PRIMARY, "42")])

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("what is the answer?"),)),
        _context(),
    )

    assert outcome.succeeded is True
    assert outcome.termination is Termination.COMPLETED
    assert outcome.content == "42"
    assert outcome.steps_taken == 1
    assert outcome.error is None


async def test_system_prompt_is_prepended_exactly_once() -> None:
    harness = _harness([_text(PRIMARY, "hi"), _text(PRIMARY, "again")])

    await harness.executor.run(
        AgentState(messages=(Message.user("hello"),)),
        _context(),
    )

    sent = harness.model.requests[0]
    assert sent.messages[0].role == "system"
    assert sent.messages[0].content == "You are a test agent."
    assert [message.role for message in sent.messages].count("system") == 1


async def test_existing_system_message_is_not_duplicated() -> None:
    harness = _harness([_text(PRIMARY, "hi")])
    state = AgentState(
        messages=(Message.system("already here"), Message.user("hello")),
    )

    await harness.executor.run(state, _context())

    sent = harness.model.requests[0]
    assert [message.role for message in sent.messages].count("system") == 1


async def test_transcript_is_returned_for_the_audit_trail() -> None:
    harness = _harness([_text(PRIMARY, "the answer")])

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("question"),)),
        _context(),
    )

    roles = [getattr(message, "role", None) for message in outcome.messages]
    assert roles == ["system", "user", "assistant"]


async def test_routing_selects_an_allowed_model() -> None:
    harness = _harness([_text(PRIMARY, "ok")])

    await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert harness.model.requests[0].model == PRIMARY


async def test_model_hint_is_a_preference_not_a_requirement() -> None:
    # A hint reorders the chain; it does not bypass the allowlist. Pointing at
    # a model the policy forbids must not select it — the hint only ranks the
    # candidates the policy already admitted.
    harness = _harness([_text(PRIMARY, "ok")], spec=_spec(model_hint="openai/forbidden"))

    await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    chosen = harness.model.requests[0].model
    assert chosen in (PRIMARY, SECONDARY)
    assert chosen.key != "openai/forbidden"


# --- tools -------------------------------------------------------------------


async def test_tool_call_is_executed_and_fed_back() -> None:
    handler = _EchoHandler(output={"echoed": "value"})
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "ping"}), _text(PRIMARY, "done")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
        handlers={"echo": handler},
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("call the tool"),)),
        _context(),
    )

    assert outcome.succeeded is True
    assert outcome.content == "done"
    assert outcome.tool_calls_made == 1
    assert len(handler.seen) == 1


async def test_tool_invocation_carries_the_authoritative_tenant() -> None:
    # The tenant on the invocation comes from the execution context, not from
    # anything the model or the tool supplied.
    handler = _EchoHandler()
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "x"}), _text(PRIMARY, "done")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
        handlers={"echo": handler},
    )

    await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    assert handler.seen[0].tenant_id == TENANT
    assert handler.seen[0].execution_id == "exec-1"


async def test_only_allowlisted_tools_are_advertised() -> None:
    harness = _harness(
        [_text(PRIMARY, "no tools for you")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"), _tool("secret_reader")),
    )

    await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    offered = [tool.name for tool in harness.model.requests[0].tools]
    assert offered == ["echo"]


async def test_a_standard_agent_is_denied_tools_even_if_it_asks() -> None:
    # The model requesting a tool cannot widen what the agent's kind permits.
    # Granting it would let a prompt override the declared configuration.
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "x"})],
        spec=_spec(kind=AgentKind.STANDARD, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    assert outcome.termination is Termination.COMPLETED
    assert outcome.tool_calls_made == 0
    assert harness.permissions.checked == []


async def test_tool_belonging_to_another_tenant_is_invisible() -> None:
    # Registered under tenant-b, so this tenant's agent must not be offered it
    # or able to reach it.
    harness = _harness(
        [_text(PRIMARY, "nothing to call")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo", tenant_id=OTHER_TENANT),),
    )

    await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert harness.model.requests[0].tools == ()


# --- permission denials ------------------------------------------------------


async def test_denied_tool_is_reported_as_a_denial_not_as_data() -> None:
    # The model must learn that the call was refused. Returning a plausible
    # empty result instead would let the model present a fabrication as fact.
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "x"}), _text(PRIMARY, "understood")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
        permissions=_RecordingBridge(default=PermissionDecision.DENY),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    assert outcome.succeeded is True
    assert outcome.tool_calls_made == 1
    transcript = "\n".join(str(getattr(m, "content", "")) for m in outcome.messages)
    assert "deny" in transcript.lower()


async def test_approval_required_is_not_an_allow() -> None:
    # REQUIRE_APPROVAL must wait for a human, not run optimistically.
    handler = _EchoHandler()
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "x"}), _text(PRIMARY, "ok")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
        handlers={"echo": handler},
        permissions=_RecordingBridge(default=PermissionDecision.REQUIRE_APPROVAL),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    assert handler.seen == []
    assert outcome.succeeded is True


async def test_a_denied_tool_does_not_stop_the_run() -> None:
    # One refusal must not abort the whole execution; the model is told and
    # can choose another path.
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "x"}), _text(PRIMARY, "another way")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
        permissions=_RecordingBridge(default=PermissionDecision.DENY),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    assert outcome.content == "another way"


# --- bounds terminate the loop with a named reason ---------------------------


async def test_step_limit_stops_a_looping_agent() -> None:
    endless = [_tool_call(PRIMARY, "echo", {"value": "again"}) for _ in range(20)]
    harness = _harness(
        endless,
        spec=_spec(
            kind=AgentKind.TOOL_USER,
            allowed_tools=frozenset({"echo"}),
            limits=AgentLimits(max_steps=3),
        ),
        tools=(_tool("echo"),),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("loop"),)),
        _context(),
    )

    assert outcome.termination is Termination.STEP_LIMIT
    assert outcome.succeeded is False
    assert outcome.steps_taken == 3


async def test_tool_call_limit_stops_before_the_next_batch() -> None:
    harness = _harness(
        [
            _tool_call(PRIMARY, "echo", {"value": "1"}),
            _tool_call(PRIMARY, "echo", {"value": "2"}),
            _tool_call(PRIMARY, "echo", {"value": "3"}),
        ],
        spec=_spec(
            kind=AgentKind.TOOL_USER,
            allowed_tools=frozenset({"echo"}),
            limits=AgentLimits(max_tool_calls=2),
        ),
        tools=(_tool("echo"),),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    assert outcome.termination is Termination.TOOL_CALL_LIMIT
    assert outcome.tool_calls_made == 2


async def test_expired_deadline_stops_before_calling_the_model() -> None:
    harness = _harness([_text(PRIMARY, "too late")])

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(deadline_epoch_ms=1),
    )

    assert outcome.termination is Termination.DEADLINE
    assert harness.model.requests == []


async def test_cancellation_before_the_first_step() -> None:
    harness = _harness([_text(PRIMARY, "never")])
    cancel_event = asyncio.Event()
    cancel_event.set()

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
        cancel_event=cancel_event,
    )

    assert outcome.termination is Termination.CANCELLED
    assert harness.model.requests == []


async def test_cancellation_between_steps() -> None:
    cancel_event = asyncio.Event()

    # The first response requests a tool so the loop reaches a second
    # iteration; a text answer would finish the run, and a finished run has no
    # cancellation to observe.
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "x"}), _text(PRIMARY, "never")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
    )
    harness.model.on_request = lambda _request: cancel_event.set()

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
        cancel_event=cancel_event,
    )

    assert outcome.termination is Termination.CANCELLED
    assert outcome.succeeded is False
    assert outcome.steps_taken == 1


# --- cost --------------------------------------------------------------------


async def test_usage_is_recorded_for_every_model_call() -> None:
    ledger = CostLedger(
        PricingTable(rates={PRIMARY.key: {"input": 1.0, "output": 2.0}}),
        emit_unpriced_warning=False,
    )
    harness = _harness(
        [
            _tool_call(PRIMARY, "echo", {"value": "x"}),
            _text(PRIMARY, "done"),
        ],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
        ledger=ledger,
    )

    await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    totals = ledger.totals()
    assert totals.input_tokens == 20
    assert totals.output_tokens == 10
    assert totals.priced_call_count == 2


async def test_cost_ceiling_stops_the_agent() -> None:
    ledger = CostLedger(
        PricingTable(rates={PRIMARY.key: {"input": 1_000_000.0, "output": 1_000_000.0}}),
        emit_unpriced_warning=False,
    )
    harness = _harness(
        [_tool_call(PRIMARY, "echo", {"value": "x"}), _text(PRIMARY, "more")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
        ledger=ledger,
        config=AgentRunConfig(cost_ceiling=1.0),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    # The ceiling is checked at the top of each step, so the first call is
    # always paid for; what the ceiling prevents is the *next* one.
    assert outcome.termination is Termination.BUDGET_EXHAUSTED
    assert outcome.succeeded is False
    assert outcome.steps_taken == 1


# --- failure paths -----------------------------------------------------------


async def test_unrunnable_agent_fails_immediately() -> None:
    harness = _harness([_text(PRIMARY, "never")], spec=_spec(status=AgentStatus.DRAFT))

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert outcome.termination is Termination.FAILED
    assert isinstance(outcome.error, InvalidRequestError)
    assert harness.model.requests == []


async def test_non_retryable_model_failure_fails_the_run() -> None:
    harness = _harness(
        [_text(PRIMARY, "never")],
        failures={PRIMARY.key: AuthenticationError("bad api key")},
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert outcome.termination is Termination.FAILED
    assert isinstance(outcome.error, AuthenticationError)


async def test_retryable_failure_falls_over_to_the_next_allowed_model() -> None:
    # The fallback model is one the policy already allowed; failover is not a
    # way around the allowlist.
    harness = _harness(
        [_text(SECONDARY, "served by the fallback")],
        failures={PRIMARY.key: ModelRateLimitedError("rate limited")},
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert outcome.succeeded is True
    assert outcome.content == "served by the fallback"
    assert harness.model.requests[-1].model == SECONDARY


async def test_no_model_satisfying_the_policy_fails_the_run() -> None:
    harness = _harness(
        [_text(PRIMARY, "never")],
        allowed_models=frozenset(),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert outcome.termination is Termination.FAILED
    assert harness.model.requests == []


async def test_malformed_tool_arguments_are_reported_to_the_model() -> None:
    # A model emitting bad JSON is routine, not fatal. It is told its
    # arguments were rejected so it can correct itself.
    bad_json = ModelResponse(
        model=PRIMARY,
        text="",
        finish_reason=FinishReason.TOOL_CALLS,
        tool_calls=(ToolCall(id="call-1", name="echo", arguments_json="{oops"),),
        usage=Usage(input_tokens=1, output_tokens=1),
    )
    harness = _harness(
        [bad_json, _text(PRIMARY, "my mistake")],
        spec=_spec(kind=AgentKind.TOOL_USER, allowed_tools=frozenset({"echo"})),
        tools=(_tool("echo"),),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("go"),)),
        _context(),
    )

    assert outcome.succeeded is True
    assert outcome.content == "my mistake"
    assert harness.permissions.checked == []


async def test_failure_is_reported_not_raised() -> None:
    # A caller decides whether one agent's failure aborts an execution, so the
    # executor reports ordinary failures instead of throwing.
    harness = _harness(
        [_text(PRIMARY, "never")],
        failures={PRIMARY.key: AuthenticationError("nope")},
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert outcome.succeeded is False
    assert outcome.error is not None


# --- structured output -------------------------------------------------------


async def test_structured_output_passes_the_schema_to_the_model() -> None:
    schema = {"type": "object", "properties": {"answer": {"type": "string"}}}
    harness = _harness(
        [_text(PRIMARY, '{"answer": "42"}')],
        config=AgentRunConfig(response_json_schema=schema),
    )

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert harness.model.requests[0].response_json_schema == schema
    assert outcome.content == '{"answer": "42"}'


# --- duration is measured ----------------------------------------------------


async def test_outcome_records_a_duration() -> None:
    harness = _harness([_text(PRIMARY, "ok")])

    started = time.monotonic()
    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert outcome.duration_ms >= 0
    assert outcome.duration_ms <= int((time.monotonic() - started) * 1000) + 100


async def test_agent_id_on_the_outcome() -> None:
    harness = _harness([_text(PRIMARY, "ok")], spec=_spec(agent_id="agent-77"))

    outcome = await harness.executor.run(
        AgentState(messages=(Message.user("hi"),)),
        _context(),
    )

    assert outcome.agent_id == "agent-77"


# --- concurrency -------------------------------------------------------------


async def test_two_executions_do_not_share_transcript_state() -> None:
    # The executor copies the incoming messages, so running the same agent for
    # two executions must not leak one transcript into the other.
    harness = _harness([_text(PRIMARY, "first"), _text(PRIMARY, "second")])
    state = AgentState(messages=(Message.user("shared"),))

    await harness.executor.run(state, _context())
    await harness.executor.run(state, _context())

    assert len(harness.model.requests) == 2
    assert len(harness.model.requests[1].messages) == len(harness.model.requests[0].messages)


async def test_concurrent_executions_are_independent() -> None:
    first = _harness([_text(PRIMARY, "one")])
    second = _harness([_text(PRIMARY, "two")])

    outcomes = await asyncio.gather(
        first.executor.run(AgentState(messages=(Message.user("a"),)), _context()),
        second.executor.run(AgentState(messages=(Message.user("b"),)), _context()),
    )

    assert outcomes[0].content == "one"
    assert outcomes[1].content == "two"


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
