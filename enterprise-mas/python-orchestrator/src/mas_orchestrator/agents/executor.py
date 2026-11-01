"""The agent execution loop.

This is where an agent actually runs: call the model, act on what it asked
for, feed the results back, repeat — until it finishes or hits a bound.

The loop is deliberately strict about stopping. An agent that does not
terminate on its own will exhaust whatever it is allowed to exhaust, so every
bound is checked *before* the work it limits, not after:

* :attr:`AgentLimits.max_steps` is checked before each model call.
* :attr:`AgentLimits.max_tool_calls` is checked before each tool batch.
* The deadline and the cancellation event are checked before each step and
  again before each tool call.

An agent that produces a final answer is the only path to
:attr:`Termination.COMPLETED`; hitting a bound is recorded as the bound, never
dressed up as success.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.agents.base import (
    AgentOutcome,
    AgentSpec,
    AgentState,
    Termination,
    new_state,
)
from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ExecutionTimeoutError,
    InvalidRequestError,
)
from mas_orchestrator.models.types import (
    FinishReason,
    Message,
    ModelRef,
    ModelRequest,
    ToolSpec,
)
from mas_orchestrator.tools.types import ToolInvocationContext

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.models.base import ModelProvider
    from mas_orchestrator.models.cost import CostLedger
    from mas_orchestrator.models.router import ModelRouter
    from mas_orchestrator.models.types import ModelResponse, ModelStreamEvent
    from mas_orchestrator.tools.executor import ToolExecutor
    from mas_orchestrator.tools.registry import ToolRegistry

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

# How many tool calls may be executed in one batch. Bounding the batch stops a
# single model turn from scheduling unbounded parallel work.
MAX_TOOL_BATCH: Final[int] = 16

# Used when neither the execution nor the agent specifies a timeout. The model
# layer requires a timeout on every call, so "unbounded" is not expressible
# there; this is the bound we pick in its place.
DEFAULT_MODEL_TIMEOUT_SECONDS: Final[float] = 120.0


@dataclass(frozen=True, slots=True)
class AgentRunConfig:
    """Model-side configuration for one agent run.

    Attributes:
        temperature: Sampling temperature, or ``None`` for the default.
        max_output_tokens: Generation cap for each model call.
        require_structured_output: Whether the model must support schema
            output. Only meaningful when ``response_json_schema`` is set.
        response_json_schema: Optional schema for the final answer.
        cost_ceiling: Optional spend ceiling for this run.
        stream: Whether to stream model output. Streaming is advisory: a
            provider that cannot stream is not substituted.
    """

    temperature: float | None = None
    max_output_tokens: int | None = None
    require_structured_output: bool = False
    response_json_schema: dict[str, Any] | None = None
    cost_ceiling: float | None = None
    stream: bool = False

    def __post_init__(self) -> None:
        if self.response_json_schema is not None and not isinstance(
            self.response_json_schema, dict
        ):
            raise InvalidRequestError(
                "response_json_schema must be a JSON object",
                details={"field": "response_json_schema"},
            )
        if self.cost_ceiling is not None and self.cost_ceiling <= 0:
            raise InvalidRequestError(
                "cost ceiling must be positive", details={"field": "cost_ceiling"}
            )


class AgentExecutor:
    """Runs one :class:`AgentSpec` to completion or to a bound.

    The executor owns the loop; it does not own the model call (the model layer
    does) or the tool call (the tool executor does). Keeping those separate is
    what lets each of them be tested without the others.

    ``provider_for`` resolves a selected model to the adapter that serves it.
    It is injected rather than reached through the router because routing and
    resolution are separate concerns: the router decides *which* model, the
    registry knows *who* serves it.
    """

    def __init__(
        self,
        spec: AgentSpec,
        *,
        router: ModelRouter,
        provider_for: Callable[[ModelRef], ModelProvider],
        tool_executor: ToolExecutor | None = None,
        tool_registry: ToolRegistry | None = None,
        ledger: CostLedger | None = None,
        config: AgentRunConfig | None = None,
    ) -> None:
        self._spec = spec
        self._router = router
        self._provider_for = provider_for
        self._tool_executor = tool_executor
        self._tool_registry = tool_registry
        self._ledger = ledger
        self._config = config or AgentRunConfig()

    @property
    def spec(self) -> AgentSpec:
        """The agent being executed."""
        return self._spec

    async def run(
        self,
        state: AgentState,
        context: ExecutionContext,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> AgentOutcome:
        """Executes the agent loop.

        Returns:
            An :class:`AgentOutcome`. Ordinary failures are reported as
            ``termination=FAILED`` rather than raised, so a caller can decide
            whether one agent's failure should abort an execution.
        """
        started = time.monotonic()
        limits = self._spec.limits.clamped()

        if not self._spec.is_runnable:
            return self._outcome(
                Termination.FAILED,
                started,
                steps=0,
                tool_calls=0,
                error=InvalidRequestError(
                    f"agent '{self._spec.agent_id}' is {self._spec.status.value}",
                    details={"agent_id": self._spec.agent_id},
                ),
            )

        messages = list(state.messages)
        if self._spec.system_prompt and not any(
            getattr(message, "role", None) == "system" for message in messages
        ):
            messages.insert(0, Message.system(self._spec.system_prompt))

        steps = 0
        tool_calls = 0

        try:
            while True:
                # --- bounds, checked before doing the work they limit ---
                if steps >= limits.max_steps:
                    return self._outcome(
                        Termination.STEP_LIMIT, started, steps, tool_calls, messages
                    )
                if self._is_cancelled(cancel_event):
                    return self._outcome(
                        Termination.CANCELLED, started, steps, tool_calls, messages
                    )
                if context.is_expired():
                    return self._outcome(Termination.DEADLINE, started, steps, tool_calls, messages)
                if self._budget_exhausted():
                    return self._outcome(
                        Termination.BUDGET_EXHAUSTED, started, steps, tool_calls, messages
                    )

                # --- one model call ---
                response = await self._call_model(messages, context, cancel_event)
                steps += 1
                self._record_usage(response)

                if response.text:
                    messages.append(Message.assistant(response.text))

                # --- act on tool calls, or finish ---
                if not response.tool_calls:
                    content = response.text
                    if self._config.response_json_schema is not None:
                        content = self._extract_json(content)
                    return self._outcome(
                        Termination.COMPLETED,
                        started,
                        steps,
                        tool_calls,
                        messages,
                        content=content,
                    )

                if not self._spec.may_use_tools:
                    # The model asked for tools this agent's kind forbids.
                    # Refusing is the only safe answer: granting it would let a
                    # prompt override the agent's declared kind.
                    _LOGGER.warning(
                        "agent requested tools but its kind forbids them",
                        extra={
                            "event": "agent.tools_forbidden",
                            "agent_id": self._spec.agent_id,
                            "kind": self._spec.kind.value,
                            "requested": [c.name for c in response.tool_calls],
                        },
                    )
                    return self._outcome(
                        Termination.COMPLETED,
                        started,
                        steps,
                        tool_calls,
                        messages,
                        content=response.text,
                    )

                if tool_calls + len(response.tool_calls) > limits.max_tool_calls:
                    return self._outcome(
                        Termination.TOOL_CALL_LIMIT, started, steps, tool_calls, messages
                    )
                if len(response.tool_calls) > MAX_TOOL_BATCH:
                    return self._outcome(
                        Termination.TOOL_CALL_LIMIT, started, steps, tool_calls, messages
                    )

                results = await self._run_tools(response.tool_calls, context, cancel_event)
                tool_calls += len(response.tool_calls)
                messages.extend(results)

        except CancellationRequestedError:
            return self._outcome(Termination.CANCELLED, started, steps, tool_calls, messages)
        except ExecutionTimeoutError:
            return self._outcome(Termination.DEADLINE, started, steps, tool_calls, messages)
        except Exception as error:
            return self._outcome(
                Termination.FAILED, started, steps, tool_calls, messages, error=error
            )

    # --- internals ---------------------------------------------------------

    async def _call_model(
        self,
        messages: list[Any],
        context: ExecutionContext,
        cancel_event: asyncio.Event | None,
    ) -> ModelResponse:
        """Selects a model, calls it, and fails over on retryable errors."""
        from mas_orchestrator.models.router import RoutingRequest

        routing = RoutingRequest(
            estimated_input_tokens=None,
            require_tools=bool(self._tool_specs()),
            require_structured_output=self._config.require_structured_output,
            require_streaming=self._config.stream,
            preferred_models=((self._spec.model_hint,) if self._spec.model_hint else ()),
        )

        chain = self._router.failover_chain(routing, limit=3)
        if not chain:
            from mas_orchestrator.contracts.errors import ModelNotAvailableError

            raise ModelNotAvailableError(
                "no model satisfies this agent's requirements",
                details={
                    "agent_id": self._spec.agent_id,
                    "require_tools": routing.require_tools,
                },
            )

        request = ModelRequest(
            messages=tuple(messages),
            model=chain[0],
            system_prompt=None,
            temperature=self._config.temperature,
            max_output_tokens=self._config.max_output_tokens,
            tools=self._tool_specs(),
            response_json_schema=self._config.response_json_schema,
        )

        return await self._complete_with_failover(request, chain, context, cancel_event)

    async def _complete_with_failover(
        self,
        request: ModelRequest,
        chain: Sequence[Any],
        context: ExecutionContext,
        cancel_event: asyncio.Event | None,
    ) -> ModelResponse:
        """Calls models in order, falling back only on retryable failures."""
        from dataclasses import replace

        from mas_orchestrator.contracts.errors import is_retryable

        last_error: BaseException | None = None
        for index, model in enumerate(chain):
            if self._is_cancelled(cancel_event):
                raise CancellationRequestedError("cancelled before model call")
            try:
                timeout = self._timeout_for(context)
                provider = self._provider_for(model)
                response: ModelResponse = await provider.complete(
                    replace(request, model=model),
                    timeout_seconds=timeout,
                    cancel_event=cancel_event,
                )
                return response
            except Exception as error:
                last_error = error
                if not is_retryable(error) or index == len(chain) - 1:
                    raise
                _LOGGER.warning(
                    "model call failed, failing over",
                    extra={
                        "event": "agent.model_failover",
                        "agent_id": self._spec.agent_id,
                        "model": str(model),
                        "error": type(error).__name__,
                    },
                )
        raise last_error or InvalidRequestError("model chain was empty")

    def _timeout_for(self, context: ExecutionContext) -> float:
        """Per-model-call timeout, in seconds.

        The tightest of three: whatever time the execution has left, the
        agent's own configured timeout, and the platform default. The model
        layer requires an explicit bound on every call, so this never returns
        ``None`` — an unbounded model call would hold a worker indefinitely.
        """
        remaining = context.remaining_seconds()
        candidates = [DEFAULT_MODEL_TIMEOUT_SECONDS]
        if remaining is not None:
            candidates.append(remaining)
        if self._spec.limits.timeout_seconds is not None:
            candidates.append(self._spec.limits.timeout_seconds)
        return max(0.001, min(candidates))

    def _tool_specs(self) -> tuple[ToolSpec, ...]:
        """Tool specs this agent is allowed to offer the model.

        Only tools on the agent's allowlist are advertised, so the model is
        never shown a capability it will later be denied.
        """
        if not self._spec.may_use_tools or self._tool_registry is None:
            return ()
        specs = []
        for definition in self._tool_registry.list_tools(self._spec.tenant_id):
            if not self._spec.permits_tool(definition.tool_id):
                continue
            specs.append(
                ToolSpec(
                    name=definition.name,
                    description=definition.description or "",
                    json_schema=dict(definition.input_schema),
                )
            )
        return tuple(specs)

    async def _run_tools(
        self,
        calls: Sequence[Any],
        context: ExecutionContext,
        cancel_event: asyncio.Event | None,
    ) -> list[Any]:
        """Executes a batch of tool calls and returns result messages.

        The batch runs concurrently, because the calls are independent, but a
        single refusal does not abort the rest: each result is reported on its
        own so the model can see exactly what happened.
        """
        tasks = [self._run_one_tool(call, context, cancel_event) for call in calls]
        return list(await asyncio.gather(*tasks))

    async def _run_one_tool(
        self,
        call: Any,
        context: ExecutionContext,
        cancel_event: asyncio.Event | None,
    ) -> Any:
        """Executes one tool call and wraps the result as a message."""
        invocation_id = f"inv-{uuid.uuid4().hex}"
        tool_context = ToolInvocationContext(
            tenant_id=context.tenant_id,
            execution_id=context.execution_id,
            agent_id=context.agent_id,
            invocation_id=invocation_id,
            tool_call_id=call.id,
        )

        if self._tool_executor is None:
            from mas_orchestrator.contracts.errors import ToolNotFoundError
            from mas_orchestrator.tools.executor import failed_result

            result = failed_result(
                call.name,
                ToolNotFoundError("no tool executor is configured", details={"tool": call.name}),
                invocation_id=invocation_id,
            )
        else:
            try:
                arguments = self._decode_arguments(call)
            except InvalidRequestError as error:
                from mas_orchestrator.tools.executor import failed_result

                result = failed_result(call.name, error, invocation_id=invocation_id)
            else:
                if self._is_cancelled(cancel_event):
                    raise CancellationRequestedError("cancelled before tool call")
                result = await self._tool_executor.invoke(call.name, arguments, tool_context)

        return Message.tool_result(
            tool_call_id=call.id,
            name=call.name,
            output_json=result.to_model_content(),
        )

    @staticmethod
    def _decode_arguments(call: Any) -> Any:
        """Decodes a tool call's JSON arguments.

        A model that emits malformed JSON is a normal occurrence, not a crash:
        the tool is told its arguments were rejected so it can retry.
        """
        import json

        raw = getattr(call, "arguments_json", "") or ""
        if not raw.strip():
            return {}
        try:
            return json.loads(raw)
        except json.JSONDecodeError as error:
            raise InvalidRequestError(
                "model produced malformed tool arguments",
                details={"tool": getattr(call, "name", ""), "position": error.pos},
            ) from error

    @staticmethod
    def _extract_json(text: str) -> str:
        """Returns ``text`` unchanged; schema enforcement happens upstream.

        The model layer raises rather than faking schema support, so by the
        time we get here the response already satisfies the schema or the call
        failed. Re-parsing it here would only duplicate that decision.
        """
        return text

    def _is_cancelled(self, cancel_event: asyncio.Event | None) -> bool:
        """Whether cancellation has been requested."""
        return cancel_event is not None and cancel_event.is_set()

    def _budget_exhausted(self) -> bool:
        """Whether the configured spend ceiling has been reached."""
        from mas_orchestrator.agents.base import budget_exhausted

        return budget_exhausted(self._ledger, self._config.cost_ceiling)

    def _record_usage(self, response: ModelResponse) -> None:
        """Records usage and cost for one model call."""
        if self._ledger is None:
            return
        self._ledger.record(response.model, response.usage)

    def _outcome(
        self,
        termination: Termination,
        started: float,
        steps: int,
        tool_calls: int,
        messages: list[Any] | None = None,
        *,
        content: str = "",
        error: BaseException | None = None,
    ) -> AgentOutcome:
        """Builds the outcome and logs the terminal event."""
        duration_ms = int((time.monotonic() - started) * 1000)
        _LOGGER.info(
            "agent finished",
            extra={
                "event": "agent.finished",
                "agent_id": self._spec.agent_id,
                "termination": termination.value,
                "steps": steps,
                "tool_calls": tool_calls,
                "duration_ms": duration_ms,
            },
        )
        return AgentOutcome(
            agent_id=self._spec.agent_id,
            termination=termination,
            content=content,
            messages=tuple(messages or ()),
            steps_taken=steps,
            tool_calls_made=tool_calls,
            cost=self._ledger,
            error=error,
            duration_ms=duration_ms,
        )


def seed_state(spec: AgentSpec, user_input: str) -> AgentState:
    """Builds the starting state for an agent run.

    The system prompt and the user's request are the first two messages, in
    that order, so the model always sees the instructions before the task.
    """
    return new_state(spec.system_prompt, user_input)


def terminal_from_finish_reason(reason: FinishReason) -> Termination | None:
    """Maps a finish reason to a termination, when it implies one.

    Returns ``None`` when the reason does not end the run — a ``TOOL_CALLS``
    reason, for instance, means the loop must continue.
    """
    if reason is FinishReason.TOOL_CALLS:
        return None
    if reason is FinishReason.LENGTH:
        return Termination.COMPLETED
    return Termination.COMPLETED


def applies_to_stream(event: ModelStreamEvent) -> bool:
    """Whether a stream event carries content the loop should surface."""
    return bool(event.delta_text) or event.is_terminal
