"""Typed model layer value objects.

Every provider adapter speaks this module and nothing else. A raw provider
response is parsed into these types at the adapter boundary, so no provider
leaks its own shape into the agent runtime, the router, or the cost calculator.

Two rules shape the design:

**No fabricated data.** When a provider does not report token usage, ``usage``
is ``None`` — not zero. Zero is a *measurement* ("this call consumed nothing"),
while ``None`` is an *absence of measurement*, and conflating them would make
every downstream budget and cost number silently wrong. The same rule applies
to cost: :class:`CostEstimate` can only be produced when pricing is
configured, and construction refuses to guess.

**Immutability.** Requests and responses are frozen dataclasses. A model call
may be retried, replayed in an evaluation, or re-serialised into an audit
event; a mutable response object would allow any of those paths to alter what
was actually returned.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = [
    "MAX_OUTPUT_TOKENS_LIMIT",
    "CostEstimate",
    "FinishReason",
    "Message",
    "ModelCapabilities",
    "ModelRef",
    "ModelRequest",
    "ModelResponse",
    "ModelStreamEvent",
    "Role",
    "ToolCall",
    "ToolCallResult",
    "ToolSpec",
    "Usage",
]

#: Hard ceiling applied when a caller omits ``max_output_tokens``. Prevents an
#: unbounded generation from consuming the whole token budget in one call.
MAX_OUTPUT_TOKENS_LIMIT: int = 8_192


class Role(StrEnum):
    """Conversation roles shared by every provider adapter."""

    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


class FinishReason(StrEnum):
    """Normalised termination reason, independent of provider vocabulary.

    Adapters map provider-specific values onto this enum; anything
    unrecognised becomes :attr:`OTHER` rather than being invented as a new
    reason, so the runtime's control flow can stay exhaustive.
    """

    STOP = "stop"
    LENGTH = "length"
    TOOL_CALLS = "tool_calls"
    CONTENT_FILTER = "content_filter"
    ERROR = "error"
    OTHER = "other"


@dataclass(frozen=True, slots=True)
class ToolSpec:
    """A tool the model may request, advertised to the provider.

    Attributes:
        name: Tool identifier, unique within one request.
        description: Human/LLM-readable description of what the tool does.
        json_schema: JSON Schema for the tool's arguments.
    """

    name: str
    description: str
    json_schema: Mapping[str, Any]

    def __post_init__(self) -> None:
        """Validates the spec.

        Raises:
            InvalidRequestError: If the name is empty or the schema is not a
                JSON object schema. A tool the model cannot name is unusable,
                and a non-object schema cannot describe arguments.
        """
        if not self.name.strip():
            raise InvalidRequestError("tool name must not be empty", details={"field": "name"})
        schema_type = self.json_schema.get("type")
        if schema_type is not None and schema_type != "object":
            raise InvalidRequestError(
                "tool argument schema must describe an object",
                details={"tool": self.name, "type": str(schema_type)},
            )


@dataclass(frozen=True, slots=True)
class ToolCall:
    """A tool invocation requested by the model.

    Attributes:
        id: Provider-assigned identifier used to correlate the result.
        name: Tool to invoke.
        arguments_json: Raw JSON string of the arguments, exactly as the
            provider produced it. Kept as a string so a malformed payload is
            surfaced at validation time rather than silently re-encoded.
    """

    id: str
    name: str
    arguments_json: str

    def __post_init__(self) -> None:
        """Validates the call.

        A *complete* call has a name. A **streamed fragment** may carry only an
        id plus a slice of the arguments, because providers emit the name once
        in the opening fragment and then stream the arguments. Requiring the
        name on every fragment would make streamed tool calls impossible, so an
        id alone is accepted; only a call with neither is meaningless.

        Raises:
            InvalidRequestError: If both the id and the name are empty.
        """
        if not self.name.strip() and not self.id.strip():
            raise InvalidRequestError(
                "tool call must have an id or a name", details={"field": "id_or_name"}
            )


@dataclass(frozen=True, slots=True)
class ToolCallResult:
    """The outcome of executing a tool requested by the model.

    Attributes:
        tool_call_id: Correlates with :attr:`ToolCall.id`.
        name: Tool that was invoked.
        output_json: JSON-encoded result, or an error description when the
            tool failed. Treated as **untrusted input** by the runtime: tool
            output can contain injected instructions.
        is_error: Whether the tool failed. A failure is still a result the
            model needs to observe, so it travels the same path.
    """

    tool_call_id: str
    name: str
    output_json: str
    is_error: bool = False


@dataclass(frozen=True, slots=True)
class Message:
    """One message in a conversation.

    Attributes:
        role: Who produced the message.
        content: Text content. May be empty when the message carries tool
            calls instead.
        name: Optional author name; used by some providers for tool results.
        tool_calls: Tool calls requested by an assistant message.
        tool_call_id: For :attr:`Role.TOOL` messages, the call being answered.
    """

    role: Role
    content: str = ""
    name: str | None = None
    tool_calls: tuple[ToolCall, ...] = ()
    tool_call_id: str | None = None

    def __post_init__(self) -> None:
        """Validates internal consistency.

        Raises:
            InvalidRequestError: If a tool result message has no
                ``tool_call_id`` (the provider cannot correlate it), or if a
                message is entirely empty, which would send a no-op to the
                model and waste tokens.
        """
        if self.role is Role.TOOL and not self.tool_call_id:
            raise InvalidRequestError(
                "a tool result message must carry a tool_call_id",
                details={"role": str(self.role)},
            )
        if not self.content.strip() and not self.tool_calls:
            raise InvalidRequestError(
                "message must have content or tool calls",
                details={"role": str(self.role)},
            )

    @classmethod
    def system(cls, content: str) -> Self:
        """Builds a system message."""
        return cls(role=Role.SYSTEM, content=content)

    @classmethod
    def user(cls, content: str) -> Self:
        """Builds a user message."""
        return cls(role=Role.USER, content=content)

    @classmethod
    def assistant(cls, content: str = "", *, tool_calls: Sequence[ToolCall] = ()) -> Self:
        """Builds an assistant message, optionally carrying tool calls."""
        return cls(role=Role.ASSISTANT, content=content, tool_calls=tuple(tool_calls))

    @classmethod
    def tool_result(cls, tool_call_id: str, name: str, output_json: str) -> Self:
        """Builds a tool result message."""
        return cls(
            role=Role.TOOL,
            content=output_json,
            name=name,
            tool_call_id=tool_call_id,
        )


@dataclass(frozen=True, slots=True)
class Usage:
    """Token usage as *reported by the provider*.

    This class is only constructed when a provider actually reports usage.
    There is deliberately no "zero" convenience constructor: a missing
    measurement must remain ``None`` downstream rather than being coerced into
    a fabricated zero.

    Attributes:
        input_tokens: Prompt tokens consumed.
        output_tokens: Completion tokens generated.
        total_tokens: Provider-reported total, when it supplies one.
        cached_input_tokens: Portion of ``input_tokens`` served from a prompt
            cache, when reported. ``None`` means "not reported", which is
            different from "not cached".
    """

    input_tokens: int
    output_tokens: int
    total_tokens: int | None = None
    cached_input_tokens: int | None = None

    def __post_init__(self) -> None:
        """Validates that reported counts are sane.

        Raises:
            InvalidRequestError: If any count is negative — a negative token
                count from a provider indicates a parsing error, and letting
                it through would corrupt every budget derived from it.
        """
        for name in ("input_tokens", "output_tokens"):
            if getattr(self, name) < 0:
                raise InvalidRequestError(
                    "token counts must not be negative",
                    details={"field": name, "value": getattr(self, name)},
                )
        if self.total_tokens is not None and self.total_tokens < 0:
            raise InvalidRequestError(
                "total_tokens must not be negative",
                details={"value": self.total_tokens},
            )
        if self.cached_input_tokens is not None and self.cached_input_tokens < 0:
            raise InvalidRequestError(
                "cached_input_tokens must not be negative",
                details={"value": self.cached_input_tokens},
            )

    @property
    def total(self) -> int:
        """Total tokens, derived when the provider omits it."""
        return (
            self.total_tokens
            if self.total_tokens is not None
            else (self.input_tokens + self.output_tokens)
        )

    def add(self, other: Usage) -> Usage:
        """Returns the sum of two usage records.

        Cached-input totals are summed only when both reports include them;
        mixing a reported and an unreported cached count would produce a
        total that is neither.
        """
        cached: int | None = None
        if self.cached_input_tokens is not None and other.cached_input_tokens is not None:
            cached = self.cached_input_tokens + other.cached_input_tokens
        return Usage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            total_tokens=self.total + other.total,
            cached_input_tokens=cached,
        )


@dataclass(frozen=True, slots=True)
class CostEstimate:
    """A cost figure computed from **configured pricing data only**.

    There is no path that produces a cost without pricing: :meth:`from_usage`
    raises when the model has no configured price, rather than returning zero
    or a placeholder. Silently reporting a zero cost is worse than reporting
    nothing, because a zero is indistinguishable from a genuinely free call
    once it reaches a budget or a dashboard.

    Attributes:
        currency: ISO-4217 code from the pricing table.
        input_cost: Cost attributable to input tokens.
        output_cost: Cost attributable to output tokens.
        total_cost: Sum of the two.
        is_complete: ``False`` when usage was not reported, meaning the cost
            is a lower bound rather than a final figure.
    """

    currency: str
    input_cost: float
    output_cost: float
    total_cost: float
    is_complete: bool = True

    @classmethod
    def from_usage(
        cls,
        usage: Usage | None,
        pricing: Mapping[str, float],
        *,
        currency: str = "USD",
    ) -> CostEstimate | None:
        """Computes a cost from reported usage and a pricing row.

        Args:
            usage: Reported usage, or ``None`` when unmeasured.
            pricing: Prices per **one million** tokens, keyed by
                ``"input"`` and ``"output"``. This scale matches how vendors
                publish rates and avoids floating-point noise on tiny numbers.
            currency: Currency code for the prices.

        Returns:
            The estimate, or ``None`` when usage was not reported. Never a
            zero-cost placeholder.

        Raises:
            ConfigurationError: If the pricing row is missing a required key.
                Misconfigured pricing must surface at the call site, not as a
                silent zero.
        """
        if usage is None:
            return None

        for key in ("input", "output"):
            if key not in pricing:
                raise ConfigurationError(
                    "pricing is missing a required rate",
                    details={"missing_key": key, "provided": sorted(pricing)},
                )

        input_cost = usage.input_tokens / 1_000_000 * pricing["input"]
        output_cost = usage.output_tokens / 1_000_000 * pricing["output"]
        return cls(
            currency=currency,
            input_cost=input_cost,
            output_cost=output_cost,
            total_cost=input_cost + output_cost,
            is_complete=True,
        )

    def add(self, other: CostEstimate) -> CostEstimate:
        """Sums two estimates.

        Raises:
            ConfigurationError: If the currencies differ. Adding amounts in
                different currencies produces a meaningless number, so this
                refuses rather than converting (conversion rates are not
                configured data here).
        """
        if self.currency != other.currency:
            raise ConfigurationError(
                "cannot add cost estimates in different currencies",
                details={"left": self.currency, "right": other.currency},
            )
        return CostEstimate(
            currency=self.currency,
            input_cost=self.input_cost + other.input_cost,
            output_cost=self.output_cost + other.output_cost,
            total_cost=self.total_cost + other.total_cost,
            is_complete=self.is_complete and other.is_complete,
        )


@dataclass(frozen=True, slots=True)
class ModelRef:
    """A reference to a specific model on a specific provider.

    Attributes:
        provider: Provider identifier (``openai``, ``anthropic``, …).
        model: Provider-native model name (``gpt-4o``, ``claude-3-5-sonnet``).

    The pair is what the allowlist and the routing policy are keyed on, so a
    model name alone is never sufficient to select a call path.
    """

    provider: str
    model: str

    def __post_init__(self) -> None:
        """Validates that both components are present."""
        if not self.provider.strip():
            raise InvalidRequestError(
                "model reference requires a provider", details={"field": "provider"}
            )
        if not self.model.strip():
            raise InvalidRequestError(
                "model reference requires a model", details={"field": "model"}
            )

    def __str__(self) -> str:
        """Renders as ``provider/model``."""
        return f"{self.provider}/{self.model}"

    @property
    def key(self) -> str:
        """Stable string key for registries and allowlists."""
        return str(self)


@dataclass(frozen=True, slots=True)
class ModelCapabilities:
    """What a model supports, used for routing decisions.

    Attributes:
        supports_tools: Whether the model can request tool calls.
        supports_streaming: Whether streaming is available.
        supports_structured_output: Whether JSON-schema-constrained output
            is available.
        supports_system_prompt: Whether a system role is honoured. Some
            models require system content to be folded into the first user
            message; the adapter does that, not the caller.
        max_input_tokens: Context window, when known. ``None`` means unknown,
            which the router treats as "cannot verify fit".
        max_output_tokens: Maximum generation length, when known.
    """

    supports_tools: bool = False
    supports_streaming: bool = False
    supports_structured_output: bool = False
    supports_system_prompt: bool = True
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None


@dataclass(frozen=True, slots=True)
class ModelRequest:
    """A complete, provider-agnostic request to a model.

    Attributes:
        messages: The conversation.
        model: Target model.
        system_prompt: Optional system instruction, kept separate from
            ``messages`` so adapters can place it correctly for providers
            that treat it specially.
        temperature: Sampling temperature; ``None`` means "provider default".
        top_p: Nucleus sampling; ``None`` means "provider default".
        max_output_tokens: Generation cap. Capped at
            :data:`MAX_OUTPUT_TOKENS_LIMIT` when omitted or excessive.
        stop_sequences: Sequences that terminate generation.
        tools: Tools the model may request.
        tool_choice: ``"auto"``, ``"none"``, ``"required"``, or a specific
            tool name.
        response_json_schema: When set, requests schema-constrained output.
        metadata: Caller-supplied non-secret metadata echoed to the provider
            (for example a user id for abuse monitoring). Never contains
            credentials.
    """

    messages: tuple[Message, ...]
    model: ModelRef
    system_prompt: str | None = None
    temperature: float | None = None
    top_p: float | None = None
    max_output_tokens: int | None = None
    stop_sequences: tuple[str, ...] = ()
    tools: tuple[ToolSpec, ...] = ()
    tool_choice: str | None = None
    response_json_schema: Mapping[str, Any] | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validates the request and normalises the output cap.

        Raises:
            InvalidRequestError: If there are no messages, or if a sampling
                parameter is outside its valid range. Rejecting out-of-range
                values here keeps provider 400s from being the only guard.
        """
        if not self.messages:
            raise InvalidRequestError(
                "a model request requires at least one message",
                details={"field": "messages"},
            )

        if self.temperature is not None and not 0.0 <= self.temperature <= 2.0:
            raise InvalidRequestError(
                "temperature must be between 0 and 2",
                details={"temperature": self.temperature},
            )
        if self.top_p is not None and not 0.0 <= self.top_p <= 1.0:
            raise InvalidRequestError(
                "top_p must be between 0 and 1", details={"top_p": self.top_p}
            )
        if self.max_output_tokens is not None and self.max_output_tokens < 1:
            raise InvalidRequestError(
                "max_output_tokens must be positive",
                details={"max_output_tokens": self.max_output_tokens},
            )

        # Bound the generation length. A caller that omits the cap must not
        # get an unbounded generation that could exhaust the token budget.
        cap = self.max_output_tokens or MAX_OUTPUT_TOKENS_LIMIT
        object.__setattr__(self, "max_output_tokens", min(cap, MAX_OUTPUT_TOKENS_LIMIT))


@dataclass(frozen=True, slots=True)
class ModelResponse:
    """A complete response from a model.

    Attributes:
        model: The model that produced this response, as actually used (which
            may differ from the requested model after failover).
        text: Assistant text content, when any.
        finish_reason: Normalised termination reason.
        tool_calls: Tool calls requested by the model.
        usage: Reported usage, or ``None`` when the provider did not report
            it. Callers must handle ``None`` rather than treating it as zero.
        raw_finish_reason: Provider-native reason, preserved for diagnostics
            when it did not map cleanly onto :class:`FinishReason`.
        response_id: Provider-assigned response id, when supplied.
    """

    model: ModelRef
    text: str = ""
    finish_reason: FinishReason = FinishReason.STOP
    tool_calls: tuple[ToolCall, ...] = ()
    usage: Usage | None = None
    raw_finish_reason: str | None = None
    response_id: str | None = None

    @property
    def is_truncated(self) -> bool:
        """Whether generation stopped because it hit the length limit."""
        return self.finish_reason is FinishReason.LENGTH

    @property
    def requires_tool_execution(self) -> bool:
        """Whether the runtime must execute tools before continuing."""
        return bool(self.tool_calls)


@dataclass(frozen=True, slots=True)
class ModelStreamEvent:
    """One incremental event from a streaming model call.

    Attributes:
        model: The model producing the stream.
        delta_text: Incremental text, when this event carries text.
        delta_tool_call: Incremental tool-call fragment, when present.
        finish_reason: Set on the terminal event only.
        usage: Set on the terminal event when the provider reports usage.
        is_terminal: Whether this is the last event of the stream.
    """

    model: ModelRef
    delta_text: str = ""
    delta_tool_call: ToolCall | None = None
    finish_reason: FinishReason | None = None
    usage: Usage | None = None
    is_terminal: bool = False
    raw_finish_reason: str | None = None
