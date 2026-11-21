"""Agent abstractions.

The agent kinds are not our invention — they come from Rust's
``crates/contracts/src/agent.rs``, which validates ``kind`` against exactly
``standard`` | ``supervisor`` | ``tool_user``. Anything else is a protocol
error, so we reuse those three names verbatim.

The execution ceilings likewise mirror ``crates/common/src/constants.rs``:
``MAX_STEPS_PER_EXECUTION = 1000`` and ``MAX_TOOL_CALLS_PER_EXECUTION = 500``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from mas_orchestrator.context import ExecutionContext
    from mas_orchestrator.models.cost import CostLedger
    from mas_orchestrator.models.types import Message

# --- Rust-aligned ceilings -------------------------------------------------

MAX_STEPS_PER_EXECUTION: Final[int] = 1_000
MAX_TOOL_CALLS_PER_EXECUTION: Final[int] = 500
MAX_DELEGATION_DEPTH: Final[int] = 5


class AgentKind(StrEnum):
    """What an agent is for. Mirrors the Rust ``kind`` field.

    * ``STANDARD`` — answers directly from the model.
    * ``TOOL_USER`` — may call tools to gather information.
    * ``SUPERVISOR`` — delegates to other agents instead of answering itself.
    """

    STANDARD = "standard"
    SUPERVISOR = "supervisor"
    TOOL_USER = "tool_user"


class AgentStatus(StrEnum):
    """Lifecycle state of a registered agent."""

    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    DISABLED = "disabled"

    @property
    def is_runnable(self) -> bool:
        """Whether an agent in this state may execute."""
        return self in (AgentStatus.PUBLISHED, AgentStatus.DEPRECATED)


class Termination(StrEnum):
    """Why an agent stopped.

    Recorded on every outcome so that "the agent finished" and "the agent was
    stopped" are never the same sentence.
    """

    COMPLETED = "completed"
    STEP_LIMIT = "step_limit"
    TOOL_CALL_LIMIT = "tool_call_limit"
    DEADLINE = "deadline"
    CANCELLED = "cancelled"
    BUDGET_EXHAUSTED = "budget_exhausted"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class AgentLimits:
    """Per-agent ceilings, clamped to the platform maxima.

    A configured value above the platform maximum is clamped rather than
    refused, because the platform maximum is a hard resource guarantee that no
    tenant configuration may exceed.
    """

    max_steps: int = 25
    max_tool_calls: int = MAX_TOOL_CALLS_PER_EXECUTION
    max_delegation_depth: int = MAX_DELEGATION_DEPTH
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.max_steps < 1:
            raise InvalidRequestError(
                "max_steps must be at least 1", details={"field": "max_steps"}
            )
        if self.max_tool_calls < 0:
            raise InvalidRequestError(
                "max_tool_calls must not be negative", details={"field": "max_tool_calls"}
            )
        if self.max_delegation_depth < 0:
            raise InvalidRequestError(
                "max_delegation_depth must not be negative",
                details={"field": "max_delegation_depth"},
            )
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise InvalidRequestError(
                "timeout must be positive", details={"field": "timeout_seconds"}
            )

    def clamped(self) -> AgentLimits:
        """Returns limits that fit inside the platform ceilings."""
        return AgentLimits(
            max_steps=min(self.max_steps, MAX_STEPS_PER_EXECUTION),
            max_tool_calls=min(self.max_tool_calls, MAX_TOOL_CALLS_PER_EXECUTION),
            max_delegation_depth=min(self.max_delegation_depth, MAX_DELEGATION_DEPTH),
            timeout_seconds=self.timeout_seconds,
        )


@dataclass(frozen=True, slots=True)
class AgentSpec:
    """An immutable agent definition.

    Attributes:
        agent_id: Stable identifier, owned by Rust.
        agent_version_id: Version identifier, owned by Rust.
        name: Human-readable name.
        slug: URL-safe identifier.
        kind: One of :class:`AgentKind`.
        tenant_id: Owning tenant.
        status: Lifecycle state.
        description: Optional description.
        model_hint: Optional preferred model, used as a routing preference
            rather than a hard requirement.
        allowed_tools: Tool ids this agent may call. A supervisor may also
            name delegated agents here.
        system_prompt: The agent's instructions.
        limits: Execution ceilings.
        tags: Free-form labels.
        read_only: Product-owned definition that user-facing agent lifecycle
            operations must not edit or delete.
    """

    agent_id: str
    agent_version_id: str
    name: str
    slug: str
    kind: AgentKind
    tenant_id: str
    status: AgentStatus = AgentStatus.PUBLISHED
    description: str | None = None
    model_hint: str | None = None
    allowed_tools: frozenset[str] = field(default_factory=frozenset)
    system_prompt: str = ""
    limits: AgentLimits = field(default_factory=AgentLimits)
    tags: tuple[str, ...] = ()
    read_only: bool = False

    def __post_init__(self) -> None:
        for field_name in ("agent_id", "agent_version_id", "name", "slug", "tenant_id"):
            if not getattr(self, field_name).strip():
                raise InvalidRequestError(
                    f"{field_name} must not be empty", details={"field": field_name}
                )
        if not isinstance(self.limits, AgentLimits):
            raise InvalidRequestError(
                "limits must be an AgentLimits instance", details={"field": "limits"}
            )
        if not isinstance(self.read_only, bool):
            raise InvalidRequestError("read_only must be a boolean", details={"field": "read_only"})

    @property
    def is_runnable(self) -> bool:
        """Whether this agent may execute at all."""
        return self.status.is_runnable

    @property
    def may_use_tools(self) -> bool:
        """Whether this agent's kind permits tool use."""
        return self.kind in (AgentKind.TOOL_USER, AgentKind.SUPERVISOR)

    def permits_tool(self, tool_id: str) -> bool:
        """Whether this agent is allowed to call a specific tool.

        An agent that cannot use tools by kind is refused regardless of its
        allowlist, so a misconfiguration cannot grant capability.
        """
        if not self.may_use_tools:
            return False
        return tool_id in self.allowed_tools


@dataclass(frozen=True, slots=True)
class AgentState:
    """Mutable-per-step state carried through an agent's execution.

    Attributes:
        messages: The conversation so far.
        steps_taken: Model calls made.
        tool_calls_made: Tool invocations made.
        depth: Delegation depth, zero for the root agent.
        metadata: Free-form per-execution annotations.
    """

    messages: tuple[Any, ...] = ()
    steps_taken: int = 0
    tool_calls_made: int = 0
    depth: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def with_messages(self, messages: Sequence[Any]) -> AgentState:
        """Returns a copy with a new message list."""
        from dataclasses import replace

        return replace(self, messages=tuple(messages))

    def with_metadata(self, **updates: Any) -> AgentState:
        """Returns a copy with additional metadata."""
        from dataclasses import replace

        merged = dict(self.metadata)
        merged.update(updates)
        return replace(self, metadata=merged)


@dataclass(frozen=True, slots=True)
class AgentOutcome:
    """The result of running one agent.

    Attributes:
        agent_id: Agent that ran.
        termination: Why it stopped.
        content: Final answer text, when it produced one.
        messages: Full transcript, for the caller's audit trail.
        steps_taken: Model calls made.
        tool_calls_made: Tool invocations made.
        cost: Cost ledger for this agent and anything it delegated to.
        error: The error that stopped it, when termination is ``FAILED``.
        duration_ms: Wall-clock duration.
    """

    agent_id: str
    termination: Termination
    content: str = ""
    messages: tuple[Any, ...] = ()
    steps_taken: int = 0
    tool_calls_made: int = 0
    cost: Any = None
    error: BaseException | None = None
    duration_ms: int = 0

    @property
    def succeeded(self) -> bool:
        """Whether the agent reached a final answer."""
        return self.termination is Termination.COMPLETED

    def __post_init__(self) -> None:
        if self.termination is Termination.FAILED and self.error is None:
            raise ConfigurationError(
                "a failed agent outcome must carry the error",
                details={"agent_id": self.agent_id},
            )


@runtime_checkable
class BaseAgent(Protocol):
    """What every agent must be able to do."""

    @property
    def spec(self) -> AgentSpec:
        """The agent's definition."""
        ...

    async def run(
        self,
        state: AgentState,
        context: ExecutionContext,
        *,
        cancel_event: Any = None,
    ) -> AgentOutcome:
        """Runs the agent to completion or to a bound.

        Implementations must respect the agent's :class:`AgentLimits`, the
        execution deadline and the cancellation event. They must not raise for
        an ordinary failure: a failure is an :class:`AgentOutcome` with
        ``termination=FAILED``.
        """
        ...


def budget_exhausted(ledger: CostLedger | None, ceiling: float | None) -> bool:
    """Whether accumulated cost has reached a configured ceiling.

    An incomplete ledger — one containing unpriced or unmeasured calls — is
    treated as **not** exhausted, because the total is only a lower bound and
    stopping on a guess would be wrong in the other direction. The caller is
    expected to surface that incompleteness separately.
    """
    if ledger is None or ceiling is None:
        return False
    totals = ledger.totals()
    if not totals.is_complete:
        return False
    return totals.total_cost >= ceiling


def deadline_from(context: ExecutionContext) -> float | None:
    """Monotonic deadline derived from the authoritative execution context.

    Returns ``None`` when the execution carries no deadline, meaning "no
    explicit bound" rather than "infinite".
    """
    if context.deadline_epoch_ms is None:
        return None
    remaining = context.remaining_seconds()
    if remaining is None:
        return None
    return time.monotonic() + remaining


def new_state(system_prompt: str = "", user_input: str = "") -> AgentState:
    """Builds an initial agent state.

    Kept here so message construction stays in one place and the system
    message is never accidentally omitted.
    """
    from mas_orchestrator.models.types import Message

    messages: list[Message] = []
    if system_prompt:
        messages.append(Message.system(system_prompt))
    if user_input:
        messages.append(Message.user(user_input))
    return AgentState(messages=tuple(messages))
