"""Idempotency handling.

Spec §16 is careful about this, and for good reason: Python may receive
duplicate deliveries, and the naive fix — a Python-side idempotency store —
creates a *second* source of truth that will disagree with Rust's.

So this module does two narrow things and no more:

1. Derives a **stable invocation identity** from the authoritative context, so
   the same execution attempt always produces the same key.
2. Classifies what a duplicate delivery should do, **by asking the authoritative
   Rust execution state** through an injected reader.

It never decides on its own that an execution already ran. A deduplication
cache that said "already done, skipping" without consulting Rust would
manufacture a durable outcome out of a stale local memory — which is exactly
the failure mode the spec warns against.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

if TYPE_CHECKING:
    from mas_orchestrator.context import ExecutionContext


class ExecutionState(StrEnum):
    """The authoritative state of an execution, as reported by Rust.

    Mirrors the states an execution can occupy. The values are what a reader
    returns; the enumeration exists so every branch is handled explicitly
    rather than by comparing strings.
    """

    UNKNOWN = "unknown"
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


class DuplicateAction(StrEnum):
    """What to do with a delivery that looks like a repeat."""

    PROCEED = "proceed"
    SKIP = "skip"
    WAIT = "wait"
    REPLAY = "replay"


@dataclass(frozen=True, slots=True)
class IdempotencyDecision:
    """The decision for one delivery.

    Attributes:
        action: What to do.
        state: The authoritative state the decision was based on.
        invocation_key: The stable identity for this attempt.
        reason: Why, for the audit trail.
    """

    action: DuplicateAction
    state: ExecutionState
    invocation_key: str
    reason: str


@runtime_checkable
class ExecutionStateReader(Protocol):
    """Reads authoritative execution state from Rust."""

    async def state_of(self, execution_id: str) -> ExecutionState:
        """Returns the current authoritative state.

        Implementations must return :attr:`ExecutionState.UNKNOWN` when the
        state cannot be determined, rather than raising: an unknown state has a
        defined, safe interpretation.
        """
        ...


class UnknownStateReader:
    """A reader that knows nothing.

    The safe stand-in for "not wired up". Returning ``UNKNOWN`` means every
    delivery proceeds, which duplicates work under a duplicate delivery — but
    the alternative, guessing that an execution is done, would drop real work.
    """

    async def state_of(self, execution_id: str) -> ExecutionState:
        """Always reports unknown."""
        return ExecutionState.UNKNOWN


def invocation_key(
    context: ExecutionContext,
    *,
    attempt: str | None = None,
) -> str:
    """Derives a stable identity for one execution attempt.

    The key is a hash of the fields that identify the attempt, not a random
    value, so a redelivery of the same message produces the same key.

    Args:
        context: The authoritative execution context.
        attempt: Task attempt id, when the work is a task attempt rather than a
            whole execution. A task attempt is idempotent at attempt
            granularity, so including it keeps retries of the same task from
            colliding.
    """
    parts = [
        context.tenant_id,
        context.execution_id,
        context.task_id or "",
        attempt or context.task_attempt_id or "",
    ]
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return f"inv-{digest[:32]}"


def decide(state: ExecutionState, invocation_key_value: str) -> IdempotencyDecision:
    """Maps an authoritative state onto an action.

    The mapping is deliberately conservative:

    * A terminal state means the work is done: skip, and let the caller use
      the authoritative result rather than re-running anything.
    * ``RUNNING`` means another worker has it: wait, or skip if the caller
      cannot wait.
    * ``UNKNOWN`` means proceed. Proceeding can duplicate effort; skipping
      without evidence can drop it. Only one of those is recoverable.
    """
    if state is ExecutionState.SUCCEEDED:
        return IdempotencyDecision(
            DuplicateAction.SKIP,
            state,
            invocation_key_value,
            "execution already succeeded; use the authoritative result",
        )
    if state is ExecutionState.FAILED:
        return IdempotencyDecision(
            DuplicateAction.SKIP,
            state,
            invocation_key_value,
            "execution already failed; use the authoritative result",
        )
    if state is ExecutionState.CANCELLED:
        return IdempotencyDecision(
            DuplicateAction.SKIP,
            state,
            invocation_key_value,
            "execution was cancelled",
        )
    if state is ExecutionState.TIMED_OUT:
        return IdempotencyDecision(
            DuplicateAction.SKIP,
            state,
            invocation_key_value,
            "execution already timed out",
        )
    if state is ExecutionState.RUNNING:
        return IdempotencyDecision(
            DuplicateAction.WAIT,
            state,
            invocation_key_value,
            "execution is already running elsewhere",
        )
    if state is ExecutionState.PENDING:
        return IdempotencyDecision(
            DuplicateAction.PROCEED,
            state,
            invocation_key_value,
            "execution is pending and not yet claimed",
        )
    return IdempotencyDecision(
        DuplicateAction.PROCEED,
        state,
        invocation_key_value,
        "state is unknown; proceeding is the only safe option",
    )


async def evaluate(
    context: ExecutionContext,
    reader: ExecutionStateReader,
    *,
    attempt: str | None = None,
) -> IdempotencyDecision:
    """Asks the authority and decides what to do.

    Args:
        context: The authoritative execution context.
        reader: Reads execution state from Rust.
        attempt: Task attempt id, when applicable.

    Returns:
        An :class:`IdempotencyDecision`.
    """
    key = invocation_key(context, attempt=attempt)
    state = await reader.state_of(context.execution_id)
    decision = decide(state, key)

    if decision.action is not DuplicateAction.PROCEED:
        _LOGGER.info(
            "duplicate delivery detected",
            extra={
                "event": "idempotency.duplicate",
                "execution_id": context.execution_id,
                "state": state.value,
                "action": decision.action.value,
            },
        )
    return decision


def should_run(decision: IdempotencyDecision) -> bool:
    """Whether the work should actually run."""
    return decision.action is DuplicateAction.PROCEED


# --- Tool-level idempotency ------------------------------------------------

# Safety classes whose effects cannot be safely repeated. Read-only tools may be
# retried freely; anything that could mutate state outside this process may not.
NON_REPEATABLE_SAFETY: Final[frozenset[str]] = frozenset({"sensitive", "destructive"})


def tool_is_repeatable(*, safety: str, idempotent: bool) -> bool:
    """Whether a tool call may be repeated after a transient failure.

    Two independent gates, because both matter:

    * An ``idempotent`` tool declares that repeating it is safe.
    * A ``sensitive`` or ``destructive`` tool is not repeatable even if it
      claims idempotency, because its safety class says the blast radius is
      too large to risk a duplicate.

    Retrying a non-idempotent tool can manufacture a duplicate durable outcome,
    which is the specific harm spec §16 warns about.
    """
    if safety in NON_REPEATABLE_SAFETY:
        return False
    return idempotent
