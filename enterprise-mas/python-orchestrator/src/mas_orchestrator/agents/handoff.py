"""Agent handoff.

A handoff is a delegation that does not come back: the current agent stops and
another takes over. It is more dangerous than delegation for one reason — the
original agent is no longer in the loop to notice anything going wrong.

So the handoff chain is bounded in three ways, each catching a failure the
others miss:

* **Depth.** A chain longer than :data:`MAX_HANDOFF_DEPTH` is refused. Depth
  limits the total work one request can cause.
* **Repeats.** An agent may appear only :data:`MAX_REPEATS_PER_AGENT` times. A
  cycle that stays within the depth limit is still a loop, and it is the
  expensive kind because every hop is a full agent run.
* **Cost.** A handoff carries a budget. Each hop spends from it, and an
  exhausted budget stops the chain. This is what bounds a chain that is
  structurally legal but does far more work than intended.

The chain is also **immutable and tenant-scoped**: :meth:`HandoffChain.extend`
returns a new chain rather than mutating, so a branch cannot corrupt the record
of how control arrived at the current agent — which is what an audit needs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Deepest handoff chain permitted.
MAX_HANDOFF_DEPTH: Final[int] = 5

#: How many times one agent may appear in a chain. Two is enough for a
#: deliberate A→B→A round trip and too few for a cycle.
MAX_REPEATS_PER_AGENT: Final[int] = 2

#: Default work budget for a chain, in abstract units.
DEFAULT_BUDGET: Final[int] = 100


@dataclass(frozen=True, slots=True)
class HandoffChain:
    """How control arrived at the current agent.

    Attributes:
        tenant_id: Owning tenant. A chain never crosses tenants.
        agents: Agent ids in the order control passed to them.
        budget: Remaining work budget.
        reasons: Why each hop was taken, parallel to ``agents`` after the first.
    """

    tenant_id: str
    agents: tuple[str, ...] = ()
    budget: int = DEFAULT_BUDGET
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "a handoff chain must belong to a tenant",
                details={"field": "tenant_id"},
            )
        if self.budget < 0:
            raise InvalidRequestError(
                "budget must not be negative", details={"budget": self.budget}
            )

    @property
    def depth(self) -> int:
        """How many hops have happened."""
        return max(0, len(self.agents) - 1)

    @property
    def current(self) -> str | None:
        """The agent currently in control."""
        return self.agents[-1] if self.agents else None

    @property
    def exhausted(self) -> bool:
        """Whether the budget is spent."""
        return self.budget <= 0

    def repeats_of(self, agent_id: str) -> int:
        """How many times an agent appears."""
        return sum(1 for entry in self.agents if entry == agent_id)


@dataclass(frozen=True, slots=True)
class HandoffDecision:
    """Whether a handoff may happen.

    Attributes:
        allowed: Whether it may proceed.
        reason: Why not, when refused.
        code: A stable machine-readable reason code.
    """

    allowed: bool
    reason: str = ""
    code: str = "ok"

    @property
    def refused(self) -> bool:
        """Whether the handoff must not happen."""
        return not self.allowed


def evaluate_handoff(
    chain: HandoffChain,
    *,
    to_agent_id: str,
    cost: int = 1,
    max_depth: int = MAX_HANDOFF_DEPTH,
) -> HandoffDecision:
    """Decides whether a handoff may proceed.

    Args:
        chain: The chain so far.
        to_agent_id: The agent to hand to.
        cost: What this hop costs from the budget.
        max_depth: Override for the depth ceiling.

    Returns:
        A :class:`HandoffDecision`.
    """
    if not to_agent_id.strip():
        return HandoffDecision(False, "a handoff must name a target agent", "missing_target")
    if cost < 0:
        return HandoffDecision(False, "cost must not be negative", "invalid_cost")

    if chain.depth >= max_depth:
        return HandoffDecision(
            False,
            f"handoff chain has reached its depth ceiling of {max_depth}",
            "depth_exceeded",
        )
    if chain.repeats_of(to_agent_id) >= MAX_REPEATS_PER_AGENT:
        return HandoffDecision(
            False,
            f"agent '{to_agent_id}' has already appeared "
            f"{MAX_REPEATS_PER_AGENT} times in this chain",
            "cycle_detected",
        )
    if chain.budget < cost:
        return HandoffDecision(
            False,
            f"handoff budget exhausted: {chain.budget} remaining, {cost} needed",
            "budget_exhausted",
        )
    return HandoffDecision(True, "", "ok")


def start_chain(*, tenant_id: str, agent_id: str, budget: int = DEFAULT_BUDGET) -> HandoffChain:
    """Begins a chain at an agent."""
    if not agent_id.strip():
        raise InvalidRequestError("a chain must start at an agent", details={"field": "agent_id"})
    return HandoffChain(tenant_id=tenant_id, agents=(agent_id,), budget=budget)


def extend(
    chain: HandoffChain,
    *,
    to_agent_id: str,
    reason: str = "",
    cost: int = 1,
    max_depth: int = MAX_HANDOFF_DEPTH,
) -> HandoffChain:
    """Returns a new chain with one more hop, refusing one that must not happen.

    Raises:
        InvalidRequestError: If the handoff is refused. Raising rather than
            returning the chain unchanged means a refused handoff cannot be
            ignored by a caller that forgets to check a verdict.
    """
    decision = evaluate_handoff(chain, to_agent_id=to_agent_id, cost=cost, max_depth=max_depth)
    if decision.refused:
        _LOGGER.warning(
            "handoff refused",
            extra={
                "event": "agent.handoff_refused",
                "to": to_agent_id,
                "code": decision.code,
                "depth": chain.depth,
            },
        )
        raise InvalidRequestError(
            f"handoff refused: {decision.reason}",
            details={
                "to_agent_id": to_agent_id,
                "code": decision.code,
                "depth": chain.depth,
                "budget": chain.budget,
            },
        )
    return replace(
        chain,
        agents=(*chain.agents, to_agent_id),
        budget=chain.budget - cost,
        reasons=(*chain.reasons, reason),
    )


def describe(chain: HandoffChain) -> str:
    """A one-line description of a chain, for diagnostics.

    Names agents only. A reason could contain user text, which must not reach a
    log.
    """
    path = " → ".join(chain.agents) if chain.agents else "(empty)"
    return f"{path} [depth={chain.depth}, budget={chain.budget}]"


def assert_same_tenant(chain: HandoffChain, tenant_id: str) -> None:
    """Refuses a chain used outside the tenant that owns it.

    Raises:
        InvalidRequestError: If the tenants differ.
    """
    if chain.tenant_id != tenant_id:
        raise InvalidRequestError(
            "handoff chain belongs to a different tenant",
            details={"chain_tenant": chain.tenant_id, "tenant": tenant_id},
        )
