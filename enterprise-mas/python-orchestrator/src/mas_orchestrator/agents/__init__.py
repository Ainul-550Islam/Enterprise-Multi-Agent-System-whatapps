"""Agent runtime.

Implements spec §E. An execution selects an agent, runs it under explicit
bounds, and records why it stopped.

The parts:

* :mod:`~mas_orchestrator.agents.base` — specs, limits, outcomes and the
  :class:`BaseAgent` protocol. Agent kinds and step ceilings mirror the Rust
  contracts exactly.
* :mod:`~mas_orchestrator.agents.executor` — the loop: model call, tool calls,
  repeat, until an answer or a bound.
* :mod:`~mas_orchestrator.agents.supervisor` — delegates to other agents
  instead of answering itself.
* :mod:`~mas_orchestrator.agents.delegation` — delegation and handoff, bounded
  by depth.
* :mod:`~mas_orchestrator.agents.planner` — model-backed planning.
* :mod:`~mas_orchestrator.agents.registry` / :mod:`~.factory` — registration
  and construction. The registry holds *factories*, so each execution gets a
  fresh agent rather than inheriting another execution's state.
* :mod:`~mas_orchestrator.agents.router` — policy-driven agent selection.
* :mod:`~mas_orchestrator.agents.runtime` — the composition root.

Two invariants hold throughout:

1. Tenancy comes from the :class:`~mas_orchestrator.context.ExecutionContext`,
   never from a call argument.
2. Every run has a bound — steps, tool calls, delegation depth, deadline or
   budget. An agent that does not stop on its own is stopped.
"""

from __future__ import annotations

from mas_orchestrator.agents.agent import (
    WORKER_TOOLS,
    AgentBundle,
    DefaultAgent,
    build_agents,
    refused_outcome,
)
from mas_orchestrator.agents.base import (
    MAX_DELEGATION_DEPTH,
    MAX_STEPS_PER_EXECUTION,
    MAX_TOOL_CALLS_PER_EXECUTION,
    AgentKind,
    AgentLimits,
    AgentOutcome,
    AgentSpec,
    AgentState,
    AgentStatus,
    BaseAgent,
    Termination,
    budget_exhausted,
    deadline_from,
    new_state,
)
from mas_orchestrator.agents.delegation import (
    DelegationRequest,
    DelegationResult,
    check_delegation,
    handoff_state,
    new_delegation_id,
    state_for_delegate,
    summarise_for_caller,
    wrap_delegation_failure,
)
from mas_orchestrator.agents.executor import (
    DEFAULT_MODEL_TIMEOUT_SECONDS,
    MAX_TOOL_BATCH,
    AgentExecutor,
    AgentRunConfig,
    seed_state,
)
from mas_orchestrator.agents.factory import (
    AgentFactory,
    build_agent_registry,
)
from mas_orchestrator.agents.planner import (
    MAX_PLAN_STEPS,
    PLAN_SCHEMA,
    Plan,
    Planner,
    PlanStep,
    parse_plan,
)
from mas_orchestrator.agents.router import (
    AgentRejection,
    AgentRouter,
    AgentRoutingPolicy,
    AgentRoutingRequest,
    build_policy,
)
from mas_orchestrator.agents.runtime import (
    DEFAULT_EXECUTION_TIMEOUT_SECONDS,
    AgentRuntime,
    ExecutionResult,
    RuntimeConfig,
    build_default_limits,
)
from mas_orchestrator.agents.supervisor import (
    MAX_PARALLEL_DELEGATIONS,
    SupervisorAgent,
    SupervisorOutcome,
)

__all__ = [
    "DEFAULT_EXECUTION_TIMEOUT_SECONDS",
    "DEFAULT_MODEL_TIMEOUT_SECONDS",
    "MAX_DELEGATION_DEPTH",
    "MAX_PARALLEL_DELEGATIONS",
    "MAX_PLAN_STEPS",
    "MAX_STEPS_PER_EXECUTION",
    "MAX_TOOL_BATCH",
    "MAX_TOOL_CALLS_PER_EXECUTION",
    "PLAN_SCHEMA",
    "WORKER_TOOLS",
    "AgentBundle",
    "AgentExecutor",
    "AgentFactory",
    "AgentKind",
    "AgentLimits",
    "AgentOutcome",
    "AgentRejection",
    "AgentRouter",
    "AgentRoutingPolicy",
    "AgentRoutingRequest",
    "AgentRunConfig",
    "AgentRuntime",
    "AgentSpec",
    "AgentState",
    "AgentStatus",
    "BaseAgent",
    "DefaultAgent",
    "DelegationRequest",
    "DelegationResult",
    "ExecutionResult",
    "Plan",
    "PlanStep",
    "Planner",
    "RuntimeConfig",
    "SupervisorAgent",
    "SupervisorOutcome",
    "Termination",
    "budget_exhausted",
    "build_agent_registry",
    "build_agents",
    "build_default_limits",
    "build_policy",
    "check_delegation",
    "deadline_from",
    "handoff_state",
    "new_delegation_id",
    "new_state",
    "parse_plan",
    "refused_outcome",
    "seed_state",
    "state_for_delegate",
    "summarise_for_caller",
    "wrap_delegation_failure",
]
