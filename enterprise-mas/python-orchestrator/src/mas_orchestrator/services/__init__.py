"""Service layer.

The services are the boundary between orchestration and storage. Each one owns
a decision that the storage layer deliberately does not make:

* :mod:`.execution_service` — the canonical spec §6 flow, end to end, reporting
  the terminal outcome to Rust rather than recording it itself.
* :mod:`.agent_service` — what may be *done* with an agent. Refuses an agent
  that is not runnable, and refuses a delegation the supervisor was not
  granted.
* :mod:`.tool_service` — which tools an agent may be shown and call. Describes
  only permitted tools, so a model is never tempted by one it cannot use.
* :mod:`.memory_service` — what an execution may recall, always fenced as
  untrusted, with durable writes screened for injection.
* :mod:`.rag_service` — retrieval filtered before ranking, with every passage
  fenced and every citation validated.
* :mod:`.policy_service` — asks the Rust authority. Defaults to deny, so
  nothing runs because nobody said no.
* :mod:`.usage_service` — usage and cost, refusing to report an incomplete
  total as though it were final.
* :mod:`.idempotency` — duplicate deliveries, classified by asking the
  authoritative execution state rather than by local memory.

None of these write to Rust-owned tables. That is the ownership rule from spec
§5 and §27, and it is why each service asks rather than records.
"""

from __future__ import annotations

from mas_orchestrator.services.agent_service import (
    AgentService,
    AgentSummary,
    classify_status,
)
from mas_orchestrator.services.execution_service import (
    ExecutionRequest,
    ExecutionService,
    LoggingStateReporter,
    StateTransitionReporter,
    build_execution_service,
)
from mas_orchestrator.services.idempotency import (
    NON_REPEATABLE_SAFETY,
    DuplicateAction,
    ExecutionState,
    ExecutionStateReader,
    IdempotencyDecision,
    UnknownStateReader,
    decide,
    evaluate,
    invocation_key,
    should_run,
    tool_is_repeatable,
)
from mas_orchestrator.services.lifecycle import (
    DrainReport,
    ExitStatus,
    ShutdownCoordinator,
    ShutdownPhase,
    exit_code_for,
)
from mas_orchestrator.services.memory_service import (
    MemoryService,
    RecalledContext,
)
from mas_orchestrator.services.policy_service import (
    PolicyService,
    PolicyVerdict,
)
from mas_orchestrator.services.rag_service import (
    RagService,
    RetrievedContext,
    require_citations,
)
from mas_orchestrator.services.tool_service import (
    ToolDescriptor,
    ToolService,
)
from mas_orchestrator.services.usage_service import (
    UsageReport,
    UsageService,
)

__all__ = [
    "NON_REPEATABLE_SAFETY",
    "AgentService",
    "AgentSummary",
    "DrainReport",
    "DuplicateAction",
    "ExecutionRequest",
    "ExecutionService",
    "ExecutionState",
    "ExecutionStateReader",
    "ExitStatus",
    "IdempotencyDecision",
    "LoggingStateReporter",
    "MemoryService",
    "PolicyService",
    "PolicyVerdict",
    "RagService",
    "RecalledContext",
    "RetrievedContext",
    "ShutdownCoordinator",
    "ShutdownPhase",
    "StateTransitionReporter",
    "ToolDescriptor",
    "ToolService",
    "UnknownStateReader",
    "UsageReport",
    "UsageService",
    "build_execution_service",
    "classify_status",
    "decide",
    "evaluate",
    "exit_code_for",
    "invocation_key",
    "require_citations",
    "should_run",
    "tool_is_repeatable",
]
