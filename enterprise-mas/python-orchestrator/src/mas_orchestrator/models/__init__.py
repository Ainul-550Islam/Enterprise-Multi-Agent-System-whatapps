"""Model layer: the provider abstraction the AI runtime depends on.

Structure:

* :mod:`mas_orchestrator.models.types` — the value objects every adapter and
  the whole runtime speak: messages, requests, responses, usage, cost.
* :mod:`mas_orchestrator.models.base` — the ``ModelProvider`` interface.
* :mod:`mas_orchestrator.models.registry` — which provider serves which model.
* :mod:`mas_orchestrator.models.router` — policy-aware model selection.
* :mod:`mas_orchestrator.models.failover` — bounded, policy-respecting fallback.
* :mod:`mas_orchestrator.models.retry` — classification, backoff, budgets.
* :mod:`mas_orchestrator.models.streaming` — SSE parsing and accumulation.
* :mod:`mas_orchestrator.models.cost` — usage and cost accounting.

Concrete adapters live in :mod:`mas_orchestrator.providers`. Nothing in this
package performs I/O except through an injected provider.
"""

from __future__ import annotations

from mas_orchestrator.models.base import (
    ModelProvider,
    assert_streaming_supported,
    capability_guard,
)
from mas_orchestrator.models.cost import (
    CostLedger,
    CostTotals,
    PricingTable,
    pricing_from_config,
)
from mas_orchestrator.models.failover import (
    FailoverAttempt,
    FailoverResult,
    execute_with_failover,
)
from mas_orchestrator.models.registry import ModelRegistry
from mas_orchestrator.models.retry import RetryOutcome, RetryPolicy, budget_from_deadline
from mas_orchestrator.models.router import (
    ModelRouter,
    RoutingPolicy,
    RoutingRequest,
    SelectionReason,
)
from mas_orchestrator.models.streaming import StreamAccumulator, aiter_stream, iter_sse_data
from mas_orchestrator.models.types import (
    CostEstimate,
    FinishReason,
    Message,
    ModelCapabilities,
    ModelRef,
    ModelRequest,
    ModelResponse,
    ModelStreamEvent,
    Role,
    ToolCall,
    ToolCallResult,
    ToolSpec,
    Usage,
)

__all__ = [
    "CostEstimate",
    "CostLedger",
    "CostTotals",
    "FailoverAttempt",
    "FailoverResult",
    "FinishReason",
    "Message",
    "ModelCapabilities",
    "ModelProvider",
    "ModelRef",
    "ModelRegistry",
    "ModelRequest",
    "ModelResponse",
    "ModelRouter",
    "ModelStreamEvent",
    "PricingTable",
    "RetryOutcome",
    "RetryPolicy",
    "Role",
    "RoutingPolicy",
    "RoutingRequest",
    "SelectionReason",
    "StreamAccumulator",
    "ToolCall",
    "ToolCallResult",
    "ToolSpec",
    "Usage",
    "aiter_stream",
    "assert_streaming_supported",
    "budget_from_deadline",
    "capability_guard",
    "execute_with_failover",
    "iter_sse_data",
    "pricing_from_config",
]
