"""Execution-context construction and propagation.

The boundary layer builds an immutable
:class:`~mas_orchestrator.context.execution_context.ExecutionContext` from data
supplied by Rust, then binds it for the duration of the work using the
``contextvars`` helpers in
:mod:`mas_orchestrator.context.contextvars`.
"""

from __future__ import annotations

from mas_orchestrator.context.contextvars import (
    bind_execution_context,
    current_execution_context,
    execution_context_scope,
    maybe_current_execution_context,
    reset_execution_context,
    unbind_execution_context,
)
from mas_orchestrator.context.execution_context import (
    MANDATORY_IDENTITY_FIELDS,
    ExecutionContext,
    build_execution_context,
)

__all__ = [
    "MANDATORY_IDENTITY_FIELDS",
    "ExecutionContext",
    "bind_execution_context",
    "build_execution_context",
    "current_execution_context",
    "execution_context_scope",
    "maybe_current_execution_context",
    "reset_execution_context",
    "unbind_execution_context",
]
