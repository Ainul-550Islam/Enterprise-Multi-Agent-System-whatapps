"""Safe async request/execution context propagation.

The orchestrator is fully asynchronous: many executions are in flight
concurrently inside one process, often within one task group. Passing the
:class:`~mas_orchestrator.context.execution_context.ExecutionContext` explicitly
through every call is correct but verbose, and a mutable global would be
catastrophically wrong (concurrent executions would see each other's tenancy).

``contextvars`` is the correct primitive: a value bound in a task is visible to
everything that task awaits, and is **copied** — not shared — when a new task is
spawned. This module wraps it with a narrow, typed API:

* :func:`bind_execution_context` binds a context and returns a reset token.
* :func:`current_execution_context` reads it, raising when unbound.
* :func:`maybe_current_execution_context` reads it, returning ``None``.

No function here stores the context in a module-level variable, and the
underlying :class:`~contextvars.ContextVar` is not exported, so no caller can
bind through a back door.

Why raising instead of defaulting: a missing context means a code path is
running outside the boundary that establishes tenancy. Silently proceeding with
a placeholder tenant would let AI work execute against the wrong scope, which
is exactly the failure the ownership rules forbid.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterator

from mas_orchestrator.context.execution_context import ExecutionContext
from mas_orchestrator.contracts.errors import InternalError

__all__ = [
    "bind_execution_context",
    "current_execution_context",
    "maybe_current_execution_context",
    "reset_execution_context",
    "unbind_execution_context",
]

#: The bound context for the current async task. Not exported by design.
_EXECUTION_CONTEXT: Final[ContextVar[ExecutionContext | None]] = ContextVar(
    "mas_execution_context", default=None
)


def bind_execution_context(context: ExecutionContext) -> Token[ExecutionContext | None]:
    """Binds *context* to the current task and returns its reset token.

    Args:
        context: The validated execution context to bind.

    Returns:
        The :class:`~contextvars.Token` needed to restore the previous value.
        Callers must pass it to :func:`reset_execution_context`; the
        :func:`execution_context_scope` helper does this automatically.
    """
    if not isinstance(context, ExecutionContext):  # pragma: no cover - type guard
        raise InternalError(
            "bind_execution_context requires an ExecutionContext",
            details={"received": type(context).__name__},
        )
    return _EXECUTION_CONTEXT.set(context)


def reset_execution_context(token: Token[ExecutionContext | None]) -> None:
    """Restores the context that was active before :func:`bind_execution_context`.

    Args:
        token: The token returned by the bind call.
    """
    _EXECUTION_CONTEXT.reset(token)


def unbind_execution_context() -> None:
    """Clears the bound context for the current task.

    Used by test teardown and by the shutdown path so that a recycled task
    cannot observe a stale context from a previous request.
    """
    _EXECUTION_CONTEXT.set(None)


def maybe_current_execution_context() -> ExecutionContext | None:
    """Returns the bound context, or ``None`` when the task has none bound.

    Prefer this in code that legitimately runs both inside and outside an
    execution (for example startup health checks).
    """
    return _EXECUTION_CONTEXT.get()


def current_execution_context() -> ExecutionContext:
    """Returns the bound context, raising when none is bound.

    Returns:
        The :class:`ExecutionContext` bound to this task.

    Raises:
        InternalError: If no context is bound. This is an internal error rather
            than a request error because reaching this point means a component
            that requires tenancy was invoked outside the execution boundary —
            a wiring defect, not a bad request.
    """
    context = _EXECUTION_CONTEXT.get()
    if context is None:
        raise InternalError(
            "no execution context is bound to the current task",
            details={"reason": "unbound_context"},
        )
    return context


@contextmanager
def execution_context_scope(context: ExecutionContext) -> Iterator[ExecutionContext]:
    """Binds *context* for the duration of the block, then restores the previous.

    Args:
        context: The execution context to bind.

    Yields:
        The same context, for convenience at the call site.

    Example:
        >>> with execution_context_scope(ctx) as bound:  # doctest: +SKIP
        ...     await runtime.run(bound)
    """
    token = bind_execution_context(context)
    try:
        yield context
    finally:
        reset_execution_context(token)
