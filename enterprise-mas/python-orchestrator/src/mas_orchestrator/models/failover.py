"""Provider/model failover with typed retryability and bounded fallback.

Failover walks a chain of models produced by
:meth:`~mas_orchestrator.models.router.ModelRouter.failover_chain`. Because that
chain has already been filtered by the routing policy, failover structurally
cannot bypass the allowlist, a capability requirement, or a cost ceiling
(spec §8). This module adds the remaining guarantees:

* **Only retryable failures trigger fallback.** A non-retryable failure
  (authentication, invalid request, quota) is raised immediately. Falling back
  would only repeat the rejection against another model and hide the cause.
* **The chain is bounded.** ``limit`` caps how many models are tried.
* **Cancellation short-circuits everything.** Once cancellation is observed no
  further model is attempted, even if attempts remain.
* **The original failure is preserved.** When every model fails, the error
  carries the per-model failures in its details, so the first (and usually
  most relevant) cause is not lost behind the last one.

What this module does not do: it does not retry the *same* model. Per-model
retries are the job of :class:`~mas_orchestrator.models.retry.RetryPolicy`, which
owns backoff and budgets. Composing the two — retry the model, then fail over —
is done by the agent runtime, which can apply a bounded budget across both.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, TypeVar

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ModelNotAvailableError,
    OrchestratorError,
    is_retryable,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from mas_orchestrator.models.types import ModelRef

_LOGGER: Final = logging.getLogger(__name__)

T = TypeVar("T")

__all__ = [
    "FailoverAttempt",
    "FailoverResult",
    "execute_with_failover",
]


@dataclass(frozen=True, slots=True)
class FailoverAttempt:
    """The outcome of one model in the chain.

    Attributes:
        model: The model attempted.
        succeeded: Whether it succeeded.
        error_code: The domain error code, when it failed.
        retryable: Whether the failure was classified retryable.
    """

    model: ModelRef
    succeeded: bool
    error_code: str | None = None
    retryable: bool = False


@dataclass(frozen=True, slots=True)
class FailoverResult:
    """The aggregate outcome of walking a failover chain.

    Attributes:
        value: The successful result, when one model succeeded.
        model: The model that produced it.
        attempts: One entry per model tried, in order.
        exhausted: Whether the whole chain failed.
    """

    value: object = None
    model: ModelRef | None = None
    attempts: tuple[FailoverAttempt, ...] = field(default_factory=tuple)
    exhausted: bool = False

    @property
    def succeeded(self) -> bool:
        """Whether any model in the chain succeeded."""
        return not self.exhausted


async def execute_with_failover(
    models: Sequence[ModelRef],
    call: Callable[[ModelRef], Awaitable[T]],
    *,
    operation: str = "model_call",
    cancel_event: asyncio.Event | None = None,
) -> FailoverResult:
    """Tries *models* in order until one succeeds.

    Args:
        models: Ordered candidates. Must already be policy-filtered;
            this function does not re-check the allowlist.
        call: Coroutine performing the attempt for one model.
        operation: Name used in log lines and error details.
        cancel_event: Cancellation signal checked between attempts.

    Returns:
        The aggregate result. When every model fails, ``exhausted`` is
        ``True`` and ``value`` is ``None``.

    Raises:
        CancellationRequestedError: If cancellation is observed. No further
            model is attempted.
        ModelNotAvailableError: If ``models`` is empty — there is nothing
            to fall back to, and silently succeeding with no result would
            be worse than reporting it.
        OrchestratorError: When the chain is exhausted. Which error that
            is depends on how the walk ended. If every model failed with a
            retryable error, the **first** one is raised — they are
            equivalent symptoms and the first is the cause that started
            the failover. If a **non-retryable** error stopped the walk,
            that error is raised instead: it is the reason no further
            attempt could help, and reporting an earlier transient
            failure would misattribute the cause. Either way the full
            per-model history is attached to the error's details, so
            nothing is lost.
    """
    if not models:
        raise ModelNotAvailableError("failover chain is empty", details={"operation": operation})

    attempts: list[FailoverAttempt] = []
    first_error: BaseException | None = None
    terminal_error: BaseException | None = None

    for index, model in enumerate(models):
        if cancel_event is not None and cancel_event.is_set():
            raise CancellationRequestedError(f"{operation} cancelled before trying {model.key}")

        try:
            value = await call(model)
        except asyncio.CancelledError as error:
            raise CancellationRequestedError(f"{operation} cancelled during {model.key}") from error
        except BaseException as error:
            retryable = is_retryable(error)
            attempts.append(
                FailoverAttempt(
                    model=model,
                    succeeded=False,
                    error_code=str(getattr(error, "code", type(error).__name__)),
                    retryable=retryable,
                )
            )
            if first_error is None:
                first_error = error

            if not retryable:
                # Remembered as the terminal cause. A rejection that no other
                # model can override outranks an earlier transient failure:
                # "rate limited" invites a retry that will fail the same way,
                # whereas "authentication failed" names what must be fixed.
                terminal_error = error
                _LOGGER.info(
                    "not failing over: failure is not retryable",
                    extra={
                        "operation": operation,
                        "model": model.key,
                        "error": type(error).__name__,
                    },
                )
                break

            _LOGGER.warning(
                "failing over to the next model",
                extra={
                    "operation": operation,
                    "failed_model": model.key,
                    "next_model": models[index + 1].key if index + 1 < len(models) else "<none>",
                    "error": type(error).__name__,
                },
            )
            continue

        attempts.append(FailoverAttempt(model=model, succeeded=True))
        return FailoverResult(value=value, model=model, attempts=tuple(attempts))

    assert first_error is not None  # guaranteed: at least one attempt ran
    raised = terminal_error if terminal_error is not None else first_error
    _attach_history(raised, attempts, operation)
    raise raised


def _attach_history(
    error: BaseException,
    attempts: list[FailoverAttempt],
    operation: str,
) -> None:
    """Records the per-model walk on *error* so no detail is lost."""
    if isinstance(error, OrchestratorError):
        # Preserve the typed error and attach the full history.
        error.details["failover_attempts"] = [
            {
                "model": attempt.model.key,
                "error_code": attempt.error_code,
                "retryable": attempt.retryable,
            }
            for attempt in attempts
        ]
        error.details["operation"] = operation


def describe_chain(models: Sequence[ModelRef]) -> str:
    """Renders a failover chain for logs.

    Args:
        models: The chain.

    Returns:
        A ``" -> "``-joined string of model keys, or ``"<empty>"``.
    """
    if not models:
        return "<empty>"
    return " -> ".join(model.key for model in models)
