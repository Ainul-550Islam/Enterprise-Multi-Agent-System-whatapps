"""Retry classification, backoff, jitter and retry budgets.

Retries are where a well-behaved service becomes an outage amplifier: an
unbounded retry loop turns a slow dependency into a self-inflicted denial of
service. This module implements the spec's rules (§15) as hard limits rather
than guidelines:

* **Only retryable errors are retried.** Classification comes from
  :data:`mas_orchestrator.contracts.errors.RETRYABLE_CODES`; nothing here
  inspects exception text to guess.
* **Exponential backoff with full jitter.** Full jitter (uniform over the
  whole interval) is used because it spreads simultaneous retries best and is
  simple to reason about.
* **Bounded attempts *and* a bounded total budget.** Attempts alone are not
  enough: a dependency that is slow rather than failing would let each attempt
  consume its full timeout. The budget caps cumulative *waiting* time.
* **Cancellation-aware waiting.** :meth:`RetryPolicy.sleep` aborts the moment
  cancellation is observed. Continuing to sleep after cancellation would hold
  a worker during shutdown.
* **Provider ``Retry-After`` is honoured but capped.** A hostile or broken peer
  must not be able to park us indefinitely.
* **Non-idempotent actions are never retried.** :meth:`RetryPolicy.execute`
  takes an explicit ``idempotent`` flag; when ``False`` the operation runs
  exactly once regardless of the error's retryability, because retrying a
  non-idempotent tool call can duplicate a durable side effect.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypeVar

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ExecutionTimeoutError,
    InvalidRequestError,
    is_retryable,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_LOGGER: Final = logging.getLogger(__name__)

T = TypeVar("T")

__all__ = [
    "RetryOutcome",
    "RetryPolicy",
    "budget_from_deadline",
]

from typing import Final  # noqa: E402  (grouped after __all__ for readability)

#: Smallest sleep we will perform. Below this, sleeping is pure overhead and
#: the failure is almost certainly persistent within the retry window.
MINIMUM_SLEEP_SECONDS: Final[float] = 0.01

#: Largest ``Retry-After`` we will honour, so a peer cannot park us forever.
MAXIMUM_HONOURED_RETRY_AFTER_SECONDS: Final[float] = 30.0


@dataclass(frozen=True, slots=True)
class RetryOutcome:
    """A record of what a retry sequence actually did.

    Returned alongside failures so callers can log and meter retry behaviour
    without reconstructing it from scattered log lines.

    Attributes:
        attempts: Number of attempts made, including the first.
        waited_seconds: Cumulative time spent sleeping between attempts.
        last_error: The final error, if the sequence did not succeed.
        gave_up_because: Why the sequence stopped — ``"success"``,
            ``"attempts_exhausted"``, ``"budget_exhausted"``,
            ``"not_retryable"``, ``"cancelled"``, or ``"non_idempotent"``.
    """

    attempts: int
    waited_seconds: float
    last_error: BaseException | None = None
    gave_up_because: str = "success"

    @property
    def succeeded(self) -> bool:
        """Whether the operation ultimately succeeded."""
        return self.gave_up_because == "success"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded retry policy for one logical operation.

    Attributes:
        max_attempts: Total attempts allowed, including the first.
        budget_seconds: Cumulative ceiling on time spent *waiting* between
            attempts. Not a ceiling on total wall-clock time: call durations
            are bounded separately by each operation's own deadline.
        backoff_base_seconds: Base interval for exponential backoff.
        backoff_max_seconds: Ceiling for any single sleep.
        honour_retry_after: Whether to honour a provider-suggested delay.
    """

    max_attempts: int = 3
    budget_seconds: float = 30.0
    backoff_base_seconds: float = 0.5
    backoff_max_seconds: float = 8.0
    honour_retry_after: bool = True

    def __post_init__(self) -> None:
        """Validates the policy.

        Raises:
            InvalidRequestError: If any bound is nonsensical. A policy that
                allows zero attempts would make every operation fail, and a
                negative budget would never permit a retry — both are caller
                errors worth surfacing immediately.
        """
        if self.max_attempts < 1:
            raise InvalidRequestError(
                "max_attempts must be at least 1",
                details={"max_attempts": self.max_attempts},
            )
        if self.budget_seconds < 0:
            raise InvalidRequestError(
                "budget_seconds must not be negative",
                details={"budget_seconds": self.budget_seconds},
            )
        if self.backoff_base_seconds <= 0:
            raise InvalidRequestError(
                "backoff_base_seconds must be positive",
                details={"backoff_base_seconds": self.backoff_base_seconds},
            )
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise InvalidRequestError(
                "backoff_max_seconds must be at least backoff_base_seconds",
                details={
                    "backoff_max_seconds": self.backoff_max_seconds,
                    "backoff_base_seconds": self.backoff_base_seconds,
                },
            )

    def sleep_duration(self, attempt: int, *, retry_after: float | None = None) -> float:
        """Computes how long to wait before attempt *attempt* + 1.

        Args:
            attempt: The attempt number that just failed (1-based).
            retry_after: Provider-suggested delay in seconds, if any.

        Returns:
            Seconds to sleep. Exponential backoff with full jitter, plus the
            provider hint when honoured, capped at ``backoff_max_seconds``.
        """
        ceiling = min(
            self.backoff_max_seconds,
            self.backoff_base_seconds * (2 ** max(0, attempt - 1)),
        )
        delay = random.uniform(0.0, ceiling)  # noqa: S311 - jitter, not a secret

        if self.honour_retry_after and retry_after is not None and retry_after > 0:
            # Cap the hint: an untrusted peer suggestion must never become an
            # unbounded sleep.
            bounded_hint = min(retry_after, MAXIMUM_HONOURED_RETRY_AFTER_SECONDS)
            delay = min(delay + bounded_hint, self.backoff_max_seconds)

        return max(MINIMUM_SLEEP_SECONDS, min(delay, self.backoff_max_seconds))

    async def sleep(
        self,
        attempt: int,
        *,
        retry_after: float | None = None,
        cancel_event: asyncio.Event | None = None,
    ) -> float:
        """Sleeps before the next attempt, aborting on cancellation.

        Args:
            attempt: The attempt number that just failed (1-based).
            retry_after: Provider-suggested delay.
            cancel_event: When set, the sleep is abandoned immediately.

        Returns:
            The number of seconds actually slept.

        Raises:
            CancellationRequestedError: If cancellation was observed before or
                during the sleep. Sleeping through a cancellation would hold
                the worker and delay shutdown.
        """
        if cancel_event is not None and cancel_event.is_set():
            raise CancellationRequestedError("cancelled during retry backoff")

        duration = self.sleep_duration(attempt, retry_after=retry_after)

        try:
            if cancel_event is None:
                await asyncio.sleep(duration)
            else:
                await asyncio.wait_for(cancel_event.wait(), timeout=duration)
                # The event fired: cancellation, not a completed sleep.
                raise CancellationRequestedError("cancelled during retry backoff")
        except TimeoutError:
            # wait_for timed out, which is the normal case: we slept.
            pass
        except asyncio.CancelledError as error:
            raise CancellationRequestedError("cancelled during retry backoff") from error

        return duration

    async def execute(
        self,
        operation: str,
        call: Callable[[], Awaitable[T]],
        *,
        idempotent: bool = True,
        cancel_event: asyncio.Event | None = None,
    ) -> T:
        """Runs *call* under this policy.

        Args:
            operation: Name used in log lines and error details.
            call: A zero-argument coroutine function performing the attempt.
            idempotent: Whether repeating the operation is safe. When
                ``False``, the operation runs exactly once even if the error
                is retryable, because a duplicate could create a second
                durable side effect.
            cancel_event: Cancellation signal observed between attempts.

        Returns:
            The successful result.

        Raises:
            CancellationRequestedError: If cancellation was observed.
            ExecutionTimeoutError: If the retry *budget* was exhausted. This
                is raised with the last error chained, so the original cause
                is never lost.
            OrchestratorError: The last error, when the failure was not
                retryable or attempts ran out.
        """
        waited = 0.0
        last_error: BaseException | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                result = await call()
                _LOGGER.debug(
                    "operation succeeded",
                    extra={"operation": operation, "attempt": attempt},
                )
                return result
            except asyncio.CancelledError as error:
                # Never retry, never wrap: the caller's task is being cancelled.
                raise CancellationRequestedError(f"{operation} was cancelled") from error
            except BaseException as error:
                last_error = error
                retryable = is_retryable(error)

                if not idempotent:
                    _LOGGER.info(
                        "not retrying a non-idempotent operation",
                        extra={"operation": operation, "attempt": attempt},
                    )
                    raise
                if not retryable:
                    raise
                if attempt >= self.max_attempts:
                    raise
                if waited >= self.budget_seconds:
                    raise ExecutionTimeoutError(
                        f"retry budget exhausted for {operation}",
                        details={
                            "operation": operation,
                            "attempts": attempt,
                            "waited_seconds": round(waited, 3),
                            "budget_seconds": self.budget_seconds,
                        },
                    ) from error

                suggested = getattr(error, "retry_after_seconds", None)
                if not isinstance(suggested, int | float):
                    suggested = None

                slept = await self.sleep(attempt, retry_after=suggested, cancel_event=cancel_event)
                waited += slept
                _LOGGER.warning(
                    "retrying operation",
                    extra={
                        "operation": operation,
                        "attempt": attempt,
                        "slept_seconds": round(slept, 3),
                        "error": type(error).__name__,
                    },
                )

        # Unreachable: every branch above either returns or raises.
        raise (
            last_error
            if last_error is not None
            else ExecutionTimeoutError(
                f"{operation} made no attempt", details={"operation": operation}
            )
        )


def budget_from_deadline(
    remaining_seconds: float | None,
    *,
    reserve_ratio: float = 0.25,
) -> float:
    """Derives a retry budget from an execution's remaining time.

    A retry budget sized independently of the execution deadline is how
    retries end up consuming an execution's entire allowance. This reserves a
    fraction of what is left for the final attempt itself, so a retry loop can
    never leave the operation with no time to actually run.

    Args:
        remaining_seconds: Time left on the execution, or ``None`` when
            unbounded.
        reserve_ratio: Fraction of the remaining time reserved for the last
            attempt.

    Returns:
        The budget in seconds. ``0.0`` when less than a second remains, since
        retrying at that point cannot help.

    Raises:
        InvalidRequestError: If ``reserve_ratio`` is outside ``[0, 1)``. A
            ratio of 1 would leave nothing for the attempt; anything above it
            would produce a negative budget.
    """
    if not 0.0 <= reserve_ratio < 1.0:
        raise InvalidRequestError(
            "reserve_ratio must be in [0, 1)",
            details={"reserve_ratio": reserve_ratio},
        )

    if remaining_seconds is None:
        # No deadline means no derived budget, and therefore no retries. That
        # is the conservative direction: a retry amplifies traffic to a
        # dependency that is already struggling, so it needs a reason — a known
        # allowance to spend — rather than a default. A caller that wants
        # retries without a deadline must say so with an explicit
        # :class:`RetryPolicy` budget.
        return 0.0
    if remaining_seconds <= 1.0:
        return 0.0

    return max(0.0, remaining_seconds * (1.0 - reserve_ratio))
