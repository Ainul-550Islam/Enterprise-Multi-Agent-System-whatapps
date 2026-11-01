"""Resilience tests for retry, backoff and derived budgets.

A retry loop is the one place where a system can amplify its own failure: a
dependency that is struggling gets more traffic, not less. These tests pin the
bounds that stop that — and the two rules that matter more than the bounds:

* a non-idempotent operation is never retried, however retryable the error is;
* a retry budget derived from a deadline always leaves time for the last
  attempt to actually run.
"""

from __future__ import annotations

import asyncio

import pytest

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ExecutionTimeoutError,
    InvalidRequestError,
    ModelRateLimitedError,
)
from mas_orchestrator.models.retry import (
    MAXIMUM_HONOURED_RETRY_AFTER_SECONDS,
    MINIMUM_SLEEP_SECONDS,
    RetryPolicy,
    budget_from_deadline,
)


def _policy(**overrides: float | int | bool) -> RetryPolicy:
    defaults: dict[str, object] = {
        "max_attempts": 3,
        "budget_seconds": 30.0,
        "backoff_base_seconds": 0.01,
        "backoff_max_seconds": 0.02,
    }
    defaults.update(overrides)
    return RetryPolicy(**defaults)  # type: ignore[arg-type]


class TestPolicyValidation:
    def test_zero_attempts_is_refused(self) -> None:
        # A policy allowing zero attempts would fail every operation; better to
        # surface that at construction.
        with pytest.raises(InvalidRequestError):
            RetryPolicy(max_attempts=0)

    def test_negative_budget_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            RetryPolicy(budget_seconds=-1.0)

    def test_non_positive_base_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            RetryPolicy(backoff_base_seconds=0)

    def test_max_below_base_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            RetryPolicy(backoff_base_seconds=2.0, backoff_max_seconds=1.0)


class TestSleepDuration:
    def test_sleep_is_always_at_least_the_minimum(self) -> None:
        # A zero-length sleep between attempts is a busy loop.
        assert _policy().sleep_duration(1) >= MINIMUM_SLEEP_SECONDS

    def test_sleep_never_exceeds_the_ceiling(self) -> None:
        policy = _policy(backoff_max_seconds=0.05)
        for attempt in range(1, 6):
            assert policy.sleep_duration(attempt) <= 0.05

    def test_a_hostile_retry_after_is_capped(self) -> None:
        # An untrusted peer suggestion must never become an unbounded sleep.
        policy = _policy(backoff_max_seconds=0.05)
        assert policy.sleep_duration(1, retry_after=9999.0) <= 0.05

    def test_honour_flag_can_be_disabled(self) -> None:
        # When disabled the provider hint is ignored entirely rather than
        # partially trusted.
        policy = _policy(backoff_max_seconds=0.05, honour_retry_after=False)
        assert policy.sleep_duration(1, retry_after=9999.0) <= 0.05
        assert MAXIMUM_HONOURED_RETRY_AFTER_SECONDS > 0


class TestExecute:
    async def test_success_on_the_first_attempt(self) -> None:
        calls = 0

        async def call() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        assert await _policy().execute("op", call) == "ok"
        assert calls == 1

    async def test_a_retryable_failure_is_retried(self) -> None:
        calls = 0

        async def call() -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise ModelRateLimitedError("slow down")
            return "ok"

        assert await _policy(max_attempts=5).execute("op", call) == "ok"
        assert calls == 3

    async def test_attempts_are_bounded(self) -> None:
        calls = 0

        async def call() -> str:
            nonlocal calls
            calls += 1
            raise ModelRateLimitedError("slow down")

        with pytest.raises(ModelRateLimitedError):
            await _policy(max_attempts=3).execute("op", call)
        assert calls == 3

    async def test_a_non_retryable_error_stops_immediately(self) -> None:
        calls = 0

        async def call() -> str:
            nonlocal calls
            calls += 1
            raise InvalidRequestError("bad input")

        with pytest.raises(InvalidRequestError):
            await _policy(max_attempts=5).execute("op", call)
        assert calls == 1

    async def test_a_non_idempotent_operation_is_never_retried(self) -> None:
        # The most important rule here: a retryable error does not make a
        # duplicate side effect acceptable.
        calls = 0

        async def call() -> str:
            nonlocal calls
            calls += 1
            raise ModelRateLimitedError("slow down")

        with pytest.raises(ModelRateLimitedError):
            await _policy(max_attempts=5).execute("op", call, idempotent=False)
        assert calls == 1

    async def test_budget_exhaustion_is_a_timeout_not_the_original_error(self) -> None:
        async def call() -> str:
            raise ModelRateLimitedError("slow down")

        with pytest.raises(ExecutionTimeoutError, match="budget exhausted"):
            await _policy(max_attempts=50, budget_seconds=0.0).execute("op", call)

    async def test_cancellation_during_backoff_is_observed(self) -> None:
        # Sleeping through a cancellation would hold the worker and delay
        # shutdown.
        cancel = asyncio.Event()
        cancel.set()

        async def call() -> str:
            raise ModelRateLimitedError("slow down")

        with pytest.raises(CancellationRequestedError):
            await _policy().execute("op", call, cancel_event=cancel)

    async def test_cancellation_of_the_task_is_never_retried(self) -> None:
        calls = 0

        async def call() -> str:
            nonlocal calls
            calls += 1
            raise asyncio.CancelledError

        with pytest.raises(CancellationRequestedError):
            await _policy(max_attempts=5).execute("op", call)
        assert calls == 1


class TestBudgetFromDeadline:
    def test_reserves_time_for_the_last_attempt(self) -> None:
        # A budget sized independently of the deadline is how retries end up
        # consuming an execution's entire allowance.
        assert budget_from_deadline(100.0, reserve_ratio=0.25) == pytest.approx(75.0)

    def test_less_than_a_second_leaves_no_budget(self) -> None:
        # Retrying at that point cannot help.
        assert budget_from_deadline(0.5) == 0.0

    def test_unknown_deadline_derives_no_retry_budget(self) -> None:
        # Deliberately conservative: a retry amplifies traffic to a dependency
        # that is already struggling, so it needs a known allowance to spend
        # rather than a default. A caller that wants retries without a deadline
        # must set an explicit budget.
        assert budget_from_deadline(None) == 0.0

    def test_under_a_second_derives_no_budget(self) -> None:
        assert budget_from_deadline(0.9) == 0.0

    def test_a_ratio_of_one_is_refused(self) -> None:
        # It would leave nothing for the attempt itself.
        with pytest.raises(InvalidRequestError):
            budget_from_deadline(10.0, reserve_ratio=1.0)

    def test_a_negative_ratio_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            budget_from_deadline(10.0, reserve_ratio=-0.1)

    def test_zero_ratio_spends_everything(self) -> None:
        assert budget_from_deadline(10.0, reserve_ratio=0.0) == pytest.approx(10.0)
