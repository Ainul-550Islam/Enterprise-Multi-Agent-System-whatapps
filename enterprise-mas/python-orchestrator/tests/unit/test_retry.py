"""Unit tests for retry classification, backoff and budgets.

The dangerous failure mode is a retry loop that amplifies an outage, so these
tests focus on the bounds: what must NOT be retried, and when waiting must stop.
"""

from __future__ import annotations

import asyncio

import pytest

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ConfigurationError,
    ExecutionTimeoutError,
    InvalidRequestError,
    ModelRateLimitedError,
    ModelTimeoutError,
    QuotaExceededError,
)
from mas_orchestrator.models.retry import RetryPolicy, budget_from_deadline

# A policy whose backoff is deterministic enough to assert on, and fast.
FAST = RetryPolicy(
    max_attempts=3,
    budget_seconds=30.0,
    backoff_base_seconds=0.001,
    backoff_max_seconds=0.01,
)


class TestPolicyValidation:
    """A nonsensical policy must be refused at construction."""

    def test_zero_attempts_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            RetryPolicy(max_attempts=0)

    def test_negative_budget_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            RetryPolicy(budget_seconds=-1)

    def test_non_positive_backoff_base_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            RetryPolicy(backoff_base_seconds=0)

    def test_max_smaller_than_base_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            RetryPolicy(backoff_base_seconds=5.0, backoff_max_seconds=1.0)


class TestSleepDuration:
    """Backoff and jitter."""

    def test_backoff_grows_exponentially(self) -> None:
        policy = RetryPolicy(backoff_base_seconds=1.0, backoff_max_seconds=100.0)
        # Full jitter means these are upper bounds, not exact values.
        for attempt in range(1, 6):
            assert policy.sleep_duration(attempt) <= min(100.0, 2.0 ** (attempt - 1))

    def test_duration_is_capped(self) -> None:
        policy = RetryPolicy(backoff_base_seconds=1.0, backoff_max_seconds=2.0)
        assert policy.sleep_duration(20) <= 2.0

    def test_duration_is_never_zero(self) -> None:
        """A zero sleep turns a retry loop into a busy loop."""
        policy = RetryPolicy(backoff_base_seconds=0.0001, backoff_max_seconds=0.0001)
        assert policy.sleep_duration(1) > 0

    def test_jitter_produces_variation(self) -> None:
        policy = RetryPolicy(backoff_base_seconds=1.0, backoff_max_seconds=1.0)
        samples = {policy.sleep_duration(1) for _ in range(50)}
        assert len(samples) > 1

    def test_retry_after_is_added(self) -> None:
        policy = RetryPolicy(backoff_base_seconds=0.1, backoff_max_seconds=10.0)
        with_hint = policy.sleep_duration(1, retry_after=2.0)
        assert with_hint >= 2.0

    def test_retry_after_is_capped(self) -> None:
        """A hostile peer must not be able to park us indefinitely."""
        policy = RetryPolicy(backoff_base_seconds=0.1, backoff_max_seconds=1.0)
        assert policy.sleep_duration(1, retry_after=9999.0) <= 1.0

    def test_retry_after_can_be_ignored(self) -> None:
        policy = RetryPolicy(
            backoff_base_seconds=0.1, backoff_max_seconds=1.0, honour_retry_after=False
        )
        assert policy.sleep_duration(1, retry_after=9999.0) <= 1.0


class TestExecute:
    """The retry loop itself."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self) -> None:
        calls = 0

        async def succeed() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        assert await FAST.execute("op", succeed) == "ok"
        assert calls == 1

    @pytest.mark.asyncio
    async def test_retryable_failure_is_retried(self) -> None:
        calls = 0

        async def flaky() -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise ModelTimeoutError("slow")
            return "ok"

        assert await FAST.execute("op", flaky) == "ok"
        assert calls == 3

    @pytest.mark.asyncio
    async def test_non_retryable_failure_is_not_retried(self) -> None:
        calls = 0

        async def always_bad() -> str:
            nonlocal calls
            calls += 1
            raise QuotaExceededError("over budget")

        with pytest.raises(QuotaExceededError):
            await FAST.execute("op", always_bad)
        assert calls == 1

    @pytest.mark.asyncio
    async def test_attempts_are_bounded(self) -> None:
        calls = 0

        async def always_timeout() -> str:
            nonlocal calls
            calls += 1
            raise ModelTimeoutError("slow")

        with pytest.raises(ModelTimeoutError):
            await FAST.execute("op", always_timeout)
        assert calls == FAST.max_attempts

    @pytest.mark.asyncio
    async def test_non_idempotent_operation_runs_once(self) -> None:
        """Retrying a non-idempotent action can duplicate a durable effect."""
        calls = 0

        async def non_idempotent() -> str:
            nonlocal calls
            calls += 1
            raise ModelTimeoutError("slow")

        with pytest.raises(ModelTimeoutError):
            await FAST.execute("op", non_idempotent, idempotent=False)
        assert calls == 1

    @pytest.mark.asyncio
    async def test_budget_exhaustion_raises_execution_timeout(self) -> None:
        """Exhausting the *budget* is distinct from exhausting attempts."""
        policy = RetryPolicy(
            max_attempts=100,
            budget_seconds=0.02,
            backoff_base_seconds=0.01,
            backoff_max_seconds=0.02,
        )

        async def always_fail() -> str:
            raise ModelTimeoutError("slow")

        with pytest.raises(ExecutionTimeoutError):
            await policy.execute("op", always_fail)

    @pytest.mark.asyncio
    async def test_cancellation_is_not_retried(self) -> None:
        calls = 0

        async def cancelled() -> str:
            nonlocal calls
            calls += 1
            raise asyncio.CancelledError()

        with pytest.raises(CancellationRequestedError):
            await FAST.execute("op", cancelled)
        assert calls == 1

    @pytest.mark.asyncio
    async def test_pre_set_cancel_event_stops_the_wait(self) -> None:
        event = asyncio.Event()
        event.set()
        with pytest.raises(CancellationRequestedError):
            await FAST.sleep(1, cancel_event=event)

    @pytest.mark.asyncio
    async def test_cancel_event_during_sleep(self) -> None:
        event = asyncio.Event()

        async def cancel_soon() -> None:
            await asyncio.sleep(0.005)
            event.set()

        async def sleep_forever() -> str:
            await FAST.sleep(1, cancel_event=event)
            raise AssertionError("sleep should have been cancelled")

        task = asyncio.create_task(cancel_soon())
        with pytest.raises(CancellationRequestedError):
            await sleep_forever()
        await task

    @pytest.mark.asyncio
    async def test_retry_after_from_error_is_honoured(self) -> None:
        policy = RetryPolicy(
            max_attempts=2,
            budget_seconds=10.0,
            backoff_base_seconds=0.001,
            backoff_max_seconds=0.05,
        )
        calls = 0

        async def rate_limited() -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ModelRateLimitedError("slow down", retry_after_seconds=0.02)
            return "ok"

        assert await policy.execute("op", rate_limited) == "ok"


class TestBudgetFromDeadline:
    """Budget derivation must reserve time for the final attempt."""

    def test_reserves_a_fraction(self) -> None:
        assert budget_from_deadline(10.0, reserve_ratio=0.25) == 7.5

    def test_unbounded_returns_zero(self) -> None:
        assert budget_from_deadline(None) == 0.0

    def test_less_than_a_second_returns_zero(self) -> None:
        """Retrying with no time left cannot help."""
        assert budget_from_deadline(0.5) == 0.0

    def test_zero_ratio_is_allowed(self) -> None:
        assert budget_from_deadline(10.0, reserve_ratio=0.0) == 10.0

    def test_ratio_of_one_is_refused(self) -> None:
        """A ratio of 1 would leave nothing for the attempt."""
        with pytest.raises(InvalidRequestError):
            budget_from_deadline(10.0, reserve_ratio=1.0)

    def test_negative_ratio_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            budget_from_deadline(10.0, reserve_ratio=-0.1)


class TestClassificationIntegration:
    """The retry layer must agree with the domain retryability table."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error", "should_retry"),
        [
            (ModelTimeoutError("x"), True),
            (ModelRateLimitedError("x"), True),
            (ConfigurationError("x"), False),
            (QuotaExceededError("x"), False),
            (InvalidRequestError("x"), False),
        ],
    )
    async def test_only_retryable_errors_retry(self, error: Exception, should_retry: bool) -> None:
        calls = 0

        async def fail() -> str:
            nonlocal calls
            calls += 1
            raise error

        with pytest.raises(type(error)):
            await FAST.execute("op", fail)

        assert calls == (FAST.max_attempts if should_retry else 1)

    def test_unknown_exception_is_never_retried(self) -> None:
        """Guessing at arbitrary exceptions is forbidden."""
        from mas_orchestrator.contracts.errors import is_retryable

        assert is_retryable(RuntimeError("boom")) is False
