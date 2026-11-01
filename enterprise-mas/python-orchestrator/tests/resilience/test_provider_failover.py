"""Resilience of provider failover.

Spec §8 requires fallback to be *bounded* and policy-respecting, and
:mod:`mas_orchestrator.models.failover` is where those guarantees live. Four
properties make failover safe rather than merely available, and each is
pinned here:

1. **Only retryable failures fall back.** A rate limit is worth trying
   elsewhere; an authentication rejection is not — the second model would
   reject the same credential, and the delay would only postpone the error
   reaching the caller.
2. **The chain is bounded.** Exhausting it is a reported outcome, never an
   infinite walk.
3. **Cancellation short-circuits everything.** Once cancellation is observed,
   no further model is attempted, even with attempts remaining.
4. **The original cause survives.** When the chain is exhausted, the *first*
   error is raised with the whole per-model history attached. Raising the last
   error would blame whichever model happened to be tried last.

These tests use real typed errors, so the retryability classification under
test is the production one — not a test-local notion of what counts as
retryable.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from mas_orchestrator.contracts.errors import (
    AuthenticationError,
    CancellationRequestedError,
    InvalidRequestError,
    ModelNotAvailableError,
    ModelProviderError,
    ModelRateLimitedError,
    ModelTimeoutError,
    OrchestratorError,
)
from mas_orchestrator.models.failover import (
    FailoverAttempt,
    FailoverResult,
    describe_chain,
    execute_with_failover,
)
from mas_orchestrator.models.types import ModelRef

PRIMARY: ModelRef = ModelRef(provider="openai", model="gpt-4o")
SECONDARY: ModelRef = ModelRef(provider="anthropic", model="claude-3-5-sonnet")
TERTIARY: ModelRef = ModelRef(provider="openai", model="gpt-4o-mini")

CHAIN: tuple[ModelRef, ...] = (PRIMARY, SECONDARY, TERTIARY)


# --- doubles -----------------------------------------------------------------


@dataclass(slots=True)
class _ScriptedProvider:
    """Answers each model with a scripted outcome.

    The script maps a model key to either a value or an exception, so a test
    states the whole scenario in one place instead of scattering closures.
    """

    outcomes: dict[str, Any]
    calls: list[str] = field(default_factory=list)

    async def __call__(self, model: ModelRef) -> Any:
        """Returns the scripted value, or raises the scripted error."""
        self.calls.append(model.key)
        outcome = self.outcomes[model.key]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _script(**outcomes: Any) -> _ScriptedProvider:
    """Builds a scripted provider from ``model_key=outcome`` pairs."""
    return _ScriptedProvider(outcomes=dict(outcomes))


def _all_failing(error: BaseException) -> _ScriptedProvider:
    """Builds a provider that fails every model with *error*."""
    return _script(**{model.key: error for model in CHAIN})


async def _call(provider: _ScriptedProvider, model: ModelRef) -> object:
    """Adapter matching the ``call`` signature."""
    return await provider(model)


# --- success -----------------------------------------------------------------


async def test_first_model_success_uses_no_fallback() -> None:
    provider = _script(**{PRIMARY.key: "ok", SECONDARY.key: "unused"})

    result = await execute_with_failover(CHAIN, provider)

    assert result.succeeded is True
    assert result.value == "ok"
    assert result.model == PRIMARY
    assert provider.calls == [PRIMARY.key]


async def test_success_is_reported_not_raised() -> None:
    result = await execute_with_failover(CHAIN, _script(**{PRIMARY.key: 42}))

    assert isinstance(result, FailoverResult)
    assert result.exhausted is False
    assert result.value == 42


async def test_attempt_history_records_the_success() -> None:
    result = await execute_with_failover(CHAIN, _script(**{PRIMARY.key: "ok"}))

    assert len(result.attempts) == 1
    attempt = result.attempts[0]
    assert isinstance(attempt, FailoverAttempt)
    assert attempt.model == PRIMARY
    assert attempt.succeeded is True
    assert attempt.error_code is None


# --- retryable failure triggers fallback -------------------------------------


async def test_rate_limit_falls_over_to_the_next_model() -> None:
    provider = _script(
        **{
            PRIMARY.key: ModelRateLimitedError("rate limited"),
            SECONDARY.key: "recovered",
        }
    )

    result = await execute_with_failover(CHAIN, provider)

    assert result.succeeded is True
    assert result.value == "recovered"
    assert result.model == SECONDARY
    assert provider.calls == [PRIMARY.key, SECONDARY.key]


async def test_timeout_falls_over() -> None:
    provider = _script(
        **{
            PRIMARY.key: ModelTimeoutError("upstream timed out"),
            SECONDARY.key: "ok",
        }
    )

    result = await execute_with_failover(CHAIN, provider)

    assert result.model == SECONDARY


async def test_provider_error_falls_over() -> None:
    provider = _script(
        **{
            PRIMARY.key: ModelProviderError("upstream 503"),
            SECONDARY.key: "ok",
        }
    )

    result = await execute_with_failover(CHAIN, provider)

    assert result.model == SECONDARY


async def test_fallback_walks_the_whole_chain() -> None:
    provider = _script(
        **{
            PRIMARY.key: ModelRateLimitedError("limited"),
            SECONDARY.key: ModelTimeoutError("slow"),
            TERTIARY.key: "eventually",
        }
    )

    result = await execute_with_failover(CHAIN, provider)

    assert result.value == "eventually"
    assert result.model == TERTIARY
    assert provider.calls == [PRIMARY.key, SECONDARY.key, TERTIARY.key]


async def test_failed_attempts_are_recorded_with_their_codes() -> None:
    provider = _script(
        **{
            PRIMARY.key: ModelRateLimitedError("limited"),
            SECONDARY.key: "ok",
        }
    )

    result = await execute_with_failover(CHAIN, provider)

    assert len(result.attempts) == 2
    first, second = result.attempts
    assert first.model == PRIMARY
    assert first.succeeded is False
    assert first.retryable is True
    assert first.error_code == "model_rate_limited"
    assert second.succeeded is True


# --- non-retryable failure stops the chain -----------------------------------


async def test_authentication_failure_does_not_fall_over() -> None:
    # The second model would reject the same credential. Falling back would
    # spend a request to learn the same thing, and would delay the error
    # reaching whoever can rotate the key.
    provider = _script(
        **{
            PRIMARY.key: AuthenticationError("bad api key"),
            SECONDARY.key: "never reached",
        }
    )

    with pytest.raises(AuthenticationError):
        await execute_with_failover(CHAIN, provider)

    assert provider.calls == [PRIMARY.key]


async def test_invalid_request_does_not_fall_over() -> None:
    provider = _script(
        **{
            PRIMARY.key: InvalidRequestError("context length exceeded"),
            SECONDARY.key: "never reached",
        }
    )

    with pytest.raises(InvalidRequestError):
        await execute_with_failover(CHAIN, provider)

    assert provider.calls == [PRIMARY.key]


async def test_non_retryable_after_a_retryable_one_stops_the_chain() -> None:
    # The terminal error is raised, not the first one. "Rate limited" would
    # invite a retry that fails the same way; "authentication failed" names
    # what actually has to be fixed.
    provider = _script(
        **{
            PRIMARY.key: ModelRateLimitedError("limited"),
            SECONDARY.key: AuthenticationError("bad key"),
            TERTIARY.key: "never reached",
        }
    )

    with pytest.raises(AuthenticationError, match="bad key"):
        await execute_with_failover(CHAIN, provider)

    assert provider.calls == [PRIMARY.key, SECONDARY.key]


async def test_terminal_error_keeps_the_full_history() -> None:
    # Choosing the terminal cause must not discard the earlier failures; the
    # whole walk stays attached to whichever error is raised.
    provider = _script(
        **{
            PRIMARY.key: ModelTimeoutError("slow"),
            SECONDARY.key: InvalidRequestError("context length exceeded"),
        }
    )

    with pytest.raises(InvalidRequestError) as caught:
        await execute_with_failover(CHAIN, provider)

    history = caught.value.details["failover_attempts"]
    assert [entry["model"] for entry in history] == [PRIMARY.key, SECONDARY.key]
    assert history[0]["retryable"] is True
    assert history[1]["retryable"] is False


async def test_all_retryable_exhaustion_still_raises_the_first() -> None:
    # No terminal cause exists here, so the first failure remains the most
    # informative: it is what started the failover.
    provider = _script(
        **{
            PRIMARY.key: ModelTimeoutError("first"),
            SECONDARY.key: ModelRateLimitedError("second"),
        }
    )

    with pytest.raises(ModelTimeoutError, match="first"):
        await execute_with_failover((PRIMARY, SECONDARY), provider)


async def test_untyped_terminal_error_is_raised_over_an_earlier_typed_one() -> None:
    # A bare exception stops the walk, so it is the cause worth reporting.
    provider = _script(
        **{
            PRIMARY.key: ModelRateLimitedError("limited"),
            SECONDARY.key: ValueError("unexpected shape"),
        }
    )

    with pytest.raises(ValueError, match="unexpected shape"):
        await execute_with_failover(CHAIN, provider)


async def test_untyped_exception_is_not_retryable() -> None:
    # A bare exception carries no classification, so it must not be guessed
    # into a retryable bucket: it stops the chain and propagates unchanged.
    provider = _script(
        **{
            PRIMARY.key: ValueError("unexpected shape"),
            SECONDARY.key: "never reached",
        }
    )

    with pytest.raises(ValueError, match="unexpected shape"):
        await execute_with_failover(CHAIN, provider)

    assert provider.calls == [PRIMARY.key]


# --- exhaustion --------------------------------------------------------------


async def test_exhausted_chain_raises_the_first_error() -> None:
    # The first failure is the most informative: the later ones may be
    # different symptoms of the same outage.
    first = ModelRateLimitedError("first failure")
    provider = _script(
        **{
            PRIMARY.key: first,
            SECONDARY.key: ModelTimeoutError("second failure"),
            TERTIARY.key: ModelProviderError("third failure"),
        }
    )

    with pytest.raises(ModelRateLimitedError, match="first failure"):
        await execute_with_failover(CHAIN, provider)

    assert provider.calls == [PRIMARY.key, SECONDARY.key, TERTIARY.key]


async def test_exhaustion_attaches_the_full_history() -> None:
    provider = _all_failing(ModelTimeoutError("down"))

    with pytest.raises(OrchestratorError) as caught:
        await execute_with_failover(CHAIN, provider, operation="complete")

    history = caught.value.details["failover_attempts"]
    assert [entry["model"] for entry in history] == [
        PRIMARY.key,
        SECONDARY.key,
        TERTIARY.key,
    ]
    assert all(entry["retryable"] is True for entry in history)
    assert caught.value.details["operation"] == "complete"


async def test_single_model_chain_raises_its_own_failure() -> None:
    provider = _script(**{PRIMARY.key: ModelTimeoutError("only model down")})

    with pytest.raises(ModelTimeoutError, match="only model down"):
        await execute_with_failover((PRIMARY,), provider)

    assert provider.calls == [PRIMARY.key]


async def test_empty_chain_is_refused() -> None:
    # Silently succeeding with no result would be worse than failing: the
    # caller would act on a completion that never happened.
    provider = _script()

    with pytest.raises(ModelNotAvailableError, match="failover chain is empty"):
        await execute_with_failover((), provider)

    assert provider.calls == []


async def test_exhaustion_history_is_present_even_for_one_model() -> None:
    provider = _script(**{PRIMARY.key: ModelTimeoutError("down")})

    with pytest.raises(OrchestratorError) as caught:
        await execute_with_failover((PRIMARY,), provider)

    assert len(caught.value.details["failover_attempts"]) == 1


# --- cancellation ------------------------------------------------------------


async def test_cancellation_before_the_first_attempt_stops_everything() -> None:
    cancel_event = asyncio.Event()
    cancel_event.set()
    provider = _script(**{PRIMARY.key: "ok"})

    with pytest.raises(CancellationRequestedError):
        await execute_with_failover(CHAIN, provider, cancel_event=cancel_event)

    assert provider.calls == []


async def test_cancellation_between_attempts_stops_the_walk() -> None:
    cancel_event = asyncio.Event()

    async def fail_then_cancel(model: ModelRef) -> object:
        if model is PRIMARY:
            cancel_event.set()
            raise ModelRateLimitedError("limited")
        raise AssertionError("must not be reached after cancellation")

    with pytest.raises(CancellationRequestedError):
        await execute_with_failover(CHAIN, fail_then_cancel, cancel_event=cancel_event)


async def test_asyncio_cancellation_is_converted() -> None:
    # A raw asyncio cancellation must not escape as-is; it is translated so the
    # caller sees the orchestrator's own cancellation type.
    async def cancelled(model: ModelRef) -> object:
        raise asyncio.CancelledError

    with pytest.raises(CancellationRequestedError):
        await execute_with_failover(CHAIN, cancelled)


async def test_cancellation_during_success_still_propagates() -> None:
    async def cancelled(model: ModelRef) -> object:
        raise asyncio.CancelledError

    with pytest.raises(CancellationRequestedError, match="cancelled during"):
        await execute_with_failover((PRIMARY,), cancelled)


# --- bounded chain -----------------------------------------------------------


async def test_chain_bound_is_enforced_by_the_chain_itself() -> None:
    # Failover honours exactly the candidates it is given. Capping the list is
    # the router's job; this asserts failover never walks past its input.
    provider = _all_failing(ModelRateLimitedError("limited"))

    with pytest.raises(ModelRateLimitedError):
        await execute_with_failover((PRIMARY, SECONDARY), provider)

    assert provider.calls == [PRIMARY.key, SECONDARY.key]


# --- describe_chain ----------------------------------------------------------


def test_describe_chain_renders_the_order() -> None:
    assert describe_chain(CHAIN) == (
        "openai/gpt-4o -> anthropic/claude-3-5-sonnet -> openai/gpt-4o-mini"
    )


def test_describe_empty_chain() -> None:
    assert describe_chain(()) == "<empty>"


def test_describe_single_model_chain() -> None:
    assert describe_chain((PRIMARY,)) == "openai/gpt-4o"


# --- result shape ------------------------------------------------------------


def test_failover_result_succeeded_is_the_inverse_of_exhausted() -> None:
    success = FailoverResult(value=1, model=PRIMARY)
    failure = FailoverResult(exhausted=True)

    assert success.succeeded is True
    assert failure.succeeded is False


def test_failover_result_defaults_to_no_value() -> None:
    empty = FailoverResult()

    assert empty.value is None
    assert empty.model is None
    assert empty.attempts == ()


def test_failover_attempt_defaults() -> None:
    attempt = FailoverAttempt(model=PRIMARY, succeeded=False)

    assert attempt.error_code is None
    assert attempt.retryable is False


def test_model_ref_key_joins_provider_and_model() -> None:
    assert PRIMARY.key == "openai/gpt-4o"
    assert str(SECONDARY) == "anthropic/claude-3-5-sonnet"


def test_model_ref_rejects_empty_components() -> None:
    with pytest.raises(InvalidRequestError):
        ModelRef(provider="", model="gpt-4o")
    with pytest.raises(InvalidRequestError):
        ModelRef(provider="openai", model="   ")


# --- classification sanity ---------------------------------------------------


@pytest.mark.parametrize(
    ("error", "expected_retryable"),
    [
        (ModelRateLimitedError("x"), True),
        (ModelTimeoutError("x"), True),
        (ModelProviderError("x"), True),
        (ModelNotAvailableError("x"), True),
        (AuthenticationError("x"), False),
        (InvalidRequestError("x"), False),
    ],
)
def test_retryability_classification(error: OrchestratorError, expected_retryable: bool) -> None:
    # The classification being asserted is the production one; failover's
    # behaviour above is only as correct as this mapping.
    assert error.retryable is expected_retryable
