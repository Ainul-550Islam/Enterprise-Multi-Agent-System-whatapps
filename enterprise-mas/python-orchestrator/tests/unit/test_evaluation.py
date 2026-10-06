"""Tests for the evaluation harness, scoring, metrics and safety suite.

The design claims under test: scoring is deterministic, a case that cannot be
evaluated is never counted as a pass, and a safety suite fails closed.
"""

from __future__ import annotations

import asyncio
import math

import pytest

from mas_orchestrator.evaluation.harness import (
    CaseResult,
    CaseStatus,
    EvalCase,
    EvalHarness,
    SuiteResult,
)
from mas_orchestrator.evaluation.metrics import (
    Aggregate,
    compare,
    duration_summary,
    mean,
    percentile,
    regression_flag,
    status_counts,
    summarise,
)
from mas_orchestrator.evaluation.safety import (
    DEFAULT_SAFETY_CASES,
    SafetyCheck,
    SafetySuiteResult,
    SafetyVerdict,
    assert_safe,
    evaluate_safety,
    run_safety_suite,
)
from mas_orchestrator.evaluation.scoring import (
    Verdict,
    is_refusal,
    normalise,
    score_case,
)


def _result(
    case_id: str,
    status: CaseStatus = CaseStatus.PASSED,
    score: float = 1.0,
    duration_ms: int = 10,
    tags: tuple[str, ...] = (),
) -> CaseResult:
    return CaseResult(
        case_id=case_id,
        status=status,
        score=score,
        duration_ms=duration_ms,
        tags=tags,
    )


class TestScoring:
    def test_contains_is_case_and_space_insensitive(self) -> None:
        # "It is 4." and "it is 4" are the same answer; a suite that disagrees
        # produces false regressions.
        assert normalise("It is  4.") == normalise("it is 4")

    def test_contains_passes(self) -> None:
        verdict = score_case(EvalCase("c", "q", expected="hello"), "well, hello there")
        assert verdict.status is CaseStatus.PASSED
        assert verdict.score == 1.0

    def test_contains_fails(self) -> None:
        assert score_case(EvalCase("c", "q", expected="hello"), "goodbye").score == 0.0

    def test_exact_requires_equality(self) -> None:
        case = EvalCase("c", "q", expected="4", check="exact")
        assert score_case(case, "4").score == 1.0
        assert score_case(case, "the answer is 4").score == 0.0

    def test_regex_supports_patterns(self) -> None:
        case = EvalCase("c", "q", expected=r"\d{3}", check="regex")
        assert score_case(case, "code 415").score == 1.0
        assert score_case(case, "no digits").score == 0.0

    def test_invalid_pattern_is_refused(self) -> None:
        # A typo in a suite must fail loudly, not silently pass everything.
        with pytest.raises(ValueError, match=r"unknown check|invalid pattern"):
            score_case(EvalCase("c", "q", expected="[", check="regex"), "x")

    def test_refusal_is_scored_as_its_own_category(self) -> None:
        # An agent that correctly refuses is behaving well; counting that as a
        # zero would push a deployment toward an agent that never says no.
        case = EvalCase("c", "q", check="refusal")
        assert score_case(case, "I can't help with that.").status is CaseStatus.PASSED
        assert score_case(case, "Sure, here you go.").status is CaseStatus.FAILED

    def test_refusal_detection_is_not_fooled_by_partial_matches(self) -> None:
        assert is_refusal("I cannot do that.")
        assert not is_refusal("I can help with that.")

    def test_json_schema_checks_keys(self) -> None:
        case = EvalCase("c", "q", expected="a,b", check="json_schema")
        assert score_case(case, '{"a": 1, "b": 2}').score == 1.0
        assert score_case(case, '{"a": 1}').score == 0.0

    def test_citation_check_rejects_unsupported_sources(self) -> None:
        case = EvalCase("c", "q", expected="2", check="citation")
        assert score_case(case, "It is true [1].").score == 1.0
        assert score_case(case, "It is true [7].").score == 0.0

    def test_unknown_check_is_refused(self) -> None:
        with pytest.raises(ValueError, match="unknown check"):
            score_case(EvalCase("c", "q", check="vibes"), "x")

    def test_nonempty_is_a_smoke_check(self) -> None:
        assert score_case(EvalCase("c", "q", check="nonempty"), "x").score == 1.0
        assert score_case(EvalCase("c", "q", check="nonempty"), "  ").score == 0.0

    def test_case_requires_an_id_and_a_request(self) -> None:
        with pytest.raises(ValueError):
            EvalCase(" ", "q")
        with pytest.raises(ValueError):
            EvalCase("c", " ")

    def test_verdict_shape(self) -> None:
        assert Verdict(CaseStatus.PASSED, 1.0, "ok").score == 1.0


class TestMetrics:
    def test_mean_of_nothing_is_zero(self) -> None:
        assert mean([]) == 0.0

    def test_percentile_is_a_real_observation(self) -> None:
        # Not interpolated: a p95 reported here is a duration that occurred.
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert percentile(values, 95) == 5.0
        assert percentile(values, 0) == 1.0

    def test_percentile_of_nothing_is_nan(self) -> None:
        # nan rather than zero: a zero duration would claim something happened
        # instantly.
        assert math.isnan(percentile([], 95))

    def test_status_counts_report_absent_statuses_as_zero(self) -> None:
        # Same keys every time, so two reports can be diffed.
        assert status_counts([_result("a")])["errored"] == 0

    def test_duration_summary_includes_failures(self) -> None:
        # A timeout is a duration too; excluding it would make a broken suite
        # look fast.
        summary = duration_summary([_result("a"), _result("b", CaseStatus.TIMED_OUT, 0.0, 5000)])
        assert summary["p95"] == 5000.0

    def test_aggregate_marks_small_samples_preliminary(self) -> None:
        suite = SuiteResult("s", (_result("a"),))
        assert any(item.preliminary for item in summarise(suite))

    def test_aggregate_reports_the_count(self) -> None:
        # A mean over two and a mean over two hundred print identically; the
        # count is what makes them different.
        assert Aggregate("m", 1.0, 2).describe() == "m=1.0000 n=2"

    def test_regression_flag_uses_a_tolerance(self) -> None:
        # Without a tolerance a suite fails on noise and the team learns to
        # ignore it.
        before = SuiteResult("s", tuple(_result(f"a{i}") for i in range(5)))
        after = SuiteResult("s", tuple(_result(f"a{i}") for i in range(5)))
        assert not regression_flag(before, after)

        worse = SuiteResult(
            "s",
            tuple(
                _result(f"a{i}", CaseStatus.FAILED, 0.0) if i < 4 else _result(f"a{i}")
                for i in range(5)
            ),
        )
        assert regression_flag(before, worse)

    def test_compare_is_keyed_by_name(self) -> None:
        before = SuiteResult("s", (_result("a"),))
        after = SuiteResult("s", (_result("a", CaseStatus.FAILED, 0.0),))
        assert compare(before, after)["mean_score.delta"] == -1.0


class TestHarness:
    async def test_results_come_back_in_case_order(self) -> None:
        async def subject(request: str) -> str:
            await asyncio.sleep(0)
            return f"answer to {request}"

        suite = await EvalHarness(subject).run([EvalCase("a", "one"), EvalCase("b", "two")])
        assert [result.case_id for result in suite.results] == ["a", "b"]

    async def test_erroring_case_is_not_scored_as_wrong(self) -> None:
        # A crash is a failure to evaluate, not a low score: averaging it in as
        # a zero conflates "the model was wrong" with "the harness broke".
        async def subject(request: str) -> str:
            raise RuntimeError("boom")

        suite = await EvalHarness(subject).run([EvalCase("a", "q")])
        result = suite.results[0]
        assert result.status is CaseStatus.ERRORED
        assert not result.counted
        assert suite.pass_rate == 0.0

    async def test_timeout_is_classified(self) -> None:
        async def slow(request: str) -> str:
            await asyncio.sleep(5)
            return "too late"

        suite = await EvalHarness(slow, timeout_seconds=0.01).run([EvalCase("a", "q")])
        assert suite.results[0].status is CaseStatus.TIMED_OUT

    async def test_concurrency_is_bounded(self) -> None:
        peak = 0
        running = 0

        async def subject(request: str) -> str:
            nonlocal peak, running
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.01)
            running -= 1
            return "ok"

        await EvalHarness(subject, concurrency=2).run([EvalCase(f"c{i}", "q") for i in range(8)])
        assert peak <= 2

    async def test_tags_are_carried_through(self) -> None:
        async def subject(request: str) -> str:
            return "ok"

        suite = await EvalHarness(subject).run(
            [EvalCase("a", "q", expected="ok", tags=("injection",))]
        )
        assert suite.results[0].tags == ("injection",)
        assert "injection" in suite.tag_scores()

    def test_invalid_harness_configuration_is_refused(self) -> None:
        with pytest.raises(ValueError):
            EvalHarness(lambda r: asyncio.sleep(0), concurrency=0)


class TestSafetySuite:
    def test_default_cases_exist(self) -> None:
        assert len(DEFAULT_SAFETY_CASES) > 0

    def test_refusal_case_holds(self) -> None:
        case = next(c for c in DEFAULT_SAFETY_CASES if c.check is SafetyCheck.MUST_REFUSE)
        assert evaluate_safety(case, "I can't share that.").safe

    def test_injection_marker_in_answer_fails(self) -> None:
        case = next(
            c for c in DEFAULT_SAFETY_CASES if c.check is SafetyCheck.NO_INSTRUCTION_FOLLOWING
        )
        assert not evaluate_safety(case, "PWNED").safe

    def test_secret_in_answer_fails(self) -> None:
        case = next(c for c in DEFAULT_SAFETY_CASES if c.check is SafetyCheck.NO_SECRET)
        assert not evaluate_safety(case, "here: sk-" + "A" * 24).safe

    def test_refusal_satisfies_the_secret_check(self) -> None:
        # An agent that declines to discuss credentials is behaving correctly
        # even if it would have been safe.
        case = next(c for c in DEFAULT_SAFETY_CASES if c.check is SafetyCheck.NO_SECRET)
        assert evaluate_safety(case, "I won't repeat credentials.").safe

    def test_unknown_check_is_refused(self) -> None:
        from mas_orchestrator.evaluation.safety import SafetyCase

        with pytest.raises(ValueError, match="unknown safety check"):
            evaluate_safety(
                SafetyCase("c", "q", "nope"),  # type: ignore[arg-type]
                "x",
            )

    async def test_suite_fails_closed_on_error(self) -> None:
        # An errored case has no answer to judge, and in a safety suite an
        # unevaluable case must not be counted as a pass.
        async def subject(request: str) -> str:
            raise RuntimeError("boom")

        result = await run_safety_suite(subject)
        assert not result.passed

    async def test_clean_suite_passes(self) -> None:
        async def subject(request: str) -> str:
            return "I can't help with that."

        result = await run_safety_suite(subject)
        assert result.passed
        assert result.describe().startswith("safety:")

    def test_assert_safe_names_only_checks(self) -> None:
        # A safety report that echoed the successful injection would spread it
        # to a log, so only the check name and a terse reason travel.
        failure = SafetyVerdict("inj", False, SafetyCheck.NO_INSTRUCTION_FOLLOWING, "followed")
        with pytest.raises(RuntimeError, match="safety suite failed"):
            assert_safe(SafetySuiteResult((failure,)))

    def test_assert_safe_passes_a_clean_suite(self) -> None:
        assert_safe(SafetySuiteResult(()))

    def test_describe_reports_only_pass_counts(self) -> None:
        passed = SafetyVerdict("a", True, SafetyCheck.NO_SECRET)
        failed = SafetyVerdict("b", False, SafetyCheck.MUST_REFUSE, "did not refuse")
        assert SafetySuiteResult((passed, failed)).describe() == (
            "safety: 1/2 passed (must_refuse)"
        )
