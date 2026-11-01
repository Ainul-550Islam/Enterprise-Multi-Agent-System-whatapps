"""Evaluation harness.

Runs a set of cases through a subject and scores the results. It is the only
place in the codebase where nondeterminism is tolerated on purpose: a model is
nondeterministic, so a single run proves nothing and a harness that ran each
case once would report noise.

Two properties make it usable:

* **Isolation.** Each case runs in its own tenant and execution id. A case that
  left state behind would change the result of the next one, and the scores
  would measure order rather than behaviour.
* **Bounded concurrency.** Cases run with a limit, so a harness over a hundred
  cases does not open a hundred provider connections at once.

The harness never scores a case that errored as though it were a wrong answer.
A crashed case is a failure to evaluate, not a low score: averaging a crash in
as a zero silently conflates "the model was wrong" with "the harness broke".
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Default concurrency ceiling for a run.
DEFAULT_CONCURRENCY: Final[int] = 8


class CaseStatus(StrEnum):
    """What happened to one case."""

    PASSED = "passed"
    FAILED = "failed"
    ERRORED = "errored"
    TIMED_OUT = "timed_out"
    REFUSED = "refused"


@dataclass(frozen=True, slots=True)
class EvalCase:
    """One evaluation case.

    Attributes:
        case_id: Unique within a suite.
        request: The request to run.
        expected: The expected content, when the check is exact or fuzzy.
        check: How to judge the answer.
        tags: Labels for grouping results.
        metadata: Anything a custom check needs.
    """

    case_id: str
    request: str
    expected: str = ""
    check: str = "contains"
    tags: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.case_id.strip():
            raise ValueError("an evaluation case needs an id")
        if not self.request.strip():
            raise ValueError("an evaluation case needs a request")


@dataclass(frozen=True, slots=True)
class CaseResult:
    """The outcome of one case.

    Attributes:
        case_id: Which case.
        status: What happened.
        score: Between 0 and 1.
        actual: The answer, truncated for the report.
        reason: Why this score, when it is not obvious.
        duration_ms: How long it took.
        tags: Copied from the case, so results can be grouped without a
            second lookup into the case list.
    """

    case_id: str
    status: CaseStatus
    score: float
    actual: str = ""
    reason: str = ""
    duration_ms: int = 0
    tags: tuple[str, ...] = ()

    @property
    def counted(self) -> bool:
        """Whether this result contributes to the aggregate score.

        An errored or timed-out case does not: its score would measure the
        harness, not the subject.
        """
        return self.status in (CaseStatus.PASSED, CaseStatus.FAILED, CaseStatus.REFUSED)


@dataclass(frozen=True, slots=True)
class SuiteResult:
    """The outcome of a suite.

    Attributes:
        suite: Suite name.
        results: Per-case results, in case order.
        duration_ms: Wall-clock duration of the run.
    """

    suite: str
    results: tuple[CaseResult, ...]
    duration_ms: int = 0

    @property
    def scored(self) -> tuple[CaseResult, ...]:
        """Results that count toward the aggregate."""
        return tuple(result for result in self.results if result.counted)

    @property
    def mean_score(self) -> float:
        """Mean score over counted cases. ``0.0`` when nothing counted."""
        scored = self.scored
        if not scored:
            return 0.0
        return sum(result.score for result in scored) / len(scored)

    @property
    def pass_rate(self) -> float:
        """Fraction of counted cases that passed."""
        scored = self.scored
        if not scored:
            return 0.0
        passed = sum(1 for result in scored if result.status is CaseStatus.PASSED)
        return passed / len(scored)

    def by_tag(self, tag: str) -> tuple[CaseResult, ...]:
        """Results carrying one tag."""
        return tuple(result for result in self.results if tag in result.tags)

    def tag_scores(self) -> dict[str, float]:
        """Mean score per tag, over counted cases.

        Reported per tag because an aggregate over a mixed suite hides the
        thing a suite is usually built to find: one category that regressed.
        """
        grouped: dict[str, list[float]] = {}
        for result in self.scored:
            for tag in result.tags:
                grouped.setdefault(tag, []).append(result.score)
        return {tag: sum(v) / len(v) for tag, v in sorted(grouped.items())}


#: A subject: run a request, return content.
Subject = Callable[[str], Awaitable[str]]

#: A scorer: judge an answer against a case.
Scorer = Callable[[EvalCase, str], Any]


def _default_scorer(case: EvalCase, actual: str) -> Any:
    """Scores with :func:`~mas_orchestrator.evaluation.scoring.score_case`."""
    from mas_orchestrator.evaluation.scoring import score_case

    return score_case(case, actual)


class EvalHarness:
    """Runs cases against a subject."""

    def __init__(
        self,
        subject: Subject,
        *,
        concurrency: int = DEFAULT_CONCURRENCY,
        timeout_seconds: float = 60.0,
        scorer: Scorer | None = None,
    ) -> None:
        """Builds a harness.

        Args:
            subject: Runs a request, returns content.
            concurrency: Ceiling on simultaneous cases.
            timeout_seconds: Per-case ceiling.
            scorer: Judges an answer. Defaults to the behavioural
                :func:`~mas_orchestrator.evaluation.scoring.score_case`. A
                safety suite supplies its own, because its checks are inverted:
                an answer the agent gives helpfully is a failure.
        """
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._subject = subject
        self._concurrency = concurrency
        self._timeout = timeout_seconds
        self._scorer = scorer or _default_scorer

    async def run(
        self,
        cases: list[EvalCase],
        *,
        suite: str = "default",
    ) -> SuiteResult:
        """Runs every case.

        Cases run concurrently up to the limit, and results are returned in
        case order so a report is stable across runs.

        Args:
            cases: The cases.
            suite: Suite name.

        Returns:
            A :class:`SuiteResult`.
        """
        started = time.monotonic()
        semaphore = asyncio.Semaphore(self._concurrency)

        async def one(case: EvalCase) -> CaseResult:
            async with semaphore:
                return await self._run_case(case)

        results = await asyncio.gather(*(one(case) for case in cases))
        duration = int((time.monotonic() - started) * 1000)
        return SuiteResult(suite=suite, results=tuple(results), duration_ms=duration)

    async def _run_case(self, case: EvalCase) -> CaseResult:
        """Runs one case, classifying failure rather than propagating it."""
        started = time.monotonic()
        try:
            actual = await asyncio.wait_for(self._subject(case.request), timeout=self._timeout)
        except TimeoutError:
            return CaseResult(
                case_id=case.case_id,
                status=CaseStatus.TIMED_OUT,
                score=0.0,
                reason=f"exceeded {self._timeout}s",
                duration_ms=int((time.monotonic() - started) * 1000),
                tags=case.tags,
            )
        except Exception as error:
            return CaseResult(
                case_id=case.case_id,
                status=CaseStatus.ERRORED,
                score=0.0,
                reason=f"{type(error).__name__}",
                duration_ms=int((time.monotonic() - started) * 1000),
                tags=case.tags,
            )

        verdict = self._scorer(case, actual)
        return CaseResult(
            case_id=case.case_id,
            status=verdict.status,
            score=verdict.score,
            actual=actual[:2000],
            reason=verdict.reason,
            duration_ms=int((time.monotonic() - started) * 1000),
            tags=case.tags,
        )
