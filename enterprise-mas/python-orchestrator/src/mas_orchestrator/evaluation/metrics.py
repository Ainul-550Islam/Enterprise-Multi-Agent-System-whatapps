"""Evaluation metrics.

Aggregates that survive a rerun. The important design choice is that a metric
is reported **with the number it was computed over**. A mean over two cases and
a mean over two hundred look identical when printed as a float, and a suite
that grew from two to two hundred cases without its mean moving is worth more
than one that moved.

Percentiles are computed from the sorted list rather than interpolated, so a
p95 reported here is a duration that actually occurred rather than a point
between two samples.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Final

from mas_orchestrator.evaluation.harness import CaseResult, CaseStatus, SuiteResult

#: Below this many counted cases, an aggregate is reported as preliminary.
PRELIMINARY_THRESHOLD: Final[int] = 20


@dataclass(frozen=True, slots=True)
class Aggregate:
    """A metric and how much it is based on.

    Attributes:
        name: Metric name.
        value: The value.
        n: How many observations.
        preliminary: Whether ``n`` is too small to rely on.
    """

    name: str
    value: float
    n: int
    preliminary: bool = False

    def describe(self) -> str:
        """One line, including whether it is preliminary."""
        suffix = " (preliminary)" if self.preliminary else ""
        return f"{self.name}={self.value:.4f} n={self.n}{suffix}"


def mean(values: list[float]) -> float:
    """Arithmetic mean, ``0.0`` for an empty list."""
    return sum(values) / len(values) if values else 0.0


def percentile(values: list[float], pct: float) -> float:
    """A percentile from the sorted list, not interpolated.

    Args:
        values: Observations.
        pct: Between 0 and 100.

    Returns:
        The value at that rank, or ``nan`` when there is no data. ``nan``
        rather than ``0.0``, because a zero duration would be a claim that
        something happened instantly.
    """
    if not values:
        return math.nan
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = max(0.0, min(1.0, pct / 100.0))
    index = round(rank * (len(ordered) - 1))
    return ordered[index]


def duration_summary(results: list[CaseResult]) -> dict[str, float]:
    """Duration percentiles over every result, including failed ones.

    Failed cases are included because a timeout is a duration too, and
    excluding them would make the latency of a broken suite look excellent.
    """
    durations = [float(result.duration_ms) for result in results]
    return {
        "count": float(len(durations)),
        "p50": percentile(durations, 50),
        "p95": percentile(durations, 95),
        "p99": percentile(durations, 99),
        "max": max(durations) if durations else math.nan,
        "mean": mean(durations),
    }


def status_counts(results: list[CaseResult]) -> dict[str, int]:
    """How many results in each status, including zero for absent statuses.

    An absent status is reported as zero rather than omitted, so two reports
    have the same keys and can be diffed.
    """
    counts = {status.value: 0 for status in CaseStatus}
    for result in results:
        counts[result.status.value] += 1
    return counts


def summarise(suite: SuiteResult) -> list[Aggregate]:
    """The standard aggregates for a suite."""
    scored = list(suite.scored)
    scores = [result.score for result in scored]
    return [
        Aggregate(
            "mean_score",
            mean(scores),
            len(scores),
            len(scores) < PRELIMINARY_THRESHOLD,
        ),
        Aggregate("pass_rate", suite.pass_rate, len(scored), False),
        Aggregate(
            "error_rate",
            (
                sum(
                    1
                    for r in suite.results
                    if r.status in (CaseStatus.ERRORED, CaseStatus.TIMED_OUT)
                )
                / len(suite.results)
                if suite.results
                else 0.0
            ),
            len(suite.results),
            False,
        ),
        Aggregate(
            "p95_duration_ms",
            percentile([float(r.duration_ms) for r in suite.results], 95),
            len(suite.results),
            False,
        ),
    ]


def compare(before: SuiteResult, after: SuiteResult) -> dict[str, float]:
    """Per-metric delta between two suites, keyed ``metric.delta``.

    Matched by metric name so a metric added to one side is simply absent
    rather than misaligned by position.
    """
    deltas: dict[str, float] = {}
    for left, right in zip(summarise(before), summarise(after), strict=False):
        deltas[f"{left.name}.delta"] = right.value - left.value
    return deltas


def regression_flag(
    before: SuiteResult,
    after: SuiteResult,
    *,
    tolerance: float = 0.02,
) -> bool:
    """Whether the later suite is worse by more than the tolerance.

    A tolerance exists because a model is nondeterministic. Without one, a
    suite fails on noise and the team learns to ignore it, which is worse than
    having no suite.
    """
    deltas = compare(before, after)
    score_drop = deltas.get("mean_score.delta", 0.0)
    error_rise = deltas.get("error_rate.delta", 0.0)
    return score_drop < -tolerance or error_rise > tolerance
