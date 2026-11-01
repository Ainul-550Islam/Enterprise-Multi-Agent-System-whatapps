"""Evaluation.

Two suites, and the distinction between them is the point of this package.

A **behavioural** suite asks whether the agent is useful: does it answer, is
the answer right, how long did it take. :mod:`.harness` and :mod:`.scoring`
serve it.

A **safety** suite asks whether the agent is safe: does it refuse an injection,
does it leak a credential, does it route data somewhere it was not asked to.
:mod:`.safety` serves it, and it is scored *inverted* — a case the agent answers
helpfully is a failure.

Keeping them separate matters because their failure modes are not comparable.
Improvements to helpfulness routinely reduce refusal rate, and an average of the
two would let a helpfulness gain hide a safety regression. Nothing here
averages them.

Two rules run through every module:

* **Deterministic scoring.** No LLM-as-judge. A judge that is itself
  nondeterministic turns a regression suite into a source of flaky results, and
  a suite that cannot be trusted stops being run.
* **Fail closed.** A case that cannot be evaluated is recorded as a failure,
  never as a pass. In a behavioural suite an unevaluable case is noise; in a
  safety suite it is indistinguishable from a breach.

:mod:`.metrics` reports every aggregate with the count it was computed over and
marks small samples preliminary, because a mean over two cases and a mean over
two hundred print identically and mean very different things.
"""

from __future__ import annotations

from mas_orchestrator.evaluation.harness import (
    DEFAULT_CONCURRENCY,
    CaseResult,
    CaseStatus,
    EvalCase,
    EvalHarness,
    Subject,
    SuiteResult,
)
from mas_orchestrator.evaluation.metrics import (
    PRELIMINARY_THRESHOLD,
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
    SECRET_PATTERNS,
    SafetyCase,
    SafetyCheck,
    SafetySuiteResult,
    SafetyVerdict,
    assert_safe,
    evaluate_safety,
    run_safety_suite,
)
from mas_orchestrator.evaluation.scoring import (
    REFUSAL_MARKERS,
    Verdict,
    is_refusal,
    normalise,
    score_case,
)

__all__ = [
    "DEFAULT_CONCURRENCY",
    "DEFAULT_SAFETY_CASES",
    "PRELIMINARY_THRESHOLD",
    "REFUSAL_MARKERS",
    "SECRET_PATTERNS",
    "Aggregate",
    "CaseResult",
    "CaseStatus",
    "EvalCase",
    "EvalHarness",
    "SafetyCase",
    "SafetyCheck",
    "SafetySuiteResult",
    "SafetyVerdict",
    "Subject",
    "SuiteResult",
    "Verdict",
    "assert_safe",
    "compare",
    "duration_summary",
    "evaluate_safety",
    "is_refusal",
    "mean",
    "normalise",
    "percentile",
    "regression_flag",
    "run_safety_suite",
    "score_case",
    "status_counts",
    "summarise",
]
