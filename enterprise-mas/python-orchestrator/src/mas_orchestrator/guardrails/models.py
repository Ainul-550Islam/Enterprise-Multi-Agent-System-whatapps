"""Shared guardrail value types.

The guardrail modules each model one concern — input, injection, PII, policy,
output — and they need a common vocabulary to report through. That vocabulary
lives here rather than in any one of them, so a caller can handle a result
without importing the module that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class GuardrailStage(StrEnum):
    """When a guardrail runs."""

    BEFORE_MODEL = "before_model"
    DURING_EXECUTION = "during_execution"
    AFTER_MODEL = "after_model"


class GuardrailOutcome(StrEnum):
    """What a guardrail decided.

    ``PASSED`` and ``FENCED`` both let the content through. ``FENCED`` records
    that it was marked as untrusted data on the way, which matters later: an
    answer derived from fenced content is answering about data, not obeying it.
    """

    PASSED = "passed"
    FENCED = "fenced"
    SANITISED = "sanitised"
    REFUSED = "refused"
    REDACTED = "redacted"
    FLAGGED = "flagged"


@dataclass(frozen=True, slots=True)
class GuardrailFinding:
    """One thing a guardrail noticed.

    Attributes:
        source: Which guardrail raised it.
        stage: When it ran.
        outcome: What was decided.
        code: A stable, machine-readable identifier.
        message: A human-readable explanation. Never contains the content that
            triggered it — a finding is logged, and content must not be.
        weight: Relative severity, when the source computes one.
        details: Source-specific structured data.
    """

    source: str
    stage: GuardrailStage
    outcome: GuardrailOutcome
    code: str
    message: str = ""
    weight: float = 0.0
    details: dict[str, Any] | None = None

    @property
    def blocking(self) -> bool:
        """Whether this finding stops the execution."""
        return self.outcome in (GuardrailOutcome.REFUSED, GuardrailOutcome.REDACTED)
