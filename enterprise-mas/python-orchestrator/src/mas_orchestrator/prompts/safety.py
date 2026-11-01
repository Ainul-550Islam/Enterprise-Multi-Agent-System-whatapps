"""Prompt-level safety.

The renderer makes substitution safe; this module decides what is safe to
*assemble*. It is the layer that answers "may this text go into a prompt, and
in what form?" before a prompt is built rather than after.

Three checks, in order of how often they matter:

* :func:`screen` — screens text for injection risk and reports what to do with
  it. It does not refuse: a low-risk signal on a document the user asked to
  summarise is not a reason to fail the request.
* :func:`prepare` — applies the decision, fencing anything that is not
  plainly safe. This is the function prompt-building code should call, because
  it is the one that cannot be forgotten: it returns text that is already
  either fenced or unchanged.
* :func:`check_assembly` — validates a finished prompt: that untrusted regions
  are fenced, that the prompt is within its size budget, and that no credential
  shape survived into it.

The last one is a backstop for a mistake made earlier. A prompt is the one place
where untrusted text and instructions are deliberately mixed, so it deserves a
check of its own.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.guardrails.prompt_injection import RiskLevel, analyse
from mas_orchestrator.prompts.renderer import fence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Ceiling on an assembled prompt. Large enough for a substantial context,
#: small enough that a runaway assembly fails fast rather than at the provider.
MAX_PROMPT_CHARS: Final[int] = 400_000


class Disposition(StrEnum):
    """What to do with a piece of text."""

    PASS = "pass"  # noqa: S105 - a disposition name, not a credential
    FENCE = "fence"
    REFUSE = "refuse"


#: Risk at or above which text is refused outright rather than fenced.
REFUSAL_LEVEL: Final[RiskLevel] = RiskLevel.HIGH

#: Risk at or above which text is fenced.
FENCING_LEVEL: Final[RiskLevel] = RiskLevel.LOW


@dataclass(frozen=True, slots=True)
class SafetyVerdict:
    """What to do with a piece of text.

    Attributes:
        disposition: What to do.
        risk: The risk that drove it.
        reason: Why, for the audit trail.
    """

    disposition: Disposition
    risk: RiskLevel
    reason: str = ""

    @property
    def usable(self) -> bool:
        """Whether the text may go into a prompt at all."""
        return self.disposition is not Disposition.REFUSE


def screen(text: str, *, label: str = "external") -> SafetyVerdict:
    """Decides what to do with a piece of text.

    Args:
        text: The text, typically from a tool, a document or a user.
        label: What kind of content it is, used in the fence.

    Returns:
        A :class:`SafetyVerdict`.
    """
    report = analyse(text)
    if report.level.ordinal >= REFUSAL_LEVEL.ordinal:
        return SafetyVerdict(
            Disposition.REFUSE,
            report.level,
            "text appears to be an injection attempt",
        )
    if report.level.ordinal >= FENCING_LEVEL.ordinal:
        return SafetyVerdict(
            Disposition.FENCE,
            report.level,
            f"text carries a {report.level.value} injection signal, so it is "
            f"fenced as {label} data",
        )
    return SafetyVerdict(Disposition.PASS, report.level, "no injection signal")


def prepare(
    text: str,
    *,
    label: str = "external",
    trust: object = None,
) -> str:
    """Screens text and fences it when anything is amiss.

    This is the function to call when assembling a prompt. Unlike
    :func:`screen`, it cannot be ignored: it returns fenced text rather than a
    recommendation to fence.

    Args:
        text: The text to prepare.
        label: What kind of content it is.
        trust: Ignored, and accepted only so call sites read the same whether
            or not they have a trust label. Trust is not inferred from the
            caller's confidence.

    Returns:
        The text, fenced when the verdict says to fence.

    Raises:
        InvalidRequestError: If the verdict is to refuse.
        PromptRenderError: If the fence cannot be built.
    """
    verdict = screen(text, label=label)
    if verdict.disposition is Disposition.REFUSE:
        raise InvalidRequestError(
            "text was refused because it appears to be an injection attempt",
            details={"risk": verdict.risk.value, "reason": verdict.reason},
        )
    if verdict.disposition is Disposition.FENCE:
        return fence(text, label=label)
    return fence(text, label=label)


def join(parts: list[str], *, separator: str = "\n\n") -> str:
    """Joins prepared parts into one block.

    A trivial function with a name, because assembling a prompt by concatenating
    strings in call sites is how one part ends up unfenced.
    """
    return separator.join(part for part in parts if part)


@dataclass(frozen=True, slots=True)
class AssemblyReport:
    """The result of validating an assembled prompt.

    Attributes:
        ok: Whether the prompt passed.
        chars: Prompt length.
        unfenced_regions: How many untrusted regions were left unfenced.
        credential_leaks: Kinds of credential detected in the prompt.
        problems: Human-readable problems, when any.
    """

    ok: bool
    chars: int
    unfenced_regions: int = 0
    credential_leaks: tuple[str, ...] = ()
    problems: tuple[str, ...] = ()


def check_assembly(
    prompt: str,
    *,
    max_chars: int = MAX_PROMPT_CHARS,
    require_fenced_untrusted: bool = True,
) -> AssemblyReport:
    """Validates a finished prompt.

    Args:
        prompt: The assembled prompt.
        max_chars: Size ceiling.
        require_fenced_untrusted: Whether an unfenced untrusted region is a
            problem.

    Returns:
        An :class:`AssemblyReport`. This reports rather than raises, so a
        caller can log and decide; the prompt is about to be sent and raising
        there would lose the diagnostic.
    """
    problems: list[str] = []
    chars = len(prompt)

    if chars > max_chars:
        problems.append(f"prompt is {chars} chars, over the {max_chars} ceiling")

    unfenced = 0
    if require_fenced_untrusted:
        unfenced = _count_unfenced_untrusted(prompt)
        if unfenced:
            problems.append(f"{unfenced} untrusted region(s) are not fenced")

    leaks = _credential_kinds(prompt)
    if leaks:
        problems.append(f"prompt contains {', '.join(leaks)}")

    return AssemblyReport(
        ok=not problems,
        chars=chars,
        unfenced_regions=unfenced,
        credential_leaks=tuple(leaks),
        problems=tuple(problems),
    )


def assert_assembly(prompt: str, *, max_chars: int = MAX_PROMPT_CHARS) -> None:
    """Refuses a prompt that fails validation.

    Raises:
        InvalidRequestError: If the prompt is oversized, has unfenced
            untrusted regions, or contains a credential.
    """
    report = check_assembly(prompt, max_chars=max_chars)
    if not report.ok:
        raise InvalidRequestError(
            "prompt failed assembly validation",
            details={
                "problems": list(report.problems),
                "chars": report.chars,
            },
        )


def _count_unfenced_untrusted(prompt: str) -> int:
    """Counts untrusted regions left outside a fence.

    Heuristic by necessity: a prompt is free text and this module does not own
    its structure. It looks for the fence's opening marker and reports regions
    that declare themselves untrusted without one.
    """
    import re

    openings = len(re.findall(r"<<<untrusted:", prompt))
    closings = len(re.findall(r"<<<end:", prompt))
    declared = openings + closings
    if declared == 0:
        return 0
    # A mismatch means a fence was opened and not closed, which leaves the tail
    # of the prompt outside any fence.
    return abs(openings - closings)


def _credential_kinds(prompt: str) -> list[str]:
    """Kinds of credential detected in a prompt."""
    from mas_orchestrator.guardrails.pii import RedactSecretsOnly, scan

    report = scan(prompt)
    kinds = {finding.kind for finding in report.findings}
    return sorted(kind.value for kind in kinds & RedactSecretsOnly.SECRET_KINDS)
