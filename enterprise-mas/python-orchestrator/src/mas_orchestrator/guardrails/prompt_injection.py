"""Prompt-injection analysis.

Detecting injection is probabilistic and cannot be made sound, so this module
is explicit about what it is: a **risk signal**, not a verdict. It reports a
score and the patterns that fired, and it is the caller's policy that decides
whether to refuse, sanitise, or proceed with the content fenced.

What it looks for is the difference between text that *discusses* instructions
and text that *issues* them. "The document says to ignore previous
instructions" is a discussion and is fine. "Ignore previous instructions" at
the start of a line, addressed to the reader, is an attempt.

Signals are also raised by **encoding tricks** — invisible characters, control
characters, homoglyphs and long base64 runs — because those are how an
instruction is hidden from a human reviewer while remaining visible to the
model.
"""

from __future__ import annotations

import base64
import binascii
import logging
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class RiskLevel(StrEnum):
    """How worrying a piece of text looks.

    * ``NONE`` — nothing fired.
    * ``LOW`` — a single weak signal; fence and proceed.
    * ``MEDIUM`` — several signals, or one strong one; fence and consider
      refusing.
    * ``HIGH`` — unambiguous attempt; refuse.
    """

    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"

    @property
    def ordinal(self) -> int:
        """Ordering value, so levels can be compared."""
        order = {
            RiskLevel.NONE: 0,
            RiskLevel.LOW: 1,
            RiskLevel.MEDIUM: 2,
            RiskLevel.HIGH: 3,
        }
        return order[self]


@dataclass(frozen=True, slots=True)
class InjectionSignal:
    """One pattern that fired.

    Attributes:
        name: Stable signal name.
        weight: Contribution to the score.
        excerpt: Where it fired, truncated. Never the whole input: signals are
            logged, and text must not reach a log line in full.
    """

    name: str
    weight: float
    excerpt: str


@dataclass(frozen=True, slots=True)
class InjectionReport:
    """The outcome of an analysis.

    Attributes:
        level: Overall risk.
        score: Sum of signal weights.
        signals: Signals that fired, heaviest first.
        scanned_chars: How much text was examined.
    """

    level: RiskLevel
    score: float
    signals: tuple[InjectionSignal, ...]
    scanned_chars: int

    @property
    def detected(self) -> bool:
        """Whether anything fired at all."""
        return bool(self.signals)


# --- Patterns --------------------------------------------------------------

# Each entry is (name, weight, pattern). Weights are judgement calls, not
# calibrated probabilities: they express how much a pattern on its own should
# worry us relative to the others.
_PATTERNS: Final[tuple[tuple[str, float, re.Pattern[str]], ...]] = (
    (
        "instruction_override",
        1.0,
        re.compile(
            r"\b(ignore|disregard|forget|override)\b[^.\n]{0,40}\b"
            r"(previous|prior|above|earlier|all|your)\b[^.\n]{0,30}\b"
            r"(instruction|instructions|prompt|rule|rules|direction|directions)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "role_hijack",
        1.0,
        re.compile(
            r"\b(you are|act as|pretend to be|roleplay as|your new role is)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "system_prompt_extraction",
        0.9,
        re.compile(
            r"\b(reveal|show|print|repeat|output|display|tell me)\b[^.\n]{0,30}\b"
            r"(system prompt|your instructions|your prompt|system message)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "delimiter_escape",
        0.8,
        re.compile(
            r"(</?(system|assistant|user|tool_result|untrusted)\s*>"
            r"|<<<\s*/?(end|untrusted|system)\b"
            r"|\[/?(system|inst)\])",
            re.IGNORECASE,
        ),
    ),
    (
        "instruction_to_agent",
        0.7,
        re.compile(r"^\s*(system|assistant|developer)\s*:", re.IGNORECASE | re.MULTILINE),
    ),
    (
        "urgency_coercion",
        0.4,
        re.compile(
            r"\b(do not tell|don't tell|do not mention|without telling|"
            r"secretly|silently|immediately and without)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "exfiltration_request",
        0.9,
        re.compile(
            r"\b(send|post|upload|transmit|exfiltrate)\b[^.\n]{0,40}\b"
            r"(to|at)\b[^.\n]{0,40}\b(https?://|http|url|endpoint|webhook)\b",
            re.IGNORECASE,
        ),
    ),
)

# Reporting frames. "The document says to ignore previous instructions" is a
# *discussion* of an injection attempt, not an attempt, and must not be scored
# the same way. When a pattern fires inside reported speech its weight is
# reduced rather than counted in full.
_REPORTING: Final[re.Pattern[str]] = re.compile(
    r"\b(says?|said|states?|stated|claims?|claimed|according to|mentions?|"
    r"mentioned|describes?|described|reports?|reported|notes?|noted|"
    r"explains?|explained|warns?|warned|quotes?|quoted)\b",
    re.IGNORECASE,
)

# How far back to look for a reporting frame.
_REPORTING_WINDOW: Final[int] = 60

# Weight multiplier when a pattern fires inside reported speech.
_REPORTED_SPEECH_FACTOR: Final[float] = 0.25

_INVISIBLE: Final[re.Pattern[str]] = re.compile(
    "["
    "\\u200b-\\u200f"  # zero-width space through right-to-left mark
    "\\u202a-\\u202e"  # bidirectional embeddings and overrides
    "\\u2066-\\u2069"  # isolated directional formatting
    "\\ufeff"  # zero-width no-break space
    "]"
)
_CONTROL: Final[re.Pattern[str]] = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_BASE64_RUN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9+/]{60,}={0,2}")
_EXCERPT_CHARS: Final[int] = 60

# Score thresholds for each level. Above HIGH the content should not reach a
# model at all; below LOW it is unremarkable.
_THRESHOLDS: Final[tuple[tuple[float, RiskLevel], ...]] = (
    (2.0, RiskLevel.HIGH),
    (1.0, RiskLevel.MEDIUM),
    (0.1, RiskLevel.LOW),
)


def analyse(text: str, *, max_chars: int = 100_000) -> InjectionReport:
    """Analyses text for injection signals.

    Args:
        text: The text to examine. Usually third-party content: a retrieved
            document, tool output, or the user's own input.
        max_chars: How much to examine. Beyond this the scan stops, so an
            enormous document cannot dominate the runtime.

    Returns:
        An :class:`InjectionReport`.
    """
    if not text:
        return InjectionReport(RiskLevel.NONE, 0.0, (), 0)

    scanned = text[:max_chars]
    signals: list[InjectionSignal] = []
    score = 0.0

    for name, weight, pattern in _PATTERNS:
        match = pattern.search(scanned)
        if match is None:
            continue

        effective = weight
        if _is_reported(scanned, match.start()):
            # Discussing an injection is not committing one. Down-weighting
            # rather than dropping keeps a genuine attempt that happens to be
            # framed as a quote from scoring zero.
            effective = weight * _REPORTED_SPEECH_FACTOR

        signals.append(
            InjectionSignal(
                name=name,
                weight=effective,
                excerpt=_excerpt(match.group(0)),
            )
        )
        score += effective

    # --- encoding tricks ---
    if _INVISIBLE.search(scanned):
        signals.append(InjectionSignal("invisible_characters", 0.6, ""))
        score += 0.6

    if _CONTROL.search(scanned):
        signals.append(InjectionSignal("control_characters", 0.4, ""))
        score += 0.4

    base64_match = _BASE64_RUN.search(scanned)
    if base64_match is not None and _decodes_as_text(base64_match.group(0)):
        signals.append(InjectionSignal("embedded_encoded_payload", 0.5, ""))
        score += 0.5

    signals.sort(key=lambda signal: -signal.weight)
    level = _level_for(score)

    if signals:
        _LOGGER.warning(
            "injection signals detected",
            extra={
                "event": "guardrail.injection",
                "level": level.value,
                "score": round(score, 2),
                "signals": [signal.name for signal in signals],
            },
        )

    return InjectionReport(
        level=level,
        score=score,
        signals=tuple(signals),
        scanned_chars=len(scanned),
    )


def is_suspicious(text: str) -> bool:
    """Whether text carries any injection signal at all."""
    return analyse(text).detected


def exceeds(text: str, level: RiskLevel) -> bool:
    """Whether text's risk level is at or above ``level``."""
    return analyse(text).level.ordinal >= level.ordinal


def _is_reported(text: str, position: int) -> bool:
    """Whether a match sits inside reported speech.

    Looks back a short window for a reporting verb. This is a heuristic, and it
    errs toward *not* reporting: a false negative here costs a down-weighted
    signal, while a false positive would let a real attempt through.
    """
    window = text[max(0, position - _REPORTING_WINDOW) : position]
    return _REPORTING.search(window) is not None


def _level_for(score: float) -> RiskLevel:
    """Maps a score onto a risk level."""
    for threshold, level in _THRESHOLDS:
        if score >= threshold:
            return level
    return RiskLevel.NONE


def _excerpt(match: str) -> str:
    """Truncates a match for reporting."""
    if len(match) <= _EXCERPT_CHARS:
        return match
    return f"{match[:_EXCERPT_CHARS]}..."


def _decodes_as_text(candidate: str) -> bool:
    """Whether a base64-looking run decodes to printable text.

    A long base64 run that decodes to readable prose is a classic way to hide
    an instruction from a human reviewer. A run that decodes to binary is
    probably just data.
    """
    padded = candidate + "=" * (-len(candidate) % 4)
    try:
        decoded = base64.b64decode(padded, validate=True)
    except (binascii.Error, ValueError):
        return False
    if not decoded:
        return False
    try:
        text = decoded.decode("utf-8")
    except UnicodeDecodeError:
        return False
    printable = sum(1 for char in text if char.isprintable() or char.isspace())
    return printable / len(text) > 0.9
