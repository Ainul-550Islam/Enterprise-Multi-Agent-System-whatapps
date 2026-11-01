"""Input validation and size limits.

The first guardrail stage. Its job is to reject a request before it costs
anything: before a model call, before a tool call, before a retrieval.

Two things are checked and reported separately, because the remedies differ:

* **Structure** — is the input a string, is it present, are the attachments of
  a permitted kind? Fixing this is the caller's job.
* **Size** — is it within bounds? Fixing this may be the caller's job or may
  legitimately require chunking, so the failure names the actual limit.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_INPUT_CHARS: Final[int] = 200_000
MAX_ATTACHMENT_BYTES: Final[int] = 8 * 1024 * 1024
MAX_ATTACHMENTS: Final[int] = 16

# Control characters other than tab and newline. These are not printable and
# have no business in user text; several of them can rewrite a terminal title
# or scramble a log line.
_CONTROL: Final[re.Pattern[str]] = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

# Zero-width and bidirectional-override characters. These are the basis of
# "Trojan Source" style attacks, where what a reviewer reads differs from what
# the model tokenises. Written as escapes so the source stays unambiguous.
_INVISIBLE: Final[re.Pattern[str]] = re.compile(
    "["
    "\\u200b-\\u200f"  # zero-width space through right-to-left mark
    "\\u202a-\\u202e"  # bidirectional embeddings and overrides
    "\\u2066-\\u2069"  # isolated directional formatting
    "\\ufeff"  # zero-width no-break space
    "]"
)


class InputVerdict(StrEnum):
    """Outcome of input validation."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    SANITISED = "sanitised"


@dataclass(frozen=True, slots=True)
class InputLimits:
    """Size ceilings for one request.

    Attributes:
        max_chars: Text ceiling.
        max_attachment_bytes: Per-attachment ceiling.
        max_attachments: Attachment count ceiling.
    """

    max_chars: int = MAX_INPUT_CHARS
    max_attachment_bytes: int = MAX_ATTACHMENT_BYTES
    max_attachments: int = MAX_ATTACHMENTS

    def __post_init__(self) -> None:
        for field_name in ("max_chars", "max_attachment_bytes", "max_attachments"):
            if getattr(self, field_name) < 1:
                raise InvalidRequestError(
                    "input limits must be positive", details={"field": field_name}
                )


@dataclass(frozen=True, slots=True)
class InputCheck:
    """The result of validating one input.

    Attributes:
        verdict: The outcome.
        text: The (possibly sanitised) text.
        reason: Why it was rejected, when it was.
        removed: What sanitisation removed, when it did.
    """

    verdict: InputVerdict
    text: str
    reason: str | None = None
    removed: tuple[str, ...] = ()

    @property
    def accepted(self) -> bool:
        """Whether the input may proceed."""
        return self.verdict is not InputVerdict.REJECTED


def validate_input(
    text: object,
    *,
    limits: InputLimits | None = None,
    sanitise: bool = True,
) -> InputCheck:
    """Validates and optionally sanitises user input.

    Args:
        text: The input to check.
        limits: Size ceilings.
        sanitise: Whether to strip invisible characters rather than reject.

    Returns:
        An :class:`InputCheck`. Never raises for ordinary bad input: a
        rejected input is a result, not an exception, because the caller has
        to turn it into a typed denial anyway.
    """
    limits = limits or InputLimits()

    if text is None:
        return InputCheck(InputVerdict.REJECTED, "", reason="input is missing")
    if not isinstance(text, str):
        return InputCheck(
            InputVerdict.REJECTED,
            "",
            reason=f"input must be a string, got {type(text).__name__}",
        )

    if not text.strip():
        return InputCheck(InputVerdict.REJECTED, text, reason="input is empty")

    if len(text) > limits.max_chars:
        return InputCheck(
            InputVerdict.REJECTED,
            text[: limits.max_chars],
            reason=(f"input exceeds the maximum length ({len(text)} > {limits.max_chars})"),
        )

    removed: list[str] = []

    if _CONTROL.search(text):
        if not sanitise:
            return InputCheck(
                InputVerdict.REJECTED, text, reason="input contains control characters"
            )
        text = _CONTROL.sub("", text)
        removed.append("control_characters")

    if _INVISIBLE.search(text):
        if not sanitise:
            return InputCheck(
                InputVerdict.REJECTED, text, reason="input contains invisible characters"
            )
        # Invisible characters are always stripped rather than rejected: they
        # cannot be typed deliberately, so rejecting would punish a paste.
        text = _INVISIBLE.sub("", text)
        removed.append("invisible_characters")

    verdict = InputVerdict.SANITISED if removed else InputVerdict.ACCEPTED
    if removed:
        _LOGGER.info(
            "input sanitised",
            extra={"event": "guardrail.input_sanitised", "removed": removed},
        )
    return InputCheck(verdict, text, removed=tuple(removed))


def enforce_input(
    text: object,
    *,
    limits: InputLimits | None = None,
) -> str:
    """Validates input and raises on rejection.

    The raising counterpart to :func:`validate_input`, for call sites where a
    rejected input should abort rather than be handled.

    Raises:
        InvalidRequestError: If the input is rejected.
    """
    check = validate_input(text, limits=limits)
    if not check.accepted:
        raise InvalidRequestError(
            f"input rejected: {check.reason}", details={"reason": check.reason}
        )
    return check.text
