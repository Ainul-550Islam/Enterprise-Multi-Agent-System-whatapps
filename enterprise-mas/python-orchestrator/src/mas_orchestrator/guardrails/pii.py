"""PII detection and redaction.

This is a **detector**, not a compliance control. It finds the shapes that look
like common identifiers and reports them with a confidence and a location, so
that whatever policy sits above can decide what to do. It deliberately does not
decide for you: silently redacting everything that looks like a number would
destroy perfectly ordinary content.

Two behaviours worth stating plainly:

* Detection is conservative in *flagging* and conservative in *redacting*. A
  false positive costs a redaction; a false negative leaks.
* Redaction replaces the value with a labelled placeholder rather than
  deleting it, so a downstream reader can see that something was removed and
  what kind of thing it was.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Final, Protocol, runtime_checkable

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class PiiKind(StrEnum):
    """Kinds of personal data recognised."""

    EMAIL = "email"
    PHONE = "phone"
    CREDIT_CARD = "credit_card"
    NATIONAL_ID = "national_id"
    IP_ADDRESS = "ip_address"
    API_KEY = "api_key"
    PRIVATE_KEY = "private_key"
    BEARER_TOKEN = "bearer_token"  # noqa: S105 - an enum member, not a credential
    IBAN = "iban"
    DATE_OF_BIRTH = "date_of_birth"


class Action(StrEnum):
    """What the policy decided to do about a finding."""

    ALLOW = "allow"
    REDACT = "redact"
    BLOCK = "block"


@dataclass(frozen=True, slots=True)
class PiiFinding:
    """One detected item.

    Attributes:
        kind: What it looks like.
        start: Offset in the scanned text.
        end: End offset, exclusive.
        confidence: 0..1. Not a probability; an expression of how distinctive
            the pattern is.
        masked: The value with most of it removed, safe to log.
    """

    kind: PiiKind
    start: int
    end: int
    confidence: float
    masked: str

    @property
    def span(self) -> tuple[int, int]:
        """The finding's offsets."""
        return (self.start, self.end)


@dataclass(frozen=True, slots=True)
class PiiReport:
    """The outcome of a scan.

    Attributes:
        findings: What was found, ordered by position.
        scanned_chars: How much text was examined.
    """

    findings: tuple[PiiFinding, ...]
    scanned_chars: int

    @property
    def detected(self) -> bool:
        """Whether anything was found."""
        return bool(self.findings)

    def kinds(self) -> tuple[PiiKind, ...]:
        """Distinct kinds found, in first-seen order."""
        seen: dict[PiiKind, None] = {}
        for finding in self.findings:
            seen.setdefault(finding.kind, None)
        return tuple(seen)


@runtime_checkable
class PiiPolicy(Protocol):
    """Decides what to do about a finding."""

    def action_for(self, finding: PiiFinding) -> Action:
        """Returns the action to take for one finding."""
        ...


class RedactSecretsOnly:
    """The default policy: redact credentials, allow personal data.

    Rationale: an API key or private key in model output is a security incident
    regardless of consent, whereas an email address in a document the user
    supplied may be exactly what they asked to see. Blocking the latter by
    default would be a policy decision this layer has no standing to make.
    """

    SECRET_KINDS: Final[frozenset[PiiKind]] = frozenset(
        {PiiKind.API_KEY, PiiKind.PRIVATE_KEY, PiiKind.BEARER_TOKEN}
    )

    def action_for(self, finding: PiiFinding) -> Action:
        """Redacts credentials, allows the rest."""
        if finding.kind in self.SECRET_KINDS:
            return Action.REDACT
        return Action.ALLOW


class RedactAll:
    """Redacts every finding.

    For tenants or environments where personal data must not leave the
    orchestrator at all.
    """

    def action_for(self, finding: PiiFinding) -> Action:
        """Redacts everything detected."""
        return Action.REDACT


# --- Patterns --------------------------------------------------------------

# (kind, confidence, pattern). Ordered so the more specific patterns are checked
# first at equal offsets.
_PATTERNS: Final[tuple[tuple[PiiKind, float, re.Pattern[str]], ...]] = (
    (PiiKind.PRIVATE_KEY, 0.99, re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    (PiiKind.BEARER_TOKEN, 0.90, re.compile(r"\bBearer\s+[A-Za-z0-9._\-]{20,}")),
    (
        PiiKind.API_KEY,
        0.85,
        re.compile(
            r"\b(sk-[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16}|ghp_[A-Za-z0-9]{30,}"
            r"|xox[baprs]-[A-Za-z0-9\-]{10,})\b"
        ),
    ),
    (PiiKind.EMAIL, 0.90, re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]{2,}\b")),
    (PiiKind.IBAN, 0.85, re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{10,30}\b")),
    (
        PiiKind.CREDIT_CARD,
        0.80,
        re.compile(r"\b(?:\d[ -]?){13,19}\b"),
    ),
    (
        PiiKind.PHONE,
        0.70,
        re.compile(r"(?<![\w.])(?:\+\d{1,3}[ -]?)?\(?\d{3}\)?[ -]?\d{3}[ -]?\d{4}(?![\w.])"),
    ),
    (
        PiiKind.IP_ADDRESS,
        0.75,
        re.compile(
            r"\b(?:(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\.){3}"
            r"(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)\b"
        ),
    ),
    (
        PiiKind.NATIONAL_ID,
        0.60,
        re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    ),
    (
        PiiKind.DATE_OF_BIRTH,
        0.50,
        re.compile(r"\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\b"),
    ),
)

_MIN_CONFIDENCE: Final[float] = 0.0


def scan(text: str, *, max_chars: int = 200_000) -> PiiReport:
    """Scans text for personal data and credentials.

    Args:
        text: The text to scan.
        max_chars: How much to examine.

    Returns:
        A :class:`PiiReport`.
    """
    if not text:
        return PiiReport((), 0)

    scanned = text[:max_chars]
    findings: list[PiiFinding] = []

    for kind, confidence, pattern in _PATTERNS:
        for match in pattern.finditer(scanned):
            findings.append(
                PiiFinding(
                    kind=kind,
                    start=match.start(),
                    end=match.end(),
                    confidence=confidence,
                    masked=_mask(match.group(0)),
                )
            )

    findings.sort(key=lambda finding: (finding.start, -finding.confidence))

    if findings:
        _LOGGER.info(
            "pii detected",
            extra={
                "event": "guardrail.pii",
                "count": len(findings),
                "kinds": sorted({finding.kind.value for finding in findings}),
            },
        )
    return PiiReport(tuple(findings), len(scanned))


def redact(
    text: str,
    *,
    policy: PiiPolicy | None = None,
) -> tuple[str, tuple[PiiFinding, ...]]:
    """Applies a policy, returning the new text and what was acted on.

    Redaction proceeds from the end of the text backwards, so earlier offsets
    stay valid as replacements change the length.

    Args:
        text: The text to process.
        policy: Decides the action per finding. Defaults to
            :class:`RedactSecretsOnly`.

    Returns:
        ``(text, redacted_findings)``.

    Raises:
        PiiPolicyDeniedError: If the policy returns ``BLOCK``.
    """
    policy = policy or RedactSecretsOnly()
    report = scan(text)
    if not report.detected:
        return text, ()

    from mas_orchestrator.contracts.errors import PiiPolicyDeniedError

    blocked = [finding for finding in report.findings if policy.action_for(finding) is Action.BLOCK]
    if blocked:
        raise PiiPolicyDeniedError(
            "content contains data blocked by the PII policy",
            details={"kinds": sorted({finding.kind.value for finding in blocked})},
        )

    to_redact = [
        finding for finding in report.findings if policy.action_for(finding) is Action.REDACT
    ]
    if not to_redact:
        return text, ()

    result = text
    for finding in sorted(to_redact, key=lambda item: -item.start):
        placeholder = f"[{finding.kind.value.upper()}_REDACTED]"
        result = result[: finding.start] + placeholder + result[finding.end :]

    return result, tuple(to_redact)


def contains_secrets(text: str) -> bool:
    """Whether text appears to contain a credential.

    Cheap pre-check for logging and emissions: a secret must not reach a log
    line or an event payload even before any policy runs.
    """
    return any(finding.kind in RedactSecretsOnly.SECRET_KINDS for finding in scan(text).findings)


def _mask(value: str) -> str:
    """Masks a value for logging, keeping only its shape."""
    if len(value) <= 8:
        return "*" * len(value)
    visible = 2 if len(value) < 20 else 4
    return f"{value[:visible]}{'*' * (len(value) - visible * 2)}{value[-visible:]}"
