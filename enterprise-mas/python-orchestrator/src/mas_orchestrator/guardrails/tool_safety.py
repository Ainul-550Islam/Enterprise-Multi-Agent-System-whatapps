"""Tool safety classification.

The tool executor asks whether a call is permitted. This module answers the
prior question: what kind of call is this, and what does that imply about
approval, idempotency and blast radius?

The classification is deliberately conservative. A tool whose safety class is
unknown is treated as :attr:`SafetyClass.SENSITIVE` rather than standard,
because the cost of asking for approval unnecessarily is a delay, while the
cost of not asking is an action nobody authorised.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from mas_orchestrator.contracts.errors import InvalidRequestError


class SafetyClass(StrEnum):
    """How much damage a call could do.

    Ordered from least to most consequential. The ordering is used by
    :func:`requires_approval`, so a new class inserted in the middle changes
    behaviour everywhere consistently.
    """

    READ_ONLY = "read_only"
    STANDARD = "standard"
    SENSITIVE = "sensitive"
    DESTRUCTIVE = "destructive"


_CLASS_ORDER: tuple[SafetyClass, ...] = (
    SafetyClass.READ_ONLY,
    SafetyClass.STANDARD,
    SafetyClass.SENSITIVE,
    SafetyClass.DESTRUCTIVE,
)


def ordinal_of(safety: SafetyClass) -> int:
    """Position of a class in the ordering."""
    return _CLASS_ORDER.index(safety)


@dataclass(frozen=True, slots=True)
class SafetyAssessment:
    """What a tool's safety settings imply.

    Attributes:
        safety: The effective class.
        requires_approval: Whether a human must approve before the call.
        repeatable: Whether the call may be retried after a transient failure.
        reversible: Whether the effect can be undone.
        reason: Why, for the audit trail.
    """

    safety: SafetyClass
    requires_approval: bool
    repeatable: bool
    reversible: bool
    reason: str


def classify(
    *,
    safety: str,
    idempotent: bool,
    requires_approval: bool = False,
    reversible: bool | None = None,
) -> SafetyAssessment:
    """Derives the full safety posture of a tool call.

    Args:
        safety: The declared safety class, or an unrecognised string.
        idempotent: Whether repeating the call is safe.
        requires_approval: Whether the tool itself demands approval.
        reversible: Whether the effect can be undone. Defaults to the class
            default when not given.

    Returns:
        A :class:`SafetyAssessment`.
    """
    try:
        resolved = SafetyClass(safety)
    except ValueError:
        # An unrecognised class is treated as sensitive. Refusing outright
        # would break a deployment that adds a class before Python learns it;
        # treating it as read-only would be the dangerous mistake.
        resolved = SafetyClass.SENSITIVE

    approval = requires_approval or resolved in (
        SafetyClass.SENSITIVE,
        SafetyClass.DESTRUCTIVE,
    )

    # A destructive call is not repeatable even if it claims idempotency: the
    # blast radius is too large to risk a duplicate on a retry.
    repeatable = idempotent and resolved not in (
        SafetyClass.SENSITIVE,
        SafetyClass.DESTRUCTIVE,
    )

    if reversible is None:
        reversible = resolved in (SafetyClass.READ_ONLY, SafetyClass.STANDARD)

    return SafetyAssessment(
        safety=resolved,
        requires_approval=approval,
        repeatable=repeatable,
        reversible=reversible,
        reason=_reason_for(resolved, requires_approval, idempotent),
    )


def _reason_for(
    safety: SafetyClass,
    declared_approval: bool,
    idempotent: bool,
) -> str:
    """Explains the assessment, for an audit record."""
    if safety is SafetyClass.DESTRUCTIVE:
        return "destructive tool: approval required and retries suppressed"
    if safety is SafetyClass.SENSITIVE:
        return "sensitive tool: approval required and retries suppressed"
    if declared_approval:
        return "tool declares that it requires approval"
    if not idempotent:
        return "tool is not idempotent, so it must not be retried"
    return "standard tool: may run without approval"


def requires_approval(safety: SafetyClass) -> bool:
    """Whether a class needs human approval before the call."""
    return ordinal_of(safety) >= ordinal_of(SafetyClass.SENSITIVE)


def is_repeatable(safety: SafetyClass, *, idempotent: bool) -> bool:
    """Whether a call may be retried after a transient failure."""
    return idempotent and ordinal_of(safety) < ordinal_of(SafetyClass.SENSITIVE)


def strictest(left: SafetyClass, right: SafetyClass) -> SafetyClass:
    """The more consequential of two classes.

    Used when a batch of tool calls is assessed as a whole: the batch inherits
    the strictest class among its members, so one destructive call cannot be
    diluted by the read-only calls around it.
    """
    return left if ordinal_of(left) >= ordinal_of(right) else right


def parse_safety(raw: str) -> SafetyClass:
    """Parses a class name, refusing an unknown one.

    Unlike :func:`classify`, this raises rather than defaulting. Use it where
    an unrecognised value is a configuration error rather than something to be
    handled defensively.
    """
    try:
        return SafetyClass(raw)
    except ValueError as error:
        raise InvalidRequestError(
            f"unknown tool safety class: {raw!r}",
            details={"expected": [member.value for member in SafetyClass]},
        ) from error
