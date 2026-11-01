"""Citation validation.

A citation that names a source the model was never given is fabrication, and a
fabricated citation is the most convincing kind of wrong answer: it is
checkable, looks checked, and is not.

This module answers a narrow question — *does every citation point at something
that was actually supplied?* — and it refuses to answer a wider one. It does
not try to judge whether a cited source supports the claim, because that is
the same task as answering the question, and a system cannot grade its own
work.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError

#: ``[1]``, ``[12]``. Deliberately narrow: a bracketed number in prose is far
#: more likely to be a citation than anything else, and widening the pattern
#: would start matching array literals and unit notation.
_CITATION_RE: Final[re.Pattern[str]] = re.compile(r"\[(\d{1,4})\]")

#: Repeated identical citations are usually a copy-paste artefact rather than a
#: real reference, and they inflate the apparent support for a claim.
MAX_REPEATS_PER_CITATION: Final[int] = 3


@dataclass(frozen=True, slots=True)
class CitationReport:
    """What the citations in a piece of output look like.

    Attributes:
        cited: Every citation index found, in order of first appearance.
        available: How many sources were supplied.
        unknown: Citations naming a source that was not supplied.
        repeated: Citations appearing more than the repeat ceiling.
        total: How many citation markers were found.
    """

    cited: tuple[int, ...] = ()
    available: int = 0
    unknown: tuple[int, ...] = ()
    repeated: tuple[int, ...] = ()
    total: int = 0

    @property
    def valid(self) -> bool:
        """Whether every citation names a supplied source."""
        return not self.unknown

    @property
    def suspicious(self) -> bool:
        """Whether the citation pattern looks like padding.

        Repeated citations are not fabrication, but they are a strong signal
        that output was generated to look supported.
        """
        return bool(self.repeated)

    @property
    def coverage(self) -> float:
        """What fraction of the supplied sources were cited.

        Zero sources supplied reports ``0.0`` rather than raising: "cited
        nothing of nothing" is a legitimate answer, not an error.
        """
        if self.available == 0:
            return 0.0
        return len(set(self.cited)) / self.available


def extract_citations(text: str) -> tuple[int, ...]:
    """Returns every citation index in the text, in order of appearance."""
    return tuple(int(match.group(1)) for match in _CITATION_RE.finditer(text))


def check_citations(
    text: str,
    *,
    available: int,
    max_repeats: int = MAX_REPEATS_PER_CITATION,
) -> CitationReport:
    """Validates the citations in a piece of output.

    Args:
        text: The output to check.
        available: How many sources were supplied. Indices are 1-based, so
            ``available=3`` permits ``[1]``, ``[2]`` and ``[3]``.
        max_repeats: How many times one index may appear.

    Returns:
        A :class:`CitationReport`.

    Raises:
        InvalidRequestError: If ``available`` is negative or ``max_repeats``
            is below one.
    """
    if available < 0:
        raise InvalidRequestError(
            "available must not be negative", details={"available": available}
        )
    if max_repeats < 1:
        raise InvalidRequestError(
            "max_repeats must be at least 1", details={"max_repeats": max_repeats}
        )

    found = extract_citations(text)
    counts: dict[int, int] = {}
    for index in found:
        counts[index] = counts.get(index, 0) + 1

    unknown = tuple(sorted(index for index in counts if index < 1 or index > available))
    repeated = tuple(sorted(index for index, count in counts.items() if count > max_repeats))

    return CitationReport(
        cited=tuple(dict.fromkeys(found)),
        available=available,
        unknown=unknown,
        repeated=repeated,
        total=len(found),
    )


def has_unknown_citations(text: str, *, available: int) -> bool:
    """Whether any citation names a source that was not supplied."""
    return not check_citations(text, available=available).valid


def strip_citations(text: str) -> str:
    """Removes citation markers.

    Used when the answer is displayed somewhere citations would be noise. The
    surrounding text is left untouched, so spacing may be uneven.
    """
    return _CITATION_RE.sub("", text)
