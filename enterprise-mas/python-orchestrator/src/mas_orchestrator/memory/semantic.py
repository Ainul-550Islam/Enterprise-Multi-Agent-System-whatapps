"""Semantic memory.

Semantic memory holds *facts* — things that are true independent of what
happened in any particular run: "this tenant's fiscal year starts in April",
"the reporting API returns CSV". It is the scope most worth keeping and the one
most worth being careful about, because a fact learned once is applied to every
future execution.

Two rules follow, and both are enforced here:

* **A semantic memory must be durable.** Writing a fact into task scope would
  discard it at the end of the run, which is either a bug or a very expensive
  cache.
* **A semantic memory must not be about one execution.** Content that
  references the current run's ids or results is episodic, and storing it as
  semantic produces a "fact" that is false for every other run.
"""

from __future__ import annotations

import logging
import re
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.memory.models import (
    MAX_MEMORY_CONTENT_CHARS,
    MemoryRecord,
    MemoryScope,
    Sensitivity,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Content mentioning one of these is about a *run*, not about the world.
_RUN_SPECIFIC_TOKENS: Final[frozenset[str]] = frozenset(
    {
        "this execution",
        "this run",
        "this task",
        "this attempt",
        "the previous step",
        "step 1",
        "step one",
        "just now",
        "earlier today",
    }
)

#: Identifiers that tie a statement to one run.
_RUN_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(
    r"\b(?:exec|execution|task|attempt|corr|correlation)[-_ ]?[0-9a-f]{6,}\b",
    re.IGNORECASE,
)


def looks_run_specific(content: str) -> bool:
    """Whether content is about one run rather than a general fact.

    Best-effort. The check exists to catch the common mistake — persisting
    "the task succeeded" as though it were knowledge — not to be a semantic
    classifier.
    """
    lowered = content.lower()
    if any(token in lowered for token in _RUN_SPECIFIC_TOKENS):
        return True
    return bool(_RUN_IDENTIFIER_RE.search(content))


def validate_fact(content: str) -> str:
    """Checks content before it is stored as a fact.

    Returns:
        The trimmed content.

    Raises:
        InvalidRequestError: If the content is empty, oversized, or clearly
            about a single run.
    """
    trimmed = (content or "").strip()
    if not trimmed:
        raise InvalidRequestError(
            "a semantic memory must not be empty", details={"field": "content"}
        )
    if len(trimmed) > MAX_MEMORY_CONTENT_CHARS:
        raise InvalidRequestError(
            "semantic memory exceeds the maximum size",
            details={"limit": MAX_MEMORY_CONTENT_CHARS, "length": len(trimmed)},
        )
    if looks_run_specific(trimmed):
        raise InvalidRequestError(
            "content describes a single run, so it is episodic rather than "
            "semantic: storing it as a fact would make it false for every "
            "other execution",
            details={"field": "content"},
        )
    return trimmed


def build_fact(
    *,
    tenant_id: str,
    content: str,
    record_id: str,
    sensitivity: Sensitivity = Sensitivity.INTERNAL,
    agent_id: str | None = None,
) -> MemoryRecord:
    """Builds a semantic memory record.

    The scope is always :attr:`MemoryScope.SEMANTIC` — a caller cannot
    accidentally file a fact somewhere that will be discarded.
    """
    return MemoryRecord(
        record_id=record_id,
        tenant_id=tenant_id,
        scope=MemoryScope.SEMANTIC,
        content=validate_fact(content),
        sensitivity=sensitivity,
        agent_id=agent_id,
    )
