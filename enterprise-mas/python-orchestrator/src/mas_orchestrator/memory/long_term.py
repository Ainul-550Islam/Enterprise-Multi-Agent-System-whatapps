"""Long-term (durable) memory.

Long-term memory is what persists: tenant preferences, standing instructions,
corrections the user has made more than once. It is also the highest-risk scope
in the system, because a durable memory is applied to every future execution and
a poisoned one is applied forever.

The risk this module is built around is **prompt injection via memory**. An
injected instruction that survives one turn is an annoyance; one that is written
to durable memory and replayed for the next six months is a compromise. So:

* :func:`build_durable_memory` runs the content through the injection analyser
  and refuses anything at medium risk or above.
* :func:`is_safe_to_persist` exposes the same check for callers that want to
  decide rather than raise.
* Content is recorded with its sensitivity, and the default is
  :attr:`Sensitivity.INTERNAL` rather than ``PUBLIC``: durable memory is
  retained, and retained data should be assumed non-public.

Note the asymmetry with short-term memory: short-term rejects content that is
about *other* runs; long-term rejects content that tries to *instruct*.
"""

from __future__ import annotations

import logging
import time
from typing import Final

from mas_orchestrator.contracts.errors import (
    InvalidRequestError,
    PromptInjectionDetectedError,
)
from mas_orchestrator.guardrails.prompt_injection import RiskLevel, analyse
from mas_orchestrator.memory.models import (
    MAX_MEMORY_CONTENT_CHARS,
    MemoryQuery,
    MemoryRecord,
    MemoryScope,
    Retention,
    Sensitivity,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Risk at or above which content is refused for durable storage.
PERSISTENCE_RISK_CEILING: Final[RiskLevel] = RiskLevel.MEDIUM

#: Default retention for a durable memory: one year. Durable does not mean
#: forever — an unbounded retention policy is a data-protection problem.
DEFAULT_RETENTION_DAYS: Final[int] = 365


def is_safe_to_persist(content: str) -> bool:
    """Whether content is safe to write to durable memory."""
    return analyse(content).level.ordinal < PERSISTENCE_RISK_CEILING.ordinal


def inspect(content: str) -> tuple[bool, RiskLevel]:
    """The persistence decision and the risk that drove it."""
    report = analyse(content)
    safe = report.level.ordinal < PERSISTENCE_RISK_CEILING.ordinal
    return safe, report.level


def build_durable_memory(
    *,
    tenant_id: str,
    content: str,
    record_id: str,
    agent_id: str | None = None,
    sensitivity: Sensitivity = Sensitivity.INTERNAL,
    retain_days: int = DEFAULT_RETENTION_DAYS,
) -> MemoryRecord:
    """Builds a durable memory record, screening it first.

    Raises:
        InvalidRequestError: If the content is empty, oversized, or the
            retention window is not positive.
        PromptInjectionDetectedError: If the content looks like an instruction
            rather than a fact or preference.
    """
    trimmed = (content or "").strip()
    if not trimmed:
        raise InvalidRequestError(
            "a durable memory must not be empty", details={"field": "content"}
        )
    if len(trimmed) > MAX_MEMORY_CONTENT_CHARS:
        raise InvalidRequestError(
            "durable memory exceeds the maximum size",
            details={"limit": MAX_MEMORY_CONTENT_CHARS, "length": len(trimmed)},
        )
    if retain_days < 1:
        raise InvalidRequestError(
            "retention must be at least one day",
            details={"retain_days": retain_days},
        )

    safe, level = inspect(trimmed)
    if not safe:
        # Refusing is the only safe option. Downgrading to a shorter scope
        # would store the instruction anyway, just for less time.
        raise PromptInjectionDetectedError(
            "content was refused for durable storage because it looks like an "
            "instruction: a durable memory is replayed into every future "
            "execution, so an injected instruction would persist",
            details={"risk_level": level.value},
        )

    now_ms = int(time.time() * 1000)
    return MemoryRecord(
        record_id=record_id,
        tenant_id=tenant_id,
        scope=MemoryScope.DURABLE,
        content=trimmed,
        sensitivity=sensitivity,
        retention=Retention(retain_until_epoch_ms=now_ms + retain_days * 86_400_000),
        agent_id=agent_id,
        created_epoch_ms=now_ms,
    )


def durable_query(
    *,
    tenant_id: str,
    text: str = "",
    agent_id: str | None = None,
    limit: int = 20,
    include_sensitive: bool = False,
) -> MemoryQuery:
    """A query over durable memory.

    ``include_sensitive`` defaults to ``False``: durable memory is the most
    likely place for a credential to have been captured, so reading it back
    unredacted has to be a deliberate decision.
    """
    return MemoryQuery(
        tenant_id=tenant_id,
        scopes=(MemoryScope.DURABLE, MemoryScope.SEMANTIC),
        text=text,
        agent_id=agent_id,
        limit=max(1, limit),
        include_sensitive=include_sensitive,
    )
