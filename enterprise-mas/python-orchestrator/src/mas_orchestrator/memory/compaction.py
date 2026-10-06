"""Memory compaction.

An execution that runs for a long time accumulates more working memory than will
fit in a prompt. Compaction reduces a set of records to something that does fit.

The rule that shapes this module: **compaction must not invent**. A summary that
adds a fact nobody said is worse than dropping the record, because a dropped
record is merely forgotten while an invented one is believed. So the strategies
here only ever *remove* or *restate*:

* :attr:`Strategy.TRUNCATE` — keep the newest records, whole and unchanged.
* :attr:`Strategy.HEAD_TAIL` — keep the oldest and newest, which preserves the
  start of the run and where it currently is, and drop the middle.
* :attr:`Strategy.DEDUPLICATE` — drop exact repeats, keeping the first.

There is deliberately no "summarise with a model" strategy. That would be a
model call whose output is then treated as memory, which is exactly the
poisoning path :mod:`~mas_orchestrator.memory.long_term` refuses.

Compaction is also lossy, and :class:`CompactionResult` records what was lost
rather than hiding it.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import InvalidRequestError

if TYPE_CHECKING:
    from mas_orchestrator.memory.models import MemoryRecord

#: Hard ceiling on what a compaction result may occupy.
DEFAULT_BUDGET_CHARS: Final[int] = 16_000


class Strategy(StrEnum):
    """How a set is reduced."""

    TRUNCATE = "truncate"
    HEAD_TAIL = "head_tail"
    DEDUPLICATE = "deduplicate"


@dataclass(frozen=True, slots=True)
class CompactionResult:
    """What compaction produced.

    Attributes:
        kept: Records retained, in their original relative order.
        dropped: How many records were removed.
        strategy: The strategy used.
        budget_chars: The budget applied.
        truncated_content: Whether any record's content was shortened.
    """

    kept: tuple[MemoryRecord, ...]
    dropped: int
    strategy: Strategy
    budget_chars: int
    truncated_content: bool = False

    @property
    def lossy(self) -> bool:
        """Whether anything was lost."""
        return self.dropped > 0 or self.truncated_content

    @property
    def chars(self) -> int:
        """How many characters the kept records occupy."""
        return sum(len(record.content) for record in self.kept)


def needs_compaction(
    records: list[MemoryRecord], *, budget_chars: int = DEFAULT_BUDGET_CHARS
) -> bool:
    """Whether a set exceeds the budget."""
    if budget_chars < 1:
        raise InvalidRequestError(
            "budget_chars must be at least 1", details={"budget_chars": budget_chars}
        )
    return total_chars(records) > budget_chars


def total_chars(records: list[MemoryRecord]) -> int:
    """How many characters a set occupies."""
    return sum(len(record.content) for record in records)


def deduplicate(records: list[MemoryRecord]) -> list[MemoryRecord]:
    """Drops exact repeats of content, keeping the first occurrence."""
    seen: set[str] = set()
    kept: list[MemoryRecord] = []
    for record in records:
        key = record.content.strip()
        if key in seen:
            continue
        seen.add(key)
        kept.append(record)
    return kept


def take_newest(records: list[MemoryRecord], *, budget_chars: int) -> list[MemoryRecord]:
    """Keeps the most recent records that fit."""
    kept: list[MemoryRecord] = []
    used = 0
    for record in reversed(records):
        size = len(record.content)
        if used + size > budget_chars:
            break
        kept.append(record)
        used += size
    kept.reverse()
    return kept


def take_head_tail(
    records: list[MemoryRecord],
    *,
    budget_chars: int,
    head_fraction: float = 0.3,
) -> list[MemoryRecord]:
    """Keeps the oldest and newest records, dropping the middle.

    The head is what started the run; the tail is where it currently is. The
    middle is the part most likely to be reconstructible from the other two.
    """
    if not 0.0 <= head_fraction <= 1.0:
        raise InvalidRequestError(
            "head_fraction must be between 0 and 1",
            details={"head_fraction": head_fraction},
        )
    head_budget = int(budget_chars * head_fraction)
    tail_budget = budget_chars - head_budget

    head: list[MemoryRecord] = []
    used = 0
    for record in records:
        size = len(record.content)
        if used + size > head_budget:
            break
        head.append(record)
        used += size

    head_ids = {record.record_id for record in head}

    tail: list[MemoryRecord] = []
    used = 0
    for record in reversed(records):
        if record.record_id in head_ids:
            continue
        size = len(record.content)
        if used + size > tail_budget:
            break
        tail.append(record)
        used += size
    tail.reverse()

    return head + tail


def compact(
    records: list[MemoryRecord],
    *,
    budget_chars: int = DEFAULT_BUDGET_CHARS,
    strategy: Strategy = Strategy.HEAD_TAIL,
    deduplicate_first: bool = True,
) -> CompactionResult:
    """Reduces a set of records to fit a character budget.

    Args:
        records: The records to reduce, oldest first.
        budget_chars: How much the result may occupy.
        strategy: How to reduce.
        deduplicate_first: Whether to drop exact repeats before applying the
            strategy. Enabled by default because it is free and never loses
            information.

    Returns:
        A :class:`CompactionResult`.

    Raises:
        InvalidRequestError: If the budget is not positive.
    """
    if budget_chars < 1:
        raise InvalidRequestError(
            "budget_chars must be at least 1", details={"budget_chars": budget_chars}
        )
    if not records:
        return CompactionResult(kept=(), dropped=0, strategy=strategy, budget_chars=budget_chars)

    working = deduplicate(records) if deduplicate_first else list(records)
    dropped = len(records) - len(working)

    if total_chars(working) <= budget_chars:
        return CompactionResult(
            kept=tuple(working),
            dropped=dropped,
            strategy=strategy,
            budget_chars=budget_chars,
        )

    if strategy is Strategy.TRUNCATE:
        kept = take_newest(working, budget_chars=budget_chars)
    elif strategy is Strategy.HEAD_TAIL:
        kept = take_head_tail(working, budget_chars=budget_chars)
    else:
        kept = working

    # Last resort: if a single record is larger than the whole budget, shorten
    # it rather than returning nothing. Truncation is recorded so the caller
    # knows the content is incomplete — a silently clipped record would be
    # read as the whole thing.
    truncated = False
    if not kept and working:
        largest = max(working, key=lambda record: len(record.content))
        clipped = _clip(largest, budget_chars)
        kept = [clipped]
        truncated = clipped is not largest

    return CompactionResult(
        kept=tuple(kept),
        dropped=len(records) - len(kept),
        strategy=strategy,
        budget_chars=budget_chars,
        truncated_content=truncated,
    )


def _clip(record: MemoryRecord, budget_chars: int) -> MemoryRecord:
    """Shortens a record's content to fit the budget."""
    if len(record.content) <= budget_chars:
        return record
    from dataclasses import replace

    marker = " …[truncated]"
    keep = max(0, budget_chars - len(marker))
    return replace(record, content=record.content[:keep] + marker)
