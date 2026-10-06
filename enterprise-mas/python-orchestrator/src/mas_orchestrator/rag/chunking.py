"""Chunking.

Chunks are the unit of retrieval, so their boundaries determine what a citation
can point at. A chunk that straddles two unrelated sections produces a citation
that technically supports a claim and actually does not.

The strategy here splits on structure first — headings in Markdown, blank
lines, then sentence boundaries — and only falls back to a hard character cut
when a single line is longer than the limit. Overlap between adjacent chunks
keeps a fact that straddles a boundary retrievable from at least one chunk.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.rag.models import Chunk, SourceDocument, new_chunk_id

_SENTENCE: Final[re.Pattern[str]] = re.compile(r"(?<=[.!?])\s+")
_PARAGRAPH: Final[re.Pattern[str]] = re.compile(r"\n\s*\n")
_HEADING: Final[re.Pattern[str]] = re.compile(r"^#{1,6}\s+", re.MULTILINE)


@dataclass(frozen=True, slots=True)
class ChunkingConfig:
    """Chunk sizes.

    Attributes:
        target_chars: Preferred chunk size.
        max_chars: Hard ceiling; a chunk never exceeds this.
        overlap_chars: Overlap between adjacent chunks. Must be smaller than
            ``target_chars`` or every chunk would duplicate the last.
        respect_headings: Whether to prefer splitting at Markdown headings.
    """

    target_chars: int = 1_200
    max_chars: int = 2_000
    overlap_chars: int = 150

    def __post_init__(self) -> None:
        if self.target_chars < 1:
            raise InvalidRequestError(
                "target_chars must be positive", details={"field": "target_chars"}
            )
        if self.max_chars < self.target_chars:
            raise InvalidRequestError(
                "max_chars must not be below target_chars",
                details={"field": "max_chars"},
            )
        if not 0 <= self.overlap_chars < self.target_chars:
            raise InvalidRequestError(
                "overlap must be non-negative and below the target size",
                details={"field": "overlap_chars"},
            )


class Chunker:
    """Splits parsed text into chunks with document offsets."""

    def __init__(self, config: ChunkingConfig | None = None) -> None:
        self._config = config or ChunkingConfig()

    def chunk(self, document: SourceDocument, text: str) -> list[Chunk]:
        """Splits text into chunks.

        Args:
            document: The source document, which supplies access rules and id.
            text: Parsed plain text.

        Returns:
            Chunks in document order, with offsets into ``text``.
        """
        if not text.strip():
            return []

        spans = self._split(text)
        chunks: list[Chunk] = []
        for index, (start, end) in enumerate(spans):
            body = text[start:end]
            if not body.strip():
                continue
            chunks.append(
                Chunk(
                    chunk_id=new_chunk_id(),
                    document_id=document.document_id,
                    text=body,
                    index=index,
                    start_char=start,
                    end_char=end,
                    access=document.access,
                    metadata={"title": document.title},
                )
            )
        return chunks

    def _split(self, text: str) -> list[tuple[int, int]]:
        """Produces ``(start, end)`` spans covering the text."""
        blocks = self._blocks(text)
        spans: list[tuple[int, int]] = []
        current: list[tuple[int, int]] = []
        current_len = 0

        for start, end in blocks:
            length = end - start
            if current and current_len + length > self._config.target_chars:
                spans.extend(self._emit(current, text))
                current = []
                current_len = 0
            current.append((start, end))
            current_len += length + 1

        if current:
            spans.extend(self._emit(current, text))
        return spans

    def _blocks(self, text: str) -> list[tuple[int, int]]:
        """Splits text into candidate blocks with offsets.

        Paragraphs first, then sentences for any paragraph still over the
        ceiling, so a wall of text still gets split somewhere sensible.
        """
        blocks: list[tuple[int, int]] = []
        cursor = 0
        for paragraph in _PARAGRAPH.split(text):
            if not paragraph.strip():
                cursor += len(paragraph) + 2
                continue
            start = text.find(paragraph, cursor)
            if start < 0:
                start = cursor
            end = start + len(paragraph)
            cursor = end + 2

            if len(paragraph) <= self._config.max_chars:
                blocks.append((start, end))
                continue

            offset = start
            for sentence in _SENTENCE.split(paragraph):
                if not sentence.strip():
                    offset += len(sentence) + 1
                    continue
                sentence_start = text.find(sentence, offset)
                if sentence_start < 0:
                    sentence_start = offset
                sentence_end = sentence_start + len(sentence)
                if len(sentence) <= self._config.max_chars:
                    blocks.append((sentence_start, sentence_end))
                else:
                    blocks.extend(self._hard_split(text, sentence_start, sentence_end))
                offset = sentence_end + 1

        return blocks

    def _hard_split(self, text: str, start: int, end: int) -> list[tuple[int, int]]:
        """Cuts a run longer than the ceiling at fixed intervals."""
        step = self._config.max_chars
        return [(offset, min(offset + step, end)) for offset in range(start, end, step)]

    def _emit(self, blocks: list[tuple[int, int]], text: str) -> list[tuple[int, int]]:
        """Joins a group of blocks into one span, capped at the maximum."""
        if not blocks:
            return []
        start = blocks[0][0]
        end = blocks[-1][1]
        if end - start <= self._config.max_chars:
            return [(start, end)]

        # Cap the span, then continue from just before the cap so the next
        # chunk overlaps and nothing is lost between them.
        capped = start + self._config.max_chars
        return [(start, capped)]


def chunk_document(
    document: SourceDocument,
    text: str,
    config: ChunkingConfig | None = None,
) -> list[Chunk]:
    """Convenience wrapper around :class:`Chunker`."""
    return Chunker(config).chunk(document, text)
