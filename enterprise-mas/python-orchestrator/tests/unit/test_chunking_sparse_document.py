"""Regression tests for chunk spans with long structural separators."""

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.rag.chunking import Chunker, ChunkingConfig
from mas_orchestrator.rag.models import (
    MAX_CHUNK_CHARS,
    DocumentAccess,
    DocumentFormat,
    SourceDocument,
)


def test_chunk_configuration_cannot_exceed_index_row_bound() -> None:
    ChunkingConfig(target_chars=MAX_CHUNK_CHARS, max_chars=MAX_CHUNK_CHARS)
    with pytest.raises(InvalidRequestError, match="maximum indexable chunk size"):
        ChunkingConfig(target_chars=MAX_CHUNK_CHARS, max_chars=MAX_CHUNK_CHARS + 1)


def test_long_separator_does_not_drop_following_paragraph() -> None:
    """A size cap must not truncate subsequent meaningful document content."""
    text = "first paragraph" + "\n\n" + " " * 350 + "\n\n" + "final paragraph"
    document = SourceDocument(
        document_id="document",
        title="Document",
        content=text.encode(),
        format=DocumentFormat.TEXT,
        access=DocumentAccess(tenant_id="tenant"),
    )
    chunks = Chunker(ChunkingConfig(target_chars=40, max_chars=100, overlap_chars=10)).chunk(
        document, text
    )

    assert any("first paragraph" in chunk.text for chunk in chunks)
    assert any("final paragraph" in chunk.text for chunk in chunks)
    assert all(len(chunk.text) <= 100 for chunk in chunks)
    assert all(chunk.text == text[chunk.start_char : chunk.end_char] for chunk in chunks)
