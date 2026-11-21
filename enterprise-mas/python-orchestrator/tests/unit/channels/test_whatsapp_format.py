"""WhatsApp segmentation at the 4096 limit, including sentence boundaries."""

from __future__ import annotations

from mas_orchestrator.channels.whatsapp_media import (
    WHATSAPP_TEXT_LIMIT,
    WhatsappMediaPipeline,
    split_by_limit,
)
from mas_orchestrator.rag.models import DocumentFormat


def test_split_by_limit_respects_paragraphs() -> None:
    first = "A" * 100
    second = "B" * 100
    text = f"{first}\n\n{second}"
    segments = split_by_limit(text, limit=WHATSAPP_TEXT_LIMIT)
    assert segments == [text]
    assert all(len(segment) <= WHATSAPP_TEXT_LIMIT for segment in segments)
    assert "\n\n".join(segments) == text


def test_default_limit_is_whatsapp_4096() -> None:
    text = "x" * (WHATSAPP_TEXT_LIMIT + 10)
    segments = split_by_limit(text)
    assert WHATSAPP_TEXT_LIMIT == 4096
    assert [len(segment) for segment in segments] == [4096, 10]
    assert "".join(segments) == text


def test_oversized_paragraph_splits_on_sentence_boundary() -> None:
    sentence = "This is a complete sentence."
    paragraph = (" " + sentence) * 200
    paragraph = paragraph.strip()
    assert len(paragraph) > WHATSAPP_TEXT_LIMIT
    segments = split_by_limit(paragraph, limit=WHATSAPP_TEXT_LIMIT)
    assert all(len(segment) <= WHATSAPP_TEXT_LIMIT for segment in segments)
    assert all(segment.endswith(".") for segment in segments[:-1])
    assert " ".join(segments) == paragraph


class _FakeParser:
    def __init__(self) -> None:
        self.calls: list[tuple[bytes, DocumentFormat]] = []

    async def parse(self, content: str | bytes, format: DocumentFormat) -> str:  # noqa: A002
        payload = content if isinstance(content, bytes) else content.encode()
        self.calls.append((payload, format))
        return "parsed-pdf"


async def test_document_pdf_goes_through_the_parser_hook() -> None:
    parser = _FakeParser()
    pipeline = WhatsappMediaPipeline(document_parser=parser)
    text = await pipeline.parse_document(b"%PDF-1.4 fake", mime_type="application/pdf")
    assert text == "parsed-pdf"
    assert parser.calls == [(b"%PDF-1.4 fake", DocumentFormat.PDF)]
