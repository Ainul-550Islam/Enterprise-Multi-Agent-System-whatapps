"""WhatsApp media pipeline: size-safe media intake, transcription hook and
WhatsApp-shaped text segmentation.

The module owns three jobs and nothing else:

* **intake validation** — media bytes are capped and typed before anything
  else looks at them (a hostile payload never reaches a parser);
* **transcription** — audio goes through the :class:`Transcriber` protocol.
  The default hook refuses explicitly (:class:`UnsupportedTranscriptionError`)
  instead of silently dropping audio;
* **segmentation** — :func:`split_by_limit` splits a reply into WhatsApp-sized
  segments respecting paragraph boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from mas_orchestrator.rag.models import DocumentFormat

#: WhatsApp hard limit for one text message body.
WHATSAPP_TEXT_LIMIT: int = 4096

#: Hard cap for an inbound media download (bytes) before it is refused.
MAX_MEDIA_BYTES: int = 16 * 1024 * 1024

#: Media mime types the intake accepts.
ALLOWED_MEDIA_MIMES: frozenset[str] = frozenset(
    {
        "audio/ogg",
        "audio/ogg; codecs=opus",
        "audio/mpeg",
        "audio/mp4",
        "image/jpeg",
        "image/png",
        "image/webp",
        "video/mp4",
        "application/pdf",
    }
)


class MediaIntakeError(ValueError):
    """Inbound media failed intake validation (size or mime policy)."""


class UnsupportedTranscriptionError(RuntimeError):
    """Raised when no transcription provider is configured.

    Explicit by design: an audio message must either be transcribed or be
    answered with a clear unsupported error the operator can act on. Silent
    drops look like success and lose customer messages.
    """


@dataclass(frozen=True, slots=True)
class Transcript:
    """The result of transcribing one media object."""

    text: str
    language: str | None
    provider: str


@runtime_checkable
class Transcriber(Protocol):
    """Hook for turning WhatsApp audio into text.

    Implementations receive the raw media bytes exactly as downloaded and
    must enforce their own provider limits. The pipeline never caches audio
    to disk.
    """

    async def transcribe(
        self,
        audio: bytes,
        *,
        mime_type: str,
        language: str | None = None,
    ) -> Transcript:
        """Transcribe `audio`, or raise a typed error."""
        ...


class UnsupportedTranscriber:
    """Default hook: refuses explicitly, never silently."""

    async def transcribe(
        self,
        audio: bytes,
        *,
        mime_type: str,
        language: str | None = None,
    ) -> Transcript:
        raise UnsupportedTranscriptionError(
            "no transcription provider is configured for WhatsApp audio; "
            "install a Transcriber implementation (channels/whatsapp_media.py) "
            f"or answer unsupported for {mime_type!r}"
        )


def validate_media(*, mime_type: str, byte_length: int) -> None:
    """Applies the intake policy; raises :class:`MediaIntakeError`."""
    if byte_length <= 0:
        raise MediaIntakeError("empty media object")
    if byte_length > MAX_MEDIA_BYTES:
        raise MediaIntakeError(
            f"media object of {byte_length} bytes exceeds the {MAX_MEDIA_BYTES} byte cap"
        )
    if mime_type not in ALLOWED_MEDIA_MIMES:
        raise MediaIntakeError(f"mime type {mime_type!r} is not on the media allowlist")


def split_by_limit(text: str, limit: int = WHATSAPP_TEXT_LIMIT) -> list[str]:
    """Splits `text` into segments of at most `limit` characters.

    Paragraph boundaries (``\\n\\n``) are preserved whenever a paragraph fits
    in one segment; paragraphs longer than `limit` are hard-split. Empty
    input yields no segments; whitespace-only paragraphs are kept (they are
    intentional spacing).
    """
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if not text:
        return []
    paragraphs = text.split("\n\n")
    segments: list[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = f"{current}\n\n{paragraph}" if current else paragraph
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            segments.append(current)
            current = ""
        pieces = _split_oversized_paragraph(paragraph, limit)
        if not pieces:
            continue
        *complete, last = pieces
        segments.extend(complete)
        current = last
    if current:
        segments.append(current)
    return segments


def _split_oversized_paragraph(paragraph: str, limit: int) -> list[str]:
    """Splits one paragraph that does not fit: sentence boundary, then hard."""
    if len(paragraph) <= limit:
        return [paragraph] if paragraph else []
    segments: list[str] = []
    remaining = paragraph
    while len(remaining) > limit:
        window = remaining[:limit]
        cut = max(window.rfind(". "), window.rfind("! "), window.rfind("? "))
        if cut >= max(1, limit // 4):
            cut += 1  # keep the punctuation; drop the following space
            segments.append(remaining[:cut].rstrip())
            remaining = remaining[cut:].lstrip()
            continue
        segments.append(remaining[:limit])
        remaining = remaining[limit:]
    if remaining:
        segments.append(remaining)
    return segments


class WhatsappMediaPipeline:
    """Intake + transcription + document parse + segmentation in one place."""

    def __init__(
        self,
        transcriber: Transcriber | None = None,
        document_parser: DocumentParser | None = None,
    ) -> None:
        self._transcriber: Transcriber = transcriber or UnsupportedTranscriber()
        self._document_parser = document_parser

    async def transcribe_audio(
        self,
        audio: bytes,
        *,
        mime_type: str,
        language: str | None = None,
    ) -> Transcript:
        """Validates intake then delegates to the configured transcriber."""
        validate_media(mime_type=mime_type, byte_length=len(audio))
        return await self._transcriber.transcribe(audio, mime_type=mime_type, language=language)

    async def parse_document(self, document: bytes, *, mime_type: str) -> str:
        """Validates intake then parses a document through the RAG parser pool."""
        validate_media(mime_type=mime_type, byte_length=len(document))
        fmt = _document_format(mime_type)
        parser = self._document_parser
        if parser is None:
            from mas_orchestrator.rag.parser_pool import ParserPool

            pool = ParserPool()
            try:
                return await pool.parse(document, fmt)
            finally:
                await pool.aclose()
        return await parser.parse(document, fmt)

    def segment_reply(self, text: str) -> list[str]:
        """Segments a reply body into WhatsApp-sized sends."""
        return split_by_limit(text)


@runtime_checkable
class DocumentParser(Protocol):
    """Hook for turning an allowed document into text (ParserPool in production)."""

    async def parse(self, content: str | bytes, format: DocumentFormat) -> str:  # noqa: A002
        """Parse `content` as `format` and return extracted text."""
        ...


def _document_format(mime_type: str) -> DocumentFormat:
    base = mime_type.split(";", 1)[0].strip().lower()
    if base == "application/pdf":
        return DocumentFormat.PDF
    raise MediaIntakeError(f"mime type {mime_type!r} is not a parsable WhatsApp document")
