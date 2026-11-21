"""Channel adapters (WhatsApp media pipeline and friends)."""

from __future__ import annotations

from mas_orchestrator.channels.whatsapp_media import (
    DocumentParser,
    MediaIntakeError,
    Transcriber,
    Transcript,
    UnsupportedTranscriber,
    UnsupportedTranscriptionError,
    WhatsappMediaPipeline,
    split_by_limit,
    validate_media,
)

__all__ = [
    "DocumentParser",
    "MediaIntakeError",
    "Transcriber",
    "Transcript",
    "UnsupportedTranscriber",
    "UnsupportedTranscriptionError",
    "WhatsappMediaPipeline",
    "split_by_limit",
    "validate_media",
]
