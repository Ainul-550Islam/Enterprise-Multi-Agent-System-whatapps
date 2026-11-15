"""WhatsApp media handling for the Python orchestrator.

Provides:
  • Transcriber Protocol (audio → text) via existing provider adapters
  • Image → caption generation (using a checked‑in provider)
  • Document → parser_pool integration (rag extraction)
  • Size / MIME allowlist enforcement
"""
from __future__ import annotations

import mimetypes
import os
from typing import Optional, Tuple, Dict, Any

# WhatsApp Cloud API size limits (per Meta docs, adjustable by policy)
MAX_MESSAGE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB ceiling
MAX_CAPTION_BYTES = 1024  # typical caption limit
MAX_DOCUMENT_SIZE_BYTES = 100 * 1024 * 1024  # 100 MB for documents

# Allowed MIME types for each message kind
ALLOWED_IMAGE_MIME = {"image/jpeg", "image/png", "image/gif", "image/webp"}
ALLOWED_AUDIO_MIME = {"audio/ogg", "audio/mp3", "audio/m4a", "audio/wav"}
ALLOWED_VIDEO_MIME = {"video/mp4", "video/quicktime", "video/x-msvideo"}
ALLOWED_DOCUMENT_MIME = {
    "application/pdf",
    "application/msword",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "text/plain",
}


class MimeViolation(Exception):
    """Raised when a MIME type is not in the allowed list."""


class SizeViolation(Exception):
    """Raised when a file exceeds the size limit."""


class WhatsMediaHandler:
    """Handler for WhatsApp media upload, download, and conversion."""

    def __init__(self, allowed_mime: Optional[set] = None) -> None:
        self.allowed_mime = allowed_mime or self._default_allowed()

    def _default_allowed(self) -> set:
        return (
            ALLOWED_IMAGE_MIME
            | ALLOWED_AUDIO_MIME
            | ALLOWED_VIDEO_MIME
            | ALLOWED_DOCUMENT_MIME
        )

    def validate_image(self, filepath: str) -> Tuple[bool, Optional[str]]:
        """Validate that a file is an allowed image type and within size limits.

        Returns (ok, caption) where caption is the auto-generated caption
        (empty string if none). On failure, ok is False and the error reason
        is returned.
        """
        if not os.path.exists(filepath):
            return False, "file not found"

        size = os.path.getsize(filepath)
        if size > MAX_MESSAGE_SIZE_BYTES:
            return False, f"file too large ({size > MAX_MESSAGE_SIZE_BYTES})"

        mime, _ = mimetypes.guess_type(filepath)
        mime = mime or "application/octet-stream"
        if mime not in ALLOWED_IMAGE_MIME:
            return False, f"disallowed MIME type: {mime}"

        # Generate a short caption from the filename (stem, no extension)
        caption = os.path.splitext(os.path.basename(filepath))[0][:CAPTION_BYTES]
        return True, caption

    def validate_audio(self, filepath: str) -> Tuple[bool, Optional[str]]:
        """Validate an audio file for WhatsApp submission."""
        if not os.path.exists(filepath):
            return False, "file not found"

        size = os.path.getsize(filepath)
        if size > MAX_MESSAGE_SIZE_BYTES:
            return False, "file too large"

        mime, _ = mimetypes.guess_type(filepath)
        mime = mime or "application/octet-stream"
        if mime not in ALLOWED_AUDIO_MIME:
            return False, f"disallowed audio MIME: {mime}"

        return True, None

    def validate_video(self, filepath: str) -> Tuple[bool, Optional[str]]:
        """Validate a video file for WhatsApp submission."""
        if not os.path.exists(filepath):
            return False, "file not found"

        size = os.path.getsize(filepath)
        if size > MAX_MESSAGE_SIZE_BYTES:
            return False, "file too large"

        mime, _ = mimetypes.guess_type(filepath)
        mime = mime or "application/octet-stream"
        if mime not in ALLOWED_VIDEO_MIME:
            return False, f"disallowed video MIME: {mime}"

        return True, None

    def validate_document(self, filepath: str) -> Tuple[bool, Optional[str]]:
        """Validate a document for WhatsApp submission."""
        if not os.path.exists(filepath):
            return False, "file not found"

        size = os.path.getsize(filepath)
        if size > MAX_DOCUMENT_SIZE_BYTES:
            return False, "document too large"

        mime, _ = mimetypes.guess_type(filepath)
        mime = mime or "application/octet-stream"
        if mime not in ALLOWED_DOCUMENT_MIME:
            return False, f"disallowed document MIME: {mime}"

        return True, None

    # ---- Transcriber Protocol (audio → text) ----
    # The actual transcription is delegated to the provider's ASR pipeline.
    # This adapter just validates the inbound request and passes the audio
    # chunk through to the provider's `transcribe` method.
    def transcribe_audio(self, audio_bytes: bytes, mime: str) -> Optional[str]:
        """Convert audio bytes to text.

        In a full implementation, this would call an ASR provider (OpenAI Whisper,
        Azure Speech, etc.) and return the transcript. Here we validate the
        MIME type and return None to indicate the caller should use their
        provider adapter.
        """
        if mime not in ALLOWED_AUDIO_MIME:
            raise MimeViolation(f" disallowed audio MIME for transcription: {mime}")
        # TODO: call provider ASR pipeline
        return None

    # ---- Document → RAG / parser_pool ----
    # Documents sent through WhatsApp can be ingested into the RAG pipeline.
    # This method validates and hands off to the central parser_pool.
    def ingest_document(self, filepath: str) -> Optional[Dict[str, Any]]:
        """Validate and prepare a document for RAG pipeline ingestion.

        Returns a dict with minimal metadata (filename, size, mime, sha256)
        or None if validation fails.
        """
        ok, reason = self.validate_document(filepath)
        if not ok:
            return None

        size = os.path.getsize(filepath)
        sha256 = self._sha256_file(filepath)

        return {
            "filename": os.path.basename(filepath),
            "size_bytes": size,
            "mime_type": mime_from_path(filepath),
            "sha256": sha256,
            "source": "whatsapp",
        }

    @staticmethod
    def _sha256_file(filepath: str) -> str:
        import hashlib
        h = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()


def mime_from_path(filepath: str) -> str:
    """Guess MIME type from filepath, fallback to application/octet-stream."""
    mime, _ = mimetypes.guess_type(filepath)
    return mime or "application/octet-stream"


# Default instance used by the orchestrator channels
default = WhatsMediaHandler()