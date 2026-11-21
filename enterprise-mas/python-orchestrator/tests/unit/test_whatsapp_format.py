"""WhatsApp text segmentation and media intake (real assertions)."""

from __future__ import annotations

import pytest

from mas_orchestrator.channels import split_by_limit as exported_split
from mas_orchestrator.channels.whatsapp_media import (
    MediaIntakeError,
    UnsupportedTranscriber,
    UnsupportedTranscriptionError,
    split_by_limit,
    validate_media,
)


class TestSplitByLimit:
    def test_split_by_limit_respects_paragraphs(self) -> None:
        paragraphs = ["First paragraph stays whole.", "Second paragraph too.", "Third one as well."]
        text = "\n\n".join(paragraphs)
        segments = split_by_limit(text, limit=40)
        # each paragraph fits alone in 40 chars, so every segment boundary must
        # fall on a paragraph boundary — never mid-paragraph
        assert segments == paragraphs
        assert all(len(segment) <= 40 for segment in segments)
        # no content lost or reordered
        assert "\n\n".join(segments) == text

    def test_paragraphs_are_packed_until_the_limit(self) -> None:
        paragraphs = ["aaa", "bbb", "ccc"]
        text = "\n\n".join(paragraphs)
        # "aaa\n\nbbb" is 9 chars: fits together; "ccc" needs its own segment
        segments = split_by_limit(text, limit=10)
        assert segments == ["aaa\n\nbbb", "ccc"]

    def test_oversized_paragraphs_are_hard_split(self) -> None:
        text = "x" * 95
        segments = split_by_limit(text, limit=40)
        assert [len(s) for s in segments] == [40, 40, 15]
        assert "".join(segments) == text

    def test_empty_input_yields_no_segments(self) -> None:
        assert split_by_limit("") == []
        assert split_by_limit("kept", limit=4) == ["kept"]

    def test_invalid_limit_is_refused(self) -> None:
        with pytest.raises(ValueError):
            split_by_limit("x", limit=0)

    def test_exported_helper_matches(self) -> None:
        assert exported_split is split_by_limit


class TestMediaIntake:
    def test_empty_media_is_refused(self) -> None:
        with pytest.raises(MediaIntakeError):
            validate_media(mime_type="audio/ogg", byte_length=0)

    def test_oversized_media_is_refused(self) -> None:
        with pytest.raises(MediaIntakeError):
            validate_media(mime_type="audio/ogg", byte_length=17 * 1024 * 1024)

    def test_unlisted_mime_is_refused(self) -> None:
        with pytest.raises(MediaIntakeError):
            validate_media(mime_type="application/x-msdownload", byte_length=10)

    def test_valid_media_passes(self) -> None:
        validate_media(mime_type="audio/ogg; codecs=opus", byte_length=1024)


class TestTranscriberHook:
    async def test_default_hook_refuses_explicitly(self) -> None:
        hook = UnsupportedTranscriber()
        with pytest.raises(UnsupportedTranscriptionError):
            await hook.transcribe(b"bytes", mime_type="audio/ogg")
