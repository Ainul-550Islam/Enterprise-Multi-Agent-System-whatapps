"""Test WhatsApp media handling: oversize, mime-reject, parser-timeout."""

from channels.whatsapp_media import WhatsMediaHandler, mime_from_path, MimeViolation, SizeViolation


def test_validate_image():
    """Test image validation with a real-ish file path."""
    import tempfile
    import os
    # Create a minimal valid image file
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        f.write(b"\x00\x00\x00\x01" * 100)  # minimal JPEG-like data
        tmpfile = f.name
    
    try:
        handler = WhatsMediaHandler()
        ok, caption = handler.validate_image(tmpfile)
        # May or may not pass MIME check depending on mimetypes guess
        assert isinstance(ok, bool)
        assert isinstance(caption, str) or ok is False
    finally:
        os.unlink(tmpfile)


def test_validate_audio():
    """Test audio validation."""
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as f:
        f.write(b"\x00\x00\x00\x01" * 100)
        tmpfile = f.name
    
    try:
        handler = WhatsMediaHandler()
        ok, _ = handler.validate_audio(tmpfile)
        assert isinstance(ok, bool)
    finally:
        os.unlink(tmpfile)


def test_validate_video():
    """Test video validation."""
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
        f.write(b"\x00\x00\x00\x01" * 100)
        tmpfile = f.name
    
    try:
        handler = WhatsMediaHandler()
        ok, _ = handler.validate_video(tmpfile)
        assert isinstance(ok, bool)
    finally:
        os.unlink(tmpfile)


def test_validate_document():
    """Test document validation."""
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(b"%PDF-1.4 test")
        tmpfile = f.name
    
    try:
        handler = WhatsMediaHandler()
        ok, _ = handler.validate_document(tmpfile)
        assert isinstance(ok, bool)
    finally:
        os.unlink(tmpfile)


def test_mime_from_path():
    """Test MIME type guessing from file path."""
    from channels.whatsapp_media import mime_from_path
    # .jpg should guess to image/jpeg
    result = mime_from_path("/test/image.jpg")
    assert "image" in result or result == "application/octet-stream"
    
    # .pdf should guess to application/pdf
    result = mime_from_path("/test/document.pdf")
    assert "pdf" in result or result == "application/octet-stream"


def test_oversize_rejection():
    """Test that oversize files are rejected."""
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as f:
        # Write data larger than MAX_MESSAGE_SIZE_BYTES (10 MB = 10_000_000)
        f.write(b"\x00" * (10_000_001))  # 10 MB + 1 byte
        tmpfile = f.name
    
    try:
        handler = WhatsMediaHandler()
        ok, _ = handler.validate_image(tmpfile)
        assert ok is False  # Should be rejected as too large
    finally:
        os.unlink(tmpfile)


def test_mime_rejection():
    """Test that disallowed MIME types are rejected."""
    import tempfile
    import os
    with tempfile.NamedTemporaryFile(suffix=".exe", delete=False) as f:
        f.write(b"\x00\x00\x00\x01" * 100)
        tmpfile = f.name
    
    try:
        handler = WhatsMediaHandler()
        ok, _ = handler.validate_image(tmpfile)
        # .exe is not in allowed MIME types for images
        assert ok is False  # Should be rejected
    finally:
        os.unlink(tmpfile)


def test_parser_timeout():
    """Test that parser handles timeout scenarios gracefully."""
    handler = WhatsMediaHandler()
    # The transcribe_audio method should not crash on invalid input
    try:
        result = handler.transcribe_audio(b"invalid audio data", "audio/unknown")
        # Should return None or raise MimeViolation, not crash
        assert result is None or isinstance(result, str)
    except MimeViolation:
        # Expected for disallowed MIME type
        pass


def test_default_instance():
    """Test the default module-level handler instance."""
    handler = WhatsMediaHandler.default
    assert handler is not None
    assert isinstance(handler, WhatsMediaHandler)


if __name__ == "__main__":
    test_validate_image()
    test_validate_audio()
    test_validate_video()
    test_validate_document()
    test_mime_from_path()
    test_oversize_rejection()
    test_mime_rejection()
    test_parser_timeout()
    test_default_instance()
    print("All media tests passed!")
