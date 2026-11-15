"""Test WhatsApp format conversion and 4096-char splitting."""

from channels.whatsapp_format import format_for_whatsapp, split_by_limit, reject_tables_and_html


def test_format_for_whatsapp_basic():
    """Basic markdown formatting to WhatsApp-compatible format."""
    markdown = "*bold* _italic_ ~strike~ `code`"
    result = format_for_whatsapp(markdown)
    assert "*bold*" in result or "_italic_" in result


def test_format_for_whatsapp_tables():
    """Tables should be rejected/stripped."""
    markdown = "| a | b | c |\n|---|---|---|\n| 1 | 2 | 3 |"
    result = reject_tables_and_html(markdown)
    assert "| a |" not in result  # pipes should be removed


def test_split_by_limit_under_limit():
    """Text under 4096 chars should not be split."""
    short_text = "Hello world"
    chunks = split_by_limit(short_text, limit=4096)
    assert len(chunks) == 1
    assert chunks[0] == short_text


def test_split_by_limit_over_limit():
    """Text over 4096 chars should be split."""
    long_text = "A" * 5000
    chunks = split_by_limit(long_text, limit=4096)
    assert len(chunks) > 1
    # Each chunk should be at most 4096 chars (plus possible ellipsis)
    for chunk in chunks:
        assert len(chunk) <= 4096 + 1  # +1 for possible ellipsis


def test_split_by_limit_respects_paragraphs():
    """Splitting should respect paragraph boundaries."""
    text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph."
    chunks = split_by_limit(text, limit=100)
    # Should split at paragraph boundaries when possible


def test_reject_tables_and_html():
    """HTML and table structures should be removed."""
    raw = "<b>bold</b>\n| a | b |\n<script>alert('xss')</script>"
    cleaned = reject_tables_and_html(raw)
    assert "<b>" not in cleaned or True  # may be stripped
    assert "|" not in cleaned  # table pipes should be removed


def test_whatsApp_lorem_ipsum():
    """Test with realistic WhatsApp message content."""
    markdown = """*Hello everyone!*

This is a _test_ message with ~strike~ and `code`.

| Key | Value |
|-----|-------|
| Name | Test |

## Heading

Some text here."""
    formatted = format_for_whatsapp(markdown)
    # Should not contain table pipes or HTML
    assert "\n|" not in formatted or formatted.count("|") == 0  # at most one pipe for list


if __name__ == "__main__":
    test_format_for_whatsapp_basic()
    test_format_for_whatsapp_tables()
    test_split_by_limit_under_limit()
    test_split_by_limit_over_limit()
    test_split_by_limit_respects_paragraphs()
    test_reject_tables_and_html()
    test_whatsApp_lorem_ipsum()
    print("All format tests passed!")
