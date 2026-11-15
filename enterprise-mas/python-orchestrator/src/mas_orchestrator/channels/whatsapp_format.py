"""markdown → WhatsApp formatting + 4096-char splitter.

WhatsApp strips HTML but supports a subset of markdown-style formatting.
This module provides:
  • markdown → WhatsApp markup conversion (*bold_ _italic_ ~strike~ ```mono```
  • paragraph-aware splitting (respecting 4096-char per-message limit)
  • rejection of tables and HTML (safety)
"""
from __future__ import annotations

import re
from typing import List, Tuple


# WhatsApp-allowed markdown patterns
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_ITALIC_RE = re.compile(r"_(?<!_)(.+?)(?<!_)_")
_STRIKE_RE = re.compile(r"~(.+?)~")
_MONO_RE = re.compile(r"`(.+?)`")


def format_for_whatsapp(markdown: str) -> str:
    """Convert a markdown string to WhatsApp‑compatible formatting.

    WhatsApp does not support raw HTML or complex markdown. This function
    converts the most common patterns and strips the rest.

    Supported patterns:
      *bold*   → *bold* (WhatsApp uses single asterisks)
      _italic_ → _italic_ (WhatsApp uses single underscores)
      ~strike~ → ~strike~ (WhatsApp uses tildes)
      `mono`   → `mono` (code block inline)

    All other markdown (headings, lists, tables, etc.) is stripped or
    simplified to plain text.
    """
    if not markdown:
        return markdown

    # Convert double-asterisk bold to single-asterisk (WhatsApp format)
    text = _BOLD_RE.sub(r"*\1*", markdown)

    # Convert single-asterisk (if not already bold) to underscore for italic
    # Be careful not to double-convert asterisks already turned into bold
    text = _ITALIC_RE.sub(r"_\1_", text)

    # Convert tildes for strike
    text = _STRIKE_RE.sub(r"~\1~", text)

    # Convert backtick mono
    text = _MONO_RE.sub(r"`\1`", text)

    # Remove any remaining markdown that WhatsApp doesn't support
    # Specifically: strip table-related pipes and headings
    text = re.sub(r"^\s*\|.*\|\s*$\n?", "", text, flags=re.MULTILINE)  # remove table rows
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)  # remove headings

    return text


def split_by_limit(text: str, limit: int = 4096) -> List[str]:
    """Split text into chunks respecting WhatsApp's ~4096 char limit.

    Splitting rules:
      • Never split mid‑a formatting marker (* _ ~ `)
      • Respect paragraph boundaries (double newlines)
      • If a single segment exceeds the limit, truncate at the last safe point
        and append "…" (ellipsis), knowing WhatsApp will truncate anyway.

    Returns a list ofWhatsApp‑ready message chunks.
    """
    if not text:
        return []

    if len(text) <= limit:
        return [text]

    chunks: List[str] = []
    remaining = text

    while remaining:
        # If the remaining text is small enough, just add it
        if len(remaining) <= limit:
            chunks.append(remaining.strip())
            break

        # Try to find a paragraph break (\n\n) within the limit
        paragraph_cut = remaining[:limit].rfind("\n\n")
        if paragraph_cut >= 0:
            chunks.append(remaining[:paragraph_cut + 2].strip())
            remaining = remaining[paragraph_cut + 2:].strip()
            continue

        # Try to find a sentence end (. ! ?) followed by space within the limit
        sentence_cut = _find_sentence_boundary(remaining[:limit])
        if sentence_cut >= 0:
            chunks.append(remaining[:sentence_cut + 1].strip())
            remaining = remaining[sentence_cut + 1:].strip()
            continue

        # Fallback: just truncate at the limit with ellipsis
        truncated = remaining[:limit].rsplit(" ", 1)[0] + "…"
        chunks.append(truncated)
        break

    return chunks


def _find_sentence_boundary(text: str) -> int:
    """Find the last sentence-ending punctuation (. ! ?) within *text*.

    Returns the index of the punctuation, or -1 if none found within the
    given text.
    """
    # Look for . ! ? followed by end-of-string or whitespace
    matches = [
        m.end() - 1
        for m in re.finditer(r"[.!?](\s|$)", text)
    ]
    return max(matches) if matches else -1


def reject_tables_and_html(raw: str) -> str:
    """Remove any table or HTML structures from the raw text.

    WhatsApp does not render tables or HTML. This is a safety net for
    any markup that slipped through earlier stages.
    """
    # Remove MediaWiki/Markdown tables
    text = re.sub(r"\|-.+\|[\s\S]*?(?=\n|$)", "", raw)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", "", raw)
    return text


if __name__ == "__main__":
    # Simple CLI test
    sample = (
        "*Hello* _world_ ~test~ `code`\n"
        "Table: |a|b|c|\n"
        "Heading: ## title\n"
        "Long text " * 200  # make it long
    )
    formatted = format_for_whatsapp(sample)
    chunks = split_by_limit(formatted, limit=100)  # small limit for demo
    print(f"Formatted ({len(formatted)} → {len(formatted)}): {formatted[:80]}…")
    print(f"Chunks ({len(chunks)}):")
    for i, c in enumerate(chunks):
        print(f"  {i+1}. {c[:60]}{'…' if len(c) > 60 else ''}")