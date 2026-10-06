"""Document parsing.

Parsing turns bytes into text. It is also the first place untrusted content
enters the system, so the parser's second job is to strip the things that only
exist to affect the reader: scripts, styles, and markup that would otherwise be
concatenated into a prompt where a model may act on it.

The parsers here are deliberately plain. They do not attempt to be a full HTML
or Markdown implementation; where a format is richer than what we handle, the
leftover markup is removed rather than passed through looking meaningful.
"""

from __future__ import annotations

import html
import json
import re
from typing import Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.rag.models import DocumentFormat

# --- HTML handling ---------------------------------------------------------

_SCRIPT: Final[re.Pattern[str]] = re.compile(
    r"<(script|style|noscript|template)\b.*?</\1\s*>", re.IGNORECASE | re.DOTALL
)
_COMMENT: Final[re.Pattern[str]] = re.compile(r"<!--.*?-->", re.DOTALL)
_TAG: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")
_BLOCK_TAGS: Final[re.Pattern[str]] = re.compile(
    r"</?(p|div|br|li|tr|h[1-6]|section|article)\s*/?>", re.IGNORECASE
)
_ENTITY_REF: Final[re.Pattern[str]] = re.compile(r"&[a-zA-Z#][a-zA-Z0-9]{0,8};")
_WS: Final[re.Pattern[str]] = re.compile(r"[ \t]+")
_BLANKS: Final[re.Pattern[str]] = re.compile(r"\n{3,}")

# --- Markdown handling -----------------------------------------------------

_MD_CODE: Final[re.Pattern[str]] = re.compile(r"```.*?```", re.DOTALL)
_MD_LINK: Final[re.Pattern[str]] = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_MD_IMAGE: Final[re.Pattern[str]] = re.compile(r"!\[[^\]]*\]\([^)]*\)")
_MD_HEADING: Final[re.Pattern[str]] = re.compile(r"^#{1,6}\s*", re.MULTILINE)
_MD_EMPHASIS: Final[re.Pattern[str]] = re.compile(r"(\*\*|__|\*|_|~~|`)")
_MD_HTML: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")

MAX_PARSED_CHARS: Final[int] = 8_000_000


class Parser:
    """Turns document content into plain text."""

    def parse(self, content: str, format: DocumentFormat) -> str:  # noqa: A002
        """Parses content into plain text.

        Args:
            content: Raw document content.
            format: The declared format.

        Returns:
            Plain text.

        Raises:
            InvalidRequestError: If JSON content is malformed, or the parsed
                text exceeds :data:`MAX_PARSED_CHARS`.
        """
        if not content.strip():
            return ""

        dispatch = {
            DocumentFormat.TEXT: _parse_text,
            DocumentFormat.MARKDOWN: _parse_markdown,
            DocumentFormat.JSON: _parse_json,
            DocumentFormat.HTML: _parse_html,
        }
        parsed = dispatch[format](content)

        if len(parsed) > MAX_PARSED_CHARS:
            raise InvalidRequestError(
                "parsed document exceeds the maximum size",
                details={"limit": MAX_PARSED_CHARS, "size": len(parsed)},
            )
        return parsed


def _parse_text(content: str) -> str:
    """Normalises whitespace only."""
    return _normalise(content)


def _parse_markdown(content: str) -> str:
    """Reduces Markdown to readable text.

    Code fences are kept, because code is often the subject of the document.
    Links keep their label and lose their target, since a URL in retrieved
    context is a plausible injection vector and rarely part of the meaning.
    """
    text = _MD_CODE.sub(lambda match: match.group(0).strip("`"), content)
    text = _MD_IMAGE.sub("", text)
    text = _MD_LINK.sub(r"\1", text)
    text = _MD_HEADING.sub("", text)
    text = _MD_EMPHASIS.sub("", text)
    text = _MD_HTML.sub(" ", text)
    return _normalise(text)


def _parse_json(content: str) -> str:
    """Flattens JSON into labelled lines.

    Values are prefixed with their path so a chunk taken out of context still
    says which field it came from.
    """
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as error:
        raise InvalidRequestError(
            "document is not valid JSON", details={"position": error.pos}
        ) from error

    lines: list[str] = []

    def walk(value: object, path: str) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{path}.{key}" if path else str(key))
        elif isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]")
        else:
            lines.append(f"{path}: {value}")

    walk(payload, "")
    return _normalise("\n".join(lines))


def _parse_html(content: str) -> str:
    """Strips HTML to text.

    Scripts, styles and comments are removed whole rather than tag-by-tag:
    their content is executable or presentational, and leaving any of it in
    retrieved context is how markup becomes instruction.
    """
    text = _SCRIPT.sub(" ", content)
    text = _COMMENT.sub(" ", text)
    text = _BLOCK_TAGS.sub("\n", text)
    text = _TAG.sub(" ", text)
    text = html.unescape(text)
    # A stray unescaped entity reference is a sign of double-encoded content.
    text = _ENTITY_REF.sub(" ", text)
    return _normalise(text)


def _normalise(text: str) -> str:
    """Collapses whitespace into a stable form."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _WS.sub(" ", text)
    text = _BLANKS.sub("\n\n", text)
    return "\n".join(line.strip() for line in text.split("\n")).strip()


def detect_format(content: str, *, declared: DocumentFormat | None = None) -> DocumentFormat:
    """Guesses a format when none is declared.

    A declared format always wins: guessing over a correct declaration would
    let a document opt itself into a different parser.
    """
    if declared is not None:
        return declared
    stripped = content.lstrip()
    if stripped.startswith("<"):
        return DocumentFormat.HTML
    if stripped.startswith("{") or stripped.startswith("["):
        return DocumentFormat.JSON
    if _MD_HEADING.search(content) or "```" in content or _MD_LINK.search(content):
        return DocumentFormat.MARKDOWN
    return DocumentFormat.TEXT
