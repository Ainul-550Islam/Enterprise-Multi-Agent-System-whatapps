"""Deterministic sensitive-value redaction.

Every log line, error detail and telemetry attribute produced by the
orchestrator passes through this module before it leaves the process. The
redaction is *deterministic* — the same input always yields the same output —
so tests can assert on it and operators can reason about it.

Two complementary mechanisms are provided:

* :func:`redact_mapping` / :func:`redact_key` — removes values whose **key**
  looks sensitive (``authorization``, ``api_key``, ``password`` …). This is the
  primary defence and is keyed on structure, so it cannot be defeated by an
  unrecognised secret format.
* :func:`redact_text` — scrubs **value patterns** (Bearer tokens, OpenAI-style
  keys, PEM blocks, long high-entropy hex/base64 runs) from free-form text.
  This catches secrets that leaked into prose, stack traces or provider
  messages.

Design rules:

* Redaction never raises: a failure while redacting must not turn a loggable
  event into a crash.
* Redaction is conservative — an ambiguous high-entropy string is redacted
  rather than left in place. A false redaction costs an operator a lookup; a
  false negative leaks a credential.
* Redaction is **not** a substitute for not collecting secrets: callers must
  still avoid placing credentials in structured fields.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Final

__all__ = [
    "REDACTED",
    "SENSITIVE_KEY_SUBSTRINGS",
    "is_sensitive_key",
    "redact_key",
    "redact_mapping",
    "redact_text",
    "redact_value",
]

#: The single replacement marker used everywhere, so an operator searching for
#: leaked material has exactly one string to grep for.
REDACTED: Final[str] = "[REDACTED]"

#: Case-insensitive substrings that mark a mapping key as sensitive. Matching
#: is a substring test so ``X-Api-Key``, ``api_key`` and ``APIKEY`` all match.
SENSITIVE_KEY_SUBSTRINGS: Final[tuple[str, ...]] = (
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "bearer",
    "client_secret",
    "credential",
    "password",
    "passwd",
    "private_key",
    "secret",
    "token",
)

# --- value-shaped secret patterns -------------------------------------------
# Ordered longest/most-specific first where patterns could overlap.

_BEARER_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+/=]{8,}")

_PEM_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    re.DOTALL,
)

# OpenAI: sk-<20+ alnum>, sk-proj-<...>, sk-ant-<...>; Anthropic: sk-ant-api03-…
_API_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?:sk|pk|rk)-[A-Za-z0-9][A-Za-z0-9_\-]{16,}"
)

# Generic assignments: api_key = "..." / "token": "..." inside prose.
_ASSIGNMENT_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?i)\b(api[_\-]?key|token|secret|password|passwd|credential|authorization)\b"
    r"\s*[:=]\s*[\"']?([^\s\"',;}]{6,})[\"']?"
)

# Long high-entropy runs: 32+ chars of base64/hex with mixed character classes.
_HIGH_ENTROPY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\b(?=[A-Za-z0-9_\-]{32,}\b)(?=[A-Za-z0-9_\-]*[A-Za-z])(?=[A-Za-z0-9_\-]*[0-9])[A-Za-z0-9_\-]{32,}\b"
)


def is_sensitive_key(key: str) -> bool:
    """Returns whether a mapping *key* designates a sensitive value.

    Args:
        key: The key name, in its original case and form.

    The comparison is case-insensitive and ignores separators so that
    ``X-API-Key``, ``api_key`` and ``APIKEY`` are all recognised.
    """
    normalised = key.lower().replace("-", "_").replace(" ", "_")
    collapsed = normalised.replace("_", "")
    for substring in SENSITIVE_KEY_SUBSTRINGS:
        plain = substring.replace("_", "")
        if plain in collapsed:
            return True
    return False


def redact_value(value: Any) -> Any:
    """Redacts an arbitrary value, recursing into containers.

    * Mappings: keys are tested with :func:`is_sensitive_key`; sensitive
      entries become :data:`REDACTED`. Non-sensitive values are recursed.
    * Sequences: each element is recursed (returned as a ``list``).
    * Strings: run through :func:`redact_text` to catch value-shaped secrets.
    * Scalars (int/float/bool/None): returned unchanged.

    Recursion is depth-limited to protect against cyclic structures — a cycle
    would otherwise raise ``RecursionError`` inside a logging call.
    """
    return _redact_value(value, depth=0)


def _redact_value(value: Any, *, depth: int) -> Any:
    """Recursive worker for :func:`redact_value` with an explicit depth guard."""
    if depth > 8:
        return REDACTED

    if value is None or isinstance(value, bool | int | float):
        return value

    if isinstance(value, str):
        return redact_text(value)

    if isinstance(value, Mapping):
        result: dict[Any, Any] = {}
        for key, item in value.items():
            key_text = str(key)
            if is_sensitive_key(key_text):
                result[key] = REDACTED
            else:
                result[key] = _redact_value(item, depth=depth + 1)
        return result

    if isinstance(value, list | tuple | set | frozenset):
        return [_redact_value(item, depth=depth + 1) for item in value]

    # Unknown object: never call str() on it, because a custom __str__ could
    # itself emit secret material. Report the type instead.
    return f"<redacted:{type(value).__name__}>"


def redact_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    """Redacts a string-keyed mapping, returning a new plain ``dict``.

    Args:
        mapping: Structured data about to be logged, attached to an error, or
            emitted as a telemetry attribute.

    Returns:
        A new dictionary with sensitive keys replaced by :data:`REDACTED` and
        remaining string values scrubbed of value-shaped secrets.
    """
    redacted = _redact_value(mapping, depth=0)
    if isinstance(redacted, dict):
        return redacted
    # Unreachable for a Mapping input; defensive so the return type is exact.
    return {"value": redacted}


def redact_key(key: str) -> str:
    """Returns :data:`REDACTED` when *key* is sensitive, else the key unchanged.

    Useful when a caller must preserve the key set of a mapping but cannot
    afford to recurse into values (for example very large payloads).
    """
    return REDACTED if is_sensitive_key(key) else key


def redact_text(text: str) -> str:
    """Scrubs value-shaped secrets out of free-form *text*.

    Applied patterns, in order:

    1. PEM private-key blocks (multi-line).
    2. ``Bearer <token>`` sequences.
    3. Vendor API keys (``sk-…``, ``pk-…``, ``rk-…``).
    4. ``key = value`` / ``"key": "value"`` assignments whose key looks secret.
    5. Long high-entropy alphanumeric runs (32+ chars with letters and digits).

    Args:
        text: Input text, possibly containing secrets.

    Returns:
        The text with every match replaced by :data:`REDACTED`. Never raises:
        on an internal error the original text is returned, because the caller
        is most likely a logger and crashing would be worse than the leak risk
        of an already-in-flight message.
    """
    if not text:
        return text

    try:
        scrubbed = _PEM_PATTERN.sub(REDACTED, text)
        # Bearer must be scrubbed BEFORE the assignment pattern: in
        # "Authorization: Bearer <token>" the assignment pattern would otherwise
        # match the literal word "Bearer" as the value and leave the token
        # standing on its own, where no later pattern recognises it.
        scrubbed = _BEARER_PATTERN.sub(f"Bearer {REDACTED}", scrubbed)
        # The assignment pattern keeps the key visible and replaces only the
        # value, preserving the diagnostic usefulness of the line.
        scrubbed = _ASSIGNMENT_PATTERN.sub(lambda m: f"{m.group(1)}={REDACTED}", scrubbed)
        scrubbed = _API_KEY_PATTERN.sub(REDACTED, scrubbed)
        scrubbed = _HIGH_ENTROPY_PATTERN.sub(REDACTED, scrubbed)
        return scrubbed
    except Exception:  # pragma: no cover - defensive, see docstring
        return text
