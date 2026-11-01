"""Stable prompt fingerprinting.

A prompt that produced a decision must be reproducible later, from an audit
record alone. That means the hash has to be stable across processes, machines
and runs — which rules out ``hash()``, whose value for a string is randomised
per process.

The fingerprint covers the template body *and* its declared variable schema,
so two prompts with identical text but different variables hash differently.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Final

# Prefix so a hash is never mistaken for a version number or an id.
FINGERPRINT_PREFIX: Final[str] = "sha256"
FINGERPRINT_LENGTH: Final[int] = 16


def fingerprint_template(
    template: str,
    variables: dict[str, Any] | None = None,
) -> str:
    """Fingerprints a template and its variable schema.

    Args:
        template: The template body.
        variables: Declared variable schema, when any.

    Returns:
        A short, stable hex fingerprint, prefixed with the algorithm.
    """
    digest = hashlib.sha256()
    digest.update(b"template\0")
    digest.update(template.encode("utf-8"))
    digest.update(b"\0variables\0")
    if variables:
        # sort_keys keeps the hash stable regardless of declaration order.
        encoded = json.dumps(variables, sort_keys=True, default=str)
        digest.update(encoded.encode("utf-8"))
    else:
        digest.update(b"{}")
    return f"{FINGERPRINT_PREFIX}:{digest.hexdigest()[:FINGERPRINT_LENGTH]}"


def fingerprint_rendering(
    template: str,
    values: dict[str, Any] | None = None,
) -> str:
    """Fingerprints a template together with the values substituted into it.

    This is the hash that belongs in an audit record: it identifies the exact
    text that was sent to a model, not merely the template it came from.
    """
    digest = hashlib.sha256()
    digest.update(b"rendering\0")
    digest.update(template.encode("utf-8"))
    digest.update(b"\0values\0")
    if values:
        encoded = json.dumps(values, sort_keys=True, default=str)
        digest.update(encoded.encode("utf-8"))
    else:
        digest.update(b"{}")
    return f"{FINGERPRINT_PREFIX}:{digest.hexdigest()[:FINGERPRINT_LENGTH]}"


def fingerprints_match(left: str, right: str) -> bool:
    """Compares two fingerprints.

    Uses a constant-time comparison. Prompt hashes are not secrets, but
    comparing them the same way regardless removes a class of mistake where
    early-exit behaviour leaks information in a context that assumed it did
    not.
    """
    if len(left) != len(right):
        return False
    difference = 0
    for a, b in zip(left, right, strict=False):
        difference |= ord(a) ^ ord(b)
    return difference == 0
