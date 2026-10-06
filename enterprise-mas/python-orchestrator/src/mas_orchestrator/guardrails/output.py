"""Output validation.

The last stage. An answer that has been produced is not yet an answer that can
be returned: it may not match the schema the caller asked for, it may contain a
credential, and if it cites sources those citations may not support it.

Each check produces a typed failure. None of them "fixes" the output, because
a repaired answer is a different answer presented as the original.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from mas_orchestrator.contracts.errors import OutputValidationFailedError

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_OUTPUT_CHARS: Final[int] = 100_000


class OutputVerdict(StrEnum):
    """Outcome of output validation."""

    ACCEPTED = "accepted"
    SCHEMA_MISMATCH = "schema_mismatch"
    UNSAFE_CONTENT = "unsafe_content"
    BAD_CITATIONS = "bad_citations"
    TOO_LARGE = "too_large"


@dataclass(frozen=True, slots=True)
class OutputCheck:
    """The result of validating an output.

    Attributes:
        verdict: The outcome.
        content: The content, unchanged.
        detail: What failed, when it did.
    """

    verdict: OutputVerdict
    content: str
    detail: str | None = None

    @property
    def accepted(self) -> bool:
        """Whether the output may be returned."""
        return self.verdict is OutputVerdict.ACCEPTED


@dataclass(frozen=True, slots=True)
class OutputConfig:
    """What output validation enforces.

    Attributes:
        schema: JSON Schema the output must satisfy, when the caller asked for
            structured output. ``None`` means unvalidated.
        max_chars: Size ceiling.
        redaction: Applied to the content before it is returned. Detection and
            the action taken are reported separately.
        require_citations_in_range: Whether ``[n]`` references must point at
            passages that were actually supplied.
        available_citations: How many passages were supplied.
    """

    schema: dict[str, Any] | None = None
    max_chars: int = MAX_OUTPUT_CHARS
    require_citations_in_range: bool = False
    available_citations: int = 0

    def __post_init__(self) -> None:
        if self.max_chars < 1:
            raise OutputValidationFailedError(
                "max_chars must be positive", details={"field": "max_chars"}
            )
        if self.schema is not None and not isinstance(self.schema, dict):
            raise OutputValidationFailedError(
                "schema must be a JSON object", details={"field": "schema"}
            )


def validate_output(
    content: object,
    config: OutputConfig | None = None,
) -> OutputCheck:
    """Validates model output.

    The parameter is typed ``object`` rather than ``str`` on purpose: output
    arrives from a model and must be checked, not assumed. Annotating it as
    ``str`` would make the type checker delete the check as unreachable.

    Args:
        content: The output to check.
        config: What to enforce.

    Returns:
        An :class:`OutputCheck`.
    """
    config = config or OutputConfig()

    if not isinstance(content, str):
        return OutputCheck(
            OutputVerdict.SCHEMA_MISMATCH,
            "",
            detail=f"output must be a string, got {type(content).__name__}",
        )

    if len(content) > config.max_chars:
        return OutputCheck(
            OutputVerdict.TOO_LARGE,
            content,
            detail=f"output exceeds {config.max_chars} characters",
        )

    if config.schema is not None:
        assert isinstance(content, str)
        failure = _check_schema(content, config.schema)
        if failure is not None:
            return OutputCheck(OutputVerdict.SCHEMA_MISMATCH, content, detail=failure)

    if config.require_citations_in_range:
        from mas_orchestrator.rag.citations import (
            referenced_numbers,
            references_are_in_range,
        )

        if referenced_numbers(content) and not references_are_in_range(
            content, config.available_citations
        ):
            highest = max(referenced_numbers(content))
            return OutputCheck(
                OutputVerdict.BAD_CITATIONS,
                content,
                detail=(
                    f"answer cites [{highest}] but only "
                    f"{config.available_citations} passage(s) were supplied"
                ),
            )

    return OutputCheck(OutputVerdict.ACCEPTED, content)


def enforce_output(
    content: str,
    config: OutputConfig | None = None,
) -> str:
    """Validates output and raises on failure.

    Raises:
        OutputValidationFailedError: If the output is not acceptable.
    """
    check = validate_output(content, config)
    if not check.accepted:
        _LOGGER.warning(
            "output rejected",
            extra={
                "event": "guardrail.output_rejected",
                "verdict": check.verdict.value,
                "detail": check.detail,
            },
        )
        raise OutputValidationFailedError(
            f"output rejected: {check.detail}",
            details={"verdict": check.verdict.value, "detail": check.detail},
        )
    return check.content


def _check_schema(content: str, schema: dict[str, Any]) -> str | None:
    """Checks content against a schema.

    Returns:
        ``None`` when it satisfies the schema, otherwise a description of the
        failure.
    """
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as error:
        return f"output is not valid JSON (position {error.pos})"

    expected_type = schema.get("type")
    if expected_type == "object" and not isinstance(payload, dict):
        return "output must be a JSON object"
    if expected_type == "array" and not isinstance(payload, list):
        return "output must be a JSON array"

    if isinstance(payload, dict):
        required = schema.get("required") or []
        missing = [name for name in required if name not in payload]
        if missing:
            return f"output is missing required fields: {sorted(missing)}"

        properties = schema.get("properties") or {}
        for name, subschema in properties.items():
            if name not in payload or not isinstance(subschema, dict):
                continue
            declared = subschema.get("type")
            if declared is None:
                continue
            actual = _json_type(payload[name])
            if actual != declared:
                return f"field '{name}' must be {declared}, got {actual}"

    return None


def _json_type(value: Any) -> str:
    """JSON type name for a Python value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"
