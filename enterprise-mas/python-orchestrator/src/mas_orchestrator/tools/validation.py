"""Argument and output validation for tool invocations.

Two things are validated here, and they are validated for different reasons:

* **Input** — the model produced these arguments. They are untrusted until
  checked, and a tool invoked with arguments it does not accept is a
  malformed invocation, not a tool failure.
* **Output** — the tool produced this value. It is data, not instructions, and
  it must fit inside the output bound before it reaches a model's context.
"""

from __future__ import annotations

import json
from typing import Any, Final

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.tools.types import (
    MAX_TOOL_ARGUMENT_BYTES,
    ToolDefinition,
)

# Types accepted for JSON-Schema "type" checks. Anything outside this set is
# not validated rather than guessed at.
_PRIMITIVES: Final[dict[str, type | tuple[type, ...]]] = {
    "object": dict,
    "array": list,
    "string": str,
    "boolean": bool,
    "null": type(None),
}

_NUMERIC: Final[tuple[type, ...]] = (int, float)


def _encoded_size(value: Any) -> int:
    """Byte size of the canonical JSON encoding of ``value``."""
    return len(json.dumps(value, default=str).encode("utf-8"))


def validate_arguments(definition: ToolDefinition, arguments: Any) -> dict[str, Any]:
    """Validates a model's arguments against a tool's input schema.

    Args:
        definition: The tool being invoked.
        arguments: The decoded arguments.

    Returns:
        The arguments as a mapping.

    Raises:
        InvalidRequestError: If the arguments are not a JSON object, exceed
            :data:`MAX_TOOL_ARGUMENT_BYTES`, are missing a required property,
            or have a property of the wrong type or outside its bounds.
    """
    if not isinstance(arguments, dict):
        raise InvalidRequestError(
            "tool arguments must be a JSON object",
            details={"tool_id": definition.tool_id, "received": type(arguments).__name__},
        )

    if _encoded_size(arguments) > MAX_TOOL_ARGUMENT_BYTES:
        raise InvalidRequestError(
            "tool arguments exceed the maximum size",
            details={"tool_id": definition.tool_id, "limit": MAX_TOOL_ARGUMENT_BYTES},
        )

    schema = definition.input_schema
    _validate_against_schema(arguments, schema, path="arguments", tool_id=definition.tool_id)

    unknown = _unknown_properties(arguments, schema)
    if unknown:
        raise InvalidRequestError(
            "tool received unknown arguments",
            details={"tool_id": definition.tool_id, "unknown": unknown},
        )

    return arguments


def _unknown_properties(value: Any, schema: Any) -> tuple[str, ...]:
    """Properties present in ``value`` that the schema does not permit.

    Only an explicit ``additionalProperties: false`` restricts the set. A
    schema that omits the keyword permits extra properties, which is the JSON
    Schema default and is honoured here rather than invented otherwise.
    """
    if not isinstance(schema, dict) or not isinstance(value, dict):
        return ()
    if schema.get("additionalProperties") is not False:
        return ()
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return ()
    return tuple(sorted(set(value) - set(properties)))


def validate_output(definition: ToolDefinition, output: Any) -> Any:
    """Validates a tool's output against its optional output schema.

    A tool with no declared output schema accepts any output; that is a
    declaration, not a fallback.

    Raises:
        InvalidRequestError: If the output does not satisfy the declared
            schema.
    """
    if definition.output_schema is None:
        return output
    _validate_against_schema(
        output, definition.output_schema, path="output", tool_id=definition.tool_id
    )
    return output


def _validate_against_schema(
    value: Any,
    schema: Any,
    *,
    path: str,
    tool_id: str,
) -> None:
    """Validates ``value`` against the subset of JSON Schema we support.

    Supported keywords: ``type``, ``properties``, ``required``, ``items``,
    ``enum``, ``minimum``, ``maximum``, ``minLength``, ``maxLength``,
    ``additionalProperties`` (only ``false`` is enforced).

    Unsupported keywords are **ignored silently for validation purposes** but
    are listed in :func:`describe_unsupported_keywords` so an operator can see
    what a schema claims but we do not check. Silently pretending to enforce
    something we do not is the failure mode this avoids.
    """
    if not isinstance(schema, dict):
        return

    expected = schema.get("type")
    if isinstance(expected, str):
        _check_type(value, expected, path=path, tool_id=tool_id)
    elif isinstance(expected, list) and not any(
        _matches_type(value, item) for item in expected if isinstance(item, str)
    ):
        raise InvalidRequestError(
            "value has an unexpected type",
            details={
                "tool_id": tool_id,
                "path": path,
                "expected": expected,
                "received": type(value).__name__,
            },
        )

    if "enum" in schema and isinstance(schema["enum"], list) and value not in schema["enum"]:
        raise InvalidRequestError(
            "value is not one of the permitted values",
            details={"tool_id": tool_id, "path": path, "enum": schema["enum"]},
        )

    if isinstance(value, bool):
        # bool is a subclass of int in Python; a boolean must not be checked
        # against numeric bounds meant for real numbers.
        pass
    elif isinstance(value, (int, float)):
        if "minimum" in schema and value < schema["minimum"]:
            raise InvalidRequestError(
                "value is below the minimum",
                details={"tool_id": tool_id, "path": path, "minimum": schema["minimum"]},
            )
        if "maximum" in schema and value > schema["maximum"]:
            raise InvalidRequestError(
                "value is above the maximum",
                details={"tool_id": tool_id, "path": path, "maximum": schema["maximum"]},
            )

    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            raise InvalidRequestError(
                "value is shorter than the minimum length",
                details={"tool_id": tool_id, "path": path, "minLength": schema["minLength"]},
            )
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            raise InvalidRequestError(
                "value is longer than the maximum length",
                details={"tool_id": tool_id, "path": path, "maxLength": schema["maxLength"]},
            )

    if isinstance(value, dict):
        properties = schema.get("properties")
        if isinstance(properties, dict):
            for name in schema.get("required", []) or []:
                if name not in value:
                    raise InvalidRequestError(
                        "a required argument is missing",
                        details={"tool_id": tool_id, "path": f"{path}.{name}"},
                    )
            for name, item in value.items():
                child = properties.get(name)
                if isinstance(child, dict):
                    _validate_against_schema(item, child, path=f"{path}.{name}", tool_id=tool_id)
        if schema.get("additionalProperties") is False and isinstance(properties, dict):
            unknown = sorted(set(value) - set(properties))
            if unknown:
                raise InvalidRequestError(
                    "value contains properties the schema does not permit",
                    details={"tool_id": tool_id, "path": path, "unknown": unknown},
                )

    if isinstance(value, list):
        items = schema.get("items")
        if isinstance(items, dict):
            for index, item in enumerate(value):
                _validate_against_schema(item, items, path=f"{path}[{index}]", tool_id=tool_id)


def _matches_type(value: Any, expected: str) -> bool:
    """Whether ``value`` is of the named JSON-Schema type."""
    if expected == "number":
        return isinstance(value, _NUMERIC) and not isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    primitive = _PRIMITIVES.get(expected)
    if primitive is None:
        return True  # unknown type keyword: not validated
    return isinstance(value, primitive) and not (
        expected in {"number", "integer"} and isinstance(value, bool)
    )


def _check_type(value: Any, expected: str, *, path: str, tool_id: str) -> None:
    """Raises if ``value`` is not of the named type."""
    if _matches_type(value, expected):
        return
    raise InvalidRequestError(
        "value has an unexpected type",
        details={
            "tool_id": tool_id,
            "path": path,
            "expected": expected,
            "received": type(value).__name__,
        },
    )


SUPPORTED_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "type",
        "properties",
        "required",
        "items",
        "enum",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "additionalProperties",
        "description",
        "title",
        "default",
        "$schema",
        "$defs",
        "definitions",
    }
)


def describe_unsupported_keywords(schema: Any) -> tuple[str, ...]:
    """Lists JSON-Schema keywords present in ``schema`` that we do not enforce.

    This exists so the gap between "the schema says" and "we check" is visible
    rather than implicit. An operator who writes ``pattern`` or ``format``
    should be able to discover that neither is enforced.
    """
    if not isinstance(schema, dict):
        return ()
    found: set[str] = set()
    for key, value in schema.items():
        if key not in SUPPORTED_KEYWORDS:
            found.add(key)
        if isinstance(value, dict):
            found.update(describe_unsupported_keywords(value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    found.update(describe_unsupported_keywords(item))
    return tuple(sorted(found))
