"""Strict validation and materialisation of Rust-owned AgentBridge snapshots.

The gRPC body signature authenticates the serialized snapshot; this module
still validates its closed schema, identity, lifecycle, UUIDs, checksum, and
resource ceilings at the Python trust boundary. A snapshot is converted to an
immutable ``AgentSpec`` for one execution and is never looked up in the
process-local tenant catalog.
"""

from __future__ import annotations

import json
import math
import uuid
from dataclasses import dataclass
from typing import Any, Final

from mas_orchestrator.agents.base import AgentKind, AgentLimits, AgentSpec, AgentStatus
from mas_orchestrator.contracts.errors import InvalidRequestError

MAX_SNAPSHOT_BYTES: Final[int] = 512 * 1024
MAX_NAME_LENGTH: Final[int] = 128
MAX_SLUG_LENGTH: Final[int] = 64
MAX_DESCRIPTION_LENGTH: Final[int] = 4 * 1024
MAX_SYSTEM_PROMPT_BYTES: Final[int] = 128 * 1024
MAX_TOOL_ID_LENGTH: Final[int] = 256
MAX_ALLOWED_TOOLS: Final[int] = 128
MAX_TAGS: Final[int] = 32
MAX_TAG_LENGTH: Final[int] = 64
MAX_STEPS: Final[int] = 1_000
MAX_TOOL_CALLS: Final[int] = 500
MAX_DELEGATION_DEPTH: Final[int] = 5

_SNAPSHOT_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "agent_id",
        "tenant_id",
        "organization_id",
        "project_id",
        "agent_version_id",
        "version_number",
        "configuration_checksum",
        "name",
        "slug",
        "kind",
        "status",
        "origin",
        "read_only",
        "configuration",
    }
)
_CONFIGURATION_FIELDS: Final[frozenset[str]] = frozenset(
    {"description", "model_hint", "system_prompt", "limits", "tags", "allowed_tools"}
)
_LIMIT_FIELDS: Final[frozenset[str]] = frozenset(
    {"max_steps", "max_tool_calls", "max_delegation_depth", "timeout_seconds"}
)


@dataclass(frozen=True, slots=True)
class ValidatedAgentSnapshot:
    """A closed Rust snapshot and its exact Python runtime definition."""

    spec: AgentSpec
    version_number: int
    configuration_checksum: str


def parse_agent_snapshot(
    raw: str,
    *,
    expected_agent_id: str,
    expected_tenant_id: str,
    expected_organization_id: str,
    expected_project_id: str,
    expected_agent_version_id: str,
) -> ValidatedAgentSnapshot:
    """Validates a wire snapshot and builds its exact immutable ``AgentSpec``.

    Args:
        raw: JSON serialized by Rust and covered by AgentBridge body HMAC.
        expected_agent_id: Agent id in the protobuf request.
        expected_tenant_id: Authoritative tenant in the protobuf request.
        expected_organization_id: Authoritative organization in the request.
        expected_project_id: Authoritative project in the request.
        expected_agent_version_id: Exact pinned version id in the request.

    Returns:
        The strictly validated snapshot projected to a runtime spec.

    Raises:
        InvalidRequestError: If the snapshot is absent, malformed, outside
            the request scope, or violates the closed runtime schema.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise InvalidRequestError(
            "agent run requires a Rust-resolved agent snapshot",
            details={"field": "agent_snapshot_json"},
        )
    encoded = raw.encode("utf-8")
    if len(encoded) > MAX_SNAPSHOT_BYTES:
        raise InvalidRequestError(
            "agent runtime snapshot exceeds the maximum accepted size",
            details={"field": "agent_snapshot_json", "limit": MAX_SNAPSHOT_BYTES},
        )

    try:
        value = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_non_finite_constant,
        )
    except (json.JSONDecodeError, UnicodeError, ValueError) as error:
        raise InvalidRequestError(
            "agent runtime snapshot is not valid strict JSON",
            details={"field": "agent_snapshot_json"},
        ) from error

    snapshot = _object(value, "agent_snapshot_json", _SNAPSHOT_FIELDS)
    identity = {
        "agent_id": expected_agent_id,
        "tenant_id": expected_tenant_id,
        "organization_id": expected_organization_id,
        "project_id": expected_project_id,
        "agent_version_id": expected_agent_version_id,
    }
    parsed_identity: dict[str, str] = {}
    for field, expected in identity.items():
        actual = _uuid(snapshot[field], field)
        expected_value = _uuid(expected, field)
        if actual != expected_value:
            raise InvalidRequestError(
                "agent runtime snapshot does not match the authoritative request scope",
                details={"field": field},
            )
        parsed_identity[field] = actual

    version_number = _positive_integer(snapshot["version_number"], "version_number")
    checksum = _string(snapshot["configuration_checksum"], "configuration_checksum", 64, 64)
    if any(character not in "0123456789abcdef" for character in checksum):
        raise InvalidRequestError(
            "agent runtime snapshot checksum must be lowercase SHA-256 hex",
            details={"field": "configuration_checksum"},
        )

    name = _string(snapshot["name"], "name", 1, MAX_NAME_LENGTH)
    slug = _string(snapshot["slug"], "slug", 1, MAX_SLUG_LENGTH)
    kind_value = _string(snapshot["kind"], "kind", 1, 32)
    status = _string(snapshot["status"], "status", 1, 32)
    origin = _string(snapshot["origin"], "origin", 1, 32)
    read_only = snapshot["read_only"]
    if not isinstance(read_only, bool):
        raise InvalidRequestError(
            "agent runtime snapshot read_only must be a boolean",
            details={"field": "read_only"},
        )
    if status != "active":
        raise InvalidRequestError(
            "agent runtime snapshot is not executable",
            details={"field": "status", "status": status},
        )
    if origin not in {"user", "system"} or read_only != (origin == "system"):
        raise InvalidRequestError(
            "agent runtime snapshot ownership fields are inconsistent",
            details={"field": "origin"},
        )

    try:
        kind = AgentKind(kind_value)
    except ValueError as error:
        raise InvalidRequestError(
            "agent runtime snapshot has an unsupported agent kind",
            details={"field": "kind", "kind": kind_value},
        ) from error
    if kind is AgentKind.SUPERVISOR:
        # The current AgentBridge contract carries one root snapshot. A
        # supervisor would need immutable snapshots for each authorized child
        # as well; falling back to the Python catalog would violate Rust's
        # version authority, so fail closed until that nested contract exists.
        raise InvalidRequestError(
            "supervisor snapshots require pinned delegated-agent snapshots",
            details={"field": "kind", "kind": kind_value},
        )

    configuration = _object(snapshot["configuration"], "configuration", _CONFIGURATION_FIELDS)
    description = _nullable_string(
        configuration["description"], "configuration.description", 0, MAX_DESCRIPTION_LENGTH
    )
    model_hint = _nullable_string(configuration["model_hint"], "configuration.model_hint", 1, 256)
    if model_hint is not None and not model_hint.strip():
        raise InvalidRequestError(
            "agent model hint must not be blank",
            details={"field": "configuration.model_hint"},
        )
    system_prompt = _string(
        configuration["system_prompt"],
        "configuration.system_prompt",
        0,
        MAX_SYSTEM_PROMPT_BYTES,
    )
    limits = _object(configuration["limits"], "configuration.limits", _LIMIT_FIELDS)
    max_steps = _bounded_integer(
        limits["max_steps"], "configuration.limits.max_steps", 1, MAX_STEPS
    )
    max_tool_calls = _bounded_integer(
        limits["max_tool_calls"], "configuration.limits.max_tool_calls", 0, MAX_TOOL_CALLS
    )
    max_delegation_depth = _bounded_integer(
        limits["max_delegation_depth"],
        "configuration.limits.max_delegation_depth",
        0,
        MAX_DELEGATION_DEPTH,
    )
    timeout_seconds = limits["timeout_seconds"]
    if timeout_seconds is not None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise InvalidRequestError(
                "agent timeout_seconds must be a finite positive number or null",
                details={"field": "configuration.limits.timeout_seconds"},
            )
        timeout_seconds = float(timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0 or timeout_seconds > 86_400:
            raise InvalidRequestError(
                "agent timeout_seconds is outside the platform bounds",
                details={"field": "configuration.limits.timeout_seconds"},
            )

    tags = _string_list(
        configuration["tags"],
        "configuration.tags",
        max_items=MAX_TAGS,
        max_item_length=MAX_TAG_LENGTH,
        require_unique=False,
    )
    allowed_tools = _string_list(
        configuration["allowed_tools"],
        "configuration.allowed_tools",
        max_items=MAX_ALLOWED_TOOLS,
        max_item_length=MAX_TOOL_ID_LENGTH,
        require_unique=True,
    )

    try:
        spec = AgentSpec(
            agent_id=parsed_identity["agent_id"],
            agent_version_id=parsed_identity["agent_version_id"],
            name=name,
            slug=slug,
            kind=kind,
            tenant_id=parsed_identity["tenant_id"],
            status=AgentStatus.PUBLISHED,
            description=description,
            model_hint=model_hint,
            allowed_tools=frozenset(allowed_tools),
            system_prompt=system_prompt,
            limits=AgentLimits(
                max_steps=max_steps,
                max_tool_calls=max_tool_calls,
                max_delegation_depth=max_delegation_depth,
                timeout_seconds=timeout_seconds,
            ),
            tags=tuple(tags),
            read_only=read_only,
        )
    except (TypeError, ValueError) as error:
        raise InvalidRequestError(
            "agent runtime snapshot cannot be materialized as an AgentSpec",
            details={"field": "configuration"},
        ) from error

    return ValidatedAgentSnapshot(
        spec=spec,
        version_number=version_number,
        configuration_checksum=checksum,
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_non_finite_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _object(value: Any, field: str, allowed: frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InvalidRequestError(
            "agent runtime snapshot object has the wrong type",
            details={"field": field},
        )
    actual = frozenset(value)
    missing = sorted(allowed - actual)
    unknown = sorted(actual - allowed)
    if missing or unknown:
        raise InvalidRequestError(
            "agent runtime snapshot object does not match the closed schema",
            details={"field": field, "missing": missing, "unknown": unknown},
        )
    return value


def _uuid(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise InvalidRequestError(
            "agent runtime identity must be a UUID string",
            details={"field": field},
        )
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise InvalidRequestError(
            "agent runtime identity must be a canonical UUID",
            details={"field": field},
        ) from error
    canonical = str(parsed)
    if parsed.int == 0 or canonical != value:
        raise InvalidRequestError(
            "agent runtime identity must be a non-nil lowercase canonical UUID",
            details={"field": field},
        )
    return canonical


def _string(value: Any, field: str, minimum: int, maximum: int) -> str:
    if not isinstance(value, str):
        raise InvalidRequestError(
            "agent runtime field must be a string",
            details={"field": field},
        )
    try:
        length = len(value.encode("utf-8"))
    except UnicodeEncodeError as error:
        raise InvalidRequestError(
            "agent runtime string contains invalid Unicode",
            details={"field": field},
        ) from error
    if length < minimum or length > maximum:
        raise InvalidRequestError(
            "agent runtime string is outside the platform bounds",
            details={"field": field, "minimum": minimum, "maximum": maximum},
        )
    return value


def _nullable_string(value: Any, field: str, minimum: int, maximum: int) -> str | None:
    if value is None:
        return None
    return _string(value, field, minimum, maximum)


def _positive_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise InvalidRequestError(
            "agent runtime version number must be a positive integer",
            details={"field": field},
        )
    return value


def _bounded_integer(value: Any, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise InvalidRequestError(
            "agent runtime limit is outside the platform bounds",
            details={"field": field, "minimum": minimum, "maximum": maximum},
        )
    return value


def _string_list(
    value: Any,
    field: str,
    *,
    max_items: int,
    max_item_length: int,
    require_unique: bool,
) -> list[str]:
    if not isinstance(value, list) or len(value) > max_items:
        raise InvalidRequestError(
            "agent runtime list is outside the platform bounds",
            details={"field": field, "maximum_items": max_items},
        )
    items = [
        _string(item, f"{field}[{index}]", 1, max_item_length) for index, item in enumerate(value)
    ]
    if require_unique and len(items) != len(set(items)):
        raise InvalidRequestError(
            "agent runtime list contains duplicate values",
            details={"field": field},
        )
    return items
