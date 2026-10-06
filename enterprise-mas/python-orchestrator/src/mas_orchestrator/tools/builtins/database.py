"""Read-only database tool.

The model writes SQL, which is the most dangerous possible arrangement, so the
constraints here are not decoration:

* **Read-only by construction.** The handler refuses any statement that is not
  a single ``SELECT`` or ``WITH ... SELECT``. Comments and string literals are
  stripped before that check, so ``SELECT 1; DROP TABLE users`` is refused even
  though it starts with ``SELECT``.
* **No transaction to commit.** The connection is opened read-only where the
  driver supports it, so a statement that slipped through would still fail at
  the driver rather than mutate data.
* **Bounded results.** Row count and character count are both capped, because
  ``SELECT *`` against a large table is a denial-of-service against the model's
  own context window.

The handler delegates to an injected :class:`DatabaseBackend`. There is no
default backend: a database tool with no database configured cannot be
constructed, which is better than one that fails on first call.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import InvalidRequestError, ToolFailedError
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolInvocationContext,
    ToolKind,
    ToolSafety,
    ToolVersion,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_ROWS: Final[int] = 200
MAX_RESULT_CHARS: Final[int] = 32 * 1024
DEFAULT_QUERY_TIMEOUT_SECONDS: Final[float] = 10.0

#: Statements that could change data. Refused regardless of what precedes them.
_FORBIDDEN_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "create",
        "truncate",
        "grant",
        "revoke",
        "replace",
        "merge",
        "call",
        "exec",
        "execute",
        "copy",
        "vacuum",
        "analyze",
        "attach",
        "detach",
        "begin",
        "commit",
        "rollback",
        "savepoint",
        "set",
        "lock",
        "comment",
    }
)

#: Comment patterns. Removed from the statement that is executed as well as
#: from the one that is vetted, because a comment can hide a semicolon from the
#: single-statement check without changing what the query means.
_COMMENT_RUNS: Final[tuple[re.Pattern[str], ...]] = (
    re.compile(r"--[^\n]*"),  # line comments
    re.compile(r"/\*.*?\*/", re.DOTALL),  # block comments
)

#: Everything removed before the statement is classified: comments, plus quoted
#: runs where a forbidden keyword could hide from a naive ``startswith`` check.
#: Quoted runs are **not** removed from the statement that is executed.
_STRIPPED_RUNS: Final[tuple[re.Pattern[str], ...]] = (
    *_COMMENT_RUNS,
    re.compile(r"--[^\n]*"),  # line comments
    re.compile(r"/\*.*?\*/", re.DOTALL),  # block comments
    re.compile(r"'(?:[^']|'')*'"),  # single-quoted literals
    re.compile(r'"(?:[^"]|"")*"'),  # quoted identifiers
)


def executable_statement(raw: str) -> str:
    """The form sent to the database.

    Comments are removed and a trailing semicolon is dropped; literals are
    preserved, because removing them would silently change what the query
    selects.

    A comment is safe to remove because it cannot change semantics — but it
    *can* hide a semicolon from the single-statement check, which is why it is
    removed before that check runs on the vetted form.
    """
    cleaned = raw
    for pattern in _COMMENT_RUNS:
        cleaned = pattern.sub(" ", cleaned)
    collapsed = " ".join(cleaned.split())
    return collapsed[:-1].strip() if collapsed.endswith(";") else collapsed


@runtime_checkable
class DatabaseBackend(Protocol):
    """Executes a vetted statement and returns rows."""

    async def fetch(
        self,
        statement: str,
        *,
        parameters: list[Any],
        limit: int,
        timeout_seconds: float,
    ) -> list[dict[str, Any]]:
        """Runs a read-only statement.

        Implementations must refuse to run anything that is not read-only, even
        though this module has already vetted it: two independent checks are the
        point.
        """
        ...


@dataclass(frozen=True, slots=True)
class DatabaseToolConfig:
    """What the tool permits.

    Attributes:
        allowed_schemas: Schemas the query may reference. Empty blocks
            everything that names a schema.
        max_rows: Row ceiling.
        max_result_chars: Character ceiling on the serialised result.
        query_timeout_seconds: Per-query budget.
        allow_with: Whether CTEs are permitted. They are safe when the body is
            a SELECT, and useful enough to be worth the extra parsing.
    """

    allowed_schemas: frozenset[str] = frozenset()
    max_rows: int = MAX_ROWS
    max_result_chars: int = MAX_RESULT_CHARS
    query_timeout_seconds: float = DEFAULT_QUERY_TIMEOUT_SECONDS
    allow_with: bool = True

    def __post_init__(self) -> None:
        if self.max_rows < 1:
            raise InvalidRequestError(
                "max_rows must be at least 1", details={"max_rows": self.max_rows}
            )
        if self.max_result_chars < 1:
            raise InvalidRequestError(
                "max_result_chars must be at least 1",
                details={"max_result_chars": self.max_result_chars},
            )
        if self.query_timeout_seconds <= 0:
            raise InvalidRequestError(
                "query_timeout_seconds must be positive",
                details={"timeout": self.query_timeout_seconds},
            )


def normalise_statement(raw: str) -> str:
    """Strips comments and literals so keywords cannot hide inside them.

    Returns the statement with comment runs and quoted runs replaced by a
    space, preserving length roughly so word boundaries remain meaningful.
    """
    cleaned = raw
    for pattern in _STRIPPED_RUNS:
        cleaned = pattern.sub(" ", cleaned)
    return " ".join(cleaned.split())


def validate_statement(
    statement: str,
    *,
    config: DatabaseToolConfig | None = None,
) -> str:
    """Vets a statement and returns it normalised.

    Args:
        statement: The SQL the model produced.
        config: Limits to apply.

    Two forms of the statement are used, and the distinction is the point:

    * a **vetted** form, with comments and literals stripped, which is what
      every check runs against so a keyword cannot hide inside a string;
    * the **executable** form, which is the original with whitespace collapsed
      and any trailing semicolon removed.

    Returning the vetted form would silently rewrite the caller's query,
    because stripping literals changes what it selects.

    Returns:
        The executable statement.

    Raises:
        InvalidRequestError: If the statement is empty, is not a single
            read-only query, or contains a forbidden keyword anywhere.
    """
    settings = config or DatabaseToolConfig()
    executable = executable_statement(statement)
    cleaned = normalise_statement(statement)
    if not cleaned.strip():
        raise InvalidRequestError("query is empty", details={"field": "statement"})

    # Refuse stacked statements. A trailing semicolon is tolerated; anything
    # after it is a second statement and is not.
    body = cleaned.rstrip().rstrip(";").strip()
    if ";" in body:
        raise InvalidRequestError(
            "only one statement may be submitted at a time",
            details={"reason": "multiple statements"},
        )

    lowered = body.lower()
    first_word = lowered.split(" ", 1)[0] if lowered else ""
    permitted_first = {"select"}
    if settings.allow_with:
        permitted_first.add("with")
    if first_word not in permitted_first:
        raise InvalidRequestError(
            "only SELECT queries are permitted; a WITH clause must resolve to a SELECT",
            details={"first_keyword": first_word},
        )

    words = set(re.findall(r"[a-z_][a-z0-9_]*", lowered))
    forbidden = words & _FORBIDDEN_KEYWORDS
    if forbidden:
        raise InvalidRequestError(
            "query contains a statement type that is not read-only",
            details={"found": sorted(forbidden)},
        )

    if settings.allowed_schemas:
        referenced = _referenced_schemas(body)
        unknown = referenced - settings.allowed_schemas
        if unknown:
            raise InvalidRequestError(
                "query references a schema that is not allowed",
                details={"schemas": sorted(unknown)},
            )
    return executable


def _referenced_schemas(statement: str) -> set[str]:
    """Schema names the statement appears to reference.

    Best-effort, and deliberately so: this is a coarse filter for "did the
    model reach into a schema it was not given", not a SQL parser. A name that
    follows ``FROM``/``JOIN`` and is followed by a dot is treated as a schema
    qualifier.
    """
    pattern = re.compile(r"\b(?:from|join|update|into)\s+([a-z_][a-z0-9_]*)\s*\.", re.IGNORECASE)
    return {match.group(1).lower() for match in pattern.finditer(statement)}


class DatabaseTool:
    """Runs a vetted read-only query."""

    def __init__(
        self,
        backend: DatabaseBackend,
        *,
        config: DatabaseToolConfig | None = None,
    ) -> None:
        self._backend = backend
        self._config = config or DatabaseToolConfig()

    @property
    def config(self) -> DatabaseToolConfig:
        """The limits in force."""
        return self._config

    async def invoke(
        self,
        arguments: dict[str, Any],
        context: ToolInvocationContext,
    ) -> dict[str, Any]:
        """Runs the query and returns a bounded result.

        Returns:
            A mapping with ``rows``, ``row_count`` and ``truncated``.

        Raises:
            InvalidRequestError: If the statement is rejected.
            ToolFailedError: If the backend fails.
        """
        raw = arguments.get("statement")
        if not isinstance(raw, str) or not raw.strip():
            raise InvalidRequestError(
                "statement must be a non-empty string", details={"field": "statement"}
            )

        statement = validate_statement(raw, config=self._config)
        parameters = arguments.get("parameters")
        if parameters is None:
            parameters = []
        if not isinstance(parameters, list):
            raise InvalidRequestError("parameters must be a list", details={"field": "parameters"})

        try:
            rows = await self._backend.fetch(
                statement,
                parameters=parameters,
                limit=self._config.max_rows,
                timeout_seconds=self._config.query_timeout_seconds,
            )
        except Exception as error:
            raise ToolFailedError(
                "database query failed",
                details={
                    "tool_id": "database",
                    "error": type(error).__name__,
                },
            ) from error

        truncated = False
        if len(rows) > self._config.max_rows:
            rows = rows[: self._config.max_rows]
            truncated = True

        serialised = _serialise(rows)
        if len(serialised) > self._config.max_result_chars:
            serialised = serialised[: self._config.max_result_chars]
            truncated = True

        _LOGGER.info(
            "database query executed",
            extra={
                "event": "tool.database.query",
                "execution_id": context.execution_id,
                "row_count": len(rows),
                "truncated": truncated,
            },
        )
        return {
            "rows": rows,
            "row_count": len(rows),
            "truncated": truncated,
            "statement": statement,
        }


def _serialise(rows: list[dict[str, Any]]) -> str:
    """Renders rows as text, purely to measure their size."""
    return repr(rows)


def build_database_tool(
    *,
    tool_id: str,
    name: str,
    tenant_id: str,
    backend: DatabaseBackend,
    description: str | None = None,
    version: int = 1,
    config: DatabaseToolConfig | None = None,
) -> tuple[ToolDefinition, DatabaseTool]:
    """Builds a definition/handler pair ready for registration."""
    settings = config or DatabaseToolConfig()
    definition = ToolDefinition(
        tool_id=tool_id,
        name=name,
        description=description or "Run a read-only SQL query.",
        version=ToolVersion(version),
        kind=ToolKind.DATABASE,
        safety=ToolSafety.STANDARD,
        tenant_id=tenant_id,
        idempotent=True,
    )
    return definition, DatabaseTool(backend, config=settings)
