"""Tool registry: id + version -> definition, with the executable bound to it.

The registry is deliberately small. It answers exactly one question — "which
definition and which executable serve this tool id for this tenant?" — and it
refuses to answer that question ambiguously, because a tool that resolves
differently depending on import order is a tool whose behaviour cannot be
audited.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError, ToolNotFoundError
from mas_orchestrator.tools.types import ToolDefinition, ToolVersion

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas_orchestrator.tools.types import ToolInvocationContext


@runtime_checkable
class ToolHandler(Protocol):
    """The executable behind a tool definition."""

    async def invoke(
        self,
        arguments: Mapping[str, object],
        context: ToolInvocationContext,
    ) -> object:
        """Executes the tool and returns its raw output.

        The handler is responsible only for doing the work. Timeout, retry,
        output bounding and failure classification are the executor's job, so a
        handler must not implement them itself.

        Raises:
            Exception: Any failure. The executor classifies it.
        """
        ...


@runtime_checkable
class AsyncClosable(Protocol):
    """Something that holds resources needing an orderly shutdown."""

    async def aclose(self) -> None:
        """Releases resources."""
        ...


class ToolRegistry:
    """Holds tool definitions and their handlers, scoped by tenant and version.

    A tool is keyed by ``(tenant_id, tool_id, version)``. Versions are never
    overwritten or mutated: registering a new version supersedes the old one
    for *new* invocations while leaving historical invocations explainable.

    Attributes:
        environment: Used only to refuse a ``dev`` registration in production.
    """

    def __init__(self, *, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._definitions: dict[tuple[str, str, int], ToolDefinition] = {}
        self._handlers: dict[tuple[str, str, int], ToolHandler] = {}
        self._current: dict[tuple[str, str], int] = {}

    def register(
        self,
        definition: ToolDefinition,
        handler: ToolHandler,
        *,
        dev_only: bool = False,
    ) -> None:
        """Registers one version of a tool.

        Args:
            definition: The tool definition, including its version.
            handler: The executable for this specific version.
            dev_only: When true, registration is refused outside development
                and test environments. Used for handlers that are unsafe to
                expose in production.

        Raises:
            ConfigurationError: If a ``dev_only`` tool is registered in
                production, or if the same version is registered twice.
            ToolNotFoundError: Never; retained for interface symmetry.
        """
        if not isinstance(handler, ToolHandler):
            raise ConfigurationError(
                "tool handler does not satisfy the ToolHandler protocol",
                details={"tool_id": definition.tool_id},
            )
        if dev_only and self._environment is Environment.PRODUCTION:
            raise ConfigurationError(
                "this tool is development-only and must not be registered in production",
                details={"tool_id": definition.tool_id, "environment": self._environment.value},
            )

        key = (definition.tenant_id, definition.tool_id, definition.version.number)
        if key in self._definitions:
            raise ConfigurationError(
                "tool version is already registered",
                details={
                    "tool_id": definition.tool_id,
                    "version": definition.version.number,
                    "tenant_id": definition.tenant_id,
                },
            )

        self._definitions[key] = definition
        self._handlers[key] = handler

        identity = (definition.tenant_id, definition.tool_id)
        previous = self._current.get(identity)
        if previous is None or definition.version.number > previous:
            self._current[identity] = definition.version.number

    def get(
        self,
        tenant_id: str,
        tool_id: str,
        version: int | None = None,
    ) -> ToolDefinition:
        """Returns a definition.

        Args:
            tenant_id: Owning tenant.
            tool_id: Tool identifier.
            version: Specific version. ``None`` resolves the newest.

        Raises:
            ToolNotFoundError: If no matching definition exists.
        """
        number = version if version is not None else self._current.get((tenant_id, tool_id))
        if number is None:
            raise ToolNotFoundError(
                f"tool '{tool_id}' is not registered",
                details={"tool_id": tool_id, "tenant_id": tenant_id},
            )
        key = (tenant_id, tool_id, number)
        definition = self._definitions.get(key)
        if definition is None:
            raise ToolNotFoundError(
                f"tool '{tool_id}' has no version {number}",
                details={"tool_id": tool_id, "version": number, "tenant_id": tenant_id},
            )
        return definition

    def handler(
        self,
        tenant_id: str,
        tool_id: str,
        version: int | None = None,
    ) -> ToolHandler:
        """Returns the handler bound to a definition.

        Raises:
            ToolNotFoundError: If no matching handler exists.
        """
        number = version if version is not None else self._current.get((tenant_id, tool_id))
        if number is None:
            raise ToolNotFoundError(
                f"tool '{tool_id}' is not registered",
                details={"tool_id": tool_id, "tenant_id": tenant_id},
            )
        handler = self._handlers.get((tenant_id, tool_id, number))
        if handler is None:
            raise ToolNotFoundError(
                f"tool '{tool_id}' has no handler for version {number}",
                details={"tool_id": tool_id, "version": number, "tenant_id": tenant_id},
            )
        return handler

    def versions(self, tenant_id: str, tool_id: str) -> tuple[int, ...]:
        """All registered versions of a tool, ascending."""
        found = [
            version
            for (owner, name, version) in self._definitions
            if owner == tenant_id and name == tool_id
        ]
        return tuple(sorted(found))

    def list_tools(self, tenant_id: str) -> tuple[ToolDefinition, ...]:
        """Every current tool definition for a tenant, sorted by id.

        Only the newest version of each tool is returned; a caller that needs a
        specific version uses :meth:`get`.
        """
        definitions = []
        for (owner, tool_id), number in self._current.items():
            if owner != tenant_id:
                continue
            definition = self._definitions.get((owner, tool_id, number))
            if definition is not None:
                definitions.append(definition)
        return tuple(sorted(definitions, key=lambda item: item.tool_id))

    async def aclose(self) -> None:
        """Closes every handler that holds resources.

        Failures are collected rather than raised on the first one, so a
        misbehaving tool cannot prevent the rest from being released.
        """
        failures: list[str] = []
        for handler in self._handlers.values():
            if isinstance(handler, AsyncClosable):
                try:
                    await handler.aclose()
                except Exception as error:
                    failures.append(f"{type(handler).__name__}: {error}")
        if failures:
            raise ConfigurationError(
                "one or more tool handlers failed to close cleanly",
                details={"failures": failures},
            )


def bump(definition: ToolDefinition, **changes: object) -> ToolDefinition:
    """Produces a new definition at the next version.

    Tool definitions are immutable, so an edit is a new version rather than a
    mutation. This is what lets an invocation record "version 3 served this
    call" and remain true after the tool has been edited to version 4.
    """
    from dataclasses import replace

    next_version = ToolVersion(definition.version.number + 1)
    return replace(definition, version=next_version, **changes)  # type: ignore[arg-type]
