"""Tool invocation service.

The tool executor applies seven ordered gates; this service is the layer above
it that answers the questions an agent loop needs *before* calling:

* :meth:`ToolService.describe` — what tools this agent may call, in a form safe
  to put in a prompt. It returns only tools the agent is permitted to use, so
  the model is never shown a tool it cannot reach and never tempted to try.
* :meth:`ToolService.invoke` — runs one call, having first confirmed the agent
  is permitted to make it.

The permission check here is deliberate duplication. The executor already
checks; checking again at the service boundary means a caller that reaches the
service directly — a workflow node, a workflow branch — is held to the same
rule, rather than to whatever the agent layer happened to enforce.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import (
    InvalidRequestError,
    ToolPermissionDeniedError,
)
from mas_orchestrator.guardrails.tool_safety import SafetyAssessment, classify

if TYPE_CHECKING:
    from mas_orchestrator.agents.base import AgentSpec
    from mas_orchestrator.events.emitter import EventEmitter
    from mas_orchestrator.tools.executor import ToolExecutor
    from mas_orchestrator.tools.registry import ToolRegistry
    from mas_orchestrator.tools.types import (
        ToolDefinition,
        ToolInvocationContext,
        ToolResult,
    )

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ToolDescriptor:
    """A tool as described to a model.

    Attributes:
        tool_id: Identifier.
        name: Human-readable name.
        description: What it does.
        version: Definition version.
        input_schema: JSON Schema for the arguments.
        requires_approval: Whether a human must approve before it runs.
    """

    tool_id: str
    name: str
    description: str
    version: int
    input_schema: dict[str, object]
    requires_approval: bool

    def as_dict(self) -> dict[str, object]:
        """A JSON-serialisable view, ready for a provider payload."""
        return {
            "name": self.tool_id,
            "description": self.description,
            "input_schema": self.input_schema,
        }


class ToolService:
    """Agent-scoped tool operations."""

    def __init__(
        self,
        registry: ToolRegistry,
        executor: ToolExecutor,
        *,
        emitter: EventEmitter | None = None,
    ) -> None:
        self._registry = registry
        self._executor = executor
        self._emitter = emitter

    def describe(
        self,
        agent: AgentSpec,
        *,
        tenant_id: str,
    ) -> tuple[ToolDescriptor, ...]:
        """The tools an agent may call, in prompt-ready form.

        Only permitted tools are returned. Describing a forbidden tool would
        invite the model to call it, and the resulting denial is a worse
        experience than never having seen it.
        """
        if not agent.may_use_tools:
            return ()
        descriptors: list[ToolDescriptor] = []
        for definition in self._registry.list_tools(tenant_id):
            if not agent.permits_tool(definition.tool_id):
                continue
            # Approval follows from the safety class. A definition cannot
            # declare itself exempt: that would let a destructive tool opt out
            # of the gate that exists because it is destructive.
            assessment = classify(
                safety=definition.safety.value,
                idempotent=definition.idempotent,
            )
            descriptors.append(
                ToolDescriptor(
                    tool_id=definition.tool_id,
                    name=definition.name,
                    description=definition.description or "",
                    version=definition.version.number,
                    input_schema=dict(definition.input_schema),
                    requires_approval=assessment.requires_approval,
                )
            )
        return tuple(descriptors)

    def assess(
        self,
        *,
        tenant_id: str,
        tool_id: str,
    ) -> SafetyAssessment:
        """The safety posture of a tool.

        Raises:
            InvalidRequestError: If the tool is not registered for the tenant.
        """
        definition = self._registry.get(tenant_id, tool_id)
        return classify(
            safety=definition.safety.value,
            idempotent=definition.idempotent,
        )

    async def invoke(
        self,
        *,
        agent: AgentSpec,
        tenant_id: str,
        tool_id: str,
        arguments: object,
        context: ToolInvocationContext,
        version: int | None = None,
        timeout_seconds: float | None = None,
    ) -> ToolResult:
        """Runs one tool call on an agent's behalf.

        Args:
            agent: The calling agent.
            tenant_id: Authoritative tenant, from the context.
            tool_id: Tool to call.
            arguments: Decoded arguments.
            context: Invocation context.
            version: Specific definition version.
            timeout_seconds: Override for this call only.

        Returns:
            A :class:`ToolResult`. The executor reports a failure as a result
            rather than raising, so the agent loop can decide.

        Raises:
            ToolPermissionDeniedError: If the agent may not call this tool.
        """
        if context.tenant_id != tenant_id:
            raise ToolPermissionDeniedError(
                "invocation context belongs to a different tenant",
                details={"tool_id": tool_id},
            )
        if not agent.permits_tool(tool_id):
            raise ToolPermissionDeniedError(
                "agent is not permitted to call this tool",
                details={"agent_id": agent.agent_id, "tool_id": tool_id},
            )

        started = time.monotonic()
        result = await self._executor.invoke(
            tool_id,
            arguments,
            context,
            version=version,
            timeout_seconds=timeout_seconds,
        )
        duration = int((time.monotonic() - started) * 1000)

        if self._emitter is not None:
            from mas_orchestrator.events.types import (
                tool_failure,
                tool_invocation_duration,
            )

            if result.success:
                await self._emitter.emit(
                    tool_invocation_duration(
                        tenant_id=tenant_id,
                        execution_id=context.execution_id,
                        correlation_id=context.correlation_id,
                        agent_id=context.agent_id,
                        tool_id=tool_id,
                        tool_version=result.tool_version,
                        duration_ms=duration,
                        truncated=result.truncated,
                    )
                )
            else:
                await self._emitter.emit(
                    tool_failure(
                        tenant_id=tenant_id,
                        execution_id=context.execution_id,
                        correlation_id=context.correlation_id,
                        agent_id=context.agent_id,
                        tool_id=tool_id,
                        error_code=_code_of(result.error_code),
                        failure_class=_code_of(result.failure_class),
                    )
                )
        return result

    def definition(self, tenant_id: str, tool_id: str) -> ToolDefinition:
        """A tool's definition.

        Raises:
            InvalidRequestError: If not registered for the tenant.
        """
        if not tenant_id.strip():
            raise InvalidRequestError("a tenant is required", details={"field": "tenant_id"})
        return self._registry.get(tenant_id, tool_id)


def _code_of(coded: object) -> str:
    """Renders an enum-valued field as its string, tolerating ``None``.

    Error codes are carried in events; error messages are not, because a
    message can echo provider content that must not reach a log.
    """
    if coded is None:
        return "unknown"
    return str(getattr(coded, "value", coded))
