"""Product-owned agent definitions and the startup registration hook."""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from mas_orchestrator.agents.base import AgentKind, AgentLimits
from mas_orchestrator.agents.builtin.platform_assistant import (
    PLATFORM_ASSISTANT_AGENT_ID,
    AgentDraftProposal,
    AssistantEvidence,
    AssistantQuickAction,
    PlatformAssistantAgent,
    PlatformAssistantResponse,
)
from mas_orchestrator.prompts.builtin_assistant import register_builtin_assistant_prompt
from mas_orchestrator.prompts.registry import PromptRegistry

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry

__all__ = [
    "PLATFORM_ASSISTANT_AGENT_ID",
    "AgentDraftProposal",
    "AssistantEvidence",
    "AssistantQuickAction",
    "PlatformAssistantAgent",
    "PlatformAssistantResponse",
    "register_builtin_agents",
]

PLATFORM_ASSISTANT_TEMPLATE: Final[Mapping[str, object]] = MappingProxyType(
    {
        "agent_id": PLATFORM_ASSISTANT_AGENT_ID,
        "agent_version_id": "1",
        "name": "Platform Assistant",
        "slug": "platform-assistant",
        "kind": AgentKind.STANDARD,
        "description": (
            "Read-only dashboard assistant for performance insights, workflow "
            "optimization suggestions, agent drafts, and recommendations."
        ),
        "system_prompt": "",
        "allowed_tools": (),
        "limits": AgentLimits(max_steps=4, max_tool_calls=0, max_delegation_depth=0),
        "tags": ("builtin", "read-only", "prompt:platform_assistant.system:v1"),
        "read_only": True,
    }
)


def register_builtin_agents(
    registry: TenantScopedAgentRegistry,
    *,
    prompts: PromptRegistry | None = None,
) -> TenantScopedAgentRegistry:
    """Registers the same immutable assistant template for every tenant.

    The prompt is registered and fingerprinted before the agent template is
    added. The tenant registry closes this hook once composition binds its
    factory or materializes a tenant, so the catalog cannot be changed from a
    request handler.
    """
    prompt_registry = prompts if prompts is not None else PromptRegistry()
    prompt = register_builtin_assistant_prompt(prompt_registry)
    template = dict(PLATFORM_ASSISTANT_TEMPLATE)
    template["tags"] = (
        "builtin",
        "read-only",
        f"prompt:{prompt.name}:v{prompt.version}",
        f"prompt_fingerprint:{prompt.fingerprint}",
    )
    registry.register_builtin(template)
    return registry
