"""Agent configuration, version and capability contract models.

The runtime's own agent model lives in :mod:`mas_orchestrator.agents.base`.
This module holds the *wire* shape Rust sends: what an agent is configured to
be, independent of how the runtime happens to execute it.

Keeping them separate is deliberate. The runtime model carries behaviour and
accumulated state; this carries configuration only, so a change to how agents
execute does not silently change what the contract means.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from mas_orchestrator.contracts.enums import AgentStatus, DeploymentStatus
from mas_orchestrator.contracts.errors import InvalidRequestError


@dataclass(frozen=True, slots=True)
class AgentVersionRef:
    """Identifies one version of one agent.

    Rust owns both identifiers. An agent is addressed by version, never by name
    or slug, because a name can be reassigned and a version cannot.
    """

    agent_id: str
    agent_version_id: str

    def __post_init__(self) -> None:
        for name in ("agent_id", "agent_version_id"):
            value = getattr(self, name)
            if not value.strip():
                raise InvalidRequestError(f"{name} must not be empty", details={"field": name})

    def __str__(self) -> str:
        return f"{self.agent_id}@{self.agent_version_id}"


@dataclass(frozen=True, slots=True)
class AgentCapabilities:
    """What an agent is permitted to do.

    Capabilities are additive grants recorded by Rust. The runtime intersects
    them with its own kind rules, so a capability granted here cannot widen what
    the agent's kind forbids.
    """

    may_use_tools: bool = False
    may_delegate: bool = False
    may_read_memory: bool = True
    may_write_memory: bool = False
    may_use_rag: bool = True
    allowed_tools: frozenset[str] = field(default_factory=frozenset)
    delegated_agent_ids: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if self.may_delegate and self.may_use_tools:
            raise InvalidRequestError(
                "an agent may not both delegate and call tools: a supervisor "
                "that also acts makes its own work unaccountable",
                details={"may_delegate": True, "may_use_tools": True},
            )
        if self.allowed_tools and not self.may_use_tools:
            raise InvalidRequestError(
                "tools were granted to an agent that may not use tools",
                details={"allowed_tools": sorted(self.allowed_tools)},
            )
        if self.delegated_agent_ids and not self.may_delegate:
            raise InvalidRequestError(
                "agents were delegated to an agent that may not delegate",
                details={"delegated_agent_ids": sorted(self.delegated_agent_ids)},
            )

    def permits_tool(self, tool_id: str) -> bool:
        """Whether a specific tool is granted."""
        return self.may_use_tools and tool_id in self.allowed_tools

    def permits_delegation(self, agent_id: str) -> bool:
        """Whether delegation to a specific agent is granted."""
        return self.may_delegate and agent_id in self.delegated_agent_ids


@dataclass(frozen=True, slots=True)
class AgentConfigContract:
    """How Rust has configured an agent.

    Attributes:
        ref: The agent version identity.
        tenant_id: Owning tenant, supplied authoritatively by Rust.
        name: Human-readable name.
        slug: URL-safe identifier.
        kind: One of ``standard``, ``supervisor``, ``tool_user``.
        status: Lifecycle status.
        deployment: Deployment state of this version.
        capabilities: What the agent may do.
        model_hint: Preferred model, a routing preference and not a guarantee.
        system_prompt: The agent's instructions.
        max_steps: Step ceiling.
        max_tool_calls: Tool-call ceiling.
        tags: Free-form labels.
    """

    ref: AgentVersionRef
    tenant_id: str
    name: str
    slug: str
    kind: str
    status: AgentStatus = AgentStatus.ACTIVE
    deployment: DeploymentStatus = DeploymentStatus.DEPLOYED
    capabilities: AgentCapabilities = field(default_factory=AgentCapabilities)
    model_hint: str | None = None
    system_prompt: str = ""
    max_steps: int = 1000
    max_tool_calls: int = 500
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "agent config requires a tenant", details={"field": "tenant_id"}
            )
        if self.kind not in ("standard", "supervisor", "tool_user"):
            raise InvalidRequestError("unknown agent kind", details={"kind": self.kind})
        if self.max_steps < 1:
            raise InvalidRequestError(
                "max_steps must be at least 1", details={"max_steps": self.max_steps}
            )
        if self.max_tool_calls < 0:
            raise InvalidRequestError(
                "max_tool_calls must not be negative",
                details={"max_tool_calls": self.max_tool_calls},
            )

    @property
    def is_selectable(self) -> bool:
        """Whether this version may be chosen to run.

        Both gates matter: an active-but-undeployed version is not runnable,
        and neither is a deployed-but-disabled one.
        """
        return self.status is AgentStatus.ACTIVE and self.deployment is DeploymentStatus.DEPLOYED

    @property
    def is_supervisor(self) -> bool:
        """Whether this agent delegates rather than answers."""
        return self.kind == "supervisor"
