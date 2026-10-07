# PURPOSE:
# Composition root for the Rust → Python agent bridge.
#
# Assembles the real object graph the bridge serves: real model providers,
# the real model registry and router, the real agent registry/router/factory,
# the real Agent Runtime, and the real ExecutionService. Also builds the
# inbound identity resolver that authenticates Rust's bearer credential.
#
# Deliberately contains no fallback: if the configured providers cannot be
# constructed, or the resulting model registry is empty, composition fails and
# the process refuses to start. A bridge that would fail every run must not
# accept connections.

"""Composition root for the production agent runtime.

Configuration in, object graph out. Every collaborator here is the real one —
there is no in-memory, echo, or stub provider anywhere below this module, and
a caller cannot obtain one through it.

The graph is::

    ProviderSpec[]  ->  real adapters  ->  ModelRegistry
    ModelRegistry   ->  ModelRouter (allowlist, failover chain, pricing)
    ModelRouter     ->  AgentFactory -> AgentRegistry (per tenant)
    AgentRegistry   ->  AgentRouter  -> AgentRuntime -> ExecutionService

Two decisions are worth stating because they are load-bearing:

**Composition fails closed.** If no provider can be built, or the built
registry contains no model, this module raises rather than returning a runtime
that would fail every call. A service that starts and then rejects everything
is worse than one that fails to start: it reports healthy and it makes the
caller's retry logic do the work of an operator.

**Agents are materialised per tenant.** :class:`AgentRegistry` keys agents by
tenant, and Rust supplies a tenant per request rather than at deploy time. The
catalog is therefore a *template* set, instantiated on first use for each
tenant through :class:`~mas_orchestrator.agents.tenant_registry.TenantScopedAgentRegistry`.
Every tenant receives the same catalog, so this is not a routing decision and
cannot be used to cross a tenant boundary.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.config.enums import ProviderMode
from mas_orchestrator.contracts.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.agents.runtime import AgentRuntime
    from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry
    from mas_orchestrator.config.settings import Settings
    from mas_orchestrator.models.cost import PricingTable
    from mas_orchestrator.models.registry import ModelRegistry
    from mas_orchestrator.providers.factory import ProviderBundle, ProviderSpec
    from mas_orchestrator.security.identity import ServiceIdentityResolver
    from mas_orchestrator.services.execution_service import ExecutionService

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_AGENT_CATALOG",
    "DEFAULT_AGENT_ID",
    "RuntimeBundle",
    "build_identity_resolver",
    "build_provider_specs",
    "build_runtime_bundle",
]

#: Agent the runtime selects when Rust does not pin one.
DEFAULT_AGENT_ID: Final[str] = "platform.default"

#: Secret references consulted for each built-in provider.
_PROVIDER_SECRET_REFS: Final[dict[str, str]] = {
    "openai": "env:MAS_OPENAI_API_KEY",
    "anthropic": "env:MAS_ANTHROPIC_API_KEY",
}


@dataclass(frozen=True, slots=True)
class RuntimeBundle:
    """The composed runtime, plus the handles a caller needs to run it.

    Attributes:
        execution_service: The canonical entrypoint for one execution.
        runtime: The underlying agent runtime.
        identity_resolver: Authenticates inbound Rust credentials.
        providers: The constructed provider bundle, kept so the composition
            root can close every adapter on shutdown.
        agent_registry: The tenant-scoped registry, exposed for diagnostics.
    """

    execution_service: ExecutionService
    runtime: AgentRuntime
    identity_resolver: ServiceIdentityResolver
    providers: ProviderBundle
    agent_registry: TenantScopedAgentRegistry

    async def aclose(self) -> None:
        """Closes every provider transport owned by the bundle."""
        await self.providers.aclose()

    def describe(self) -> dict[str, object]:
        """A log-safe summary of what was composed.

        Returns:
            A mapping with provider ids, model count and the agent catalog.
            No credential, endpoint or secret reference appears: this summary
            is written to logs at startup, and startup logs are the single
            most widely read — and most widely forwarded — output a service
            produces.
        """
        return {
            "providers": sorted(
                str(getattr(item, "provider_id", "unknown")) for item in self.providers.providers
            ),
            "skipped_providers": list(self.providers.skipped),
            "agent_catalog": sorted(self.agent_registry.catalog_ids),
            "default_agent_id": self.agent_registry.default_agent_id,
        }


def build_provider_specs(settings: Settings) -> list[ProviderSpec]:
    """Builds provider specifications from configuration.

    Args:
        settings: Loaded service settings.

    Returns:
        One specification per enabled provider, in configuration order.

    Raises:
        ConfigurationError: If providers are disabled, if an unknown provider
            is enabled, or if an enabled provider has no credential reference.

    A provider with no credential reference is a configuration error rather
    than a provider that is quietly skipped. Skipping it would produce a
    registry that works for some models and fails for others depending on
    which key happened to be present in the environment.
    """
    from mas_orchestrator.providers.factory import ProviderSpec

    if settings.providers.mode is ProviderMode.DISABLED:
        raise ConfigurationError(
            "model providers are disabled; the agent bridge requires a live provider",
            details={"mode": settings.providers.mode.value},
        )

    enabled = tuple(dict.fromkeys(settings.providers.enabled_providers))
    if not enabled:
        raise ConfigurationError(
            "no model providers are enabled; the agent bridge cannot run without one",
            details={"field": "providers.enabled_providers"},
        )

    specs: list[ProviderSpec] = []
    for provider_id in enabled:
        secret_ref = _PROVIDER_SECRET_REFS.get(provider_id)
        if secret_ref is None:
            raise ConfigurationError(
                "unknown provider id; only built-in adapters can be enabled here",
                details={
                    "provider": provider_id,
                    "supported": sorted(_PROVIDER_SECRET_REFS),
                },
            )
        specs.append(
            ProviderSpec(
                provider_id=provider_id,
                enabled=True,
                api_key_secret_ref=secret_ref,
                connect_timeout_seconds=settings.providers.connect_timeout_seconds,
                read_timeout_seconds=settings.providers.request_timeout_seconds,
            )
        )
    return specs


def build_identity_resolver(settings: Settings) -> ServiceIdentityResolver:
    """Builds the resolver that authenticates Rust's bearer credential.

    Args:
        settings: Loaded service settings.

    Returns:
        A resolver over the configured service token.

    Raises:
        ConfigurationError: If no service token is configured. An
            unauthenticated bridge is not a degraded mode — any process that
            can reach the port could spend the platform's model budget, so the
            resolver refuses to exist rather than accepting anonymous callers.

    The token is read from a secret reference, never from a committed
    configuration value, and the resolver never renders it.
    """
    from mas_orchestrator.security.credentials import resolve_secret
    from mas_orchestrator.security.identity import (
        ServiceScope,
        StaticTokenServiceIdentityResolver,
    )

    try:
        # ``Secret`` refuses an empty value, so a configured-but-empty token is
        # reported here as a configuration error rather than becoming a
        # resolver that accepts empty credentials.
        token = resolve_secret(settings.security.service_token_ref).reveal()
    except ConfigurationError as error:
        raise ConfigurationError(
            "the agent bridge requires a configured service token; "
            "anonymous callers are never accepted",
            details={"field": "security.service_token_ref"},
        ) from error

    return StaticTokenServiceIdentityResolver(
        token=token,
        subject="rust-core",
        scopes=frozenset(
            {
                ServiceScope.EXECUTE_AGENT,
                ServiceScope.INVOKE_API,
                ServiceScope.READ_HEALTH,
            }
        ),
    )


def build_runtime_bundle(settings: Settings) -> RuntimeBundle:
    """Composes the production runtime graph.

    Args:
        settings: Loaded service settings.

    Returns:
        A :class:`RuntimeBundle`.

    Raises:
        ConfigurationError: At any stage where composition cannot produce a
            working runtime. Every failure names the field that must change.

    The registry is checked to be non-empty after providers are built. A
    provider that constructs cleanly but registers no models is the most
    quietly broken state in the system: every call would route to nothing and
    report "no model satisfies the policy", which reads like a request problem
    rather than a deployment one.
    """
    from mas_orchestrator.agents.factory import AgentFactory
    from mas_orchestrator.agents.router import AgentRouter, build_policy
    from mas_orchestrator.agents.runtime import AgentRuntime, RuntimeConfig
    from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry
    from mas_orchestrator.models.registry import ModelRegistry
    from mas_orchestrator.models.router import ModelRouter
    from mas_orchestrator.models.router import build_policy as build_model_policy
    from mas_orchestrator.providers.factory import build_providers
    from mas_orchestrator.services.execution_service import build_execution_service

    specs = build_provider_specs(settings)

    registry = ModelRegistry()
    providers = build_providers(specs, environment=settings.environment, registry=registry)

    if len(registry) == 0:
        raise ConfigurationError(
            "the constructed providers registered no models; routing cannot succeed",
            details={
                "providers": [
                    str(getattr(item, "provider_id", "unknown")) for item in providers.providers
                ]
            },
        )

    pricing = _pricing_table(settings)
    allowlist = _allowlist_for(settings, registry)
    model_router = ModelRouter(
        registry,
        build_model_policy(
            allowlist,
            preferred_models=_preferred_models(settings, allowlist),
        ),
        # The router prices candidates from raw rates; the runtime wraps the
        # same rates in a validated PricingTable to accumulate cost.
        pricing=pricing.rates,
    )

    agent_registry = TenantScopedAgentRegistry(
        environment=settings.environment,
        catalog=DEFAULT_AGENT_CATALOG,
        default_agent_id=_default_agent_id(settings),
    )
    agent_factory = AgentFactory(
        router=model_router,
        provider_for=registry.resolve,  # type: ignore[arg-type]
        tool_executor=None,
        tool_registry=None,
        agent_registry=agent_registry,  # type: ignore[arg-type]
    )
    agent_registry.bind_factory(agent_factory)

    agent_router = AgentRouter(
        # The registry is typed against AgentRegistry; TenantScopedAgentRegistry
        # satisfies the same protocol. The cast is local and documented rather
        # than hidden.
        agent_registry,  # type: ignore[arg-type]
        build_policy(tuple(agent_registry.catalog_ids)),
    )

    runtime = AgentRuntime(
        registry=agent_registry,  # type: ignore[arg-type]
        router=agent_router,
        factory=agent_factory,
        pricing=pricing,
        config=RuntimeConfig(
            default_timeout_seconds=settings.agents.max_run_seconds,
            max_parallel_agents=1,
        ),
    )
    execution_service = build_execution_service(runtime)

    bundle = RuntimeBundle(
        execution_service=execution_service,
        runtime=runtime,
        identity_resolver=build_identity_resolver(settings),
        providers=providers,
        agent_registry=agent_registry,
    )

    _LOGGER.info(
        "runtime composed",
        extra={"event": "startup.runtime", **bundle.describe()},
    )
    return bundle


def _pricing_table(settings: Settings) -> PricingTable:
    """Builds the pricing table from configuration.

    Args:
        settings: Loaded service settings.

    Returns:
        A validated :class:`PricingTable`. An empty configuration yields an
        empty table, which reports cost as unknown rather than as zero — a
        fabricated zero would be indistinguishable from a free call.
    """
    from mas_orchestrator.models.cost import PricingTable

    return PricingTable(
        rates={key: dict(value) for key, value in settings.providers.pricing.items()}
    )


def _allowlist_for(settings: Settings, registry: ModelRegistry) -> tuple[str, ...]:
    """Resolves the routing allowlist.

    Args:
        settings: Loaded service settings.
        registry: The populated model registry.

    Returns:
        The allowlisted ``provider/model`` keys.

    Raises:
        ConfigurationError: If an allowlisted model is not in the registry.

    An allowlist naming a model no provider offers is a deployment error, not
    a routing miss. Left alone it would route nothing and report "no model
    satisfies the policy" for every request — indistinguishable from a caller
    asking for something forbidden.
    """
    if not settings.providers.allowed_models:
        return tuple(registry.models())

    unknown = [key for key in settings.providers.allowed_models if key not in registry]
    if unknown:
        raise ConfigurationError(
            "allowlisted models are not offered by any enabled provider",
            details={"unknown": unknown, "available": sorted(registry.models())},
        )
    return tuple(settings.providers.allowed_models)


def _preferred_models(settings: Settings, allowlist: Sequence[str]) -> tuple[str, ...]:
    """Resolves the routing preference order.

    Args:
        settings: Loaded service settings.
        allowlist: The resolved allowlist.

    Returns:
        The configured default model first, then the failover chain. Both are
        filtered to the allowlist: a preference outside the allowlist could
        never be honoured, so it is dropped rather than kept as a trap.
    """
    ordered: list[str] = []
    if settings.providers.default_model:
        ordered.append(settings.providers.default_model)
    ordered.extend(settings.providers.failover_models)

    allowed = set(allowlist)
    return tuple(key for key in ordered if key in allowed)


def _default_agent_id(settings: Settings) -> str:
    """The agent selected when Rust does not pin one.

    Args:
        settings: Loaded service settings.

    Returns:
        The configured default agent id, or :data:`DEFAULT_AGENT_ID`.
    """
    configured = getattr(settings.agents, "default_agent_id", None)
    return configured or DEFAULT_AGENT_ID


# The catalog is defined after the functions that read it so the module reads
# top-down. Each entry is a tenant-agnostic template; the tenant id is stamped
# in when the catalog is materialised.
def _catalog() -> tuple[dict[str, object], ...]:
    """The built-in agent catalog.

    Returns:
        Template definitions for the agents every tenant receives.

    Only agents that can run without tools are included. A tool-user agent
    built without a tool executor can be shown tools it can never call, which
    produces confident answers from imagination; the tool-capable catalog is
    registered by the deployment that also configures tools.
    """
    from mas_orchestrator.agents.base import AgentKind, AgentLimits

    return (
        {
            "agent_id": DEFAULT_AGENT_ID,
            "name": "Platform Assistant",
            "slug": "platform-default",
            "kind": AgentKind.STANDARD,
            "description": "General-purpose assistant used when no agent is pinned.",
            "system_prompt": (
                "You are the Enterprise Multi-Agent System assistant. "
                "Answer the request directly and concisely. "
                "Never invent facts, and say so when you do not know."
            ),
            "limits": AgentLimits(),
        },
        {
            "agent_id": "platform.analyst",
            "name": "Platform Analyst",
            "slug": "platform-analyst",
            "kind": AgentKind.STANDARD,
            "description": "Reasoning-focused agent for analysis requests.",
            "system_prompt": (
                "You are an analyst for the Enterprise Multi-Agent System. "
                "Break the request into the reasoning it requires, then answer. "
                "State assumptions explicitly."
            ),
            "limits": AgentLimits(),
        },
    )


DEFAULT_AGENT_CATALOG: Final[tuple[dict[str, object], ...]] = _catalog()
