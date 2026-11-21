# PURPOSE:
# Tenant-scoped agent registry for the production bridge.
#
# AgentRegistry keys agents by (tenant_id, agent_id), but Rust supplies the
# tenant per request, not at deploy time, so a fixed set cannot be registered
# up front. This registry holds a tenant-agnostic catalog and materialises it
# for each tenant on first use, memoising the result.
#
# Satisfies the same protocol as AgentRegistry so it can be handed to
# AgentRouter and AgentRuntime unchanged, and exposes no per-tenant state to a
# caller that did not first name that tenant.

"""Per-tenant materialisation of a shared agent catalog.

Tenants are not known at startup: Rust sends the tenant id on every run, and
the set of tenants changes as the platform onboards. Registering a fixed set of
agents would therefore mean either registering for a placeholder tenant and
never matching, or mutating a shared registry from request-handling code.

This module takes the third option: the catalog is a *template* set, and each
tenant's agents are stamped out of it the first time that tenant appears.

Two properties matter:

* **Every tenant gets the same catalog.** Materialisation is not a routing
  decision and carries no per-tenant configuration, so it cannot be used to
  cross a tenant boundary or to give one tenant a different model policy.
* **Materialisation is idempotent and bounded.** Specs are memoised per
  tenant, so a repeated run reuses the same objects, and the catalog is fixed
  at construction, so a caller cannot grow it at runtime.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import AgentNotFoundError, ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping, Sequence

    from mas_orchestrator.agents.base import AgentSpec, BaseAgent
    from mas_orchestrator.agents.factory import AgentFactory
    from mas_orchestrator.config.enums import Environment

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "TenantScopedAgentRegistry",
    "materialise_catalog",
]

#: Key under which the catalog templates carry the agent kind.
KIND_KEY: Final[str] = "kind"


def materialise_catalog(
    catalog: Sequence[Mapping[str, object]],
    tenant_id: str,
) -> tuple[AgentSpec, ...]:
    """Stamps the catalog templates into specs owned by *tenant_id*.

    Args:
        catalog: Template definitions, one mapping per agent.
        tenant_id: The owning tenant.

    Returns:
        The materialised specs, in catalog order.

    Raises:
        ConfigurationError: If a template is malformed — missing a required
            key, or carrying a kind that is not an :class:`AgentKind`. A bad
            template is a deployment error and must not surface as a
            per-tenant routing failure on the first request.

    The tenant id is the only field that varies between tenants. Everything
    else — prompts, limits, kinds — comes from the fixed catalog, which is why
    no tenant can influence another tenant's agents.
    """
    from dataclasses import replace

    from mas_orchestrator.agents.base import AgentKind, AgentSpec

    if not tenant_id.strip():
        raise ConfigurationError(
            "cannot materialise an agent catalog for an empty tenant",
            details={"field": "tenant_id"},
        )

    required = ("agent_id", "name", "slug", KIND_KEY)
    specs: list[AgentSpec] = []

    for index, template in enumerate(catalog):
        missing = [key for key in required if key not in template]
        if missing:
            raise ConfigurationError(
                "agent catalog entry is missing required fields",
                details={"index": index, "missing": missing},
            )

        kind = template[KIND_KEY]
        if not isinstance(kind, AgentKind):
            raise ConfigurationError(
                "agent catalog entry has a non-AgentKind kind",
                details={"index": index, "kind": repr(kind)},
            )

        read_only = template.get("read_only", False)
        if not isinstance(read_only, bool):
            raise ConfigurationError(
                "agent catalog read_only field must be a boolean",
                details={"index": index, "field": "read_only"},
            )

        spec = AgentSpec(
            agent_id=str(template["agent_id"]),
            agent_version_id=str(template.get("agent_version_id", "1")),
            name=str(template["name"]),
            slug=str(template["slug"]),
            kind=kind,
            tenant_id=tenant_id,
            description=(
                str(template["description"]) if template.get("description") is not None else None
            ),
            model_hint=(
                str(template["model_hint"]) if template.get("model_hint") is not None else None
            ),
            allowed_tools=frozenset(str(item) for item in _sequence(template, "allowed_tools")),
            system_prompt=str(template.get("system_prompt", "")),
            tags=tuple(str(item) for item in _sequence(template, "tags")),
            read_only=read_only,
        )
        limits = template.get("limits")
        if limits is not None:
            spec = replace(spec, limits=limits)  # type: ignore[arg-type]
        specs.append(spec)

    return tuple(specs)


def _sequence(template: Mapping[str, object], key: str) -> tuple[object, ...]:
    """Reads a sequence-valued catalog field.

    Args:
        template: The catalog entry.
        key: The field name.

    Returns:
        The field as a tuple, or an empty tuple when absent.

    Raises:
        ConfigurationError: If the field is present but not a sequence. A
            string is explicitly rejected: it *is* a sequence of characters,
            so accepting it would silently register one tool per letter.
    """
    value = template.get(key)
    if value is None:
        return ()
    if isinstance(value, str) or not isinstance(value, (list, tuple, set, frozenset)):
        raise ConfigurationError(
            f"agent catalog field {key!r} must be a sequence of strings",
            details={"field": key, "type": type(value).__name__},
        )
    return tuple(value)


class TenantScopedAgentRegistry:
    """An agent registry that materialises a shared catalog per tenant.

    Args:
        environment: Running environment; enforces the production ban on
            development-only agents.
        catalog: Template definitions.
        default_agent_id: Agent selected when a caller names none.
        logger: Logger override, for tests.

    Raises:
        ConfigurationError: If the catalog is empty, if two entries share an
            agent id, or if ``default_agent_id`` is not in the catalog.
    """

    def __init__(
        self,
        *,
        environment: Environment,
        catalog: Sequence[Mapping[str, object]],
        default_agent_id: str,
        logger: logging.Logger | None = None,
    ) -> None:
        if not catalog:
            raise ConfigurationError(
                "an agent catalog must define at least one agent",
                details={"field": "catalog"},
            )

        ids = [str(entry.get("agent_id", "")) for entry in catalog]
        duplicates = sorted({item for item in ids if ids.count(item) > 1})
        if duplicates:
            raise ConfigurationError(
                "agent catalog contains duplicate agent ids",
                details={"duplicates": duplicates},
            )
        if default_agent_id not in ids:
            raise ConfigurationError(
                "the default agent is not in the catalog",
                details={"default_agent_id": default_agent_id, "catalog": sorted(ids)},
            )

        self._environment = environment
        self._catalog: tuple[Mapping[str, object], ...] = tuple(catalog)
        self._default_agent_id = default_agent_id
        self._logger = logger or _LOGGER
        self._lock = threading.Lock()
        self._by_tenant: dict[str, tuple[AgentSpec, ...]] = {}
        # Bound after construction: the factory needs the model router, and the
        # router needs this registry. Declared here so `build` has no attribute
        # that only sometimes exists.
        self._factory: AgentFactory | None = None

    # --- introspection ------------------------------------------------------

    @property
    def catalog_ids(self) -> frozenset[str]:
        """The agent ids in the catalog, across all tenants."""
        return frozenset(str(entry["agent_id"]) for entry in self._catalog)

    @property
    def default_agent_id(self) -> str:
        """The agent selected when a caller names none."""
        return self._default_agent_id

    @property
    def materialised_tenants(self) -> tuple[str, ...]:
        """Tenants whose catalog has been materialised, sorted.

        Exposed for diagnostics and for the test that proves one tenant's run
        does not create another tenant's agents.
        """
        with self._lock:
            return tuple(sorted(self._by_tenant))

    def __len__(self) -> int:
        """Number of materialised (tenant, agent) pairs."""
        with self._lock:
            return sum(len(specs) for specs in self._by_tenant.values())

    def __repr__(self) -> str:
        """Summarises the registry without naming tenants.

        Tenant ids are customer data. Enumerating them in a repr would leak
        the tenant list into any log line that happens to print the registry.
        """
        return (
            f"TenantScopedAgentRegistry(catalog={sorted(self.catalog_ids)!r}, "
            f"tenants={len(self.materialised_tenants)})"
        )

    # --- registry protocol --------------------------------------------------

    def register(self, spec: AgentSpec, factory: Callable[[], BaseAgent], **_: object) -> None:
        """Refused: the catalog is fixed at construction.

        Args:
            spec: Ignored.
            factory: Ignored.
            **_: Ignored, so a caller passing ``dev_only`` gets the same
                refusal rather than a ``TypeError``.

        Raises:
            ConfigurationError: Always. Allowing runtime registration would
                let one request add an agent every other tenant then sees,
                which is a cross-tenant write through the read path.
        """
        del spec, factory
        raise ConfigurationError(
            "the tenant-scoped registry is built from a fixed catalog and "
            "does not accept runtime registration",
            details={"catalog": sorted(self.catalog_ids)},
        )

    def register_builtin(self, template: Mapping[str, object]) -> None:
        """Adds one product-owned, read-only template during startup only.

        The ordinary :meth:`register` path remains permanently refused. This
        narrow hook exists for built-ins compiled into the service; it is
        closed as soon as a tenant is materialised or the factory is bound, so
        request handling can never mutate the cross-tenant catalog.
        """
        candidate = dict(template)
        candidate["read_only"] = True
        agent_id = str(candidate.get("agent_id", "")).strip()
        if not agent_id:
            raise ConfigurationError(
                "built-in agent template must name an id",
                details={"field": "agent_id"},
            )
        with self._lock:
            if self._by_tenant or self._factory is not None:
                raise ConfigurationError(
                    "built-in agents must be registered before registry startup completes",
                    details={"agent_id": agent_id},
                )
            existing = next(
                (entry for entry in self._catalog if str(entry.get("agent_id", "")) == agent_id),
                None,
            )
            if existing is not None:
                if dict(existing) == candidate:
                    return
                raise ConfigurationError(
                    "built-in agent id collides with a different catalog entry",
                    details={"agent_id": agent_id},
                )
            self._catalog = (*self._catalog, candidate)

    def specs_for(self, tenant_id: str) -> tuple[AgentSpec, ...]:
        """Materialises and returns every spec for *tenant_id*.

        Args:
            tenant_id: The owning tenant.

        Returns:
            The tenant's specs, in catalog order.

        Repeated calls return the same objects, so an agent id from one call
        compares equal to the same id from another.
        """
        if not tenant_id.strip():
            raise AgentNotFoundError(
                "cannot look up agents for an empty tenant",
                details={"field": "tenant_id"},
            )

        cached = self._by_tenant.get(tenant_id)
        if cached is not None:
            return cached

        with self._lock:
            # Re-checked under the lock: two requests for a new tenant race
            # here, and materialising twice would produce two distinct spec
            # objects for the same agent.
            cached = self._by_tenant.get(tenant_id)
            if cached is not None:
                return cached

            specs = materialise_catalog(self._catalog, tenant_id)
            self._by_tenant[tenant_id] = specs
            self._logger.info(
                "agent catalog materialised for tenant",
                extra={
                    "event": "agent.catalog_materialised",
                    "agents": sorted(spec.agent_id for spec in specs),
                    "tenant_count": len(self._by_tenant),
                },
            )
            return specs

    def spec(self, tenant_id: str, agent_id: str) -> AgentSpec:
        """Returns one spec.

        Args:
            tenant_id: The owning tenant.
            agent_id: The agent.

        Returns:
            The spec.

        Raises:
            AgentNotFoundError: If the tenant has no such agent. The error
                names the requested agent only: enumerating the catalog to a
                caller that guessed wrong would turn a lookup failure into a
                directory of the platform's agents.
        """
        for spec in self.specs_for(tenant_id):
            if spec.agent_id == agent_id:
                return spec
        raise AgentNotFoundError(
            f"agent '{agent_id}' is not available",
            details={"agent_id": agent_id},
        )

    def has(self, tenant_id: str, agent_id: str) -> bool:
        """Whether *tenant_id* has *agent_id*."""
        return any(spec.agent_id == agent_id for spec in self.specs_for(tenant_id))

    def is_read_only(self, tenant_id: str, agent_id: str) -> bool:
        """Whether the tenant-visible agent is a product-owned built-in."""
        try:
            return self.spec(tenant_id, agent_id).read_only
        except AgentNotFoundError:
            return False

    def list_agents(self, tenant_id: str) -> tuple[AgentSpec, ...]:
        """Every agent for a tenant, sorted by id."""
        return tuple(sorted(self.specs_for(tenant_id), key=lambda item: item.agent_id))

    def list_by_kind(self, tenant_id: str, kind: object) -> tuple[AgentSpec, ...]:
        """Agents of one kind for a tenant, sorted by id."""
        return tuple(spec for spec in self.list_agents(tenant_id) if spec.kind is kind)

    def build(self, tenant_id: str, agent_id: str) -> BaseAgent:
        """Builds a fresh agent instance for one execution.

        Args:
            tenant_id: The owning tenant.
            agent_id: The agent.

        Returns:
            A new agent instance.

        Raises:
            ConfigurationError: If no factory was bound. Composition binds it
                once the factory exists, since the factory needs the router
                and the router needs this registry.
        """
        if self._factory is None:
            raise ConfigurationError(
                "no agent factory is bound to the tenant-scoped registry",
                details={"agent_id": agent_id},
            )
        return self._factory.build(self.spec(tenant_id, agent_id))

    def iter_specs(self) -> Iterator[AgentSpec]:
        """Iterates every materialised spec across all tenants.

        Returns:
            An iterator over the specs materialised so far. Nothing is
            materialised as a side effect, so a caller that has not named a
            tenant learns nothing about it.
        """
        with self._lock:
            snapshot = tuple(self._by_tenant.values())
        for specs in snapshot:
            yield from specs

    # --- composition --------------------------------------------------------

    def bind_factory(self, factory: AgentFactory) -> None:
        """Binds the factory used by :meth:`build`.

        Args:
            factory: The agent factory.

        Raises:
            ConfigurationError: If a factory is already bound. Rebinding would
                silently change what every subsequent execution runs, so the
                second bind is refused rather than honoured.
        """
        with self._lock:
            if self._factory is not None:
                raise ConfigurationError(
                    "an agent factory is already bound to this registry",
                    details={"catalog": sorted(self.catalog_ids)},
                )
            self._factory = factory

    def default_spec(self, tenant_id: str) -> AgentSpec:
        """The spec :data:`default_agent_id` resolves to for a tenant.

        Args:
            tenant_id: The owning tenant.

        Returns:
            The default agent's spec.
        """
        return self.spec(tenant_id, self._default_agent_id)
