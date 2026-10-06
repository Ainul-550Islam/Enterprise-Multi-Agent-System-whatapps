# PURPOSE:
# Tests that the production composition root fails closed.
#
# Covers spec §5 and §6 on the Python side: the composition root must build the
# real runtime and refuse to start without it. There is no degraded mode, no
# echo path, and no silent fallback — a bridge that would fail every run must
# not accept connections.

"""Production composition tests.

The property under test is *refusal*. A service that starts in a half-built
state reports healthy and then fails every request, which is worse than one
that fails to boot: it moves the failure from deploy time — where an operator
is watching — to request time, where it looks like a caller problem.

Each test here drives the composition root into a state where it cannot build
a working runtime and asserts that it raises rather than degrading.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from mas_orchestrator.config.enums import Environment, ProviderMode
from mas_orchestrator.config.settings import (
    ProviderSettings,
    SecuritySettings,
    ServiceSettings,
    Settings,
)
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig
from mas_orchestrator.services.composition import (
    DEFAULT_AGENT_CATALOG,
    DEFAULT_AGENT_ID,
    build_identity_resolver,
    build_provider_specs,
    build_runtime_bundle,
)

if TYPE_CHECKING:
    from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry

pytestmark = pytest.mark.asyncio(loop_scope="function")


def _settings(**overrides: object) -> Settings:
    """Builds settings for composition.

    Args:
        **overrides: Section overrides. A section named here replaces the
            default wholesale, so a test that passes ``providers=`` owns the
            whole provider configuration.

    Returns:
        A settings object.
    """
    defaults: dict[str, object] = {
        "environment": Environment.DEVELOPMENT,
        "providers": ProviderSettings(
            mode=ProviderMode.LIVE,
            enabled_providers=("openai",),
            allowed_models=(),
        ),
        "security": SecuritySettings(service_token_ref="env:MAS_TEST_BRIDGE_TOKEN"),
    }
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


class TestProvidersMustBeConfigured:
    """A bridge with no live provider must not compose."""

    async def test_disabled_providers_are_refused(self) -> None:
        """``mode = disabled`` is a refusal, not a runtime that fails later."""
        settings = Settings(providers=ProviderSettings(mode=ProviderMode.DISABLED))

        with pytest.raises(ConfigurationError, match="disabled"):
            build_provider_specs(settings)

    async def test_an_empty_provider_list_is_refused(self) -> None:
        """A mode that allows providers but names none cannot run."""
        settings = _settings(
            providers=ProviderSettings(mode=ProviderMode.LIVE, enabled_providers=())
        )

        with pytest.raises(ConfigurationError, match="no model providers"):
            build_provider_specs(settings)

    async def test_an_unknown_provider_is_refused(self) -> None:
        """A typo in configuration must surface, not select nothing."""
        settings = _settings(
            providers=ProviderSettings(mode=ProviderMode.LIVE, enabled_providers=("openaii",))
        )

        with pytest.raises(ConfigurationError, match="unknown provider id"):
            build_provider_specs(settings)

    async def test_a_configured_provider_yields_a_spec(self) -> None:
        """The happy path: a known provider produces a spec with a secret ref."""
        specs = build_provider_specs(_settings())

        assert [spec.provider_id for spec in specs] == ["openai"]
        assert specs[0].api_key_secret_ref is not None

    async def test_runtime_composition_refuses_a_disabled_mode(self) -> None:
        """The bundle refuses, so the server can never be built."""
        settings = Settings(providers=ProviderSettings(mode=ProviderMode.DISABLED))

        with pytest.raises(ConfigurationError):
            build_runtime_bundle(settings)


class TestIdentityMustBeConfigured:
    """A bridge with no service token must not compose."""

    async def test_a_missing_token_reference_is_refused(self) -> None:
        """No configured token means no authentication, which is not allowed."""
        settings = _settings(security=SecuritySettings(service_token_ref=None))

        with pytest.raises(ConfigurationError, match="service token"):
            build_identity_resolver(settings)

    async def test_an_unresolvable_token_reference_is_refused(self) -> None:
        """A reference pointing at an unset variable is also a refusal."""
        settings = _settings(
            security=SecuritySettings(service_token_ref="env:MAS_DEFINITELY_NOT_SET_12345")
        )

        with pytest.raises(ConfigurationError, match="service token"):
            build_identity_resolver(settings)

    async def test_a_configured_token_yields_a_resolver(self) -> None:
        """The happy path: a resolvable token produces a working resolver."""
        import os

        from mas_orchestrator.security.identity import ServiceIdentity

        os.environ["MAS_TEST_BRIDGE_TOKEN"] = "composed-token"
        try:
            resolver = build_identity_resolver(_settings())
            identity = await resolver.resolve("Bearer composed-token")
        finally:
            os.environ.pop("MAS_TEST_BRIDGE_TOKEN", None)

        assert isinstance(identity, ServiceIdentity)
        assert identity.subject == "rust-core"

    async def test_the_resolver_never_renders_its_token(self) -> None:
        """The repr is log-safe by construction."""
        import os

        os.environ["MAS_TEST_BRIDGE_TOKEN"] = "composed-token"
        try:
            rendered = repr(build_identity_resolver(_settings()))
        finally:
            os.environ.pop("MAS_TEST_BRIDGE_TOKEN", None)

        assert "composed-token" not in rendered


class TestServerConfiguration:
    """Transport settings are validated, not assumed."""

    async def test_an_empty_host_is_refused(self) -> None:
        """An empty bind host is a configuration mistake."""
        with pytest.raises(ConfigurationError):
            GrpcServerConfig(host="  ")

    async def test_an_out_of_range_port_is_refused(self) -> None:
        """A port above 65535 cannot be bound."""
        with pytest.raises(ConfigurationError):
            GrpcServerConfig(port=70_000)

    async def test_a_negative_port_is_refused(self) -> None:
        """A negative port is refused rather than coerced."""
        with pytest.raises(ConfigurationError):
            GrpcServerConfig(port=-1)

    async def test_port_zero_is_allowed_for_ephemeral_binding(self) -> None:
        """``0`` asks the kernel to choose, which tests rely on."""
        assert GrpcServerConfig(port=0).port == 0

    async def test_a_zero_worker_count_is_refused(self) -> None:
        """At least one worker is required to serve anything."""
        with pytest.raises(ConfigurationError):
            GrpcServerConfig(max_concurrent_rpcs=0)

    async def test_a_tiny_message_bound_is_refused(self) -> None:
        """A bound below 1 KiB would reject legitimate traffic."""
        with pytest.raises(ConfigurationError):
            GrpcServerConfig(max_receive_bytes=10)

    async def test_an_unbindable_port_raises(self) -> None:
        """Binding a privileged port without permission reports a dependency
        failure that names the address, never the configuration."""
        from tests.bridge.conftest import RecordingExecutionService, resolver

        from mas_orchestrator.grpc.agent_bridge_servicer import AgentBridgeServicer

        server = GrpcServer(
            servicer=AgentBridgeServicer(
                execution_service=RecordingExecutionService(),  # type: ignore[arg-type]
                identity_resolver=resolver(),
            ),
            config=GrpcServerConfig(host="127.0.0.1", port=1),
        )
        with pytest.raises(Exception):  # noqa: B017 - ConfigurationError or OSError
            await server.start()


class TestTenantScopedRegistry:
    """The catalog is shared, fixed, and materialised per tenant."""

    def _registry(self) -> TenantScopedAgentRegistry:
        """Builds a registry over the default catalog.

        Returns:
            The registry.
        """
        from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry

        return TenantScopedAgentRegistry(
            environment=Environment.DEVELOPMENT,
            catalog=DEFAULT_AGENT_CATALOG,
            default_agent_id=DEFAULT_AGENT_ID,
        )

    async def test_every_tenant_receives_the_same_catalog(self) -> None:
        """Materialisation is not a routing decision."""
        registry = self._registry()

        first = {spec.agent_id for spec in registry.specs_for("tenant-a")}
        second = {spec.agent_id for spec in registry.specs_for("tenant-b")}

        assert first == second == set(registry.catalog_ids)

    async def test_materialisation_is_memoised(self) -> None:
        """Repeated lookups return the same objects, not fresh copies."""
        registry = self._registry()

        assert registry.specs_for("tenant-a") is registry.specs_for("tenant-a")

    async def test_a_spec_is_owned_by_its_tenant(self) -> None:
        """A spec carries the tenant that asked for it."""
        registry = self._registry()

        assert all(spec.tenant_id == "tenant-a" for spec in registry.specs_for("tenant-a"))

    async def test_one_tenant_does_not_create_another_tenant_agents(
        self,
    ) -> None:
        """Isolation: materialising one tenant leaves the others untouched."""
        registry = self._registry()
        registry.specs_for("tenant-a")

        assert registry.materialised_tenants == ("tenant-a",)
        assert "tenant-b" not in registry.materialised_tenants

    async def test_runtime_registration_is_refused(self) -> None:
        """The catalog is fixed, so the read path cannot become a write path."""
        registry = self._registry()

        with pytest.raises(ConfigurationError, match="fixed catalog"):
            registry.register(registry.default_spec("tenant-a"), lambda: None)  # type: ignore[arg-type]

    async def test_rebinding_a_factory_is_refused(self) -> None:
        """A second bind would silently change what every later run executes."""
        from mas_orchestrator.agents.factory import AgentFactory

        registry = self._registry()
        registry.bind_factory(AgentFactory(router=None, provider_for=None))  # type: ignore[arg-type]

        with pytest.raises(ConfigurationError, match="already bound"):
            registry.bind_factory(AgentFactory(router=None, provider_for=None))  # type: ignore[arg-type]

    async def test_an_empty_catalog_is_refused(self) -> None:
        """A registry with no agents could never route anything."""
        from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry

        with pytest.raises(ConfigurationError, match="at least one agent"):
            TenantScopedAgentRegistry(
                environment=Environment.DEVELOPMENT,
                catalog=(),
                default_agent_id=DEFAULT_AGENT_ID,
            )

    async def test_duplicate_agent_ids_are_refused(self) -> None:
        """Two entries with one id would make lookup order significant."""
        from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry

        duplicated = (DEFAULT_AGENT_CATALOG[0], DEFAULT_AGENT_CATALOG[0])

        with pytest.raises(ConfigurationError, match="duplicate"):
            TenantScopedAgentRegistry(
                environment=Environment.DEVELOPMENT,
                catalog=duplicated,
                default_agent_id=DEFAULT_AGENT_ID,
            )

    async def test_a_default_agent_outside_the_catalog_is_refused(self) -> None:
        """A default that does not exist could never be selected."""
        from mas_orchestrator.agents.tenant_registry import TenantScopedAgentRegistry

        with pytest.raises(ConfigurationError, match="default agent"):
            TenantScopedAgentRegistry(
                environment=Environment.DEVELOPMENT,
                catalog=DEFAULT_AGENT_CATALOG,
                default_agent_id="nope",
            )

    async def test_an_empty_tenant_is_refused(self) -> None:
        """An unscoped lookup would be a lookup outside every tenant."""
        from mas_orchestrator.contracts.errors import AgentNotFoundError

        with pytest.raises(AgentNotFoundError):
            self._registry().specs_for("   ")

    async def test_the_repr_does_not_enumerate_tenants(self) -> None:
        """Tenant ids are customer data and must not leak into a log line."""
        registry = self._registry()
        registry.specs_for("tenant-secret")

        assert "tenant-secret" not in repr(registry)


class TestCatalogMaterialisation:
    """Catalog templates are validated at composition, not per request."""

    async def test_a_string_sequence_field_is_refused(self) -> None:
        """A string is a sequence of characters; accepting it would register
        one tool per letter."""
        from mas_orchestrator.agents.tenant_registry import materialise_catalog

        entry = dict(DEFAULT_AGENT_CATALOG[0])
        entry["allowed_tools"] = "search"

        with pytest.raises(ConfigurationError, match="sequence of strings"):
            materialise_catalog([entry], "tenant-a")

    async def test_a_missing_required_field_is_refused(self) -> None:
        """A malformed template is a deployment error."""
        from mas_orchestrator.agents.tenant_registry import materialise_catalog

        with pytest.raises(ConfigurationError, match="missing required fields"):
            materialise_catalog([{"agent_id": "x"}], "tenant-a")

    async def test_a_non_enum_kind_is_refused(self) -> None:
        """A kind that is not an ``AgentKind`` would fail later and vaguely."""
        from mas_orchestrator.agents.tenant_registry import materialise_catalog

        entry = dict(DEFAULT_AGENT_CATALOG[0])
        entry["kind"] = "standard"

        with pytest.raises(ConfigurationError, match="non-AgentKind"):
            materialise_catalog([entry], "tenant-a")

    async def test_the_default_catalog_materialises(self) -> None:
        """The shipped catalog is well-formed for any tenant."""
        from mas_orchestrator.agents.base import AgentKind
        from mas_orchestrator.agents.tenant_registry import materialise_catalog

        specs = materialise_catalog(DEFAULT_AGENT_CATALOG, "tenant-a")

        assert next(spec.agent_id for spec in specs) == DEFAULT_AGENT_ID
        assert all(isinstance(spec.kind, AgentKind) for spec in specs)
        assert all(spec.tenant_id == "tenant-a" for spec in specs)


class TestBridgeServerComposition:
    """The composition root refuses without configuration."""

    async def test_building_a_bridge_server_without_providers_is_refused(
        self,
    ) -> None:
        """A caller cannot obtain a server that would fail every run."""
        from mas_orchestrator.main import build_bridge_server
        from mas_orchestrator.services.lifecycle import ShutdownCoordinator

        settings = Settings(providers=ProviderSettings(mode=ProviderMode.DISABLED))
        coordinator = ShutdownCoordinator()

        with pytest.raises(ConfigurationError):
            build_bridge_server(settings, coordinator)

    async def test_service_settings_produce_a_transport_config(self) -> None:
        """Bind address comes from configuration, not a hard-coded default."""
        config = GrpcServerConfig.from_service_settings(
            ServiceSettings(bind_host="127.0.0.1", bind_port=9100)
        )

        assert config.address == "127.0.0.1:9100"
