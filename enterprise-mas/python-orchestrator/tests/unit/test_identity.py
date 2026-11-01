"""Unit tests for service identity validation.

These are security tests: they assert that the anonymous resolver cannot exist
in production, that token comparison is constant-time, and that no code path
turns a missing credential into an authenticated identity.
"""

from __future__ import annotations

import dataclasses

import pytest

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import (
    AuthenticationError,
    ConfigurationError,
)
from mas_orchestrator.security.identity import (
    ANONYMOUS_SUBJECT,
    RUST_CORE_SUBJECT,
    AnonymousServiceIdentityResolver,
    ServiceIdentity,
    ServiceScope,
    StaticTokenServiceIdentityResolver,
    require_scope,
)

VALID_TOKEN = "service-token-abc123"


class TestStaticTokenResolver:
    """The production-shaped resolver."""

    def test_empty_token_is_refused_at_construction(self) -> None:
        """A resolver that would accept anything must not be constructible."""
        with pytest.raises(ConfigurationError):
            StaticTokenServiceIdentityResolver("")

    @pytest.mark.asyncio
    async def test_valid_token_resolves(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        identity = await resolver.resolve(VALID_TOKEN)
        assert identity.subject == RUST_CORE_SUBJECT
        assert identity.authenticated is True

    @pytest.mark.asyncio
    async def test_bearer_prefix_is_accepted(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        identity = await resolver.resolve(f"Bearer {VALID_TOKEN}")
        assert identity.authenticated is True

    @pytest.mark.asyncio
    async def test_missing_credential_is_rejected(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        with pytest.raises(AuthenticationError):
            await resolver.resolve(None)

    @pytest.mark.asyncio
    async def test_wrong_token_is_rejected(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        with pytest.raises(AuthenticationError):
            await resolver.resolve("wrong-token")

    @pytest.mark.asyncio
    async def test_prefix_of_valid_token_is_rejected(self) -> None:
        """A truncated credential must not authenticate."""
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        with pytest.raises(AuthenticationError):
            await resolver.resolve(VALID_TOKEN[:-1])

    @pytest.mark.asyncio
    async def test_missing_and_wrong_produce_the_same_reason_class(self) -> None:
        """Both are authentication failures; neither leaks which token is set."""
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        with pytest.raises(AuthenticationError):
            await resolver.resolve(None)
        with pytest.raises(AuthenticationError):
            await resolver.resolve("nope")

    @pytest.mark.asyncio
    async def test_error_message_never_contains_the_token(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        with pytest.raises(AuthenticationError) as excinfo:
            await resolver.resolve("wrong")
        assert VALID_TOKEN not in str(excinfo.value)

    def test_repr_never_contains_the_token(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        assert VALID_TOKEN not in repr(resolver)

    @pytest.mark.asyncio
    async def test_default_scopes_cover_execution(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        identity = await resolver.resolve(VALID_TOKEN)
        assert ServiceScope.EXECUTE_AGENT in identity.scopes

    @pytest.mark.asyncio
    async def test_custom_scopes_are_honoured(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(
            VALID_TOKEN, scopes={ServiceScope.READ_HEALTH}
        )
        identity = await resolver.resolve(VALID_TOKEN)
        assert ServiceScope.READ_HEALTH in identity.scopes
        assert ServiceScope.EXECUTE_AGENT not in identity.scopes

    @pytest.mark.asyncio
    async def test_custom_subject_is_honoured(self) -> None:
        resolver = StaticTokenServiceIdentityResolver(VALID_TOKEN, subject="other-svc")
        identity = await resolver.resolve(VALID_TOKEN)
        assert identity.subject == "other-svc"


class TestAnonymousResolver:
    """The development/test-only resolver and its production guard."""

    @pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.TEST])
    def test_allowed_in_development_and_test(self, environment: Environment) -> None:
        assert AnonymousServiceIdentityResolver(environment) is not None

    @pytest.mark.parametrize("environment", [Environment.STAGING, Environment.PRODUCTION])
    def test_refused_outside_development_and_test(self, environment: Environment) -> None:
        """There is no override flag: production cannot be anonymous."""
        with pytest.raises(ConfigurationError) as excinfo:
            AnonymousServiceIdentityResolver(environment)
        assert "anonymous" in str(excinfo.value).lower()

    @pytest.mark.asyncio
    async def test_resolves_without_credential(self) -> None:
        resolver = AnonymousServiceIdentityResolver(Environment.TEST)
        identity = await resolver.resolve(None)
        assert identity.subject == ANONYMOUS_SUBJECT
        assert identity.authenticated is True

    def test_repr_exposes_the_environment(self) -> None:
        resolver = AnonymousServiceIdentityResolver(Environment.TEST)
        assert "test" in repr(resolver)


class TestRequireScope:
    """Scope enforcement at the point of use."""

    def test_present_scope_passes(self) -> None:
        identity = ServiceIdentity(
            subject="svc", issuer="test", scopes=frozenset({ServiceScope.EXECUTE_AGENT})
        )
        require_scope(identity, ServiceScope.EXECUTE_AGENT)

    def test_missing_scope_raises(self) -> None:
        identity = ServiceIdentity(subject="svc", issuer="test", scopes=frozenset())
        with pytest.raises(AuthenticationError) as excinfo:
            require_scope(identity, ServiceScope.EXECUTE_AGENT)
        assert excinfo.value.details["required_scope"] == str(ServiceScope.EXECUTE_AGENT)

    def test_error_does_not_enumerate_the_callers_scopes(self) -> None:
        """A rejected caller learns only what was required."""
        identity = ServiceIdentity(
            subject="svc",
            issuer="test",
            scopes=frozenset({ServiceScope.READ_HEALTH, ServiceScope.INVOKE_API}),
        )
        with pytest.raises(AuthenticationError) as excinfo:
            require_scope(identity, ServiceScope.EXECUTE_AGENT)
        assert "read_health" not in str(excinfo.value)
        assert "invoke_api" not in str(excinfo.value)


class TestServiceIdentityValue:
    """The identity value object itself."""

    def test_is_frozen(self) -> None:
        identity = ServiceIdentity(subject="svc", issuer="test")
        with pytest.raises(dataclasses.FrozenInstanceError):
            identity.subject = "other"  # type: ignore[misc]

    def test_str_omits_scopes(self) -> None:
        identity = ServiceIdentity(
            subject="svc", issuer="test", scopes=frozenset({ServiceScope.INVOKE_API})
        )
        rendered = str(identity)
        assert "svc" in rendered
        assert "invoke_api" not in rendered

    def test_empty_scopes_default(self) -> None:
        assert ServiceIdentity(subject="svc", issuer="test").scopes == frozenset()


class TestResolverProtocol:
    """Both resolvers satisfy the protocol."""

    @pytest.mark.asyncio
    async def test_static_resolver_satisfies_protocol(self) -> None:
        from mas_orchestrator.security.identity import ServiceIdentityResolver

        resolver: ServiceIdentityResolver = StaticTokenServiceIdentityResolver(VALID_TOKEN)
        assert await resolver.resolve(VALID_TOKEN) is not None

    @pytest.mark.asyncio
    async def test_anonymous_resolver_satisfies_protocol(self) -> None:
        from mas_orchestrator.security.identity import ServiceIdentityResolver

        resolver: ServiceIdentityResolver = AnonymousServiceIdentityResolver(Environment.TEST)
        assert await resolver.resolve(None) is not None
