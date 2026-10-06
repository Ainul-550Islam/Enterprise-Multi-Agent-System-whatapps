"""Provider adapter construction from validated configuration.

This is the composition root for the model layer. It is the only place that:

* resolves secret references into credentials;
* decides which adapter class serves which provider id;
* refuses to construct a provider whose configuration would be unsafe.

Two rules keep production safe:

**No secret literals.** Every credential arrives as a *reference*
(``env:NAME``, ``file:PATH``) and is resolved through
:mod:`mas_orchestrator.security.credentials`. A factory that accepted a raw key
string would make it easy to ship a key in config, so it does not.

**Disabled means disabled.** A provider whose configuration says it is disabled
is not constructed at all. There is no "construct anyway and fail at call time"
path, because a provider that exists but cannot work is a runtime landmine:
it would be selected by the router and then fail.

Unknown or misconfigured providers raise
:class:`~mas_orchestrator.contracts.errors.ConfigurationError` at startup rather
than degrading at call time.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import resolve_secret

if TYPE_CHECKING:
    from mas_orchestrator.config.enums import Environment
    from mas_orchestrator.models.registry import ModelRegistry
    from mas_orchestrator.models.types import ModelCapabilities
    from mas_orchestrator.providers.anthropic import AnthropicProvider
    from mas_orchestrator.providers.compatible import (
        CompatibleProvider,
        CompatibleProviderConfig,
    )
    from mas_orchestrator.providers.openai import OpenAIProvider

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "PROVIDER_IDS",
    "ProviderBundle",
    "ProviderSpec",
    "build_providers",
]

#: Provider ids this factory knows how to construct.
PROVIDER_IDS: Final[frozenset[str]] = frozenset({"openai", "anthropic", "compatible"})


@dataclass(frozen=True, slots=True)
class ProviderSpec:
    """Configuration for one provider instance.

    Attributes:
        provider_id: Which adapter to construct.
        enabled: Whether to construct it. A disabled provider is skipped
            entirely rather than built in a non-working state.
        api_key_secret_ref: Secret reference for the credential, e.g.
            ``env:MAS_OPENAI_API_KEY``. Required for ``openai`` and
            ``anthropic``.
        base_url: Override for the API base URL. Must remain on an allowlisted
            host; for arbitrary hosts use ``compatible``.
        organization: Optional OpenAI organization header.
        connect_timeout_seconds: Connection budget.
        read_timeout_seconds: Per-read budget.
        compatible: Endpoint configuration, required when ``provider_id`` is
            ``compatible``.
    """

    provider_id: str
    enabled: bool = True
    api_key_secret_ref: str | None = None
    base_url: str | None = None
    organization: str | None = None
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 60.0
    compatible: CompatibleProviderConfig | None = None

    def __post_init__(self) -> None:
        """Validates the specification.

        Raises:
            ConfigurationError: If the provider id is unknown, if timeouts are
                non-positive, if a credential reference is missing for a
                provider that needs one, or if ``compatible`` is set for a
                non-compatible provider (which would silently ignore it).
        """
        if not self.provider_id.strip():
            raise ConfigurationError(
                "provider specification requires a provider_id",
                details={"field": "provider_id"},
            )
        if self.provider_id not in PROVIDER_IDS:
            raise ConfigurationError(
                "unknown provider id",
                details={
                    "provider": self.provider_id,
                    "supported": sorted(PROVIDER_IDS),
                },
            )
        if self.connect_timeout_seconds <= 0 or self.read_timeout_seconds <= 0:
            raise ConfigurationError(
                "provider timeouts must be positive",
                details={
                    "provider": self.provider_id,
                    "connect": self.connect_timeout_seconds,
                    "read": self.read_timeout_seconds,
                },
            )
        if self.compatible is not None and self.provider_id != "compatible":
            raise ConfigurationError(
                "compatible configuration is only valid for the compatible provider",
                details={"provider": self.provider_id},
            )
        if self.provider_id == "compatible":
            return
        if not self.api_key_secret_ref:
            raise ConfigurationError(
                "provider requires a credential secret reference",
                details={"provider": self.provider_id},
            )


@dataclass(slots=True)
class ProviderBundle:
    """The set of providers built from configuration.

    Attributes:
        providers: Constructed providers, in configuration order.
        skipped: Ids that were configured but disabled, so startup logs can say
            so explicitly rather than leaving them silently absent.

    Provides :meth:`aclose` so the composition root can shut every adapter down
    through one call.
    """

    providers: list[object] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    async def aclose(self) -> None:
        """Closes every provider that owns a transport.

        Failures are logged and swallowed: shutdown must complete even if one
        adapter's connection pool is already broken.
        """
        for provider in self.providers:
            closer = getattr(provider, "aclose", None)
            if closer is None:
                continue
            try:
                await closer()
            except Exception:
                _LOGGER.warning(
                    "provider close failed during shutdown",
                    extra={"provider": getattr(provider, "provider_id", "unknown")},
                )


def build_providers(
    specs: list[ProviderSpec],
    *,
    environment: Environment,
    registry: ModelRegistry | None = None,
) -> ProviderBundle:
    """Constructs every enabled provider described by *specs*.

    Args:
        specs: Provider specifications, in configuration order.
        environment: Running environment; gates plaintext endpoints.
        registry: Optional registry to register providers into. When supplied,
            each provider is registered as it is built, and the caller does not
            need a second pass.

    Returns:
        The bundle of constructed providers.

    Raises:
        ConfigurationError: If any specification is invalid, or if a duplicate
            provider id is configured (which would make lookup order matter).
    """
    from mas_orchestrator.providers.anthropic import AnthropicProvider
    from mas_orchestrator.providers.compatible import CompatibleProvider
    from mas_orchestrator.providers.openai import OpenAIProvider

    bundle = ProviderBundle()
    seen: set[str] = set()

    for spec in specs:
        if spec.provider_id in seen:
            raise ConfigurationError(
                "duplicate provider specification",
                details={"provider": spec.provider_id},
            )
        seen.add(spec.provider_id)

        if not spec.enabled:
            bundle.skipped.append(spec.provider_id)
            _LOGGER.info(
                "provider is disabled; not constructed",
                extra={"provider": spec.provider_id},
            )
            continue

        provider: OpenAIProvider | AnthropicProvider | CompatibleProvider

        if spec.provider_id == "openai":
            assert spec.api_key_secret_ref is not None  # validated in __post_init__
            provider = OpenAIProvider(
                resolve_secret(spec.api_key_secret_ref),
                base_url=spec.base_url or "https://api.openai.com/v1",
                organization=spec.organization,
                connect_timeout_seconds=spec.connect_timeout_seconds,
                read_timeout_seconds=spec.read_timeout_seconds,
            )
        elif spec.provider_id == "anthropic":
            assert spec.api_key_secret_ref is not None  # validated in __post_init__
            provider = AnthropicProvider(
                resolve_secret(spec.api_key_secret_ref),
                base_url=spec.base_url or "https://api.anthropic.com/v1",
                connect_timeout_seconds=spec.connect_timeout_seconds,
                read_timeout_seconds=spec.read_timeout_seconds,
            )
        else:
            if spec.compatible is None:
                raise ConfigurationError(
                    "compatible provider requires an endpoint configuration",
                    details={"provider": spec.provider_id},
                )
            provider = CompatibleProvider(spec.compatible, environment=environment)

        bundle.providers.append(provider)
        if registry is not None:
            registry.register(provider)
            _register_known_models(registry, provider)

        _LOGGER.info("constructed provider", extra={"provider": spec.provider_id})

    return bundle


def _register_known_models(registry: ModelRegistry, provider: object) -> None:
    """Registers the models a provider advertises.

    Adapters expose their model table as ``_capabilities``; a provider that
    does not expose one is simply not enumerated, which means its models must
    be bound explicitly with
    :meth:`~mas_orchestrator.models.registry.ModelRegistry.register_model`.
    """
    table = getattr(provider, "_capabilities", None)
    provider_id = getattr(provider, "provider_id", None)
    if not isinstance(table, dict) or not isinstance(provider_id, str):
        return

    from mas_orchestrator.models.types import ModelRef

    for model_name in table:
        registry.register_model(ModelRef(provider=provider_id, model=model_name), provider_id)


def capabilities_from_declaration(
    *,
    supports_tools: bool = False,
    supports_streaming: bool = False,
    supports_structured_output: bool = False,
    supports_system_prompt: bool = True,
    max_input_tokens: int | None = None,
    max_output_tokens: int | None = None,
) -> ModelCapabilities:
    """Builds a :class:`ModelCapabilities` from configuration values.

    Used for compatible endpoints, whose capabilities cannot be discovered and
    must therefore be declared.

    Returns:
        The capability object.
    """
    from mas_orchestrator.models.types import ModelCapabilities

    return ModelCapabilities(
        supports_tools=supports_tools,
        supports_streaming=supports_streaming,
        supports_structured_output=supports_structured_output,
        supports_system_prompt=supports_system_prompt,
        max_input_tokens=max_input_tokens,
        max_output_tokens=max_output_tokens,
    )
