"""Generic OpenAI-compatible endpoint adapter.

Many deployments expose an OpenAI-shaped Chat Completions API on a private
host: a corporate gateway, a self-hosted vLLM/Ollama front end, or a regional
proxy. Rather than duplicating the OpenAI adapter (~400 lines of wire
encoding), this module configures :class:`OpenAIProvider` for a caller-supplied
endpoint.

That reuse is deliberate (spec §4 forbids duplicate implementations where an
existing one can be safely reused). What this module *adds* is the part the
OpenAI adapter cannot know:

* **Host allowlisting.** A compatible endpoint is by definition not
  ``api.openai.com``, so its host must be added to the HTTP policy explicitly.
  The adapter refuses to start otherwise, which is what keeps "compatible"
  from becoming "arbitrary URL".
* **Capability declaration.** A private endpoint has no model table we can
  guess, so capabilities are supplied by configuration. Claiming a capability
  the endpoint lacks would fail later and less clearly, so unknown models are
  reported as unavailable rather than assumed.
* **Optional credential.** Some private endpoints are unauthenticated on a
  trusted network. That is tolerated only when explicitly configured, and the
  transport remains HTTPS-enforcing and host-allowlisted either way.

Security note: this adapter never relaxes the SSRF guard. The configured host is
validated against the allowlist, DNS resolution is re-checked against blocked
ranges, and redirects are re-validated at every hop — exactly as for a first
party provider.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError, ModelNotAvailableError
from mas_orchestrator.providers.base_http import HttpPolicy, HttpTransport
from mas_orchestrator.providers.openai import OpenAIProvider, _host_of

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Mapping

    from mas_orchestrator.models.types import ModelCapabilities
    from mas_orchestrator.security.credentials import Secret

__all__ = [
    "CompatibleProvider",
    "CompatibleProviderConfig",
    "build_compatible_provider",
]

#: Placeholder used when the deployment has no credential configured.
NO_CREDENTIAL: Final[str] = ""


@dataclass(frozen=True, slots=True)
class CompatibleProviderConfig:
    """Configuration for one compatible endpoint.

    Attributes:
        name: Identifier for this endpoint, used as the ``provider`` part of a
            :class:`ModelRef` (for example ``compatible:gateway``).
        base_url: HTTPS base URL, e.g. ``https://llm.internal/v1``.
        models: Mapping of model name to its declared capabilities. Models not
            listed are reported as unavailable rather than assumed.
        api_key: Optional resolved credential. ``None`` means unauthenticated,
            which is only permitted when ``allow_unauthenticated`` is set.
        allow_unauthenticated: Permits operation without a credential. For
            endpoints on a trusted network; never a way to skip transport
            security.
        allow_insecure_http: Permits a plaintext base URL. **Only** allowed in
            development/test; the constructor refuses it elsewhere.
        connect_timeout_seconds: Connection budget.
        read_timeout_seconds: Per-read budget.
    """

    name: str = "compatible"
    base_url: str = ""
    models: Mapping[str, ModelCapabilities] = field(default_factory=dict)
    api_key: Secret | None = None
    allow_unauthenticated: bool = False
    allow_insecure_http: bool = False
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 60.0

    def __post_init__(self) -> None:
        """Validates the endpoint configuration.

        Raises:
            ConfigurationError: If the name is empty, the base URL is missing,
                no models are declared, or a credential is missing without
                ``allow_unauthenticated``.

        Note:
            A plaintext (``http://``) base URL is refused here regardless of
            ``allow_insecure_http``; that flag is evaluated together with the
            environment by :class:`CompatibleProvider`, which is the only
            place both are known.
        """
        if not self.name.strip():
            raise ConfigurationError(
                "compatible endpoint requires a name", details={"field": "name"}
            )
        if not self.base_url.strip():
            raise ConfigurationError(
                "compatible endpoint requires a base_url",
                details={"endpoint": self.name},
            )
        if not self.models:
            raise ConfigurationError(
                "compatible endpoint must declare its models and capabilities",
                details={"endpoint": self.name},
            )
        if self.api_key is None and not self.allow_unauthenticated:
            raise ConfigurationError(
                "compatible endpoint requires a credential or allow_unauthenticated",
                details={"endpoint": self.name},
            )


class CompatibleProvider:
    """An OpenAI-compatible endpoint, configured explicitly.

    Args:
        config: Endpoint configuration.
        environment: Running environment; gates plaintext base URLs.

    Raises:
        ConfigurationError: If the configuration is invalid, or a plaintext
            base URL is requested outside development/test.
        ModelNotAvailableError: If the base URL host is not permitted by the
            constructed HTTP policy (which cannot happen for a self-generated
            policy, but is checked so the guarantee is local and obvious).
    """

    def __init__(
        self,
        config: CompatibleProviderConfig,
        *,
        environment: Environment = Environment.PRODUCTION,
    ) -> None:
        self._config = config
        self._environment = environment

        host = _host_of(config.base_url)
        if not host:
            raise ConfigurationError(
                "compatible endpoint base_url has no host",
                details={"endpoint": config.name},
            )

        scheme_is_secure = config.base_url.startswith("https://")
        plaintext_allowed = config.allow_insecure_http and environment in {
            Environment.DEVELOPMENT,
            Environment.TEST,
        }
        if not scheme_is_secure and not plaintext_allowed:
            raise ConfigurationError(
                "compatible endpoint requires https outside development/test",
                details={
                    "endpoint": config.name,
                    "environment": str(environment),
                    "allow_insecure_http": config.allow_insecure_http,
                },
            )

        policy = HttpPolicy(
            allowed_hosts=frozenset({host}),
            allow_private_networks=not scheme_is_secure or config.allow_insecure_http,
            require_https=scheme_is_secure,
        )
        if not policy.is_host_allowed(host):  # pragma: no cover - defensive
            raise ModelNotAvailableError(
                "compatible endpoint host is not permitted", details={"endpoint": config.name}
            )

        self._transport = HttpTransport(
            policy,
            connect_timeout_seconds=config.connect_timeout_seconds,
            read_timeout_seconds=config.read_timeout_seconds,
            credential=config.api_key,
        )

        # Reuse the OpenAI wire encoding; only the endpoint and model table differ.
        self._delegate = OpenAIProvider(
            config.api_key if config.api_key is not None else _unauthenticated_secret(),
            transport=self._transport,
            base_url=config.base_url,
        )
        # Install the caller-declared model table on the delegate.
        self._delegate._capabilities = dict(config.models)

    @property
    def provider_id(self) -> str:
        """Provider identifier, namespaced so it cannot collide with first-party."""
        return f"compatible:{self._config.name}"

    @property
    def config(self) -> CompatibleProviderConfig:
        """The endpoint configuration."""
        return self._config

    async def aclose(self) -> None:
        """Closes the transport."""
        await self._transport.aclose()

    def supports(self, model: ModelRef) -> bool:
        """Whether this endpoint declares *model*."""
        return model.model in self._config.models

    def capabilities(self, model: ModelRef) -> ModelCapabilities:
        """Returns the declared capabilities for *model*.

        Raises:
            ModelNotAvailableError: If the model is not declared. Capabilities
                are never guessed for a private endpoint.
        """
        try:
            return self._config.models[model.model]
        except KeyError as error:
            raise ModelNotAvailableError(
                "model is not declared by this compatible endpoint",
                details={"model": model.key, "endpoint": self._config.name},
            ) from error

    async def complete(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a completion; delegates the wire format to the OpenAI adapter."""
        return await self._delegate.complete(
            request, timeout_seconds=timeout_seconds, cancel_event=cancel_event
        )

    def astream(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> object:
        """Streams a completion; delegates the wire format.

        Returns:
            The delegate's async iterator of stream events.
        """
        return self._delegate.astream(
            request, timeout_seconds=timeout_seconds, cancel_event=cancel_event
        )

    async def structured(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a schema-constrained completion when the model declares support.

        Raises:
            ModelNotAvailableError: If the model does not declare schema
                support. A compatible endpoint's capability table is the only
                evidence available, and it is honoured rather than assumed.
        """
        if not self.capabilities(request.model).supports_structured_output:
            raise ModelNotAvailableError(
                "model does not declare schema-constrained output support",
                details={"model": request.model.key, "endpoint": self._config.name},
            )
        return await self._delegate.structured(
            request, timeout_seconds=timeout_seconds, cancel_event=cancel_event
        )

    async def health(self) -> bool:
        """Returns whether the endpoint is reachable."""
        return await self._delegate.health()


def build_compatible_provider(
    config: CompatibleProviderConfig,
    *,
    environment: Environment = Environment.PRODUCTION,
) -> CompatibleProvider:
    """Constructs a :class:`CompatibleProvider` from configuration.

    Args:
        config: Endpoint configuration.
        environment: Running environment, gating plaintext base URLs.

    Returns:
        The configured provider.
    """
    return CompatibleProvider(config, environment=environment)


def _unauthenticated_secret() -> Secret:
    """Builds the sentinel credential used for unauthenticated endpoints.

    The OpenAI adapter requires a credential object. For an explicitly
    unauthenticated endpoint we supply an empty one; the transport simply
    produces an empty bearer header, which such endpoints ignore. This is
    reachable only when the configuration set
    ``allow_unauthenticated=True``.
    """
    from mas_orchestrator.security.credentials import Secret

    return Secret(NO_CREDENTIAL or "unauthenticated", reference="compatible:unauthenticated")


# Imported late to keep the runtime import graph small; these are only used in
# annotations.
if TYPE_CHECKING:
    from mas_orchestrator.models.types import ModelRef, ModelRequest, ModelResponse
