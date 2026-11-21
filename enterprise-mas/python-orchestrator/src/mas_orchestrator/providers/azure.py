"""Azure OpenAI provider adapter.

Azure OpenAI speaks the OpenAI Chat Completions wire format, but it is **not**
the same endpoint: authentication is an ``api-key`` header (not a bearer token),
the deployment is part of the URL path, and an ``api-version`` query parameter
is mandatory. This module reuses :class:`OpenAIProvider` for the wire encoding
and decoding — spec §4 forbids a second copy of ~400 lines of request shaping —
and adds exactly the three things the OpenAI adapter cannot know.

What this file must **not** do: accept an arbitrary host, accept a raw key from
configuration, silently drop ``api-version``, or fall back to a bearer header
that Azure would reject.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Final
from urllib.parse import urlparse

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    ModelNotAvailableError,
    ModelTimeoutError,
)
from mas_orchestrator.providers.base_http import (
    HttpPolicy,
    HttpTransport,
)
from mas_orchestrator.providers.openai import OpenAIProvider, _host_of

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas_orchestrator.models.types import ModelCapabilities, ModelRef
    from mas_orchestrator.security.credentials import Secret

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "AZURE_DEFAULT_API_VERSION",
    "AzureOpenAIProvider",
    "AzureProviderConfig",
    "build_azure_provider",
]

#: Default Azure OpenAI API version. overridable per deployment.
AZURE_DEFAULT_API_VERSION: Final[str] = "2024-06-01"


def _normalise_endpoint(endpoint: str) -> str:
    """Validates an Azure OpenAI endpoint.

    Raises:
        ConfigurationError: If the endpoint is not an https URL on an
            ``*.openai.azure.com`` host.
    """
    raw = endpoint.strip().rstrip("/")
    if not raw:
        raise ConfigurationError("azure endpoint is required")
    parsed = urlparse(raw)
    if parsed.scheme != "https":
        raise ConfigurationError(
            "azure endpoint must be https",
            details={"field": "endpoint"},
        )
    host = (parsed.hostname or "").lower()
    if host != "openai.azure.com" and not host.endswith(".openai.azure.com"):
        raise ConfigurationError(
            "azure endpoint host is not an Azure OpenAI host",
            details={"field": "endpoint", "host": host},
        )
    if parsed.query or parsed.fragment:
        raise ConfigurationError(
            "azure endpoint must not carry a query or fragment",
            details={"field": "endpoint"},
        )
    return raw


@dataclass(frozen=True, slots=True)
class AzureProviderConfig:
    """Configuration for one Azure OpenAI deployment endpoint.

    Attributes:
        endpoint: ``https://<resource>.openai.azure.com``.
        api_version: Azure OpenAI ``api-version`` query value.
        deployment: Default deployment name. When ``None``, the model name in
            the request is used as the deployment name.
        models: Declared model/deployment capabilities. Azure cannot enumerate
            deployments, so they must be declared rather than guessed.
    """

    endpoint: str
    api_version: str = AZURE_DEFAULT_API_VERSION
    deployment: str | None = None
    models: Mapping[str, ModelCapabilities] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "endpoint", _normalise_endpoint(self.endpoint))
        version = self.api_version.strip()
        if (
            not version
            or len(version) > 64
            or not all(ch.isalnum() or ch in ".-" for ch in version)
        ):
            raise ConfigurationError(
                "azure api_version must be a short alphanumeric token",
                details={"field": "api_version"},
            )
        object.__setattr__(self, "api_version", version)
        if self.deployment is not None:
            deployment = self.deployment.strip()
            if not deployment or len(deployment) > 200:
                raise ConfigurationError(
                    "azure deployment must be a short token",
                    details={"field": "deployment"},
                )
            object.__setattr__(self, "deployment", deployment)


class AzureOpenAIProvider(OpenAIProvider):
    """Adapter for Azure OpenAI, reusing the OpenAI wire codec.

    Args:
        api_key: Resolved API key.
        config: Validated Azure endpoint configuration.
        transport: Optional pre-built transport; one is built from defaults
            when omitted.
        connect_timeout_seconds: Connection budget.
        read_timeout_seconds: Per-read budget.
    """

    provider_id: ClassVar[str] = "azure"

    def __init__(
        self,
        api_key: Secret,
        config: AzureProviderConfig,
        *,
        transport: HttpTransport | None = None,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 60.0,
    ) -> None:
        self._azure_config = config
        host = _host_of(config.endpoint)
        if transport is not None:
            self._transport = transport
            self._owns_transport = False
        else:
            policy = HttpPolicy.for_providers(extra_hosts={host})
            self._transport = HttpTransport(
                policy,
                connect_timeout_seconds=connect_timeout_seconds,
                read_timeout_seconds=read_timeout_seconds,
            )
            self._owns_transport = True
        # `OpenAIProvider.__init__` would reject the Azure host, so the base
        # state is initialised directly and only its codec methods are reused.
        self._api_key = api_key
        self._base_url = f"{config.endpoint}/openai"
        self._organization = None
        self._capabilities = dict(config.models or {})

    def _headers(self) -> dict[str, str]:
        """Azure authenticates with ``api-key``, not a bearer token."""
        headers = super()._headers()
        headers["api-key"] = self._api_key.reveal()
        return headers

    def _deployment_for(self, payload: Mapping[str, Any]) -> str:
        deployment = self._azure_config.deployment or str(payload.get("model") or "")
        if not deployment:
            raise ConfigurationError(
                "azure deployment is required; set it in config or use the model name",
                details={"provider": self.provider_id},
            )
        return deployment

    async def _send(self, payload: dict[str, Any], timeout_seconds: float) -> Any:
        """Posts to the deployment-scoped Azure endpoint."""
        deployment = self._deployment_for(payload)
        url = (
            f"{self._base_url}/deployments/{deployment}/chat/completions"
            f"?api-version={self._azure_config.api_version}"
        )
        try:
            return await asyncio.wait_for(
                self._transport.request("POST", url, json_body=payload, headers=self._headers()),
                timeout=timeout_seconds,
            )
        except TimeoutError as error:
            raise ModelTimeoutError(
                "model call exceeded its deadline",
                details={
                    "model": payload.get("model"),
                    "timeout_seconds": timeout_seconds,
                },
            ) from error

    async def health(self) -> bool:
        """Returns whether the Azure OpenAI endpoint answers."""
        try:
            response = await self._transport.request(
                "GET",
                f"{self._base_url}?api-version={self._azure_config.api_version}",
                headers=self._headers(),
                max_response_bytes=4096,
            )
        except Exception:
            return False
        return response.status_code < 500

    def supports(self, model: ModelRef) -> bool:
        """Whether this provider serves *model*. Never raises."""
        return model.provider == self.provider_id and model.model in self._capabilities


def build_azure_provider(
    api_key: Secret,
    config: AzureProviderConfig,
    *,
    transport: HttpTransport | None = None,
    connect_timeout_seconds: float = 5.0,
    read_timeout_seconds: float = 60.0,
) -> AzureOpenAIProvider:
    """Builds an Azure OpenAI provider, refusing an unusable configuration."""
    if not config.models:
        raise ModelNotAvailableError(
            "azure provider requires at least one declared deployment model",
            details={"provider": "azure"},
        )
    return AzureOpenAIProvider(
        api_key,
        config,
        transport=transport,
        connect_timeout_seconds=connect_timeout_seconds,
        read_timeout_seconds=read_timeout_seconds,
    )
