"""Concrete model provider adapters.

Each adapter implements :class:`~mas_orchestrator.models.base.ModelProvider`
against one vendor's wire protocol, and shares the guarded HTTP transport in
:mod:`mas_orchestrator.providers.base_http` so that SSRF protection, timeouts,
response bounds and header redaction hold identically everywhere.

Adapters are constructed only through
:mod:`mas_orchestrator.providers.factory`, which resolves credentials from
secret references and refuses to build a provider that is disabled or
misconfigured. Nothing here reads the environment or configuration directly.
"""

from __future__ import annotations

from mas_orchestrator.providers.anthropic import (
    ANTHROPIC_DEFAULT_HOST,
    ANTHROPIC_VERSION,
    AnthropicProvider,
)
from mas_orchestrator.providers.base_http import (
    BLOCKED_NETWORKS,
    DEFAULT_MAX_RESPONSE_BYTES,
    HttpPolicy,
    HttpTransport,
    TransportResponse,
    classify_status,
    is_blocked_address,
    parse_retry_after,
)
from mas_orchestrator.providers.compatible import (
    CompatibleProvider,
    CompatibleProviderConfig,
    build_compatible_provider,
)
from mas_orchestrator.providers.factory import (
    PROVIDER_IDS,
    ProviderBundle,
    ProviderSpec,
    build_providers,
)
from mas_orchestrator.providers.openai import OPENAI_DEFAULT_HOST, OpenAIProvider

__all__ = [
    "ANTHROPIC_DEFAULT_HOST",
    "ANTHROPIC_VERSION",
    "BLOCKED_NETWORKS",
    "DEFAULT_MAX_RESPONSE_BYTES",
    "OPENAI_DEFAULT_HOST",
    "PROVIDER_IDS",
    "AnthropicProvider",
    "CompatibleProvider",
    "CompatibleProviderConfig",
    "HttpPolicy",
    "HttpTransport",
    "OpenAIProvider",
    "ProviderBundle",
    "ProviderSpec",
    "TransportResponse",
    "build_compatible_provider",
    "build_providers",
    "classify_status",
    "is_blocked_address",
    "parse_retry_after",
]
