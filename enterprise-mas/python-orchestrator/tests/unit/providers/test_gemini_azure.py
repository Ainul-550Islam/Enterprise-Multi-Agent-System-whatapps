"""Gemini generate/stream/tool-call mapping and Azure deployment URL/auth."""

from __future__ import annotations

import json
from typing import Any

import pytest

from mas_orchestrator.config.enums import ProviderKind, classify_provider_kind
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.models.types import (
    FinishReason,
    Message,
    ModelCapabilities,
    ModelRef,
    ModelRequest,
)
from mas_orchestrator.providers.azure import AzureOpenAIProvider, AzureProviderConfig
from mas_orchestrator.providers.base_http import TransportResponse
from mas_orchestrator.providers.factory import COMPATIBLE_PRESET_IDS, PROVIDER_IDS, ProviderSpec
from mas_orchestrator.providers.gemini import GeminiProvider
from mas_orchestrator.security.credentials import Secret

KEY = Secret("test-key-not-real", reference="test")
GEMINI_MODEL = ModelRef("gemini", "gemini-2.0-flash")
AZURE_ENDPOINT = "https://mas-test.openai.azure.com"


class _RecordingTransport:
    def __init__(self, *responses: TransportResponse) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def request(self, method: str, url: str, **kwargs: Any) -> TransportResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return self._responses.pop(0)


def _response(status_code: int, document: object, **headers: str) -> TransportResponse:
    body = document if isinstance(document, bytes) else json.dumps(document).encode()
    return TransportResponse(status_code, body, dict(headers))


def test_gemini_and_azure_openai_kinds_are_first_class() -> None:
    assert classify_provider_kind("gemini") is ProviderKind.GEMINI
    assert classify_provider_kind("azure_openai") is ProviderKind.AZURE
    assert classify_provider_kind("azure-prod") is ProviderKind.AZURE
    assert {"gemini", "azure"} <= PROVIDER_IDS


def test_compatible_presets_are_registered() -> None:
    for preset in ("ollama", "vllm", "groq", "openrouter"):
        assert preset in COMPATIBLE_PRESET_IDS
        assert classify_provider_kind(preset) is ProviderKind.COMPATIBLE
        ProviderSpec(provider_id=preset, enabled=False)


async def test_gemini_generate_maps_text_and_usage() -> None:
    transport = _RecordingTransport(
        _response(
            200,
            {
                "candidates": [
                    {"content": {"parts": [{"text": "answer"}]}, "finishReason": "STOP"}
                ],
                "usageMetadata": {
                    "promptTokenCount": 2,
                    "candidatesTokenCount": 1,
                    "totalTokenCount": 3,
                },
            },
        )
    )
    provider = GeminiProvider(KEY, transport=transport)  # type: ignore[arg-type]
    response = await provider.complete(
        ModelRequest(messages=(Message.user("hello"),), model=GEMINI_MODEL),
        timeout_seconds=5.0,
    )
    assert response.text == "answer"
    assert response.finish_reason is FinishReason.STOP
    assert response.usage is not None and response.usage.total_tokens == 3
    assert transport.calls[0]["headers"]["x-goog-api-key"] == "test-key-not-real"
    assert ":generateContent" in transport.calls[0]["url"]


async def test_gemini_tool_call_mapping() -> None:
    transport = _RecordingTransport(
        _response(
            200,
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [{"functionCall": {"name": "lookup", "args": {"q": "x"}}}]
                        },
                        "finishReason": "STOP",
                    }
                ]
            },
        )
    )
    provider = GeminiProvider(KEY, transport=transport)  # type: ignore[arg-type]
    response = await provider.complete(
        ModelRequest(messages=(Message.user("hello"),), model=GEMINI_MODEL),
        timeout_seconds=5.0,
    )
    assert response.finish_reason is FinishReason.TOOL_CALLS
    assert response.tool_calls[0].name == "lookup"


async def test_gemini_stream_emits_text_deltas() -> None:
    transport = _RecordingTransport(
        _response(
            200,
            b'data: {"candidates":[{"content":{"parts":[{"text":"hel"}]}}]}\n\n'
            b'data: {"candidates":[{"content":{"parts":[{"text":"lo"}]},'
            b'"finishReason":"STOP"}]}\n\n',
        )
    )
    provider = GeminiProvider(KEY, transport=transport)  # type: ignore[arg-type]
    chunks = [
        event
        async for event in provider.astream(
            ModelRequest(messages=(Message.user("hello"),), model=GEMINI_MODEL),
            timeout_seconds=5.0,
        )
    ]
    texts = [event.delta_text for event in chunks if event.delta_text]
    assert texts


def test_azure_requires_deployment_host_https_and_api_version() -> None:
    with pytest.raises(ConfigurationError, match="https"):
        AzureProviderConfig(endpoint="http://mas-test.openai.azure.com")
    with pytest.raises(ConfigurationError, match="Azure OpenAI host"):
        AzureProviderConfig(endpoint="https://example.com")
    config = AzureProviderConfig(
        endpoint=AZURE_ENDPOINT,
        api_version="2024-06-01",
        models={"gpt-4o": ModelCapabilities()},
    )
    provider = AzureOpenAIProvider(KEY, config)
    assert "openai.azure.com" in provider._azure_config.endpoint
    assert provider._azure_config.api_version == "2024-06-01"


async def test_azure_sends_api_key_header_and_deployment_path() -> None:
    transport = _RecordingTransport(
        _response(
            200,
            {
                "choices": [
                    {"message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
                ]
            },
        )
    )
    config = AzureProviderConfig(
        endpoint=AZURE_ENDPOINT,
        deployment="gpt-4o",
        models={"gpt-4o": ModelCapabilities()},
    )
    provider = AzureOpenAIProvider(KEY, config, transport=transport)  # type: ignore[arg-type]
    response = await provider.complete(
        ModelRequest(messages=(Message.user("hello"),), model=ModelRef("azure", "gpt-4o")),
        timeout_seconds=5.0,
    )
    assert response.text == "ok"
    call = transport.calls[0]
    assert "deployments/gpt-4o/chat/completions" in call["url"]
    assert "api-version=" in call["url"]
    assert call["headers"].get("api-key") == "test-key-not-real"
