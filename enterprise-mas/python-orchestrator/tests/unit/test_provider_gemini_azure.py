"""Unit tests for the Gemini and Azure OpenAI provider adapters.

No network access: a recording stub transport is injected, so every test is
deterministic. These tests cover the vendor-specific surface only — the shared
HTTP guard, timeouts and error mapping are covered by
:mod:`tests.security.test_http_policy` and the retry suite.

What is verified here:

* Gemini refuses any base URL that is not Google's Generative Language host.
* Gemini encodes typed requests into the ``generateContent`` wire shape.
* Gemini decodes finish reasons, usage and tool calls — and reports ``usage``
  as ``None`` rather than fabricating zero when the API omits it.
* Azure refuses endpoints that are not ``*.openai.azure.com`` over https.
* Azure authenticates with ``api-key`` and targets the deployment-scoped
  endpoint with a mandatory ``api-version``.
* Azure reuses the OpenAI codec for request and response shaping.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ConfigurationError,
    InvalidRequestError,
    ModelNotAvailableError,
    ModelProviderError,
    ModelRateLimitedError,
)
from mas_orchestrator.models.types import (
    FinishReason,
    Message,
    ModelCapabilities,
    ModelRef,
    ModelRequest,
    Role,
    ToolCall,
    ToolSpec,
)
from mas_orchestrator.providers.azure import (
    AzureOpenAIProvider,
    AzureProviderConfig,
    build_azure_provider,
)
from mas_orchestrator.providers.base_http import TransportResponse
from mas_orchestrator.providers.factory import PROVIDER_IDS, ProviderSpec
from mas_orchestrator.providers.gemini import (
    GEMINI_DEFAULT_BASE_URL,
    GEMINI_DEFAULT_HOST,
    GeminiProvider,
)
from mas_orchestrator.security.credentials import Secret

KEY = Secret("test-key-not-real", reference="test")

GEMINI_MODEL = ModelRef("gemini", "gemini-2.0-flash")
AZURE_MODEL = ModelRef("azure", "gpt-4o")

AZURE_ENDPOINT = "https://mas-test.openai.azure.com"


class _RecordingTransport:
    """A transport that returns canned responses and records every request."""

    def __init__(self, response: TransportResponse) -> None:
        self.policy = None
        self.calls: list[dict[str, Any]] = []
        self._response = response

    async def request(
        self,
        method: str,
        url: str,
        *,
        json_body: object | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        max_response_bytes: int | None = None,
    ) -> TransportResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "json_body": json_body,
                "headers": dict(headers or {}),
            }
        )
        return self._response

    async def aclose(self) -> None:
        return None


def _response(status_code: int, document: object, **headers: str) -> TransportResponse:
    body = document if isinstance(document, bytes) else json.dumps(document).encode()
    return TransportResponse(status_code, body, dict(headers))


def _gemini(transport: _RecordingTransport | None = None) -> GeminiProvider:
    return GeminiProvider(KEY, transport=transport)  # type: ignore[arg-type]


def _azure(
    transport: _RecordingTransport | None = None,
    config: AzureProviderConfig | None = None,
) -> AzureOpenAIProvider:
    return AzureOpenAIProvider(
        KEY,
        config
        or AzureProviderConfig(endpoint=AZURE_ENDPOINT, models={"gpt-4o": ModelCapabilities()}),
        transport=transport,  # type: ignore[arg-type]
    )


def _request(model: ModelRef, **overrides: object) -> ModelRequest:
    base: dict[str, object] = {"messages": (Message.user("hello"),), "model": model}
    base.update(overrides)
    return ModelRequest(**base)  # type: ignore[arg-type]


# -- Gemini: model ownership ------------------------------------------------


class TestGeminiSupports:
    """Model ownership and capabilities."""

    def test_known_model(self) -> None:
        assert _gemini().supports(GEMINI_MODEL) is True

    def test_unknown_model(self) -> None:
        assert _gemini().supports(ModelRef("gemini", "nope")) is False

    def test_other_provider(self) -> None:
        assert _gemini().supports(ModelRef("openai", "gpt-4o")) is False

    def test_capabilities_for_known_model(self) -> None:
        assert _gemini().capabilities(GEMINI_MODEL).supports_tools is True

    def test_capabilities_for_unknown_model_raises(self) -> None:
        with pytest.raises(ModelNotAvailableError):
            _gemini().capabilities(ModelRef("gemini", "nope"))


# -- Gemini: host restriction ----------------------------------------------


class TestGeminiHostRestriction:
    """The adapter may talk to Google's host and nothing else."""

    def test_default_base_url_is_google(self) -> None:
        assert GEMINI_DEFAULT_HOST in GEMINI_DEFAULT_BASE_URL

    def test_google_host_is_accepted(self) -> None:
        provider = GeminiProvider(
            KEY,
            transport=_RecordingTransport(_response(200, {})),  # type: ignore[arg-type]
            base_url=f"https://{GEMINI_DEFAULT_HOST}/v1beta",
        )
        assert provider.provider_id == "gemini"

    def test_other_host_is_refused(self) -> None:
        with pytest.raises(ModelNotAvailableError):
            GeminiProvider(KEY, base_url="https://evil.example.com/v1beta")

    def test_azure_host_is_refused(self) -> None:
        with pytest.raises(ModelNotAvailableError):
            GeminiProvider(KEY, base_url=f"{AZURE_ENDPOINT}/openai")

    def test_http_scheme_is_refused(self) -> None:
        with pytest.raises(ModelNotAvailableError):
            GeminiProvider(KEY, base_url=f"http://{GEMINI_DEFAULT_HOST}/v1beta")


# -- Gemini: request encoding ----------------------------------------------


class TestGeminiEncoding:
    """``generateContent`` payload shape."""

    def test_contents_and_roles(self) -> None:
        request = ModelRequest(
            messages=(
                Message(role=Role.USER, content="hi"),
                Message(role=Role.ASSISTANT, content="hello"),
            ),
            model=GEMINI_MODEL,
        )
        payload = _gemini()._encode_request(request, stream=False)
        assert payload["contents"] == [
            {"role": "user", "parts": [{"text": "hi"}]},
            {"role": "model", "parts": [{"text": "hello"}]},
        ]

    def test_system_prompt_becomes_system_instruction(self) -> None:
        request = _request(GEMINI_MODEL, system_prompt="be terse")
        payload = _gemini()._encode_request(request, stream=False)
        assert payload["systemInstruction"] == {"parts": [{"text": "be terse"}]}
        assert all(item["role"] != "system" for item in payload["contents"])

    def test_generation_config_mapping(self) -> None:
        request = _request(
            GEMINI_MODEL,
            temperature=0.2,
            top_p=0.9,
            max_output_tokens=64,
            stop_sequences=("END",),
        )
        payload = _gemini()._encode_request(request, stream=False)
        assert payload["generationConfig"] == {
            "temperature": 0.2,
            "topP": 0.9,
            "maxOutputTokens": 64,
            "stopSequences": ["END"],
        }

    def test_response_schema_requests_json_mime(self) -> None:
        request = _request(GEMINI_MODEL, response_json_schema={"type": "object", "properties": {}})
        payload = _gemini()._encode_request(request, stream=False)
        config = payload["generationConfig"]
        assert config["responseMimeType"] == "application/json"
        assert config["responseSchema"] == {"type": "object", "properties": {}}

    def test_tools_become_function_declarations(self) -> None:
        tool = ToolSpec(
            name="lookup",
            description="look things up",
            json_schema={"type": "object", "properties": {"q": {"type": "string"}}},
        )
        request = _request(GEMINI_MODEL, tools=(tool,))
        payload = _gemini()._encode_request(request, stream=False)
        declarations = payload["tools"][0]["functionDeclarations"]
        assert declarations[0]["name"] == "lookup"
        assert declarations[0]["parameters"]["type"] == "object"
        assert payload["toolConfig"] == {"functionCallingConfig": {"mode": "AUTO"}}

    @pytest.mark.parametrize(
        ("choice", "expected"),
        [
            ("none", {"functionCallingConfig": {"mode": "NONE"}}),
            ("required", {"functionCallingConfig": {"mode": "ANY"}}),
            (
                "lookup",
                {
                    "functionCallingConfig": {
                        "mode": "ANY",
                        "allowedFunctionNames": ["lookup"],
                    }
                },
            ),
        ],
    )
    def test_tool_choice_modes(self, choice: str, expected: dict[str, Any]) -> None:
        tool = ToolSpec(
            name="lookup",
            description="look things up",
            json_schema={"type": "object", "properties": {}},
        )
        request = _request(GEMINI_MODEL, tools=(tool,), tool_choice=choice)
        payload = _gemini()._encode_request(request, stream=False)
        assert payload["toolConfig"] == expected

    def test_tool_result_message_becomes_function_response(self) -> None:
        request = ModelRequest(
            messages=(
                Message(role=Role.USER, content="find it"),
                Message(
                    role=Role.ASSISTANT,
                    content="",
                    tool_calls=(ToolCall(id="t1", name="lookup", arguments_json="{}"),),
                ),
                Message(role=Role.TOOL, content="found it", tool_call_id="t1", name="lookup"),
            ),
            model=GEMINI_MODEL,
        )
        payload = _gemini()._encode_request(request, stream=False)
        last = payload["contents"][-1]
        assert last["role"] == "user"
        assert last["parts"] == [
            {
                "functionResponse": {
                    "name": "lookup",
                    "response": {"output": "found it"},
                }
            }
        ]

    def test_endpoint_paths(self) -> None:
        provider = _gemini()
        assert provider._endpoint("gemini-2.0-flash", stream=False).endswith(
            "/models/gemini-2.0-flash:generateContent"
        )
        assert provider._endpoint("gemini-2.0-flash", stream=True).endswith(
            "/models/gemini-2.0-flash:streamGenerateContent?alt=sse"
        )


# -- Gemini: response decoding ---------------------------------------------


class TestGeminiDecoding:
    """Finish reasons, usage discipline and tool calls."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("STOP", FinishReason.STOP),
            ("MAX_TOKENS", FinishReason.LENGTH),
            ("SAFETY", FinishReason.CONTENT_FILTER),
            ("RECITATION", FinishReason.CONTENT_FILTER),
            ("WEIRD_NEW_REASON", FinishReason.OTHER),
        ],
    )
    def test_finish_reason_mapping(self, raw: str, expected: FinishReason) -> None:
        assert GeminiProvider._decode_finish(raw) is expected

    def test_usage_absent_is_none_not_zero(self) -> None:
        assert GeminiProvider._decode_usage(None) is None
        assert GeminiProvider._decode_usage({}) is None
        assert GeminiProvider._decode_usage({"promptTokenCount": 1}) is None

    def test_usage_present_is_reported(self) -> None:
        usage = GeminiProvider._decode_usage(
            {"promptTokenCount": 3, "candidatesTokenCount": 5, "totalTokenCount": 8}
        )
        assert usage is not None
        assert (usage.input_tokens, usage.output_tokens, usage.total_tokens) == (3, 5, 8)

    def test_candidate_decodes_text_and_tool_calls(self) -> None:
        candidate = {
            "content": {
                "parts": [
                    {"text": "let me check"},
                    {"functionCall": {"name": "lookup", "args": {"q": "x"}}},
                ]
            },
            "finishReason": "STOP",
        }
        text, tool_calls, finish, raw = _gemini()._decode_candidate(candidate)
        assert text == "let me check"
        # The raw Gemini reason is preserved; `complete()` promotes a response
        # carrying tool calls to FinishReason.TOOL_CALLS.
        assert finish is FinishReason.STOP
        assert raw == "STOP"
        assert len(tool_calls) == 1
        assert tool_calls[0].name == "lookup"
        assert json.loads(tool_calls[0].arguments_json) == {"q": "x"}

    async def test_complete_promotes_tool_calls_finish(self) -> None:
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
        response = await _gemini(transport).complete(_request(GEMINI_MODEL), timeout_seconds=5.0)
        assert response.finish_reason is FinishReason.TOOL_CALLS
        assert response.requires_tool_execution is True


class TestGeminiComplete:
    """End-to-end completion against a canned response."""

    async def test_complete_decodes_response(self) -> None:
        transport = _RecordingTransport(
            _response(
                200,
                {
                    "candidates": [
                        {
                            "content": {"parts": [{"text": "answer"}]},
                            "finishReason": "STOP",
                        }
                    ],
                    "usageMetadata": {
                        "promptTokenCount": 2,
                        "candidatesTokenCount": 1,
                        "totalTokenCount": 3,
                    },
                },
            )
        )
        provider = _gemini(transport)
        response = await provider.complete(_request(GEMINI_MODEL), timeout_seconds=5.0)
        assert response.text == "answer"
        assert response.finish_reason is FinishReason.STOP
        assert response.usage is not None and response.usage.total_tokens == 3
        call = transport.calls[0]
        assert call["method"] == "POST"
        assert ":generateContent" in call["url"]
        assert call["headers"]["x-goog-api-key"] == "test-key-not-real"

    async def test_complete_reports_no_usage_when_absent(self) -> None:
        transport = _RecordingTransport(
            _response(
                200,
                {
                    "candidates": [
                        {
                            "content": {"parts": [{"text": "answer"}]},
                            "finishReason": "STOP",
                        }
                    ]
                },
            )
        )
        response = await _gemini(transport).complete(_request(GEMINI_MODEL), timeout_seconds=5.0)
        assert response.usage is None

    async def test_empty_candidates_is_retryable_provider_error(self) -> None:
        transport = _RecordingTransport(_response(200, {"candidates": []}))
        with pytest.raises(ModelProviderError):
            await _gemini(transport).complete(_request(GEMINI_MODEL), timeout_seconds=5.0)

    async def test_malformed_body_is_provider_error(self) -> None:
        transport = _RecordingTransport(_response(200, b"not-json"))
        with pytest.raises(ModelProviderError):
            await _gemini(transport).complete(_request(GEMINI_MODEL), timeout_seconds=5.0)

    async def test_rate_limit_maps_with_retry_after(self) -> None:
        transport = _RecordingTransport(
            _response(429, {"error": {"message": "slow down"}}, **{"retry-after": "7"})
        )
        with pytest.raises(ModelRateLimitedError) as raised:
            await _gemini(transport).complete(_request(GEMINI_MODEL), timeout_seconds=5.0)
        assert raised.value.retry_after_seconds == 7.0

    async def test_server_error_is_retryable(self) -> None:
        transport = _RecordingTransport(_response(503, {"error": {}}))
        with pytest.raises(ModelProviderError) as raised:
            await _gemini(transport).complete(_request(GEMINI_MODEL), timeout_seconds=5.0)
        assert raised.value.retryable is True

    async def test_not_found_maps_to_not_available(self) -> None:
        transport = _RecordingTransport(_response(404, {"error": {}}))
        with pytest.raises(ModelNotAvailableError):
            await _gemini(transport).complete(_request(GEMINI_MODEL), timeout_seconds=5.0)

    async def test_cancel_event_short_circuits(self) -> None:
        transport = _RecordingTransport(_response(200, {}))
        cancel = asyncio.Event()
        cancel.set()
        with pytest.raises(CancellationRequestedError):
            await _gemini(transport).complete(
                _request(GEMINI_MODEL), timeout_seconds=5.0, cancel_event=cancel
            )
        assert transport.calls == []

    async def test_structured_requires_schema(self) -> None:
        transport = _RecordingTransport(_response(200, {}))
        with pytest.raises(InvalidRequestError):
            await _gemini(transport).structured(_request(GEMINI_MODEL), timeout_seconds=5.0)

    async def test_stream_yields_text_then_terminal(self) -> None:
        body = (
            b'data: {"candidates":[{"content":{"parts":[{"text":"he"}]}}]}\n\n'
            b'data: {"candidates":[{"content":{"parts":[{"text":"llo"}]},'
            b'"finishReason":"STOP"}]}\n\n'
        )
        transport = _RecordingTransport(_response(200, body))
        events = []
        async for event in _gemini(transport).astream(_request(GEMINI_MODEL), timeout_seconds=5.0):
            events.append(event)
        assert [event.delta_text for event in events if event.delta_text] == ["he", "llo"]
        terminal = events[-1]
        assert terminal.is_terminal is True
        assert terminal.finish_reason is FinishReason.STOP
        assert ":streamGenerateContent" in transport.calls[0]["url"]


# -- Azure: configuration ---------------------------------------------------


class TestAzureConfig:
    """Endpoint and api-version validation."""

    def test_valid_endpoint_is_normalised(self) -> None:
        config = AzureProviderConfig(endpoint=f"{AZURE_ENDPOINT}/")
        assert config.endpoint == AZURE_ENDPOINT

    def test_http_endpoint_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            AzureProviderConfig(endpoint="http://mas-test.openai.azure.com")

    def test_non_azure_host_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            AzureProviderConfig(endpoint="https://evil.example.com")

    def test_lookalike_host_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            AzureProviderConfig(endpoint="https://openai.azure.com.evil.example.com")

    def test_query_string_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            AzureProviderConfig(endpoint=f"{AZURE_ENDPOINT}?api-version=1")

    @pytest.mark.parametrize("version", ["", "  ", "a b", "x" * 65, "1;drop"])
    def test_bad_api_version_is_refused(self, version: str) -> None:
        with pytest.raises(ConfigurationError):
            AzureProviderConfig(endpoint=AZURE_ENDPOINT, api_version=version)

    def test_default_api_version(self) -> None:
        assert AzureProviderConfig(endpoint=AZURE_ENDPOINT).api_version == "2024-06-01"


class TestAzureProviderSpec:
    """``ProviderSpec`` cross-field rules for azure."""

    def test_azure_config_rejected_for_other_providers(self) -> None:
        with pytest.raises(ConfigurationError):
            ProviderSpec(
                provider_id="openai",
                enabled=True,
                api_key_secret_ref="env:MAS_OPENAI_API_KEY",
                azure=AzureProviderConfig(endpoint=AZURE_ENDPOINT),
            )

    def test_azure_provider_requires_config(self) -> None:
        with pytest.raises(ConfigurationError):
            ProviderSpec(
                provider_id="azure",
                enabled=True,
                api_key_secret_ref="env:MAS_AZURE_OPENAI_API_KEY",
            )

    def test_gemini_and_azure_are_known_provider_ids(self) -> None:
        assert {"gemini", "azure"} <= PROVIDER_IDS


# -- Azure: wire behaviour --------------------------------------------------


class TestAzureWire:
    """Headers, endpoint shape and OpenAI-codec reuse."""

    def test_api_key_header_not_bearer(self) -> None:
        headers = _azure()._headers()
        assert headers["api-key"] == "test-key-not-real"

    async def test_send_targets_deployment_endpoint_with_api_version(self) -> None:
        config = AzureProviderConfig(
            endpoint=AZURE_ENDPOINT,
            api_version="2024-06-01",
            deployment="gpt4o-prod",
            models={"gpt-4o": ModelCapabilities()},
        )
        transport = _RecordingTransport(
            _response(
                200,
                {
                    "choices": [
                        {
                            "message": {"role": "assistant", "content": "hi"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2,
                    },
                },
            )
        )
        provider = _azure(transport, config)
        response = await provider.complete(_request(AZURE_MODEL), timeout_seconds=5.0)
        call = transport.calls[0]
        assert call["url"] == (
            f"{AZURE_ENDPOINT}/openai/deployments/gpt4o-prod/chat/completions"
            "?api-version=2024-06-01"
        )
        assert call["headers"]["api-key"] == "test-key-not-real"
        assert response.text == "hi"
        assert response.finish_reason is FinishReason.STOP
        assert response.usage is not None and response.usage.total_tokens == 2

    async def test_deployment_falls_back_to_model_name(self) -> None:
        transport = _RecordingTransport(
            _response(
                200,
                {
                    "choices": [
                        {
                            "message": {"role": "assistant", "content": "hi"},
                            "finish_reason": "stop",
                        }
                    ]
                },
            )
        )
        provider = _azure(transport)
        await provider.complete(_request(AZURE_MODEL), timeout_seconds=5.0)
        assert "/deployments/gpt-4o/chat/completions" in transport.calls[0]["url"]

    async def test_rate_limit_maps_with_retry_after(self) -> None:
        transport = _RecordingTransport(_response(429, {"error": {}}, **{"retry-after": "3"}))
        with pytest.raises(ModelRateLimitedError) as raised:
            await _azure(transport).complete(_request(AZURE_MODEL), timeout_seconds=5.0)
        assert raised.value.retry_after_seconds == 3.0

    def test_declared_models_drive_supports(self) -> None:
        config = AzureProviderConfig(
            endpoint=AZURE_ENDPOINT,
            models={"gpt-4o": ModelCapabilities()},
        )
        provider = _azure(config=config)
        assert provider.supports(AZURE_MODEL) is True
        assert provider.supports(ModelRef("azure", "unknown-deployment")) is False

    def test_build_requires_declared_models(self) -> None:
        with pytest.raises(ModelNotAvailableError):
            build_azure_provider(KEY, AzureProviderConfig(endpoint=AZURE_ENDPOINT))

    def test_build_with_models_succeeds(self) -> None:
        provider = build_azure_provider(
            KEY,
            AzureProviderConfig(endpoint=AZURE_ENDPOINT, models={"gpt-4o": ModelCapabilities()}),
        )
        assert provider.provider_id == "azure"
