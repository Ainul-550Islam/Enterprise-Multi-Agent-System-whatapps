"""Unit tests for provider wire encoding, without any network access.

These tests verify that each adapter translates the typed
:class:`~mas_orchestrator.models.types.ModelRequest` into the payload its
vendor actually expects, and that it refuses requests the vendor cannot serve.

No I/O occurs: a stub transport is injected, so the tests are fast and
deterministic. The HTTP guard, timeouts and error mapping are covered in
:mod:`tests.security.test_http_policy` and the retry suite.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError, ModelNotAvailableError
from mas_orchestrator.models.types import (
    Message,
    ModelRef,
    ModelRequest,
    ToolSpec,
)
from mas_orchestrator.providers.anthropic import AnthropicProvider
from mas_orchestrator.providers.openai import OpenAIProvider
from mas_orchestrator.security.credentials import Secret

KEY = Secret("test-key-not-real", reference="test")

OPENAI_MODEL = ModelRef("openai", "gpt-4o")
ANTHROPIC_MODEL = ModelRef("anthropic", "claude-sonnet-4-20250514")


class _StubTransport:
    """A transport that records nothing and never performs I/O."""

    def __init__(self) -> None:
        self.policy = None


def _openai() -> OpenAIProvider:
    return OpenAIProvider(KEY, transport=_StubTransport())  # type: ignore[arg-type]


def _anthropic() -> AnthropicProvider:
    return AnthropicProvider(KEY, transport=_StubTransport())  # type: ignore[arg-type]


def _request(model: ModelRef, **overrides: object) -> ModelRequest:
    base: dict[str, object] = {"messages": (Message.user("hello"),), "model": model}
    base.update(overrides)
    return ModelRequest(**base)  # type: ignore[arg-type]


class TestOpenAISupports:
    """Model ownership."""

    def test_known_model(self) -> None:
        assert _openai().supports(OPENAI_MODEL) is True

    def test_unknown_model(self) -> None:
        assert _openai().supports(ModelRef("openai", "nope")) is False

    def test_other_provider(self) -> None:
        assert _openai().supports(ANTHROPIC_MODEL) is False

    def test_capabilities_for_known_model(self) -> None:
        assert _openai().capabilities(OPENAI_MODEL).supports_tools is True

    def test_capabilities_for_unknown_model_raises(self) -> None:
        with pytest.raises(ModelNotAvailableError):
            _openai().capabilities(ModelRef("openai", "nope"))


class TestOpenAIEncoding:
    """Chat Completions payload shape."""

    def test_model_and_messages(self) -> None:
        payload = _openai()._encode_request(_request(OPENAI_MODEL), stream=False)
        assert payload["model"] == "gpt-4o"
        assert payload["messages"][0] == {"role": "user", "content": "hello"}

    def test_system_prompt_is_a_message(self) -> None:
        payload = _openai()._encode_request(
            _request(OPENAI_MODEL, system_prompt="be brief"), stream=False
        )
        assert payload["messages"][0] == {"role": "system", "content": "be brief"}

    def test_sampling_parameters(self) -> None:
        payload = _openai()._encode_request(
            _request(OPENAI_MODEL, temperature=0.3, top_p=0.9), stream=False
        )
        assert payload["temperature"] == 0.3
        assert payload["top_p"] == 0.9

    def test_absent_sampling_parameters_are_omitted(self) -> None:
        """Omitting them means 'provider default', not zero."""
        payload = _openai()._encode_request(_request(OPENAI_MODEL), stream=False)
        assert "temperature" not in payload
        assert "top_p" not in payload

    def test_max_tokens_is_set(self) -> None:
        payload = _openai()._encode_request(
            _request(OPENAI_MODEL, max_output_tokens=256), stream=False
        )
        assert payload["max_tokens"] == 256

    def test_stop_sequences(self) -> None:
        payload = _openai()._encode_request(
            _request(OPENAI_MODEL, stop_sequences=("END",)), stream=False
        )
        assert payload["stop"] == ["END"]

    def test_tools_are_encoded_as_functions(self) -> None:
        tool = ToolSpec(name="search", description="Search the web", json_schema={"type": "object"})
        payload = _openai()._encode_request(_request(OPENAI_MODEL, tools=(tool,)), stream=False)
        assert payload["tools"][0]["type"] == "function"
        assert payload["tools"][0]["function"]["name"] == "search"

    def test_tool_choice_keyword(self) -> None:
        payload = _openai()._encode_request(
            _request(OPENAI_MODEL, tools=(ToolSpec("s", "d", {}),), tool_choice="auto"),
            stream=False,
        )
        assert payload["tool_choice"] == "auto"

    def test_tool_choice_specific_tool(self) -> None:
        payload = _openai()._encode_request(
            _request(OPENAI_MODEL, tools=(ToolSpec("s", "d", {}),), tool_choice="search"),
            stream=False,
        )
        assert payload["tool_choice"] == {"type": "function", "function": {"name": "search"}}

    def test_structured_output_schema(self) -> None:
        payload = _openai()._encode_request(
            _request(OPENAI_MODEL, response_json_schema={"type": "object"}), stream=False
        )
        assert payload["response_format"]["type"] == "json_schema"
        assert payload["response_format"]["json_schema"]["schema"] == {"type": "object"}

    def test_stream_requests_usage(self) -> None:
        """Without this option OpenAI does not report usage on a stream."""
        payload = _openai()._encode_request(_request(OPENAI_MODEL), stream=True)
        assert payload["stream"] is True
        assert payload["stream_options"] == {"include_usage": True}

    def test_assistant_tool_calls_are_encoded(self) -> None:
        request = _request(
            OPENAI_MODEL,
            messages=(
                Message.user("hi"),
                Message.assistant(
                    tool_calls=(
                        Message.assistant("x").tool_calls[0]
                        if False
                        else __import__(
                            "mas_orchestrator.models.types", fromlist=["ToolCall"]
                        ).ToolCall(id="c1", name="search", arguments_json='{"q":"x"}'),
                    )
                ),
            ),
        )
        payload = _openai()._encode_request(request, stream=False)
        tool_calls = payload["messages"][1]["tool_calls"]
        assert tool_calls[0]["id"] == "c1"
        assert tool_calls[0]["function"]["name"] == "search"
        assert tool_calls[0]["function"]["arguments"] == '{"q":"x"}'

    def test_tool_result_message(self) -> None:
        request = _request(
            OPENAI_MODEL,
            messages=(
                Message.user("hi"),
                Message.tool_result("c1", "search", '{"ok":true}'),
            ),
        )
        payload = _openai()._encode_request(request, stream=False)
        encoded = payload["messages"][1]
        assert encoded["role"] == "tool"
        assert encoded["tool_call_id"] == "c1"

    def test_model_without_system_prompt_support_is_refused(self) -> None:
        """Folding it into a user message would change its authority."""
        provider = _openai()
        no_system = ModelRef("openai", "o3-mini")
        with pytest.raises(InvalidRequestError):
            provider._encode_request(_request(no_system, system_prompt="be brief"), stream=False)


class TestOpenAIUsageDecoding:
    """Usage extraction."""

    def test_reported_usage(self) -> None:
        usage = OpenAIProvider._decode_usage(
            {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
        )
        assert usage is not None
        assert usage.input_tokens == 10
        assert usage.output_tokens == 5
        assert usage.total == 15

    def test_cached_tokens(self) -> None:
        usage = OpenAIProvider._decode_usage(
            {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "prompt_tokens_details": {"cached_tokens": 4},
            }
        )
        assert usage is not None
        assert usage.cached_input_tokens == 4

    def test_absent_usage_is_none(self) -> None:
        """Never fabricate a zero measurement."""
        assert OpenAIProvider._decode_usage(None) is None

    def test_malformed_usage_is_none(self) -> None:
        assert OpenAIProvider._decode_usage({"prompt_tokens": "x"}) is None


class TestAnthropicSupports:
    """Model ownership."""

    def test_known_model(self) -> None:
        assert _anthropic().supports(ANTHROPIC_MODEL) is True

    def test_unknown_model(self) -> None:
        assert _anthropic().supports(ModelRef("anthropic", "nope")) is False


class TestAnthropicEncoding:
    """Messages API payload shape — differs from OpenAI in three ways."""

    def test_system_is_top_level(self) -> None:
        payload = _anthropic()._encode_request(
            _request(ANTHROPIC_MODEL, system_prompt="be brief"), stream=False
        )
        assert payload["system"] == "be brief"
        # ...and must NOT appear as a message.
        assert all(m["role"] != "system" for m in payload["messages"])

    def test_messages_use_content_blocks(self) -> None:
        payload = _anthropic()._encode_request(_request(ANTHROPIC_MODEL), stream=False)
        assert payload["messages"][0]["content"][0] == {"type": "text", "text": "hello"}

    def test_max_tokens_is_required(self) -> None:
        """Anthropic rejects a request without max_tokens."""
        payload = _anthropic()._encode_request(_request(ANTHROPIC_MODEL), stream=False)
        assert payload["max_tokens"] > 0

    def test_tools_use_input_schema(self) -> None:
        tool = ToolSpec(name="search", description="d", json_schema={"type": "object"})
        payload = _anthropic()._encode_request(
            _request(ANTHROPIC_MODEL, tools=(tool,)), stream=False
        )
        assert payload["tools"][0]["name"] == "search"
        assert payload["tools"][0]["input_schema"] == {"type": "object"}
        # OpenAI's nested "function" wrapper must NOT be present.
        assert "function" not in payload["tools"][0]

    def test_tool_choice_mapping(self) -> None:
        tool = ToolSpec(name="search", description="d", json_schema={})
        payload = _anthropic()._encode_request(
            _request(ANTHROPIC_MODEL, tools=(tool,), tool_choice="auto"), stream=False
        )
        assert payload["tool_choice"] == {"type": "auto"}

    def test_tool_choice_required_maps_to_any(self) -> None:
        tool = ToolSpec(name="search", description="d", json_schema={})
        payload = _anthropic()._encode_request(
            _request(ANTHROPIC_MODEL, tools=(tool,), tool_choice="required"), stream=False
        )
        assert payload["tool_choice"] == {"type": "any"}

    def test_tool_result_becomes_a_user_message(self) -> None:
        request = _request(
            ANTHROPIC_MODEL,
            messages=(
                Message.user("hi"),
                Message.tool_result("c1", "search", '{"ok":true}'),
            ),
        )
        payload = _anthropic()._encode_request(request, stream=False)
        result_message = payload["messages"][-1]
        assert result_message["role"] == "user"

        # Strict alternation means the preceding user text and the tool result
        # share one turn: text block first, then the tool_result block.
        blocks = result_message["content"]
        result_block = next(b for b in blocks if b["type"] == "tool_result")
        assert result_block["tool_use_id"] == "c1"
        assert blocks[0]["type"] == "text"

    def test_consecutive_same_role_messages_are_merged(self) -> None:
        """Anthropic requires strict alternation."""
        request = _request(
            ANTHROPIC_MODEL,
            messages=(Message.user("one"), Message.user("two")),
        )
        payload = _anthropic()._encode_request(request, stream=False)
        assert len(payload["messages"]) == 1
        assert len(payload["messages"][0]["content"]) == 2

    def test_system_message_is_refused(self) -> None:
        """System content belongs in system_prompt, not a message."""
        request = _request(ANTHROPIC_MODEL, messages=(Message.system("nope"),))
        with pytest.raises(InvalidRequestError):
            _anthropic()._encode_request(request, stream=False)

    def test_structured_output_is_refused(self) -> None:
        """Fake it by prompting would be a silent fallback."""
        request = _request(ANTHROPIC_MODEL, response_json_schema={"type": "object"})
        with pytest.raises(InvalidRequestError):
            _anthropic()._encode_request(request, stream=False)


class TestAnthropicUsageDecoding:
    """Usage extraction."""

    def test_reported_usage(self) -> None:
        usage = AnthropicProvider._decode_usage({"input_tokens": 10, "output_tokens": 5})
        assert usage is not None
        assert usage.total == 15
        # Anthropic reports no total, so ours is derived rather than invented.
        assert usage.total_tokens is None

    def test_cached_tokens(self) -> None:
        usage = AnthropicProvider._decode_usage(
            {"input_tokens": 10, "output_tokens": 5, "cache_read_input_tokens": 3}
        )
        assert usage is not None
        assert usage.cached_input_tokens == 3

    def test_absent_usage_is_none(self) -> None:
        assert AnthropicProvider._decode_usage(None) is None


class TestAnthropicStructuredRefusal:
    """The structured() method must never silently fall back."""

    @pytest.mark.asyncio
    async def test_structured_always_refuses(self) -> None:
        with pytest.raises(ModelNotAvailableError):
            await _anthropic().structured(
                _request(ANTHROPIC_MODEL, response_json_schema={"type": "object"}),
                timeout_seconds=1.0,
            )


class TestNoCredentialLeakage:
    """Adapters must not expose the credential in their repr."""

    def test_openai_headers_are_well_formed(self) -> None:
        headers = _openai()._headers()
        assert headers["content-type"] == "application/json"

    def test_anthropic_uses_a_header_not_a_bearer(self) -> None:
        headers = _anthropic()._headers()
        assert headers["x-api-key"] == KEY.reveal()
        assert "authorization" not in headers

    def test_anthropic_version_is_set(self) -> None:
        assert _anthropic()._headers()["anthropic-version"] == "2023-06-01"


class TestSafeJsonObject:
    """Malformed tool arguments must not produce a provider 400."""

    def test_valid_json(self) -> None:
        from mas_orchestrator.providers.anthropic import _safe_json_object

        assert _safe_json_object('{"a":1}') == {"a": 1}

    def test_invalid_json_becomes_empty(self) -> None:
        from mas_orchestrator.providers.anthropic import _safe_json_object

        assert _safe_json_object("not json") == {}

    def test_non_object_json_becomes_empty(self) -> None:
        from mas_orchestrator.providers.anthropic import _safe_json_object

        assert _safe_json_object("[1,2]") == {}
