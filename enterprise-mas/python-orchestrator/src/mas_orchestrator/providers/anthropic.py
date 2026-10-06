"""Anthropic provider adapter.

Implements :class:`~mas_orchestrator.models.base.ModelProvider` against the
Anthropic Messages API. The wire format differs from OpenAI's in three ways
that this adapter normalises:

* **System prompts are a top-level field**, not a message with a role.
* **Tool use travels in content blocks.** A single assistant message may mix
  ``text`` and ``tool_use`` blocks, and a tool result is a ``tool_result``
  block inside a *user* message rather than a message with a tool role.
* **Usage is reported as ``input_tokens`` / ``output_tokens``** (there is no
  ``total_tokens``), and prompt caching is reported under
  ``cache_read_input_tokens``.

Everything else — the HTTP guard, timeouts, cancellation, typed errors, and the
"no fabricated usage" rule — is identical to the OpenAI adapter and comes from
the shared :mod:`~mas_orchestrator.providers.base_http` transport.

Structured output note: Anthropic has no native JSON-schema mode. This adapter
**refuses** to fake it by appending instructions to the prompt, because that is
a silent fallback that produces invalid JSON under load. :meth:`structured`
raises :class:`ModelNotAvailableError` so the router can select a model that
genuinely supports the capability.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    InvalidRequestError,
    ModelNotAvailableError,
    ModelProviderError,
    ModelRateLimitedError,
    ModelTimeoutError,
)
from mas_orchestrator.models.types import (
    FinishReason,
    Message,
    ModelRef,
    ModelRequest,
    ModelResponse,
    ModelStreamEvent,
    Role,
    Usage,
)
from mas_orchestrator.providers.base_http import (
    HttpPolicy,
    HttpTransport,
    TransportResponse,
    classify_status,
    parse_retry_after,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from mas_orchestrator.models.types import ModelCapabilities
    from mas_orchestrator.security.credentials import Secret

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "ANTHROPIC_DEFAULT_HOST",
    "ANTHROPIC_VERSION",
    "AnthropicProvider",
]

#: Only host this adapter is permitted to contact by default.
ANTHROPIC_DEFAULT_HOST: Final[str] = "api.anthropic.com"

#: The API version this adapter targets.
ANTHROPIC_VERSION: Final[str] = "2023-06-01"

#: Maps Anthropic stop reasons onto the normalised enum.
_STOP_REASONS: Final[Mapping[str, FinishReason]] = {
    "end_turn": FinishReason.STOP,
    "stop_sequence": FinishReason.STOP,
    "max_tokens": FinishReason.LENGTH,
    "tool_use": FinishReason.TOOL_CALLS,
    "pause_turn": FinishReason.OTHER,
    "refusal": FinishReason.CONTENT_FILTER,
}


def _build_capabilities_table() -> Mapping[str, ModelCapabilities]:
    """Builds the capability table for known Anthropic models."""
    from mas_orchestrator.models.types import ModelCapabilities

    return {
        "claude-opus-4-20250514": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=False,
            supports_system_prompt=True,
            max_input_tokens=200_000,
            max_output_tokens=32_000,
        ),
        "claude-sonnet-4-20250514": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=False,
            supports_system_prompt=True,
            max_input_tokens=200_000,
            max_output_tokens=64_000,
        ),
        "claude-3-7-sonnet-20250219": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=False,
            supports_system_prompt=True,
            max_input_tokens=200_000,
            max_output_tokens=64_000,
        ),
        "claude-3-5-haiku-20241022": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=False,
            supports_system_prompt=True,
            max_input_tokens=200_000,
            max_output_tokens=8_192,
        ),
    }


class AnthropicProvider:
    """Adapter for the Anthropic Messages API.

    Args:
        api_key: Resolved API key.
        transport: Optional pre-built transport (injectable for tests).
        base_url: API base URL; must remain on an allowlisted host.
        anthropic_version: API version header value.

    Raises:
        ModelNotAvailableError: If the base URL host is not permitted.
    """

    provider_id: Final[str] = "anthropic"

    def __init__(
        self,
        api_key: Secret,
        *,
        transport: HttpTransport | None = None,
        base_url: str = f"https://{ANTHROPIC_DEFAULT_HOST}/v1",
        anthropic_version: str = ANTHROPIC_VERSION,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 60.0,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._anthropic_version = anthropic_version
        self._capabilities = _build_capabilities_table()

        if transport is not None:
            self._transport = transport
            self._owns_transport = False
        else:
            policy = HttpPolicy.for_providers()
            if not policy.is_host_allowed(_host_of(self._base_url)):
                raise ModelNotAvailableError(
                    "anthropic base URL host is not permitted by the HTTP policy",
                    details={"reason": "host_not_allowlisted"},
                )
            self._transport = HttpTransport(
                policy,
                connect_timeout_seconds=connect_timeout_seconds,
                read_timeout_seconds=read_timeout_seconds,
                credential=None,  # Anthropic uses a header, not a bearer token.
            )
            self._owns_transport = True

    async def aclose(self) -> None:
        """Closes the transport, if this provider created it."""
        if self._owns_transport:
            await self._transport.aclose()

    def supports(self, model: ModelRef) -> bool:
        """Whether this provider serves *model*. Never raises."""
        return model.provider == self.provider_id and model.model in self._capabilities

    def capabilities(self, model: ModelRef) -> ModelCapabilities:
        """Returns capabilities for *model*.

        Raises:
            ModelNotAvailableError: If the model is unknown to this adapter.
        """
        try:
            return self._capabilities[model.model]
        except KeyError as error:
            raise ModelNotAvailableError(
                "unknown model for provider",
                details={"model": model.key, "provider": self.provider_id},
            ) from error

    def _headers(self) -> dict[str, str]:
        """Anthropic authenticates with an api-key header, not a bearer token."""
        return {
            "content-type": "application/json",
            "x-api-key": self._api_key.reveal(),
            "anthropic-version": self._anthropic_version,
        }

    def _encode_request(self, request: ModelRequest, *, stream: bool) -> dict[str, Any]:
        """Converts a typed request into the Anthropic wire payload.

        Raises:
            InvalidRequestError: If a schema was requested (unsupported) or if
                consecutive same-role messages could not be merged, which
                Anthropic would reject.
        """
        self.capabilities(request.model)  # validates the model is known

        payload: dict[str, Any] = {
            "model": request.model.model,
            "messages": self._encode_messages(request.messages),
            "max_tokens": request.max_output_tokens or 4_096,  # Anthropic requires max_tokens.
            "stream": stream,
        }

        if request.system_prompt:
            payload["system"] = request.system_prompt
        if request.temperature is not None:
            payload["temperature"] = request.temperature
        if request.top_p is not None:
            payload["top_p"] = request.top_p
        if request.stop_sequences:
            payload["stop_sequences"] = list(request.stop_sequences)
        if request.tools:
            payload["tools"] = [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": dict(tool.json_schema),
                }
                for tool in request.tools
            ]
            if request.tool_choice:
                payload["tool_choice"] = self._encode_tool_choice(request.tool_choice)

        if request.response_json_schema is not None:
            raise InvalidRequestError(
                "anthropic models do not support native schema-constrained output",
                details={"model": request.model.key},
            )

        return payload

    @staticmethod
    def _encode_tool_choice(choice: str) -> dict[str, Any] | str:
        """Maps the tool_choice string onto the Anthropic representation."""
        if choice == "auto":
            return {"type": "auto"}
        if choice == "none":
            return {"type": "none"}
        if choice == "required":
            return {"type": "any"}
        return {"type": "tool", "name": choice}

    def _encode_messages(self, messages: tuple[Message, ...]) -> list[dict[str, Any]]:
        """Converts typed messages to Anthropic's block-based form.

        Consecutive messages with the same role are merged, because Anthropic
        requires strict alternation between ``user`` and ``assistant``. A tool
        result is folded into a ``user`` message as a ``tool_result`` block.
        """
        encoded: list[dict[str, Any]] = []

        for message in messages:
            if message.role is Role.TOOL:
                block = {
                    "type": "tool_result",
                    "tool_use_id": message.tool_call_id or "",
                    "content": message.content,
                }
                if encoded and encoded[-1]["role"] == "user":
                    encoded[-1]["content"].append(block)
                else:
                    encoded.append({"role": "user", "content": [block]})
                continue

            if message.role is Role.SYSTEM:
                # System content is a top-level field; a system *message* is a
                # caller mistake, and folding it into a user message would
                # change its authority, so it is refused.
                raise InvalidRequestError(
                    "system content must be supplied via system_prompt, not as a message",
                    details={"role": str(message.role)},
                )

            role = "assistant" if message.role is Role.ASSISTANT else "user"
            blocks: list[dict[str, Any]] = []
            if message.content:
                blocks.append({"type": "text", "text": message.content})
            for call in message.tool_calls:
                blocks.append(
                    {
                        "type": "tool_use",
                        "id": call.id,
                        "name": call.name,
                        "input": _safe_json_object(call.arguments_json),
                    }
                )

            if encoded and encoded[-1]["role"] == role:
                encoded[-1]["content"].extend(blocks)
            else:
                encoded.append({"role": role, "content": blocks})

        return encoded

    @staticmethod
    def _decode_usage(raw: Any) -> Usage | None:
        """Extracts usage, returning ``None`` when the provider omits it."""
        if not isinstance(raw, dict):
            return None

        prompt = raw.get("input_tokens")
        completion = raw.get("output_tokens")
        if not isinstance(prompt, int) or not isinstance(completion, int):
            return None

        cached = raw.get("cache_read_input_tokens")
        return Usage(
            input_tokens=prompt,
            output_tokens=completion,
            total_tokens=None,  # Anthropic does not report a total.
            cached_input_tokens=cached if isinstance(cached, int) else None,
        )

    @staticmethod
    def _decode_content(raw: Any) -> tuple[str, list[Any]]:
        """Extracts text and tool calls from Anthropic content blocks."""
        from mas_orchestrator.models.types import ToolCall

        text_parts: list[str] = []
        tool_calls: list[ToolCall] = []

        if not isinstance(raw, list):
            return "", []

        for block in raw:
            if not isinstance(block, dict):
                continue
            block_type = block.get("type")
            if block_type == "text":
                text = block.get("text")
                if isinstance(text, str):
                    text_parts.append(text)
            elif block_type == "tool_use":
                tool_calls.append(
                    ToolCall(
                        id=str(block.get("id", "")),
                        name=str(block.get("name", "")),
                        arguments_json=json.dumps(block.get("input", {})),
                    )
                )

        return "".join(text_parts), tool_calls

    def _raise_for_status(self, response: TransportResponse) -> None:
        """Raises the typed error implied by an HTTP status.

        The provider's error body is never echoed: it is untrusted content.
        """
        if response.status_code < 400:
            return
        retry_after = parse_retry_after(response.headers.get("retry-after"))
        raise classify_status(response.status_code, retry_after)

    async def complete(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a completion to conclusion.

        Raises:
            CancellationRequestedError: If cancellation was already requested.
            ModelTimeoutError: If the deadline elapsed.
            ModelRateLimitedError: On HTTP 429.
            ModelProviderError: On any other provider failure.
        """
        if cancel_event is not None and cancel_event.is_set():
            raise CancellationRequestedError("cancelled before the model call")

        payload = self._encode_request(request, stream=False)
        response = await self._send(payload, timeout_seconds)
        self._raise_for_status(response)

        decoded = response.json()
        if not isinstance(decoded, dict):
            raise ModelProviderError(
                "provider returned a malformed response",
                details={"reason": "not_an_object"},
            )

        text, tool_calls = self._decode_content(decoded.get("content"))
        raw_reason = decoded.get("stop_reason")
        reason = _STOP_REASONS.get(
            raw_reason if isinstance(raw_reason, str) else "", FinishReason.OTHER
        )

        return ModelResponse(
            model=request.model,
            text=text,
            finish_reason=reason,
            tool_calls=tuple(tool_calls),
            usage=self._decode_usage(decoded.get("usage")),
            raw_finish_reason=raw_reason if isinstance(raw_reason, str) else None,
            response_id=decoded.get("id") if isinstance(decoded.get("id"), str) else None,
        )

    async def _send(self, payload: dict[str, Any], timeout_seconds: float) -> TransportResponse:
        """Posts to the messages endpoint under the call deadline."""
        url = f"{self._base_url}/messages"
        try:
            return await asyncio.wait_for(
                self._transport.request("POST", url, json_body=payload, headers=self._headers()),
                timeout=timeout_seconds,
            )
        except TimeoutError as error:
            raise ModelTimeoutError(
                "model call exceeded its deadline",
                details={"model": payload.get("model"), "timeout_seconds": timeout_seconds},
            ) from error

    async def astream(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> AsyncIterator[ModelStreamEvent]:
        """Streams a completion incrementally.

        Raises:
            ModelNotAvailableError: If the model does not support streaming.
            CancellationRequestedError: If cancellation is observed mid-stream.
            ModelProviderError: On a transport failure.
        """
        from mas_orchestrator.models.base import assert_streaming_supported
        from mas_orchestrator.models.streaming import iter_sse_data

        assert_streaming_supported(self, request.model)

        if cancel_event is not None and cancel_event.is_set():
            raise CancellationRequestedError("cancelled before the model call")

        payload = self._encode_request(request, stream=True)
        url = f"{self._base_url}/messages"

        response = await asyncio.wait_for(
            self._transport.request("POST", url, json_body=payload, headers=self._headers()),
            timeout=timeout_seconds,
        )
        self._raise_for_status(response)

        buffered_usage: Usage | None = None
        last_reason: FinishReason | None = None
        last_raw_reason: str | None = None

        for data in iter_sse_data(response.body):
            if cancel_event is not None and cancel_event.is_set():
                raise CancellationRequestedError("cancelled during streaming")

            for event in self._decode_chunk(data, request.model):
                if event.usage is not None:
                    buffered_usage = event.usage
                if event.finish_reason is not None:
                    last_reason = event.finish_reason
                    last_raw_reason = event.raw_finish_reason
                if not event.is_terminal:
                    yield event

        yield ModelStreamEvent(
            model=request.model,
            finish_reason=last_reason or FinishReason.STOP,
            usage=buffered_usage,
            is_terminal=True,
            raw_finish_reason=last_raw_reason,
        )

    def _decode_chunk(self, data: bytes, model: ModelRef) -> list[ModelStreamEvent]:
        """Decodes one SSE payload into stream events."""
        events: list[ModelStreamEvent] = []

        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            _LOGGER.warning("skipping an undecodable stream chunk")
            return events

        if not isinstance(payload, dict):
            return events

        event_type = payload.get("type")

        # Usage arrives on message_start and message_delta events.
        if event_type == "message_start":
            message = payload.get("message")
            if isinstance(message, dict):
                usage = self._decode_usage(message.get("usage"))
                if usage is not None:
                    events.append(ModelStreamEvent(model=model, usage=usage))
            return events

        if event_type == "message_delta":
            usage = self._decode_usage(payload.get("usage"))
            raw_reason = None
            delta = payload.get("delta")
            if isinstance(delta, dict) and isinstance(delta.get("stop_reason"), str):
                raw_reason = delta["stop_reason"]
            if usage is None and raw_reason is None:
                return events
            events.append(
                ModelStreamEvent(
                    model=model,
                    usage=usage,
                    finish_reason=(
                        _STOP_REASONS.get(raw_reason, FinishReason.OTHER) if raw_reason else None
                    ),
                    raw_finish_reason=raw_reason,
                )
            )
            return events

        if event_type == "content_block_delta":
            delta = payload.get("delta")
            if not isinstance(delta, dict):
                return events
            delta_type = delta.get("type")
            if delta_type == "text_delta":
                text = delta.get("text")
                if isinstance(text, str) and text:
                    events.append(ModelStreamEvent(model=model, delta_text=text))
            elif delta_type == "input_json_delta":
                partial = delta.get("partial_json")
                if isinstance(partial, str) and partial:
                    from mas_orchestrator.models.types import ToolCall

                    events.append(
                        ModelStreamEvent(
                            model=model,
                            delta_tool_call=ToolCall(id="", name="", arguments_json=partial),
                        )
                    )
            return events

        if event_type == "content_block_start":
            block = payload.get("content_block")
            if isinstance(block, dict) and block.get("type") == "tool_use":
                from mas_orchestrator.models.types import ToolCall

                events.append(
                    ModelStreamEvent(
                        model=model,
                        delta_tool_call=ToolCall(
                            id=str(block.get("id", "")),
                            name=str(block.get("name", "")),
                            arguments_json="",
                        ),
                    )
                )
            return events

        return events

    async def structured(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Always refuses: Anthropic has no native schema-constrained output.

        Raises:
            ModelNotAvailableError: Always. Appending instructions to the
                prompt would be a silent fallback that produces invalid JSON
                under load, so the router must pick a model that supports the
                capability natively instead.
        """
        del request, timeout_seconds, cancel_event
        raise ModelNotAvailableError(
            "anthropic models do not support native schema-constrained output",
            details={"provider": self.provider_id, "reason": "unsupported_capability"},
        )

    async def health(self) -> bool:
        """Returns whether the Anthropic API is reachable.

        Uses a minimal message request, which is the cheapest available probe
        that exercises authentication. It consumes a small number of tokens;
        there is no free metadata endpoint on this API.
        """
        try:
            response = await self._transport.request(
                "POST",
                f"{self._base_url}/messages",
                json_body={
                    "model": next(iter(self._capabilities), "claude-3-5-haiku-20241022"),
                    "max_tokens": 1,
                    "messages": [{"role": "user", "content": [{"type": "text", "text": "hi"}]}],
                },
                headers=self._headers(),
            )
        except (ModelProviderError, ModelTimeoutError, ModelRateLimitedError):
            return False
        return response.status_code < 500


def _safe_json_object(raw: str) -> dict[str, Any]:
    """Decodes a tool-call argument string, tolerating a malformed payload.

    Anthropic requires the tool input to be a JSON *object*. A model
    occasionally emits invalid JSON; passing it through raw would produce a
    provider 400, so it is replaced with an empty object and the discrepancy
    is surfaced by the tool layer's own schema validation instead.
    """
    try:
        decoded = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _host_of(url: str) -> str:
    """Extracts the hostname from a URL for policy checks."""
    from urllib.parse import urlparse

    return urlparse(url).hostname or ""
