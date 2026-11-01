"""OpenAI provider adapter.

Implements :class:`~mas_orchestrator.models.base.ModelProvider` against the
OpenAI Chat Completions API. Only this file knows the OpenAI wire format; the
rest of the orchestrator works with
:mod:`mas_orchestrator.models.types`.

Behavioural contract:

* **All I/O goes through** :class:`~mas_orchestrator.providers.base_http.HttpTransport`,
  which enforces the host allowlist, blocks private address ranges after DNS
  resolution, bounds the response size, and redacts headers.
* **Timeouts** cover connect, read and write; a call that exceeds its deadline
  raises :class:`ModelTimeoutError`.
* **Cancellation** is checked before the call and observed while streaming, so
  a cancelled execution stops generating promptly.
* **Usage is reported only when present.** If OpenAI omits ``usage``, the
  response carries ``usage=None`` — never a fabricated zero.
* **Errors are typed.** 429 becomes :class:`ModelRateLimitedError` carrying the
  parsed ``Retry-After``; 5xx becomes a retryable
  :class:`ModelProviderError`; a body we cannot parse becomes a retryable
  :class:`ModelProviderError`. No raw provider text is echoed into an error
  message.
* **No retries here.** Retry and failover belong to
  :mod:`mas_orchestrator.models.retry` and
  :mod:`mas_orchestrator.models.failover`, so budgets and allowlists apply
  uniformly.
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
    "OPENAI_DEFAULT_HOST",
    "OpenAIProvider",
]

#: Only host this adapter is permitted to contact by default.
OPENAI_DEFAULT_HOST: Final[str] = "api.openai.com"

#: Models this adapter knows how to drive, with their capabilities.
_KNOWN_MODELS: Final[Mapping[str, ModelCapabilities]] = {}


def _build_capabilities_table() -> Mapping[str, ModelCapabilities]:
    """Builds the capability table.

    Kept as a function so the module imports without constructing
    (and therefore validating) every capability object at import time.
    """
    from mas_orchestrator.models.types import ModelCapabilities

    return {
        "gpt-4o": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=True,
            supports_system_prompt=True,
            max_input_tokens=128_000,
            max_output_tokens=16_384,
        ),
        "gpt-4o-mini": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=True,
            supports_system_prompt=True,
            max_input_tokens=128_000,
            max_output_tokens=16_384,
        ),
        "gpt-4.1": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=True,
            supports_system_prompt=True,
            max_input_tokens=1_047_576,
            max_output_tokens=32_768,
        ),
        "gpt-4.1-mini": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=True,
            supports_system_prompt=True,
            max_input_tokens=1_047_576,
            max_output_tokens=32_768,
        ),
        "o3-mini": ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=True,
            supports_system_prompt=False,
            max_input_tokens=200_000,
            max_output_tokens=100_000,
        ),
    }


#: Maps OpenAI finish reasons onto the normalised enum.
_FINISH_REASONS: Final[Mapping[str, FinishReason]] = {
    "stop": FinishReason.STOP,
    "length": FinishReason.LENGTH,
    "tool_calls": FinishReason.TOOL_CALLS,
    "function_call": FinishReason.TOOL_CALLS,
    "content_filter": FinishReason.CONTENT_FILTER,
}


class OpenAIProvider:
    """Adapter for the OpenAI Chat Completions API.

    Args:
        api_key: Resolved API key. Never read from configuration directly.
        transport: Optional pre-built transport; built from defaults when
            omitted. Injectable for tests.
        base_url: API base URL. Must remain on an allowlisted host.
        organization: Optional OpenAI organization id sent as a header.

    Raises:
        ConfigurationError: If the base URL is not https, or the host is not
            permitted by the policy.
    """

    provider_id: Final[str] = "openai"

    def __init__(
        self,
        api_key: Secret,
        *,
        transport: HttpTransport | None = None,
        base_url: str = f"https://{OPENAI_DEFAULT_HOST}/v1",
        organization: str | None = None,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 60.0,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._organization = organization
        self._capabilities = _build_capabilities_table()

        if transport is not None:
            self._transport = transport
            self._owns_transport = False
        else:
            policy = HttpPolicy.for_providers()
            if not policy.is_host_allowed(_host_of(self._base_url)):
                raise ModelNotAvailableError(
                    "openai base URL host is not permitted by the HTTP policy",
                    details={"reason": "host_not_allowlisted"},
                )
            self._transport = HttpTransport(
                policy,
                connect_timeout_seconds=connect_timeout_seconds,
                read_timeout_seconds=read_timeout_seconds,
                credential=api_key,
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
        """Per-request headers beyond the shared credential."""
        headers = {"content-type": "application/json"}
        if self._organization:
            headers["OpenAI-Organization"] = self._organization
        return headers

    def _encode_request(self, request: ModelRequest, *, stream: bool) -> dict[str, Any]:
        """Converts a typed request into the OpenAI wire payload.

        Raises:
            InvalidRequestError: If the model does not support system prompts
                and one was supplied. Folding it into a user message would
                change the caller's intent, so it is refused instead.
        """
        capabilities = self.capabilities(request.model)

        messages: list[dict[str, Any]] = []
        if request.system_prompt:
            if not capabilities.supports_system_prompt:
                raise InvalidRequestError(
                    "model does not support a system prompt",
                    details={"model": request.model.key},
                )
            messages.append({"role": "system", "content": request.system_prompt})
        messages.extend(self._encode_message(message) for message in request.messages)

        payload: dict[str, Any] = {
            "model": request.model.model,
            "messages": messages,
            "max_tokens": request.max_output_tokens,
            "stream": stream,
        }

        if request.temperature is not None:
            payload["temperature"] = request.temperature
        if request.top_p is not None:
            payload["top_p"] = request.top_p
        if request.stop_sequences:
            payload["stop"] = list(request.stop_sequences)
        if request.tools:
            payload["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": dict(tool.json_schema),
                    },
                }
                for tool in request.tools
            ]
            if request.tool_choice:
                payload["tool_choice"] = self._encode_tool_choice(request.tool_choice)

        if request.response_json_schema is not None:
            if not capabilities.supports_structured_output:
                raise InvalidRequestError(
                    "model does not support schema-constrained output",
                    details={"model": request.model.key},
                )
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "structured_output",
                    "schema": dict(request.response_json_schema),
                    "strict": False,
                },
            }

        if stream:
            # Required for usage to be reported on a streamed response.
            payload["stream_options"] = {"include_usage": True}

        return payload

    @staticmethod
    def _encode_tool_choice(choice: str) -> dict[str, Any] | str:
        """Maps the tool_choice string onto the OpenAI representation."""
        if choice in {"auto", "none", "required"}:
            return choice
        return {"type": "function", "function": {"name": choice}}

    @staticmethod
    def _encode_message(message: Message) -> dict[str, Any]:
        """Converts one typed message to the OpenAI wire form."""
        encoded: dict[str, Any] = {"role": message.role.value, "content": message.content}

        if message.role is Role.TOOL and message.tool_call_id:
            encoded["tool_call_id"] = message.tool_call_id
        if message.name and message.role is not Role.TOOL:
            encoded["name"] = message.name
        if message.tool_calls:
            encoded["tool_calls"] = [
                {
                    "id": call.id,
                    "type": "function",
                    "function": {
                        "name": call.name,
                        "arguments": call.arguments_json,
                    },
                }
                for call in message.tool_calls
            ]
            if not encoded["content"]:
                encoded["content"] = ""
        return encoded

    @staticmethod
    def _decode_usage(raw: Any) -> Usage | None:
        """Extracts usage, returning ``None`` when the provider omits it."""
        if not isinstance(raw, dict):
            return None

        prompt = raw.get("prompt_tokens")
        completion = raw.get("completion_tokens")
        if not isinstance(prompt, int) or not isinstance(completion, int):
            return None

        total = raw.get("total_tokens")
        cached = None
        details = raw.get("prompt_tokens_details")
        if isinstance(details, dict) and isinstance(details.get("cached_tokens"), int):
            cached = details["cached_tokens"]

        return Usage(
            input_tokens=prompt,
            output_tokens=completion,
            total_tokens=total if isinstance(total, int) else None,
            cached_input_tokens=cached,
        )

    @staticmethod
    def _decode_message(raw: Mapping[str, Any]) -> tuple[str, list[Any]]:
        """Extracts text and tool calls from a response message."""
        content = raw.get("content")
        text = content if isinstance(content, str) else ""

        tool_calls: list[Any] = []
        raw_calls = raw.get("tool_calls")
        if isinstance(raw_calls, list):
            for call in raw_calls:
                if not isinstance(call, dict):
                    continue
                function = call.get("function")
                if not isinstance(function, dict):
                    continue
                from mas_orchestrator.models.types import ToolCall

                tool_calls.append(
                    ToolCall(
                        id=str(call.get("id", "")),
                        name=str(function.get("name", "")),
                        arguments_json=str(function.get("arguments", "{}")),
                    )
                )
        return text, tool_calls

    def _raise_for_status(self, response: TransportResponse) -> None:
        """Raises the typed error implied by an HTTP status.

        The provider's own error text is never propagated: it is untrusted
        content that may echo request material. Only the HTTP status and our
        own classification are surfaced.
        """
        if response.status_code < 400:
            return
        retry_after = parse_retry_after(response.headers.get("retry-after"))
        error = classify_status(response.status_code, retry_after)
        raise error

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
            ModelProviderError: On any other provider failure, including an
                unparseable body.
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

        choices = decoded.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ModelProviderError(
                "provider returned no choices", details={"provider": self.provider_id}
            )

        first = choices[0]
        if not isinstance(first, dict):
            raise ModelProviderError(
                "provider returned a malformed choice",
                details={"reason": "choice_not_an_object"},
            )

        message = first.get("message")
        if not isinstance(message, dict):
            raise ModelProviderError(
                "provider returned a malformed message",
                details={"reason": "message_not_an_object"},
            )

        text, tool_calls = self._decode_message(message)
        raw_reason = first.get("finish_reason")
        reason = _FINISH_REASONS.get(
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
        """Posts to the chat completions endpoint.

        Raises:
            ModelTimeoutError: If the overall deadline elapsed.
        """
        url = f"{self._base_url}/chat/completions"
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
            ModelProviderError: On a malformed chunk or a transport failure.
        """
        from mas_orchestrator.models.base import assert_streaming_supported

        assert_streaming_supported(self, request.model)

        if cancel_event is not None and cancel_event.is_set():
            raise CancellationRequestedError("cancelled before the model call")

        payload = self._encode_request(request, stream=True)
        url = f"{self._base_url}/chat/completions"

        # Accumulated state for the terminal event.
        buffered_usage: Usage | None = None
        last_reason: FinishReason | None = None
        last_raw_reason: str | None = None

        try:
            async for chunk in self._iter_sse(url, payload, timeout_seconds):
                if cancel_event is not None and cancel_event.is_set():
                    raise CancellationRequestedError("cancelled during streaming")

                for event in self._decode_chunk(chunk, request.model):
                    if event.usage is not None:
                        buffered_usage = event.usage
                    if event.finish_reason is not None:
                        last_reason = event.finish_reason
                        last_raw_reason = event.raw_finish_reason
                    if not event.is_terminal:
                        yield event
        except TimeoutError as error:
            raise ModelTimeoutError(
                "model stream exceeded its deadline",
                details={"model": request.model.key},
            ) from error

        yield ModelStreamEvent(
            model=request.model,
            finish_reason=last_reason or FinishReason.STOP,
            usage=buffered_usage,
            is_terminal=True,
            raw_finish_reason=last_raw_reason,
        )

    async def _iter_sse(
        self,
        url: str,
        payload: dict[str, Any],
        timeout_seconds: float,
    ) -> AsyncIterator[bytes]:
        """Yields raw Server-Sent-Event lines from the streaming response.

        The SSE parsing lives in :mod:`mas_orchestrator.models.streaming` so
        it is shared with every provider; this method only produces bytes.
        """
        from mas_orchestrator.models.streaming import iter_sse_data

        response = await asyncio.wait_for(
            self._transport.request("POST", url, json_body=payload, headers=self._headers()),
            timeout=timeout_seconds,
        )
        self._raise_for_status(response)

        for data in iter_sse_data(response.body):
            yield data

    def _decode_chunk(
        self,
        data: bytes,
        model: ModelRef,
    ) -> list[ModelStreamEvent]:
        """Decodes one SSE payload into stream events."""
        events: list[ModelStreamEvent] = []

        if data.strip() == b"[DONE]":
            return events

        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            # A single undecodable chunk is not worth failing the stream for;
            # skipping it keeps one bad byte from destroying a good response.
            _LOGGER.warning("skipping an undecodable stream chunk")
            return events

        if not isinstance(payload, dict):
            return events

        # Usage arrives on a final chunk that carries no choices.
        usage = self._decode_usage(payload.get("usage"))
        if usage is not None and not payload.get("choices"):
            events.append(ModelStreamEvent(model=model, usage=usage))

        choices = payload.get("choices")
        if not isinstance(choices, list):
            return events

        for choice in choices:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta")
            raw_reason = choice.get("finish_reason")
            reason = (
                _FINISH_REASONS.get(raw_reason, FinishReason.OTHER)
                if isinstance(raw_reason, str)
                else None
            )

            text = ""
            tool_call: Any = None
            if isinstance(delta, dict):
                content = delta.get("content")
                if isinstance(content, str):
                    text = content
                raw_calls = delta.get("tool_calls")
                if isinstance(raw_calls, list) and raw_calls:
                    call = raw_calls[0]
                    if isinstance(call, dict):
                        function = call.get("function")
                        function = function if isinstance(function, dict) else {}
                        from mas_orchestrator.models.types import ToolCall

                        tool_call = ToolCall(
                            id=str(call.get("id", "")),
                            name=str(function.get("name", "")),
                            arguments_json=str(function.get("arguments", "")),
                        )

            if text or tool_call is not None or reason is not None:
                events.append(
                    ModelStreamEvent(
                        model=model,
                        delta_text=text,
                        delta_tool_call=tool_call,
                        finish_reason=reason,
                        raw_finish_reason=raw_reason if isinstance(raw_reason, str) else None,
                    )
                )

        return events

    async def structured(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a schema-constrained completion.

        Raises:
            InvalidRequestError: If no schema was supplied.
            ModelNotAvailableError: If the model cannot constrain output.
        """
        if request.response_json_schema is None:
            raise InvalidRequestError(
                "structured output requires response_json_schema",
                details={"field": "response_json_schema"},
            )
        if not self.capabilities(request.model).supports_structured_output:
            raise ModelNotAvailableError(
                "model does not support schema-constrained output",
                details={"model": request.model.key, "provider": self.provider_id},
            )
        return await self.complete(
            request, timeout_seconds=timeout_seconds, cancel_event=cancel_event
        )

    async def health(self) -> bool:
        """Returns whether the OpenAI API is reachable.

        Uses the models list endpoint, which is cheap and does not consume
        generation quota.
        """
        try:
            response = await self._transport.request(
                "GET", f"{self._base_url}/models", headers=self._headers()
            )
        except (ModelProviderError, ModelTimeoutError, ModelRateLimitedError):
            return False
        return response.status_code < 500


def _host_of(url: str) -> str:
    """Extracts the hostname from a URL for policy checks."""
    from urllib.parse import urlparse

    return urlparse(url).hostname or ""
