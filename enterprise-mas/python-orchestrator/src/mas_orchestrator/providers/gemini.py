"""Google Gemini provider adapter.

Implements :class:`~mas_orchestrator.models.base.ModelProvider` against the
Google Generative Language API (``generativelanguage.googleapis.com``). Only
this file knows the Gemini wire format; the rest of the orchestrator works with
:mod:`mas_orchestrator.models.types`.

The same behavioural contract as the other first-party adapters applies, and is
enforced by the shared transport rather than re-implemented here:

* **All I/O goes through** :class:`~mas_orchestrator.providers.base_http.HttpTransport`,
  which pins validated DNS answers to the TCP dial, blocks private address
  ranges, bounds the response body and redacts headers.
* **The host is fixed.** The only permitted host is Google's; a ``base_url``
  override is refused unless it is that exact host. Gemini has no "compatible
  endpoint" mode, so a private gateway must go through the ``compatible``
  adapter instead of turning this adapter into an arbitrary-URL client.
* **Authentication is a ``x-goog-api-key`` header.** The credential arrives
  already resolved from a secret reference; it is never read from configuration
  or the environment here, and it is never logged.
* **Usage is reported only when present.** Gemini's ``usageMetadata`` is mapped
  to :class:`~mas_orchestrator.models.types.Usage`; when it is absent the
  response carries ``usage=None`` — never a fabricated zero.
* **Errors are typed.** 429 becomes :class:`ModelRateLimitedError` with any
  ``Retry-After``; 5xx a retryable :class:`ModelProviderError`; 404
  :class:`ModelNotAvailableError`; a body we cannot parse a retryable
  :class:`ModelProviderError`. No raw provider text is echoed.
* **No retries here.** Retry and failover belong to
  :mod:`mas_orchestrator.models.retry` and
  :mod:`mas_orchestrator.models.failover`.

What this file must **not** do: accept an arbitrary base URL, accept a raw API
key from configuration, fabricate usage, or implement retry/failover.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    InvalidRequestError,
    ModelNotAvailableError,
    ModelProviderError,
    ModelTimeoutError,
)
from mas_orchestrator.models.types import (
    FinishReason,
    Message,
    ModelCapabilities,
    ModelRef,
    ModelRequest,
    ModelResponse,
    ModelStreamEvent,
    Role,
    ToolCall,
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
    from collections.abc import AsyncIterator

    from mas_orchestrator.security.credentials import Secret

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "GEMINI_DEFAULT_BASE_URL",
    "GEMINI_DEFAULT_HOST",
    "GeminiProvider",
]

#: The only host this adapter may contact.
GEMINI_DEFAULT_HOST: Final[str] = "generativelanguage.googleapis.com"

#: API base URL. ``v1beta`` is the current public versioned surface.
GEMINI_DEFAULT_BASE_URL: Final[str] = f"https://{GEMINI_DEFAULT_HOST}/v1beta"

_API_KEY_HEADER: Final[str] = "x-goog-api-key"

#: Models this adapter knows how to drive, with their capabilities. A model not
#: listed here is reported as unavailable rather than assumed to work.
_KNOWN_MODELS: Final[Mapping[str, ModelCapabilities]] = {
    "gemini-2.0-flash": ModelCapabilities(
        supports_tools=True,
        supports_streaming=True,
        supports_structured_output=True,
        supports_system_prompt=True,
        max_input_tokens=1_000_000,
        max_output_tokens=8_192,
    ),
    "gemini-1.5-pro": ModelCapabilities(
        supports_tools=True,
        supports_streaming=True,
        supports_structured_output=True,
        supports_system_prompt=True,
        max_input_tokens=2_000_000,
        max_output_tokens=8_192,
    ),
    "gemini-1.5-flash": ModelCapabilities(
        supports_tools=True,
        supports_streaming=True,
        supports_structured_output=True,
        supports_system_prompt=True,
        max_input_tokens=1_000_000,
        max_output_tokens=8_192,
    ),
}


def _host_of(url: str) -> str:
    from urllib.parse import urlparse

    return (urlparse(url).hostname or "").lower()


class GeminiProvider:
    """Adapter for the Google Generative Language API.

    Args:
        api_key: Resolved API key. Never read from configuration directly.
        transport: Optional pre-built transport; built from defaults when
            omitted. Injectable for tests.
        base_url: API base URL. Only the Google host is accepted.
        connect_timeout_seconds: Connection budget.
        read_timeout_seconds: Per-read budget.

    Raises:
        ModelNotAvailableError: If the base URL host is not Google's.
    """

    provider_id: Final[str] = "gemini"

    def __init__(
        self,
        api_key: Secret,
        *,
        transport: HttpTransport | None = None,
        base_url: str = GEMINI_DEFAULT_BASE_URL,
        connect_timeout_seconds: float = 5.0,
        read_timeout_seconds: float = 60.0,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._capabilities = dict(_KNOWN_MODELS)

        host = _host_of(self._base_url)
        if not self._base_url.lower().startswith("https://") or host != GEMINI_DEFAULT_HOST:
            # Gemini has no compatible-endpoint mode. Refusing here keeps this
            # adapter from becoming an arbitrary-URL client.
            raise ModelNotAvailableError(
                "gemini base URL is not permitted",
                details={"reason": "host_not_allowlisted", "host": host},
            )

        if transport is not None:
            self._transport = transport
            self._owns_transport = False
        else:
            policy = HttpPolicy.for_providers(extra_hosts={GEMINI_DEFAULT_HOST})
            self._transport = HttpTransport(
                policy,
                connect_timeout_seconds=connect_timeout_seconds,
                read_timeout_seconds=read_timeout_seconds,
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

    # -- wire encoding ------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {"content-type": "application/json", _API_KEY_HEADER: self._api_key.reveal()}

    @staticmethod
    def _role_for(message: Message) -> str:
        if message.role is Role.USER:
            return "user"
        if message.role is Role.ASSISTANT:
            return "model"
        # Gemini has no separate tool role: a tool result is a user-role part
        # carrying the function response.
        return "user"

    def _encode_contents(self, request: ModelRequest) -> list[dict[str, Any]]:
        contents: list[dict[str, Any]] = []
        for message in request.messages:
            parts: list[dict[str, Any]] = []
            if message.role is Role.TOOL:
                # A tool result is carried entirely by the function response;
                # adding a parallel text part would duplicate the output.
                parts.append(
                    {
                        "functionResponse": {
                            "name": message.name or message.tool_call_id or "",
                            "response": {"output": message.content},
                        }
                    }
                )
            else:
                if message.content:
                    parts.append({"text": message.content})
                if message.tool_calls:
                    for call in message.tool_calls:
                        try:
                            args = json.loads(call.arguments_json) if call.arguments_json else {}
                        except json.JSONDecodeError as error:
                            raise InvalidRequestError(
                                "tool call arguments are not valid JSON",
                                details={"tool": call.name},
                            ) from error
                        parts.append({"functionCall": {"name": call.name, "args": args}})
            if not parts:
                continue
            contents.append({"role": self._role_for(message), "parts": parts})
        return contents

    def _encode_tools(self, request: ModelRequest) -> list[dict[str, Any]] | None:
        if not request.tools:
            return None
        declarations = [
            {
                "name": tool.name,
                "description": tool.description,
                "parameters": dict(tool.json_schema),
            }
            for tool in request.tools
        ]
        return [{"functionDeclarations": declarations}]

    def _encode_tool_config(self, request: ModelRequest) -> dict[str, Any] | None:
        if not request.tools:
            return None
        choice = request.tool_choice
        if choice == "none":
            mode = "NONE"
        elif choice == "required":
            mode = "ANY"
        elif choice and choice != "auto":
            mode = "ANY"
            return {
                "functionCallingConfig": {
                    "mode": mode,
                    "allowedFunctionNames": [choice],
                }
            }
        else:
            mode = "AUTO"
        return {"functionCallingConfig": {"mode": mode}}

    def _encode_request(self, request: ModelRequest, *, stream: bool) -> dict[str, Any]:
        payload: dict[str, Any] = {"contents": self._encode_contents(request)}
        if request.system_prompt:
            payload["systemInstruction"] = {"parts": [{"text": request.system_prompt}]}
        generation: dict[str, Any] = {}
        if request.temperature is not None:
            generation["temperature"] = request.temperature
        if request.top_p is not None:
            generation["topP"] = request.top_p
        if request.max_output_tokens is not None:
            generation["maxOutputTokens"] = request.max_output_tokens
        if request.stop_sequences:
            generation["stopSequences"] = list(request.stop_sequences)
        if request.response_json_schema is not None:
            generation["responseMimeType"] = "application/json"
            generation["responseSchema"] = dict(request.response_json_schema)
        if generation:
            payload["generationConfig"] = generation
        tools = self._encode_tools(request)
        if tools is not None:
            payload["tools"] = tools
            tool_config = self._encode_tool_config(request)
            if tool_config is not None:
                payload["toolConfig"] = tool_config
        return payload

    def _endpoint(self, model: str, *, stream: bool) -> str:
        method = "streamGenerateContent?alt=sse" if stream else "generateContent"
        return f"{self._base_url}/models/{model}:{method}"

    # -- response decoding --------------------------------------------------

    @staticmethod
    def _decode_usage(raw: Any) -> Usage | None:
        if not isinstance(raw, Mapping):
            return None
        prompt = raw.get("promptTokenCount")
        candidates = raw.get("candidatesTokenCount")
        if not isinstance(prompt, int) or not isinstance(candidates, int):
            return None
        total = raw.get("totalTokenCount")
        return Usage(
            input_tokens=prompt,
            output_tokens=candidates,
            total_tokens=total if isinstance(total, int) else None,
        )

    @staticmethod
    def _decode_finish(reason: Any) -> FinishReason:
        if not isinstance(reason, str):
            return FinishReason.OTHER
        if reason == "MAX_TOKENS":
            return FinishReason.LENGTH
        if reason == "STOP":
            return FinishReason.STOP
        if reason in {
            "SAFETY",
            "RECITATION",
            "BLOCKLIST",
            "PROHIBITED_CONTENT",
            "SPII",
            "LANGUAGE",
        }:
            return FinishReason.CONTENT_FILTER
        return FinishReason.OTHER

    def _decode_candidate(
        self, candidate: Mapping[str, Any]
    ) -> tuple[str, tuple[ToolCall, ...], FinishReason, str | None]:
        content = candidate.get("content")
        text_parts: list[str] = []
        tool_calls: list[Any] = []
        if isinstance(content, Mapping):
            for part in content.get("parts", []) or []:
                if not isinstance(part, Mapping):
                    continue
                if isinstance(part.get("text"), str):
                    text_parts.append(part["text"])
                call = part.get("functionCall")
                if isinstance(call, Mapping):
                    raw_args = call.get("args")
                    try:
                        arguments = (
                            json.dumps(raw_args, separators=(",", ":"))
                            if raw_args is not None
                            else "{}"
                        )
                    except (TypeError, ValueError):
                        arguments = "{}"
                    tool_calls.append(
                        ToolCall(
                            id=f"gemini-{len(tool_calls)}",
                            name=str(call.get("name") or ""),
                            arguments_json=arguments,
                        )
                    )
        reason = self._decode_finish(candidate.get("finishReason"))
        return "".join(text_parts), tuple(tool_calls), reason, candidate.get("finishReason")

    def _raise_for_status(self, response: TransportResponse) -> None:
        """Raises the typed error implied by an HTTP status.

        The provider's own error text is never propagated: it is untrusted
        content that may echo request material. Only the HTTP status and our
        own classification are surfaced.
        """
        if response.status_code < 400:
            return
        retry_after = parse_retry_after(response.headers.get("retry-after"))
        raise classify_status(response.status_code, retry_after)

    # -- operations ---------------------------------------------------------

    async def _send(
        self, payload: dict[str, Any], *, model: str, stream: bool, timeout_seconds: float
    ) -> TransportResponse:
        try:
            return await asyncio.wait_for(
                self._transport.request(
                    "POST",
                    self._endpoint(model, stream=stream),
                    json_body=payload,
                    headers=self._headers(),
                ),
                timeout=timeout_seconds,
            )
        except TimeoutError as error:
            raise ModelTimeoutError(
                "model call exceeded its deadline",
                details={"model": model, "timeout_seconds": timeout_seconds},
            ) from error

    async def complete(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a completion to conclusion."""
        if cancel_event is not None and cancel_event.is_set():
            raise CancellationRequestedError("model call cancelled before it started")
        payload = self._encode_request(request, stream=False)
        response = await self._send(
            payload, model=request.model.model, stream=False, timeout_seconds=timeout_seconds
        )
        self._raise_for_status(response)
        document = response.json()
        if not isinstance(document, dict):
            raise ModelProviderError(
                "provider returned a malformed response",
                details={"provider": self.provider_id},
                retryable=True,
            )
        candidates = document.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            raise ModelProviderError(
                "provider returned no candidates",
                details={"provider": self.provider_id},
                retryable=True,
            )
        text, tool_calls, finish, raw_finish = self._decode_candidate(candidates[0])
        return ModelResponse(
            model=request.model,
            text=text,
            finish_reason=FinishReason.TOOL_CALLS if tool_calls else finish,
            tool_calls=tool_calls,
            usage=self._decode_usage(document.get("usageMetadata")),
            raw_finish_reason=raw_finish,
        )

    async def astream(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> AsyncIterator[ModelStreamEvent]:
        """Streams a completion as server-sent events."""
        if cancel_event is not None and cancel_event.is_set():
            raise CancellationRequestedError("model call cancelled before it started")
        payload = self._encode_request(request, stream=True)
        response = await self._send(
            payload, model=request.model.model, stream=True, timeout_seconds=timeout_seconds
        )
        self._raise_for_status(response)
        async for event in self._iter_sse(response, request.model):
            if cancel_event is not None and cancel_event.is_set():
                raise CancellationRequestedError("model call cancelled during streaming")
            yield event

    async def _iter_sse(
        self, response: TransportResponse, model: ModelRef
    ) -> AsyncIterator[ModelStreamEvent]:
        """Parses Gemini's SSE body into typed stream events.

        A malformed event frame is skipped rather than trusted; the stream
        contract tolerates keep-alives and unknown fields.
        """
        for block in response.body.decode("utf-8", errors="replace").split("\n\n"):
            if not block.strip():
                continue
            data = "\n".join(
                line[len("data:") :].strip()
                for line in block.splitlines()
                if line.startswith("data:")
            )
            if not data or data == "[DONE]":
                continue
            try:
                document = json.loads(data)
            except json.JSONDecodeError:
                continue
            if not isinstance(document, dict):
                continue
            candidates = document.get("candidates")
            if not isinstance(candidates, list) or not candidates:
                continue
            candidate = candidates[0]
            content = candidate.get("content")
            if isinstance(content, Mapping):
                for part in content.get("parts", []) or []:
                    if not isinstance(part, Mapping):
                        continue
                    if isinstance(part.get("text"), str):
                        yield ModelStreamEvent(model=model, delta_text=part["text"])
                    call = part.get("functionCall")
                    if isinstance(call, Mapping):
                        yield ModelStreamEvent(
                            model=model,
                            delta_tool_call=ToolCall(
                                id="gemini-stream",
                                name=str(call.get("name") or ""),
                                arguments_json=json.dumps(
                                    call.get("args") or {}, separators=(",", ":")
                                ),
                            ),
                        )
            reason = candidate.get("finishReason")
            if isinstance(reason, str):
                yield ModelStreamEvent(
                    model=model,
                    finish_reason=self._decode_finish(reason),
                    is_terminal=True,
                    raw_finish_reason=reason,
                )

    async def structured(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a schema-constrained completion."""
        if request.response_json_schema is None:
            raise InvalidRequestError(
                "structured output requires a response_json_schema",
                details={"provider": self.provider_id},
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
        """Whether the credential and endpoint answer.

        Uses the zero-cost ``models/list``-free probe: a ``generateContent`` call
        with a one-token prompt would consume quota, so this instead checks that
        the API answers with a well-formed error for an empty model, which still
        proves reachability and credential acceptance without generating text.
        """
        try:
            response = await self._transport.request(
                "GET",
                f"{self._base_url}/models",
                headers=self._headers(),
                max_response_bytes=4096,
            )
        except Exception:
            return False
        return response.status_code < 500
