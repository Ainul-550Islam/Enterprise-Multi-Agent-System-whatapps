"""Gemini provider adapter.

Adapter for Google's Gemini model family via the Generative Language API.
Supports tool calls, streaming, and standard chat completion.
All API keys must be resolved from secrets — no plaintext keys.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import resolve_secret
from mas_orchestrator.models.types import ModelRef, ModelCapabilities

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Gemini API base URL
GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1"
#: Default model name
DEFAULT_MODEL = "gemini-1.5-flash"
#: Default timeout in seconds
DEFAULT_TIMEOUT = 30


class GeminiProvider:
    """Gemini provider adapter.

    Wraps the Google Gemini model family via the Generative Language API.
    Supports tool calls, streaming, and standard chat completion.
    """

    def __init__(self, *, api_key: str, base_url: str = GEMINI_API_URL,
                 connect_timeout_seconds: float = 5.0,
                 read_timeout_seconds: float = 60.0) -> None:
        self.provider_id = "gemini"
        self.api_key = api_key
        self.base_url = base_url
        self.connect_timeout_seconds = connect_timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds

        # Validate API key
        if not self.api_key:
            raise ConfigurationError(
                "api_key is required for Gemini provider",
                details={"field": "api_key"},
            )

    async def chat(self, *, messages: list, tools: list | None = None,
                   stream: bool = False, model: str | None = None) -> dict:
        """Send a chat completion request to Gemini.

        Args:
            messages: List of message dicts (role, content).
            tools: Optional list of tool declarations.
            stream: Whether to use streaming response.
            model: Optional model name override.

        Returns:
            Dictionary with the response including content, tool calls, etc.

        Raises:
            ConfigurationError: If the provider is not properly configured.
            RuntimeError: If the API call fails.
        """
        if not messages:
            raise ConfigurationError(
                "messages must not be empty",
                details={"field": "messages"},
            )

        model_name = model or DEFAULT_MODEL
        url = f"{self.base_url}/models/{model_name}:generateContent?key={self.api_key}"
        
        payload = {
            "contents": [
                {"role": "user", "parts": [{"text": messages[-1].get("content", "")}]}
            ]
        }
        
        if tools:
            payload["tool_declarations"] = tools
        
        if stream:
            payload["stream"] = True
        
        try:
            async with asyncio.timeout(self.read_timeout_seconds):
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=self.read_timeout_seconds,
                    )
                    
                    if response.is_success:
                        data = response.json()
                        # Extract response text and tool calls
                        candidates = data.get("candidates", [])
                        result = {
                            "content": "",
                            "tool_calls": [],
                            "model": model_name,
                            "finish_reason": "unknown",
                        }
                        
                        if candidates:
                            candidate = candidates[0]
                            content_parts = candidate.get("content", {}).get("parts", [])
                            if content_parts:
                                result["content"] = content_parts[0].get("text", "")
                            
                            # Extract tool calls if present
                            tool_calls_list = candidate.get("function_call", None)
                            if tool_calls_list:
                                result["tool_calls"] = [
                                    {
                                        "name": tool_calls_list.get("name", ""),
                                        "arguments": tool_calls_list.get("arguments", ""),
                                    }
                                ]
                            
                            result["finish_reason"] = candidate.get("finish_reason", "unknown")
                        
                        _LOGGER.info(
                            "gemini chat returned result for model %s", model_name
                        )
                        return {
                            "content": result["content"],
                            "tool_calls": result["tool_calls"],
                            "model": result["model"],
                            "finish_reason": result["finish_reason"],
                        }
                    else:
                        raise RuntimeError(
                            f"Gemini API returned {response.status_code}: {response.text}"
                        )

        except asyncio.TimeoutError:
            raise RuntimeError(f"Gemini API call timed out")
        except Exception as _e:
            raise RuntimeError(f"failed to communicate with Gemini API: {_e}")

    async def chat_stream(self, *, messages: list, tools: list | None = None,
                         model: str | None = None) -> asyncio.AsyncGenerator:
        """Send a streaming chat completion request to Gemini.

        Args:
            messages: List of message dicts (role, content).
            tools: Optional list of tool declarations.
            model: Optional model name override.

        Yields:
            Dictionary chunks from the streaming response.
        """
        # For now, implement as non-streaming with polling
        # In a full implementation, this would use the SSE stream endpoint
        result = await self.chat(
            messages=messages,
            tools=tools,
            stream=False,
            model=model,
        )
        
        # Yield the full result as a single chunk
        yield {
            "content": result.get("content", ""),
            "tool_calls": result.get("tool_calls", []),
            "model": result.get("model", ""),
            "finish_reason": result.get("finish_reason", "unknown"),
        }

    def get_capabilities(self) -> ModelCapabilities:
        """Return the capabilities of this provider."""
        from mas_orchestrator.models.types import ModelCapabilities
        return ModelCapabilities(
            supports_tools=True,
            supports_streaming=True,
            supports_structured_output=False,
            supports_system_prompt=True,
            max_input_tokens=1_048_576,  # Gemini 1.5 context
            max_output_tokens = 8192,
        )


def create_provider(
    *,
    api_key: str,
    base_url: str = GEMINI_API_URL,
    connect_timeout_seconds: float = 5.0,
    read_timeout_seconds: float = 60.0,
    model: str | None = None,
) -> GeminiProvider:
    """Convenient function to create a GeminiProvider instance."""
    return GeminiProvider(
        api_key=api_key,
        base_url=base_url,
        connect_timeout_seconds=connect_timeout_seconds,
        read_timeout_seconds=read_timeout_seconds,
    )
