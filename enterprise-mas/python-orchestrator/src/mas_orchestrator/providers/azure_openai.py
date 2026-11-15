"""Azure OpenAI provider adapter.

Adapter for Azure OpenAI service via the REST API.
Supports deployment-based model access with custom deployment names.
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

#: Azure OpenAI API base URL template
AZURE_API_URL_TEMPLATE = "https://{deployment_name}.openai.azure.com/openai/deployments/{deployment_name}/extensions"
#: Default API version
DEFAULT_API_VERSION = "2024-02-15"
#: Default model/deployment name
DEFAULT_DEPLOYMENT = "gpt-4"


class AzureOpenAIProvider:
    """Azure OpenAI provider adapter.

    Wraps the Azure OpenAI service via deployment-based model access.
    Supports chat completion with tool calls and standard chat.
    """

    def __init__(self, *, api_key: str, base_url: str | None = None,
                 api_version: str = DEFAULT_API_VERSION,
                 deployment_name: str = DEFAULT_DEPLOYMENT,
                 connect_timeout_seconds: float = 5.0,
                 read_timeout_seconds: float = 60.0) -> None:
        self.provider_id = "azure_openai"
        self.api_key = api_key
        self.base_url = base_url or f"https://{deployment_name}.openai.azure.com"
        self.api_version = api_version
        self.deployment_name = deployment_name
        self.connect_timeout_seconds = connect_timeout_seconds
        self.read_timeout_seconds = read_timeout_seconds

        # Validate API key
        if not self.api_key:
            raise ConfigurationError(
                "api_key is required for Azure OpenAI provider",
                details={"field": "api_key"},
            )

        # Validate deployment name
        if not self.deployment_name:
            raise ConfigurationError(
                "deployment_name is required for Azure OpenAI provider",
                details={"field": "deployment_name"},
            )

    async def chat(self, *, messages: list, tools: list | None = None,
                   stream: bool = False, model: str | None = None) -> dict:
        """Send a chat completion request to Azure OpenAI.

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
                details={"field": "deployment_name"},
            )

        # Use the deployment name as the model
        model_name = model or self.deployment_name
        # Construct the API endpoint
        url = f"{self.base_url}/chat/completions?api-version={self.api_version}"
        
        payload = {
            "messages": messages,
            "model": model_name,
            "temperature": 0.7,
        }
        
        if tools:
            payload["tools"] = tools
        
        try:
            async with asyncio.timeout(self.read_timeout_seconds):
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        url,
                        json=payload,
                        headers={
                            "Content-Type": "application/json",
                            "api-key": self.api_key,
                        },
                        timeout=self.read_timeout_seconds,
                    )
                    
                    if response.is_success:
                        data = response.json()
                        # Extract the choice and message
                        choices = data.get("choices", [])
                        result = {
                            "content": "",
                            "tool_calls": [],
                            "model": data.get("model", model_name),
                            "finish_reason": "unknown",
                        }
                        
                        if choices:
                            choice = choices[0]
                            message = choice.get("message", {})
                            
                            # Extract content
                            content = message.get("content", "")
                            result["content"] = content if content else ""
                            
                            # Extract tool calls if present
                            tool_calls = message.get("tool_calls", [])
                            if tool_calls:
                                result["tool_calls"] = []
                                for tc in tool_calls:
                                    result["tool_calls"].append({
                                        "name": tc.get("function", {}).get("name", ""),
                                        "arguments": tc.get("function", {}).get("arguments", ""),
                                    })
                            
                            result["finish_reason"] = choice.get("finish_reason", "unknown")
                        
                        _LOGGER.info(
                            "azure_openai chat returned result for deployment %s",
                            self.deployment_name
                        )
                        return {
                            "content": result["content"],
                            "tool_calls": result["tool_calls"],
                            "model": result["model"],
                            "finish_reason": result["finish_reason"],
                        }
                    else:
                        raise RuntimeError(
                            f"Azure OpenAI returned {response.status_code}: {response.text}"
                        )

        except asyncio.TimeoutError:
            raise RuntimeError(f"Azure OpenAI API call timed out")
        except Exception as e:
            raise RuntimeError(f"failed to communicate with Azure OpenAI: {e}")

    async def chat_stream(self, *, messages: list, tools: list | None = None,
                         model: str | None = None) -> asyncio.AsyncGenerator:
        """Send a streaming chat completion request to Azure OpenAI.

        Args:
            messages: List of message dicts (role, content).
            tools: Optional list of tool declarations.
            model: Optional model name override.

        Yields:
            Dictionary chunks from the streaming response.
        """
        # For now, implement as non-streaming
        result = await self.chat(
            messages=messages,
            tools=tools,
            stream=False,
            model=model,
        )
        
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
            max_input_tokens=8192,
            max_output_tokens = 4096,
        )


def create_provider(
    *,
    api_key: str,
    base_url: str | None = None,
    api_version: str = DEFAULT_API_VERSION,
    deployment_name: str = DEFAULT_DEPLOYMENT,
    connect_timeout_seconds: float = 5.0,
    read_timeout_seconds: float = 60.0,
) -> AzureOpenAIProvider:
    """Convenient function to create an AzureOpenAIProvider instance."""
    return AzureOpenAIProvider(
        api_key=api_key,
        base_url=base_url,
        api_version=api_version,
        deployment_name=deployment_name,
        connect_timeout_seconds=connect_timeout_seconds,
        read_timeout_seconds=read_timeout_seconds,
    )
