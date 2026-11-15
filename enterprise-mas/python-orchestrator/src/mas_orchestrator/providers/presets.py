"""Named provider presets for common deployment configurations.

Presets are pre-configured sets of provider specifications that can be
used to quickly set up a deployment with commonly-used providers.
Each preset is a list of :class:`ProviderSpec` objects.
"""

from __future__ import annotations

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.providers.factory import ProviderSpec, PROVIDER_IDS
from mas_orchestrator.security.credentials import resolve_secret


def ollama_preset() -> list[ProviderSpec]:
    """Preset for Ollama local models.

    Uses the ``compatible`` provider pointing at a local Ollama instance.
    Suitable for development and offline work.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "http://localhost:11434",
                "model": "llama3",
                "supports_tools": True,
                "supports_streaming": True,
            },
        ),
    ]


def vllm_preset() -> list[ProviderSpec]:
    """Preset for vLLM serving endpoints.

    Uses the ``compatible`` provider pointing at a vLLM instance.
    Suitable for production serving with high throughput.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "http://localhost:8000",
                "model": "meta-llama/llama-3-8b-instruct",
                "supports_tools": True,
                "supports_streaming": True,
            },
        ),
    ]


def groq_preset() -> list[ProviderSpec]:
    """Preset for Groq high-speed inference.

    Uses the ``compatible`` provider pointing at the Groq API.
    Suitable for low-latency requirements.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "https://api.groq.com/openai/v1",
                "model": "llama3-8b-8192",
                "supports_tools": True,
                "supports_streaming": False,
            },
            api_key_secret_ref="env:GROQ_API_KEY",
        ),
    ]


def together_preset() -> list[ProviderSpec]:
    """Preset for Together AI.

    Uses the ``compatible`` provider pointing at Together AI.
    Suitable for wide model selection.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "https://api.together.xyz/v1",
                "model": "meta-llama/Meta-Llama-3-8B-Instruct",
                "supports_tools": True,
                "supports_streaming": False,
            },
            api_key_secret_ref="env:TOGETHER_API_KEY",
        ),
    ]


def openrouter_preset() -> list[ProviderSpec]:
    """Preset for OpenRouter.

    Uses the ``compatible`` provider pointing at OpenRouter.
    Suitable for accessing many models through a single endpoint.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "https://openrouter.ai/api/v1",
                "model": "anthropic/claude-3-haiku",
                "supports_tools": True,
                "supports_streaming": False,
            },
            api_key_secret_ref="env:OPENROUTER_API_KEY",
        ),
    ]


def mistral_preset() -> list[ProviderSpec]:
    """Preset for Mistral AI.

    Uses the ``compatible`` provider pointing at the Mistral API.
    Suitable for European deployments.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "https://api.mistral.ai/v1",
                "model": "mistral-large-2402",
                "supports_tools": True,
                "supports_streaming": False,
            },
            api_key_secret_ref="env:MISTRAL_API_KEY",
        ),
    ]


def deepseek_preset() -> list[ProviderSpec]:
    """Preset for DeepSeek.

    Uses the ``compatible`` provider pointing at the DeepSeek API.
    Suitable for cost-effective coding assistance.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "https://api.deepseek.com/v1",
                "model": "deepseek-coder-v2",
                "supports_tools": True,
                "supports_streaming": False,
            },
            api_key_secret_ref="env:DEEPSEEK_API_KEY",
        ),
    ]


def production_preset(environment: Environment = Environment.PRODUCTION) -> list[ProviderSpec]:
    """Preset for production deployment.

    Uses a combination of OpenAI and Anthropic providers with model
    allowlisting. This preset satisfies the production guardrails in
    ``config/validation.py``.

    Args:
        environment: Current environment.

    Returns:
        A list of ProviderSpec objects.
    """
    if environment is Environment.PRODUCTION:
        return [
            ProviderSpec(
                provider_id="openai",
                enabled=True,
                api_key_secret_ref="env:OPENAI_API_KEY",
            ),
            ProviderSpec(
                provider_id="anthropic",
                enabled=True,
                api_key_secret_ref="env:ANTHROPIC_API_KEY",
            ),
        ]
    else:
        # Development preset
        return [
            ProviderSpec(
                provider_id="openai",
                enabled=True,
                api_key_secret_ref="env:OPENAI_API_KEY",
            ),
            ProviderSpec(
                provider_id="anthropic",
                enabled=True,
                api_key_secret_ref="env:ANTHROPIC_API_KEY",
            ),
            ProviderSpec(
                provider_id="compatible",
                enabled=True,
                compatible={
                    "api_base": "http://localhost:11434",
                    "model": "llama3",
                    "supports_tools": True,
                    "supports_streaming": True,
                },
            ),
        ]


def development_preset() -> list[ProviderSpec]:
    """Preset for development deployment.

    Uses a combination of providers suitable for development and testing.
    Includes local Ollama as a fallback.

    Returns:
        A list of ProviderSpec objects.
    """
    return [
        ProviderSpec(
            provider_id="openai",
            enabled=True,
            api_key_secret_ref="env:OPENAI_API_KEY",
        ),
        ProviderSpec(
            provider_id="anthropic",
            enabled=True,
            api_key_secret_ref="env:ANTHROPIC_API_KEY",
        ),
        ProviderSpec(
            provider_id="compatible",
            enabled=True,
            compatible={
                "api_base": "http://localhost:11434",
                "model": "llama3",
                "supports_tools": True,
                "supports_streaming": True,
            },
        ),
    ]


def get_preset(name: str, environment: Environment = Environment.PRODUCTION) -> list[ProviderSpec]:
    """Get a preset by name.

    Args:
        name: The preset name. Supported names: "ollama", "vllm", "groq",
            "together", "openrouter", "mistral", "deepseek",
            "production", "development".
        environment: Current environment (used by "production" and "development").

    Returns:
        A list of ProviderSpec objects.

    Raises:
        ValueError: If the preset name is not recognized.
    """
    presets = {
        "ollama": ollama_preset,
        "vllm": vllm_preset,
        "groq": groq_preset,
        "together": together_preset,
        "openrouter": openrouter_preset,
        "mistral": mistral_preset,
        "deepseek": deepseek_preset,
        "production": lambda env: production_preset(env),
        "development": development_preset,
    }
    
    if name not in presets:
        raise ValueError(
            f"unknown preset: {name}",
            details={"supported": sorted(presets.keys())},
        )
    
    return presets[name](environment)
