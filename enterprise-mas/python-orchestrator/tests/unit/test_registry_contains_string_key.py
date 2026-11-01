"""Regression tests for ``ModelRegistry.__contains__`` with string keys.

The routing allowlist is configured as ``provider/model`` strings, and
``_allowlist_for`` guards every allowlisted key with ``key not in registry``.
A string has no ``.key`` attribute, so a membership test that only understood
``ModelRef`` objects silently reported every allowlisted model as unknown and
refused to start with "allowlisted models are not offered by any enabled
provider" — while listing the very model it called unknown under "available".
"""

from __future__ import annotations

import pytest

from mas_orchestrator.config.settings import load_settings
from mas_orchestrator.models.registry import ModelRegistry
from mas_orchestrator.models.types import ModelRef
from mas_orchestrator.providers.factory import build_providers
from mas_orchestrator.services.composition import build_provider_specs

_OPENAI_KEY = "sk-test-not-a-real-key-000000000000"


@pytest.fixture()
def registry(monkeypatch: pytest.MonkeyPatch) -> ModelRegistry:
    """A registry populated by the real composition root for one provider.

    Args:
        monkeypatch: Used to configure live OpenAI construction.

    Returns:
        The populated registry.
    """
    monkeypatch.setenv("MAS_ORCH_PROVIDERS__MODE", "live")
    monkeypatch.setenv("MAS_ORCH_PROVIDERS__ENABLED_PROVIDERS", '["openai"]')
    monkeypatch.setenv("MAS_OPENAI_API_KEY", _OPENAI_KEY)
    settings = load_settings()
    built = ModelRegistry()
    build_providers(
        build_provider_specs(settings), environment=settings.environment, registry=built
    )
    return built


def test_allowlist_string_is_recognised(registry: ModelRegistry) -> None:
    """A configured ``provider/model`` string is servable.

    Args:
        registry: The populated registry.
    """
    assert "openai/gpt-4o-mini" in registry


def test_model_ref_object_is_still_recognised(registry: ModelRegistry) -> None:
    """The original object-based contract keeps working.

    Args:
        registry: The populated registry.
    """
    assert ModelRef(provider="openai", model="gpt-4o-mini") in registry


@pytest.mark.parametrize(
    "candidate",
    ["openai/does-not-exist", "anthropic/gpt-4o-mini", "gpt-4o-mini", "nonsense", None, 42],
)
def test_unservable_candidates_are_refused(registry: ModelRegistry, candidate: object) -> None:
    """Unknown, unqualified, and non-model objects are never servable.

    Args:
        registry: The populated registry.
        candidate: The value to test for membership.
    """
    assert candidate not in registry
