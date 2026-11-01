"""Environment overrides must be able to configure collection-valued settings.

Pydantic v2 coerces an environment string to a scalar field type but refuses it
for ``tuple[str, ...]``: ``"openai"``, ``'["openai"]'`` and
``"openai,anthropic"`` are all rejected with ``tuple_type``. ``_env_overrides``
used to hand the raw string straight to the model and document that "Pydantic
coerces them", so **every list-valued setting was unreachable from the
environment**.

That is not a stylistic problem. A container deployment has no other way to name
a model provider, so the orchestrator image could not be started at all: it died
at boot with ``no model providers are enabled`` no matter what was set.

These tests pin the decoding so the regression cannot return.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


from mas_orchestrator.config.settings import load_settings


def _load(monkeypatch: pytest.MonkeyPatch, **values: str) -> object:
    """Loads settings with the given ``MAS_ORCH_*`` variables set.

    Args:
        monkeypatch: Pytest fixture used to set and restore the environment.
        **values: Environment variable names mapped to their raw values.

    Returns:
        The loaded settings.
    """
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    return load_settings()


def test_a_json_array_configures_a_tuple_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    """The JSON form is the unambiguous spelling for a list."""
    settings = _load(monkeypatch, MAS_ORCH_PROVIDERS__ENABLED_PROVIDERS='["openai"]')
    assert settings.providers.enabled_providers == ("openai",)


def test_a_comma_separated_value_configures_a_tuple_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The shell-conventional comma form works too.

    An operator writing a plain comma-separated list should not have to discover
    that this one setting demands JSON.
    """
    settings = _load(
        monkeypatch,
        MAS_ORCH_PROVIDERS__ENABLED_PROVIDERS="openai,anthropic",
    )
    assert settings.providers.enabled_providers == ("openai", "anthropic")


def test_a_single_name_is_still_a_one_element_tuple(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bare provider name is a list of one, not a bare string."""
    settings = _load(monkeypatch, MAS_ORCH_PROVIDERS__ENABLED_PROVIDERS="openai")
    assert settings.providers.enabled_providers == ("openai",)


def test_scalar_settings_are_left_to_pydantic(monkeypatch: pytest.MonkeyPatch) -> None:
    """Decoding must not disturb ordinary scalars."""
    settings = _load(monkeypatch, MAS_ORCH_SERVICE__BIND_PORT="19200")
    assert settings.service.bind_port == 19200


def test_a_value_that_only_looks_structured_survives(monkeypatch: pytest.MonkeyPatch) -> None:
    """A secret starting with ``[`` must not break startup.

    Falling back to the raw string is deliberate: a value that happens to open
    with a bracket is far more likely to be a credential than malformed JSON.
    """
    settings = _load(monkeypatch, MAS_ORCH_SECURITY__SECRET_ENV_PREFIX="[not-json")
    assert settings.security.secret_env_prefix == "[not-json"
