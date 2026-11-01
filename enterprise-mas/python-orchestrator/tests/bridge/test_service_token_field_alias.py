"""Regression tests for the two service-token field spellings.

``security.service_token_ref`` is the field the bridge actually reads, but the
production startup checks demand ``service.service_token_secret_ref`` and tell
an operator to set exactly that. A deployment could therefore satisfy every
startup check and still die at composition time with "the agent bridge requires
a configured service token", where the field it was told to set was ignored.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.config.settings import (
    Environment,
    ProviderMode,
    ProviderSettings,
    SecuritySettings,
    ServiceSettings,
    Settings,
)
from mas_orchestrator.services.composition import build_identity_resolver

_TOKEN = "svc-token-0123456789abcdef0123456789"


def _settings(security: SecuritySettings, service: ServiceSettings) -> Settings:
    """Builds settings holding only the token configuration under test.

    Args:
        security: The security section.
        service: The service section.

    Returns:
        A settings object wired for live composition.
    """
    return Settings(
        environment=Environment.PRODUCTION,
        providers=ProviderSettings(
            mode=ProviderMode.LIVE, enabled_providers=("openai",), allowed_models=()
        ),
        security=security,
        service=service,
    )


@pytest.mark.parametrize(
    "security_ref",
    [f"env:{_TOKEN}"],
)
def test_canonical_security_field_resolves(
    monkeypatch: pytest.MonkeyPatch, security_ref: str
) -> None:
    """The canonical ``security.service_token_ref`` still works.

    Args:
        monkeypatch: Used to set the backing environment variable.
        security_ref: The reference configured on the security section.
    """
    monkeypatch.setenv(_TOKEN, "resolved-token-value-000000000000")
    resolver = build_identity_resolver(
        _settings(SecuritySettings(service_token_ref=security_ref), ServiceSettings())
    )
    assert resolver is not None


def test_service_field_is_honoured_when_canonical_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token configured only on the service section is not ignored.

    This is the field the production startup check names in its remediation, so
    setting it must be sufficient on its own.

    Args:
        monkeypatch: Used to set the backing environment variable.
    """
    monkeypatch.setenv(_TOKEN, "resolved-token-value-000000000000")
    resolver = build_identity_resolver(
        _settings(
            SecuritySettings(service_token_ref=None),
            ServiceSettings(service_token_secret_ref=f"env:{_TOKEN}"),
        )
    )
    assert resolver is not None


def test_no_token_configured_still_refuses_to_compose() -> None:
    """With neither field set, composition refuses as before."""
    with pytest.raises(Exception) as error:
        build_identity_resolver(
            _settings(SecuritySettings(service_token_ref=None), ServiceSettings())
        )
    assert "service token" in str(error.value)
