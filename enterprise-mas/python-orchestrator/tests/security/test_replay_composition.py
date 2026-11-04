"""Replay-store configuration and production composition regressions."""

from __future__ import annotations

import pytest

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.config.settings import SecuritySettings, Settings
from mas_orchestrator.config.validation import production_ready_settings, validate_settings
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.main import _internal_replay_guard
from mas_orchestrator.security.internal_replay import InMemoryReplayGuard, PostgresReplayGuard


def test_production_validation_requires_a_replay_store_secret_reference() -> None:
    settings = production_ready_settings()
    absent = settings.model_copy(
        update={
            "security": settings.security.model_copy(
                update={"internal_replay_database_url_ref": None}
            )
        }
    )
    assert "security.internal_replay_database_url_ref" in {
        violation.field for violation in validate_settings(absent)
    }
    with pytest.raises(ConfigurationError):
        _internal_replay_guard(absent)


def test_staging_cannot_use_process_local_replay_guard() -> None:
    with pytest.raises(ConfigurationError):
        _internal_replay_guard(Settings(environment=Environment.STAGING))


def test_development_can_use_process_local_guard() -> None:
    assert isinstance(_internal_replay_guard(Settings()), InMemoryReplayGuard)


def test_configured_secret_ref_produces_postgres_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAS_TEST_REPLAY_COMPOSITION_DSN", "postgresql://localhost/replay-test")
    settings = Settings(
        security=SecuritySettings(
            internal_replay_database_url_ref="env:MAS_TEST_REPLAY_COMPOSITION_DSN"
        )
    )
    assert isinstance(_internal_replay_guard(settings), PostgresReplayGuard)
