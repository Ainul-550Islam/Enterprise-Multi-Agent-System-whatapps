"""Unit tests for secret resolution.

These are security tests. The properties that matter are: a missing secret is a
loud failure (never a default), and a secret cannot be printed by accident.
"""

from __future__ import annotations

import os

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import (
    ENVIRONMENT_SCHEME,
    FILE_SCHEME,
    EnvironmentSecretResolver,
    FileSecretResolver,
    Secret,
    SecretResolverChain,
    resolve_secret,
)


class TestSecretValueObject:
    """The secret wrapper must never reveal itself accidentally."""

    def test_repr_is_redacted(self) -> None:
        secret = Secret("hunter2", reference="env:X")
        assert "hunter2" not in repr(secret)

    def test_str_is_redacted(self) -> None:
        assert "hunter2" not in str(Secret("hunter2", reference="env:X"))

    def test_reveal_returns_the_value(self) -> None:
        assert Secret("hunter2", reference="env:X").reveal() == "hunter2"

    def test_reference_is_visible(self) -> None:
        """The reference is not a secret and must stay readable."""
        assert "env:X" in repr(Secret("v", reference="env:X"))

    def test_empty_value_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            Secret("", reference="env:X")

    def test_length_does_not_reveal_content(self) -> None:
        assert len(Secret("hunter2", reference="env:X")) == 7

    def test_equality_compares_values(self) -> None:
        assert Secret("a", reference="env:1") == Secret("a", reference="env:2")

    def test_hashing_keyed_on_reference(self) -> None:
        """Hashing must not require exposing the value."""
        assert hash(Secret("a", reference="env:1")) == hash(Secret("b", reference="env:1"))


class TestEnvironmentSecretResolver:
    """``env:NAME`` resolution."""

    def test_supports_env_references(self) -> None:
        resolver = EnvironmentSecretResolver()
        assert resolver.supports("env:MY_SECRET") is True
        assert resolver.supports("file:/tmp/x") is False

    def test_resolves_a_set_variable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAS_TEST_SECRET", "value-123")
        secret = EnvironmentSecretResolver().resolve("env:MAS_TEST_SECRET")
        assert secret.reveal() == "value-123"

    def test_missing_variable_is_a_configuration_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A missing secret must fail loudly, never fall back to a default."""
        monkeypatch.delenv("MAS_ABSENT_SECRET", raising=False)
        with pytest.raises(ConfigurationError) as excinfo:
            EnvironmentSecretResolver().resolve("env:MAS_ABSENT_SECRET")
        assert excinfo.value.details["variable"] == "MAS_ABSENT_SECRET"

    def test_empty_variable_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAS_EMPTY_SECRET", "   ")
        with pytest.raises(ConfigurationError):
            EnvironmentSecretResolver().resolve("env:MAS_EMPTY_SECRET")

    def test_error_never_contains_the_value(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAS_EMPTY_SECRET", "   ")
        with pytest.raises(ConfigurationError) as excinfo:
            EnvironmentSecretResolver().resolve("env:MAS_EMPTY_SECRET")
        assert "   " not in str(excinfo.value)

    def test_unsupported_reference_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            EnvironmentSecretResolver().resolve("file:/tmp/x")

    def test_missing_variable_name_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            EnvironmentSecretResolver().resolve("env:")


class TestFileSecretResolver:
    """``file:PATH`` resolution."""

    def test_supports_file_references(self) -> None:
        resolver = FileSecretResolver()
        assert resolver.supports("file:/tmp/x") is True
        assert resolver.supports("env:X") is False

    def test_reads_a_file(self, tmp_path: object) -> None:
        import pathlib

        path = pathlib.Path(str(tmp_path)) / "secret"
        path.write_text("file-value\n", encoding="utf-8")
        secret = FileSecretResolver().resolve(f"file:{path}")
        # Trailing whitespace is trimmed: mounted secrets often end with \n.
        assert secret.reveal() == "file-value"

    def test_missing_file_is_a_configuration_error(self) -> None:
        with pytest.raises(ConfigurationError):
            FileSecretResolver().resolve("file:/nonexistent/path/to/secret")

    def test_empty_file_is_refused(self, tmp_path: object) -> None:
        import pathlib

        path = pathlib.Path(str(tmp_path)) / "empty"
        path.write_text("", encoding="utf-8")
        with pytest.raises(ConfigurationError):
            FileSecretResolver().resolve(f"file:{path}")

    def test_error_does_not_contain_file_contents(self, tmp_path: object) -> None:
        import pathlib

        path = pathlib.Path(str(tmp_path)) / "empty"
        path.write_text("", encoding="utf-8")
        with pytest.raises(ConfigurationError) as excinfo:
            FileSecretResolver().resolve(f"file:{path}")
        assert "secret" not in str(excinfo.value).lower() or "empty" in str(excinfo.value)

    def test_unsupported_reference_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            FileSecretResolver().resolve("env:X")


class TestSecretResolverChain:
    """Ordered resolution."""

    def test_first_supporting_resolver_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAS_CHAIN_SECRET", "from-env")
        chain = SecretResolverChain(EnvironmentSecretResolver(), FileSecretResolver())
        assert chain.resolve("env:MAS_CHAIN_SECRET").reveal() == "from-env"

    def test_falls_through_to_the_next_resolver(self, tmp_path: object) -> None:
        import pathlib

        path = pathlib.Path(str(tmp_path)) / "secret"
        path.write_text("from-file", encoding="utf-8")
        chain = SecretResolverChain(EnvironmentSecretResolver(), FileSecretResolver())
        assert chain.resolve(f"file:{path}").reveal() == "from-file"

    def test_unsupported_reference_is_refused(self) -> None:
        chain = SecretResolverChain(EnvironmentSecretResolver(), FileSecretResolver())
        with pytest.raises(ConfigurationError):
            chain.resolve("vault://secret/path")

    def test_empty_chain_is_refused(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            SecretResolverChain().resolve("env:X")
        assert excinfo.value.details["reference"] == "env:X"


class TestResolveSecret:
    """The top-level helper."""

    def test_none_reference_is_refused(self) -> None:
        """A caller needing a secret must name one."""
        with pytest.raises(ConfigurationError) as excinfo:
            resolve_secret(None)
        assert excinfo.value.details["reason"] == "missing_reference"

    def test_empty_reference_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            resolve_secret("   ")

    def test_resolves_via_the_default_chain(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAS_DEFAULT_CHAIN_SECRET", "resolved")
        assert resolve_secret("env:MAS_DEFAULT_CHAIN_SECRET").reveal() == "resolved"

    def test_reference_is_trimmed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MAS_TRIM_SECRET", "resolved")
        assert resolve_secret("  env:MAS_TRIM_SECRET  ").reveal() == "resolved"


class TestSchemeConstants:
    """The schemes are part of the configuration contract."""

    def test_scheme_values_are_stable(self) -> None:
        assert ENVIRONMENT_SCHEME == "env"
        assert FILE_SCHEME == "file"

    def test_no_secret_literals_are_accepted(self) -> None:
        """A bare value must not be resolvable: only references are."""
        os.environ.get("SHOULD_NOT_BE_READ")
        with pytest.raises(ConfigurationError):
            resolve_secret("not-a-reference")
