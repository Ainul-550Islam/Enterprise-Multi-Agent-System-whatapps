"""Unit tests for TLS/mTLS transport configuration.

The unsafe-configuration guards are the point of these tests: plaintext in
production and half-configured mTLS must be impossible, not merely discouraged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from mas_orchestrator.config.enums import Environment, TransportSecurity
from mas_orchestrator.contracts.errors import ConfigurationError

if TYPE_CHECKING:
    import pathlib

from mas_orchestrator.security.transport import (
    INSECURE_ALLOWED_ENVIRONMENTS,
    ClientTransportConfig,
    ServerTransportConfig,
    build_channel_credentials,
    build_server_credentials,
    keepalive_options,
    validate_transport_security,
)

PEM = b"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"
KEY = b"-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----\n"


def _pem(tmp_path: pathlib.Path, name: str, data: bytes = PEM) -> str:
    path = tmp_path / name
    path.write_bytes(data)
    return str(path)


class TestValidateTransportSecurity:
    """The production plaintext guard."""

    @pytest.mark.parametrize("environment", [Environment.DEVELOPMENT, Environment.TEST])
    def test_insecure_allowed_in_dev_and_test(self, environment: Environment) -> None:
        validate_transport_security(TransportSecurity.INSECURE, environment, direction="client")

    @pytest.mark.parametrize("environment", [Environment.STAGING, Environment.PRODUCTION])
    def test_insecure_refused_in_staging_and_production(self, environment: Environment) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            validate_transport_security(TransportSecurity.INSECURE, environment, direction="client")
        assert "insecure" in str(excinfo.value).lower()

    def test_error_names_the_allowed_environments(self) -> None:
        with pytest.raises(ConfigurationError) as excinfo:
            validate_transport_security(
                TransportSecurity.INSECURE, Environment.PRODUCTION, direction="server"
            )
        assert excinfo.value.details["allowed_environments"]

    def test_tls_and_mtls_are_always_allowed(self) -> None:
        for security in (TransportSecurity.TLS, TransportSecurity.MTLS):
            validate_transport_security(security, Environment.PRODUCTION, direction="client")

    def test_allowed_environment_set_is_exactly_dev_and_test(self) -> None:
        assert {
            Environment.DEVELOPMENT,
            Environment.TEST,
        } == INSECURE_ALLOWED_ENVIRONMENTS


class TestBuildChannelCredentials:
    """Client-side credential construction."""

    def test_insecure_returns_none_in_development(self) -> None:
        config = ClientTransportConfig(
            security=TransportSecurity.INSECURE, environment=Environment.DEVELOPMENT
        )
        assert build_channel_credentials(config) is None

    def test_insecure_refused_in_production(self) -> None:
        config = ClientTransportConfig(
            security=TransportSecurity.INSECURE, environment=Environment.PRODUCTION
        )
        with pytest.raises(ConfigurationError):
            build_channel_credentials(config)

    def test_tls_without_ca_bundle_uses_system_roots(self) -> None:
        config = ClientTransportConfig(
            security=TransportSecurity.TLS, environment=Environment.PRODUCTION
        )
        assert build_channel_credentials(config) is not None

    def test_tls_with_ca_bundle(self, tmp_path: pathlib.Path) -> None:
        config = ClientTransportConfig(
            security=TransportSecurity.TLS,
            environment=Environment.PRODUCTION,
            ca_bundle_path=_pem(tmp_path, "ca.pem"),
        )
        assert build_channel_credentials(config) is not None

    def test_mtls_requires_both_certificate_and_key(self, tmp_path: pathlib.Path) -> None:
        """Half-configured mTLS must fail, not silently downgrade to TLS."""
        config = ClientTransportConfig(
            security=TransportSecurity.MTLS,
            environment=Environment.PRODUCTION,
            client_certificate_path=_pem(tmp_path, "cert.pem"),
        )
        with pytest.raises(ConfigurationError) as excinfo:
            build_channel_credentials(config)
        assert excinfo.value.details["key_configured"] is False

    def test_mtls_with_both_materials(self, tmp_path: pathlib.Path) -> None:
        config = ClientTransportConfig(
            security=TransportSecurity.MTLS,
            environment=Environment.PRODUCTION,
            client_certificate_path=_pem(tmp_path, "cert.pem"),
            client_key_path=_pem(tmp_path, "key.pem", KEY),
        )
        assert build_channel_credentials(config) is not None

    def test_unreadable_ca_bundle_is_refused(self) -> None:
        config = ClientTransportConfig(
            security=TransportSecurity.TLS,
            environment=Environment.PRODUCTION,
            ca_bundle_path="/nonexistent/ca.pem",
        )
        with pytest.raises(ConfigurationError):
            build_channel_credentials(config)

    def test_empty_pem_file_is_refused(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "empty.pem"
        path.write_text("   ", encoding="utf-8")
        config = ClientTransportConfig(
            security=TransportSecurity.TLS,
            environment=Environment.PRODUCTION,
            ca_bundle_path=str(path),
        )
        with pytest.raises(ConfigurationError):
            build_channel_credentials(config)

    def test_pem_contents_never_appear_in_errors(self, tmp_path: pathlib.Path) -> None:
        path = tmp_path / "empty.pem"
        path.write_text("   ", encoding="utf-8")
        config = ClientTransportConfig(
            security=TransportSecurity.TLS,
            environment=Environment.PRODUCTION,
            ca_bundle_path=str(path),
        )
        with pytest.raises(ConfigurationError) as excinfo:
            build_channel_credentials(config)
        assert "MIIB" not in str(excinfo.value)


class TestBuildServerCredentials:
    """Server-side credential construction."""

    def test_insecure_returns_none_in_test(self) -> None:
        config = ServerTransportConfig(
            security=TransportSecurity.INSECURE, environment=Environment.TEST
        )
        assert build_server_credentials(config) is None

    def test_tls_requires_certificate_and_key(self, tmp_path: pathlib.Path) -> None:
        config = ServerTransportConfig(
            security=TransportSecurity.TLS,
            environment=Environment.PRODUCTION,
            certificate_chain_path=_pem(tmp_path, "cert.pem"),
        )
        with pytest.raises(ConfigurationError):
            build_server_credentials(config)

    def test_tls_with_both_materials(self, tmp_path: pathlib.Path) -> None:
        config = ServerTransportConfig(
            security=TransportSecurity.TLS,
            environment=Environment.PRODUCTION,
            certificate_chain_path=_pem(tmp_path, "cert.pem"),
            private_key_path=_pem(tmp_path, "key.pem", KEY),
        )
        assert build_server_credentials(config) is not None

    def test_requiring_client_cert_needs_a_ca_bundle(self, tmp_path: pathlib.Path) -> None:
        """Verifying clients without a CA to verify against is impossible."""
        config = ServerTransportConfig(
            security=TransportSecurity.TLS,
            environment=Environment.PRODUCTION,
            certificate_chain_path=_pem(tmp_path, "cert.pem"),
            private_key_path=_pem(tmp_path, "key.pem", KEY),
            require_client_certificate=True,
        )
        with pytest.raises(ConfigurationError) as excinfo:
            build_server_credentials(config)
        assert excinfo.value.details["reason"] == "missing_ca_bundle"

    def test_mtls_with_ca_bundle(self, tmp_path: pathlib.Path) -> None:
        config = ServerTransportConfig(
            security=TransportSecurity.MTLS,
            environment=Environment.PRODUCTION,
            certificate_chain_path=_pem(tmp_path, "cert.pem"),
            private_key_path=_pem(tmp_path, "key.pem", KEY),
            ca_bundle_path=_pem(tmp_path, "ca.pem"),
        )
        assert build_server_credentials(config) is not None


class TestKeepaliveOptions:
    """Keepalive channel options."""

    def test_zero_interval_disables_keepalive(self) -> None:
        assert keepalive_options(interval_seconds=0) == ()

    def test_positive_interval_produces_options(self) -> None:
        options = dict(keepalive_options(interval_seconds=30.0))
        assert options["grpc.keepalive_time_ms"] == 30_000
        assert options["grpc.keepalive_timeout_ms"] == 7_500

    def test_explicit_timeout_is_used(self) -> None:
        options = dict(keepalive_options(interval_seconds=30.0, timeout_seconds=5.0))
        assert options["grpc.keepalive_timeout_ms"] == 5_000

    def test_pings_without_data_are_permitted(self) -> None:
        """An idle channel must still be pinged or a silent peer goes undetected."""
        options = dict(keepalive_options(interval_seconds=30.0))
        assert options["grpc.keepalive_permit_without_calls"] == 1

    def test_negative_interval_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            keepalive_options(interval_seconds=-1)

    def test_non_positive_timeout_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            keepalive_options(interval_seconds=30.0, timeout_seconds=0)


class TestConfigDefaults:
    """Defaults must be safe, not convenient."""

    def test_client_default_is_tls(self) -> None:
        assert ClientTransportConfig().security is TransportSecurity.TLS

    def test_client_default_environment_is_development(self) -> None:
        """Development is the only default that permits plaintext at all."""
        assert ClientTransportConfig().environment is Environment.DEVELOPMENT

    def test_server_default_is_tls(self) -> None:
        assert ServerTransportConfig().security is TransportSecurity.TLS
