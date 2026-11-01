"""TLS/mTLS client and server security configuration.

Transport security is configuration-driven through
:class:`mas_orchestrator.config.enums.TransportSecurity`, but two rules are
enforced unconditionally, because a misconfiguration here is a full
credential-and-data exposure rather than a degraded feature:

1. ``insecure`` is **refused in staging and production**. There is no override
   flag; the only way to run insecure is to run in development/test.
2. ``mtls`` requires *both* a client certificate and a key. Requesting mTLS and
   supplying only one is a configuration error, not a silent downgrade to TLS.

Certificate material is never accepted inline: configuration supplies file
paths (or secret references resolved by
:mod:`mas_orchestrator.security.credentials`), so private keys never live in a
settings object that might be logged.
"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

import grpc

from mas_orchestrator.config.enums import Environment, TransportSecurity
from mas_orchestrator.contracts.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = [
    "INSECURE_ALLOWED_ENVIRONMENTS",
    "ClientTransportConfig",
    "ServerTransportConfig",
    "build_channel_credentials",
    "build_server_credentials",
    "validate_transport_security",
]

#: Environments where plaintext transport is tolerable.
INSECURE_ALLOWED_ENVIRONMENTS: Final[frozenset[Environment]] = frozenset(
    {Environment.DEVELOPMENT, Environment.TEST}
)


@dataclass(frozen=True, slots=True)
class ClientTransportConfig:
    """Everything needed to build client-side channel credentials.

    Attributes:
        security: The requested transport security mode.
        environment: The running environment, which gates ``insecure``.
        ca_bundle_path: Path to a PEM bundle of trusted roots. ``None`` means
            "use the system trust store", which is correct when the platform
            CA is installed at the OS level.
        client_certificate_path: Client certificate for mTLS.
        client_key_path: Client private key for mTLS.
    """

    security: TransportSecurity = TransportSecurity.TLS
    environment: Environment = Environment.DEVELOPMENT
    ca_bundle_path: str | None = None
    client_certificate_path: str | None = None
    client_key_path: str | None = None


@dataclass(frozen=True, slots=True)
class ServerTransportConfig:
    """Everything needed to build server-side credentials.

    Attributes:
        security: The requested transport security mode.
        environment: The running environment, which gates ``insecure``.
        certificate_chain_path: PEM certificate chain for the server.
        private_key_path: PEM private key for the server.
        ca_bundle_path: PEM bundle used to verify client certificates (mTLS).
        require_client_certificate: When ``True``, clients must present a
            certificate that chains to ``ca_bundle_path``.
    """

    security: TransportSecurity = TransportSecurity.TLS
    environment: Environment = Environment.DEVELOPMENT
    certificate_chain_path: str | None = None
    private_key_path: str | None = None
    ca_bundle_path: str | None = None
    require_client_certificate: bool = False


def validate_transport_security(
    security: TransportSecurity,
    environment: Environment,
    *,
    direction: str,
) -> None:
    """Refuses a transport configuration that would be unsafe.

    Args:
        security: The requested mode.
        environment: The running environment.
        direction: ``"client"`` or ``"server"``, used only to make the error
            message actionable.

    Raises:
        ConfigurationError: If ``insecure`` is requested outside
            development/test. Nothing else is refused here; the remaining
            checks are material-availability checks performed by the builders.
    """
    if security is TransportSecurity.INSECURE and environment not in INSECURE_ALLOWED_ENVIRONMENTS:
        raise ConfigurationError(
            "insecure transport is not permitted outside development/test",
            details={
                "direction": direction,
                "environment": str(environment),
                "security": str(security),
                "allowed_environments": sorted(str(e) for e in INSECURE_ALLOWED_ENVIRONMENTS),
            },
        )


def _read_pem(path: str, *, label: str) -> bytes:
    """Reads a PEM file, converting OS errors into configuration errors.

    Args:
        path: Filesystem path to the PEM file.
        label: What the file is, for the error message.

    Raises:
        ConfigurationError: If the file is missing or unreadable. The file
            contents are never included in the error.
    """
    candidate = pathlib.Path(path)
    try:
        data = candidate.read_bytes()
    except OSError as error:
        raise ConfigurationError(
            f"{label} could not be read",
            details={"label": label, "errno": error.errno},
        ) from error

    if not data.strip():
        raise ConfigurationError(
            f"{label} is empty",
            details={"label": label},
        )
    return data


def build_channel_credentials(config: ClientTransportConfig) -> grpc.ChannelCredentials | None:
    """Builds client channel credentials, or ``None`` for plaintext.

    Args:
        config: The client transport configuration.

    Returns:
        gRPC channel credentials for TLS/mTLS, or ``None`` when plaintext is
        explicitly configured **and** permitted by the environment.

    Raises:
        ConfigurationError: If ``insecure`` is not permitted in this
            environment, if the CA bundle is configured but unreadable, or if
            mTLS is requested without both certificate and key.

    Note:
        For mTLS the client key is read from disk on every call so that a
        rotated credential is picked up on reconnect without a process
        restart.
    """
    validate_transport_security(config.security, config.environment, direction="client")

    if config.security is TransportSecurity.INSECURE:
        return None

    root_certificates: bytes | None = None
    if config.ca_bundle_path:
        root_certificates = _read_pem(config.ca_bundle_path, label="CA bundle")

    if config.security is TransportSecurity.MTLS:
        if not config.client_certificate_path or not config.client_key_path:
            raise ConfigurationError(
                "mTLS requires both a client certificate and a client key",
                details={
                    "certificate_configured": bool(config.client_certificate_path),
                    "key_configured": bool(config.client_key_path),
                },
            )
        certificate_chain = _read_pem(config.client_certificate_path, label="client certificate")
        private_key = _read_pem(config.client_key_path, label="client key")
        return grpc.ssl_channel_credentials(
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )

    return grpc.ssl_channel_credentials(root_certificates=root_certificates)


def build_server_credentials(config: ServerTransportConfig) -> grpc.ServerCredentials | None:
    """Builds server credentials, or ``None`` for plaintext.

    Args:
        config: The server transport configuration.

    Returns:
        gRPC server credentials for TLS/mTLS, or ``None`` when plaintext is
        explicitly configured and permitted by the environment.

    Raises:
        ConfigurationError: If ``insecure`` is not permitted, if TLS material
            is missing or unreadable, or if client certificates are required
            without a CA bundle to verify them against.
    """
    validate_transport_security(config.security, config.environment, direction="server")

    if config.security is TransportSecurity.INSECURE:
        return None

    if not config.certificate_chain_path or not config.private_key_path:
        raise ConfigurationError(
            "TLS requires both a server certificate chain and a private key",
            details={
                "certificate_configured": bool(config.certificate_chain_path),
                "key_configured": bool(config.private_key_path),
            },
        )

    certificate_chain = _read_pem(config.certificate_chain_path, label="server certificate chain")
    private_key = _read_pem(config.private_key_path, label="server private key")

    if config.security is TransportSecurity.MTLS or config.require_client_certificate:
        if not config.ca_bundle_path:
            raise ConfigurationError(
                "client certificate verification requires a CA bundle",
                details={"reason": "missing_ca_bundle"},
            )
        root_certificates = _read_pem(config.ca_bundle_path, label="client CA bundle")
        return grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=[(private_key, certificate_chain)],
            root_certificates=root_certificates,
            require_client_auth=True,
        )

    return grpc.ssl_server_credentials(
        private_key_certificate_chain_pairs=[(private_key, certificate_chain)]
    )


def keepalive_options(
    *,
    interval_seconds: float,
    timeout_seconds: float | None = None,
) -> Sequence[tuple[str, int]]:
    """Builds gRPC keepalive channel options.

    Args:
        interval_seconds: How often to send a keepalive ping on an idle
            channel. ``0`` disables keepalive entirely, which is what a
            caller wants in tests where pings only add noise.
        timeout_seconds: How long to wait for a ping ack before declaring the
            channel dead. Defaults to a quarter of the interval, bounded to a
            minimum of one second.

    Returns:
        Channel option pairs suitable for ``grpc.aio.secure_channel``.

    Raises:
        ConfigurationError: If the interval is negative, which would be
            interpreted by gRPC as "ping as fast as possible".
    """
    if interval_seconds < 0:
        raise ConfigurationError(
            "keepalive interval must not be negative",
            details={"interval_seconds": interval_seconds},
        )

    if interval_seconds == 0:
        return ()

    interval_ms = int(interval_seconds * 1000)
    if timeout_seconds is None:
        timeout_ms = max(1000, interval_ms // 4)
    else:
        if timeout_seconds <= 0:
            raise ConfigurationError(
                "keepalive timeout must be positive",
                details={"timeout_seconds": timeout_seconds},
            )
        timeout_ms = int(timeout_seconds * 1000)

    return (
        ("grpc.keepalive_time_ms", interval_ms),
        ("grpc.keepalive_timeout_ms", timeout_ms),
        # Allow pings on an idle channel, which is the whole point here: the
        # Rust connection may sit idle between executions.
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    )
