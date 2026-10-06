"""Secure gRPC channel creation, lifecycle and shutdown.

A channel is a long-lived, expensive resource: it holds a connection pool, TLS
sessions and HTTP/2 state. This module owns its lifecycle so that:

* **Creation is uniform.** Every channel gets the same keepalive, message-size
  and TLS settings, so there is no code path that accidentally builds an
  unauthenticated or unbounded channel.
* **Shutdown is graceful and bounded.** :meth:`ManagedChannel.close` drains
  in-flight calls for a configurable grace period and then force-closes, so a
  hung call cannot wedge process shutdown forever.
* **Readiness is observable.** :meth:`ManagedChannel.wait_until_ready` gives the
  startup path a way to fail fast (or to wait, when configured) instead of
  letting the first real execution discover a dead connection.

Reconnection is deliberately *not* implemented here. gRPC channels reconnect
automatically; adding a bespoke reconnect loop would compete with the library's
own backoff. What this module adds is observability of the connection state.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Final

import grpc

from mas_orchestrator.config.enums import Environment, TransportSecurity
from mas_orchestrator.contracts.errors import ConfigurationError, DependencyUnavailableError
from mas_orchestrator.grpc.errors import STATUS_TO_CODE, error_from_code
from mas_orchestrator.security.transport import (
    ClientTransportConfig,
    build_channel_credentials,
    keepalive_options,
)

__all__ = [
    "DEFAULT_MAX_MESSAGE_BYTES",
    "ChannelConfig",
    "ManagedChannel",
    "create_channel",
]


#: Maximum gRPC message size accepted in either direction.
#:
#: Sized for a large prompt plus retrieved context, but bounded: an unbounded
#: channel turns a hostile or buggy caller into a memory-exhaustion vector.
DEFAULT_MAX_MESSAGE_BYTES: Final[int] = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ChannelConfig:
    """Everything required to build a channel to the Rust Core.

    Attributes:
        endpoint: ``host:port`` target. Must be non-empty.
        transport: Requested transport security.
        ca_bundle_path: Optional PEM root bundle for TLS verification.
        client_certificate_path: Client certificate for mTLS.
        client_key_path: Client key for mTLS.
        keepalive_interval_seconds: Idle ping interval; ``0`` disables.
        max_message_bytes: Maximum message size in each direction.
        connect_timeout_seconds: How long :meth:`ManagedChannel.wait_until_ready`
            will wait before declaring the peer unreachable.
    """

    endpoint: str = "127.0.0.1:50051"
    transport: TransportSecurity = TransportSecurity.TLS
    ca_bundle_path: str | None = None
    client_certificate_path: str | None = None
    client_key_path: str | None = None
    keepalive_interval_seconds: float = 30.0
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES
    connect_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        """Validates numeric bounds so gRPC never receives a nonsense option.

        Raises:
            ConfigurationError: If the endpoint is empty, or if any numeric
                option is out of range.
        """
        if not self.endpoint.strip():
            raise ConfigurationError(
                "channel endpoint must not be empty",
                details={"field": "endpoint"},
            )
        if self.max_message_bytes < 1024:
            raise ConfigurationError(
                "max_message_bytes is too small to carry a request",
                details={"max_message_bytes": self.max_message_bytes},
            )
        if self.connect_timeout_seconds <= 0:
            raise ConfigurationError(
                "connect_timeout_seconds must be positive",
                details={"connect_timeout_seconds": self.connect_timeout_seconds},
            )
        if self.keepalive_interval_seconds < 0:
            raise ConfigurationError(
                "keepalive_interval_seconds must not be negative",
                details={"keepalive_interval_seconds": self.keepalive_interval_seconds},
            )


def _channel_options(config: ChannelConfig) -> list[tuple[str, int]]:
    """Builds the full gRPC channel option list for *config*."""
    options: list[tuple[str, int]] = [
        ("grpc.max_send_message_length", config.max_message_bytes),
        ("grpc.max_receive_message_length", config.max_message_bytes),
    ]
    options.extend(keepalive_options(interval_seconds=config.keepalive_interval_seconds))
    return options


def create_channel(
    config: ChannelConfig,
    *,
    environment: Environment = Environment.PRODUCTION,
) -> grpc.aio.Channel:
    """Creates a channel to the Rust Core.

    Args:
        config: Channel configuration.
        environment: The running environment, forwarded to the transport layer
            so that ``insecure`` can be refused outside development/test. The
            default is the strictest environment, so a caller that forgets to
            pass it cannot accidentally obtain a plaintext channel.

    Returns:
        A ready-to-use ``grpc.aio.Channel``.

    Raises:
        ConfigurationError: If the configuration is invalid or the requested
            transport is not permitted for the environment.
    """
    resolved_environment = (
        environment if isinstance(environment, Environment) else Environment.PRODUCTION
    )

    credentials = build_channel_credentials(
        ClientTransportConfig(
            security=config.transport,
            environment=resolved_environment,
            ca_bundle_path=config.ca_bundle_path,
            client_certificate_path=config.client_certificate_path,
            client_key_path=config.client_key_path,
        )
    )
    options = _channel_options(config)

    if credentials is None:
        # Reached only when the environment permits plaintext.
        return grpc.aio.insecure_channel(config.endpoint, options=options)
    return grpc.aio.secure_channel(config.endpoint, credentials, options=options)


class ManagedChannel:
    """Owns a channel's lifecycle with a bounded, graceful shutdown.

    Args:
        channel: The underlying channel this object manages.
        grace_seconds: How long to let in-flight calls drain during
            :meth:`close` before cancelling them and closing immediately.

    The channel is intentionally *not* created here: creation is synchronous
    and can fail on configuration, while this object is about lifecycle.
    Keeping them separate lets tests inject a fake channel.
    """

    def __init__(self, channel: grpc.aio.Channel, *, grace_seconds: float = 10.0) -> None:
        if grace_seconds < 0:
            raise ConfigurationError(
                "grace_seconds must not be negative",
                details={"grace_seconds": grace_seconds},
            )
        self._channel = channel
        self._grace_seconds = grace_seconds
        self._closed = False

    @property
    def channel(self) -> grpc.aio.Channel:
        """The underlying channel, for building stubs."""
        return self._channel

    @property
    def is_closed(self) -> bool:
        """Whether :meth:`close` has already completed."""
        return self._closed

    async def wait_until_ready(self, timeout_seconds: float) -> None:
        """Waits for the channel to become ready.

        Args:
            timeout_seconds: Maximum wait.

        Raises:
            DependencyUnavailableError: If the channel did not become ready in
                time, or if the wait was cancelled (cancellation means shutdown
                was requested, which is equivalent to the peer being
                unavailable from the caller's perspective).
        """
        try:
            await asyncio.wait_for(self._channel.channel_ready(), timeout=timeout_seconds)
        except TimeoutError as error:
            raise DependencyUnavailableError(
                "rust core channel did not become ready in time",
                details={
                    "timeout_seconds": timeout_seconds,
                    "endpoint": self._channel_target(),
                },
            ) from error
        except asyncio.CancelledError as error:
            raise DependencyUnavailableError(
                "rust core channel readiness wait was cancelled",
                details={"endpoint": self._channel_target()},
            ) from error

    def _channel_target(self) -> str:
        """Best-effort description of the channel target for diagnostics."""
        target = getattr(self._channel, "_channel", None)
        return str(getattr(target, "target", lambda: "unknown")())

    async def close(self) -> None:
        """Closes the channel, letting in-flight calls drain first.

        Draining is bounded by ``grace_seconds``: after that, remaining calls
        are cancelled and the channel closes. This guarantees the process can
        always shut down, which matters because a hung gRPC call would
        otherwise hold the service open past its deployment grace period.

        Calling :meth:`close` more than once is a no-op, so shutdown paths can
        be idempotent.
        """
        if self._closed:
            return
        self._closed = True
        # None => cancel immediately after the grace period; the value is
        # deliberately not "wait forever".
        await self._channel.close(grace=self._grace_seconds)


def classify_channel_error(error: BaseException) -> DependencyUnavailableError:
    """Normalises a transport-level failure into a typed error.

    Args:
        error: Any exception raised while using the channel.

    Returns:
        A :class:`DependencyUnavailableError`, or the more specific typed error
        when the failure is already a gRPC status we understand. This keeps the
        "Rust is unreachable" classification consistent regardless of whether
        the failure surfaced as a timeout, a connection reset, or a status.
    """
    if isinstance(error, grpc.aio.AioRpcError):
        code = STATUS_TO_CODE.get(error.code())
        if code is not None:
            typed = error_from_code(code, f"rust core call failed: {error.code().name}")
            # Transport-level codes mean the peer, not the request, is at fault.
            return typed  # type: ignore[return-value]

    if isinstance(error, DependencyUnavailableError):
        return error

    return DependencyUnavailableError(
        "rust core transport failure",
        details={"error_type": type(error).__name__},
    )
