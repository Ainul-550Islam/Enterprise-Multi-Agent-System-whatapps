# PURPOSE:
# gRPC server bootstrap for the AgentBridgeApi surface.
# Owns the listen socket, the graceful-stop sequence wired into the platform's
# ShutdownCoordinator, and the message-size bounds. It performs no AI work and
# makes no authorization decision of its own.

"""Server bootstrap for the inbound Rust → Python gRPC surface.

The orchestrator exposes exactly one service to Rust on this port:
``AgentBridgeApi``. Keeping the surface narrow is deliberate — every additional
service is another place a caller could arrive without the identity checks the
servicer enforces.

Three properties this module is responsible for:

* **Bounded messages.** Both directions are capped. An unbounded inbound
  message is a memory-exhaustion primitive, and an unbounded outbound one can
  wedge the Rust client's decoder.
* **Orderly shutdown.** Stopping goes through the same
  :class:`~mas_orchestrator.services.lifecycle.ShutdownCoordinator` as the rest
  of the process, so an in-flight agent run is drained rather than severed. The
  coordinator reports what was abandoned instead of the process exiting as
  though nothing was left behind.
* **No silent bind fallback.** Binding to the configured address either works
  or raises. There is no "try the port, else pick another" path, because a
  service listening somewhere other than where its clients are configured to
  look is worse than one that fails to start.
"""

from __future__ import annotations

import logging
from concurrent import futures
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.grpc.generated import mas_api_pb2_grpc

if TYPE_CHECKING:
    from grpc import aio

    from mas_orchestrator.config.settings import ServiceSettings
    from mas_orchestrator.grpc.agent_bridge_servicer import AgentBridgeServicer
    from mas_orchestrator.services.lifecycle import ShutdownCoordinator

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_MAX_CONCURRENT_RPCS",
    "GRACEFUL_STOP_SECONDS",
    "GrpcServer",
    "GrpcServerConfig",
]

#: Maximum in-flight RPCs handled concurrently. Bounds the number of agent runs
#: the process will hold in memory at once; beyond this, gRPC applies back
#: pressure rather than the process growing without limit.
DEFAULT_MAX_CONCURRENT_RPCS: Final[int] = 64

#: Grace period handed to ``server.stop()``. In-flight runs get this long to
#: finish after the drain signal before the server severs them.
GRACEFUL_STOP_SECONDS: Final[float] = 5.0

#: Ceiling on an inbound message. Matches the platform's prompt bound so an
#: oversized request is refused at the transport rather than parsed first.
DEFAULT_MAX_RECEIVE_BYTES: Final[int] = 4 * 1024 * 1024

#: Ceiling on an outbound message.
DEFAULT_MAX_SEND_BYTES: Final[int] = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class GrpcServerConfig:
    """Transport settings for the bridge server.

    Attributes:
        host: Bind address. Defaults to loopback: this surface is internal and
            must never be reachable from outside the deployment network.
        port: Bind port. ``0`` asks the kernel for a free port, which is what
            tests use; :attr:`GrpcServer.bound_port` reports what was chosen.
            It is deliberately *not* rejected here: distinguishing "invalid"
            from "pick one" by magic value would be guesswork, and a
            production operator who configures ``0`` gets a random port
            reported in the startup log rather than a silent failure.
        max_concurrent_rpcs: Ceiling on concurrently served calls.
        graceful_stop_seconds: Grace period for in-flight calls on stop.
        max_receive_bytes: Inbound message ceiling.
        max_send_bytes: Outbound message ceiling.
    """

    host: str = "127.0.0.1"
    port: int = 9100
    max_concurrent_rpcs: int = DEFAULT_MAX_CONCURRENT_RPCS
    graceful_stop_seconds: float = GRACEFUL_STOP_SECONDS
    max_receive_bytes: int = DEFAULT_MAX_RECEIVE_BYTES
    max_send_bytes: int = DEFAULT_MAX_SEND_BYTES

    def __post_init__(self) -> None:
        from mas_orchestrator.contracts.errors import ConfigurationError

        if not self.host.strip():
            raise ConfigurationError("grpc bind host must not be empty", details={"field": "host"})
        if not 0 <= self.port <= 65535:
            raise ConfigurationError(
                "grpc bind port must be between 0 and 65535",
                details={"field": "port", "value": self.port},
            )
        if self.max_concurrent_rpcs < 1:
            raise ConfigurationError(
                "max_concurrent_rpcs must be at least 1",
                details={"field": "max_concurrent_rpcs"},
            )
        if self.graceful_stop_seconds < 0:
            raise ConfigurationError(
                "graceful_stop_seconds must not be negative",
                details={"field": "graceful_stop_seconds"},
            )
        if self.max_receive_bytes < 1024 or self.max_send_bytes < 1024:
            raise ConfigurationError(
                "message size bounds must be at least 1 KiB",
                details={
                    "max_receive_bytes": self.max_receive_bytes,
                    "max_send_bytes": self.max_send_bytes,
                },
            )

    @classmethod
    def from_service_settings(
        cls,
        settings: ServiceSettings,
        *,
        graceful_stop_seconds: float = GRACEFUL_STOP_SECONDS,
    ) -> GrpcServerConfig:
        """Derives transport settings from the service configuration.

        Args:
            settings: The orchestrator's service settings.
            graceful_stop_seconds: Grace period, taken from the shutdown
                configuration by the caller.

        Returns:
            A validated transport configuration.

        The bind address comes from configuration rather than being hard-coded
        so an operator can place the service on a private interface, and the
        loopback default keeps an accidental deployment from exposing it.
        """
        return cls(
            host=settings.bind_host,
            port=settings.bind_port,
            graceful_stop_seconds=graceful_stop_seconds,
            max_receive_bytes=settings.max_request_bytes,
        )

    @property
    def address(self) -> str:
        """The bind address, for log lines."""
        return f"{self.host}:{self.port}"


class GrpcServer:
    """Owns the gRPC server exposing ``AgentBridgeApi``.

    Args:
        servicer: The bridge servicer to register.
        config: Transport settings.
        logger: Logger override, for tests.
    """

    def __init__(
        self,
        *,
        servicer: AgentBridgeServicer,
        config: GrpcServerConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._servicer = servicer
        self._config = config or GrpcServerConfig()
        self._logger = logger or _LOGGER
        self._server: aio.Server | None = None
        self._bound_port: int | None = None

    @property
    def config(self) -> GrpcServerConfig:
        """The transport configuration in force."""
        return self._config

    @property
    def bound_port(self) -> int | None:
        """The port actually bound, or ``None`` before :meth:`start`.

        Meaningful when the configuration asks for port ``0``: the kernel picks
        a free port and this reports which one. Tests rely on it.
        """
        return self._bound_port

    @property
    def running(self) -> bool:
        """Whether the server has been started and not yet stopped."""
        return self._server is not None

    async def start(self) -> int:
        """Builds the server, registers the service and binds the socket.

        Returns:
            The bound port.

        Raises:
            DependencyUnavailableError: If the socket cannot be bound. The
                cause is logged, but the port and host are reported rather
                than any credential or configuration value.
        """
        from grpc import aio

        from mas_orchestrator.contracts.errors import DependencyUnavailableError

        server = aio.server(
            futures.ThreadPoolExecutor(
                max_workers=self._config.max_concurrent_rpcs,
                thread_name_prefix="mas-bridge",
            ),
            options=(
                ("grpc.max_receive_message_length", self._config.max_receive_bytes),
                ("grpc.max_send_message_length", self._config.max_send_bytes),
                ("grpc.so_reuseport", 0),
            ),
        )
        # The generated registrar is untyped; the call is a no-op on failure
        # because binding below is what actually validates the address.
        mas_api_pb2_grpc.add_AgentBridgeApiServicer_to_server(  # type: ignore[no-untyped-call]
            self._servicer, server
        )

        try:
            bound: int = server.add_insecure_port(self._config.address)
        except OSError as error:
            await server.stop(0)
            # Reports the address, never the configuration: a bind failure is
            # an operational fault, and the fix is to free the port or change
            # the configured address.
            raise DependencyUnavailableError(
                f"cannot bind the agent bridge server to {self._config.address}",
                details={"host": self._config.host, "port": self._config.port},
            ) from error

        if bound == 0:
            await server.stop(0)
            raise DependencyUnavailableError(
                f"cannot bind the agent bridge server to {self._config.address}",
                details={"host": self._config.host, "port": self._config.port},
            )

        await server.start()
        self._server = server
        self._bound_port = bound

        self._logger.info(
            "agent bridge server listening",
            extra={
                "event": "bridge.listening",
                "address": self._config.address,
                "bound_port": bound,
                "service": "mas.api.v1.AgentBridgeApi",
                "max_receive_bytes": self._config.max_receive_bytes,
            },
        )
        return bound

    async def stop(self) -> None:
        """Stops the server, giving in-flight calls a grace period.

        A grace period of zero severs calls immediately, which is the right
        behaviour only when the process is already shutting down hard. The
        default lets a nearly-finished agent run answer instead of forcing Rust
        to classify it as a transport failure and retry it.
        """
        server = self._server
        if server is None:
            return

        self._server = None
        self._logger.info(
            "agent bridge server stopping",
            extra={
                "event": "bridge.stopping",
                "grace_seconds": self._config.graceful_stop_seconds,
            },
        )
        await server.stop(self._config.graceful_stop_seconds)
        self._bound_port = None
        self._logger.info(
            "agent bridge server stopped",
            extra={"event": "bridge.stopped"},
        )

    def register_with(self, coordinator: ShutdownCoordinator) -> None:
        """Wires this server into the process shutdown coordinator.

        The server is registered as a *transport*, so it is closed after
        in-flight tasks are drained and before telemetry is flushed — the
        ordering spec §24 requires.

        Args:
            coordinator: The process shutdown coordinator.
        """
        coordinator.register_transport(_ServerCloseable(self))

    async def serve_forever(self) -> None:
        """Blocks until the server terminates.

        Used by a long-running process. A caller that wants to stop the server
        calls :meth:`stop` from another task.
        """
        server = self._server
        if server is None:
            raise RuntimeError("serve_forever called before start")
        await server.wait_for_termination()


@dataclass(frozen=True, slots=True)
class _ServerCloseable:
    """Adapts :class:`GrpcServer` to the coordinator's ``Closeable`` protocol.

    Attributes:
        server: The server to close.
    """

    server: GrpcServer

    async def aclose(self) -> None:
        """Stops the server as part of the shutdown sequence."""
        await self.server.stop()
