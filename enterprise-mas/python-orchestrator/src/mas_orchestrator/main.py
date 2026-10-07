# PURPOSE:
# Process entrypoint for the Python orchestrator.
#
# Loads configuration, wires signal handling to a graceful drain, composes the
# real agent bridge server, and serves until shutdown. Exits with distinct
# codes for a configuration failure and an unclean drain, so an operator can
# tell "it never started" from "it stopped badly".

"""Process entrypoint.

The ordering in :func:`main` is the security design, not housekeeping:

1. **Load settings first.** Configuration is validated before anything is
   constructed, because a process that starts with bad configuration and then
   discovers it mid-run has already served some requests under it.
2. **Configure logging before anything logs.** Structured logging is what every
   later diagnostic depends on; a traceback printed before it is configured is
   a traceback nobody can correlate.
3. **Register signal handlers before accepting work.** A signal arriving before
   a handler exists is a default ``SIGTERM`` kill, which abandons in-flight
   executions.
4. **Build the runtime, then serve.**
5. **Shutdown is the same path regardless of how it was triggered** — signal,
   exception, or normal completion — so there is one drain implementation to
   reason about rather than three.

Spec §24 is explicit that the process must not terminate while silently
abandoning authoritative execution state, so shutdown goes through
:class:`~mas_orchestrator.services.lifecycle.ShutdownCoordinator` and the exit
code is derived from the drain report. An unclean drain exits non-zero, so an
orchestrator can tell "stopped" from "stopped badly".

Configuration failure exits with a distinct code and prints a message that
names the field. It does not print values: settings include credentials, and a
startup error on a terminal is not a place to put one.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.config.settings import load_settings
from mas_orchestrator.config.validation import validate_settings_or_raise
from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.data_hub_servicer import DataHubBridgeServicer
from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig
from mas_orchestrator.observability.logging import LoggingConfig, configure_logging
from mas_orchestrator.security.identity import ServiceScope
from mas_orchestrator.security.internal_service import DEFAULT_ALLOWED_CALLERS
from mas_orchestrator.services.composition import build_runtime_bundle
from mas_orchestrator.services.lifecycle import (
    DrainReport,
    ShutdownCoordinator,
    exit_code_for,
)
from mas_orchestrator.version import __version__

if TYPE_CHECKING:
    from collections.abc import Awaitable, Sequence

    from mas_orchestrator.config.settings import Settings

_LOGGER: Final[logging.Logger] = logging.getLogger("mas_orchestrator.main")

#: Exit code for a configuration failure. Distinct from a drain failure so an
#: operator can tell "it never started" from "it stopped badly".
EXIT_CONFIGURATION_ERROR: Final[int] = 78

#: Signals that begin a graceful drain.
SHUTDOWN_SIGNALS: Final[tuple[int, ...]] = (signal.SIGTERM, signal.SIGINT)


@dataclass(frozen=True, slots=True)
class Bootstrap:
    """What :func:`bootstrap` produced.

    Attributes:
        settings: Validated configuration.
        coordinator: Owns the drain.
        serve: Runs until the coordinator stops accepting work.
    """

    settings: Any
    coordinator: ShutdownCoordinator
    serve: Any


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parses command-line arguments.

    Deliberately few. Every operational value belongs in configuration, where
    it is validated and versioned; a flag that sets a security-relevant value
    would bypass that.
    """
    parser = argparse.ArgumentParser(
        prog="mas-orchestrator",
        description="Multi-agent AI orchestrator service.",
    )
    parser.add_argument("--version", action="version", version=f"mas-orchestrator {__version__}")
    parser.add_argument(
        "--environment",
        choices=("development", "staging", "production"),
        default=None,
        help="Deployment environment; overrides MAS_ENV.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one shutdown drill and exit; used by smoke tests.",
    )
    return parser.parse_args(argv)


def bootstrap(
    *,
    environment: str | None = None,
) -> Bootstrap:
    """Loads configuration and builds the process scaffolding.

    Args:
        environment: Environment override.

    Returns:
        A :class:`Bootstrap`.

    Raises:
        Exception: Propagated from settings loading. Callers must let it
            propagate: a misconfigured process must not start.
    """
    from mas_orchestrator.config.enums import Environment

    resolved = Environment(environment) if environment else None
    settings = validate_settings_or_raise(load_settings(environment=resolved))

    configure_logging(
        LoggingConfig(
            level=settings.observability.log_level,
            json_output=settings.observability.log_format.value == "json",
            redact=settings.observability.redact_logs,
        )
    )

    _LOGGER.info(
        "configuration loaded",
        extra={
            "event": "startup.config",
            "environment": settings.environment.value,
            "bind_port": settings.service.bind_port,
            "version": __version__,
        },
    )

    coordinator = ShutdownCoordinator(
        drain_deadline_seconds=settings.service.shutdown_grace_seconds,
    )
    return Bootstrap(
        settings=settings,
        coordinator=coordinator,
        serve=_serve_stub,
    )


def _internal_signing_secret(settings: Settings) -> str | None:
    """Resolves the shared secret Rust signs bridge calls with.

    The secret is read from a secret reference, never from a committed
    configuration value, and it is revealed exactly once here so it never
    becomes part of a logged settings dump.

    Args:
        settings: Loaded service settings.

    Returns:
        The secret, or ``None`` when no reference is configured — which the
        servicer treats as "this deployment does not sign calls", a posture
        production validation refuses.

    Raises:
        ConfigurationError: If a reference is configured but cannot be
            resolved. A half-provisioned secret manager must be a loud boot
            failure, not a bridge that silently stops verifying signatures.
    """
    reference = settings.security.internal_service_secret_ref
    if not reference:
        return None
    from mas_orchestrator.security.credentials import resolve_secret

    return resolve_secret(reference).reveal()


def build_bridge_server(
    settings: Settings,
    coordinator: ShutdownCoordinator,
) -> GrpcServer:
    """Composes the real agent bridge server from configuration.

    Args:
        settings: Loaded service settings.
        coordinator: The process shutdown coordinator, so the server is closed
            as part of the drain rather than severed at process exit.

    Returns:
        A server, built but not yet started.

    Raises:
        ConfigurationError: If the agent runtime cannot be composed. There is
            no degraded mode: a bridge that cannot reach a real model provider
            must not start, because a bridge that starts and then fails every
            run is indistinguishable from one that is being retried.

    Both bridge servicers share one listener and one AgentBridge authentication
    boundary. The runtime bundle is registered as a closeable so provider and
    Data Hub resources are released after drain, including failed startup.

    This is the composition root for the Rust → Python path. It is separate
    from :func:`bootstrap` so that loading configuration can be tested without
    binding a socket or resolving provider credentials.
    """
    bundle = build_runtime_bundle(settings)
    servicer = AgentBridgeServicer(
        execution_service=bundle.execution_service,
        identity_resolver=bundle.identity_resolver,
        config=AgentBridgeServicerConfig(
            require_scope=ServiceScope.EXECUTE_AGENT,
            max_input_bytes=settings.service.max_request_bytes,
            internal_service_secret=_internal_signing_secret(settings),
            allowed_internal_callers=DEFAULT_ALLOWED_CALLERS
            | frozenset(settings.security.extra_allowed_internal_callers),
            # A configured secret already makes the signature mandatory; this
            # flag additionally turns an *unconfigured* secret into a boot
            # failure rather than a silent skip. Production asks for exactly
            # that: a bridge that has not been provisioned must not quietly
            # start accepting traffic.
            require_internal_signature=settings.is_production,
        ),
    )
    data_hub_servicer = DataHubBridgeServicer(
        service=bundle.data_hub_service,
        agent_bridge=servicer,
    )
    server = GrpcServer(
        servicer=servicer,
        data_hub_servicer=data_hub_servicer,
        closeables=(bundle,),
        config=GrpcServerConfig.from_service_settings(
            settings.service,
            environment=settings.environment,
            graceful_stop_seconds=settings.service.shutdown_grace_seconds,
        ),
    )
    server.register_with(coordinator)
    return server


async def _serve_bridge(server: GrpcServer, coordinator: ShutdownCoordinator) -> None:
    """Starts the bridge and serves until the coordinator drains.

    Args:
        server: A composed bridge server.
        coordinator: The process shutdown coordinator.

    Raises:
        RuntimeError: If the coordinator is already draining, so a caller
            cannot mistake "started" for "serving".
    """
    if not coordinator.accepting:
        raise RuntimeError("cannot serve: the coordinator is already draining")
    await server.start()
    try:
        await server.serve_forever()
    finally:
        await server.stop()


async def _serve_stub(coordinator: ShutdownCoordinator) -> None:
    """Waits until the coordinator stops accepting work.

    The real server binds a gRPC listener here. It is a stub in the sense that
    no socket is opened: this entrypoint is exercised by lifecycle tests, and
    opening a port by default would make an import-and-run dangerous.

    This is not a fallback. It raises if the coordinator is already draining,
    so a caller cannot mistake "started" for "serving".
    """
    if not coordinator.accepting:
        raise RuntimeError("cannot serve: the coordinator is already draining")
    _LOGGER.info("service ready", extra={"event": "startup.ready", "listening": False})
    running = True
    while running:
        await asyncio.sleep(0.25)
        running = coordinator.accepting
    _LOGGER.info("service stopped accepting work", extra={"event": "shutdown.stopped"})


def install_signal_handlers(
    coordinator: ShutdownCoordinator,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Begins a drain when ``SIGTERM`` or ``SIGINT`` arrives.

    Installed before work is accepted. A signal arriving before a handler
    exists is a default kill, which abandons in-flight executions — precisely
    what spec §24 forbids.

    The handler schedules the drain rather than running it, because a signal
    handler that does real work can re-enter the loop.
    """

    def _handler(signum: int, _frame: object) -> None:
        name = signal.Signals(signum).name
        _LOGGER.info("signal received", extra={"event": "shutdown.signal", "signal": name})
        loop.create_task(coordinator.shutdown())

    for signum in SHUTDOWN_SIGNALS:
        try:
            loop.add_signal_handler(signum, _handler, signum, None)
        except (NotImplementedError, RuntimeError):
            # Windows and non-main threads have no signal handlers. Falling
            # back to signal.signal keeps the process stoppable rather than
            # unstoppable, which is the worse alternative.
            signal.signal(signum, _handler)


async def _drill(coordinator: ShutdownCoordinator) -> int:
    """Runs one drain and reports the exit code it would have produced.

    The shutdown is scheduled up front, so the drill exercises the same path a
    signal would: a server running when the drain begins, rather than a drain
    of a server that never started.
    """
    serving = asyncio.get_running_loop().create_task(_serve_stub(coordinator))
    asyncio.get_running_loop().create_task(_drill_stop(coordinator))
    report = await run(coordinator, asyncio.shield(serving))
    return exit_code_for(report)


async def _drill_stop(coordinator: ShutdownCoordinator) -> None:
    """Ends a drill once the server has logged that it is ready.

    A signal substitute: it triggers the drain from outside the server, so the
    drill exercises the same interruption a real shutdown causes.
    """
    await asyncio.sleep(0.05)
    await coordinator.shutdown()


async def run(coordinator: ShutdownCoordinator, serve: Awaitable[Any]) -> DrainReport:
    """Serves, then drains.

    ``serve`` is an already-created awaitable rather than a callable, so the
    caller owns how the server was started — a plain coroutine, or a task that
    something else can cancel.

    The drain runs whether the server returned normally or raised, so an
    exception in the server cannot leave in-flight executions abandoned.

    Returns:
        The :class:`DrainReport`, describing how clean the stop was.
    """
    try:
        await serve
    except asyncio.CancelledError:
        _LOGGER.warning("serve cancelled", extra={"event": "shutdown.cancelled"})
    except Exception as error:
        _LOGGER.error(
            "serve failed",
            extra={"event": "serve.failed", "error_type": type(error).__name__},
            exc_info=error,
        )
    finally:
        report = await coordinator.shutdown()
    _LOGGER.info(
        "shutdown complete",
        extra={
            "event": "shutdown.complete",
            "phase": report.phase.value,
            "clean": report.clean,
            "pending": len(report.pending),
        },
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    """Runs the process.

    Returns:
        A process exit code.
    """
    args = parse_args(argv)

    try:
        built = bootstrap(environment=args.environment)
    except Exception as error:
        # Named without values: settings include credentials, and a startup
        # error must not print one.
        print(
            f"configuration error: {type(error).__name__}: {error}",
            file=sys.stderr,
        )
        return EXIT_CONFIGURATION_ERROR

    if args.once:
        # A shutdown drill: proves the drain path without binding a port or
        # waiting for a signal. It deliberately uses the stub server so the
        # drill cannot fail because a port is already taken.
        return asyncio.run(_drill(built.coordinator))

    async def _amain() -> int:
        coordinator = built.coordinator
        install_signal_handlers(coordinator, asyncio.get_running_loop())
        server = build_bridge_server(built.settings, coordinator)
        report = await run(coordinator, _serve_bridge(server, coordinator))
        return exit_code_for(report)

    return asyncio.run(_amain())


if __name__ == "__main__":  # pragma: no cover - exercised by the console script
    raise SystemExit(main())
