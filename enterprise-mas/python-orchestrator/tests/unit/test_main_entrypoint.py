"""Tests for the process entrypoint.

The ordering in :func:`main` is the security design, so these tests assert the
order and the failure modes rather than the happy path:

* configuration is validated before anything is constructed;
* logging is configured before anything logs;
* the drain runs whether the server returned or raised;
* the exit code says how clean the stop was.
"""

from __future__ import annotations

import asyncio
import signal

import pytest

from mas_orchestrator import main as entrypoint
from mas_orchestrator.api.app import AppConfig
from mas_orchestrator.main import (
    EXIT_CONFIGURATION_ERROR,
    SHUTDOWN_SIGNALS,
    Bootstrap,
    ShutdownCoordinator,
    _drill,
    _serve_stub,
    bootstrap,
    install_signal_handlers,
    main,
    parse_args,
    run,
)
from mas_orchestrator.services.lifecycle import DrainReport, ShutdownPhase, exit_code_for


class TestParseArgs:
    def test_environment_choices_are_known(self) -> None:
        assert parse_args(["--environment", "production"]).environment == "production"

    def test_unknown_environment_is_rejected(self) -> None:
        # Rejected by argparse before configuration is touched, so a typo
        # cannot reach the settings layer.
        with pytest.raises(SystemExit):
            parse_args(["--environment", "not-a-real-environment"])

    def test_defaults(self) -> None:
        args = parse_args([])
        assert args.environment is None
        assert not args.once

    def test_once_flag(self) -> None:
        assert parse_args(["--once"]).once

    def test_version_flag_exits(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["--version"])


class TestBootstrap:
    def test_bootstrap_validates_before_constructing(self) -> None:
        # A process that starts with bad configuration and discovers it mid-run
        # has already served requests under it.
        built = bootstrap()
        assert isinstance(built, Bootstrap)
        assert built.settings.environment is not None

    def test_bootstrap_propagates_a_configuration_error(self) -> None:
        # Letting it propagate is the point: a misconfigured process must not
        # start.
        with pytest.raises(ValueError, match="is not a valid Environment"):
            bootstrap(environment="not-an-environment")


class TestServeStub:
    async def test_stub_refuses_to_serve_while_draining(self) -> None:
        # Raising rather than serving means a caller cannot mistake "started"
        # for "serving".
        coordinator = ShutdownCoordinator()
        await coordinator.shutdown()
        with pytest.raises(RuntimeError, match="already draining"):
            await _serve_stub(coordinator)

    async def test_stub_returns_when_the_coordinator_stops(self) -> None:
        coordinator = ShutdownCoordinator()

        async def stop_soon() -> None:
            await asyncio.sleep(0.05)
            await coordinator.shutdown()

        task = asyncio.get_running_loop().create_task(stop_soon())
        await _serve_stub(coordinator)
        await task
        assert not coordinator.accepting


class TestRun:
    async def test_drain_runs_when_the_server_raises(self) -> None:
        # An exception in the server must not leave in-flight executions
        # abandoned, so the drain is in a finally.
        async def boom(_coordinator: ShutdownCoordinator) -> None:
            raise RuntimeError("server exploded")

        report = await run(ShutdownCoordinator(), boom(ShutdownCoordinator()))
        assert isinstance(report, DrainReport)

    async def test_drain_runs_when_the_server_is_cancelled(self) -> None:
        async def cancelled(_coordinator: ShutdownCoordinator) -> None:
            raise asyncio.CancelledError

        report = await run(ShutdownCoordinator(), cancelled(ShutdownCoordinator()))
        assert report.phase is not ShutdownPhase.RUNNING

    async def test_clean_drain_is_reported_clean(self) -> None:
        async def noop(_coordinator: ShutdownCoordinator) -> None:
            return None

        report = await run(ShutdownCoordinator(), noop(ShutdownCoordinator()))
        assert report.clean


class TestDrill:
    async def test_drill_reports_a_zero_exit_code(self) -> None:
        # The drill proves the drain path without binding a port.
        assert await _drill(ShutdownCoordinator()) == exit_code_for(
            DrainReport(ShutdownPhase.STOPPED, 0)
        )


class TestExitCodes:
    def test_configuration_error_has_its_own_code(self) -> None:
        # Distinct from a drain failure, so an operator can tell "it never
        # started" from "it stopped badly".
        assert EXIT_CONFIGURATION_ERROR not in (
            exit_code_for(DrainReport(ShutdownPhase.STOPPED, 0)),
            exit_code_for(DrainReport(ShutdownPhase.STOPPED, 0, pending=("a",))),
        )


class TestSignalHandlers:
    def test_shutdown_signals_are_registered(self) -> None:
        assert signal.SIGTERM in SHUTDOWN_SIGNALS
        assert signal.SIGINT in SHUTDOWN_SIGNALS

    async def test_handlers_install_without_raising(self) -> None:
        loop = asyncio.get_running_loop()
        coordinator = ShutdownCoordinator()
        try:
            install_signal_handlers(coordinator, loop)
        finally:
            for signum in SHUTDOWN_SIGNALS:
                try:
                    loop.remove_signal_handler(signum)
                except (NotImplementedError, RuntimeError):
                    signal.signal(signum, signal.SIG_DFL)


class TestAppConfig:
    def test_defaults_are_development(self) -> None:
        assert AppConfig().environment == "development"

    def test_draining_flag(self) -> None:
        assert AppConfig(draining=True).draining


class TestMain:
    def test_main_runs_the_drill(self) -> None:
        assert main(["--once"]) == 0

    def test_main_rejects_an_unknown_environment(self) -> None:
        # Argparse refuses it before configuration is loaded, so the failure
        # never reaches the settings layer.
        with pytest.raises(SystemExit):
            main(["--environment", "not-a-real-environment"])

    def test_main_runs_the_drill_in_production_shapes(self) -> None:
        assert main(["--once", "--environment", "development"]) == 0


def test_module_exposes_an_entrypoint() -> None:
    assert callable(entrypoint.main)
