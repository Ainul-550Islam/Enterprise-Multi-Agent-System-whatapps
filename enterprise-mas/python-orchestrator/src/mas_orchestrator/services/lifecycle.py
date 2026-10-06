"""Graceful shutdown.

Spec §24 gives the order explicitly: stop accepting new work, signal in-flight
tasks, honour cancellation deadlines, close provider connections, close
broker/gRPC connections, flush safe telemetry, exit with the correct status.

The order is not arbitrary. Accepting new work during a drain would create
work that is guaranteed to be abandoned. Closing transports before in-flight
tasks are signalled would fail them at the socket rather than at the
cancellation point. Flushing telemetry last is what makes the shutdown
observable at all.

The final rule — *do not terminate while silently abandoning authoritative
execution state* — is why :meth:`ShutdownCoordinator.drain` returns a report:
if tasks are still running when the deadline passes, the caller is told, with
their ids, rather than the process exiting as though nothing was left behind.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Final, Protocol, runtime_checkable

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


class ShutdownPhase(StrEnum):
    """Where the shutdown has reached."""

    RUNNING = "running"
    DRAINING = "draining"
    CLOSING_PROVIDERS = "closing_providers"
    CLOSING_TRANSPORTS = "closing_transports"
    FLUSHING = "flushing"
    STOPPED = "stopped"


class ExitStatus(IntEnum):
    """Process exit codes.

    A distinct non-zero code per failure mode, so a supervisor can tell an
    abandoned execution from a failed flush without reading logs.
    """

    OK = 0
    TASKS_ABANDONED = 70
    FLUSH_FAILED = 71
    CLOSE_FAILED = 72


@dataclass(frozen=True, slots=True)
class DrainReport:
    """What the drain left behind.

    Attributes:
        phase: Phase reached.
        waited_ms: How long the drain waited.
        pending: Ids of tasks still running when the deadline passed.
        cancelled: How many tasks were signalled.
        exit_status: The status the process should exit with.
    """

    phase: ShutdownPhase
    waited_ms: int
    pending: tuple[str, ...] = ()
    cancelled: int = 0
    exit_status: ExitStatus = ExitStatus.OK

    @property
    def clean(self) -> bool:
        """Whether nothing was abandoned."""
        return not self.pending


@runtime_checkable
class Closeable(Protocol):
    """Something that must be closed during shutdown."""

    async def aclose(self) -> None:
        """Releases the resource."""
        ...


@runtime_checkable
class Flushable(Protocol):
    """Something that buffers telemetry."""

    async def flush(self) -> None:
        """Writes out whatever is buffered."""
        ...


@dataclass(slots=True)
class ShutdownCoordinator:
    """Runs the shutdown sequence in the specified order."""

    drain_deadline_seconds: float = 30.0
    #: Grace period after cancellation before a task is considered stuck.
    cancel_grace_seconds: float = 5.0
    phase: ShutdownPhase = ShutdownPhase.RUNNING
    _accepting: bool = True
    _tasks: dict[str, asyncio.Task[object]] = field(default_factory=dict)
    _providers: list[Closeable] = field(default_factory=list)
    _transports: list[Closeable] = field(default_factory=list)
    _flushers: list[Flushable] = field(default_factory=list)

    @property
    def accepting(self) -> bool:
        """Whether new work may start."""
        return self._accepting

    @property
    def in_flight(self) -> int:
        """How many tasks are registered."""
        return len(self._tasks)

    # --- registration -------------------------------------------------------

    def register_provider(self, provider: Closeable) -> None:
        """Registers a provider connection to close."""
        self._providers.append(provider)

    def register_transport(self, transport: Closeable) -> None:
        """Registers a broker or gRPC connection to close."""
        self._transports.append(transport)

    def register_flusher(self, flusher: Flushable) -> None:
        """Registers a telemetry buffer to flush."""
        self._flushers.append(flusher)

    def track(self, task_id: str, task: asyncio.Task[object]) -> None:
        """Registers an in-flight task.

        Raises:
            RuntimeError: If the coordinator is no longer accepting work.
                Refusing is the point: a task registered after the drain began
                would be guaranteed to be abandoned.
        """
        if not self._accepting:
            raise RuntimeError(f"cannot track task {task_id}: shutdown is draining new work")
        self._tasks[task_id] = task
        task.add_done_callback(lambda _t: self._tasks.pop(task_id, None))

    # --- shutdown -----------------------------------------------------------

    async def shutdown(self) -> DrainReport:
        """Runs the full sequence and returns what it left behind."""
        started = time.monotonic()
        report = await self.drain()

        status = report.exit_status

        if not await self._close_all(self._providers, "provider"):
            status = ExitStatus.CLOSE_FAILED
        self.phase = ShutdownPhase.CLOSING_PROVIDERS

        if not await self._close_all(self._transports, "transport"):
            status = ExitStatus.CLOSE_FAILED
        self.phase = ShutdownPhase.CLOSING_TRANSPORTS

        self.phase = ShutdownPhase.FLUSHING
        # A flush failure only sets the status when nothing worse has already
        # been recorded: abandoned work outranks lost telemetry.
        if status is ExitStatus.OK and not await self._flush_all():
            status = ExitStatus.FLUSH_FAILED

        self.phase = ShutdownPhase.STOPPED
        waited = int((time.monotonic() - started) * 1000)
        return DrainReport(
            phase=self.phase,
            waited_ms=waited,
            pending=report.pending,
            cancelled=report.cancelled,
            exit_status=status,
        )

    async def drain(self) -> DrainReport:
        """Stops accepting work and waits for in-flight tasks.

        Returns:
            A :class:`DrainReport`. Anything still running when the deadline
            passes is named in ``pending``, so the caller cannot exit believing
            the slate is clean.
        """
        started = time.monotonic()
        self._accepting = False
        self.phase = ShutdownPhase.DRAINING

        tasks = list(self._tasks.items())
        if tasks:
            _LOGGER.info(
                "draining in-flight work",
                extra={"event": "shutdown.drain", "tasks": len(tasks)},
            )
            for _task_id, task in tasks:
                if not task.done():
                    task.cancel()

            # ``asyncio.wait`` rather than ``wait_for``: wait_for cancels and
            # then *awaits* the cancelled work, so a task that never yields
            # would hang the drain instead of being reported. Observing the
            # deadline is the point; waiting indefinitely for cleanup is not.
            _done, still_running = await asyncio.wait(
                [task for _task_id, task in tasks],
                timeout=self.drain_deadline_seconds,
            )
            if still_running:
                _LOGGER.warning(
                    "drain deadline exceeded",
                    extra={
                        "event": "shutdown.drain_timeout",
                        "deadline_seconds": self.drain_deadline_seconds,
                        "still_running": len(still_running),
                    },
                )

        pending = tuple(task_id for task_id, task in self._tasks.items() if not task.done())
        if pending:
            _LOGGER.error(
                "tasks still running after the drain deadline",
                extra={"event": "shutdown.tasks_abandoned", "pending": list(pending)},
            )

        waited = int((time.monotonic() - started) * 1000)
        return DrainReport(
            phase=self.phase,
            waited_ms=waited,
            pending=pending,
            cancelled=len(tasks),
            exit_status=ExitStatus.TASKS_ABANDONED if pending else ExitStatus.OK,
        )

    async def _close_all(self, resources: list[Closeable], label: str) -> bool:
        """Closes resources one at a time.

        One failure does not stop the rest: a provider that will not close
        must not prevent the transports from closing.
        """
        ok = True
        for resource in resources:
            try:
                await resource.aclose()
            except Exception as error:
                ok = False
                _LOGGER.warning(
                    "close failed during shutdown",
                    extra={
                        "event": "shutdown.close_failed",
                        "label": label,
                        "error": type(error).__name__,
                    },
                )
        return ok

    async def _flush_all(self) -> bool:
        """Flushes telemetry buffers."""
        return await self._close_all(
            [_AsClosable(flusher) for flusher in self._flushers],
            "flusher",
        )


class _AsClosable:
    """Adapts a :class:`Flushable` to the :class:`Closeable` protocol."""

    def __init__(self, flusher: Flushable) -> None:
        self._flusher = flusher

    async def aclose(self) -> None:
        """Flushes."""
        await self._flusher.flush()


def exit_code_for(report: DrainReport) -> int:
    """Maps a drain report onto a process exit code."""
    return int(report.exit_status)
