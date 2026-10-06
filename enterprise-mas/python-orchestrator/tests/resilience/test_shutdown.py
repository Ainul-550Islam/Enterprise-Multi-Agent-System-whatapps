"""Resilience of the shutdown sequence.

Spec §24 ends with a rule rather than a suggestion: the process must not
terminate while *silently* abandoning authoritative execution state. Silently
is the operative word — abandoning work is sometimes the only option left, but
exiting as though the slate were clean is never acceptable.

These tests pin the three properties that make that rule real:

1. Work is refused once the drain has begun, so nothing new can be created
   that is guaranteed to be abandoned.
2. A task that will not die is *named* in the report and escalates the exit
   status, rather than being waited for forever or quietly forgotten.
3. Telemetry is flushed last, and a lost flush is reported as a lesser failure
   than abandoned work.

Every test drives the real :class:`ShutdownCoordinator`. No fake shutdown
ordering: a reimplementation would prove nothing about the sequence the
process actually runs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Coroutine

import pytest

from mas_orchestrator.services.lifecycle import (
    DrainReport,
    ExitStatus,
    ShutdownCoordinator,
    ShutdownPhase,
)

# Long enough that no test can finish before it, short enough that a leaked
# task would not outlive the suite by much.
STUCK_SLEEP_SECONDS: Final[float] = 30.0
DRAIN_DEADLINE_SECONDS: Final[float] = 0.05


# --- doubles -----------------------------------------------------------------
#
# These are recording doubles, not mocks of the coordinator's behaviour. They
# exist because the coordinator closes whatever it is handed and the test needs
# to observe the order and the failures it was told about.


@dataclass(slots=True)
class _Closeable:
    """A resource that closes, optionally failing."""

    label: str
    fail: bool = False
    closed: bool = False
    close_order: list[str] = field(default_factory=list)

    async def aclose(self) -> None:
        """Records the close and raises when configured to."""
        self.closed = True
        self.close_order.append(self.label)
        if self.fail:
            raise RuntimeError(f"{self.label} refused to close")


@dataclass(slots=True)
class _OrderTracker:
    """Shared ordering log across several closeables."""

    events: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _TrackedCloseable:
    """A closeable that writes into a shared log."""

    label: str
    tracker: _OrderTracker

    async def aclose(self) -> None:
        """Appends its label to the shared log."""
        self.tracker.events.append(self.label)


@dataclass(slots=True)
class _TrackedFlushable:
    """A telemetry buffer that writes into a shared log."""

    label: str
    tracker: _OrderTracker

    async def flush(self) -> None:
        """Appends its label to the shared log."""
        self.tracker.events.append(self.label)


@dataclass(slots=True)
class _Flusher:
    """A telemetry buffer that may fail to flush."""

    fail: bool = False
    flushed: int = 0

    async def flush(self) -> None:
        """Records the flush and raises when configured to."""
        self.flushed += 1
        if self.fail:
            raise RuntimeError("telemetry sink unavailable")


async def _quick() -> str:
    """Work that finishes immediately."""
    return "done"


async def _stuck(stop: asyncio.Event) -> str:
    """Work that refuses cancellation until ``stop`` is set.

    A realistic stand-in for the worst case: an awaiting operation whose
    cancellation point is swallowed, or a cleanup step that hangs.
    """
    while True:
        try:
            await asyncio.sleep(STUCK_SLEEP_SECONDS)
        except asyncio.CancelledError:
            if stop.is_set():
                raise
            # Otherwise: swallow the cancellation and go back to waiting.


def _spawn(coro: Coroutine[Any, Any, object]) -> asyncio.Task[object]:
    """Wraps a coroutine in a task."""
    return asyncio.ensure_future(coro)


async def _start(coro: Coroutine[Any, Any, object]) -> asyncio.Task[object]:
    """Starts a task and lets it reach its first await point.

    The yield matters. A task that has never been stepped has no suspension
    point to interrupt, so cancelling it completes it outright — which would
    make a "stuck task" test pass for the wrong reason, proving nothing about
    work that is genuinely mid-flight.
    """
    task = asyncio.ensure_future(coro)
    await asyncio.sleep(0)
    return task


async def _release(stop: asyncio.Event) -> None:
    """Reaps deliberately stuck tasks so the event loop stays clean.

    Sets ``stop`` first, so the stuck task re-raises :class:`CancelledError`
    instead of swallowing the cancellation.
    """
    stop.set()
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
    await asyncio.sleep(0)


@pytest.fixture
def stuck_stop() -> asyncio.Event:
    """Release valve so a deliberately stuck task can be reaped."""
    return asyncio.Event()


# --- accepting work ----------------------------------------------------------


async def test_coordinator_accepts_work_before_shutdown() -> None:
    coordinator = ShutdownCoordinator()
    assert coordinator.accepting is True
    assert coordinator.phase is ShutdownPhase.RUNNING

    task = await _start(_quick())
    coordinator.track("t-1", task)
    assert coordinator.in_flight == 1


async def test_draining_refuses_new_work() -> None:
    # The reason for refusing is not tidiness. Work registered after the drain
    # began is work the drain will never wait for, so accepting it would
    # guarantee abandonment.
    coordinator = ShutdownCoordinator(drain_deadline_seconds=DRAIN_DEADLINE_SECONDS)
    await coordinator.drain()

    assert coordinator.accepting is False
    with pytest.raises(RuntimeError, match="draining new work"):
        task = await _start(_quick())
        coordinator.track("late", task)


async def test_completed_tasks_are_forgotten() -> None:
    coordinator = ShutdownCoordinator()
    task = await _start(_quick())
    coordinator.track("t-1", task)
    await asyncio.wait([task])

    # The done-callback removes it; a drain would otherwise report completed
    # work as in-flight.
    assert coordinator.in_flight == 0


# --- the happy path ----------------------------------------------------------


async def test_clean_shutdown_reports_ok() -> None:
    coordinator = ShutdownCoordinator()
    coordinator.track("t-1", await _start(_quick()))
    coordinator.track("t-2", await _start(_quick()))
    report = await coordinator.shutdown()

    assert report.clean is True
    assert report.pending == ()
    assert report.exit_status is ExitStatus.OK
    assert report.phase is ShutdownPhase.STOPPED
    assert coordinator.accepting is False


async def test_shutdown_with_nothing_registered_is_clean() -> None:
    report = await ShutdownCoordinator().shutdown()

    assert report.clean is True
    assert report.cancelled == 0
    assert report.exit_status is ExitStatus.OK


async def test_work_that_never_started_is_not_abandoned() -> None:
    # Worth pinning because it is easy to mistake for a bug. A task that has
    # not been stepped yet has no suspension point, so cancelling it completes
    # it immediately: it never ran, so there is no authoritative state to
    # abandon and the drain is legitimately clean.
    coordinator = ShutdownCoordinator(drain_deadline_seconds=DRAIN_DEADLINE_SECONDS)
    task = asyncio.ensure_future(_stuck(asyncio.Event()))
    coordinator.track("never-started", task)

    report = await coordinator.drain()

    assert report.pending == ()
    assert report.clean is True
    assert task.cancelled() is True


async def test_waited_ms_is_measured() -> None:
    report = await ShutdownCoordinator().shutdown()
    assert report.waited_ms >= 0


# --- abandonment is reported, never hidden -----------------------------------


async def test_stuck_task_is_named_in_the_report(stuck_stop: asyncio.Event) -> None:
    coordinator = ShutdownCoordinator(drain_deadline_seconds=DRAIN_DEADLINE_SECONDS)
    task = await _start(_stuck(stuck_stop))
    coordinator.track("stuck-1", task)
    report = await coordinator.drain()

    await _release(stuck_stop)
    assert report.pending == ("stuck-1",)
    assert report.clean is False
    assert report.exit_status is ExitStatus.TASKS_ABANDONED


async def test_abandonment_survives_the_full_shutdown_sequence(
    stuck_stop: asyncio.Event,
) -> None:
    # The critical assertion: closing providers, closing transports and
    # flushing telemetry all succeed, and the report *still* says work was
    # abandoned. A clean exit must not be manufactured by a tidy tail.
    coordinator = ShutdownCoordinator(drain_deadline_seconds=DRAIN_DEADLINE_SECONDS)
    task = await _start(_stuck(stuck_stop))
    coordinator.track("stuck-1", task)
    coordinator.register_provider(_Closeable("provider"))
    coordinator.register_transport(_Closeable("transport"))
    coordinator.register_flusher(_Flusher())

    report = await coordinator.shutdown()

    await _release(stuck_stop)
    assert report.clean is False
    assert report.exit_status is ExitStatus.TASKS_ABANDONED
    assert report.phase is ShutdownPhase.STOPPED


async def test_drain_reports_every_stuck_task(stuck_stop: asyncio.Event) -> None:
    coordinator = ShutdownCoordinator(drain_deadline_seconds=DRAIN_DEADLINE_SECONDS)
    for index in range(3):
        coordinator.track(f"stuck-{index}", await _start(_stuck(stuck_stop)))

    report = await coordinator.drain()

    await _release(stuck_stop)
    assert len(report.pending) == 3
    assert report.cancelled == 3


async def test_mixed_work_reports_only_what_survived(stuck_stop: asyncio.Event) -> None:
    coordinator = ShutdownCoordinator(drain_deadline_seconds=DRAIN_DEADLINE_SECONDS)
    task = await _start(_quick())
    coordinator.track("quick-1", task)
    task = await _start(_stuck(stuck_stop))
    coordinator.track("stuck-1", task)

    # The quick task has already finished and de-registered itself, so it is
    # neither cancelled nor reported: only work that was genuinely in flight
    # when the drain began is counted.
    assert coordinator.in_flight == 1

    report = await coordinator.drain()

    await _release(stuck_stop)
    assert report.pending == ("stuck-1",)
    assert report.cancelled == 1


# --- close failures ----------------------------------------------------------


async def test_provider_close_failure_escalates_status() -> None:
    coordinator = ShutdownCoordinator()
    coordinator.register_provider(_Closeable("provider", fail=True))

    report = await coordinator.shutdown()

    assert report.exit_status is ExitStatus.CLOSE_FAILED


async def test_transport_close_failure_escalates_status() -> None:
    coordinator = ShutdownCoordinator()
    coordinator.register_transport(_Closeable("transport", fail=True))

    report = await coordinator.shutdown()

    assert report.exit_status is ExitStatus.CLOSE_FAILED


async def test_one_failed_close_does_not_stop_the_others() -> None:
    # A provider that will not close must not strand the transport behind it;
    # every resource gets its chance.
    failing = _Closeable("provider-a", fail=True)
    surviving = _Closeable("provider-b")
    coordinator = ShutdownCoordinator()
    coordinator.register_provider(failing)
    coordinator.register_provider(surviving)

    report = await coordinator.shutdown()

    assert report.exit_status is ExitStatus.CLOSE_FAILED
    assert surviving.closed is True
    assert failing.closed is True


async def test_close_failure_outranks_flush_failure() -> None:
    # Both are failures, but they are not equally bad. A socket that would not
    # close may have left a peer holding state; lost telemetry is only lost
    # telemetry.
    coordinator = ShutdownCoordinator()
    coordinator.register_provider(_Closeable("provider", fail=True))
    coordinator.register_flusher(_Flusher(fail=True))

    report = await coordinator.shutdown()

    assert report.exit_status is ExitStatus.CLOSE_FAILED


# --- flushing is last --------------------------------------------------------


async def test_flush_failure_is_reported() -> None:
    coordinator = ShutdownCoordinator()
    coordinator.register_flusher(_Flusher(fail=True))

    report = await coordinator.shutdown()

    assert report.exit_status is ExitStatus.FLUSH_FAILED


async def test_flush_happens_after_resources_are_closed() -> None:
    # Telemetry is written last so that close failures are themselves visible
    # in the flushed output. Flushing first would lose the evidence.
    tracker = _OrderTracker()
    coordinator = ShutdownCoordinator()
    coordinator.register_provider(_TrackedCloseable("provider", tracker))
    coordinator.register_transport(_TrackedCloseable("transport", tracker))
    coordinator.register_flusher(_TrackedFlushable("flush", tracker))

    await coordinator.shutdown()

    assert tracker.events == ["provider", "transport", "flush"]


async def test_abandoned_work_outranks_lost_telemetry(stuck_stop: asyncio.Event) -> None:
    # The ordering rule from the source: a flush failure only sets the status
    # when nothing worse has already been recorded. Abandoned authoritative
    # state is worse than a missing metric.
    coordinator = ShutdownCoordinator(drain_deadline_seconds=DRAIN_DEADLINE_SECONDS)
    task = await _start(_stuck(stuck_stop))
    coordinator.track("stuck-1", task)
    coordinator.register_flusher(_Flusher(fail=True))

    report = await coordinator.shutdown()

    await _release(stuck_stop)
    assert report.exit_status is ExitStatus.TASKS_ABANDONED


async def test_flush_failure_does_not_mask_clean_drain() -> None:
    coordinator = ShutdownCoordinator()
    task = await _start(_quick())
    coordinator.track("t-1", task)
    coordinator.register_flusher(_Flusher(fail=True))

    report = await coordinator.shutdown()

    # Nothing was abandoned — the drain itself was clean. The flush loss is
    # reported on its own.
    assert report.clean is True
    assert report.exit_status is ExitStatus.FLUSH_FAILED


async def test_every_flusher_is_flushed() -> None:
    first = _Flusher()
    second = _Flusher()
    coordinator = ShutdownCoordinator()
    coordinator.register_flusher(first)
    coordinator.register_flusher(second)

    await coordinator.shutdown()

    assert first.flushed == 1
    assert second.flushed == 1


# --- the report itself -------------------------------------------------------


def test_drain_report_clean_is_derived_from_pending() -> None:
    clean = DrainReport(phase=ShutdownPhase.STOPPED, waited_ms=0)
    dirty = DrainReport(phase=ShutdownPhase.STOPPED, waited_ms=0, pending=("a",))

    assert clean.clean is True
    assert dirty.clean is False


def test_exit_statuses_are_distinct_and_nonzero() -> None:
    # A supervisor distinguishes failure modes by exit code, so two different
    # failures must never share one.
    codes = [
        ExitStatus.TASKS_ABANDONED,
        ExitStatus.FLUSH_FAILED,
        ExitStatus.CLOSE_FAILED,
    ]
    assert len({int(code) for code in codes}) == len(codes)
    assert all(code != 0 for code in codes)
    assert int(ExitStatus.OK) == 0


def test_exit_status_orders_by_severity() -> None:
    # Abandoning authoritative state is the worst outcome, so it carries the
    # lowest non-zero code by convention and wins every comparison in the
    # coordinator.
    assert ExitStatus.TASKS_ABANDONED < ExitStatus.FLUSH_FAILED
    assert ExitStatus.TASKS_ABANDONED < ExitStatus.CLOSE_FAILED
