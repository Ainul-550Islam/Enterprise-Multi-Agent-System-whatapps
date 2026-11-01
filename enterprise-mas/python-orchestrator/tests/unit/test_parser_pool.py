"""Regression tests for process-isolated document parsing."""

from __future__ import annotations

import asyncio
import queue
import threading

import pytest

import mas_orchestrator.rag.parser_pool as parser_pool_module
from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError
from mas_orchestrator.rag.models import DocumentFormat
from mas_orchestrator.rag.parser_pool import ParserPool, ParserPoolConfig


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"workers": 0}, "worker count"),
        ({"timeout_seconds": 0.01}, "timeout"),
        ({"memory_bytes": 1}, "memory limit"),
    ],
)
def test_parser_pool_config_rejects_unsafe_resource_limits(
    kwargs: dict[str, int | float], message: str
) -> None:
    """A pool never silently accepts an unusable safety bound."""
    with pytest.raises(ConfigurationError, match=message):
        ParserPoolConfig(**kwargs)


@pytest.mark.asyncio
async def test_parser_pool_runs_plain_text_in_a_child_process() -> None:
    """The normal path returns parsed text through the bounded IPC channel."""
    pool = ParserPool(ParserPoolConfig(timeout_seconds=5.0))

    try:
        parsed = await pool.parse("A safe document.", DocumentFormat.TEXT)
    finally:
        await pool.aclose()

    assert parsed == "A safe document."


@pytest.mark.asyncio
async def test_parser_pool_cancellation_waits_for_blocking_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caller cancellation signals the blocking side before the task resolves."""
    started = threading.Event()
    cleaned = threading.Event()

    def wait_for_cancellation(
        content: str | bytes,
        document_format: DocumentFormat,
        cancel_requested: threading.Event,
    ) -> str:
        del content, document_format
        started.set()
        cancel_requested.wait(timeout=2.0)
        cleaned.set()
        raise InvalidRequestError("document parsing was cancelled")

    pool = ParserPool()
    monkeypatch.setattr(pool, "_parse_blocking", wait_for_cancellation)
    task = asyncio.create_task(pool.parse("cancel me", DocumentFormat.TEXT))

    assert await asyncio.to_thread(started.wait, 1.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert cleaned.is_set()
    await pool.aclose()


class _EmptyQueue:
    """A child-result queue that never yields a result."""

    def get(self, timeout: float | None = None) -> tuple[str, str]:
        del timeout
        raise queue.Empty

    def close(self) -> None:
        return None

    def join_thread(self) -> None:
        return None


class _StuckProcess:
    """Minimal process double used to exercise timeout cleanup deterministically."""

    def __init__(self) -> None:
        self._alive = True
        self.terminated = False
        self.killed = False

    def start(self) -> None:
        return None

    def is_alive(self) -> bool:
        return self._alive

    def terminate(self) -> None:
        self.terminated = True
        self._alive = False

    def join(self, timeout: float | None = None) -> None:
        del timeout
        return None

    def kill(self) -> None:
        self.killed = True
        self._alive = False


class _StuckContext:
    """Spawn-context double with one indefinitely-running child."""

    def __init__(self) -> None:
        self.process = _StuckProcess()
        self.queue = _EmptyQueue()

    def Queue(self, maxsize: int) -> _EmptyQueue:  # noqa: N802
        assert maxsize == 1
        return self.queue

    def Process(  # noqa: N802
        self,
        *,
        target: object,
        args: tuple[object, ...],
        daemon: bool,
    ) -> _StuckProcess:
        assert target is parser_pool_module._parse_worker
        assert daemon
        assert len(args) == 4
        return self.process


def test_parser_pool_timeout_terminates_the_child(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hung parser becomes a typed timeout and is terminated in ``finally``."""
    context = _StuckContext()
    pool = ParserPool(ParserPoolConfig(timeout_seconds=0.1))
    monkeypatch.setattr(parser_pool_module.multiprocessing, "get_context", lambda method: context)

    with pytest.raises(InvalidRequestError, match="time limit"):
        pool._parse_blocking("hang", DocumentFormat.TEXT, threading.Event())

    assert context.process.terminated
    assert not context.process.killed
