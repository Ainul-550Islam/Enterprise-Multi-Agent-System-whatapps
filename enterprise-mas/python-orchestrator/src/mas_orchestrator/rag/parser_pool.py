"""Process-isolated parsing for untrusted Data Hub documents.

Rich document parsers regularly operate on compressed containers and complex
object graphs.  Running them in the gRPC event loop makes a parser hang block
unrelated executions; running them in a thread only moves the blockage.  This
module gives each parse a short-lived child process with a wall-clock timeout,
a bounded address space on POSIX, bounded concurrent jobs, and deterministic
cleanup on cancellation.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import queue
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError
from mas_orchestrator.rag.parsing import Parser

if TYPE_CHECKING:
    from mas_orchestrator.rag.models import DocumentFormat

_DEFAULT_TIMEOUT_SECONDS: Final[float] = 30.0
_DEFAULT_MEMORY_BYTES: Final[int] = 256 * 1024 * 1024
_DEFAULT_WORKERS: Final[int] = 2
_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ParserPoolConfig:
    """Resource limits for isolated parser jobs."""

    workers: int = _DEFAULT_WORKERS
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    memory_bytes: int = _DEFAULT_MEMORY_BYTES

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 32:
            raise ConfigurationError("parser worker count must be in 1..=32")
        if not 0.1 <= self.timeout_seconds <= 300:
            raise ConfigurationError("parser timeout must be in 0.1..=300 seconds")
        if not 64 * 1024 * 1024 <= self.memory_bytes <= 2 * 1024 * 1024 * 1024:
            raise ConfigurationError("parser memory limit must be in 64 MiB..=2 GiB")


def _apply_memory_limit(memory_bytes: int) -> None:
    """Applies a best-effort POSIX address-space cap in the child only."""
    try:
        import resource

        _soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        target_hard = hard if hard != resource.RLIM_INFINITY else memory_bytes
        target = min(memory_bytes, target_hard)
        resource.setrlimit(resource.RLIMIT_AS, (target, target_hard))
    except (ImportError, OSError, ValueError):
        # Windows has no `resource` module and some constrained containers do
        # not permit lowering limits.  Timeout and process isolation remain in
        # force; deployment health reporting records platform limits separately.
        return


def _parse_worker(
    result_queue: multiprocessing.queues.Queue[tuple[str, str]],
    content: str | bytes,
    document_format: DocumentFormat,
    memory_bytes: int,
) -> None:
    """Child-process entry point; only safe error categories cross the IPC boundary."""
    _apply_memory_limit(memory_bytes)
    try:
        parsed = Parser().parse(content, document_format)
    except Exception as error:
        result_queue.put(("error", type(error).__name__))
        return
    result_queue.put(("ok", parsed))


class ParserPool:
    """A bounded asynchronous facade around one child process per parse.

    The child is intentionally short-lived instead of being a generic process
    pool: a parser that leaks memory or becomes uninterruptible is terminated
    after *this* document and cannot poison subsequent documents.
    """

    def __init__(self, config: ParserPoolConfig | None = None) -> None:
        self._config = config or ParserPoolConfig()
        self._slots = asyncio.Semaphore(self._config.workers)
        self._closed = False

    async def parse(self, content: str | bytes, format: DocumentFormat) -> str:  # noqa: A002
        """Parses content without blocking the event loop or exceeding configured limits."""
        if self._closed:
            raise ConfigurationError("parser pool is closed")
        cancel_requested = threading.Event()
        async with self._slots:
            work = asyncio.create_task(
                asyncio.to_thread(self._parse_blocking, content, format, cancel_requested)
            )
            try:
                return await asyncio.shield(work)
            except asyncio.CancelledError:
                # Do not leave an orphan parser after an RPC timeout/cancel.
                # The blocking side polls this event and terminates its child.
                cancel_requested.set()
                try:
                    await asyncio.shield(work)
                except Exception as cleanup_error:
                    # The caller's cancellation is authoritative; cleanup errors
                    # are deliberately not allowed to turn it into success.
                    _LOGGER.debug(
                        "parser cleanup after cancellation failed", exc_info=cleanup_error
                    )
                raise

    async def aclose(self) -> None:
        """Prevents new parser jobs; active jobs finish/cancel through their callers."""
        self._closed = True

    def _parse_blocking(
        self,
        content: str | bytes,
        document_format: DocumentFormat,
        cancel_requested: threading.Event,
    ) -> str:
        context = multiprocessing.get_context("spawn")
        results: multiprocessing.queues.Queue[tuple[str, str]] = context.Queue(maxsize=1)
        process = context.Process(
            target=_parse_worker,
            args=(results, content, document_format, self._config.memory_bytes),
            daemon=True,
        )
        process.start()
        deadline = time.monotonic() + self._config.timeout_seconds
        received: tuple[str, str] | None = None
        try:
            while received is None:
                if cancel_requested.is_set():
                    raise InvalidRequestError("document parsing was cancelled")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise InvalidRequestError(
                        "document parsing exceeded its time limit",
                        details={"timeout_seconds": self._config.timeout_seconds},
                    )
                try:
                    received = results.get(timeout=min(remaining, 0.1))
                except queue.Empty:
                    if not process.is_alive():
                        raise InvalidRequestError(
                            "document parser exited without a result"
                        ) from None
            kind, value = received
            if kind == "ok":
                return value
            raise InvalidRequestError(
                "document parser rejected the content",
                details={"parser_error": value},
            )
        finally:
            if process.is_alive():
                process.terminate()
            process.join(timeout=1.0)
            if process.is_alive():
                process.kill()
                process.join(timeout=1.0)
            results.close()
            results.join_thread()
