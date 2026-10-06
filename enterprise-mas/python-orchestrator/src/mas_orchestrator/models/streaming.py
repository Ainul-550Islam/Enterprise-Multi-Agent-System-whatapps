"""Streaming event handling: SSE parsing, accumulation and cancellation.

Streaming exists so a caller can show partial output and cancel early, but it
introduces failure modes that a non-streaming call does not have: a stream can
stop mid-response, produce a partial tool call, or report usage only after the
content is finished. This module centralises the handling of all three.

Design points:

* **One SSE parser, shared by every provider.** ``iter_sse_data`` handles the
  framing (``data:`` lines, blank-line delimited events, the ``[DONE]``
  sentinel). Adapters then decode only their own payloads.
* **A deadline covers the whole stream**, not just the first chunk. A stream
  that goes silent is a timeout, not a successful end.
* **Partial results are preserved on failure.** :class:`StreamAccumulator`
  keeps what was received, so a cancelled or timed-out stream can still report
  the text it produced rather than discarding it. Losing it would force the
  caller to re-run and pay twice.
* **Cancellation is observed, not just polled.** :func:`aiter_stream` checks the
  cancellation event between chunks and stops promptly.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ExecutionTimeoutError,
    ModelTimeoutError,
)
from mas_orchestrator.models.types import ToolCall

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable

    from mas_orchestrator.models.types import (
        FinishReason,
        ModelRef,
        ModelStreamEvent,
        Usage,
    )

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "StreamAccumulator",
    "aiter_stream",
    "iter_sse_data",
]


def iter_sse_data(body: bytes) -> Iterable[bytes]:
    """Yields the ``data:`` payloads from a Server-Sent-Events body.

    Handles the shapes real providers emit:

    * ``data: <payload>`` lines, with or without a leading space;
    * blank lines terminating an event;
    * the ``[DONE]`` sentinel, which is yielded so the caller can stop;
    * ``:`` comment/keepalive lines, which are skipped;
    * both ``\\n`` and ``\\r\\n`` line endings.

    An unterminated final event is yielded rather than dropped: a truncated
    stream should still deliver the last complete ``data:`` line it managed to
    send.

    Args:
        body: The raw response body.

    Yields:
        Each ``data:`` payload, stripped of the prefix and surrounding space.
    """
    text = body.decode("utf-8", errors="replace")

    for raw_event in text.split("\n\n"):
        if not raw_event.strip():
            continue

        payload_parts: list[str] = []
        for raw_line in raw_event.replace("\r\n", "\n").split("\n"):
            line = raw_line.rstrip("\r")
            if not line or line.startswith(":"):
                # Comment or keepalive line; carries no data.
                continue
            if not line.startswith("data:"):
                continue
            payload_parts.append(line[len("data:") :].lstrip())

        if payload_parts:
            yield "\n".join(payload_parts).encode("utf-8")


@dataclass(slots=True)
class StreamAccumulator:
    """Accumulates a stream into a final result, preserving partial output.

    Attributes:
        text: All text received so far.
        tool_calls: Tool calls assembled from deltas.
        usage: Usage reported on the terminal chunk, when available.
        finish_reason: Finish reason from the terminal chunk.
        is_complete: Whether a terminal event was observed.
    """

    model: ModelRef
    text: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: Usage | None = None
    finish_reason: FinishReason | None = None
    is_complete: bool = False

    def apply(self, event: ModelStreamEvent) -> None:
        """Folds one stream event into the accumulated state.

        Tool-call deltas are merged by position: a streamed tool call arrives
        as several fragments sharing an index, with the name in the first and
        argument pieces in the rest. Fragments are appended to the most recent
        call unless a new ``id`` begins one.

        Args:
            event: The event to fold in.
        """
        if event.delta_text:
            self.text += event.delta_text

        if event.delta_tool_call is not None:
            self._merge_tool_call(event.delta_tool_call)

        if event.usage is not None:
            self.usage = event.usage

        if event.finish_reason is not None:
            self.finish_reason = event.finish_reason

        if event.is_terminal:
            self.is_complete = True

    def _merge_tool_call(self, fragment: ToolCall) -> None:
        """Merges a streamed tool-call fragment into the pending call."""
        if fragment.id and (not self.tool_calls or self.tool_calls[-1].id != fragment.id):
            self.tool_calls.append(fragment)
            return

        if not self.tool_calls:
            self.tool_calls.append(fragment)
            return

        previous = self.tool_calls[-1]
        merged = ToolCall(
            id=previous.id or fragment.id,
            name=previous.name or fragment.name,
            arguments_json=previous.arguments_json + fragment.arguments_json,
        )
        self.tool_calls[-1] = merged

    @property
    def has_partial_output(self) -> bool:
        """Whether anything was received, even if the stream did not finish."""
        return bool(self.text) or bool(self.tool_calls)


async def aiter_stream(
    events: AsyncIterator[ModelStreamEvent],
    *,
    model: ModelRef,
    timeout_seconds: float,
    cancel_event: asyncio.Event | None = None,
    max_chunks: int = 100_000,
) -> StreamAccumulator:
    """Consumes a stream into an accumulator under a deadline.

    Args:
        events: The provider's event iterator.
        model: Model producing the stream, for the accumulator.
        timeout_seconds: Deadline covering the **whole** stream.
        cancel_event: When set, consumption stops and
            :class:`CancellationRequestedError` is raised.
        max_chunks: Safety bound on chunk count. A stream that never ends
            would otherwise consume memory without limit.

    Returns:
        The accumulator. On timeout or cancellation it is returned via the
        exception's context rather than raised away — see raises.

    Raises:
        CancellationRequestedError: If cancellation was observed. The
            accumulated partial output is attached to the error's
            ``details`` under ``"partial"`` so a caller that wants to keep
            it can, without the type system forcing a try/except around
            every stream.
        ModelTimeoutError: If the deadline elapsed while streaming, also
            carrying partial output.
        ExecutionTimeoutError: If the chunk cap was hit before the stream
            ended, which indicates a runaway stream rather than a slow one.
    """
    accumulator = StreamAccumulator(model=model)
    chunks = 0

    async def consume() -> None:
        nonlocal chunks
        async for event in events:
            if cancel_event is not None and cancel_event.is_set():
                raise CancellationRequestedError("cancelled during streaming")

            chunks += 1
            if chunks > max_chunks:
                raise ExecutionTimeoutError(
                    "stream exceeded the maximum chunk count",
                    details={"max_chunks": max_chunks},
                )

            accumulator.apply(event)

    try:
        await asyncio.wait_for(consume(), timeout=timeout_seconds)
    except TimeoutError as error:
        raise ModelTimeoutError(
            "stream exceeded its deadline",
            details={
                "timeout_seconds": timeout_seconds,
                "chunks_received": chunks,
                "has_partial_output": accumulator.has_partial_output,
            },
        ) from error
    except CancellationRequestedError as error:
        error.details.setdefault("chunks_received", chunks)
        error.details.setdefault("has_partial_output", accumulator.has_partial_output)
        raise

    if not accumulator.is_complete:
        # The iterator ended without a terminal event. That is a truncated
        # stream, not a successful one: callers must not treat it as final.
        _LOGGER.warning(
            "stream ended without a terminal event",
            extra={"chunks": chunks},
        )

    return accumulator
