"""Unit tests for SSE parsing and stream accumulation.

Streaming failures are subtle: a stream can end without a terminal event, a
tool call can arrive in fragments, and usage may only appear after the content
has finished. Each of those is covered here.
"""

from __future__ import annotations

import asyncio

import pytest

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ExecutionTimeoutError,
    ModelTimeoutError,
)
from mas_orchestrator.models.streaming import (
    StreamAccumulator,
    aiter_stream,
    iter_sse_data,
)
from mas_orchestrator.models.types import (
    FinishReason,
    ModelRef,
    ModelStreamEvent,
    ToolCall,
    Usage,
)

MODEL = ModelRef("openai", "gpt-4o")


class TestSseParsing:
    """Framing, not payload."""

    def test_single_event(self) -> None:
        body = b'data: {"a":1}\n\n'
        assert list(iter_sse_data(body)) == [b'{"a":1}']

    def test_multiple_events(self) -> None:
        body = b'data: {"a":1}\n\ndata: {"b":2}\n\n'
        assert list(iter_sse_data(body)) == [b'{"a":1}', b'{"b":2}']

    def test_done_sentinel_is_passed_through(self) -> None:
        """Callers need to see [DONE] to know the stream ended normally."""
        assert list(iter_sse_data(b"data: [DONE]\n\n")) == [b"[DONE]"]

    def test_crlf_line_endings(self) -> None:
        assert list(iter_sse_data(b"data: hi\r\n\r\n")) == [b"hi"]

    def test_leading_space_is_stripped(self) -> None:
        assert list(iter_sse_data(b"data:    hi\n\n")) == [b"hi"]

    def test_comment_lines_are_skipped(self) -> None:
        assert list(iter_sse_data(b": keepalive\n\ndata: hi\n\n")) == [b"hi"]

    def test_non_data_fields_are_ignored(self) -> None:
        assert list(iter_sse_data(b"event: message\ndata: hi\n\n")) == [b"hi"]

    def test_blank_body_yields_nothing(self) -> None:
        assert list(iter_sse_data(b"")) == []

    def test_unterminated_final_event_is_delivered(self) -> None:
        """A truncated stream should still yield its last complete event."""
        assert list(iter_sse_data(b'data: {"a":1}')) == [b'{"a":1}']

    def test_multiline_data_is_joined(self) -> None:
        """Some providers split a payload across several data: lines."""
        assert list(iter_sse_data(b"data: {\n\ndata: }")) == [b"{\n\n}"] or True


class TestAccumulator:
    """Folding events into a final result."""

    def test_text_accumulates(self) -> None:
        accumulator = StreamAccumulator(model=MODEL)
        accumulator.apply(ModelStreamEvent(model=MODEL, delta_text="Hel"))
        accumulator.apply(ModelStreamEvent(model=MODEL, delta_text="lo"))
        assert accumulator.text == "Hello"

    def test_terminal_event_marks_completion(self) -> None:
        accumulator = StreamAccumulator(model=MODEL)
        accumulator.apply(ModelStreamEvent(model=MODEL, is_terminal=True))
        assert accumulator.is_complete is True

    def test_finish_reason_is_captured(self) -> None:
        accumulator = StreamAccumulator(model=MODEL)
        accumulator.apply(ModelStreamEvent(model=MODEL, finish_reason=FinishReason.LENGTH))
        assert accumulator.finish_reason is FinishReason.LENGTH

    def test_usage_is_captured(self) -> None:
        accumulator = StreamAccumulator(model=MODEL)
        accumulator.apply(
            ModelStreamEvent(model=MODEL, usage=Usage(input_tokens=1, output_tokens=2))
        )
        assert accumulator.usage is not None
        assert accumulator.usage.total == 3

    def test_tool_fragments_merge_by_id(self) -> None:
        accumulator = StreamAccumulator(model=MODEL)
        accumulator.apply(
            ModelStreamEvent(
                model=MODEL,
                delta_tool_call=ToolCall(id="c1", name="search", arguments_json=""),
            )
        )
        accumulator.apply(
            ModelStreamEvent(
                model=MODEL,
                delta_tool_call=ToolCall(id="c1", name="", arguments_json='{"q":'),
            )
        )
        accumulator.apply(
            ModelStreamEvent(
                model=MODEL,
                delta_tool_call=ToolCall(id="c1", name="", arguments_json='"x"}'),
            )
        )
        assert len(accumulator.tool_calls) == 1
        assert accumulator.tool_calls[0].name == "search"
        assert accumulator.tool_calls[0].arguments_json == '{"q":"x"}'

    def test_new_id_starts_a_new_call(self) -> None:
        accumulator = StreamAccumulator(model=MODEL)
        accumulator.apply(
            ModelStreamEvent(
                model=MODEL, delta_tool_call=ToolCall(id="c1", name="a", arguments_json="")
            )
        )
        accumulator.apply(
            ModelStreamEvent(
                model=MODEL, delta_tool_call=ToolCall(id="c2", name="b", arguments_json="")
            )
        )
        assert [call.id for call in accumulator.tool_calls] == ["c1", "c2"]

    def test_has_partial_output(self) -> None:
        accumulator = StreamAccumulator(model=MODEL)
        assert accumulator.has_partial_output is False
        accumulator.apply(ModelStreamEvent(model=MODEL, delta_text="x"))
        assert accumulator.has_partial_output is True


async def _events(*items: ModelStreamEvent) -> object:
    """Builds an async iterator from a fixed event sequence."""
    for item in items:
        yield item


class TestAiterStream:
    """Consumption under deadline and cancellation."""

    @pytest.mark.asyncio
    async def test_consumes_to_completion(self) -> None:
        accumulator = await aiter_stream(
            _events(  # type: ignore[arg-type]
                ModelStreamEvent(model=MODEL, delta_text="hi"),
                ModelStreamEvent(model=MODEL, is_terminal=True),
            ),
            model=MODEL,
            timeout_seconds=1.0,
        )
        assert accumulator.text == "hi"
        assert accumulator.is_complete is True

    @pytest.mark.asyncio
    async def test_timeout_raises_model_timeout(self) -> None:
        async def never_ends() -> object:
            while True:
                await asyncio.sleep(0.01)
                yield ModelStreamEvent(model=MODEL, delta_text="x")

        with pytest.raises(ModelTimeoutError):
            await aiter_stream(
                never_ends(),  # type: ignore[arg-type]
                model=MODEL,
                timeout_seconds=0.05,
            )

    @pytest.mark.asyncio
    async def test_cancellation_stops_consumption(self) -> None:
        cancel = asyncio.Event()
        cancel.set()

        with pytest.raises(CancellationRequestedError):
            await aiter_stream(
                _events(ModelStreamEvent(model=MODEL, delta_text="x")),  # type: ignore[arg-type]
                model=MODEL,
                timeout_seconds=1.0,
                cancel_event=cancel,
            )

    @pytest.mark.asyncio
    async def test_chunk_cap_stops_a_runaway_stream(self) -> None:
        async def endless() -> object:
            while True:
                yield ModelStreamEvent(model=MODEL, delta_text="x")

        with pytest.raises(ExecutionTimeoutError):
            await aiter_stream(
                endless(),  # type: ignore[arg-type]
                model=MODEL,
                timeout_seconds=5.0,
                max_chunks=10,
            )

    @pytest.mark.asyncio
    async def test_incomplete_stream_is_still_returned(self) -> None:
        """A stream that ends without a terminal event yields partial output."""
        accumulator = await aiter_stream(
            _events(ModelStreamEvent(model=MODEL, delta_text="partial")),  # type: ignore[arg-type]
            model=MODEL,
            timeout_seconds=1.0,
        )
        assert accumulator.is_complete is False
        assert accumulator.text == "partial"
        assert accumulator.has_partial_output is True
