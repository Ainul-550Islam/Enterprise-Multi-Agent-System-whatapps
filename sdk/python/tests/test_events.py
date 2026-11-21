"""SSE frame parsing for the platform event stream."""

from __future__ import annotations

from mas_sdk.client import parse_event_frame


def test_event_parsing() -> None:
    frame = parse_event_frame(
        "event: execution.update\nid: 42\ndata: {\"status\": \"running\"}\n\n"
    )
    assert frame["event"] == "execution.update"
    assert frame["id"] == "42"
    assert frame["data"] == '{"status": "running"}'


def test_multi_line_data_joins_with_newlines() -> None:
    frame = parse_event_frame("data: line one\ndata: line two\nretry: 2500\n")
    assert frame["event"] == "message"
    assert frame["data"] == "line one\nline two"
    assert frame["retry"] == "2500"


def test_empty_frames_keep_defaults() -> None:
    frame = parse_event_frame("")
    assert frame["event"] == "message"
    assert frame["data"] == ""
