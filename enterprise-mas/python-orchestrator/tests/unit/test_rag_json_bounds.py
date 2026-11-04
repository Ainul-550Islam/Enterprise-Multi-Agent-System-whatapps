"""JSON output amplification and malformed-input bounds."""

import json

import pytest

import mas_orchestrator.rag.parsing as parsing
from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.rag.models import DocumentFormat


def test_json_flatten_stops_before_accumulating_over_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(parsing, "MAX_PARSED_CHARS", 120)
    observed: list[int] = []
    original = parsing._append_limited

    def record(lines: list[str], value: str, chars: int) -> int:
        observed.append(len(lines))
        return original(lines, value, chars)

    monkeypatch.setattr(parsing, "_append_limited", record)
    raw = json.dumps({"a" * 40: list(range(1000))})
    with pytest.raises(InvalidRequestError, match="maximum size"):
        parsing.Parser().parse(raw, DocumentFormat.JSON)
    assert observed == [0, 1, 2]


def test_json_with_deep_nesting_fails_as_invalid_request() -> None:
    raw = "[" * 3000 + "0" + "]" * 3000
    with pytest.raises(InvalidRequestError, match="JSON"):
        parsing.Parser().parse(raw, DocumentFormat.JSON)


def test_json_with_integer_beyond_python_decoder_limit_fails_safely() -> None:
    raw = '{"number":' + "9" * 10000 + "}"
    with pytest.raises(InvalidRequestError, match="bounded JSON") as error:
        parsing.Parser().parse(raw, DocumentFormat.JSON)
    assert "999999" not in str(error.value)


def test_small_raw_json_cannot_expand_past_real_output_limit() -> None:
    raw = json.dumps({"a" * 50_000: [0] * 200})
    assert len(raw) < 60_000
    with pytest.raises(InvalidRequestError, match="maximum size"):
        parsing.Parser().parse(raw, DocumentFormat.JSON)


def test_json_rejects_nesting_over_explicit_limit_even_when_decoder_accepts_it() -> None:
    raw = "[" * (parsing.MAX_JSON_DEPTH + 1) + "0" + "]" * (parsing.MAX_JSON_DEPTH + 1)
    with pytest.raises(InvalidRequestError, match="nesting"):
        parsing.Parser().parse(raw, DocumentFormat.JSON)


def test_json_flatten_preserves_empty_key_path_format() -> None:
    assert parsing.Parser().parse('{"": {"a": 1}}', DocumentFormat.JSON) == "a: 1"
