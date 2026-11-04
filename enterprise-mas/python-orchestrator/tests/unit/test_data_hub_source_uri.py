"""Citation URI normalization at the Data Hub trust boundary."""

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.data_hub.service import _safe_source_uri


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("https://docs.example.test/guide?token=secret#section", "https://docs.example.test/guide"),
        ("/guide?token=secret#section", "/guide"),
        ("guide?token=secret", "guide"),
        ("?token=secret", ""),
    ],
)
def test_source_uri_drops_query_and_fragment_even_for_relative_paths(
    value: str | None, expected: str | None
) -> None:
    assert _safe_source_uri(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "//evil.example/path?token=secret",
        "https://user:pass@docs.example.test/path",
        "javascript:alert(1)",
        "https:///missing-host",
        "https://[broken/path",
        "https://docs.example.test:bad/path",
        "https://docs.example.test/\tsecret",
        "https://docs.example.test/\\tsecret",
        "\\\\evil.example/path",
        " https://docs.example.test/",
    ],
)
def test_source_uri_rejects_ambiguous_or_unsafe_urls(value: str) -> None:
    with pytest.raises(InvalidRequestError):
        _safe_source_uri(value)
