"""Brave, Tavily, SearXNG: guarded HTTP and SSRF/allowlist refusals."""

from __future__ import annotations

from typing import Any

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError, ToolFailedError
from mas_orchestrator.providers.base_http import TransportResponse
from mas_orchestrator.security.credentials import Secret
from mas_orchestrator.tools.backends.search import (
    BraveSearchBackend,
    SearxngSearchBackend,
    TavilySearchBackend,
)


class _RecordingTransport:
    def __init__(self, status_code: int = 200, body: bytes = b"{}") -> None:
        self.calls: list[dict[str, Any]] = []
        self._status = status_code
        self._body = body

    async def request(self, method: str, url: str, **kwargs: Any) -> TransportResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return TransportResponse(status_code=self._status, body=self._body, headers={})


class _SsrfTransport:
    async def request(self, method: str, url: str, **kwargs: Any) -> TransportResponse:
        raise InvalidRequestError("redirect to a private address is refused", details={"url": url})


async def test_brave_maps_web_results() -> None:
    body = b'{"web":{"results":[{"title":"T","url":"https://example.com/a","description":"D"}]}}'
    transport = _RecordingTransport(200, body)
    backend = BraveSearchBackend(
        transport=transport, api_key=Secret("k", reference="test://search")
    )
    results = await backend.search("hello", limit=5, timeout_seconds=5.0)
    assert results[0].url == "https://example.com/a"
    assert "api.search.brave.com" in transport.calls[0]["url"]
    assert transport.calls[0]["headers"]["x-subscription-token"] == "k"


async def test_tavily_posts_json() -> None:
    body = b'{"results":[{"title":"T","url":"https://example.com/a","content":"C"}]}'
    transport = _RecordingTransport(200, body)
    backend = TavilySearchBackend(
        transport=transport, api_key=Secret("k", reference="test://search")
    )
    results = await backend.search("hello", limit=3, timeout_seconds=5.0)
    assert results[0].snippet == "C"
    assert transport.calls[0]["method"] == "POST"
    assert transport.calls[0]["json_body"]["query"] == "hello"


async def test_searxng_requires_operator_allowlisted_host() -> None:
    with pytest.raises(InvalidRequestError, match="allowlist"):
        SearxngSearchBackend(
            transport=_RecordingTransport(),
            base_url="https://evil.example.com",
            allowed_hosts=frozenset({"search.example.com"}),
        )


def test_searxng_refuses_loopback_and_private_hosts() -> None:
    with pytest.raises(InvalidRequestError, match="loopback"):
        SearxngSearchBackend(transport=_RecordingTransport(), base_url="http://127.0.0.1:8888")
    with pytest.raises(InvalidRequestError, match="private"):
        SearxngSearchBackend(transport=_RecordingTransport(), base_url="http://10.0.0.9:8080")


async def test_searxng_ssrf_redirect_is_refused() -> None:
    backend = SearxngSearchBackend(
        transport=_SsrfTransport(), base_url="https://search.example.com"
    )
    with pytest.raises(InvalidRequestError, match="private address"):
        await backend.search("q", limit=1, timeout_seconds=5.0)


async def test_empty_query_is_refused() -> None:
    backend = BraveSearchBackend(
        transport=_RecordingTransport(), api_key=Secret("k", reference="test://search")
    )
    with pytest.raises(InvalidRequestError, match="query"):
        await backend.search("  ", limit=1, timeout_seconds=5.0)


async def test_brave_4xx_is_a_typed_failure() -> None:
    backend = BraveSearchBackend(
        transport=_RecordingTransport(403, b"{}"), api_key=Secret("k", reference="test://search")
    )
    with pytest.raises(ToolFailedError):
        await backend.search("q", limit=1, timeout_seconds=5.0)
