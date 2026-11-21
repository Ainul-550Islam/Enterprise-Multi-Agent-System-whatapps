"""Web search backends: Brave, Tavily, and SearXNG.

All three implement
:class:`~mas_orchestrator.tools.builtins.search.SearchBackend` and return
:class:`~mas_orchestrator.tools.builtins.search.SearchResult` records. Result
text is untrusted retrieved content, exactly like every other tool output: the
tool layer fences it, this layer only fetches and maps it.

Failure taxonomy (shared):

* bad input -- :class:`~mas_orchestrator.contracts.errors.InvalidRequestError`;
* the provider refused the query (HTTP 4xx) --
  :class:`~mas_orchestrator.contracts.errors.ToolFailedError`;
* the provider could not be reached (network / HTTP 5xx) --
  :class:`~mas_orchestrator.contracts.errors.DependencyUnavailableError`.
"""

from __future__ import annotations

import ipaddress
from typing import Any
from urllib.parse import quote_plus, urlsplit

from mas_orchestrator.contracts.errors import (
    DependencyUnavailableError,
    InvalidRequestError,
    ToolFailedError,
)
from mas_orchestrator.security.credentials import Secret
from mas_orchestrator.tools.builtins.search import SearchResult


def _require_query(query: str) -> str:
    cleaned = query.strip()
    if not cleaned:
        raise InvalidRequestError("search query must not be empty", details={"field": "query"})
    return cleaned


def _require_limit(limit: int) -> int:
    if not isinstance(limit, int) or not 1 <= limit <= 50:
        raise InvalidRequestError(
            "search limit must be between 1 and 50", details={"field": "limit"}
        )
    return limit


async def _guarded_request(transport: Any, method: str, url: str, **kwargs: Any) -> Any:
    try:
        return await transport.request(method, url, **kwargs)
    except (InvalidRequestError, ToolFailedError, DependencyUnavailableError):
        raise
    except Exception as error:
        raise DependencyUnavailableError(
            "search provider could not be reached",
            details={"url": url},
        ) from error


def _result_list(document: Any, *, provider: str) -> list[dict[str, Any]]:
    if not isinstance(document, dict):
        raise ToolFailedError(
            f"{provider} returned a response that is not a JSON object",
            details={"provider": provider},
        )
    results = document.get("results")
    if results is None:
        raise ToolFailedError(
            f"{provider} returned no result list",
            details={"provider": provider},
        )
    if not isinstance(results, list):
        raise ToolFailedError(
            f"{provider} returned a malformed result list",
            details={"provider": provider},
        )
    return [item for item in results if isinstance(item, dict)]


def _map_result(item: dict[str, Any], *, field: str = "content") -> SearchResult | None:
    url = item.get("url")
    if not isinstance(url, str) or not url.strip():
        return None
    title = item.get("title")
    snippet = item.get(field) or item.get("description") or item.get("snippet") or ""
    score = item.get("score")
    return SearchResult(
        title=str(title) if isinstance(title, (str, int, float)) else "",
        url=url.strip(),
        snippet=str(snippet),
        score=float(score) if isinstance(score, (int, float)) else None,
    )


class BraveSearchBackend:
    """Search through the Brave Search API.

    Args:
        transport: The platform guarded HTTP transport.
        api_key: Brave ``X-Subscription-Token`` value.

    Raises:
        InvalidRequestError: If the configuration is malformed.
    """

    _ENDPOINT = "https://api.search.brave.com/res/v1/web/search"

    def __init__(self, *, transport: Any, api_key: Secret) -> None:
        if transport is None or not hasattr(transport, "request"):
            raise InvalidRequestError(
                "brave backend requires the guarded transport", details={"field": "transport"}
            )
        if not isinstance(api_key, Secret):
            raise InvalidRequestError(
                "brave api_key must be a Secret", details={"field": "api_key"}
            )
        self._transport = transport
        self._api_key = api_key

    async def search(self, query: str, *, limit: int, timeout_seconds: float) -> list[SearchResult]:
        cleaned = _require_query(query)
        count = _require_limit(limit)
        url = f"{self._ENDPOINT}?q={quote_plus(cleaned)}&count={count}"
        response = await _guarded_request(
            self._transport,
            "GET",
            url,
            headers={"x-subscription-token": self._api_key.reveal()},
        )
        if response.status_code != 200:
            raise _refusal_or_unavailable(response.status_code, "brave")
        document = response.json()
        if not isinstance(document, dict) or not isinstance(document.get("web"), dict):
            raise ToolFailedError(
                "brave returned a response without a web block", details={"provider": "brave"}
            )
        items = document["web"].get("results")
        if not isinstance(items, list):
            raise ToolFailedError(
                "brave returned a malformed web result list", details={"provider": "brave"}
            )
        mapped = [
            candidate
            for item in items
            if isinstance(item, dict)
            for candidate in [_map_result(item, field="description")]
            if candidate is not None
        ]
        return mapped[:count]


class TavilySearchBackend:
    """Search through the Tavily search API.

    Args:
        transport: The platform guarded HTTP transport.
        api_key: Tavily API key (sent as a bearer credential).

    Raises:
        InvalidRequestError: If the configuration is malformed.
    """

    _ENDPOINT = "https://api.tavily.com/search"

    def __init__(self, *, transport: Any, api_key: Secret) -> None:
        if transport is None or not hasattr(transport, "request"):
            raise InvalidRequestError(
                "tavily backend requires the guarded transport", details={"field": "transport"}
            )
        if not isinstance(api_key, Secret):
            raise InvalidRequestError(
                "tavily api_key must be a Secret", details={"field": "api_key"}
            )
        self._transport = transport
        self._api_key = api_key

    async def search(self, query: str, *, limit: int, timeout_seconds: float) -> list[SearchResult]:
        cleaned = _require_query(query)
        count = _require_limit(limit)
        response = await _guarded_request(
            self._transport,
            "POST",
            self._ENDPOINT,
            json_body={"query": cleaned, "max_results": count},
            headers={
                "authorization": f"Bearer {self._api_key.reveal()}",
                "content-type": "application/json",
            },
        )
        if response.status_code != 200:
            raise _refusal_or_unavailable(response.status_code, "tavily")
        mapped = [
            candidate
            for item in _result_list(response.json(), provider="tavily")
            for candidate in [_map_result(item)]
            if candidate is not None
        ]
        return mapped[:count]


class SearxngSearchBackend:
    """Search through a self-hosted SearXNG instance.

    Args:
        transport: The platform guarded HTTP transport.
        base_url: The instance base URL, e.g. ``https://search.example.com``.
        timeout_seconds_default: Kept for interface symmetry; each call's
            ``timeout_seconds`` is the budget.

    Raises:
        InvalidRequestError: If the configuration is malformed.
    """

    def __init__(
        self,
        *,
        transport: Any,
        base_url: str,
        allowed_hosts: frozenset[str] | None = None,
        allow_private_hosts: bool = False,
    ) -> None:
        if transport is None or not hasattr(transport, "request"):
            raise InvalidRequestError(
                "searxng backend requires the guarded transport", details={"field": "transport"}
            )
        cleaned = base_url.strip().rstrip("/")
        if not cleaned.startswith(("https://", "http://")):
            raise InvalidRequestError(
                "searxng base_url must be an absolute http(s) URL",
                details={"field": "base_url"},
            )
        host = (urlsplit(cleaned).hostname or "").lower()
        if not host:
            raise InvalidRequestError("searxng base_url has no host", details={"field": "base_url"})
        allowlist = allowed_hosts if allowed_hosts is not None else frozenset({host})
        if host not in {item.lower() for item in allowlist}:
            raise InvalidRequestError(
                "searxng host is not on the operator allowlist",
                details={"field": "base_url", "host": host},
            )
        if not allow_private_hosts:
            if host in {"localhost", "localhost.localdomain"}:
                raise InvalidRequestError(
                    "searxng host is loopback; set allow_private_hosts for operator instances",
                    details={"field": "base_url"},
                )
            try:
                address = ipaddress.ip_address(host)
            except ValueError:
                address = None
            if address is not None:
                from mas_orchestrator.providers.base_http import is_blocked_address

                if is_blocked_address(str(address)):
                    raise InvalidRequestError(
                        "searxng host is a private or loopback address",
                        details={"field": "base_url"},
                    )
        self._transport = transport
        self._base_url = cleaned

    async def search(self, query: str, *, limit: int, timeout_seconds: float) -> list[SearchResult]:
        cleaned = _require_query(query)
        count = _require_limit(limit)
        url = f"{self._base_url}/search?q={quote_plus(cleaned)}&format=json"
        response = await _guarded_request(self._transport, "GET", url)
        if response.status_code != 200:
            raise _refusal_or_unavailable(response.status_code, "searxng")
        mapped = [
            candidate
            for item in _result_list(response.json(), provider="searxng")
            for candidate in [_map_result(item)]
            if candidate is not None
        ]
        return mapped[:count]


def _refusal_or_unavailable(status_code: int, provider: str) -> Exception:
    if 500 <= status_code < 600:
        return DependencyUnavailableError(
            f"{provider} is unavailable",
            details={"provider": provider, "status_code": status_code},
        )
    return ToolFailedError(
        f"{provider} refused the query",
        details={"provider": provider, "status_code": status_code},
    )
