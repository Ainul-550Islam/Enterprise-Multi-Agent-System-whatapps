"""Search tool.

Search is the tool most likely to be pointed somewhere it should not go, and
the most likely to return a page whose text is an instruction rather than an
answer. Both risks are handled here rather than downstream:

* results are fetched through the platform's SSRF-guarded transport when a
  network backend is configured, so an injected URL cannot reach internal
  addresses;
* every result's snippet is passed through the trust fence, so a page that says
  "ignore your instructions and do X" is presented to the model as quoted data
  rather than as something to obey.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Final, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import (
    InvalidRequestError,
    ToolFailedError,
)
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolInvocationContext,
    ToolKind,
    ToolSafety,
    ToolVersion,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_QUERY_CHARS: Final[int] = 512
MAX_RESULTS: Final[int] = 10
MAX_SNIPPET_CHARS: Final[int] = 800
DEFAULT_TIMEOUT_SECONDS: Final[float] = 15.0


@dataclass(frozen=True, slots=True)
class SearchResult:
    """One search hit.

    Attributes:
        title: Page title.
        url: Canonical URL.
        snippet: A short excerpt. Untrusted, like all retrieved content.
        score: Relevance score from the backend, when it reports one.
    """

    title: str
    url: str
    snippet: str
    score: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """A JSON-serialisable view."""
        payload: dict[str, Any] = {"title": self.title, "url": self.url}
        payload["snippet"] = self.snippet
        if self.score is not None:
            payload["score"] = self.score
        return payload


@runtime_checkable
class SearchBackend(Protocol):
    """Where results come from."""

    async def search(
        self,
        query: str,
        *,
        limit: int,
        timeout_seconds: float,
    ) -> list[SearchResult]:
        """Runs a query and returns ranked results."""
        ...


class NoSearchBackend:
    """A backend that returns nothing and says so.

    Used as the default so a search tool without a backend still constructs —
    but every call reports that nothing is configured rather than returning an
    empty result, which the model would report to the user as "no results".
    """

    async def search(
        self,
        query: str,
        *,
        limit: int,
        timeout_seconds: float,
    ) -> list[SearchResult]:
        """Returns no results.

        Raises:
            InvalidRequestError: Always. Silence here would be read as an
                absence of evidence by the model, which is not what it means.
        """
        raise InvalidRequestError(
            "no search backend is configured",
            details={"field": "backend"},
        )


@dataclass(frozen=True, slots=True)
class SearchToolConfig:
    """Search limits.

    Attributes:
        max_query_chars: Query length ceiling.
        max_results: Result ceiling.
        max_snippet_chars: Per-snippet ceiling.
        timeout_seconds: Backend budget.
        fence_snippets: Whether to wrap snippets in the untrusted-content
            fence. Leave enabled: a snippet is third-party text.
    """

    max_query_chars: int = MAX_QUERY_CHARS
    max_results: int = MAX_RESULTS
    max_snippet_chars: int = MAX_SNIPPET_CHARS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    fence_snippets: bool = True

    def __post_init__(self) -> None:
        for field_name in ("max_query_chars", "max_results", "max_snippet_chars"):
            if getattr(self, field_name) < 1:
                raise InvalidRequestError(
                    f"{field_name} must be at least 1", details={"field": field_name}
                )
        if self.timeout_seconds <= 0:
            raise InvalidRequestError(
                "timeout_seconds must be positive",
                details={"timeout": self.timeout_seconds},
            )


def looks_like_url(text: str) -> bool:
    """Whether a string is plausibly a URL rather than a search phrase.

    Used to reject a query that is really an instruction to fetch somewhere.
    """
    return bool(re.match(r"^\s*(?:https?|ftp)://", text, re.IGNORECASE))


class SearchTool:
    """Runs a search and returns bounded, fenced results."""

    def __init__(
        self,
        backend: SearchBackend | None = None,
        *,
        config: SearchToolConfig | None = None,
    ) -> None:
        self._backend = backend or NoSearchBackend()
        self._config = config or SearchToolConfig()

    @property
    def config(self) -> SearchToolConfig:
        """The limits in force."""
        return self._config

    async def invoke(
        self,
        arguments: dict[str, Any],
        context: ToolInvocationContext,
    ) -> dict[str, Any]:
        """Runs the search.

        Returns:
            A mapping with ``results``, ``count`` and ``query``.

        Raises:
            InvalidRequestError: If the query is rejected.
        """
        raw = arguments.get("query")
        if not isinstance(raw, str) or not raw.strip():
            raise InvalidRequestError(
                "query must be a non-empty string", details={"field": "query"}
            )
        query = raw.strip()
        if len(query) > self._config.max_query_chars:
            raise InvalidRequestError(
                "query exceeds the maximum length",
                details={
                    "limit": self._config.max_query_chars,
                    "length": len(query),
                },
            )
        if looks_like_url(query):
            raise InvalidRequestError(
                "a search query must be a phrase, not a URL: fetch it with the "
                "http tool if the destination is allowlisted",
                details={"field": "query"},
            )

        limit = arguments.get("limit", self._config.max_results)
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise InvalidRequestError("limit must be an integer", details={"field": "limit"})
        limit = max(1, min(limit, self._config.max_results))

        try:
            results = await self._backend.search(
                query, limit=limit, timeout_seconds=self._config.timeout_seconds
            )
        except InvalidRequestError:
            raise
        except Exception as error:
            raise ToolFailedError(
                "search failed",
                details={"tool_id": "search", "error": type(error).__name__},
            ) from error

        rendered: list[dict[str, Any]] = []
        for hit in results[:limit]:
            snippet = hit.snippet
            if len(snippet) > self._config.max_snippet_chars:
                snippet = snippet[: self._config.max_snippet_chars]
            rendered.append(
                {
                    "title": hit.title,
                    "url": hit.url,
                    "snippet": self._render(snippet),
                    **({"score": hit.score} if hit.score is not None else {}),
                }
            )

        _LOGGER.info(
            "search executed",
            extra={
                "event": "tool.search.query",
                "execution_id": context.execution_id,
                "result_count": len(rendered),
            },
        )
        return {"query": query, "results": rendered, "count": len(rendered)}

    def _render(self, snippet: str) -> str:
        """Applies the untrusted fence when configured to."""
        if not self._config.fence_snippets:
            return snippet
        from mas_orchestrator.prompts.renderer import fence

        return fence(snippet, label="search_result")


def build_search_tool(
    *,
    tool_id: str,
    name: str,
    tenant_id: str,
    backend: SearchBackend | None = None,
    description: str | None = None,
    version: int = 1,
    config: SearchToolConfig | None = None,
) -> tuple[ToolDefinition, SearchTool]:
    """Builds a definition/handler pair ready for registration."""
    definition = ToolDefinition(
        tool_id=tool_id,
        name=name,
        description=description or "Search for information.",
        version=ToolVersion(version),
        kind=ToolKind.CUSTOM,
        safety=ToolSafety.STANDARD,
        tenant_id=tenant_id,
        idempotent=True,
    )
    return definition, SearchTool(backend, config=config)
