"""Tavily Search backend.

Searches the web using the Tavily API.
All API keys must be resolved from secrets — no plaintext keys.
Outbound HTTP is pinned to api.tavily.com to prevent SSRF.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import resolve_secret

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Tavily API endpoint (pinned, not configurable via input)
TAVILY_API_URL = "https://api.tavily.com/search"
#: Default number of results to return
DEFAULT_RESULTS = 10
#: Default timeout in seconds
DEFAULT_TIMEOUT = 30


class TavilySearchBackend:
    """Tavily Search backend.

    Requires the following configuration keys:
    - api_key: Secret reference for Tavily API key (e.g. env:TAVILY_API_KEY)

    Optional configuration keys:
    - results: number of results to return (1-10)
    - timeout: connection timeout in seconds
    - include_answer: whether to include the generated answer
    """

    def __init__(self, *, config: dict, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._api_key = resolve_secret(config.get("api_key", ""))
        self._results = int(config.get("results", DEFAULT_RESULTS))
        self._timeout = float(config.get("timeout", DEFAULT_TIMEOUT))
        self._include_answer = config.get("include_answer", True)

        # Validate results count
        if self._results < 1 or self._results > 10:
            raise ConfigurationError(
                f"results must be between 1 and 10, got {self._results}",
                details={"field": "results"},
            )

        # Validate configuration
        if not self._api_key:
            raise ConfigurationError(
                "api_key (secret reference) is required for Tavily Search",
                details={"field": "api_key"},
            )

    async def search(self, *, query: str, scope: list[str] | None = None) -> dict:
        """Search the web using Tavily.

        Args:
            query: Search query string.
            scope: Optional list of domains or scopes to restrict results.

        Returns:
            Dictionary with search results including titles, URLs, and snippets.

        Raises:
            ConfigurationError: If the backend is not properly configured.
            RuntimeError: If the API call fails.
        """
        # Validate query
        if not query or not query.strip():
            raise ConfigurationError(
                "search query must not be empty",
                details={"field": "query"},
            )

        # Build the API request
        params = {
            "query": query,
            "limit": self._results,
            "include_answer": self._include_answer,
            "search_depth": "basic",
        }

        if scope:
            # Restrict to specified domains
            params["include_domains"] = scope

        try:
            async with asyncio.timeout(self._timeout):
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        TAVILY_API_URL,
                        params=params,
                        headers={
                            "Authorization": f"Bearer {self._api_key}",
                        },
                    )
                    
                    if response.is_success:
                        data = response.json()
                        results = []
                        for result in data.get("results", []):
                            results.append({
                                "title": result.get("title", ""),
                                "url": result.get("url", ""),
                                "content": result.get("content", ""),
                                "score": result.get("score", 0.0),
                            })
                        
                        _LOGGER.info(
                            "tavily search returned %d results for: %s",
                            len(results), query[:50]
                        )
                        return {
                            "query": query,
                            "results": results,
                            "total": len(results),
                            "answer": data.get("answer", ""),
                        }
                    else:
                        raise RuntimeError(
                            f"Tavily API returned {response.status_code}: {response.text}"
                        )

        except asyncio.TimeoutError:
            raise RuntimeError(f"Tavily API call timed out")
        except Exception as e:
            raise RuntimeError(f"failed to perform Tavily Search: {e}")


def create_backend(
    *,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> TavilySearchBackend:
    """Convenient function to create a TavilySearchBackend instance."""
    return TavilySearchBackend(config=config, environment=environment)
