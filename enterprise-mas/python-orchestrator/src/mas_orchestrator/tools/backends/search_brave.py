"""Brave Search backend.

Searches the web using the Brave Search API.
All API keys must be resolved from secrets — no plaintext keys.
Outbound HTTP is pinned to api.brave.com to prevent SSRF.
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

#: Brave Search API endpoint (pinned, not configurable via input)
BRAVE_API_URL = "https://api.brave.com/research/v1/search"
#: Default number of results to return
DEFAULT_RESULTS = 10
#: Default timeout in seconds
DEFAULT_TIMEOUT = 30


class BraveSearchBackend:
    """Brave Search backend.

    Requires the following configuration keys:
    - api_key: Secret reference for Brave API key (e.g. env:BRAVE_API_KEY)

    Optional configuration keys:
    - results: number of results to return (1-50)
    - timeout: connection timeout in seconds
    """

    def __init__(self, *, config: dict, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._api_key = resolve_secret(config.get("api_key", ""))
        self._results = int(config.get("results", DEFAULT_RESULTS))
        self._timeout = float(config.get("timeout", DEFAULT_TIMEOUT))

        # Validate results count
        if self._results < 1 or self._results > 50:
            raise ConfigurationError(
                f"results must be between 1 and 50, got {self._results}",
                details={"field": "results"},
            )

        # Validate configuration
        if not self._api_key:
            raise ConfigurationError(
                "api_key (secret reference) is required for Brave Search",
                details={"field": "api_key"},
            )

    async def search(self, *, query: str, scope: list[str] | None = None) -> dict:
        """Search the web using Brave Search.

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
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self._api_key,
        }

        params = {
            "q": query,
            "count": self._results,
            "search_lang": "en",
        }

        if scope:
            # Restrict to specified domains
            params["filter"] = "web"
            # Note: scope filtering is applied client-side after API response

        try:
            async with asyncio.timeout(self._timeout):
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        BRAVE_API_URL,
                        headers=headers,
                        params=params,
                    )
                    
                    if response.is_success:
                        data = response.json()
                        results = []
                        for result in data.get("results", []):
                            # Client-side scope filtering
                            if scope:
                                # Check if result URL matches allowed scopes
                                result_url = result.get("url", "")
                                if not any(s in result_url for s in scope):
                                    continue
                            results.append({
                                "title": result.get("title", ""),
                                "url": result.get("url", ""),
                                "description": result.get("description", ""),
                                "score": result.get("score", 0.0),
                            })
                        
                        _LOGGER.info(
                            "brave search returned %d results for: %s",
                            len(results), query[:50]
                        )
                        return {
                            "query": query,
                            "results": results,
                            "total": len(results),
                        }
                    else:
                        raise RuntimeError(
                            f"Brave Search API returned {response.status_code}: {response.text}"
                        )

        except asyncio.TimeoutError:
            raise RuntimeError(f"Brave Search API call timed out")
        except Exception as e:
            raise RuntimeError(f"failed to perform Brave Search: {e}")


def create_backend(
    *,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> BraveSearchBackend:
    """Convenient function to create a BraveSearchBackend instance."""
    return BraveSearchBackend(config=config, environment=environment)
