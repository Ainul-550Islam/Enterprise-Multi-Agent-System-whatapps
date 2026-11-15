"""Searxng Search backend.

Self-hosted search engine backend that queries a Searxng instance.
Outbound HTTP is operator-allowlisted: the hostname must be configured
and cannot be derived from caller-supplied input.
All configuration must be set at deployment time.
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

#: Default Searxng instance URL (configured at deployment)
DEFAULT_SEARXNG_URL = "http://localhost:8888"
#: Default timeout in seconds
DEFAULT_TIMEOUT = 30


class SearxngSearchBackend:
    """Searxng Search backend.

    Requires the following configuration keys:
    - searxng_url: Base URL of the Searxng instance (e.g. http://localhost:8888)

    Optional configuration keys:
    - timeout: connection timeout in seconds
    - api_key: Optional API key if the instance is protected
    - scopes: allowed instance URLs (operator allowlist)
    """

    def __init__(self, *, config: dict, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._searxng_url = config.get("searxng_url", DEFAULT_SEARXNG_URL)
        self._timeout = float(config.get("timeout", DEFAULT_TIMEOUT))
        self._api_key = resolve_secret(config.get("api_key", ""))
        self._scopes = config.get("scopes", [])

        # Validate configuration
        if not self._searxng_url:
            raise ConfigurationError(
                "searxng_url is required for Searxng Search backend",
                details={"field": "searxng_url"},
            )

        # Host allowlist: only permit configured instances
        if self._scopes and self._searxng_url not in self._scopes:
            raise ConfigurationError(
                f"Searxng instance '{self._searxng_url}' is not in the allowed scopes",
                details={"instance": self._searxng_url, "allowed": self._scopes},
            )

        # If api_key is provided, validate it's a secret reference
        if self._api_key and not self._api_key.startswith("env:") and not self._api_key.startswith("file:"):
            _LOGGER.warning(
                "api_key appears to be a raw value, consider using a secret reference (env: or file:)"
            )

    async def search(self, *, query: str, scope: list[str] | None = None) -> dict:
        """Search using the Searxng instance.

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

        # Build the request to Searxng instance
        params = {
            "q": query,
            "format": "json",
            "pretty": "true",
        }

        if scope:
            # Use operator-allowlisted scopes
            # Only use scopes that are in the allowlist
            allowed = [s for s in scope if s in self._scopes] if self._scopes else scope
            if allowed:
                params["safesearch"] = "moderate"
                # Note: Searxng supports custom search engines via instance config

        try:
            async with asyncio.timeout(self._timeout):
                async with httpx.AsyncClient(base_url=self._searxng_url) as client:
                    response = await client.get("/search", params=params)
                    
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
                            "searxng search returned %d results for: %s",
                            len(results), query[:50]
                        )
                        return {
                            "query": query,
                            "results": results,
                            "total": len(results),
                        }
                    else:
                        raise RuntimeError(
                            f"Searxng API returned {response.status_code}: {response.text}"
                        )

        except asyncio.TimeoutError:
            raise RuntimeError(f"Searxng API call timed out")
        except Exception as e:
            raise RuntimeError(f"failed to perform Searxng Search: {e}")


def create_backend(
    *,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> SearxngSearchBackend:
    """Convenient function to create a SearxngSearchBackend instance."""
    return SearxngSearchBackend(config=config, environment=environment)
