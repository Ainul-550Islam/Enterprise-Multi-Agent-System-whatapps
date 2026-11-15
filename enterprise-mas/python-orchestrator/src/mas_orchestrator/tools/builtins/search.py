"""Search tool builtin - factory hook.

Provides the search capability through the configured backend.
This is a factory hook that routes to the appropriate backend
based on the configuration.
"""

from __future__ import annotations

import asyncio
from mas_orchestrator.tools.backends import build_backend
from mas_orchestrator.config.enums import Environment


def search_web(
    *,
    backend_kind: str,
    config: dict,
    environment: Environment,
    query: str,
    scope: list[str] | None = None,
) -> dict:
    """Search the web using the configured backend.

    Args:
        backend_kind: The backend kind string (e.g. "brave", "tavily", "searxng").
        config: Backend configuration dictionary.
        environment: Current deployment environment.
        query: Search query string.
        scope: Optional list of domains or scopes to restrict results.

    Returns:
        Dictionary with search results including titles, URLs, and snippets.

    Raises:
        ConfigurationError: If the backend is not properly configured.
    """
    backend = build_backend(
        kind=backend_kind,
        config=config,
        environment=environment,
    )
    return asyncio.run(backend.search(
        query=query,
        scope=scope,
    ))
