"""Database tool builtin - factory hook.

Provides the read-only database query capability through the configured
PostgresReadOnly backend. This is a factory hook that routes to the
appropriate backend based on the configuration.
"""

from __future__ import annotations

import asyncio
from mas_orchestrator.tools.backends import build_backend
from mas_orchestrator.config.enums import Environment


def query_database(
    *,
    backend_kind: str,
    config: dict,
    environment: Environment,
    sql: str,
) -> dict:
    """Execute a read-only database query using the configured backend.

    Args:
        backend_kind: The backend kind string (e.g. "postgres_ro").
        config: Backend configuration dictionary.
        environment: Current deployment environment.
        sql: SQL SELECT query to execute.

    Returns:
        Dictionary with "columns" and "rows" keys.

    Raises:
        ConfigurationError: If the backend is not properly configured.
    """
    backend = build_backend(
        kind=backend_kind,
        config=config,
        environment=environment,
    )
    return asyncio.run(backend.query(
        sql=sql,
    ))
