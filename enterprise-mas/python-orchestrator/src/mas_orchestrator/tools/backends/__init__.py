"""Tool backends factory and registry.

Provides a configuration-driven way to instantiate various tool backends
(e.g. email, search, database, sandbox) based on the active settings.
Each backend is registered here and constructed via the factory function.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Registry of backend constructors, keyed by backend kind.
_BACKEND_REGISTRY: dict[str, callable] = {}


def register_backend(kind: str) -> callable:
    """Decorator to register a backend constructor by kind string.

    Args:
        kind: The configuration kind string (e.g. "smtp", "sendgrid", "brave",
            "tavily", "searxng", "postgres_ro", "sandbox_remote").
    
    Returns:
        The decorated constructor function.
    """
    def decorator(func: callable) -> callable:
        if kind in _BACKEND_REGISTRY:
            _LOGGER.warning(
                "overwriting existing backend registration for kind: %s", kind
            )
        _BACKEND_REGISTRY[kind] = func
        _LOGGER.info("registered backend kind: %s", kind)
        return func
    return decorator


def get_backend_registry() -> dict[str, callable]:
    """Return the current backend registry (for testing/inspection)."""
    return _BACKEND_REGISTRY.copy()


def build_backend(
    *,
    kind: str,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> object:
    """Construct a backend instance from its kind and configuration.

    Args:
        kind: The backend kind string from the registry.
        config: Configuration dictionary for the backend.
        environment: Current deployment environment.

    Returns:
        An initialized backend instance.

    Raises:
        ConfigurationError: If the kind is unknown or configuration is invalid.
    """
    from mas_orchestrator.tools.backends.email_smtp import SmtpEmailBackend
    from mas_orchestrator.tools.backends.email_sendgrid import SendGridEmailBackend
    from mas_orchestrator.tools.backends.search_brave import BraveSearchBackend
    from mas_orchestrator.tools.backends.search_tavily import TavilySearchBackend
    from mas_orchestrator.tools.backends.search_searxng import SearxngSearchBackend
    from mas_orchestrator.tools.backends.database_postgres_ro import PostgresReadOnlyBackend
    from mas_orchestrator.tools.backends.sandbox_remote import RemoteSandbox

    registry = get_backend_registry()

    if kind not in registry:
        raise ConfigurationError(
            f"unknown backend kind: {kind}",
            details={"kind": kind, "available": sorted(registry.keys())},
        )

    constructor = registry[kind]
    return constructor(config=config, environment=environment)
