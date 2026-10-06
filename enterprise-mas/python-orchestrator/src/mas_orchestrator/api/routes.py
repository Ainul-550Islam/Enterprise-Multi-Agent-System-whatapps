"""Route registration and versioning.

Routes are a table, not decorators. That is a deliberate choice: a table can be
inspected, tested, and checked for duplicates, whereas a set of decorators
scattered across modules can silently register the same path twice with the
second definition winning.

Versioning is in the path (``/v1/...``). A version in a header is invisible in
logs and in access control rules, both of which are places where knowing which
contract a caller is using matters.

Every route declares whether it is served while the service is draining. Work
routes are not: accepting new work during a drain guarantees abandoned
executions, which spec §24 forbids.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

#: The current API version prefix.
API_VERSION: Final[str] = "v1"

#: Path prefixes that bypass versioning, because their callers cannot be
#: updated: orchestrators and load balancers.
UNVERSIONED_PATHS: Final[frozenset[str]] = frozenset({"/healthz", "/readyz"})

Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class Method(StrEnum):
    """HTTP methods this API serves."""

    GET = "GET"
    POST = "POST"
    DELETE = "DELETE"


@dataclass(frozen=True, slots=True)
class Route:
    """One registered route.

    Attributes:
        method: HTTP method.
        path: Full path, including the version prefix.
        handler: The handler.
        served_while_draining: Whether the route answers during shutdown.
        authenticated: Whether the route requires an authenticated caller.
    """

    method: Method
    path: str
    handler: Handler
    served_while_draining: bool = False
    authenticated: bool = True

    @property
    def key(self) -> tuple[str, str]:
        """Identity used for duplicate detection."""
        return (self.method.value, self.path)


class RouteTable:
    """A versioned route table."""

    def __init__(self, version: str = API_VERSION) -> None:
        self._version = version
        self._routes: dict[tuple[str, str], Route] = {}

    @property
    def version(self) -> str:
        """The API version prefix."""
        return self._version

    def add(
        self,
        method: Method,
        path: str,
        handler: Handler,
        *,
        served_while_draining: bool = False,
        authenticated: bool = True,
    ) -> Route:
        """Registers a route.

        The version prefix is applied automatically unless the path is one of
        :data:`UNVERSIONED_PATHS`.

        Raises:
            ValueError: If the route is already registered. Duplicate
                registration is a bug, and silently replacing a route would
                hide which handler actually serves it.
        """
        full = path if path in UNVERSIONED_PATHS else f"/{self._version}{path}"
        route = Route(
            method=method,
            path=full,
            handler=handler,
            served_while_draining=served_while_draining,
            authenticated=authenticated,
        )
        if route.key in self._routes:
            raise ValueError(f"route already registered: {method.value} {full}")
        self._routes[route.key] = route
        return route

    def get(self, method: str, path: str) -> Route | None:
        """Looks up a route."""
        return self._routes.get((method.upper(), path))

    def routes(self) -> tuple[Route, ...]:
        """Every registered route, in insertion order."""
        return tuple(self._routes.values())

    def paths(self) -> tuple[str, ...]:
        """Every registered path, sorted.

        Sorted so the list can be compared between builds without depending on
        registration order.
        """
        return tuple(sorted({route.path for route in self._routes.values()}))

    def __len__(self) -> int:
        return len(self._routes)


def build_default_table(
    *,
    execute: Handler,
    health: Handler,
    ready: Handler,
    write_memory: Handler | None = None,
    retrieve: Handler | None = None,
) -> RouteTable:
    """Builds the standard route table.

    Health and readiness are unversioned and always served. Work routes are
    versioned and stop accepting requests as soon as a drain begins.
    """
    table = RouteTable()
    table.add(Method.GET, "/healthz", health, served_while_draining=True, authenticated=False)
    table.add(Method.GET, "/readyz", ready, served_while_draining=True, authenticated=False)
    table.add(Method.POST, "/executions", execute)
    if write_memory is not None:
        table.add(Method.POST, "/memory", write_memory)
    if retrieve is not None:
        table.add(Method.POST, "/retrieval", retrieve)
    return table
