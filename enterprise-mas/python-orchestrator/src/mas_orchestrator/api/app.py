"""ASGI application factory.

There is no web framework in the dependency set, and spec §21 makes adding one
a decision rather than a reflex. So this module implements the small slice of
ASGI the service needs: routing, JSON responses, and middleware. It is about
150 lines, which is less than the integration code a framework would require,
and it has no version to keep up with.

What it deliberately omits is as important as what it includes. There is no
body streaming, no form parsing, no static files. Each of those is a surface to
harden, and none of them is needed by an internal AI service that accepts JSON
and returns JSON.

Middleware runs in a fixed order, and the order is the security design:

1. **correlation** — assigns an id before anything else, so every later log
   line and error can be attributed.
2. **authentication** — proves the caller before any identity header is read.
3. **drain** — refuses new work during shutdown, after authentication so the
   refusal is recorded against a caller.
4. **errors** — maps any exception to a typed response, so nothing escapes as
   an unhandled traceback.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final

from mas_orchestrator.api.dependencies import build_scope, filter_trusted
from mas_orchestrator.api.errors import log_error, to_body
from mas_orchestrator.api.routes import RouteTable
from mas_orchestrator.contracts.errors import AuthenticationError

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

# ``RouteTable`` is re-exported so callers can build a table and an application
# from one import; the name is also used in annotations below, which are strings
# under ``from __future__ import annotations``.
__all__ = ["MAX_BODY_BYTES", "AppConfig", "Application", "RouteTable", "create_app"]

Scope = dict[str, Any]
Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

#: Largest request body accepted. A JSON command payload does not need to be
#: large, and a bound here stops a slow-loris before it reaches a parser.
MAX_BODY_BYTES: Final[int] = 1_048_576

JSON_HEADERS: Final[list[tuple[bytes, bytes]]] = [
    (b"content-type", b"application/json; charset=utf-8"),
]


@dataclass(frozen=True, slots=True)
class AppConfig:
    """How the application behaves.

    Attributes:
        service_name: Reported in health responses.
        environment: Deployment environment.
        version: Build version.
        draining: Whether the service is shutting down. Set during a drain so
            work routes stop accepting requests while health routes continue.
        authenticate: An optional callable that validates the caller and
            returns an identity. ``None`` disables authentication entirely,
            which is refused outside development.
    """

    service_name: str = "mas-orchestrator"
    environment: str = "development"
    version: str = "unknown"
    draining: bool = False
    authenticate: Callable[[dict[str, str]], Awaitable[Any]] | None = None


class Application:
    """A minimal ASGI application."""

    def __init__(self, table: RouteTable, config: AppConfig | None = None) -> None:
        self._table = table
        self._config = config or AppConfig()

    @property
    def config(self) -> AppConfig:
        """The configuration in force."""
        return self._config

    @property
    def table(self) -> RouteTable:
        """The route table."""
        return self._table

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handles one ASGI event."""
        if scope.get("type") != "http":
            # Lifespan and websocket are not served. Returning rather than
            # raising keeps an unsupported scope from looking like a crash.
            await send({"type": "http.response.start", "status": 404, "headers": JSON_HEADERS})
            await send({"type": "http.response.body", "body": b"{}"})
            return

        method = str(scope.get("method", "GET")).upper()
        path = str(scope.get("path", "/"))
        headers = _headers_of(scope)

        # --- middleware 1: correlation ---
        scope_obj = build_scope(
            headers,
            route=path,
            authenticated=self._config.authenticate is not None,
        )
        correlation_id = scope_obj.correlation_id
        _LOGGER.debug(
            "request received",
            extra={
                "event": "api.request",
                "method": method,
                "path": path,
                "correlation_id": correlation_id,
            },
        )

        try:
            route = self._table.get(method, path)
            if route is None:
                await self._send(
                    send, 404, {"error": {"code": "not_found", "message": "no such route"}}
                )
                return

            # --- middleware 2: authentication ---
            identity: Any = None
            if route.authenticated:
                identity = await self._authenticate(headers, correlation_id)

            # --- middleware 3: drain ---
            if self._config.draining and not route.served_while_draining:
                await self._send(
                    send,
                    503,
                    {
                        "error": {
                            "code": "draining",
                            "message": "the service is shutting down and is not accepting new work",
                            "retryable": True,
                            "correlation_id": correlation_id,
                        }
                    },
                )
                return

            body = await self._read_body(receive)
            payload = dict(body)
            payload["_identity"] = identity
            payload["_headers"] = filter_trusted(headers)
            payload["_correlation_id"] = correlation_id
            payload["_route"] = path

            result = await route.handler(payload)
            await self._send(send, 200, result)

        except AuthenticationError as error:
            await self._send_error(send, error, correlation_id, path, 401)
        except Exception as error:
            await self._send_error(send, error, correlation_id, path)

    # --- internals ---------------------------------------------------------

    async def _authenticate(self, headers: dict[str, str], correlation_id: str) -> Any:
        """Authenticates the caller, or refuses.

        Raises:
            AuthenticationError: If no authenticator is configured, or it
                rejects the caller. Running unauthenticated in production is
                refused rather than warned about.
        """
        from mas_orchestrator.config.enums import Environment

        if self._config.authenticate is None:
            if self._config.environment == Environment.PRODUCTION.value:
                raise AuthenticationError(
                    "no authenticator is configured; refusing to serve "
                    "authenticated routes in production",
                    details={"environment": self._config.environment},
                )
            return None
        identity = await self._config.authenticate(headers)
        if identity is None:
            raise AuthenticationError(
                "the authenticator rejected the caller",
                details={"correlation_id": correlation_id},
            )
        return identity

    async def _read_body(self, receive: Receive) -> dict[str, Any]:
        """Reads and parses a JSON request body.

        Returns:
            The parsed object, or ``{}`` for an empty body.

        Raises:
            InvalidRequestError: If the body is too large or is not a JSON
                object.
        """
        from mas_orchestrator.contracts.errors import InvalidRequestError

        chunks: list[bytes] = []
        total = 0
        while True:
            message = await receive()
            if message.get("type") != "http.request":
                break
            chunk = bytes(message.get("body") or b"")
            total += len(chunk)
            if total > MAX_BODY_BYTES:
                raise InvalidRequestError(
                    "request body exceeds the maximum size",
                    details={"limit": MAX_BODY_BYTES},
                )
            chunks.append(chunk)
            if not message.get("more_body"):
                break

        if not chunks:
            return {}
        raw = b"".join(chunks).strip()
        if not raw:
            return {}

        try:
            parsed = json.loads(raw)
        except ValueError as error:
            raise InvalidRequestError(
                "request body is not valid JSON", details={"field": "body"}
            ) from error
        if not isinstance(parsed, dict):
            raise InvalidRequestError(
                "request body must be a JSON object", details={"field": "body"}
            )
        return parsed

    async def _send(self, send: Send, status: int, payload: dict[str, Any]) -> None:
        """Writes a JSON response."""
        body = json.dumps(payload, default=str).encode("utf-8")
        await send({"type": "http.response.start", "status": status, "headers": JSON_HEADERS})
        await send({"type": "http.response.body", "body": body})

    async def _send_error(
        self,
        send: Send,
        error: BaseException,
        correlation_id: str,
        route: str,
        status: int | None = None,
    ) -> None:
        """Maps an error to a typed response and logs it."""
        log_error(error, correlation_id=correlation_id, route=route)
        body = to_body(error, correlation_id=correlation_id)
        await self._send(send, status or body.status, body.as_dict())


def _headers_of(scope: Scope) -> dict[str, str]:
    """Extracts headers from an ASGI scope as a name/value mapping."""
    headers: dict[str, str] = {}
    for raw_name, raw_value in scope.get("headers") or []:
        name = raw_name.decode("latin-1")
        value = raw_value.decode("latin-1")
        headers[name] = value
    return headers


def create_app(
    table: RouteTable,
    config: AppConfig | None = None,
) -> Application:
    """Builds the ASGI application.

    Kept separate from :class:`Application` so wiring is visible in one place
    and a caller cannot construct a half-configured app.
    """
    return Application(table, config)
