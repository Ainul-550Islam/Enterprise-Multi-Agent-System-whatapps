"""HTTP tool — the only way an agent may reach the network.

This tool deliberately owns **no** HTTP client of its own. Every request goes
through :class:`~mas_orchestrator.providers.base_http.HttpTransport`, which is
the same SSRF-guarded transport the model providers use. That guarantees:

* the destination host must be on an explicit allowlist,
* every DNS answer is checked against blocked ranges before connecting, so a
  name that rebinds to loopback cannot slip through,
* redirects are re-validated hop by hop rather than followed blindly,
* credentials in headers are never logged.

Adding a second, unrestricted HTTP client here would create a bypass around
all of that, which is why :meth:`HttpTool.invoke` has no code path that
constructs one.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlsplit, urlunsplit

from mas_orchestrator.contracts.errors import InvalidRequestError
from mas_orchestrator.providers.base_http import HttpPolicy, HttpTransport
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolInvocationContext,
    ToolKind,
    ToolSafety,
    ToolVersion,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

DEFAULT_READ_TIMEOUT_SECONDS: Final[float] = 15.0
MAX_RESPONSE_CHARS: Final[int] = 64 * 1024

# Methods an agent is permitted to use. CONNECT and TRACE are excluded because
# they are tunnelling/escaping primitives rather than data operations.
ALLOWED_METHODS: Final[frozenset[str]] = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})

# Response headers dropped before the body reaches a model. These can carry
# session material that has no business being in a prompt.
_SENSITIVE_RESPONSE_HEADERS: Final[frozenset[str]] = frozenset(
    {"set-cookie", "authorization", "proxy-authenticate", "www-authenticate", "cookie"}
)

INPUT_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "method": {"type": "string", "enum": sorted(ALLOWED_METHODS)},
        "url": {"type": "string", "minLength": 1, "maxLength": 2048},
        "headers": {"type": "object"},
        "body": {"type": "string"},
    },
    "required": ["method", "url"],
}


def _safe_url_for_log(url: str) -> str:
    """Returns a destination-safe URL without query/fragment credential risk."""
    parsed = urlsplit(url)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _scrub_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Removes credential-bearing request headers supplied by the model.

    The model chooses the *body* and non-sensitive headers of a request; it
    must never be able to attach an ``Authorization`` header of its own,
    because that would let it present credentials the platform did not grant.
    """
    return {
        key: value
        for key, value in headers.items()
        if key.strip().lower() not in _SENSITIVE_RESPONSE_HEADERS
    }


@dataclass(frozen=True, slots=True)
class HttpToolConfig:
    """What an :class:`HttpTool` is allowed to reach.

    Attributes:
        allowed_hosts: Hostnames this tool may contact. Never empty: an empty
            allowlist means the tool can reach nothing, which is the safe
            default rather than a misconfiguration to work around.
        allowed_methods: HTTP methods permitted. Defaults to
            :data:`ALLOWED_METHODS`.
        max_response_bytes: Response body ceiling, enforced by the transport.
        max_redirects: Redirects followed, each hop re-validated against the
            allowlist. Zero disables redirects.
        require_https: Refuses plaintext URLs. Leave enabled unless there is a
            concrete, reviewed reason.
        connect_timeout_seconds: Connection establishment budget.
        read_timeout_seconds: Per-read budget.
        write_timeout_seconds: Request-body budget.
    """

    allowed_hosts: frozenset[str]
    allowed_methods: frozenset[str] = ALLOWED_METHODS
    max_response_bytes: int = 1024 * 1024
    max_redirects: int = 3
    require_https: bool = True
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS
    write_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not self.allowed_hosts:
            raise InvalidRequestError(
                "http tool requires a non-empty host allowlist",
                details={"field": "allowed_hosts"},
            )
        if not self.allowed_methods:
            raise InvalidRequestError(
                "http tool requires at least one allowed method",
                details={"field": "allowed_methods"},
            )
        unsupported = self.allowed_methods - ALLOWED_METHODS
        if unsupported:
            raise InvalidRequestError(
                "http tool received unsupported methods",
                details={"unsupported": sorted(unsupported)},
            )
        for field_name in (
            "connect_timeout_seconds",
            "read_timeout_seconds",
            "write_timeout_seconds",
        ):
            if getattr(self, field_name) <= 0:
                raise InvalidRequestError(
                    "http tool timeouts must be positive", details={"field": field_name}
                )
        if self.max_redirects < 0:
            raise InvalidRequestError(
                "max_redirects must not be negative", details={"field": "max_redirects"}
            )


class HttpTool:
    """Invokes an allowlisted HTTP endpoint on the model's behalf."""

    def __init__(
        self,
        config: HttpToolConfig,
        *,
        transport: HttpTransport | None = None,
    ) -> None:
        self._config = config
        self._transport = transport or HttpTransport(
            policy=HttpPolicy(
                allowed_hosts=config.allowed_hosts,
                max_response_bytes=config.max_response_bytes,
                max_redirects=config.max_redirects,
                require_https=config.require_https,
            ),
            connect_timeout_seconds=config.connect_timeout_seconds,
            read_timeout_seconds=config.read_timeout_seconds,
            write_timeout_seconds=config.write_timeout_seconds,
        )

    async def aclose(self) -> None:
        """Closes the underlying transport."""
        await self._transport.aclose()

    async def invoke(
        self,
        arguments: Mapping[str, object],
        context: ToolInvocationContext,
    ) -> dict[str, Any]:
        """Performs one request.

        Args:
            arguments: Validated against :data:`INPUT_SCHEMA`.
            context: Invocation context, used only for logging correlation.

        Returns:
            A dict with ``status``, ``headers`` (scrubbed) and ``body``.

        Raises:
            InvalidRequestError: If the method or URL is not permitted.
            ToolFailedError: If the body is not valid JSON when one is sent.
        """
        method = str(arguments["method"]).upper()
        url = str(arguments["url"])

        if method not in self._config.allowed_methods:
            raise InvalidRequestError(
                "http method is not permitted for this tool",
                details={"method": method, "allowed": sorted(self._config.allowed_methods)},
            )

        supplied = arguments.get("headers") or {}
        if not isinstance(supplied, dict):
            raise InvalidRequestError(
                "http headers must be a JSON object", details={"field": "headers"}
            )
        headers = _scrub_headers({str(k): str(v) for k, v in supplied.items()})

        raw_body = arguments.get("body")
        json_body: object | None = None
        if raw_body is not None:
            if isinstance(raw_body, str):
                # The model supplies the body as a string; it must be valid
                # JSON to go out, because sending an unparseable body would
                # only produce an opaque 400 from the far end.
                try:
                    json_body = json.loads(raw_body)
                except json.JSONDecodeError as error:
                    raise InvalidRequestError(
                        "http request body is not valid JSON",
                        details={"field": "body", "position": error.pos},
                    ) from error
            else:
                json_body = raw_body

        _LOGGER.info(
            "http tool request",
            extra={
                "event": "tool.http.request",
                "invocation_id": context.invocation_id,
                "method": method,
                "url": _safe_url_for_log(url),
            },
        )

        response = await self._transport.request(
            method,
            url,
            json_body=json_body,
            headers=headers,
        )

        # The transport already capped the body at max_response_bytes, so this
        # second cap only keeps the agent's context from ballooning; it is not
        # the security boundary.
        body_text = response.body.decode("utf-8", errors="replace")[:MAX_RESPONSE_CHARS]

        return {
            "status": response.status_code,
            "headers": {
                key: value
                for key, value in response.headers.items()
                if key.lower() not in _SENSITIVE_RESPONSE_HEADERS
            },
            "body": body_text,
            "truncated": response.is_truncated or len(response.body) > MAX_RESPONSE_CHARS,
        }


def build_http_tool(
    *,
    tool_id: str,
    name: str,
    tenant_id: str,
    allowed_hosts: frozenset[str] | set[str],
    description: str | None = None,
    version: int = 1,
    safety: ToolSafety = ToolSafety.STANDARD,
    allowed_methods: frozenset[str] | set[str] = ALLOWED_METHODS,
    max_response_bytes: int = 1024 * 1024,
    max_redirects: int = 3,
    require_https: bool = True,
    read_timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
) -> tuple[ToolDefinition, HttpTool]:
    """Builds a definition/handler pair ready for registration.

    Kept here so the schema and the handler cannot drift apart: the definition
    returned by this function is the schema the handler validates against.
    """
    config = HttpToolConfig(
        allowed_hosts=frozenset(allowed_hosts),
        allowed_methods=frozenset(allowed_methods),
        max_response_bytes=max_response_bytes,
        max_redirects=max_redirects,
        require_https=require_https,
        read_timeout_seconds=read_timeout_seconds,
    )

    definition = ToolDefinition(
        tool_id=tool_id,
        name=name,
        version=ToolVersion(version),
        kind=ToolKind.HTTP,
        safety=safety,
        input_schema=INPUT_SCHEMA,
        description=description or f"HTTP requests limited to {sorted(config.allowed_hosts)}",
        tenant_id=tenant_id,
        default_timeout_seconds=read_timeout_seconds,
        idempotent=False,  # a write is not safely repeatable
    )

    return definition, HttpTool(config)
