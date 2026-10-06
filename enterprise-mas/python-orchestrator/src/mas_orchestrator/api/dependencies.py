"""Request-scoped dependencies.

This module is where the spec §6 tenancy rule is actually enforced:

    Do not derive tenant ownership from arbitrary HTTP headers at the Python
    edge.

The enforcement is :data:`TRUSTED_HEADER_PREFIX`. Only headers under that
prefix are read for identity, and only when the request has already been
authenticated. A plain ``x-tenant-id`` is ignored — not rejected, *ignored*,
because a caller that sends one is not necessarily attacking; it may be
copying an example. Silently ignoring it is safer than trusting it and safer
than failing a legitimate request that merely carries a stray header.

The second responsibility here is correlation. Every request gets a
correlation id: the one supplied by Rust if present, otherwise a generated one.
A request without a correlation id is traceable rather than anonymous.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.context.execution_context import ExecutionContext

if TYPE_CHECKING:
    from collections.abc import Mapping
from mas_orchestrator.contracts.errors import (
    AuthenticationError,
    InvalidRequestError,
    TenantContextError,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Headers trusted to carry identity. Everything else is ignored.
TRUSTED_HEADER_PREFIX: Final[str] = "x-mas-"

#: Header carrying the tenant, within the trusted namespace.
HEADER_TENANT_ID: Final[str] = "x-mas-tenant-id"
HEADER_ORGANIZATION_ID: Final[str] = "x-mas-organization-id"
HEADER_EXECUTION_ID: Final[str] = "x-mas-execution-id"
HEADER_AGENT_ID: Final[str] = "x-mas-agent-id"
HEADER_CORRELATION_ID: Final[str] = "x-mas-correlation-id"
HEADER_REQUEST_ID: Final[str] = "x-mas-request-id"
HEADER_AGENT_VERSION_ID: Final[str] = "x-mas-agent-version-id"
HEADER_TASK_ID: Final[str] = "x-mas-task-id"
HEADER_TASK_ATTEMPT_ID: Final[str] = "x-mas-task-attempt-id"

#: The unprefixed names a caller might send instead. Recorded so the refusal
#: is explainable rather than mysterious.
UNTRUSTED_TENANT_HEADERS: Final[frozenset[str]] = frozenset(
    {"x-tenant-id", "x-tenant", "x-organization-id", "x-org-id"}
)


@dataclass(frozen=True, slots=True)
class RequestScope:
    """What one request is working on.

    Attributes:
        correlation_id: Correlation identifier, supplied or generated.
        route: Which route is serving it.
        started_epoch_ms: When it started.
        authenticated: Whether the caller proved who it is.
    """

    correlation_id: str
    route: str = ""
    started_epoch_ms: int = 0
    authenticated: bool = False

    @property
    def elapsed_ms(self) -> int:
        """How long the request has been running."""
        return max(0, int(time.time() * 1000) - self.started_epoch_ms)


def new_correlation_id() -> str:
    """Generates a correlation identifier."""
    return f"corr-{uuid.uuid4().hex}"


def is_trusted_header(name: str) -> bool:
    """Whether a header is in the trusted namespace."""
    return name.lower().startswith(TRUSTED_HEADER_PREFIX)


def filter_trusted(headers: Mapping[str, str]) -> dict[str, str]:
    """Keeps only trusted headers, lowercased.

    Args:
        headers: Raw request headers.

    Returns:
        The trusted subset. An untrusted header is dropped rather than
        rejected, so a stray header on an otherwise valid request does not
        break it.
    """
    trusted = {name.lower(): value for name, value in headers.items() if is_trusted_header(name)}
    supplied = {name.lower() for name in headers}
    stray = supplied & UNTRUSTED_TENANT_HEADERS
    if stray:
        # Logged rather than refused: this is worth knowing about, because a
        # client that sends a tenant header is either mistaken or probing.
        _LOGGER.warning(
            "request carried a tenant header outside the trusted namespace",
            extra={"event": "api.untrusted_tenant_header", "headers": sorted(stray)},
        )
    return trusted


def build_scope(
    headers: Mapping[str, str],
    *,
    route: str = "",
    authenticated: bool = False,
) -> RequestScope:
    """Builds a request scope from headers.

    Args:
        headers: Raw request headers.
        route: Route being served.
        authenticated: Whether the caller was authenticated by the transport.

    Returns:
        A :class:`RequestScope`.
    """
    trusted = filter_trusted(headers)
    correlation = trusted.get(HEADER_CORRELATION_ID) or new_correlation_id()
    return RequestScope(
        correlation_id=correlation,
        route=route,
        started_epoch_ms=int(time.time() * 1000),
        authenticated=authenticated,
    )


def context_from_headers(
    headers: Mapping[str, str],
    *,
    authenticated: bool = True,
    protocol_version: Any = None,
    caller: Any = None,
) -> ExecutionContext:
    """Builds an execution context from trusted headers.

    The tenant can only come from :data:`HEADER_TENANT_ID`. There is no
    parameter to override it and no fallback to a default tenant, because a
    default would mean an unauthenticated call runs as somebody.

    Args:
        headers: Raw request headers.
        authenticated: Whether the transport authenticated the caller.
        protocol_version: Negotiated protocol version.
        caller: Authenticated caller identity.

    Raises:
        AuthenticationError: If the request is not authenticated, or the caller
            identity is missing.
        TenantContextError: If a mandatory identity header is absent.
    """
    if not authenticated:
        raise AuthenticationError(
            "the request was not authenticated by the transport",
            details={"hint": "identity headers are only honoured on authenticated requests"},
        )
    if caller is None:
        raise AuthenticationError(
            "the request carries no authenticated caller identity",
            details={"field": "caller"},
        )

    trusted = filter_trusted(headers)
    required = {
        "tenant_id": HEADER_TENANT_ID,
        "organization_id": HEADER_ORGANIZATION_ID,
        "execution_id": HEADER_EXECUTION_ID,
        "agent_id": HEADER_AGENT_ID,
    }
    missing = [name for name, header in required.items() if not trusted.get(header, "").strip()]
    if missing:
        raise TenantContextError(
            "request is missing mandatory identity headers",
            details={"missing": sorted(missing), "namespace": TRUSTED_HEADER_PREFIX},
        )

    if protocol_version is None:
        raise InvalidRequestError(
            "a protocol version is required", details={"field": "protocol_version"}
        )

    return ExecutionContext(
        tenant_id=trusted[HEADER_TENANT_ID],
        organization_id=trusted[HEADER_ORGANIZATION_ID],
        execution_id=trusted[HEADER_EXECUTION_ID],
        agent_id=trusted[HEADER_AGENT_ID],
        correlation_id=trusted.get(HEADER_CORRELATION_ID) or new_correlation_id(),
        request_id=trusted.get(HEADER_REQUEST_ID) or new_correlation_id(),
        protocol_version=protocol_version,
        caller=caller,
        task_id=trusted.get(HEADER_TASK_ID),
        task_attempt_id=trusted.get(HEADER_TASK_ATTEMPT_ID),
        agent_version_id=trusted.get(HEADER_AGENT_VERSION_ID),
    )


def assert_same_tenant(context: ExecutionContext, tenant_id: str) -> None:
    """Refuses a request whose context belongs to another tenant.

    Raises:
        TenantContextError: If the tenants differ.
    """
    if context.tenant_id != tenant_id:
        raise TenantContextError(
            "execution context belongs to a different tenant",
            details={"context_tenant": context.tenant_id, "requested": tenant_id},
        )
