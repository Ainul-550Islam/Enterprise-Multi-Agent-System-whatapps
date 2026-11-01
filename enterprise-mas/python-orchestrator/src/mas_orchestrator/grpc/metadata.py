# PURPOSE:
# Correlation, tenancy and service metadata propagation rules — the single
# mapping between gRPC metadata keys (the wire) and typed values (the domain).
#
# Owning layer: `mas_orchestrator.grpc` (the process edge).
#
# Production responsibility: make it impossible, by construction, for a header
# to establish tenancy. The authoritative tenant and organization come from the
# request body, which Rust populates from its own `RuntimeContext`; the
# `x-tenant-id` / `x-organization-id` headers exist only to be cross-checked
# against it, so a spliced or misrouted call is caught instead of executing
# under two scopes at once.
#
# Security boundary: the header names here are a shared contract with
# `crates/ai-bridge/src/lib.rs` and with `mas_security::internal_service`. A
# rename on one side turns every call into an unauthenticated one, which is
# why they are declared together in this module rather than inline at their
# call sites.
#
# It must NOT:
#   • derive tenancy from a header, ever, on any path;
#   • set `x-tenant-id` on an outbound call to influence Rust's decision;
#   • render a credential in `repr` — `RustCallMetadata` reports presence, not
#     value.

"""Correlation, tenancy and service metadata propagation rules.

This module defines **the** mapping between gRPC metadata keys (the wire) and
the typed values the orchestrator uses (the domain). Getting it wrong is a
security bug, so the rules are stated explicitly:

**Inbound (Rust -> Python)**

* The authoritative tenant and organization come from the **request body**
  (``AgentRunRequest.tenant_id`` / ``organization_id``), which Rust populates
  from its own ``RuntimeContext``. They are *not* taken from headers.
* The ``x-tenant-id`` / ``x-organization-id`` headers are therefore treated as
  a **cross-check**, not as a source of truth. If a header is present and
  disagrees with the body, the call is refused with
  :class:`~mas_orchestrator.contracts.errors.TenantContextError`. This detects
  header/body splicing and misrouted traffic without ever letting a header
  establish tenancy.
* ``x-correlation-id`` / ``x-request-id`` / ``traceparent`` are propagation
  metadata only; they never influence authorization.

**Outbound (Python -> Rust)**

* Python attaches its service identity, the correlation id it is currently
  serving (so a Rust log line can be joined to the AI work that caused it), and
  the negotiated protocol version.
* Python **never** sets ``x-tenant-id`` on an outbound call to influence
  Rust's decision: tenancy on the way back is conveyed in the typed body.

Note on the HTTP API: the rule "never derive tenancy from headers" applies with
full force there, because that edge is reachable by anything that can reach the
service. See :mod:`mas_orchestrator.api.dependencies`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import TenantContextError

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = [
    "AUTHORIZATION_HEADER",
    "CORRELATION_HEADER",
    "GRPC_TIMEOUT_HEADER",
    "INTERNAL_SERVICE_HEADER",
    "INTERNAL_SIGNATURE_HEADER",
    "ORGANIZATION_HEADER",
    "PROTOCOL_VERSION_HEADER",
    "REQUEST_ID_HEADER",
    "TENANT_HEADER",
    "TRACEPARENT_HEADER",
    "RustCallMetadata",
    "build_outbound_metadata",
    "extract_call_metadata",
    "parse_grpc_timeout",
    "reconcile_tenant_scope",
]

# ---------------------------------------------------------------------------
# Header names — must match `crates/ai-bridge/src/lib.rs` exactly.
# ---------------------------------------------------------------------------

#: Bearer credential presented by the Rust Core service.
AUTHORIZATION_HEADER: Final[str] = "authorization"
#: Tenant scope header (cross-check only, never authoritative for Python).
TENANT_HEADER: Final[str] = "x-tenant-id"
#: Organization scope header (cross-check only).
ORGANIZATION_HEADER: Final[str] = "x-organization-id"
#: End-to-end correlation identifier.
CORRELATION_HEADER: Final[str] = "x-correlation-id"
#: Platform request identifier.
REQUEST_ID_HEADER: Final[str] = "x-request-id"
#: Wire protocol version claimed by the caller.
PROTOCOL_VERSION_HEADER: Final[str] = "x-ai-protocol-version"
#: W3C trace context.
TRACEPARENT_HEADER: Final[str] = "traceparent"
#: gRPC's own deadline header, parsed separately.
GRPC_TIMEOUT_HEADER: Final[str] = "grpc-timeout"
#: Body-bound internal signature (``v1:<service>:<millis>:<hmac-hex>``).
#:
#: The names must match ``mas_security::internal_service`` on the Rust side
#: byte for byte; a rename on one side turns every call into an
#: unauthenticated one, which is why they are declared together here rather
#: than inline at the two call sites.
INTERNAL_SIGNATURE_HEADER: Final[str] = "x-mas-internal-signature"
#: The calling service's claimed id, cross-checked against the signature.
INTERNAL_SERVICE_HEADER: Final[str] = "x-mas-internal-service"

#: gRPC timeout unit suffixes -> nanoseconds. The wire format is
#: ``<1-8 digits><unit>`` where unit is one of n/u/m/S/M/H.
_TIMEOUT_UNITS: Final[Mapping[str, int]] = {
    "n": 1,
    "u": 1_000,
    "m": 1_000_000,
    "S": 1_000_000_000,
    "M": 60_000_000_000,
    "H": 3_600_000_000_000,
}


@dataclass(frozen=True, slots=True)
class RustCallMetadata:
    """Typed view of the metadata accompanying an inbound Rust call.

    Attributes:
        credential: Raw bearer credential, or ``None`` when absent. Never
            logged and never included in ``repr``.
        correlation_id: End-to-end correlation identifier, if supplied.
        request_id: Platform request identifier, if supplied.
        protocol_version: Declared wire protocol version, if supplied.
        tenant_id: Tenant header value, if supplied. **Cross-check only** —
            never used to establish tenancy.
        organization_id: Organization header value, if supplied.
        traceparent: W3C trace context, if supplied.
        timeout_seconds: Deadline decoded from ``grpc-timeout``, if supplied.
        internal_signature: Body-bound signature, if supplied. Verified against
            the exact request bytes before any other identity check runs.
        internal_service: The calling service id claimed in the signature.
    """

    credential: str | None = None
    correlation_id: str | None = None
    request_id: str | None = None
    protocol_version: str | None = None
    tenant_id: str | None = None
    organization_id: str | None = None
    traceparent: str | None = None
    timeout_seconds: float | None = None
    internal_signature: str | None = None
    internal_service: str | None = None

    def __repr__(self) -> str:
        """Never renders the credential, even in a debug dump."""
        return (
            f"RustCallMetadata(correlation_id={self.correlation_id!r}, "
            f"request_id={self.request_id!r}, "
            f"protocol_version={self.protocol_version!r}, "
            f"tenant_id={self.tenant_id!r}, credential=<present={self.credential is not None}>, "
            f"internal_service={self.internal_service!r}, "
            f"signature=<present={self.internal_signature is not None}>)"
        )


def _first_value(pairs: Sequence[tuple[str, str]], key: str) -> str | None:
    """Returns the first non-empty value for *key*, or ``None``.

    gRPC metadata may repeat a key; taking the first non-empty value is the
    same rule the Rust client follows when it sets a header once, and avoids
    silently honouring a later, duplicated value.
    """
    for name, value in pairs:
        if name.lower() == key:
            trimmed = value.strip()
            if trimmed:
                return trimmed
    return None


def extract_call_metadata(pairs: Sequence[tuple[str, str]]) -> RustCallMetadata:
    """Builds a :class:`RustCallMetadata` from raw gRPC metadata pairs.

    Args:
        pairs: The invocation metadata as ``(key, value)`` tuples, exactly as
            provided by ``grpc.aio.ServicerContext.invocation_metadata()``.

    Returns:
        The typed metadata. Every field is optional: absence is handled by the
        validation layers that require them, not by defaulting here.
    """
    return RustCallMetadata(
        credential=_first_value(pairs, AUTHORIZATION_HEADER),
        correlation_id=_first_value(pairs, CORRELATION_HEADER),
        request_id=_first_value(pairs, REQUEST_ID_HEADER),
        protocol_version=_first_value(pairs, PROTOCOL_VERSION_HEADER),
        tenant_id=_first_value(pairs, TENANT_HEADER),
        organization_id=_first_value(pairs, ORGANIZATION_HEADER),
        traceparent=_first_value(pairs, TRACEPARENT_HEADER),
        timeout_seconds=parse_grpc_timeout(_first_value(pairs, GRPC_TIMEOUT_HEADER)),
        internal_signature=_first_value(pairs, INTERNAL_SIGNATURE_HEADER),
        internal_service=_first_value(pairs, INTERNAL_SERVICE_HEADER),
    )


def parse_grpc_timeout(raw: str | None) -> float | None:
    """Decodes a ``grpc-timeout`` value into seconds.

    The wire format is ``<1-8 digits><unit>`` where the unit is one of
    ``n`` (nanoseconds), ``u`` (microseconds), ``m`` (milliseconds),
    ``S`` (seconds), ``M`` (minutes) or ``H`` (hours).

    Args:
        raw: The header value, or ``None``.

    Returns:
        The timeout in seconds, or ``None`` when absent.

    Raises:
        TenantContextError: Never. A malformed timeout yields ``None`` rather
            than failing the call — gRPC itself enforces its own deadline, and
            refusing the call over a header we only use for planning would
            create a denial-of-service vector. The value is ignored, not
            guessed at.
    """
    if not raw:
        return None

    value = raw.strip()
    if len(value) < 2 or len(value) > 9:
        return None

    unit = value[-1]
    digits = value[:-1]
    if not digits.isdigit() or unit not in _TIMEOUT_UNITS:
        return None

    return int(digits) * _TIMEOUT_UNITS[unit] / 1_000_000_000.0


def reconcile_tenant_scope(
    *,
    body_tenant_id: str,
    body_organization_id: str,
    metadata: RustCallMetadata,
) -> None:
    """Cross-checks body tenancy against the metadata headers.

    The body is authoritative because Rust fills it from its own
    ``RuntimeContext``. The headers are checked for **agreement**, so a
    misrouted or spliced call is caught instead of silently executing under two
    different scopes.

    Args:
        body_tenant_id: Authoritative tenant from the request body.
        body_organization_id: Authoritative organization from the request body.
        metadata: The parsed inbound metadata.

    Raises:
        TenantContextError: If a tenant/organization header is present and
            disagrees with the body. Absent headers are tolerated: some Rust
            call paths legitimately omit them, and the body still carries the
            authoritative value.
    """
    if metadata.tenant_id is not None and metadata.tenant_id != body_tenant_id:
        raise TenantContextError(
            "tenant header disagrees with the authoritative request body",
            details={
                "reason": "tenant_mismatch",
                "source": "header_vs_body",
                "header_field": TENANT_HEADER,
            },
        )

    if metadata.organization_id is not None and metadata.organization_id != body_organization_id:
        raise TenantContextError(
            "organization header disagrees with the authoritative request body",
            details={
                "reason": "organization_mismatch",
                "source": "header_vs_body",
                "header_field": ORGANIZATION_HEADER,
            },
        )


def build_outbound_metadata(
    *,
    correlation_id: str | None = None,
    request_id: str | None = None,
    protocol_version: str | None = None,
    credential: str | None = None,
    traceparent: str | None = None,
) -> list[tuple[str, str]]:
    """Builds outbound metadata for Python -> Rust calls.

    Deliberately **absent**: ``x-tenant-id`` and ``x-organization-id``. Python
    must never attempt to steer Rust's tenancy decision through a header;
    on the way back, scope is conveyed in the typed request body where Rust
    validates it against its own context.

    Args:
        correlation_id: Correlation id of the work currently being served.
        request_id: Platform request id currently being served.
        protocol_version: The negotiated protocol version to declare.
        credential: Bearer credential for the Rust Core, if configured.
        traceparent: W3C trace context to continue.

    Returns:
        Metadata pairs in a stable order. Empty values are omitted rather than
        sent as empty strings, which some intermediaries reject.
    """
    pairs: list[tuple[str, str]] = []
    if credential:
        pairs.append((AUTHORIZATION_HEADER, credential))
    if correlation_id:
        pairs.append((CORRELATION_HEADER, correlation_id))
    if request_id:
        pairs.append((REQUEST_ID_HEADER, request_id))
    if protocol_version:
        pairs.append((PROTOCOL_VERSION_HEADER, protocol_version))
    if traceparent:
        pairs.append((TRACEPARENT_HEADER, traceparent))
    return pairs
