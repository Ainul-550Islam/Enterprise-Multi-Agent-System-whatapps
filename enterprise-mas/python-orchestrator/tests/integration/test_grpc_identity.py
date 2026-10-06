"""Integration tests for the Rust-to-Python identity path.

These wire the real metadata extractor to the real context builder and the real
tenant reconciliation. The property under test is the one spec §6 is about:
tenancy arrives from Rust and is cross-checked, never taken on trust from a
header and never invented by Python.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.context.execution_context import (
    ExecutionContext,
    build_execution_context,
)
from mas_orchestrator.contracts.errors import (
    AuthenticationError,
    ProtocolError,
    TenantContextError,
)
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.grpc.metadata import (
    AUTHORIZATION_HEADER,
    CORRELATION_HEADER,
    GRPC_TIMEOUT_HEADER,
    ORGANIZATION_HEADER,
    PROTOCOL_VERSION_HEADER,
    REQUEST_ID_HEADER,
    TENANT_HEADER,
    build_outbound_metadata,
    extract_call_metadata,
    parse_grpc_timeout,
    reconcile_tenant_scope,
)
from mas_orchestrator.security.identity import ServiceIdentity, ServiceScope

TENANT = "tenant-alpha"
OTHER = "tenant-beta"
CREDENTIAL = "service-token-value"


def _pairs(**overrides: str) -> list[tuple[str, str]]:
    base = {
        AUTHORIZATION_HEADER: f"Bearer {CREDENTIAL}",
        TENANT_HEADER: TENANT,
        ORGANIZATION_HEADER: "org-1",
        CORRELATION_HEADER: "corr-1",
        REQUEST_ID_HEADER: "req-1",
        PROTOCOL_VERSION_HEADER: str(CURRENT_PROTOCOL_VERSION),
        GRPC_TIMEOUT_HEADER: "30S",
    }
    base.update(overrides)
    return [(key, value) for key, value in base.items() if value]


class _Resolver:
    """Resolves the one credential this service accepts.

    Strips the ``Bearer`` scheme the way a real resolver must: the header
    carries the scheme, the comparison is on the token.
    """

    def __init__(self, accepted: str = CREDENTIAL) -> None:
        self._accepted = accepted
        self.seen: list[str] = []

    def __call__(self, credential: str | None) -> ServiceIdentity:
        self.seen.append(credential or "")
        presented = credential or ""
        if presented.lower().startswith("bearer "):
            presented = presented[7:]
        if presented != self._accepted:
            raise AuthenticationError(
                "the presented credential was not accepted",
                details={"field": "credential"},
            )
        return ServiceIdentity(
            subject="rust-core",
            issuer="static",
            scopes=frozenset({ServiceScope.EXECUTE_AGENT}),
        )


class TestMetadataExtraction:
    def test_round_trips_every_field(self) -> None:
        metadata = extract_call_metadata(_pairs())
        assert metadata.tenant_id == TENANT
        assert metadata.correlation_id == "corr-1"
        assert metadata.timeout_seconds == 30.0

    def test_credential_never_appears_in_repr(self) -> None:
        # A debug dump is not a place to put a service token.
        assert CREDENTIAL not in repr(extract_call_metadata(_pairs()))

    def test_absent_fields_are_none_not_defaults(self) -> None:
        # Absence is handled by the validation layers that require them, not by
        # inventing a value here.
        metadata = extract_call_metadata([])
        assert metadata.tenant_id is None
        assert metadata.timeout_seconds is None

    def test_first_value_wins(self) -> None:
        # A duplicated header could otherwise carry a second, different tenant.
        pairs = [(TENANT_HEADER, TENANT), (TENANT_HEADER, OTHER)]
        assert extract_call_metadata(pairs).tenant_id == TENANT


class TestTimeoutParsing:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("30S", 30.0),
            ("1500m", 1.5),
            ("2M", 120.0),
            ("500000u", 0.5),
        ],
    )
    def test_units_are_decoded(self, raw: str, expected: float) -> None:
        assert parse_grpc_timeout(raw) == pytest.approx(expected)

    @pytest.mark.parametrize("raw", ["", "junk", "S", "30X", "123456789012S"])
    def test_malformed_values_yield_none(self, raw: str) -> None:
        # Ignored rather than refused: gRPC enforces its own deadline, and
        # refusing over a header used only for planning would be a DoS vector.
        assert parse_grpc_timeout(raw) is None


class TestTenantReconciliation:
    def test_agreeing_header_and_body_is_accepted(self) -> None:
        metadata = extract_call_metadata(_pairs())
        assert (
            reconcile_tenant_scope(
                body_tenant_id=TENANT,
                body_organization_id="org-1",
                metadata=metadata,
            )
            is None
        )

    def test_disagreeing_header_is_refused(self) -> None:
        # A spliced or misrouted call must be caught rather than executing under
        # two different scopes.
        metadata = extract_call_metadata(_pairs(**{TENANT_HEADER: OTHER}))
        with pytest.raises(TenantContextError, match="tenant header disagrees"):
            reconcile_tenant_scope(
                body_tenant_id=TENANT,
                body_organization_id="org-1",
                metadata=metadata,
            )

    def test_disagreeing_organization_is_refused(self) -> None:
        metadata = extract_call_metadata(_pairs(**{ORGANIZATION_HEADER: "org-9"}))
        with pytest.raises(TenantContextError, match="organization header"):
            reconcile_tenant_scope(
                body_tenant_id=TENANT,
                body_organization_id="org-1",
                metadata=metadata,
            )

    def test_absent_headers_are_tolerated(self) -> None:
        # Some Rust call paths legitimately omit them; the body is
        # authoritative and still carries the value.
        metadata = extract_call_metadata([])
        assert (
            reconcile_tenant_scope(
                body_tenant_id=TENANT,
                body_organization_id="org-1",
                metadata=metadata,
            )
            is None
        )


class TestContextAssembly:
    def test_metadata_to_context(self) -> None:
        metadata = extract_call_metadata(_pairs())
        context = build_execution_context(
            tenant_id=TENANT,
            organization_id="org-1",
            execution_id="exec-1",
            agent_id="worker",
            agent_version_id="v1",
            protocol_version=metadata.protocol_version or str(CURRENT_PROTOCOL_VERSION),
            caller=_Resolver()(metadata.credential),
            correlation_id=metadata.correlation_id or "",
            request_id=metadata.request_id or "",
        )
        assert isinstance(context, ExecutionContext)
        assert context.tenant_id == TENANT
        assert context.correlation_id == "corr-1"

    def test_unsupported_protocol_version_is_refused(self) -> None:
        metadata = extract_call_metadata(_pairs(**{PROTOCOL_VERSION_HEADER: "99.0.0"}))
        with pytest.raises(ProtocolError):
            build_execution_context(
                tenant_id=TENANT,
                organization_id="org-1",
                execution_id="exec-1",
                agent_id="worker",
                agent_version_id="v1",
                protocol_version=metadata.protocol_version or "",
                caller=_Resolver()(metadata.credential),
                correlation_id=metadata.correlation_id or "",
                request_id=metadata.request_id or "",
            )

    def test_bad_credential_never_produces_a_context(self) -> None:
        # An unauthenticated call raises rather than producing an identity
        # object; there is no anonymous sentinel to be misused downstream.
        metadata = extract_call_metadata(_pairs())
        with pytest.raises(AuthenticationError):
            _Resolver(accepted="something-else")(metadata.credential)

    def test_deadline_is_carried_from_the_timeout(self) -> None:
        metadata = extract_call_metadata(_pairs(**{GRPC_TIMEOUT_HEADER: "5S"}))
        assert metadata.timeout_seconds == 5.0


class TestOutboundMetadata:
    def test_outbound_carries_correlation(self) -> None:
        pairs = build_outbound_metadata(correlation_id="corr-1", request_id="req-1")
        assert (CORRELATION_HEADER, "corr-1") in pairs

    def test_credential_is_not_forwarded_by_default(self) -> None:
        # Forwarding Rust's inbound credential to a downstream service would
        # widen its blast radius to every hop.
        assert AUTHORIZATION_HEADER not in dict(build_outbound_metadata(correlation_id="c"))

    def test_tenant_is_deliberately_absent(self) -> None:
        # Python must never steer Rust's tenancy decision through a header. On
        # the way back, scope travels in the typed request body where Rust
        # validates it against its own context.
        headers = dict(
            build_outbound_metadata(
                correlation_id="c",
                request_id="r",
                protocol_version=str(CURRENT_PROTOCOL_VERSION),
                credential="token",
            )
        )
        assert TENANT_HEADER not in headers
        assert ORGANIZATION_HEADER not in headers

    def test_empty_values_are_omitted(self) -> None:
        # Some intermediaries reject an empty header value, so it is not sent.
        assert build_outbound_metadata(correlation_id="") == []
