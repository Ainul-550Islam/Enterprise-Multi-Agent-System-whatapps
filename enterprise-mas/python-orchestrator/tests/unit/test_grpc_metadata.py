"""Unit tests for gRPC metadata propagation and tenancy cross-checks.

The tenancy rules here are security controls: a header must never be able to
establish or change the tenant of an execution.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import TenantContextError
from mas_orchestrator.grpc.metadata import (
    AUTHORIZATION_HEADER,
    CORRELATION_HEADER,
    GRPC_TIMEOUT_HEADER,
    ORGANIZATION_HEADER,
    PROTOCOL_VERSION_HEADER,
    REQUEST_ID_HEADER,
    TENANT_HEADER,
    TRACEPARENT_HEADER,
    RustCallMetadata,
    build_outbound_metadata,
    extract_call_metadata,
    parse_grpc_timeout,
    reconcile_tenant_scope,
)


def _metadata(**overrides: str) -> RustCallMetadata:
    """Builds metadata with sensible defaults plus overrides."""
    defaults = {
        "credential": "Bearer test-token",
        "correlation_id": "corr-1",
        "request_id": "req-1",
        "protocol_version": "1.0.0",
        "tenant_id": "tenant-1",
        "organization_id": "org-1",
    }
    defaults.update(overrides)
    return RustCallMetadata(**defaults)  # type: ignore[arg-type]


class TestExtractCallMetadata:
    """Parsing raw gRPC metadata pairs."""

    def test_extracts_all_known_headers(self) -> None:
        pairs = [
            (AUTHORIZATION_HEADER, "Bearer abc"),
            (TENANT_HEADER, "tenant-1"),
            (ORGANIZATION_HEADER, "org-1"),
            (CORRELATION_HEADER, "corr-1"),
            (REQUEST_ID_HEADER, "req-1"),
            (PROTOCOL_VERSION_HEADER, "1.0.0"),
            (TRACEPARENT_HEADER, "00-abc-def-01"),
            (GRPC_TIMEOUT_HEADER, "1500m"),
        ]
        metadata = extract_call_metadata(pairs)
        assert metadata.credential == "Bearer abc"
        assert metadata.tenant_id == "tenant-1"
        assert metadata.organization_id == "org-1"
        assert metadata.correlation_id == "corr-1"
        assert metadata.request_id == "req-1"
        assert metadata.protocol_version == "1.0.0"
        assert metadata.traceparent == "00-abc-def-01"
        assert metadata.timeout_seconds == 1.5

    def test_missing_headers_yield_none(self) -> None:
        metadata = extract_call_metadata([])
        assert metadata.credential is None
        assert metadata.tenant_id is None
        assert metadata.correlation_id is None
        assert metadata.timeout_seconds is None

    def test_header_names_are_case_insensitive(self) -> None:
        metadata = extract_call_metadata([("X-Tenant-Id", "tenant-9")])
        assert metadata.tenant_id == "tenant-9"

    def test_first_non_empty_value_wins(self) -> None:
        """A duplicated header must not let a later value take effect."""
        metadata = extract_call_metadata(
            [(CORRELATION_HEADER, "first"), (CORRELATION_HEADER, "second")]
        )
        assert metadata.correlation_id == "first"

    def test_empty_value_is_skipped(self) -> None:
        metadata = extract_call_metadata(
            [(CORRELATION_HEADER, "   "), (CORRELATION_HEADER, "real")]
        )
        assert metadata.correlation_id == "real"

    def test_unknown_headers_are_ignored(self) -> None:
        metadata = extract_call_metadata([("x-unknown-thing", "value")])
        assert metadata.correlation_id is None


class TestMetadataRepr:
    """The credential must never be printable."""

    def test_repr_hides_the_credential(self) -> None:
        rendered = repr(_metadata(credential="Bearer super-secret-token"))
        assert "super-secret-token" not in rendered

    def test_repr_reports_credential_presence(self) -> None:
        assert "credential=<present=True>" in repr(_metadata())
        assert "credential=<present=False>" in repr(RustCallMetadata())


class TestGrpcTimeoutParsing:
    """Deadline decoding."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("1S", 1.0),
            ("1500m", 1.5),
            ("500m", 0.5),
            ("2M", 120.0),
            ("1H", 3600.0),
            ("500u", 0.0005),
            ("1000000n", 0.001),
        ],
    )
    def test_valid_values(self, raw: str, expected: float) -> None:
        assert parse_grpc_timeout(raw) == pytest.approx(expected)

    @pytest.mark.parametrize(
        "raw",
        ["", "0", "1", "S", "1X", "abc", "123456789S", "1.5S", "-1S"],
    )
    def test_malformed_values_are_ignored_not_fatal(self, raw: str) -> None:
        """A bad timeout must not fail the call; gRPC enforces its own."""
        assert parse_grpc_timeout(raw) is None

    def test_none_is_none(self) -> None:
        assert parse_grpc_timeout(None) is None


class TestReconcileTenantScope:
    """The critical tenancy cross-check."""

    def test_matching_headers_are_accepted(self) -> None:
        reconcile_tenant_scope(
            body_tenant_id="tenant-1",
            body_organization_id="org-1",
            metadata=_metadata(),
        )

    def test_mismatched_tenant_is_refused(self) -> None:
        with pytest.raises(TenantContextError) as excinfo:
            reconcile_tenant_scope(
                body_tenant_id="tenant-1",
                body_organization_id="org-1",
                metadata=_metadata(tenant_id="tenant-EVIL"),
            )
        assert excinfo.value.details["reason"] == "tenant_mismatch"

    def test_mismatched_organization_is_refused(self) -> None:
        with pytest.raises(TenantContextError) as excinfo:
            reconcile_tenant_scope(
                body_tenant_id="tenant-1",
                body_organization_id="org-1",
                metadata=_metadata(organization_id="org-EVIL"),
            )
        assert excinfo.value.details["reason"] == "organization_mismatch"

    def test_absent_headers_are_tolerated(self) -> None:
        """Some Rust call paths legitimately omit the headers."""
        reconcile_tenant_scope(
            body_tenant_id="tenant-1",
            body_organization_id="org-1",
            metadata=RustCallMetadata(),
        )

    def test_error_never_echoes_the_values(self) -> None:
        """Naming the bad tenant in an error would leak another tenant's id."""
        with pytest.raises(TenantContextError) as excinfo:
            reconcile_tenant_scope(
                body_tenant_id="tenant-1",
                body_organization_id="org-1",
                metadata=_metadata(tenant_id="tenant-EVIL"),
            )
        assert "tenant-EVIL" not in str(excinfo.value)
        assert "tenant-1" not in str(excinfo.value)


class TestBuildOutboundMetadata:
    """Outbound metadata rules."""

    def test_includes_provided_values(self) -> None:
        pairs = build_outbound_metadata(
            correlation_id="corr-1",
            request_id="req-1",
            protocol_version="1.0.0",
            credential="Bearer tok",
        )
        assert (AUTHORIZATION_HEADER, "Bearer tok") in pairs
        assert (CORRELATION_HEADER, "corr-1") in pairs
        assert (REQUEST_ID_HEADER, "req-1") in pairs
        assert (PROTOCOL_VERSION_HEADER, "1.0.0") in pairs

    def test_never_sets_tenant_headers(self) -> None:
        """Python must not try to steer Rust's tenancy via a header."""
        pairs = build_outbound_metadata(
            correlation_id="c", request_id="r", protocol_version="1.0.0", credential="t"
        )
        keys = [key for key, _ in pairs]
        assert TENANT_HEADER not in keys
        assert ORGANIZATION_HEADER not in keys

    def test_omits_empty_values(self) -> None:
        pairs = build_outbound_metadata()
        assert pairs == []

    def test_traceparent_is_included_when_present(self) -> None:
        pairs = build_outbound_metadata(traceparent="00-abc-def-01")
        assert (TRACEPARENT_HEADER, "00-abc-def-01") in pairs
