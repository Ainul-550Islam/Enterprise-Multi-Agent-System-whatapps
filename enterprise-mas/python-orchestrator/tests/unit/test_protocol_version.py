"""Unit tests for protocol version negotiation.

Version negotiation is a security boundary: a caller we do not understand must
be refused, never defaulted into something plausible.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import ProtocolError
from mas_orchestrator.contracts.protocol_version import (
    CURRENT_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
    ProtocolVersion,
    negotiate_protocol_version,
    parse_protocol_version,
)


class TestParseProtocolVersion:
    """Strict parsing."""

    def test_parses_canonical_form(self) -> None:
        assert parse_protocol_version("1.0.0") == ProtocolVersion(1, 0, 0)

    def test_comparison_is_numeric_not_lexical(self) -> None:
        """1.10.0 must sort above 1.9.0."""
        assert ProtocolVersion(1, 10, 0) > ProtocolVersion(1, 9, 0)

    def test_accepts_large_components(self) -> None:
        assert parse_protocol_version("12.345.6789") == ProtocolVersion(12, 345, 6789)

    def test_str_round_trips(self) -> None:
        assert str(parse_protocol_version("2.3.4")) == "2.3.4"

    @pytest.mark.parametrize(
        "raw",
        [
            "",
            "1",
            "1.0",
            "1.0.0.0",
            "1.0.x",
            "1.0.0-rc1",
            "1.0.0+build",
            "v1.0.0",
            "1.-1.0",
        ],
    )
    def test_malformed_versions_are_rejected(self, raw: str) -> None:
        with pytest.raises(ProtocolError):
            parse_protocol_version(raw)

    def test_major_zero_is_rejected_as_unstable(self) -> None:
        """Pre-stable contracts are never negotiated automatically."""
        with pytest.raises(ProtocolError) as excinfo:
            parse_protocol_version("0.9.0")
        assert excinfo.value.details["reason"] == "unstable_major"

    def test_is_stable_flag(self) -> None:
        assert ProtocolVersion(1, 0, 0).is_stable is True
        assert ProtocolVersion(0, 1, 0).is_stable is False


class TestNegotiateProtocolVersion:
    """Negotiation rules."""

    def test_current_version_is_supported(self) -> None:
        assert CURRENT_PROTOCOL_VERSION in SUPPORTED_PROTOCOL_VERSIONS

    def test_supported_version_is_accepted(self) -> None:
        assert negotiate_protocol_version("1.0.0") == ProtocolVersion(1, 0, 0)

    def test_missing_version_is_rejected(self) -> None:
        """Defaulting a missing version would admit incompatible callers."""
        with pytest.raises(ProtocolError) as excinfo:
            negotiate_protocol_version(None)
        assert excinfo.value.details["reason"] == "absent"

    def test_blank_version_is_rejected(self) -> None:
        with pytest.raises(ProtocolError):
            negotiate_protocol_version("   ")

    def test_future_major_is_rejected_with_major_mismatch(self) -> None:
        with pytest.raises(ProtocolError) as excinfo:
            negotiate_protocol_version("2.0.0")
        assert excinfo.value.details["reason"] == "major_mismatch"

    def test_past_major_is_rejected(self) -> None:
        with pytest.raises(ProtocolError) as excinfo:
            negotiate_protocol_version("0.1.0")
        assert excinfo.value.details["reason"] in {"major_mismatch", "unstable_major"}

    def test_unsupported_minor_is_rejected(self) -> None:
        """A newer caller may send fields we would silently ignore."""
        with pytest.raises(ProtocolError) as excinfo:
            negotiate_protocol_version("1.99.0")
        assert excinfo.value.details["reason"] == "unsupported_minor"

    def test_error_lists_supported_versions(self) -> None:
        """The error must tell the operator what to upgrade to."""
        with pytest.raises(ProtocolError) as excinfo:
            negotiate_protocol_version("9.9.9")
        assert excinfo.value.details["supported"]

    def test_malformed_version_rejected_during_negotiation(self) -> None:
        with pytest.raises(ProtocolError):
            negotiate_protocol_version("not-a-version")


class TestOrdering:
    """Version ordering semantics."""

    def test_ordering_across_major(self) -> None:
        versions = [ProtocolVersion(2, 0, 0), ProtocolVersion(1, 5, 0), ProtocolVersion(1, 0, 9)]
        assert sorted(versions) == [
            ProtocolVersion(1, 0, 9),
            ProtocolVersion(1, 5, 0),
            ProtocolVersion(2, 0, 0),
        ]

    def test_equality_and_hashing(self) -> None:
        """Frozen dataclass semantics enable use in sets and dicts."""
        assert ProtocolVersion(1, 0, 0) == ProtocolVersion(1, 0, 0)
        assert len({ProtocolVersion(1, 0, 0), ProtocolVersion(1, 0, 0)}) == 1
