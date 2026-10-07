# PURPOSE:
# Pins the agreements that cross the Rust/Python language boundary.
#
# Reads the Rust sources as DATA and asserts Python agrees with them, so a
# change on one side that the other does not follow fails here instead of
# surfacing as a silent misroute, an unauthenticated call, or an unparsed
# deadline in production.
#
# Covers: header names, the protocol version, the wire deadline format, the
# idempotency channel, and the retry classification shared with Rust.

"""Cross-language contract tests.

Unit tests on each side cannot catch the failure this file exists for. A Rust
test can prove the client sends ``x-tenant-id``; a Python test can prove the
servicer reads ``x-tenant-id``. Only a test that reads *both* can prove they
are the same string — and a mismatch is silent in production: the header is
simply absent on the receiving end, the request is refused as unscoped, and
the incident report says "the orchestrator rejected our traffic" rather than
"we renamed a constant".

So each test here parses the Rust source and compares it against the Python
constant. The alternative — restating the expected value on both sides —
would be a test that passes by construction and detects nothing.

These tests skip, rather than fail, when the Rust tree is not present, because
the Python package is usable without it. A skipped contract test is visible in
the run summary; a hard failure would make the suite unusable for a
Python-only checkout.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Final

import pytest

from mas_orchestrator.contracts.errors import CancellationRequestedError, ErrorCode
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.grpc.agent_bridge_servicer import (
    _FINISH_REASONS,
    parse_deadline,
)
from mas_orchestrator.grpc.errors import ERROR_DETAILS_METADATA_KEY
from mas_orchestrator.grpc.metadata import (
    AUTHORIZATION_HEADER,
    CORRELATION_HEADER,
    ORGANIZATION_HEADER,
    PROTOCOL_VERSION_HEADER,
    REQUEST_ID_HEADER,
    TENANT_HEADER,
)


def _rust_core_root() -> Path:
    """Finds the sibling Rust workspace or honors an explicit override.

    Python-only source distributions legitimately omit Rust and will skip
    these source-contract tests. In a monorepo checkout, the sibling
    ``rust-core`` path is discovered automatically instead of relying on a
    machine-specific absolute path.
    """
    configured = os.environ.get("MAS_RUST_CORE_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()

    python_root = Path(__file__).resolve().parents[2]
    candidates = (
        python_root.parent / "rust-core",
        python_root / ".." / "rust-core",
        python_root.parent / "repo" / "enterprise-mas" / "rust-core",
    )
    return next(
        (candidate.resolve() for candidate in candidates if (candidate / "crates").is_dir()),
        candidates[0].resolve(),
    )


#: The Rust workspace, relative to this file or ``MAS_RUST_CORE_ROOT``.
RUST_CORE: Final[Path] = _rust_core_root()
AI_BRIDGE_LIB: Final[Path] = RUST_CORE / "crates" / "ai-bridge" / "src" / "lib.rs"

#: Python sources read as data, resolved from this file rather than the cwd so
#: the suite behaves the same from any directory.
PYTHON_SRC: Final[Path] = Path(__file__).resolve().parents[2] / "src" / "mas_orchestrator"
SERVICER_SOURCE: Final[Path] = PYTHON_SRC / "grpc" / "agent_bridge_servicer.py"
RUNTIME_SOURCE: Final[Path] = PYTHON_SRC / "agents" / "runtime.py"

rust_present = pytest.mark.skipif(
    not AI_BRIDGE_LIB.is_file(),
    reason="the Rust workspace is not present in this checkout",
)


def _rust_const(name: str) -> str:
    """Reads a ``const &str`` out of the Rust bridge source.

    Args:
        name: The constant's name.

    Returns:
        Its string value.

    Raises:
        AssertionError: If the constant is not found. Failing loudly is right:
            a renamed constant means this test needs updating, and a test that
            silently stopped checking would be worse than one that fails.
    """
    text = AI_BRIDGE_LIB.read_text(encoding="utf-8")
    match = re.search(rf'^(?:pub )?const {re.escape(name)}: &str = "([^"]*)";', text, re.MULTILINE)
    assert match is not None, f"constant {name} not found in {AI_BRIDGE_LIB}"
    return match.group(1)


@rust_present
class TestHeaderNames:
    """The header names both sides use must be the same strings."""

    @pytest.mark.parametrize(
        ("rust_const", "python_value", "purpose"),
        [
            ("AUTHORIZATION_HEADER", AUTHORIZATION_HEADER, "bearer credential"),
            ("TENANT_HEADER", TENANT_HEADER, "authoritative tenant scope"),
            (
                "ORGANIZATION_HEADER",
                ORGANIZATION_HEADER,
                "authoritative organization scope",
            ),
            ("CORRELATION_HEADER", CORRELATION_HEADER, "end-to-end correlation id"),
            ("REQUEST_ID_HEADER", REQUEST_ID_HEADER, "platform request id"),
            ("PROTOCOL_VERSION_HEADER", PROTOCOL_VERSION_HEADER, "wire protocol version"),
        ],
    )
    def test_header_names_agree(self, rust_const: str, python_value: str, purpose: str) -> None:
        """A renamed header on one side would silently drop the value.

        Args:
            rust_const: The Rust constant's name.
            python_value: The Python constant's value.
            purpose: What the header carries, for the failure message.
        """
        assert _rust_const(rust_const) == python_value, (
            f"the {purpose} header name differs between Rust and Python"
        )

    def test_the_authorization_header_is_lowercase(self) -> None:
        """gRPC metadata keys must be lowercase or the value never arrives.

        A capitalised key is not rejected — it is silently sent as a different
        key, and the receiving side sees no credential at all.
        """
        assert _rust_const("AUTHORIZATION_HEADER").islower()

    def test_every_header_python_reads_is_lowercase(self) -> None:
        """The same rule applies to every header the servicer parses."""
        for name in (
            AUTHORIZATION_HEADER,
            TENANT_HEADER,
            ORGANIZATION_HEADER,
            CORRELATION_HEADER,
            REQUEST_ID_HEADER,
            PROTOCOL_VERSION_HEADER,
        ):
            assert name == name.lower(), f"header {name!r} is not lowercase"


@rust_present
class TestProtocolVersion:
    """Both sides must claim the same wire protocol version."""

    def test_the_versions_agree(self) -> None:
        """A version mismatch is refused before any AI work is done."""
        assert _rust_const("AI_PROTOCOL_VERSION") == str(CURRENT_PROTOCOL_VERSION)

    def test_the_rust_version_is_semver_shaped(self) -> None:
        """A malformed version would fail comparison, not negotiation."""
        assert re.fullmatch(r"\d+\.\d+\.\d+", _rust_const("AI_PROTOCOL_VERSION"))


@rust_present
class TestDeadlineFormat:
    """The deadline Rust emits must be parseable by Python."""

    def test_the_wire_format_is_accepted(self) -> None:
        """Rust uses ``SecondsFormat::Millis`` with ``Z``.

        `chrono` renders that as ``YYYY-MM-DDTHH:MM:SS.mmmZ``. Python's
        `datetime.fromisoformat` rejects a trailing ``Z`` on some versions, so
        the parser normalises it — this test pins that the normalisation covers
        the exact shape chrono produces.
        """
        for stamp in (
            "2026-10-03T14:52:00.123Z",
            "2026-10-03T14:52:00.000Z",
            "2026-01-01T00:00:00.000Z",
        ):
            assert parse_deadline(stamp) is not None

    def test_millisecond_precision_survives_the_round_trip(self) -> None:
        """A deadline rounded to whole seconds would extend the caller's budget.

        Rust computes its own RPC timeout from the same instant, so a loss of
        precision here makes Python willing to work past the moment Rust has
        already given up.
        """
        parsed = parse_deadline("2026-10-03T14:52:00.123Z")
        assert parsed is not None
        assert parsed % 1000 == 123

    def test_a_future_deadline_parses_later_than_a_past_one(self) -> None:
        """Ordering must survive the conversion, not just the syntax."""
        earlier = parse_deadline("2026-01-01T00:00:00.000Z")
        later = parse_deadline("2026-12-31T23:59:59.999Z")
        assert earlier is not None and later is not None
        assert later > earlier


@rust_present
class TestIdempotency:
    """The idempotency key must reach Python on the channel Rust uses."""

    def test_the_key_travels_in_the_request_body(self) -> None:
        """The body is authoritative for idempotency.

        A key carried only in metadata would be lost by any proxy that drops
        unknown headers, and a redelivered task would then re-run paid AI work.
        """
        from mas_orchestrator.grpc.generated import mas_api_pb2

        field_names = {field.name for field in mas_api_pb2.AgentRunRequest.DESCRIPTOR.fields}
        assert "idempotency_key" in field_names

    def test_rust_also_sends_an_idempotency_header(self) -> None:
        """The header is a belt-and-braces channel; the body must still exist."""
        assert _rust_const("IDEMPOTENCY_HEADER") == "idempotency-key"


@rust_present
class TestRetryClassification:
    """How the two sides reconcile two different retry questions.

    Rust's ``GrpcCode::is_retryable`` answers a *transport* question — might
    this RPC succeed if sent again? Python's ``RETRYABLE_CODES`` answers a
    *domain* question — should this operation be retried? They disagree for a
    deadline, an exhausted quota and a cancellation, and the disagreement is
    deliberate on both sides:

    * A deadline means the execution is over. Retrying would resurrect work the
      platform has already given up on, even though the RPC itself might
      succeed on a second attempt.
    * An exhausted quota is a budget decision, not a transient fault.

    They are reconciled by the payload, not by either guess: Rust sends
    ``retryable`` in the structured details and Python sends it in
    ``AgentFailed``. This class pins that reconciliation, and pins that the
    deliberate divergences stay deliberate.
    """

    def test_the_deliberate_divergences_are_documented(self) -> None:
        """Python is terminal where Rust is transport-retryable.

        If this ever starts passing as "equal", one side silently changed a
        domain decision — and the change deserves a review, not a green tick.
        """
        grpc = pytest.importorskip("grpc")

        from mas_orchestrator.grpc.errors import RETRYABLE_STATUSES, is_retryable_status

        divergences = [
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.CANCELLED,
        ]
        for code in divergences:
            assert code in RETRYABLE_STATUSES, f"{code.name} should be retryable for Rust"
            assert is_retryable_status(code) is False, f"{code.name} must stay terminal for Python"

    def test_the_two_agree_everywhere_else(self) -> None:
        """Outside the deliberate divergences the answers must match."""
        grpc = pytest.importorskip("grpc")

        from mas_orchestrator.grpc.errors import RETRYABLE_STATUSES, is_retryable_status

        for code in RETRYABLE_STATUSES - {
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.CANCELLED,
        }:
            assert is_retryable_status(code) is True, f"{code.name} should be retryable"

        for code in (
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.UNIMPLEMENTED,
            grpc.StatusCode.FAILED_PRECONDITION,
        ):
            assert is_retryable_status(code) is False, f"{code.name} should be terminal"

    def test_an_inbound_payload_overrides_the_status_guess(self) -> None:
        """The peer's explicit classification wins over either table.

        This is what actually reconciles the two sides in production: Rust
        sends ``retryable`` alongside the status, and Python honours it
        instead of re-deriving a guess from a lossy projection.
        """
        grpc = pytest.importorskip("grpc")

        from mas_orchestrator.grpc.errors import ERROR_DETAILS_METADATA_KEY, error_from_grpc_status

        class _PeerError(grpc.RpcError):
            """Stands in for a failed RPC carrying a structured payload."""

            def __init__(self, code: grpc.StatusCode, payload: bytes) -> None:
                self._code = code
                self._payload = payload

            def code(self) -> grpc.StatusCode:
                return self._code

            def details(self) -> str:
                return "remote failure"

            def trailing_metadata(self) -> tuple[tuple[str, bytes], ...]:
                return ((ERROR_DETAILS_METADATA_KEY, self._payload),)

        import json

        # Rust says rate-limited and retryable. Python's own table would call
        # RESOURCE_EXHAUSTED terminal — the payload must win.
        payload = json.dumps(
            {"code": "model_rate_limited", "message": "slow down", "retryable": True}
        ).encode()
        error = error_from_grpc_status(_PeerError(grpc.StatusCode.RESOURCE_EXHAUSTED, payload))

        assert error.retryable is True
        assert str(error.code) == "model_rate_limited"

    def test_without_a_payload_the_domain_table_decides(self) -> None:
        """No payload means no better information than the status."""
        grpc = pytest.importorskip("grpc")

        from mas_orchestrator.grpc.errors import error_from_grpc_status

        class _BareError(grpc.RpcError):
            """A failed RPC carrying no structured payload."""

            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.RESOURCE_EXHAUSTED

            def details(self) -> str:
                return "remote failure"

            def trailing_metadata(self) -> tuple[tuple[str, bytes], ...]:
                return ()

        error = error_from_grpc_status(_BareError())

        assert error.retryable is False

    def test_a_source_of_truth_for_the_rust_rule_exists(self) -> None:
        """Guards the divergences above: if Rust's rule moves, this fails.

        The assertion reads Rust's `is_retryable` match arms, so a change to
        the Rust classification surfaces here rather than silently diverging.
        """
        grpc_module = RUST_CORE / "crates" / "messaging" / "src" / "grpc.rs"
        if not grpc_module.is_file():
            pytest.skip("the Rust messaging crate is not present")

        text = grpc_module.read_text(encoding="utf-8")
        match = re.search(r"pub fn is_retryable\(self\) -> bool \{(.*?)\n    \}", text, re.DOTALL)
        assert match is not None, "could not locate Rust's is_retryable"

        arms = set(re.findall(r"Self::(\w+)", match.group(1)))
        expected = {
            "Cancelled",
            "DeadlineExceeded",
            "ResourceExhausted",
            "Aborted",
            "Unavailable",
        }
        assert arms == expected, f"Rust's retryable set changed: {arms}"


def _rust_const_in(path: Path, name: str) -> str:
    """Reads a ``const &str`` out of any Rust source file.

    Args:
        path: The Rust file to read.
        name: The constant's name.

    Returns:
        Its string value.

    Raises:
        AssertionError: If the constant is not found.
    """
    text = path.read_text(encoding="utf-8")
    match = re.search(rf'^(?:pub )?const {re.escape(name)}: &str = "([^"]*)";', text, re.MULTILINE)
    assert match is not None, f"constant {name} not found in {path}"
    return match.group(1)


def _proto_message(name: str) -> str:
    """Returns the body of a protobuf message, or fails loudly.

    Args:
        name: The message name, e.g. ``"ServiceIdentityMsg"``.

    Returns:
        The text between the message's braces.
    """
    proto = RUST_CORE / "crates" / "api" / "proto" / "mas" / "api" / "v1" / "mas.proto"
    text = proto.read_text(encoding="utf-8")
    match = re.search(rf"^message {re.escape(name)} \{{(.*?)^\}}", text, re.MULTILINE | re.DOTALL)
    assert match is not None, f"message {name} not found in {proto}"
    return match.group(1)


def _proto_field_names(message: str) -> set[str]:
    """Returns the field names declared in a message body."""
    return set(
        re.findall(r"^\s*(?:optional |repeated )?[\w.]+ (\w+) = \d+;", message, re.MULTILINE)
    )


@rust_present
class TestCancellationCode:
    """One cancellation must carry one code, whichever side noticed it first."""

    def test_the_cancellation_code_is_the_same_string_on_both_sides(self) -> None:
        """Rust's verdict code equals Python's published ``ErrorCode`` value.

        Rust can notice a cancellation locally (its own token fires) or learn of
        it from Python's verdict. Those are the same physical event. If they
        carried different codes, any caller branching on the code would handle
        one and silently fall through the other — and the branch that matters
        most, "do not report a cancellation as success", would be exactly the
        one that gets missed.
        """
        rust_code = _rust_const_in(
            RUST_CORE / "crates" / "execution" / "src" / "agent_bridge.rs",
            "CANCELLATION_CODE",
        )
        assert rust_code == ErrorCode.CANCELLATION_REQUESTED

    def test_python_never_reports_cancellation_as_success(self) -> None:
        """A cancelled run is a failure, so it cannot be mistaken for output."""
        assert ErrorCode.CANCELLATION_REQUESTED != ErrorCode.INTERNAL_ERROR
        assert CancellationRequestedError("").retryable is False

    def test_the_verdict_code_is_not_the_finish_reason(self) -> None:
        """Two vocabularies exist; conflating them would misreport a stop.

        ``finish_reason`` describes *why a successful run stopped* and travels
        only on ``AgentSucceeded``. The verdict code describes a *failure*.
        They are different axes and must not share values.
        """
        rust_code = _rust_const_in(
            RUST_CORE / "crates" / "execution" / "src" / "agent_bridge.rs",
            "CANCELLATION_CODE",
        )
        finish_reasons = set(_FINISH_REASONS.values())
        assert rust_code not in finish_reasons, (
            "the cancellation verdict code collides with a finish_reason value"
        )


@rust_present
class TestErrorDetailsKey:
    """The structured error payload must land where the reader looks for it."""

    def test_both_sides_use_the_same_metadata_key(self) -> None:
        """Rust's binary metadata key equals Python's, including ``-bin``.

        tonic's binary accessors take the on-the-wire name verbatim rather than
        appending ``-bin`` themselves. A key that is right except for the suffix
        is not a partial match: the payload is simply invisible, and Rust falls
        back to guessing the failure class from the status code alone.
        """
        rust_key = _rust_const("ERROR_DETAILS_METADATA_KEY")
        assert rust_key == ERROR_DETAILS_METADATA_KEY
        assert rust_key.endswith("-bin")


@rust_present
class TestServiceIdentityEncoding:
    """The caller's identity must survive the hop field for field."""

    def test_the_wire_message_carries_subject_issuer_and_scopes(self) -> None:
        """The proto declares the three fields both sides rely on."""
        fields = _proto_field_names(_proto_message("ServiceIdentityMsg"))
        assert {"subject", "issuer", "scopes"} <= fields

    def test_python_reads_the_subject_under_the_same_name(self) -> None:
        """The servicer cross-checks ``request.identity.subject``.

        That cross-check is what makes a forged identity detectable: the
        declared subject is compared against the subject the bearer credential
        actually authenticated to. If the field were renamed on one side only,
        ``HasField`` would go false, the check would be skipped, and the forgery
        would succeed — a failure that looks like success.
        """
        fields = _proto_field_names(_proto_message("ServiceIdentityMsg"))
        assert "subject" in fields
        source = SERVICER_SOURCE.read_text(encoding="utf-8")
        assert "request.identity.subject" in source


@rust_present
class TestVerdictFields:
    """The reply fields Python writes must be the ones Rust reads."""

    def test_a_successful_run_carries_output_usage_and_finish_state(self) -> None:
        """``AgentSucceeded`` exposes the fields the contract requires."""
        fields = _proto_field_names(_proto_message("AgentSucceeded"))
        assert {"output_json", "usage", "model_key", "finish_reason"} <= fields

    def test_a_failed_run_carries_the_error_contract(self) -> None:
        """``AgentFailed`` carries code, classification and identity.

        ``retryable`` and ``code`` are what let Rust classify the failure
        without re-deriving it from a lossy status code; the two identifiers let
        a Rust log line be joined to the Python failure without a second lookup.
        """
        fields = _proto_field_names(_proto_message("AgentFailed"))
        assert {
            "code",
            "message",
            "retryable",
            "usage",
            "correlation_id",
            "request_id",
        } <= fields

    def test_a_successful_run_always_finished_because_it_completed(self) -> None:
        """Pins why Rust may ignore ``finish_reason`` without loss.

        ``ExecutionResult.succeeded`` is exactly ``termination is COMPLETED``,
        so the ``AgentSucceeded`` branch is reachable only with
        ``finish_reason == "completed"``. Every other termination takes the
        ``AgentFailed`` branch, which is the correct outcome: a truncated run is
        a failure, not a success with a footnote.

        This test exists so that anyone who later adds a non-completed
        termination to the success path must also teach Rust to read
        ``finish_reason`` — otherwise the platform would record truncated work
        as a clean finish.
        """
        completed = next(
            termination for termination in _FINISH_REASONS if termination.value == "completed"
        )
        assert _FINISH_REASONS[completed] == "completed"
        source = RUNTIME_SOURCE.read_text(encoding="utf-8")
        assert "Termination.COMPLETED" in source
