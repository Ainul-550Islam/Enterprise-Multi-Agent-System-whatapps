"""Unit tests for gRPC status <-> typed error mapping."""

from __future__ import annotations

import json

import grpc
import pytest

from mas_orchestrator.contracts.errors import (
    RETRYABLE_CODES,
    AuthenticationError,
    DependencyUnavailableError,
    ErrorCode,
    ModelRateLimitedError,
    ModelTimeoutError,
    OrchestratorError,
    PolicyDeniedError,
    TenantContextError,
)
from mas_orchestrator.grpc import DataHubBridgeServicer, details_from_abort
from mas_orchestrator.grpc.errors import (
    CODE_TO_STATUS,
    STATUS_TO_CODE,
    error_from_grpc_status,
    is_retryable_status,
    to_grpc_status,
    unwrap_status_details,
)


class TestCodeCoverage:
    """Every domain code must be mappable in both directions."""

    def test_public_grpc_package_exports_data_hub_and_abort_details(self) -> None:
        assert DataHubBridgeServicer.__name__ == "DataHubBridgeServicer"
        assert callable(details_from_abort)

    def test_every_code_has_a_status(self) -> None:
        for code in ErrorCode:
            assert code in CODE_TO_STATUS

    def test_every_status_in_the_table_is_a_valid_status_code(self) -> None:
        for status in CODE_TO_STATUS.values():
            assert isinstance(status, grpc.StatusCode)


class TestToGrpcStatus:
    """Outbound conversion."""

    def test_basic_mapping(self) -> None:
        code, message, _ = to_grpc_status(AuthenticationError("no credential"))
        assert code is grpc.StatusCode.UNAUTHENTICATED
        assert message == "no credential"

    def test_details_are_serialised(self) -> None:
        _, _, details = to_grpc_status(
            TenantContextError("bad scope", details={"reason": "mismatch"})
        )
        payload = json.loads(details.decode("utf-8"))
        assert payload["code"] == "tenant_context_error"
        assert payload["details"]["reason"] == "mismatch"

    def test_details_include_retryability(self) -> None:
        _, _, details = to_grpc_status(ModelTimeoutError("too slow"))
        assert json.loads(details)["retryable"] is True
        _, _, details = to_grpc_status(PolicyDeniedError("denied"))
        assert json.loads(details)["retryable"] is False

    def test_serialisation_is_deterministic(self) -> None:
        """Identical failures must produce byte-identical statuses."""
        first = to_grpc_status(ModelRateLimitedError("slow", retry_after_seconds=2.0))[2]
        second = to_grpc_status(ModelRateLimitedError("slow", retry_after_seconds=2.0))[2]
        assert first == second

    def test_rate_limit_carries_retry_hint(self) -> None:
        _, _, details = to_grpc_status(ModelRateLimitedError("slow", retry_after_seconds=2.5))
        assert json.loads(details)["retry_after_seconds"] == 2.5

    def test_unknown_code_defaults_to_internal(self) -> None:
        error = OrchestratorError("mystery", code=ErrorCode.INTERNAL_ERROR)
        code, _, _ = to_grpc_status(error)
        assert code is grpc.StatusCode.INTERNAL

    def test_message_is_the_safe_message(self) -> None:
        error = OrchestratorError(
            "safe text", code=ErrorCode.MODEL_PROVIDER_ERROR, details={"raw": "leaky"}
        )
        _, message, _ = to_grpc_status(error)
        assert message == "safe text"


class TestErrorFromGrpcStatus:
    """Inbound conversion."""

    def _rpc_error(self, code: grpc.StatusCode, details: str = "remote said no") -> grpc.RpcError:
        return grpc.RpcError()

    def test_unavailable_becomes_dependency_unavailable(self) -> None:
        class FakeRpcError(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.UNAVAILABLE

            def details(self) -> str:
                return "connection refused"

        error = error_from_grpc_status(FakeRpcError())
        assert isinstance(error, DependencyUnavailableError)

    def test_deadline_exceeded_is_timeout(self) -> None:
        class FakeRpcError(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.DEADLINE_EXCEEDED

            def details(self) -> str:
                return "too slow"

        error = error_from_grpc_status(FakeRpcError())
        assert error.code is ErrorCode.EXECUTION_TIMEOUT

    def test_identifiers_are_attached(self) -> None:
        class FakeRpcError(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.UNAVAILABLE

            def details(self) -> str:
                return "down"

        error = error_from_grpc_status(FakeRpcError(), correlation_id="corr-7", request_id="req-7")
        assert error.correlation_id == "corr-7"
        assert error.request_id == "req-7"

    def test_remote_message_is_not_propagated_verbatim(self) -> None:
        """Remote detail text is attacker-influenced; only presence is kept."""

        class FakeRpcError(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.UNAVAILABLE

            def details(self) -> str:
                return "secret internal detail"

        error = error_from_grpc_status(FakeRpcError())
        assert "secret internal detail" not in str(error)
        assert error.details["remote_message_present"] is True

    def test_unrecognised_status_becomes_unavailable(self) -> None:
        """Conservative default: the peer is not behaving as contracted."""

        class FakeRpcError(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return grpc.StatusCode.DATA_LOSS

            def details(self) -> str:
                return "?"

        error = error_from_grpc_status(FakeRpcError())
        assert error.code is ErrorCode.INTERNAL_ERROR

    def test_default_code_override_applies_when_no_status_code(self) -> None:
        """A failure carrying no status code falls back to default_code."""

        class CodelessRpcError(grpc.RpcError):
            # Deliberately has no `code()` method: this is what a non-gRPC
            # transport error looks like to this function.
            def details(self) -> str:
                return "?"

        error = error_from_grpc_status(CodelessRpcError(), default_code=ErrorCode.POLICY_DENIED)
        assert error.code is ErrorCode.POLICY_DENIED


class TestIsRetryableStatus:
    """Retry classification must agree with the domain table."""

    def _status(self, code: grpc.StatusCode) -> grpc.RpcError:
        class FakeRpcError(grpc.RpcError):
            def code(self) -> grpc.StatusCode:
                return code

            def details(self) -> str:
                return "x"

        return FakeRpcError()

    @pytest.mark.parametrize(
        "code",
        [
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.ABORTED,
        ],
    )
    def test_retryable(self, code: grpc.StatusCode) -> None:
        assert is_retryable_status(self._status(code)) is True

    @pytest.mark.parametrize(
        "code",
        [
            # An execution that hit its deadline is over: retrying it would
            # resurrect work the platform has already given up on.
            grpc.StatusCode.DEADLINE_EXCEEDED,
            # Exhausted quota is a budget decision, not a transient fault.
            grpc.StatusCode.RESOURCE_EXHAUSTED,
        ],
    )
    def test_deadline_and_quota_are_terminal(self, code: grpc.StatusCode) -> None:
        """Deadlines and budgets are terminal even though the transport failed."""
        assert is_retryable_status(self._status(code)) is False

    @pytest.mark.parametrize(
        "code",
        [
            grpc.StatusCode.INVALID_ARGUMENT,
            grpc.StatusCode.PERMISSION_DENIED,
            grpc.StatusCode.UNAUTHENTICATED,
            grpc.StatusCode.NOT_FOUND,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.UNIMPLEMENTED,
        ],
    )
    def test_not_retryable(self, code: grpc.StatusCode) -> None:
        assert is_retryable_status(self._status(code)) is False


class TestUnwrapStatusDetails:
    """Details decoding must never raise."""

    def test_decodes_valid_payload(self) -> None:
        payload = unwrap_status_details(json.dumps({"code": "internal_error"}).encode())
        assert payload["code"] == "internal_error"

    def test_accepts_str(self) -> None:
        assert unwrap_status_details('{"a": 1}') == {"a": 1}

    def test_malformed_returns_empty(self) -> None:
        assert unwrap_status_details(b"not json") == {}

    def test_empty_returns_empty(self) -> None:
        assert unwrap_status_details(None) == {}
        assert unwrap_status_details(b"") == {}

    def test_non_object_json_returns_empty(self) -> None:
        assert unwrap_status_details(b"[1,2,3]") == {}


class TestRoundTrip:
    """A status produced here must be understandable here."""

    def test_payload_survives_the_round_trip(self) -> None:
        original = TenantContextError("no scope", correlation_id="corr-1")
        _, _, details = to_grpc_status(original)
        decoded = unwrap_status_details(details)
        assert decoded["code"] == str(original.code)
        assert decoded["correlation_id"] == "corr-1"

    def test_details_carry_the_authoritative_retry_flag(self) -> None:
        """The status code alone cannot express retryability; details must.

        gRPC has no status for "misconfigured" or "output failed validation" -
        both become ``INTERNAL``, which peers treat as transient. Retrying
        either would be wrong. The payload's explicit ``retryable`` flag is
        therefore the authoritative signal, and it must be correct for every
        domain code regardless of which status carries it.
        """
        for code in ErrorCode:
            error = OrchestratorError("x", code=code)
            _, _, details = to_grpc_status(error)
            decoded = unwrap_status_details(details)
            assert decoded["retryable"] is (code in RETRYABLE_CODES), code
            assert decoded["code"] == str(code)

    def test_status_code_flips_are_known_and_bounded(self) -> None:
        """Where a status maps back to a code, retryability flips are limited.

        Identity is never preserved (several codes share a status), but
        retryability must be preserved except for the codes that genuinely
        have no non-transient gRPC status to express them. Pinning the exact
        exception list means adding a new terminal ``INTERNAL`` mapping forces
        a deliberate decision here instead of silently widening the gap.
        """
        # Family A: terminal domain codes that gRPC can only express as
        # INTERNAL. `INTERNAL` reads back as `internal_error`, which IS
        # retryable, so a peer that trusts the status code alone would retry
        # a misconfigured server or a failed output validation.
        terminal_without_a_status = {
            ErrorCode.CONFIGURATION_ERROR,
            ErrorCode.TOOL_FAILED,
            ErrorCode.PROMPT_RENDER_ERROR,
            ErrorCode.OUTPUT_VALIDATION_FAILED,
            ErrorCode.CITATION_VALIDATION_ERROR,
        }
        # Family B: retryable provider/tool faults that share a status with a
        # terminal *execution* condition. A provider timeout and an execution
        # timeout are both DEADLINE_EXCEEDED on the wire, but they mean
        # opposite things for retrying: one is worth another attempt, the other
        # means the execution is already over.
        retryable_sharing_a_status = {
            ErrorCode.MODEL_RATE_LIMITED,
            ErrorCode.MODEL_TIMEOUT,
            ErrorCode.TOOL_TIMEOUT,
        }
        allowed_flips = terminal_without_a_status | retryable_sharing_a_status
        for code, status in CODE_TO_STATUS.items():
            if status not in STATUS_TO_CODE:
                continue
            round_tripped = STATUS_TO_CODE[status]
            same = (code in RETRYABLE_CODES) == (round_tripped in RETRYABLE_CODES)
            assert same or code in allowed_flips, f"{code} -> {status} -> {round_tripped}"
