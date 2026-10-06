"""Unit tests for the typed error model.

These tests pin the contract properties the rest of the orchestrator depends
on: stable codes, explicit retryability, safe rendering, and the guarantee that
no subclass can accidentally opt into retrying a terminal failure.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import (
    RETRYABLE_CODES,
    TERMINAL_CODES,
    AuthenticationError,
    ConfigurationError,
    ErrorCode,
    InternalError,
    MemoryBackendError,
    ModelProviderError,
    ModelRateLimitedError,
    OrchestratorError,
    ProtocolError,
    TenantContextError,
    error_from_code,
    is_retryable,
)


class TestErrorCodeCoverage:
    """The code enum is the wire contract; its shape must not drift."""

    def test_every_code_has_a_concrete_exception_type(self) -> None:
        """No code may exist without a typed exception to reconstruct it."""
        for code in ErrorCode:
            reconstructed = error_from_code(code, "boom")
            assert reconstructed.code is code

    def test_retryable_and_terminal_partition_the_code_space(self) -> None:
        """The two sets are disjoint and together cover every code."""
        assert not (RETRYABLE_CODES & TERMINAL_CODES)
        assert frozenset(ErrorCode) == (RETRYABLE_CODES | TERMINAL_CODES)

    def test_cancellation_is_never_retryable(self) -> None:
        """Cancelled work must never be resurrected by a retry."""
        from mas_orchestrator.contracts.errors import CancellationRequestedError

        assert CancellationRequestedError("cancelled").retryable is False

    def test_authentication_failures_are_never_retryable(self) -> None:
        """Retrying an auth failure only delays reporting it."""
        assert AuthenticationError("nope").retryable is False

    def test_expected_retryable_codes(self) -> None:
        """Transient provider/transport failures are the retryable set."""
        assert ErrorCode.MODEL_RATE_LIMITED in RETRYABLE_CODES
        assert ErrorCode.MODEL_TIMEOUT in RETRYABLE_CODES
        assert ErrorCode.MODEL_PROVIDER_ERROR in RETRYABLE_CODES
        assert ErrorCode.DEPENDENCY_UNAVAILABLE in RETRYABLE_CODES


class TestOrchestratorError:
    """Base-class behaviour."""

    def test_default_code_is_internal(self) -> None:
        error = OrchestratorError("something failed")
        assert error.code is ErrorCode.INTERNAL_ERROR

    def test_retryability_is_derived_from_the_code_by_default(self) -> None:
        """A subclass that forgets to pass retryable still behaves sanely."""
        assert OrchestratorError("x", code=ErrorCode.MODEL_TIMEOUT).retryable is True
        assert OrchestratorError("x", code=ErrorCode.POLICY_DENIED).retryable is False

    def test_explicit_retryable_overrides_the_code(self) -> None:
        """A caller may force a non-retryable rate-limit (budget exhausted)."""
        error = OrchestratorError(
            "budget exhausted", code=ErrorCode.MODEL_RATE_LIMITED, retryable=False
        )
        assert error.retryable is False

    def test_str_includes_code_and_correlation(self) -> None:
        error = TenantContextError("no tenant", correlation_id="corr-1", request_id="req-1")
        rendered = str(error)
        assert "tenant_context_error" in rendered
        assert "corr-1" in rendered
        assert "req-1" in rendered

    def test_str_omits_identifiers_when_absent(self) -> None:
        rendered = str(ConfigurationError("bad config"))
        assert "[" not in rendered

    def test_repr_exposes_retryability_and_detail_keys(self) -> None:
        error = OrchestratorError("x", details={"a": 1, "b": 2})
        assert "retryable" in repr(error)
        assert "['a', 'b']" in repr(error)

    def test_str_never_leaks_details(self) -> None:
        """Details are structured; a stray str() must not dump them."""
        error = OrchestratorError("failed", details={"secret_note": "s3cr3t-detail"})
        assert "s3cr3t-detail" not in str(error)


class TestWithContext:
    """The copy-on-enrich API used at boundaries."""

    def test_with_context_preserves_code_and_message(self) -> None:
        original = ProtocolError("bad version")
        enriched = original.with_context(correlation_id="corr-9")
        assert enriched.code is original.code
        assert enriched.message == original.message

    def test_with_context_merges_details_without_mutating(self) -> None:
        original = ProtocolError("bad version", details={"a": 1})
        enriched = original.with_context(details={"b": 2})
        assert enriched.details == {"a": 1, "b": 2}
        assert original.details == {"a": 1}

    def test_with_context_does_not_clear_existing_identifiers(self) -> None:
        original = ProtocolError("bad version", correlation_id="corr-1")
        enriched = original.with_context(request_id="req-2")
        assert enriched.correlation_id == "corr-1"
        assert enriched.request_id == "req-2"

    def test_with_context_returns_the_concrete_type(self) -> None:
        enriched = ProtocolError("x").with_context()
        assert isinstance(enriched, ProtocolError)


class TestToPayload:
    """Serialisation for gRPC status details and event payloads."""

    def test_payload_carries_code_message_retryable(self) -> None:
        payload = ModelProviderError("upstream 500", status_code=500).to_payload()
        assert payload["code"] == "model_provider_error"
        assert payload["message"] == "upstream 500"
        assert payload["retryable"] is True
        assert payload["status_code"] == 500

    def test_payload_omits_absent_optional_fields(self) -> None:
        payload = InternalError("boom").to_payload()
        assert "correlation_id" not in payload
        assert "request_id" not in payload
        assert "details" not in payload

    def test_rate_limit_payload_carries_retry_hint(self) -> None:
        payload = ModelRateLimitedError("slow down", retry_after_seconds=2.5).to_payload()
        assert payload["retry_after_seconds"] == 2.5


class TestErrorFromCode:
    """Reconstruction of typed errors from wire codes."""

    def test_known_code_yields_the_concrete_type(self) -> None:
        error = error_from_code(ErrorCode.MEMORY_ERROR, "store failed")
        assert isinstance(error, MemoryBackendError)

    def test_unknown_code_falls_back_to_internal(self) -> None:
        """A newer Rust build must not break error handling here."""
        error = error_from_code("brand_new_code", "unmapped")
        assert isinstance(error, InternalError)
        assert error.details["unknown_code"] == "brand_new_code"

    def test_unknown_code_preserves_the_original_message(self) -> None:
        error = error_from_code("nope", "original message")
        assert error.message == "original message"


class TestIsRetryable:
    """Retry classification helper."""

    def test_typed_error_is_classified(self) -> None:
        assert is_retryable(ModelProviderError("x")) is True
        assert is_retryable(ConfigurationError("x")) is False

    def test_untyped_exception_is_never_retryable(self) -> None:
        """Guessing at arbitrary exceptions is forbidden."""
        assert is_retryable(RuntimeError("boom")) is False
        assert is_retryable(ValueError("boom")) is False

    def test_cancelled_error_is_not_retryable(self) -> None:
        """asyncio cancellation must not be retried implicitly."""
        import asyncio

        assert is_retryable(asyncio.CancelledError()) is False


class TestErrorCatching:
    """Errors must remain catchable by their base type."""

    def test_all_errors_share_a_base(self) -> None:
        error: OrchestratorError = ConfigurationError("x")
        with pytest.raises(OrchestratorError):
            raise error

    def test_specific_catch_still_works(self) -> None:
        with pytest.raises(TenantContextError):
            raise TenantContextError("no tenant")
