"""Unit tests for the execution context.

The context is the tenancy boundary, so these tests focus on what must be
rejected rather than on what is accepted.
"""

from __future__ import annotations

import dataclasses
import time

import pytest

from mas_orchestrator.context.contextvars import (
    current_execution_context,
    execution_context_scope,
    maybe_current_execution_context,
    unbind_execution_context,
)
from mas_orchestrator.context.execution_context import (
    MANDATORY_IDENTITY_FIELDS,
    ExecutionContext,
    build_execution_context,
)
from mas_orchestrator.contracts.errors import (
    InternalError,
    InvalidRequestError,
    TenantContextError,
)
from mas_orchestrator.contracts.protocol_version import ProtocolVersion
from mas_orchestrator.security.identity import (
    ANONYMOUS_SUBJECT,
    ServiceIdentity,
    ServiceScope,
)

# Base keyword arguments that satisfy every mandatory field.
VALID_KWARGS: dict[str, object] = {
    "tenant_id": "tenant-1",
    "organization_id": "org-1",
    "execution_id": "exec-1",
    "agent_id": "agent-1",
    "correlation_id": "corr-1",
    "request_id": "req-1",
    "protocol_version": "1.0.0",
}


# Built synchronously: the context needs only an identity value object, and
# spinning up an event loop per test leaks loop state into pytest-asyncio.
CALLER = ServiceIdentity(
    subject=ANONYMOUS_SUBJECT,
    issuer="anonymous",
    scopes=frozenset({ServiceScope.EXECUTE_AGENT}),
)


def _build(**overrides: object) -> ExecutionContext:
    """Builds a context using a fixed TEST-only caller identity."""
    kwargs = dict(VALID_KWARGS)
    kwargs["caller"] = CALLER
    kwargs.update(overrides)
    return build_execution_context(**kwargs)  # type: ignore[arg-type]


class TestMandatoryIdentity:
    """Missing or inconsistent identity must be refused."""

    @pytest.mark.parametrize("field", MANDATORY_IDENTITY_FIELDS)
    def test_missing_mandatory_field_is_refused(self, field: str) -> None:
        with pytest.raises(TenantContextError) as excinfo:
            _build(**{field: ""})
        assert field in excinfo.value.details["missing_fields"]

    @pytest.mark.parametrize("field", MANDATORY_IDENTITY_FIELDS)
    def test_whitespace_only_field_is_refused(self, field: str) -> None:
        with pytest.raises(TenantContextError):
            _build(**{field: "   "})

    def test_tenant_equal_to_organization_is_refused(self) -> None:
        """Indicates a caller that passed the same identifier twice."""
        with pytest.raises(TenantContextError) as excinfo:
            _build(tenant_id="same", organization_id="same")
        assert excinfo.value.details["reason"] == "duplicate_scope_identifier"

    def test_optional_fields_may_be_absent(self) -> None:
        context = _build()
        assert context.task_id is None
        assert context.task_attempt_id is None
        assert context.agent_version_id is None
        assert context.project_id is None


class TestValidConstruction:
    """A well-formed context is immutable and usable."""

    def test_protocol_version_is_negotiated(self) -> None:
        assert _build().protocol_version == ProtocolVersion(1, 0, 0)

    def test_optional_fields_are_retained(self) -> None:
        context = _build(
            project_id="proj-1",
            task_id="task-1",
            task_attempt_id="attempt-2",
            agent_version_id="ver-3",
        )
        assert context.project_id == "proj-1"
        assert context.task_id == "task-1"
        assert context.task_attempt_id == "attempt-2"
        assert context.agent_version_id == "ver-3"

    def test_context_is_frozen(self) -> None:
        context = _build()
        with pytest.raises(dataclasses.FrozenInstanceError):
            context.tenant_id = "tenant-2"  # type: ignore[misc]

    def test_with_deadline_returns_a_copy(self) -> None:
        original = _build()
        copy = original.with_deadline(1_700_000_000_000)
        assert original.deadline_epoch_ms is None
        assert copy.deadline_epoch_ms == 1_700_000_000_000

    def test_expired_deadline_at_intake_is_rejected(self) -> None:
        past = int(time.time() * 1000) - 60_000
        with pytest.raises(InvalidRequestError):
            _build(deadline_epoch_ms=past)


class TestDeadlineHelpers:
    """Deadline arithmetic."""

    def test_remaining_seconds_when_unbounded(self) -> None:
        assert _build().remaining_seconds() is None

    def test_remaining_seconds_is_computed(self) -> None:
        now = 4_000_000_000_000
        context = _build(deadline_epoch_ms=now + 5_000)
        assert context.remaining_seconds(now_epoch_ms=now) == 5.0

    def test_past_deadline_yields_zero_not_negative(self) -> None:
        now = 4_000_000_000_000
        context = _build(deadline_epoch_ms=now - 5_000)
        assert context.remaining_seconds(now_epoch_ms=now) == 0.0

    def test_is_expired(self) -> None:
        now = 4_000_000_000_000
        assert _build(deadline_epoch_ms=now - 1).is_expired(now_epoch_ms=now) is True
        assert _build(deadline_epoch_ms=now + 1).is_expired(now_epoch_ms=now) is False
        assert _build().is_expired() is False


class TestInvocationIdentity:
    """Idempotency key derivation."""

    def test_includes_attempt_so_retries_are_distinct(self) -> None:
        first = _build(task_id="task-1", task_attempt_id="attempt-1")
        second = _build(task_id="task-1", task_attempt_id="attempt-2")
        assert first.invocation_identity != second.invocation_identity

    def test_duplicate_delivery_collapses_to_one_key(self) -> None:
        first = _build(task_id="task-1", task_attempt_id="attempt-1")
        second = _build(task_id="task-1", task_attempt_id="attempt-1")
        assert first.invocation_identity == second.invocation_identity

    def test_differs_across_agents(self) -> None:
        assert _build(agent_id="a").invocation_identity != _build(agent_id="b").invocation_identity


class TestLogFields:
    """Structured logging projection."""

    def test_identity_fields_are_present(self) -> None:
        fields = _build().to_log_fields()
        assert fields["tenant_id"] == "tenant-1"
        assert fields["execution_id"] == "exec-1"
        assert fields["correlation_id"] == "corr-1"

    def test_absent_optionals_are_omitted(self) -> None:
        fields = _build().to_log_fields()
        assert "task_id" not in fields
        assert "project_id" not in fields

    def test_attributes_are_not_dumped(self) -> None:
        """Caller-defined attributes must never be bulk-logged."""
        context = _build(attributes={"big": "payload"})
        assert "big" not in context.to_log_fields()


class TestContextVarPropagation:
    """Async-safe binding."""

    def test_unbound_context_raises(self) -> None:
        unbind_execution_context()
        with pytest.raises(InternalError):
            current_execution_context()

    def test_maybe_returns_none_when_unbound(self) -> None:
        unbind_execution_context()
        assert maybe_current_execution_context() is None

    def test_scope_binds_then_restores(self) -> None:
        unbind_execution_context()
        context = _build()
        with execution_context_scope(context) as bound:
            assert bound is context
            assert current_execution_context() is context
        assert maybe_current_execution_context() is None

    def test_scope_restores_an_outer_context(self) -> None:
        outer = _build(execution_id="outer")
        inner = _build(execution_id="inner")
        with execution_context_scope(outer):
            with execution_context_scope(inner):
                assert current_execution_context().execution_id == "inner"
            assert current_execution_context().execution_id == "outer"


class TestScopeHelpers:
    """Scope checks on the caller identity."""

    def test_identity_reports_scopes(self) -> None:
        assert CALLER.has_scope(ServiceScope.EXECUTE_AGENT) is True

    def test_identity_str_omits_scopes(self) -> None:
        rendered = str(CALLER)
        assert "anonymous" in rendered
        assert "execute_agent" not in rendered
