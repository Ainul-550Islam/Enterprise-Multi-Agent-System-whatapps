"""Tests for the wire contract models.

These types are what crosses the Rust/Python boundary, so the properties that
matter are the ones that stop a malformed or mis-attributed message from being
accepted at all:

* identity fields are mandatory and non-empty;
* a terminal status must actually be terminal;
* tenancy is derived from the context, never taken as an argument.
"""

from __future__ import annotations

import time

import pytest

from mas_orchestrator.contracts.agent import (
    AgentCapabilities,
    AgentConfigContract,
    AgentVersionRef,
)
from mas_orchestrator.contracts.enums import (
    AgentStatus,
    ContractEnum,
    DeploymentStatus,
    ExecutionStatus,
    ExecutionStepStatus,
    TaskPriority,
    TaskStatus,
    TenantStatus,
    ToolStatus,
    WorkflowStatus,
)
from mas_orchestrator.contracts.errors import InvalidRequestError, ProtocolError
from mas_orchestrator.contracts.execution import (
    ExecutionRef,
    ExecutionRequestContract,
    ExecutionResultContract,
    ExecutionStepResult,
)
from mas_orchestrator.contracts.messages import (
    HEADER_CORRELATION_ID,
    HEADER_EXECUTION_ID,
    HEADER_PROTOCOL_VERSION,
    AiMessage,
    Envelope,
    MessageKind,
)
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.contracts.task import (
    TaskAttemptRef,
    TaskAttemptResult,
    TaskContract,
    TaskRef,
)

TENANT = "tenant-alpha"


class _FakeContext:
    tenant_id = TENANT
    execution_id = "exec-1"
    correlation_id = "corr-1"


class TestContractEnum:
    def test_every_member_round_trips(self) -> None:
        for enum_cls in (
            ExecutionStatus,
            ExecutionStepStatus,
            TaskStatus,
            TaskPriority,
            AgentStatus,
            DeploymentStatus,
            ToolStatus,
            TenantStatus,
            WorkflowStatus,
        ):
            for member in enum_cls:
                assert enum_cls.from_wire(member.wire) is member

    def test_unknown_wire_value_is_refused(self) -> None:
        with pytest.raises((ProtocolError, ValueError)):
            ExecutionStatus.from_wire("not-a-status")

    def test_every_enum_is_a_contract_enum(self) -> None:
        assert issubclass(ExecutionStatus, ContractEnum)

    def test_terminal_statuses_are_flagged(self) -> None:
        assert ExecutionStatus.COMPLETED.is_terminal
        assert not ExecutionStatus.RUNNING.is_terminal


class TestExecutionRef:
    def test_tenant_is_required(self) -> None:
        with pytest.raises(InvalidRequestError):
            ExecutionRef(tenant_id=" ", execution_id="e", correlation_id="c")

    def test_execution_id_is_required(self) -> None:
        with pytest.raises(InvalidRequestError):
            ExecutionRef(tenant_id=TENANT, execution_id=" ", correlation_id="c")

    def test_from_context_derives_tenancy(self) -> None:
        # There is deliberately no tenant parameter on the factory: a reference
        # built any other way could name a tenant the caller invented.
        ref = ExecutionRef.from_context(_FakeContext())
        assert ref.tenant_id == TENANT
        assert ref.execution_id == "exec-1"


class TestExecutionStepResult:
    def test_non_terminal_status_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError, match="terminal"):
            ExecutionStepResult(step_id="s1", status=ExecutionStepStatus.RUNNING)

    def test_failure_requires_an_error_code(self) -> None:
        with pytest.raises(InvalidRequestError, match="error code"):
            ExecutionStepResult(step_id="s1", status=ExecutionStepStatus.FAILED)

    def test_failed_flag(self) -> None:
        step = ExecutionStepResult(
            step_id="s1", status=ExecutionStepStatus.FAILED, error_code="tool_failed"
        )
        assert step.failed

    def test_step_id_is_required(self) -> None:
        with pytest.raises(InvalidRequestError):
            ExecutionStepResult(step_id=" ", status=ExecutionStepStatus.COMPLETED)


class TestExecutionResultContract:
    def test_builds_with_a_terminal_status(self) -> None:
        contract = ExecutionResultContract(status=ExecutionStatus.COMPLETED)
        assert contract.succeeded

    def test_non_terminal_status_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError, match="terminal"):
            ExecutionResultContract(status=ExecutionStatus.RUNNING)

    def test_failure_requires_an_error_code(self) -> None:
        # A failure without a code is a failure nobody can act on.
        with pytest.raises(InvalidRequestError, match="error code"):
            ExecutionResultContract(status=ExecutionStatus.FAILED)

    def test_failed_steps_are_reported(self) -> None:
        failed = ExecutionStepResult(
            step_id="s1", status=ExecutionStepStatus.FAILED, error_code="tool_failed"
        )
        contract = ExecutionResultContract(
            status=ExecutionStatus.FAILED, steps=(failed,), error_code="tool_failed"
        )
        assert contract.failed_steps == (failed,)


class TestExecutionRequestContract:
    def test_identity_is_required(self) -> None:
        with pytest.raises(InvalidRequestError):
            ExecutionRequestContract(execution_id=" ", agent_id="a")

    def test_pending_approval_refuses_to_run(self) -> None:
        from mas_orchestrator.contracts.enums import ApprovalStatus

        # An execution awaiting approval must not reach the runtime at all.
        with pytest.raises(InvalidRequestError, match="approval"):
            ExecutionRequestContract(
                execution_id="e",
                agent_id="a",
                approval=ApprovalStatus.PENDING,
            )


def _task_ref() -> TaskRef:
    return TaskRef(tenant_id=TENANT, task_id="t1", execution_id="exec-1")


class TestTaskContracts:
    def test_task_ref_requires_identity(self) -> None:
        with pytest.raises(InvalidRequestError, match="task reference requires"):
            TaskRef(tenant_id=" ", task_id="t", execution_id="e")

    def test_attempt_number_starts_at_one(self) -> None:
        with pytest.raises(InvalidRequestError, match="start at 1"):
            TaskAttemptRef(task=_task_ref(), attempt_number=0)

    def test_attempt_id_is_derived(self) -> None:
        # Derived rather than stored, so two views of the same attempt agree
        # without having been handed the same id.
        assert TaskAttemptRef(task=_task_ref(), attempt_number=2).attempt_id == "t1:2"

    def test_attempt_result_requires_a_terminal_status(self) -> None:
        with pytest.raises(InvalidRequestError, match="terminal"):
            TaskAttemptResult(
                ref=TaskAttemptRef(task=_task_ref(), attempt_number=1),
                status=TaskStatus.RUNNING,
            )

    def test_failed_attempt_may_be_retryable(self) -> None:
        # ``failed`` is not terminal in Rust's model, so it is accepted here —
        # a failed attempt may be retried.
        result = TaskAttemptResult(
            ref=TaskAttemptRef(task=_task_ref(), attempt_number=1),
            status=TaskStatus.FAILED,
            error_code="tool_failed",
            retryable=True,
        )
        assert result.retryable
        assert not result.exhausted

    def test_only_a_failure_may_be_retryable(self) -> None:
        with pytest.raises(InvalidRequestError, match="retryable"):
            TaskAttemptResult(
                ref=TaskAttemptRef(task=_task_ref(), attempt_number=1),
                status=TaskStatus.COMPLETED,
                retryable=True,
            )

    def test_task_contract_requires_an_agent(self) -> None:
        with pytest.raises(InvalidRequestError, match="agent id"):
            TaskContract(ref=_task_ref(), status=TaskStatus.PENDING, agent_id=" ")

    def test_task_runnable_reflects_status(self) -> None:
        pending = TaskContract(ref=_task_ref(), status=TaskStatus.PENDING, agent_id="a")
        done = TaskContract(ref=_task_ref(), status=TaskStatus.COMPLETED, agent_id="a")
        assert pending.runnable
        assert not done.runnable


class TestAgentContracts:
    def test_version_ref_requires_both_ids(self) -> None:
        # An agent is addressed by version, never by name: a name can be
        # reassigned and a version cannot.
        with pytest.raises(InvalidRequestError, match="must not be empty"):
            AgentVersionRef(agent_id="a", agent_version_id=" ")

    def test_version_ref_renders(self) -> None:
        assert str(AgentVersionRef(agent_id="a", agent_version_id="v1")) == "a@v1"

    def test_capabilities_default_to_safe(self) -> None:
        # A capability is granted, never assumed.
        capabilities = AgentCapabilities()
        assert not capabilities.may_use_tools
        assert not capabilities.may_delegate

    def test_supervisor_may_not_also_use_tools(self) -> None:
        # A supervisor that also acts makes its own work unaccountable.
        with pytest.raises(InvalidRequestError, match="delegate and call tools"):
            AgentCapabilities(may_delegate=True, may_use_tools=True)

    def test_tools_require_the_capability(self) -> None:
        with pytest.raises(InvalidRequestError, match="may not use tools"):
            AgentCapabilities(allowed_tools=frozenset({"builtin.http"}))

    def test_permits_tool_checks_both(self) -> None:
        capabilities = AgentCapabilities(
            may_use_tools=True, allowed_tools=frozenset({"builtin.http"})
        )
        assert capabilities.permits_tool("builtin.http")
        assert not capabilities.permits_tool("builtin.email")

    def test_agent_config_requires_a_tenant(self) -> None:
        with pytest.raises(InvalidRequestError, match="tenant"):
            AgentConfigContract(
                ref=AgentVersionRef(agent_id="a", agent_version_id="v1"),
                tenant_id=" ",
                name="n",
                slug="n",
                kind="standard",
            )

    def test_unknown_kind_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError, match="unknown agent kind"):
            AgentConfigContract(
                ref=AgentVersionRef(agent_id="a", agent_version_id="v1"),
                tenant_id=TENANT,
                name="n",
                slug="n",
                kind="oracle",
            )

    def test_selectable_requires_active_and_deployed(self) -> None:
        # Both gates matter: active-but-undeployed is not runnable, and neither
        # is deployed-but-disabled.
        def build(**overrides: object) -> AgentConfigContract:
            defaults: dict[str, object] = {
                "ref": AgentVersionRef(agent_id="a", agent_version_id="v1"),
                "tenant_id": TENANT,
                "name": "n",
                "slug": "n",
                "kind": "standard",
            }
            defaults.update(overrides)
            return AgentConfigContract(**defaults)  # type: ignore[arg-type]

        assert build().is_selectable
        assert not build(status=AgentStatus.DISABLED).is_selectable
        assert not build(deployment=DeploymentStatus.NOT_DEPLOYED).is_selectable

    def test_supervisor_flag(self) -> None:
        config = AgentConfigContract(
            ref=AgentVersionRef(agent_id="a", agent_version_id="v1"),
            tenant_id=TENANT,
            name="n",
            slug="n",
            kind="supervisor",
        )
        assert config.is_supervisor


class TestEnvelope:
    def test_occurred_time_is_stamped(self) -> None:
        assert Envelope(kind=MessageKind.EXECUTION_REQUEST).occurred_epoch_ms > 0

    def test_headers_round_trip(self) -> None:
        envelope = Envelope(
            kind=MessageKind.EXECUTION_REQUEST,
            request_id="r",
            correlation_id="c",
            execution_id="e",
            organization_id="o",
        )
        headers = envelope.to_headers()
        assert headers[HEADER_CORRELATION_ID] == "c"
        assert headers[HEADER_EXECUTION_ID] == "e"

    def test_from_headers_negotiates_the_version(self) -> None:
        envelope = Envelope.from_headers(
            MessageKind.EXECUTION_REQUEST, {HEADER_PROTOCOL_VERSION: str(CURRENT_PROTOCOL_VERSION)}
        )
        assert envelope.protocol_version == CURRENT_PROTOCOL_VERSION

    def test_missing_version_is_refused(self) -> None:
        # Negotiated, not read: an absent version is a protocol error rather
        # than a default.
        with pytest.raises(ProtocolError):
            Envelope.from_headers(MessageKind.EXECUTION_REQUEST, {})

    def test_unsupported_version_is_refused(self) -> None:
        with pytest.raises(ProtocolError):
            Envelope.from_headers(
                MessageKind.EXECUTION_REQUEST, {HEADER_PROTOCOL_VERSION: "99.0.0"}
            )

    def test_stamp_is_recent(self) -> None:
        envelope = Envelope(kind=MessageKind.EXECUTION_REQUEST)
        assert abs(envelope.occurred_epoch_ms - int(time.time() * 1000)) < 5000


class TestAiMessage:
    def test_requires_an_execution_id(self) -> None:
        message = AiMessage(envelope=Envelope(kind=MessageKind.EXECUTION_REQUEST))
        with pytest.raises(ProtocolError, match="execution id"):
            message.require_execution_id()

    def test_returns_the_execution_id(self) -> None:
        message = AiMessage(envelope=Envelope(kind=MessageKind.EXECUTION_REQUEST, execution_id="e"))
        assert message.require_execution_id() == "e"

    def test_health_check_needs_no_execution(self) -> None:
        # Correlation fields are optional at the envelope level precisely
        # because a health check carries none of them.
        message = AiMessage(envelope=Envelope(kind=MessageKind.HEALTH_CHECK))
        assert message.envelope.execution_id == ""
