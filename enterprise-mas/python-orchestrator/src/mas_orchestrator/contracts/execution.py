"""Execution request/result/event contract models.

These are the Python-side representations of what Rust owns. They are
*representations*, not the authority: constructing one of these does not change
any execution state, and nothing here writes to Rust's tables.

The tenancy rule is enforced structurally. Every model takes its tenant and
organization from an :class:`~mas_orchestrator.context.ExecutionContext` and
there is no constructor parameter that accepts a tenant id from anywhere else —
in particular none that would let a caller-supplied header set it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mas_orchestrator.contracts.enums import (
    ApprovalStatus,
    ExecutionStatus,
    ExecutionStepStatus,
)
from mas_orchestrator.contracts.errors import InvalidRequestError


@dataclass(frozen=True, slots=True)
class ExecutionRef:
    """Identifies one execution.

    Attributes:
        tenant_id: Owning tenant, from the authoritative context.
        execution_id: Execution identifier.
        correlation_id: Correlation identifier.
    """

    tenant_id: str
    execution_id: str
    correlation_id: str

    def __post_init__(self) -> None:
        if not self.tenant_id.strip():
            raise InvalidRequestError(
                "execution reference requires a tenant", details={"field": "tenant_id"}
            )
        if not self.execution_id.strip():
            raise InvalidRequestError(
                "execution reference requires an execution id",
                details={"field": "execution_id"},
            )

    @classmethod
    def from_context(cls, context: Any) -> ExecutionRef:
        """Builds a reference from the authoritative context.

        The tenant can only come from here. There is deliberately no
        ``tenant_id`` parameter on this factory.
        """
        return cls(
            tenant_id=context.tenant_id,
            execution_id=context.execution_id,
            correlation_id=context.correlation_id,
        )


@dataclass(frozen=True, slots=True)
class ExecutionStepResult:
    """The result of one step of an execution.

    Attributes:
        step_id: Step identifier.
        status: Terminal status reached.
        output: A JSON-serialisable summary of the step's output.
        error_code: Error code, when the step failed.
        duration_ms: How long the step took.
    """

    step_id: str
    status: ExecutionStepStatus
    output: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    duration_ms: int = 0

    def __post_init__(self) -> None:
        if not self.step_id.strip():
            raise InvalidRequestError(
                "step result requires a step id", details={"field": "step_id"}
            )
        if not self.status.is_terminal:
            raise InvalidRequestError(
                "a step result must report a terminal status",
                details={"status": self.status.wire},
            )
        if self.status is ExecutionStepStatus.FAILED and not self.error_code:
            raise InvalidRequestError(
                "a failed step result must carry an error code",
                details={"field": "error_code"},
            )

    @property
    def failed(self) -> bool:
        """Whether the step failed."""
        return self.status is ExecutionStepStatus.FAILED


@dataclass(frozen=True, slots=True)
class ExecutionResultContract:
    """The reported outcome of a whole execution.

    This is what Python hands back to Rust. Rust decides the canonical
    transition; this carries the evidence.

    Attributes:
        status: The status Python believes was reached.
        steps: Step results, in execution order.
        content: The final answer, when the execution produced one.
        error_code: Error code, when it failed.
        usage: Token usage, when measured.
        duration_ms: Wall-clock duration.
    """

    status: ExecutionStatus
    content: str = ""
    steps: tuple[ExecutionStepResult, ...] = ()
    error_code: str | None = None
    usage: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0

    def __post_init__(self) -> None:
        if not self.status.is_terminal:
            raise InvalidRequestError(
                "an execution result must report a terminal status",
                details={"status": self.status.wire},
            )
        if self.status is ExecutionStatus.FAILED and not self.error_code:
            raise InvalidRequestError(
                "a failed execution result must carry an error code",
                details={"field": "error_code"},
            )

    @property
    def succeeded(self) -> bool:
        """Whether the execution completed."""
        return self.status is ExecutionStatus.COMPLETED

    @property
    def failed_steps(self) -> tuple[ExecutionStepResult, ...]:
        """The steps that failed, in order."""
        return tuple(step for step in self.steps if step.failed)


@dataclass(frozen=True, slots=True)
class ExecutionRequestContract:
    """What Rust asked Python to run.

    Attributes:
        execution_id: Execution identifier.
        task_id: Task identifier, when the work is task-scoped.
        task_attempt_id: Attempt identifier, when known.
        agent_id: Agent to run.
        input: The input payload. Untrusted until guardrails have run.
        approval: Whether the step is gated on approval.
        deadline_epoch_ms: When the work must stop.
    """

    execution_id: str
    agent_id: str
    input: dict[str, Any] = field(default_factory=dict)
    task_id: str | None = None
    task_attempt_id: str | None = None
    approval: ApprovalStatus = ApprovalStatus.NOT_REQUIRED
    deadline_epoch_ms: int | None = None

    def __post_init__(self) -> None:
        if not self.execution_id.strip():
            raise InvalidRequestError(
                "execution request requires an execution id",
                details={"field": "execution_id"},
            )
        if not self.agent_id.strip():
            raise InvalidRequestError(
                "execution request requires an agent id",
                details={"field": "agent_id"},
            )
        if self.approval is ApprovalStatus.PENDING:
            raise InvalidRequestError(
                "an execution that is still awaiting approval must not be run",
                details={"field": "approval"},
            )
