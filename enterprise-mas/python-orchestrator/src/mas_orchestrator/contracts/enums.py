"""Shared contract enumerations.

These mirror the Rust ``string_enum!`` definitions in
``crates/common/src/enums.rs``, whose docstring states the rule that makes
copying them necessary rather than redundant:

    The string values are part of the platform contract (database rows,
    events, API payloads) and must never be renamed once shipped.

So the Python side does not invent its own vocabulary. Every value here is the
canonical Rust wire string, and :meth:`from_wire` accepts *only* that string —
an unrecognised value raises rather than being coerced to a default, because a
coerced status is how two systems come to disagree about what happened.

Where Rust defines ``is_terminal``, the Python side defines
:attr:`~ContractEnum.is_terminal` with the same membership, so a terminal state
means the same thing in both languages.
"""

from __future__ import annotations

from enum import Enum

from mas_orchestrator.contracts.errors import ProtocolError


class ContractEnum(Enum):
    """A contract enumeration with a stable wire string.

    Subclasses declare variants whose values are the canonical Rust strings.
    """

    @property
    def wire(self) -> str:
        """The canonical wire value.

        Defined as a property rather than aliasing ``.value`` so callers read
        the same way at every call site, and so a subclass cannot accidentally
        change the meaning of the wire value by overriding an attribute.
        """
        return str(self.value)

    @property
    def is_terminal(self) -> bool:
        """Whether the state is terminal.

        The base implementation reports ``False``. Subclasses that model a
        lifecycle override it with the same membership Rust uses.
        """
        return False

    def __str__(self) -> str:
        return self.wire

    @classmethod
    def from_wire(cls, raw: str) -> ContractEnum:
        """Parses a wire string.

        Raises:
            ProtocolError: If the string is not a canonical value. There is no
                fallback: an unknown status means the two sides disagree, and
                continuing with a guess is worse than stopping.
        """
        try:
            return cls(raw)
        except ValueError as error:
            raise ProtocolError(
                f"invalid {cls.__name__} value: {raw!r}",
                details={
                    "type": cls.__name__,
                    "value": raw,
                    "expected": [member.wire for member in cls],
                },
            ) from error

    @classmethod
    def wire_values(cls) -> tuple[str, ...]:
        """Every canonical wire value, in declaration order."""
        return tuple(member.wire for member in cls)


class ExecutionStatus(ContractEnum):
    """Lifecycle of one execution.

    Mirrors ``ExecutionStatus`` in Rust.
    """

    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Mirrors Rust's ``is_terminal``: completed, failed or cancelled."""
        return self in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }


class ExecutionStepStatus(ContractEnum):
    """Status of a single runtime step inside an execution."""

    PENDING = "pending"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Mirrors Rust: completed, failed, skipped or cancelled."""
        return self in {
            ExecutionStepStatus.COMPLETED,
            ExecutionStepStatus.FAILED,
            ExecutionStepStatus.SKIPPED,
            ExecutionStepStatus.CANCELLED,
        }


class TaskStatus(ContractEnum):
    """Processing status of a task."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DEAD_LETTERED = "dead_lettered"

    @property
    def is_terminal(self) -> bool:
        """Mirrors Rust: completed, cancelled or dead-lettered.

        ``failed`` is deliberately *not* terminal here, matching Rust: a failed
        task may be retried, so treating it as terminal would suppress retries.
        """
        return self in {
            TaskStatus.COMPLETED,
            TaskStatus.CANCELLED,
            TaskStatus.DEAD_LETTERED,
        }


class TaskPriority(ContractEnum):
    """Scheduling priority of a task."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class AgentStatus(ContractEnum):
    """Lifecycle status of an agent."""

    DRAFT = "draft"
    ACTIVE = "active"
    DISABLED = "disabled"
    ARCHIVED = "archived"

    @property
    def is_operational(self) -> bool:
        """Whether the agent may be selected to run."""
        return self is AgentStatus.ACTIVE


class DeploymentStatus(ContractEnum):
    """Deployment state of a published agent version."""

    NOT_DEPLOYED = "not_deployed"
    DEPLOYING = "deploying"
    DEPLOYED = "deployed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class ToolStatus(ContractEnum):
    """Publication lifecycle of a tool."""

    DRAFT = "draft"
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    DISABLED = "disabled"


class TenantStatus(ContractEnum):
    """Lifecycle status of a tenant."""

    PROVISIONING = "provisioning"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    ARCHIVED = "archived"

    @property
    def is_operational(self) -> bool:
        """Whether the tenant may execute workloads."""
        return self is TenantStatus.ACTIVE


class WorkflowStatus(ContractEnum):
    """Lifecycle states of a workflow."""

    DRAFT = "draft"
    VALIDATING = "validating"
    PUBLISHED = "published"
    DISABLED = "disabled"
    ARCHIVED = "archived"

    @property
    def is_publishable(self) -> bool:
        """Whether the workflow may be executed."""
        return self is WorkflowStatus.PUBLISHED


class UserStatus(ContractEnum):
    """Lifecycle status of a user account."""

    PENDING_ACTIVATION = "pending_activation"
    ACTIVE = "active"
    DISABLED = "disabled"
    LOCKED = "locked"


class ConnectorStatus(ContractEnum):
    """Operational status of an external-system connector."""

    PENDING_VERIFICATION = "pending_verification"
    ACTIVE = "active"
    ERROR = "error"
    DISABLED = "disabled"


class ApprovalStatus(ContractEnum):
    """Whether a step requires human approval before it runs.

    Approval is owned by Rust; this enumeration exists so Python can represent
    "waiting" without inventing its own approval mechanism.
    """

    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"

    @property
    def is_decided(self) -> bool:
        """Whether a decision has been reached."""
        return self in {
            ApprovalStatus.APPROVED,
            ApprovalStatus.REJECTED,
            ApprovalStatus.EXPIRED,
        }
