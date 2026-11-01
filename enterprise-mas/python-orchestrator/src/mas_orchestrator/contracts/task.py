"""Task and task-attempt contract models.

A task is Rust's unit of work; a task attempt is one try at it. Python receives
attempts and reports results — it does not decide how many attempts there are,
or whether a failed attempt is retried. Those decisions belong to the Rust
scheduler, and duplicating them here would put Python in competition with it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mas_orchestrator.contracts.enums import TaskPriority, TaskStatus
from mas_orchestrator.contracts.errors import InvalidRequestError

#: Attempts above this are refused rather than run. A runaway retry loop is a
#: cost incident, and the boundary has to be somewhere.
MAX_ATTEMPT_NUMBER: int = 25


@dataclass(frozen=True, slots=True)
class TaskRef:
    """Identifies one task.

    Attributes:
        tenant_id: Owning tenant, from the authoritative context.
        task_id: Task identifier.
        execution_id: Execution the task belongs to.
    """

    tenant_id: str
    task_id: str
    execution_id: str

    def __post_init__(self) -> None:
        for name in ("tenant_id", "task_id", "execution_id"):
            if not str(getattr(self, name) or "").strip():
                raise InvalidRequestError(
                    f"task reference requires {name}", details={"field": name}
                )


@dataclass(frozen=True, slots=True)
class TaskAttemptRef:
    """Identifies one attempt at a task.

    Attributes:
        task: The task being attempted.
        attempt_number: Which attempt this is, starting at 1.
    """

    task: TaskRef
    attempt_number: int

    def __post_init__(self) -> None:
        if self.attempt_number < 1:
            raise InvalidRequestError(
                "attempt numbers start at 1",
                details={"attempt_number": self.attempt_number},
            )
        if self.attempt_number > MAX_ATTEMPT_NUMBER:
            raise InvalidRequestError(
                "attempt number exceeds the retry ceiling",
                details={
                    "attempt_number": self.attempt_number,
                    "limit": MAX_ATTEMPT_NUMBER,
                },
            )

    @property
    def attempt_id(self) -> str:
        """A stable identifier for this attempt.

        Derived rather than stored, so two views of the same attempt agree
        without having to have been given the same id.
        """
        return f"{self.task.task_id}:{self.attempt_number}"


@dataclass(frozen=True, slots=True)
class TaskContract:
    """The task as Rust described it.

    Attributes:
        ref: The task identity.
        status: Current status.
        priority: Scheduling priority.
        agent_id: Agent assigned to run it.
        input: Task input payload. Untrusted until guardrails have run.
        max_attempts: How many attempts Rust permits.
    """

    ref: TaskRef
    status: TaskStatus
    agent_id: str
    priority: TaskPriority = TaskPriority.NORMAL
    input: dict[str, Any] = field(default_factory=dict)
    max_attempts: int = 3

    def __post_init__(self) -> None:
        if not self.agent_id.strip():
            raise InvalidRequestError("task requires an agent id", details={"field": "agent_id"})
        if self.max_attempts < 1:
            raise InvalidRequestError(
                "a task must allow at least one attempt",
                details={"max_attempts": self.max_attempts},
            )

    @property
    def runnable(self) -> bool:
        """Whether the task is in a state Python may accept work for.

        A terminal task is refused. Note that ``failed`` is *not* terminal in
        Rust's model — a failed task may be retried — so it is accepted here.
        """
        return not self.status.is_terminal


@dataclass(frozen=True, slots=True)
class TaskAttemptResult:
    """What Python reports back about one attempt.

    Attributes:
        ref: The attempt identity.
        status: Status reached by this attempt.
        output: A JSON-serialisable summary.
        error_code: Error code, when it failed.
        retryable: Whether a further attempt could plausibly succeed.
        duration_ms: How long the attempt took.
    """

    ref: TaskAttemptRef
    status: TaskStatus
    output: dict[str, Any] = field(default_factory=dict)
    error_code: str | None = None
    retryable: bool = False
    duration_ms: int = 0

    def __post_init__(self) -> None:
        if not self.status.is_terminal and self.status is not TaskStatus.FAILED:
            raise InvalidRequestError(
                "an attempt result must report a terminal status, or a failure that Rust may retry",
                details={"status": self.status.wire},
            )
        if self.status is TaskStatus.FAILED and not self.error_code:
            raise InvalidRequestError(
                "a failed attempt result must carry an error code",
                details={"field": "error_code"},
            )
        if self.retryable and self.status is not TaskStatus.FAILED:
            raise InvalidRequestError(
                "only a failed attempt may be marked retryable",
                details={"status": self.status.wire},
            )

    @property
    def exhausted(self) -> bool:
        """Whether no further attempt is permitted for this task."""
        return self.ref.attempt_number >= MAX_ATTEMPT_NUMBER
