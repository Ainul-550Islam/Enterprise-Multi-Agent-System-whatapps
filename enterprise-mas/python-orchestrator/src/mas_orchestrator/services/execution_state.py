"""Authoritative execution state, read from Rust's durable store.

Spec section 16 is careful about this: Python may receive duplicate
deliveries and must decide what to do with them **by asking the
authoritative Rust execution state** — never from a local cache. This
module provides the real reader for that question and the typed error a
duplicate of an in-flight execution must produce, so "already running" is
never serialised as a normal successful empty result.

The reader is a read-only lookup against Rust's ``executions`` table in the
shared platform database. It is tenant-scoped through ``app.current_tenant``
(the table is row-level secured), and it answers ``UNKNOWN`` — never
raises — when the state cannot be determined, because an unknown state has
a defined, safe interpretation (proceed).
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar, Final

from mas_orchestrator.contracts.errors import ErrorCode, OrchestratorError
from mas_orchestrator.services.idempotency import ExecutionState

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Rust's ``executions.status`` domain values, mapped onto the idempotency
#: state machine. ``paused`` counts as in-flight: the work exists and has not
#: finished, so a duplicate delivery must not run it again.
_STATUS_TO_STATE: Final[dict[str, ExecutionState]] = {
    "pending": ExecutionState.PENDING,
    "running": ExecutionState.RUNNING,
    "paused": ExecutionState.RUNNING,
    "completed": ExecutionState.SUCCEEDED,
    "failed": ExecutionState.FAILED,
    "cancelled": ExecutionState.CANCELLED,
    "timed_out": ExecutionState.TIMED_OUT,
}

_SELECT_STATUS_SQL: Final[str] = "SELECT status FROM executions WHERE id = %s::uuid"
_SET_TENANT_SQL: Final[str] = "SELECT set_config('app.current_tenant', %s, true)"


class ExecutionInProgressError(OrchestratorError):
    """A duplicate delivery of an execution that is still running elsewhere.

    Raised in place of the old behaviour — serialising the delivery as a
    normal successful empty result — because "already running" and
    "completed" are different facts and the caller must be able to tell
    them apart. Retryable on purpose: the run in flight will eventually
    settle, and a later delivery can then observe the terminal state.

    Attributes:
        execution_id: The execution that is already in flight.
        state: The authoritative state the refusal was based on.
        invocation_key: The stable identity of the refused attempt.
    """

    default_code = ErrorCode.DUPLICATE_REQUEST

    def __init__(
        self,
        message: str,
        *,
        execution_id: str,
        state: ExecutionState,
        invocation_key: str,
    ) -> None:
        super().__init__(
            message,
            retryable=True,
            details={
                "execution_id": execution_id,
                "state": state.value,
                "invocation_key": invocation_key,
            },
        )
        self.execution_id = execution_id
        self.state = state
        self.invocation_key = invocation_key


class RustExecutionStateReader:
    """Reads the authoritative execution state from Rust's Postgres store.

    The lookup is one bounded, read-only query inside a transaction that
    binds ``app.current_tenant``, so the row-level security policy — not the
    reader — confines the read to the caller's tenant. Unknown ids, unknown
    status values and database failures all report
    :attr:`ExecutionState.UNKNOWN` rather than raising: the reader answers a
    question, and "I cannot tell" is a defined, safe answer for it.
    """

    def __init__(self, pool: Any) -> None:
        """Wraps an already-built Psycopg ``AsyncConnectionPool``."""
        self._pool = pool

    @staticmethod
    def builder(
        database_url: str,
        *,
        min_size: int = 1,
        max_size: int = 4,
    ) -> Any:
        """Builds a bounded pool without opening network connections."""
        from psycopg_pool import AsyncConnectionPool

        pool: Any = AsyncConnectionPool(
            conninfo=database_url,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"autocommit": False, "application_name": "mas-execution-state-reader"},
        )
        return pool

    async def state_of(self, execution_id: str, *, tenant_id: str) -> ExecutionState:
        """Returns the current authoritative state of *execution_id*.

        Args:
            execution_id: The execution to look up (UUID text).
            tenant_id: The authoritative tenant scope; the query is confined
                to it through the row-level security policy.

        Returns:
            The mapped :class:`ExecutionState`, or
            :attr:`ExecutionState.UNKNOWN` when the state cannot be
            determined. Never raises.
        """
        try:
            async with self._pool.connection() as connection, connection.transaction():
                await connection.execute(_SET_TENANT_SQL, (tenant_id,))
                cursor = await connection.execute(_SELECT_STATUS_SQL, (execution_id,))
                row: Sequence[Any] | None = await cursor.fetchone()
        except Exception as error:  # the reader answers, it never raises
            _LOGGER.warning(
                "execution state lookup failed; reporting unknown",
                extra={
                    "event": "idempotency.state_lookup_failed",
                    "execution_id": execution_id,
                    "error_type": type(error).__name__,
                },
            )
            return ExecutionState.UNKNOWN
        if row is None:
            return ExecutionState.UNKNOWN
        return _STATUS_TO_STATE.get(str(row[0]), ExecutionState.UNKNOWN)


class StateReportError(RuntimeError):
    """A terminal transition could not be delivered to Rust's ledger.

    The reporting protocol is a delivery obligation, not a best effort: a
    reporter that cannot write the fact must raise so the failure surfaces
    instead of being logged away. Anything that raises this is a real
    operational problem — a missing execution, a conflicting terminal fact,
    or an unreachable database.
    """


class RustStateTransitionReporter:
    """Durable terminal-transition reporter for Rust's ``executions`` ledger.

    The reader above proves what Rust believes; this reporter writes the
    terminal fact back through the same channel the domain uses — the
    ``executions`` row — so a settled run is visible to Rust's state machine
    instead of only in a log line. The write mirrors the domain's own settle
    shape (``status``, ``error``, ``finished_at``, a ``version`` bump) and is
    confined by the row-level security policy exactly like the reader: the
    caller's authoritative ``tenant_id`` is bound per report with
    ``set_config('app.current_tenant', ...)``, so no report can cross a
    tenant boundary even if a caller passes the wrong execution id.

    Rust still owns the canonical transition. The reporter only asserts the
    terminal fact, and only over a non-terminal row (``status NOT IN
    ('completed', 'failed', 'cancelled')``): whoever settles first wins, a
    redelivery of the same terminal fact is idempotent, and a conflicting
    terminal fact raises rather than being masked.

    The termination vocabulary of the reporting protocol maps onto the
    ledger's status vocabulary as: ``completed`` → ``completed``,
    ``cancelled`` → ``cancelled``, everything else (``failed``, ``deadline``,
    ``step_limit``, ``tool_call_limit``, ``budget_exhausted``, …) →
    ``failed``. The original termination and the error code are preserved in
    the ``error`` record so no information is lost in the mapping. The
    reported ``duration_ms`` is acknowledged but not stored: the ledger's
    duration is derived from ``started_at``/``finished_at``.
    """

    #: The terminal statuses the ``executions`` ledger recognizes. A report
    #: never overwrites a row already in one of these.
    _TERMINAL = ("completed", "failed", "cancelled")

    #: Protocol terminations → ledger status. Unlisted terminations are
    #: failures; the mapping is total by that default.
    _TERMINATION_TO_STATUS: ClassVar[dict[str, str]] = {
        "completed": "completed",
        "cancelled": "cancelled",
    }

    _SET_TENANT_SQL = "SELECT set_config('app.current_tenant', %s, true)"
    _REPORT_SQL = (
        "UPDATE executions SET status = %s, error = COALESCE(%s::jsonb, error), "
        "finished_at = COALESCE(finished_at, now()), version = version + 1 "
        "WHERE id = %s::uuid AND status NOT IN ('completed', 'failed', 'cancelled')"
    )
    _SELECT_STATUS_SQL = "SELECT status FROM executions WHERE id = %s::uuid"

    def __init__(self, pool: Any) -> None:
        """Wraps an already-built Psycopg ``AsyncConnectionPool``."""
        self._pool = pool

    @staticmethod
    def builder(database_url: str) -> Any:
        """Pool builder with the same connection policy as the reader."""
        return RustExecutionStateReader.builder(database_url)

    async def report(
        self,
        execution_id: str,
        *,
        termination: str,
        duration_ms: int,
        error_code: str | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """Reports one terminal outcome to Rust's ledger.

        Args:
            execution_id: Canonical execution identifier (a UUID).
            termination: Terminal outcome — ``completed``, ``cancelled``,
                ``failed``, or a limit/deadline termination (mapped to
                ``failed``; the original value is preserved in the error
                record).
            duration_ms: Observed duration. The ledger derives its duration
                from timestamps, so this is accepted and not stored.
            error_code: Classified failure code for non-completed outcomes.
            tenant_id: Authoritative tenant for the row-level security
                binding. A durable report without it cannot be delivered.

        Raises:
            StateReportError: The fact could not be delivered — no tenant, an
                unknown execution, a conflicting terminal fact, or a database
                failure. Never swallowed: a lost report must surface.
        """
        if not tenant_id:
            raise StateReportError(
                "a durable state report needs the authoritative tenant; "
                "refusing to write an unscoped transition"
            )
        status = self._TERMINATION_TO_STATUS.get(termination, "failed")
        error: str | None = None
        if status != "completed":
            record = {
                "code": error_code,
                "termination": termination,
                "duration_ms": duration_ms,
            }
            error = json.dumps({k: v for k, v in record.items() if v is not None})
        try:
            async with self._pool.connection() as conn, conn.transaction():
                await conn.execute(self._SET_TENANT_SQL, (tenant_id,))
                result = await conn.execute(self._REPORT_SQL, (status, error, execution_id))
                if getattr(result, "rowcount", 0) == 1:
                    return
                current = await self._current_status(conn, execution_id)
        except StateReportError:
            raise
        except Exception as exc:  # a report that cannot deliver must raise
            raise StateReportError(
                f"could not report terminal state of execution {execution_id!r}: {exc}"
            ) from exc
        if current is None:
            raise StateReportError(
                f"execution {execution_id!r} is unknown in tenant {tenant_id!r}; "
                "the terminal report cannot land on a missing row"
            )
        if current == status:
            # Redelivery of a fact the ledger already holds. Delivered.
            return
        raise StateReportError(
            f"execution {execution_id!r} already settled as {current!r}; "
            f"refusing to overwrite with the conflicting fact {status!r}"
        )

    @staticmethod
    async def _current_status(conn: Any, execution_id: str) -> str | None:
        cur = await conn.execute(RustStateTransitionReporter._SELECT_STATUS_SQL, (execution_id,))
        row = await cur.fetchone()
        return None if row is None else str(row[0])
