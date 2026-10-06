"""Usage and cost service.

Spec §6 puts usage metadata in the execution path, and spec §17 asks for token
and cost telemetry. Both are handled here, and the thing that makes the module
worth having is its handling of *incompleteness*.

A cost total is only meaningful if every call in it was measured and priced.
When some calls were not, the total is a lower bound — and reporting a lower
bound as though it were final is how a cost incident goes unnoticed until the
invoice arrives. So :class:`UsageReport` carries
:attr:`~UsageReport.is_complete`, and the service refuses to describe an
incomplete total as anything else.

Unpriced calls are never counted as free. A model with no configured price
contributes tokens but no money, and the distinction is recorded rather than
smoothed over.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import QuotaExceededError

if TYPE_CHECKING:
    from mas_orchestrator.events.emitter import EventEmitter
    from mas_orchestrator.models.cost import CostLedger, CostTotals

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class UsageReport:
    """Usage and cost for one execution.

    Attributes:
        totals: The ledger's totals.
        execution_id: Execution the usage belongs to.
        tenant_id: Owning tenant.
    """

    totals: CostTotals
    execution_id: str = ""
    tenant_id: str = ""

    @property
    def is_complete(self) -> bool:
        """Whether every call was measured and priced.

        ``False`` means the money figure is a lower bound.
        """
        return self.totals.is_complete

    @property
    def total_cost(self) -> float:
        """The money total. A lower bound unless :attr:`is_complete`."""
        return self.totals.total_cost

    @property
    def total_tokens(self) -> int:
        """Reported tokens across every measured call."""
        return self.totals.total_tokens

    def describe(self) -> str:
        """A one-line summary that states whether the total is complete.

        Used in diagnostics. It says "at least" when the figure is a lower
        bound, because a number without that qualifier reads as final.
        """
        qualifier = "" if self.is_complete else "at least "
        return (
            f"{qualifier}{self.total_cost:.6f} {self.totals.currency}, "
            f"{self.total_tokens} tokens, {self.totals.call_count} calls"
        )


class UsageService:
    """Records and reports execution usage."""

    def __init__(
        self,
        *,
        emitter: EventEmitter | None = None,
    ) -> None:
        self._emitter = emitter
        self._ledgers: dict[str, CostLedger] = {}

    def ledger_for(self, execution_id: str, ledger: CostLedger) -> CostLedger:
        """Binds a ledger to an execution.

        Raises:
            ValueError: If one is already bound. Replacing a ledger mid-run
                would silently discard the usage recorded so far, which is
                exactly the kind of undercount this module exists to prevent.
        """
        if execution_id in self._ledgers:
            raise ValueError(f"a ledger is already bound to {execution_id}")
        self._ledgers[execution_id] = ledger
        return ledger

    def release(self, execution_id: str) -> None:
        """Forgets a bound ledger."""
        self._ledgers.pop(execution_id, None)

    def report(self, execution_id: str, *, tenant_id: str = "") -> UsageReport:
        """Builds a usage report for an execution.

        Returns:
            A :class:`UsageReport`. An execution with no ledger reports zeroes
            rather than raising: "no usage was recorded" is a valid answer, and
            it is not the same as an error.
        """
        ledger = self._ledgers.get(execution_id)
        if ledger is None:
            from mas_orchestrator.models.cost import CostTotals

            return UsageReport(
                totals=CostTotals(),
                execution_id=execution_id,
                tenant_id=tenant_id,
            )
        return UsageReport(
            totals=ledger.totals(),
            execution_id=execution_id,
            tenant_id=tenant_id,
        )

    async def publish(
        self,
        execution_id: str,
        *,
        tenant_id: str,
        correlation_id: str,
        agent_id: str = "",
    ) -> UsageReport:
        """Reports usage and emits token and cost events.

        Emits nothing when no ledger is bound. Emitting zero for an unmeasured
        execution would be a measurement that was never taken.
        """
        report = self.report(execution_id, tenant_id=tenant_id)
        ledger = self._ledgers.get(execution_id)
        if ledger is None or self._emitter is None:
            return report

        from mas_orchestrator.events.types import model_cost, model_tokens

        totals = report.totals
        await self._emitter.emit(
            model_tokens(
                tenant_id=tenant_id,
                execution_id=execution_id,
                correlation_id=correlation_id,
                agent_id=agent_id,
                model="aggregate",
                input_tokens=totals.input_tokens,
                output_tokens=totals.output_tokens,
            )
        )
        await self._emitter.emit(
            model_cost(
                tenant_id=tenant_id,
                execution_id=execution_id,
                correlation_id=correlation_id,
                agent_id=agent_id,
                model="aggregate",
                currency=totals.currency,
                total_cost=totals.total_cost,
                is_complete=totals.is_complete,
            )
        )
        return report

    def enforce_quota(
        self,
        execution_id: str,
        *,
        max_cost: float | None = None,
        max_tokens: int | None = None,
    ) -> None:
        """Refuses an execution that has exceeded a budget.

        Raises:
            QuotaExceededError: If either ceiling is breached. A ``None``
                ceiling is not enforced rather than treated as zero, because
                "no limit" and "no usage allowed" are different settings.
        """
        report = self.report(execution_id)
        if max_cost is not None and report.total_cost > max_cost:
            raise QuotaExceededError(
                "execution exceeded its cost budget",
                details={
                    "execution_id": execution_id,
                    "cost": report.total_cost,
                    "limit": max_cost,
                    "is_complete": report.is_complete,
                },
            )
        if max_tokens is not None and report.total_tokens > max_tokens:
            raise QuotaExceededError(
                "execution exceeded its token budget",
                details={
                    "execution_id": execution_id,
                    "tokens": report.total_tokens,
                    "limit": max_tokens,
                },
            )
