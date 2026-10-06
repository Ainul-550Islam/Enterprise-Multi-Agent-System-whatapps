"""Token usage and cost accounting for AI work.

Cost reporting is easy to get wrong in a way that is very hard to notice: a
missing price silently becomes zero, and a zero looks exactly like a free call
on a dashboard. This module is built so that cannot happen.

Rules:

* **No fabricated pricing.** :class:`CostLedger` refuses to price a model it
  has no configured rate for. It records the usage as *unpriced* instead of
  assigning it a cost of zero.
* **No fabricated usage.** A call whose provider reported no usage is recorded
  as *unmeasured*. It still counts as a call, but it contributes no token
  totals, because adding a guessed number would corrupt every aggregate.
* **Totals are honest about their own completeness.** :attr:`CostLedger.totals`
  reports ``priced_call_count``, ``unpriced_call_count`` and
  ``unmeasured_call_count`` alongside the money, so a consumer can see how much
  of the figure is actually backed by data.

Allocation: a ledger is per-execution. Execution-scoped accumulation is what
makes a cost figure attributable to the tenant and execution that caused it;
a process-global counter could not answer "what did this execution cost?".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.models.types import CostEstimate

if TYPE_CHECKING:
    from mas_orchestrator.models.types import ModelRef, Usage

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "CostLedger",
    "CostTotals",
    "PricingTable",
]

#: The rate keys a pricing row must supply.
REQUIRED_RATE_KEYS: Final[tuple[str, ...]] = ("input", "output")


@dataclass(frozen=True, slots=True)
class PricingTable:
    """Configured prices, keyed by model.

    Attributes:
        currency: ISO-4217 code for every rate in the table. A single currency
            avoids the need for conversion, which would introduce rates that
            are not configured data.
        rates: Model key -> ``{"input": ..., "output": ...}``, expressed per
            **one million** tokens.
    """

    currency: str = "USD"
    rates: dict[str, dict[str, float]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validates the table.

        Raises:
            ConfigurationError: If a row is missing a required rate key, if a
                rate is negative, or if the currency code is blank. Catching a
                malformed table here keeps it from surfacing as a wrong cost
                much later.
        """
        if not self.currency.strip():
            raise ConfigurationError(
                "pricing table requires a currency", details={"field": "currency"}
            )
        for model_key, row in self.rates.items():
            for key in REQUIRED_RATE_KEYS:
                if key not in row:
                    raise ConfigurationError(
                        "pricing row is missing a required rate",
                        details={"model": model_key, "missing_key": key},
                    )
            for key, value in row.items():
                if value < 0:
                    raise ConfigurationError(
                        "pricing rate must not be negative",
                        details={"model": model_key, "key": key, "value": value},
                    )

    def rate_for(self, model: ModelRef) -> dict[str, float] | None:
        """Returns the rate row for *model*, or ``None`` when unpriced."""
        return self.rates.get(model.key)

    def is_priced(self, model: ModelRef) -> bool:
        """Whether a rate is configured for *model*."""
        return model.key in self.rates

    def models(self) -> list[str]:
        """Priced model keys, sorted."""
        return sorted(self.rates)


@dataclass(frozen=True, slots=True)
class CostTotals:
    """Aggregate usage and cost for one ledger.

    Attributes:
        currency: Currency of the money fields.
        input_tokens: Sum of reported input tokens.
        output_tokens: Sum of reported output tokens.
        total_cost: Sum of priced costs.
        priced_call_count: Calls with both usage *and* pricing.
        unpriced_call_count: Calls with usage but **no** configured price.
            These contributed tokens but no money; the money total excludes
            them rather than counting them as free.
        unmeasured_call_count: Calls whose provider reported no usage at all.
    """

    currency: str = "USD"
    input_tokens: int = 0
    output_tokens: int = 0
    total_cost: float = 0.0
    priced_call_count: int = 0
    unpriced_call_count: int = 0
    unmeasured_call_count: int = 0

    @property
    def total_tokens(self) -> int:
        """Sum of reported input and output tokens."""
        return self.input_tokens + self.output_tokens

    @property
    def call_count(self) -> int:
        """Total calls recorded, regardless of measurability."""
        return self.priced_call_count + self.unpriced_call_count + self.unmeasured_call_count

    @property
    def is_complete(self) -> bool:
        """Whether every recorded call was both measured and priced.

        A ``False`` value means the money figure is a **lower bound**. Reporting
        it as final would understate cost, so consumers must check this.
        """
        return self.unpriced_call_count == 0 and self.unmeasured_call_count == 0


class CostLedger:
    """Accumulates usage and cost for one execution.

    Args:
        pricing: The configured pricing table.
        emit_unpriced_warning: Whether to log when a model has no configured
            price. Enabled by default so a missing rate is noticed in logs
            rather than discovered on an invoice.
    """

    def __init__(
        self,
        pricing: PricingTable,
        *,
        emit_unpriced_warning: bool = True,
    ) -> None:
        self._pricing = pricing
        self._emit_warning = emit_unpriced_warning
        self._input_tokens = 0
        self._output_tokens = 0
        self._total_cost = 0.0
        self._priced_calls = 0
        self._unpriced_calls = 0
        self._unmeasured_calls = 0
        self._per_model: dict[str, float] = {}

    @property
    def pricing(self) -> PricingTable:
        """The pricing table in use."""
        return self._pricing

    def record(self, model: ModelRef, usage: Usage | None) -> CostEstimate | None:
        """Records one model call.

        Args:
            model: The model that was called.
            usage: Reported usage, or ``None`` when the provider did not
                report it.

        Returns:
            The cost estimate for this call, or ``None`` when it could not be
            priced. ``None`` is returned both for unmeasured calls and for
            unpriced models — use :attr:`CostTotals.is_complete` to tell how
            much of the total is backed by data.

        Raises:
            ConfigurationError: If the model *is* priced but its rate row is
                malformed. Propagated from :class:`CostEstimate`; a corrupt
                price must fail loudly rather than be skipped.
        """
        if usage is None:
            # No measurement: count the call, but add no tokens. Inventing a
            # number here would corrupt every aggregate derived from this ledger.
            self._unmeasured_calls += 1
            _LOGGER.debug("model call reported no usage", extra={"model": model.key})
            return None

        self._input_tokens += usage.input_tokens
        self._output_tokens += usage.output_tokens

        rates = self._pricing.rate_for(model)
        if rates is None:
            self._unpriced_calls += 1
            if self._emit_warning:
                _LOGGER.warning(
                    "no pricing configured for model; cost is understated",
                    extra={"model": model.key},
                )
            return None

        estimate = CostEstimate.from_usage(usage, rates, currency=self._pricing.currency)
        if estimate is None:  # pragma: no cover - usage is not None here
            return None

        self._total_cost += estimate.total_cost
        self._priced_calls += 1
        self._per_model[model.key] = self._per_model.get(model.key, 0.0) + estimate.total_cost
        return estimate

    def totals(self) -> CostTotals:
        """Returns the aggregate totals for this ledger."""
        return CostTotals(
            currency=self._pricing.currency,
            input_tokens=self._input_tokens,
            output_tokens=self._output_tokens,
            total_cost=self._total_cost,
            priced_call_count=self._priced_calls,
            unpriced_call_count=self._unpriced_calls,
            unmeasured_call_count=self._unmeasured_calls,
        )

    def cost_by_model(self) -> dict[str, float]:
        """Cost per model key, for models that could be priced."""
        return dict(sorted(self._per_model.items()))

    def merge(self, other: CostLedger) -> None:
        """Folds another ledger into this one.

        Used when a supervisor aggregates sub-agent costs into the parent
        execution's ledger.

        Raises:
            ConfigurationError: If the currencies differ. Adding amounts in
                different currencies yields a meaningless number, and there are
                no conversion rates configured here.
        """
        if self._pricing.currency != other._pricing.currency:
            raise ConfigurationError(
                "cannot merge ledgers with different currencies",
                details={
                    "left": self._pricing.currency,
                    "right": other._pricing.currency,
                },
            )
        self._input_tokens += other._input_tokens
        self._output_tokens += other._output_tokens
        self._total_cost += other._total_cost
        self._priced_calls += other._priced_calls
        self._unpriced_calls += other._unpriced_calls
        self._unmeasured_calls += other._unmeasured_calls
        for model_key, cost in other._per_model.items():
            self._per_model[model_key] = self._per_model.get(model_key, 0.0) + cost


def pricing_from_config(raw: dict[str, dict[str, float]], *, currency: str = "USD") -> PricingTable:
    """Builds a :class:`PricingTable` from configuration data.

    Args:
        raw: Model key -> rate row, as loaded from configuration.
        currency: Currency code.

    Returns:
        A validated table.

    Raises:
        ConfigurationError: As :class:`PricingTable`. Configuration errors
            surface at startup rather than at first use.
    """
    return PricingTable(currency=currency, rates=dict(raw))
