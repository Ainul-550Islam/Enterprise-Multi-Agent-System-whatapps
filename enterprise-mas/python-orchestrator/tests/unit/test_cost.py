"""Unit tests for usage and cost accounting.

The central property: a number that cannot be measured or priced must never be
reported as zero, because a zero is indistinguishable from a genuinely free
call once it reaches a dashboard or a budget.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.models.cost import (
    CostLedger,
    PricingTable,
    pricing_from_config,
)
from mas_orchestrator.models.types import ModelRef, Usage

GPT = ModelRef("openai", "gpt-4o")
UNPRICED = ModelRef("openai", "unknown-model")

PRICING = PricingTable(
    currency="USD",
    rates={"openai/gpt-4o": {"input": 2.50, "output": 10.00}},
)


class TestPricingTable:
    """Pricing must be complete and sane."""

    def test_rate_lookup(self) -> None:
        assert PRICING.rate_for(GPT) is not None

    def test_unknown_model_is_unpriced(self) -> None:
        assert PRICING.rate_for(UNPRICED) is None

    def test_is_priced(self) -> None:
        assert PRICING.is_priced(GPT) is True
        assert PRICING.is_priced(UNPRICED) is False

    def test_missing_rate_key_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            PricingTable(rates={"m": {"input": 1.0}})

    def test_negative_rate_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            PricingTable(rates={"m": {"input": -1.0, "output": 1.0}})

    def test_blank_currency_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            PricingTable(currency=" ", rates={})

    def test_models_are_sorted(self) -> None:
        table = PricingTable(rates={"z": {"input": 1, "output": 1}, "a": {"input": 1, "output": 1}})
        assert table.models() == ["a", "z"]

    def test_from_config(self) -> None:
        table = pricing_from_config({"openai/gpt-4o": {"input": 1.0, "output": 2.0}})
        assert table.is_priced(GPT) is True


class TestCostLedger:
    """Ledger accumulation must stay honest about what it knows."""

    def test_priced_call_contributes_cost(self) -> None:
        ledger = CostLedger(PRICING)
        estimate = ledger.record(GPT, Usage(input_tokens=1_000_000, output_tokens=1_000_000))
        assert estimate is not None
        assert estimate.total_cost == pytest.approx(12.50)

    def test_unmeasured_call_is_counted_but_adds_no_tokens(self) -> None:
        """Inventing tokens here would corrupt every aggregate."""
        ledger = CostLedger(PRICING)
        assert ledger.record(GPT, None) is None
        totals = ledger.totals()
        assert totals.unmeasured_call_count == 1
        assert totals.input_tokens == 0
        assert totals.total_cost == 0.0

    def test_unpriced_model_is_counted_but_adds_no_cost(self) -> None:
        """An unpriced model must not look free."""
        ledger = CostLedger(PRICING, emit_unpriced_warning=False)
        assert ledger.record(UNPRICED, Usage(input_tokens=100, output_tokens=100)) is None
        totals = ledger.totals()
        assert totals.unpriced_call_count == 1
        assert totals.input_tokens == 100
        assert totals.total_cost == 0.0

    def test_is_complete_is_false_when_anything_is_unknown(self) -> None:
        ledger = CostLedger(PRICING, emit_unpriced_warning=False)
        ledger.record(GPT, Usage(input_tokens=1, output_tokens=1))
        assert ledger.totals().is_complete is True

        ledger.record(UNPRICED, Usage(input_tokens=1, output_tokens=1))
        assert ledger.totals().is_complete is False

        ledger.record(GPT, None)
        assert ledger.totals().is_complete is False

    def test_tokens_accumulate(self) -> None:
        ledger = CostLedger(PRICING)
        ledger.record(GPT, Usage(input_tokens=10, output_tokens=5))
        ledger.record(GPT, Usage(input_tokens=1, output_tokens=2))
        totals = ledger.totals()
        assert totals.input_tokens == 11
        assert totals.output_tokens == 7
        assert totals.total_tokens == 18

    def test_cost_accumulates(self) -> None:
        ledger = CostLedger(PRICING)
        for _ in range(4):
            ledger.record(GPT, Usage(input_tokens=1_000_000, output_tokens=0))
        assert ledger.totals().total_cost == pytest.approx(10.00)

    def test_call_count_spans_all_categories(self) -> None:
        ledger = CostLedger(PRICING, emit_unpriced_warning=False)
        ledger.record(GPT, Usage(input_tokens=1, output_tokens=1))
        ledger.record(UNPRICED, Usage(input_tokens=1, output_tokens=1))
        ledger.record(GPT, None)
        assert ledger.totals().call_count == 3
        assert ledger.totals().priced_call_count == 1

    def test_cost_by_model(self) -> None:
        ledger = CostLedger(PRICING)
        ledger.record(GPT, Usage(input_tokens=1_000_000, output_tokens=0))
        assert ledger.cost_by_model() == {"openai/gpt-4o": pytest.approx(2.50)}

    def test_empty_ledger_is_complete(self) -> None:
        """Nothing recorded means nothing is unaccounted for."""
        totals = CostLedger(PRICING).totals()
        assert totals.is_complete is True
        assert totals.call_count == 0


class TestLedgerMerge:
    """Aggregating sub-agent costs into a parent execution."""

    def test_merge_sums_everything(self) -> None:
        parent = CostLedger(PRICING)
        parent.record(GPT, Usage(input_tokens=1_000_000, output_tokens=0))

        child = CostLedger(PRICING)
        child.record(GPT, Usage(input_tokens=0, output_tokens=1_000_000))

        parent.merge(child)
        totals = parent.totals()
        assert totals.total_cost == pytest.approx(12.50)
        assert totals.input_tokens == 1_000_000
        assert totals.output_tokens == 1_000_000

    def test_merge_refuses_mixed_currencies(self) -> None:
        parent = CostLedger(PRICING)
        child = CostLedger(PricingTable(currency="EUR", rates={}))
        with pytest.raises(ConfigurationError):
            parent.merge(child)

    def test_merge_preserves_incompleteness(self) -> None:
        """A complete parent merged with an unpriced child is incomplete."""
        parent = CostLedger(PRICING)
        parent.record(GPT, Usage(input_tokens=1, output_tokens=1))

        child = CostLedger(PRICING, emit_unpriced_warning=False)
        child.record(UNPRICED, Usage(input_tokens=1, output_tokens=1))

        parent.merge(child)
        assert parent.totals().is_complete is False
