"""Unit tests for the model layer value objects.

The properties under test are the ones that silently corrupt data if violated:
usage that is invented rather than measured, cost that is inferred rather than
priced, and mutable objects that let a retried call alter a recorded response.
"""

from __future__ import annotations

import dataclasses
from typing import ClassVar

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError
from mas_orchestrator.models.types import (
    MAX_OUTPUT_TOKENS_LIMIT,
    CostEstimate,
    FinishReason,
    Message,
    ModelRef,
    ModelRequest,
    ModelResponse,
    Role,
    ToolCall,
    ToolSpec,
    Usage,
)


class TestUsage:
    """Usage is a measurement, never a default."""

    def test_reported_counts_are_stored(self) -> None:
        usage = Usage(input_tokens=10, output_tokens=20)
        assert usage.input_tokens == 10
        assert usage.output_tokens == 20

    def test_total_is_derived_when_absent(self) -> None:
        assert Usage(input_tokens=10, output_tokens=5).total == 15

    def test_reported_total_wins(self) -> None:
        assert Usage(input_tokens=10, output_tokens=5, total_tokens=99).total == 99

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"input_tokens": -1, "output_tokens": 1},
            {"input_tokens": 1, "output_tokens": -1},
            {"input_tokens": 1, "output_tokens": 1, "total_tokens": -1},
            {"input_tokens": 1, "output_tokens": 1, "cached_input_tokens": -1},
        ],
    )
    def test_negative_counts_are_refused(self, kwargs: dict[str, int]) -> None:
        with pytest.raises(InvalidRequestError):
            Usage(**kwargs)

    def test_add_sums_everything(self) -> None:
        first = Usage(input_tokens=10, output_tokens=5)
        second = Usage(input_tokens=1, output_tokens=2)
        combined = first.add(second)
        assert combined.input_tokens == 11
        assert combined.output_tokens == 7
        assert combined.total == 18

    def test_add_treats_unreported_cached_as_unknown(self) -> None:
        """A reported plus an unreported cached count must not become a number."""
        with_cache = Usage(input_tokens=1, output_tokens=1, cached_input_tokens=5)
        without = Usage(input_tokens=1, output_tokens=1)
        assert with_cache.add(without).cached_input_tokens is None

    def test_add_sums_cached_when_both_report(self) -> None:
        one = Usage(input_tokens=1, output_tokens=1, cached_input_tokens=5)
        two = Usage(input_tokens=1, output_tokens=1, cached_input_tokens=3)
        assert one.add(two).cached_input_tokens == 8

    def test_absence_is_none_not_zero(self) -> None:
        """The type has no zero default: unmeasured must stay distinguishable."""
        assert ModelResponse(model=ModelRef("openai", "gpt-4o")).usage is None


class TestCostEstimate:
    """Cost can only be computed from configured pricing."""

    PRICING: ClassVar[dict[str, float]] = {"input": 2.50, "output": 10.00}

    def test_computes_from_reported_usage(self) -> None:
        estimate = CostEstimate.from_usage(
            Usage(input_tokens=1_000_000, output_tokens=1_000_000), self.PRICING
        )
        assert estimate is not None
        assert estimate.input_cost == 2.50
        assert estimate.output_cost == 10.00
        assert estimate.total_cost == 12.50

    def test_absent_usage_yields_none_not_zero(self) -> None:
        """An unmeasured call must not be reported as free."""
        assert CostEstimate.from_usage(None, self.PRICING) is None

    def test_prices_are_per_million(self) -> None:
        estimate = CostEstimate.from_usage(Usage(input_tokens=1_000, output_tokens=0), self.PRICING)
        assert estimate is not None
        assert estimate.input_cost == pytest.approx(0.0025)

    def test_missing_rate_key_is_refused(self) -> None:
        """A malformed price must fail, not silently price at zero."""
        with pytest.raises(ConfigurationError):
            CostEstimate.from_usage(Usage(input_tokens=1, output_tokens=1), {"input": 1.0})

    def test_add_sums(self) -> None:
        first = CostEstimate.from_usage(
            Usage(input_tokens=1_000_000, output_tokens=0), self.PRICING
        )
        second = CostEstimate.from_usage(
            Usage(input_tokens=0, output_tokens=1_000_000), self.PRICING
        )
        assert first is not None and second is not None
        assert first.add(second).total_cost == pytest.approx(12.50)

    def test_add_refuses_mixed_currencies(self) -> None:
        with pytest.raises(ConfigurationError):
            CostEstimate("USD", 1.0, 1.0, 2.0).add(CostEstimate("EUR", 1.0, 1.0, 2.0))


class TestMessage:
    """Message construction and invariants."""

    def test_system_helper(self) -> None:
        assert Message.system("hi").role is Role.SYSTEM

    def test_user_helper(self) -> None:
        assert Message.user("hi").role is Role.USER

    def test_tool_result_requires_an_id(self) -> None:
        with pytest.raises(InvalidRequestError):
            Message(role=Role.TOOL, content="{}")

    def test_empty_message_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            Message(role=Role.USER, content="   ")

    def test_tool_calls_alone_are_valid(self) -> None:
        message = Message.assistant(tool_calls=[ToolCall(id="1", name="t", arguments_json="{}")])
        assert message.content == ""

    def test_tool_result_helper(self) -> None:
        message = Message.tool_result("call-1", "search", '{"a":1}')
        assert message.role is Role.TOOL
        assert message.tool_call_id == "call-1"
        assert message.name == "search"

    def test_message_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            Message.user("hi").content = "bye"  # type: ignore[misc]


class TestToolCall:
    """Tool call validation."""

    def test_valid_call(self) -> None:
        call = ToolCall(id="1", name="search", arguments_json="{}")
        assert call.name == "search"

    def test_fragment_with_only_an_id_is_allowed(self) -> None:
        """Streamed argument fragments carry an id but no name."""
        assert ToolCall(id="c1", name="", arguments_json='{"q":').id == "c1"

    def test_call_with_only_a_name_is_allowed(self) -> None:
        """Some providers do not emit ids."""
        assert ToolCall(id="", name="search", arguments_json="{}").name == "search"

    def test_call_with_neither_id_nor_name_is_refused(self) -> None:
        """A tool call with neither is meaningless."""
        with pytest.raises(InvalidRequestError):
            ToolCall(id="", name="", arguments_json="{}")


class TestToolSpec:
    """Tool declaration validation."""

    def test_object_schema_is_valid(self) -> None:
        spec = ToolSpec(name="s", description="d", json_schema={"type": "object"})
        assert spec.name == "s"

    def test_non_object_schema_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            ToolSpec(name="s", description="d", json_schema={"type": "array"})

    def test_absent_type_is_allowed(self) -> None:
        """JSON Schema permits omitting `type`."""
        assert ToolSpec(name="s", description="d", json_schema={}).name == "s"

    def test_empty_name_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            ToolSpec(name=" ", description="d", json_schema={})


class TestModelRef:
    """Model reference identity."""

    def test_str_form(self) -> None:
        assert str(ModelRef("openai", "gpt-4o")) == "openai/gpt-4o"

    def test_key_matches_str(self) -> None:
        ref = ModelRef("openai", "gpt-4o")
        assert ref.key == str(ref)

    def test_empty_provider_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            ModelRef("", "gpt-4o")

    def test_empty_model_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            ModelRef("openai", "")


class TestModelRequest:
    """Request validation and normalisation."""

    def _request(self, **overrides: object) -> ModelRequest:
        base: dict[str, object] = {
            "messages": (Message.user("hi"),),
            "model": ModelRef("openai", "gpt-4o"),
        }
        base.update(overrides)
        return ModelRequest(**base)  # type: ignore[arg-type]

    def test_requires_messages(self) -> None:
        with pytest.raises(InvalidRequestError):
            ModelRequest(messages=(), model=ModelRef("openai", "gpt-4o"))

    def test_temperature_range_is_enforced(self) -> None:
        with pytest.raises(InvalidRequestError):
            self._request(temperature=3.0)
        with pytest.raises(InvalidRequestError):
            self._request(temperature=-0.1)

    def test_top_p_range_is_enforced(self) -> None:
        with pytest.raises(InvalidRequestError):
            self._request(top_p=1.5)

    def test_non_positive_max_output_is_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            self._request(max_output_tokens=0)

    def test_omitted_output_cap_is_bounded(self) -> None:
        """An omitted cap must not produce an unbounded generation."""
        assert self._request().max_output_tokens == MAX_OUTPUT_TOKENS_LIMIT

    def test_excessive_output_cap_is_clamped(self) -> None:
        request = self._request(max_output_tokens=MAX_OUTPUT_TOKENS_LIMIT * 10)
        assert request.max_output_tokens == MAX_OUTPUT_TOKENS_LIMIT

    def test_request_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            self._request().temperature = 0.0  # type: ignore[misc]


class TestModelResponse:
    """Response semantics."""

    def test_is_truncated(self) -> None:
        response = ModelResponse(
            model=ModelRef("openai", "gpt-4o"), finish_reason=FinishReason.LENGTH
        )
        assert response.is_truncated is True

    def test_requires_tool_execution(self) -> None:
        response = ModelResponse(
            model=ModelRef("openai", "gpt-4o"),
            tool_calls=(ToolCall(id="1", name="t", arguments_json="{}"),),
        )
        assert response.requires_tool_execution is True

    def test_defaults_are_explicit(self) -> None:
        response = ModelResponse(model=ModelRef("openai", "gpt-4o"))
        assert response.text == ""
        assert response.finish_reason is FinishReason.STOP
        assert response.usage is None
