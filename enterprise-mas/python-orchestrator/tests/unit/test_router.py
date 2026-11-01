"""Unit tests for policy-aware model routing.

The property that matters most: **failover must never violate policy**. These
tests assert that every model in a failover chain satisfies the same
constraints as the primary, so a fallback cannot become an escape hatch around
the allowlist or a capability requirement.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

from mas_orchestrator.contracts.errors import ConfigurationError, ModelNotAvailableError
from mas_orchestrator.models.registry import ModelRegistry
from mas_orchestrator.models.router import (
    ModelRouter,
    RoutingPolicy,
    RoutingRequest,
    build_policy,
)
from mas_orchestrator.models.types import ModelCapabilities, ModelRef

FAST = ModelRef("openai", "gpt-4o-mini")
SMART = ModelRef("openai", "gpt-4o")
OTHER = ModelRef("anthropic", "claude-sonnet-4-20250514")


class _FakeProvider:
    """A provider with a fixed model table, for deterministic tests."""

    def __init__(self, provider_id: str, models: dict[str, ModelCapabilities]) -> None:
        self.provider_id = provider_id
        self._capabilities = models

    def supports(self, model: ModelRef) -> bool:
        return model.model in self._capabilities

    def capabilities(self, model: ModelRef) -> ModelCapabilities:
        return self._capabilities[model.model]


def _capabilities(
    *,
    tools: bool = True,
    streaming: bool = True,
    structured: bool = True,
    window: int | None = 100_000,
) -> ModelCapabilities:
    return ModelCapabilities(
        supports_tools=tools,
        supports_streaming=streaming,
        supports_structured_output=structured,
        supports_system_prompt=True,
        max_input_tokens=window,
        max_output_tokens=8_192,
    )


def _registry() -> ModelRegistry:
    registry = ModelRegistry()
    registry.register(
        _FakeProvider(
            "openai",
            {
                "gpt-4o": _capabilities(),
                "gpt-4o-mini": _capabilities(structured=False),
            },
        )
    )
    registry.register(
        _FakeProvider(
            "anthropic",
            {
                "claude-sonnet-4-20250514": _capabilities(structured=False),
            },
        )
    )
    registry.register_model(FAST, "openai")
    registry.register_model(SMART, "openai")
    registry.register_model(OTHER, "anthropic")
    return registry


ALL = frozenset({FAST.key, SMART.key, OTHER.key})


class TestPolicyValidation:
    """Policy construction."""

    def test_negative_bounds_are_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            RoutingPolicy(allowed_models=ALL, max_input_tokens=-1)

    def test_negative_price_ceiling_is_refused(self) -> None:
        with pytest.raises(ConfigurationError):
            RoutingPolicy(allowed_models=ALL, max_cost_per_million_input=-1.0)

    def test_empty_allowlist_permits_nothing(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy())
        with pytest.raises(ModelNotAvailableError):
            router.select(RoutingRequest())

    def test_build_policy_helper(self) -> None:
        policy = build_policy([FAST.key], preferred_models=[FAST.key])
        assert policy.is_allowlisted(FAST) is True
        assert policy.is_allowlisted(SMART) is False


class TestSelection:
    """Model selection is deterministic and policy-respecting."""

    def test_preferred_model_wins(self) -> None:
        router = ModelRouter(
            _registry(), RoutingPolicy(allowed_models=ALL, preferred_models=[SMART.key])
        )
        assert router.select(RoutingRequest()) == SMART

    def test_selection_is_deterministic(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models=ALL))
        first = router.select(RoutingRequest())
        assert all(router.select(RoutingRequest()) == first for _ in range(5))

    def test_unlisted_models_are_never_selected(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={FAST.key}))
        assert router.select(RoutingRequest()) == FAST

    def test_capability_requirement_filters(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models=ALL))
        selected = router.select(RoutingRequest(require_structured_output=True))
        assert selected == SMART  # the only model with structured output

    def test_tool_requirement_filters(self) -> None:
        """A model without tool support must not serve a tool-using agent."""
        registry = ModelRegistry()
        registry.register(
            _FakeProvider(
                "openai",
                {"no-tools": _capabilities(tools=False), "with-tools": _capabilities()},
            )
        )
        registry.register_model(ModelRef("openai", "no-tools"), "openai")
        registry.register_model(ModelRef("openai", "with-tools"), "openai")

        router = ModelRouter(
            registry,
            RoutingPolicy(
                allowed_models={"openai/no-tools", "openai/with-tools"},
                preferred_models=["openai/no-tools"],
            ),
        )
        selected = router.select(RoutingRequest(require_tools=True))
        assert selected == ModelRef("openai", "with-tools")

    def test_no_satisfying_model_raises(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={FAST.key}))
        with pytest.raises(ModelNotAvailableError):
            router.select(RoutingRequest(require_structured_output=True))

    def test_error_explains_every_rejection(self) -> None:
        """When nothing satisfies the policy, the error names the reason."""
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={FAST.key, OTHER.key}))
        with pytest.raises(ModelNotAvailableError) as excinfo:
            router.select(RoutingRequest(require_structured_output=True))
        # Neither candidate has structured output, so both are rejected for it.
        reasons = {r["model"]: r["reason"] for r in excinfo.value.details["rejections"]}
        assert reasons[FAST.key] == "no_structured_output"
        assert reasons[OTHER.key] == "no_structured_output"

    def test_explain_reports_the_capability_gap(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models=ALL))
        reasons = {
            r.model.key: r for r in router.explain(RoutingRequest(require_structured_output=True))
        }
        assert reasons[SMART.key].accepted is True
        assert reasons[FAST.key].rejection == "no_structured_output"
        assert reasons[OTHER.key].rejection == "no_structured_output"


class TestContextWindow:
    """Fit checking."""

    def test_small_window_is_rejected(self) -> None:
        """A model whose window is below the policy floor is rejected."""
        router = ModelRouter(
            _registry(), RoutingPolicy(allowed_models={SMART.key}, max_input_tokens=200_000)
        )
        with pytest.raises(ModelNotAvailableError) as excinfo:
            router.select(RoutingRequest())
        assert excinfo.value.details["rejections"][0]["reason"] == "context_too_small"

    def test_oversized_request_is_rejected(self) -> None:
        """A request that does not fit the model's window is rejected."""
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={SMART.key}))
        with pytest.raises(ModelNotAvailableError) as excinfo:
            router.select(RoutingRequest(estimated_input_tokens=500_000))
        assert excinfo.value.details["rejections"][0]["reason"] == "context_too_small"

    def test_fitting_request_is_accepted(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={SMART.key}))
        assert router.select(RoutingRequest(estimated_input_tokens=500)) == SMART

    def test_unknown_window_is_rejected_when_checked(self) -> None:
        """'Unknown' cannot be shown to fit."""
        registry = ModelRegistry()
        registry.register(_FakeProvider("openai", {"m": _capabilities(window=None)}))
        registry.register_model(ModelRef("openai", "m"), "openai")

        router = ModelRouter(
            registry,
            RoutingPolicy(allowed_models={"openai/m"}, max_input_tokens=1_000),
        )
        with pytest.raises(ModelNotAvailableError) as excinfo:
            router.select(RoutingRequest(estimated_input_tokens=10))
        assert excinfo.value.details["rejections"][0]["reason"] == "unknown_context_window"


class TestPriceCeilings:
    """Cost controls are routing constraints, not afterthoughts."""

    PRICING: ClassVar[dict[str, dict[str, float]]] = {
        "openai/gpt-4o": {"input": 10.0, "output": 30.0},
        "openai/gpt-4o-mini": {"input": 0.15, "output": 0.60},
    }

    def test_expensive_model_is_excluded(self) -> None:
        router = ModelRouter(
            _registry(),
            RoutingPolicy(allowed_models={FAST.key, SMART.key}, max_cost_per_million_input=1.0),
            pricing=self.PRICING,
        )
        assert router.select(RoutingRequest()) == FAST

    def test_unpriced_model_is_excluded_when_ceiling_set(self) -> None:
        """An unpriced model cannot be shown to be within budget."""
        router = ModelRouter(
            _registry(),
            RoutingPolicy(allowed_models={OTHER.key}, max_cost_per_million_input=1.0),
            pricing=self.PRICING,
        )
        with pytest.raises(ModelNotAvailableError) as excinfo:
            router.select(RoutingRequest())
        assert excinfo.value.details["rejections"][0]["reason"] == "no_pricing_data"

    def test_unpriced_model_is_allowed_without_a_ceiling(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={OTHER.key}))
        assert router.select(RoutingRequest()) == OTHER


class TestFailoverChain:
    """Failover must not bypass policy."""

    def test_chain_only_contains_policy_compliant_models(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models=ALL))
        chain = router.failover_chain(RoutingRequest(require_structured_output=True), limit=5)
        # Only SMART has structured output, so it is the sole entry.
        assert chain == [SMART]

    def test_chain_excludes_unlisted_models(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={FAST.key, SMART.key}))
        chain = router.failover_chain(RoutingRequest(), limit=5)
        assert OTHER not in chain

    def test_chain_is_bounded(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models=ALL))
        assert len(router.failover_chain(RoutingRequest(), limit=2)) == 2

    def test_chain_respects_preference_order(self) -> None:
        router = ModelRouter(
            _registry(),
            RoutingPolicy(allowed_models=ALL, preferred_models=[OTHER.key, SMART.key, FAST.key]),
        )
        chain = router.failover_chain(RoutingRequest(), limit=3)
        assert chain == [OTHER, SMART, FAST]

    def test_request_preference_overrides_policy(self) -> None:
        router = ModelRouter(
            _registry(), RoutingPolicy(allowed_models=ALL, preferred_models=[FAST.key])
        )
        chain = router.failover_chain(RoutingRequest(preferred_models=[SMART.key]), limit=2)
        assert chain[0] == SMART

    def test_chain_empty_when_nothing_complies(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={FAST.key}))
        assert router.failover_chain(RoutingRequest(require_structured_output=True)) == []

    def test_invalid_limit_is_refused(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models=ALL))
        with pytest.raises(ConfigurationError):
            router.failover_chain(RoutingRequest(), limit=0)


class TestExplain:
    """Diagnostics."""

    def test_explain_covers_every_candidate(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models=ALL))
        reasons = router.explain(RoutingRequest())
        assert len(reasons) == 3

    def test_explain_marks_acceptance(self) -> None:
        router = ModelRouter(_registry(), RoutingPolicy(allowed_models={SMART.key}))
        reasons = router.explain(RoutingRequest())
        assert reasons[0].accepted is True
        assert reasons[0].rejection is None
