"""Policy-aware model routing.

The router decides which model serves a request, and it is the component that
makes "policy" real: an allowlist, a capability requirement and a cost
preference are all *constraints*, and a candidate that violates any of them is
not a candidate at all.

The ordering rule that matters most:

    **Failover must never violate policy.**

A fallback model is chosen from the same filtered candidate set as the primary.
It is never an escape hatch around the allowlist, the capability check, or the
cost ceiling (spec §8). If no candidate satisfies every constraint the router
raises rather than relaxing one — a relaxed constraint is how a "temporary"
fallback becomes a permanent hole.

Selection is **deterministic**: candidates are ordered by an explicit,
documented key, so the same request and the same policy always pick the same
model. Non-deterministic routing makes incidents unreproducible and cost
unpredictable.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import ConfigurationError, ModelNotAvailableError

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from mas_orchestrator.models.registry import ModelRegistry
    from mas_orchestrator.models.types import ModelCapabilities, ModelRef

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = [
    "ModelRouter",
    "RoutingPolicy",
    "RoutingRequest",
    "SelectionReason",
]


@dataclass(frozen=True, slots=True)
class SelectionReason:
    """Why a model was (or was not) selected.

    Returned alongside the decision so the caller can log and meter routing
    behaviour without reconstructing it. Also used to build a useful error when
    everything is rejected: "model X rejected: not allowlisted" is actionable in
    a way that "no model available" is not.

    Attributes:
        model: The candidate model.
        accepted: Whether it satisfied every constraint.
        rejection: Why it was rejected, when it was.
    """

    model: ModelRef
    accepted: bool
    rejection: str | None = None


@dataclass(frozen=True, slots=True)
class RoutingPolicy:
    """Constraints applied to every routing decision.

    Attributes:
        allowed_models: Allowlist of ``provider/model`` keys. An empty
            allowlist means **nothing is permitted**, which is the safe default
            for an unconfigured service.
        preferred_models: Ordered preference used to break ties. A model that
            appears earlier wins; models not listed sort last.
        max_input_tokens: Rejects models whose context window is too small.
            ``None`` disables the check. A model with an *unknown* window is
            rejected when the check is enabled, because "unknown" cannot be
            shown to fit.
        max_cost_per_million_input: Optional ceiling on input price. Requires
            pricing data; candidates with no configured price are rejected when
            the ceiling is set.
        max_cost_per_million_output: Optional ceiling on output price.
    """

    allowed_models: frozenset[str] = field(default_factory=frozenset)
    preferred_models: tuple[str, ...] = ()
    max_input_tokens: int | None = None
    max_cost_per_million_input: float | None = None
    max_cost_per_million_output: float | None = None

    def __post_init__(self) -> None:
        """Validates the policy.

        Raises:
            ConfigurationError: If a numeric bound is negative, which would
                silently reject every candidate while looking configured.
        """
        for name in (
            "max_input_tokens",
            "max_cost_per_million_input",
            "max_cost_per_million_output",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ConfigurationError(f"{name} must not be negative", details={"value": value})

    def is_allowlisted(self, model: ModelRef) -> bool:
        """Whether *model* appears on the allowlist."""
        return model.key in self.allowed_models


@dataclass(frozen=True, slots=True)
class RoutingRequest:
    """What a caller needs from a model.

    Attributes:
        require_tools: The model must support tool calling.
        require_structured_output: The model must support schema-constrained
            output natively.
        require_streaming: The model must support streaming.
        estimated_input_tokens: Used to check the context window. ``None``
            skips the fit check.
        preferred_models: Per-request override of the policy preference order.
    """

    require_tools: bool = False
    require_structured_output: bool = False
    require_streaming: bool = False
    estimated_input_tokens: int | None = None
    preferred_models: tuple[str, ...] = ()


class ModelRouter:
    """Selects a model for a request under a policy.

    Args:
        registry: The provider/model registry to select from.
        policy: The constraints to apply.
        pricing: Optional pricing table keyed by model key, used for the cost
            ceilings. Missing entries make a candidate ineligible *only* when a
            ceiling is configured.
    """

    def __init__(
        self,
        registry: ModelRegistry,
        policy: RoutingPolicy,
        *,
        pricing: dict[str, dict[str, float]] | None = None,
    ) -> None:
        self._registry = registry
        self._policy = policy
        self._pricing: dict[str, dict[str, float]] = pricing or {}

    @property
    def policy(self) -> RoutingPolicy:
        """The policy in force."""
        return self._policy

    def _candidates(self) -> list[ModelRef]:
        """Enumerates candidate models in a deterministic order.

        Candidates come from the allowlist (so an unallowlisted model can never
        be reached, even as a fallback), ordered by preference and then by key
        so ties are stable.
        """
        from mas_orchestrator.models.types import ModelRef

        known = dict(self._registry.iter_models())
        candidates: list[ModelRef] = []

        for key in sorted(self._policy.allowed_models):
            provider_id = known.get(key)
            if provider_id is None:
                continue
            provider, _, model = key.partition("/")
            candidates.append(ModelRef(provider=provider, model=model))

        preference = self._policy.preferred_models
        candidates.sort(key=lambda ref: (self._rank(ref, preference), ref.key))
        return candidates

    @staticmethod
    def _rank(model: ModelRef, preference: Sequence[str]) -> int:
        """Preference rank: lower is better; unlisted models sort last."""
        try:
            return preference.index(model.key)
        except ValueError:
            return len(preference)

    def evaluate(self, model: ModelRef, request: RoutingRequest) -> SelectionReason:
        """Checks one candidate against every constraint.

        Args:
            model: The candidate.
            request: What the caller needs.

        Returns:
            The accept/reject reason. A registry lookup failure is reported as
            a rejection rather than raised, so one unreachable provider does
            not abort evaluation of the rest.
        """
        if not self._policy.is_allowlisted(model):
            return SelectionReason(model, False, "not_allowlisted")

        if self._registry.find(model) is None:
            return SelectionReason(model, False, "no_provider")

        try:
            capabilities: ModelCapabilities = self._registry.capabilities(model)
        except ModelNotAvailableError:
            return SelectionReason(model, False, "no_provider")

        if request.require_tools and not capabilities.supports_tools:
            return SelectionReason(model, False, "no_tool_support")
        if request.require_structured_output and not capabilities.supports_structured_output:
            return SelectionReason(model, False, "no_structured_output")
        if request.require_streaming and not capabilities.supports_streaming:
            return SelectionReason(model, False, "no_streaming")

        # Two distinct checks, both about the context window:
        #
        # 1. Policy floor: the model's declared window must be at least as large
        #    as the policy expects to be able to send. This is a property of the
        #    model and applies even when the caller gave no size estimate.
        # 2. Request fit: this particular request must fit inside that window.
        #
        # An unknown window fails both, because "unknown" cannot be shown to
        # satisfy either.
        if self._policy.max_input_tokens is not None:
            if capabilities.max_input_tokens is None:
                return SelectionReason(model, False, "unknown_context_window")
            if capabilities.max_input_tokens < self._policy.max_input_tokens:
                return SelectionReason(model, False, "context_too_small")

        if request.estimated_input_tokens is not None:
            if capabilities.max_input_tokens is None:
                return SelectionReason(model, False, "unknown_context_window")
            if request.estimated_input_tokens > capabilities.max_input_tokens:
                return SelectionReason(model, False, "context_too_small")

        rejection = self._check_price(model)
        if rejection is not None:
            return SelectionReason(model, False, rejection)

        return SelectionReason(model, True)

    def _check_price(self, model: ModelRef) -> str | None:
        """Checks configured price ceilings, returning a rejection reason."""
        ceiling_in = self._policy.max_cost_per_million_input
        ceiling_out = self._policy.max_cost_per_million_output
        if ceiling_in is None and ceiling_out is None:
            return None

        prices = self._pricing.get(model.key)
        if prices is None:
            # A ceiling is configured but this model has no price data. Treat
            # as ineligible: an unpriced model cannot be shown to be within
            # budget, and silently allowing it defeats the control.
            return "no_pricing_data"

        if ceiling_in is not None:
            actual = prices.get("input")
            if actual is None or actual > ceiling_in:
                return "input_too_expensive"
        if ceiling_out is not None:
            actual = prices.get("output")
            if actual is None or actual > ceiling_out:
                return "output_too_expensive"
        return None

    def select(self, request: RoutingRequest) -> ModelRef:
        """Returns the best model satisfying every constraint.

        Args:
            request: What the caller needs.

        Returns:
            The selected model.

        Raises:
            ModelNotAvailableError: If no candidate satisfies the constraints.
                The error details include every rejection reason, so an
                operator can see whether the allowlist, a capability, or a
                price ceiling is responsible.
        """
        rejections: list[dict[str, str]] = []

        for candidate in self._candidates():
            reason = self.evaluate(candidate, request)
            if reason.accepted:
                return candidate
            rejections.append({"model": candidate.key, "reason": reason.rejection or "unknown"})

        raise ModelNotAvailableError(
            "no model satisfies the routing policy",
            details={
                "require_tools": request.require_tools,
                "require_structured_output": request.require_structured_output,
                "require_streaming": request.require_streaming,
                "allowlist_size": len(self._policy.allowed_models),
                "rejections": rejections,
            },
        )

    def failover_chain(
        self,
        request: RoutingRequest,
        *,
        limit: int = 3,
    ) -> list[ModelRef]:
        """Returns an ordered list of fallback models.

        Crucially, every entry has passed the *same* evaluation as the primary.
        This is the mechanism that guarantees failover cannot bypass the
        allowlist, a capability requirement, or a cost ceiling.

        Args:
            request: What the caller needs.
            limit: Maximum number of models to return.

        Returns:
            Ordered candidates, best first. May be empty only if nothing
            satisfies the policy — in which case :meth:`select` would have
            raised.

        Raises:
            ConfigurationError: If ``limit`` is less than one.
        """
        if limit < 1:
            raise ConfigurationError(
                "failover chain limit must be at least 1", details={"limit": limit}
            )

        accepted = [
            candidate
            for candidate in self._candidates()
            if self.evaluate(candidate, request).accepted
        ]

        preference = request.preferred_models or self._policy.preferred_models
        if preference:
            accepted.sort(key=lambda ref: (self._rank(ref, preference), ref.key))

        return accepted[:limit]

    def explain(self, request: RoutingRequest) -> list[SelectionReason]:
        """Evaluates every candidate, for diagnostics and tests.

        Args:
            request: What the caller needs.

        Returns:
            One reason per candidate, in routing order.
        """
        return [self.evaluate(c, request) for c in self._candidates()]


def build_policy(
    allowed_models: Iterable[str],
    *,
    preferred_models: Iterable[str] = (),
    max_input_tokens: int | None = None,
) -> RoutingPolicy:
    """Convenience constructor for a :class:`RoutingPolicy`.

    Args:
        allowed_models: Allowlist entries as ``provider/model`` keys.
        preferred_models: Preference order.
        max_input_tokens: Context-window requirement.

    Returns:
        The policy.
    """
    return RoutingPolicy(
        allowed_models=frozenset(allowed_models),
        preferred_models=tuple(preferred_models),
        max_input_tokens=max_input_tokens,
    )
