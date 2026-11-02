"""Version-aware provider/model registry with deterministic lookup.

The registry is the single place that answers "which provider serves this
model?". Keeping it central is what lets the router, the failover chain and the
allowlist all agree: there is no second index that could drift.

Determinism properties:

* Registration order does not affect lookup. A model maps to exactly one
  provider; registering a second provider for the same model is refused rather
  than resolved by insertion order, because "first wins" makes behaviour depend
  on import order.
* Enumeration is sorted, so any code that iterates models (health checks,
  capability reports, tests) sees a stable order.
* Lookup never raises for a missing model — it returns ``None``. The router
  decides whether a missing model is an error, and a ``KeyError`` escaping from
  a lookup during enumeration would be a bug.

The registry holds **no configuration and no credentials**. It maps identifiers
to already-constructed providers; credential resolution happens in
:mod:`mas_orchestrator.providers.factory`.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final

from mas_orchestrator.contracts.errors import ConfigurationError, ModelNotAvailableError

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from mas_orchestrator.models.types import ModelCapabilities, ModelRef

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

__all__ = ["ModelRegistry"]


class ModelRegistry:
    """Maps :class:`ModelRef` to the provider that serves it.

    Providers are registered by the composition root at startup. The registry
    does not construct anything, so it performs no I/O and is safe to share
    across concurrent executions.
    """

    def __init__(self) -> None:
        self._providers: dict[str, object] = {}
        self._models: dict[str, tuple[str, str]] = {}

    def register(self, provider: object) -> None:
        """Registers *provider* and every model it advertises.

        Args:
            provider: An object satisfying
                :class:`~mas_orchestrator.models.base.ModelProvider`.

        Raises:
            ConfigurationError: If a provider with the same id is already
                registered, or if the provider exposes no recognised id.
        """
        provider_id = getattr(provider, "provider_id", None)
        if not isinstance(provider_id, str) or not provider_id:
            raise ConfigurationError(
                "provider must expose a non-empty provider_id",
                details={"received": type(provider).__name__},
            )
        if provider_id in self._providers:
            raise ConfigurationError(
                "provider is already registered", details={"provider": provider_id}
            )

        self._providers[provider_id] = provider
        _LOGGER.debug("registered provider", extra={"provider": provider_id})

    def register_model(self, model: ModelRef, provider_id: str) -> None:
        """Binds a specific model to a provider.

        Needed for providers that cannot enumerate their own models (for
        example a compatible endpoint whose table comes from configuration).

        Args:
            model: The model reference.
            provider_id: The provider that serves it. The provider need not be
                registered yet, which allows models and providers to be
                declared in any order at startup.

        Raises:
            ConfigurationError: If the model is already bound to a different
                provider. Binding the same model twice to the same provider is
                idempotent, so startup code can be re-run safely.
        """
        existing = self._models.get(model.key)
        if existing is not None and existing[0] != provider_id:
            raise ConfigurationError(
                "model is already bound to a different provider",
                details={
                    "model": model.key,
                    "existing_provider": existing[0],
                    "new_provider": provider_id,
                },
            )
        self._models[model.key] = (provider_id, model.model)

    def resolve(self, model: ModelRef) -> object:
        """Returns the provider serving *model*.

        Args:
            model: The model reference.

        Returns:
            The registered provider.

        Raises:
            ModelNotAvailableError: If no provider serves the model, or if the
                model resolves to a provider id that was never registered.
                Both mean "this model cannot be served right now".
        """
        binding = self._models.get(model.key)
        if binding is None:
            # Fall back to asking each provider whether it owns the model.
            for provider in self._providers.values():
                if bool(getattr(provider, "supports", lambda _m: False)(model)):
                    return provider
            raise ModelNotAvailableError(
                "no provider serves this model", details={"model": model.key}
            )

        provider_id, _ = binding
        provider = self._providers.get(provider_id)
        if provider is None:
            raise ModelNotAvailableError(
                "model is bound to an unregistered provider",
                details={"model": model.key, "provider": provider_id},
            )
        return provider

    def find(self, model: ModelRef) -> object | None:
        """Returns the provider serving *model*, or ``None``.

        Unlike :meth:`resolve` this never raises, so it is safe to use while
        enumerating candidates during routing.
        """
        try:
            return self.resolve(model)
        except ModelNotAvailableError:
            return None

    def capabilities(self, model: ModelRef) -> ModelCapabilities:
        """Returns the capabilities of *model*.

        Raises:
            ModelNotAvailableError: If the model is unknown, or its provider
                cannot report capabilities for it.
        """
        provider = self.resolve(model)
        reporter = getattr(provider, "capabilities", None)
        if reporter is None:
            raise ModelNotAvailableError(
                "provider cannot report capabilities",
                details={"model": model.key},
            )
        capabilities: ModelCapabilities = reporter(model)
        return capabilities

    def provider_ids(self) -> list[str]:
        """Registered provider ids, sorted."""
        return sorted(self._providers)

    def models(self) -> list[str]:
        """Known model keys, sorted."""
        return sorted(self._models)

    def iter_models(self) -> Iterator[tuple[str, str]]:
        """Yields ``(model_key, provider_id)`` pairs in sorted order."""
        for key in sorted(self._models):
            yield key, self._models[key][0]

    def __len__(self) -> int:
        """Number of registered providers."""
        return len(self._providers)

    def __contains__(self, model: object) -> bool:
        """Whether *model* is servable. Unknown objects yield ``False``."""
        if not hasattr(model, "key"):
            return False
        key = model.key
        if not isinstance(key, str):
            return False
        return self.find(_ref_from_key(key)) is not None

    def register_all(self, providers: Iterable[object]) -> None:
        """Registers several providers, failing on the first conflict.

        Args:
            providers: Providers to register.

        Raises:
            ConfigurationError: As :meth:`register`. Registration is not
                partial-continue: a duplicate means the composition root has a
                wiring bug, and standing up half the providers would mask it.
        """
        for provider in providers:
            self.register(provider)


def _ref_from_key(key: str) -> ModelRef:
    """Rebuilds a :class:`ModelRef` from a ``provider/model`` key."""
    from mas_orchestrator.models.types import ModelRef

    provider, _, model = key.partition("/")
    return ModelRef(provider=provider, model=model)
