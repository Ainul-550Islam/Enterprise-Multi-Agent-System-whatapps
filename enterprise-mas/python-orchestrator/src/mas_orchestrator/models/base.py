"""The model provider interface every adapter implements.

One interface, four operations: :meth:`~ModelProvider.complete`,
:meth:`~ModelProvider.astream`, :meth:`~ModelProvider.structured`, and
:meth:`~ModelProvider.health`. Every adapter — OpenAI, Anthropic, or any
OpenAI-compatible endpoint — is substitutable for every other one, which is
what makes policy-driven routing and failover possible at all.

Contract every adapter must honour (spec §8):

* **Authentication through secret references.** An adapter receives an already
  resolved credential; it never reads configuration or the environment
  itself, so there is one place to audit secret access.
* **Timeouts on every phase.** Connect, read and write are bounded. An adapter
  must not be able to hang an execution.
* **Cancellation.** :meth:`complete` accepts a cancellation signal and must
  stop promptly, releasing the connection rather than leaving it in a pool.
* **Retryable classification.** Adapters raise *typed* errors
  (:mod:`mas_orchestrator.contracts.errors`) whose retryability is already
  correct. The retry layer never inspects provider payloads to decide.
* **Rate limits and 5xx** map to :class:`ModelRateLimitedError` and
  :class:`ModelProviderError` respectively, with any provider-supplied
  ``Retry-After`` preserved on the former.
* **Malformed responses** raise :class:`ModelProviderError`; they are never
  coerced into a partial success.
* **Usage extraction** happens only when the provider reports it. Otherwise
  ``usage`` is ``None``.
* **No credential logging.** Adapters route everything through the shared
  transport in :mod:`mas_orchestrator.providers.base_http`, which redacts.

Adapters must **not** implement retry or failover internally. Those belong to
:mod:`mas_orchestrator.models.retry` and
:mod:`mas_orchestrator.models.failover`, so that retry budgets and allowlists
are enforced uniformly regardless of which provider is in use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    ModelNotAvailableError,
    ModelProviderError,
)

if TYPE_CHECKING:
    import asyncio
    from collections.abc import AsyncIterator

    from mas_orchestrator.models.types import (
        ModelCapabilities,
        ModelRef,
        ModelRequest,
        ModelResponse,
        ModelStreamEvent,
    )

__all__ = [
    "ModelProvider",
    "capability_guard",
]


@runtime_checkable
class ModelProvider(Protocol):
    """A single model backend.

    Implementations are stateless with respect to conversation content: all
    context arrives in :class:`ModelRequest`. This keeps an adapter safe to
    share across concurrent executions, which matters because the orchestrator
    serves many tenants in one process.
    """

    @property
    def provider_id(self) -> str:
        """Stable provider identifier used in :class:`ModelRef` and config."""
        ...

    def supports(self, model: ModelRef) -> bool:
        """Whether this provider can serve *model*.

        Args:
            model: The candidate model reference.

        Returns:
            ``True`` when the provider owns the model. Must never raise: the
            registry calls this while enumerating candidates, and one
            misbehaving provider must not break routing for the rest.
        """
        ...

    def capabilities(self, model: ModelRef) -> ModelCapabilities:
        """Returns the capabilities of *model*.

        Raises:
            ModelNotAvailableError: If the model is unknown to this provider.
        """
        ...

    async def complete(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a completion to conclusion.

        Args:
            request: The typed request.
            timeout_seconds: Hard deadline for the whole call. Must be
                enforced by the adapter; the caller treats exceeding it as a
                bug even if the provider eventually returns.
            cancel_event: Optional cancellation signal. When set before or
                during the call, the adapter must abort and raise
                :class:`CancellationRequestedError`.

        Returns:
            The typed response.

        Raises:
            ModelRateLimitedError: The provider signalled rate limiting.
            ModelTimeoutError: The deadline elapsed.
            ModelProviderError: Any other provider-side failure, including a
                malformed response.
            CancellationRequestedError: Cancellation was observed.
        """
        ...

    def astream(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> AsyncIterator[ModelStreamEvent]:
        """Streams a completion incrementally.

        Args:
            request: The typed request.
            timeout_seconds: Hard deadline covering the whole stream, not
                just the first chunk. A stream that stops producing is a
                timeout, not a silent end.
            cancel_event: Optional cancellation signal.

        Yields:
            Incremental events. The final event has ``is_terminal=True`` and
            carries the finish reason and, when available, usage.

        Raises:
            ModelNotAvailableError: If the provider does not support
                streaming for this model. Refusing here is required: silently
                buffering the stream and yielding it as one event would
                change the caller's latency and cancellation behaviour.
            ModelTimeoutError: The deadline elapsed mid-stream.
            ModelProviderError: Any other provider-side failure.
        """
        ...

    async def structured(
        self,
        request: ModelRequest,
        *,
        timeout_seconds: float,
        cancel_event: asyncio.Event | None = None,
    ) -> ModelResponse:
        """Runs a completion constrained to the request's JSON schema.

        Adapters that do not have native schema support must **not** fake it
        by appending instructions to the prompt and hoping: that is a silent
        fallback, and it produces invalid JSON under load. They raise
        :class:`ModelNotAvailableError` so the router can pick a model that
        genuinely supports the capability.

        Args:
            request: The typed request. ``response_json_schema`` must be set.
            timeout_seconds: Hard deadline.
            cancel_event: Optional cancellation signal.

        Returns:
            The typed response, whose ``text`` is a JSON document.

        Raises:
            InvalidRequestError: If no JSON schema was supplied.
            ModelNotAvailableError: If the model cannot do schema-constrained
                output natively.
        """
        ...

    async def health(self) -> bool:
        """Returns whether the provider is currently reachable.

        Implementations must be cheap and must not consume quota. A provider
        whose health check costs tokens will distort its own cost reporting.
        """
        ...


def capability_guard(
    provider: ModelProvider,
    model: ModelRef,
    *,
    require: str,
) -> None:
    """Refuses a call the model cannot actually perform.

    This is the mechanism that stops a fallback from silently degrading a
    request into something the model does not support — for example routing a
    tool-using agent to a model without tool support, which would fail much
    later and much less clearly.

    Args:
        provider: The provider being asked.
        model: The model being asked.
        require: Name of a :class:`ModelCapabilities` field that must be true
            (``"supports_tools"``, ``"supports_structured_output"``,
            ``"supports_streaming"``).

    Raises:
        ConfigurationError: If ``require`` is not a known boolean capability.
            A typo in a capability name must fail loudly rather than silently
            skipping the check.
        ModelNotAvailableError: If the model lacks the capability.
    """
    valid = {"supports_tools", "supports_streaming", "supports_structured_output"}
    if require not in valid:
        raise ConfigurationError(
            "unknown capability requirement",
            details={"require": require, "valid": sorted(valid)},
        )

    capabilities = provider.capabilities(model)
    if not getattr(capabilities, require, False):
        raise ModelNotAvailableError(
            "model does not support the required capability",
            details={
                "model": model.key,
                "provider": provider.provider_id,
                "require": require,
            },
        )


def assert_streaming_supported(provider: ModelProvider, model: ModelRef) -> None:
    """Raises when *model* cannot stream, distinguishing it from other failures.

    Streaming is the one capability where a silent fallback (buffering into a
    single event) would be actively harmful, because it changes cancellation
    semantics. This helper keeps that refusal explicit and consistent.

    Raises:
        ModelNotAvailableError: If streaming is unsupported.
    """
    if not provider.capabilities(model).supports_streaming:
        raise ModelNotAvailableError(
            "model does not support streaming",
            details={"model": model.key, "provider": provider.provider_id},
        )


def raise_malformed(provider_id: str, detail: str) -> ModelProviderError:
    """Builds the canonical 'provider returned something we cannot parse' error.

    Args:
        provider_id: The provider that produced the payload.
        detail: What was wrong, already redacted by the caller. The raw
            provider body must never be passed here.

    Returns:
        A retryable :class:`ModelProviderError`. Retryable because a malformed
        response is frequently a truncated or error-page body from a
        transient upstream fault, and one more attempt often succeeds.
    """
    return ModelProviderError(
        f"{provider_id} returned a malformed response",
        details={"provider": provider_id, "reason": detail},
    )
