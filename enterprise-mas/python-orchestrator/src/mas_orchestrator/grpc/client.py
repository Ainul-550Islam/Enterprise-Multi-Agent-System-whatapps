"""Authenticated async Rust Core gRPC client.

This is the outbound half of the bridge: when the orchestrator needs to call
**back** into the Rust Core (to report an event, request a policy decision, or
transition execution state), it goes through :class:`RustCoreClient`.

What the client guarantees:

* **Deadlines are always attached.** A call without a deadline can hang forever
  and pin a worker; the client computes one from the configuration and from any
  remaining execution budget, and refuses to make the call if the budget is
  already exhausted.
* **Cancellation is propagated.** Every call accepts an
  :class:`asyncio.Event`-like cancellation signal via the caller's own task
  cancellation; there is no bespoke cancellation token that could be ignored.
* **Retries are bounded and classified.** Only errors whose domain code is
  retryable are retried, attempts are capped, and a total retry budget stops a
  slow dependency from consuming an execution's whole time allowance.
* **Metadata is propagated automatically.** Correlation, request and protocol
  metadata are attached from the bound execution context, so a Rust log line
  can always be joined to the AI work that produced it.
* **Tenancy is never sent as a header.** Scope travels in the typed body where
  Rust validates it against its own context.

What it does *not* do: it does not implement a competing idempotency system.
Duplicate protection for execution state is Rust's authoritative concern
(spec §16); this client only de-duplicates its own in-flight calls.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, TypeVar

import grpc

from mas_orchestrator.config.enums import Environment, TransportSecurity
from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    DependencyUnavailableError,
    ExecutionTimeoutError,
    is_retryable,
)
from mas_orchestrator.grpc.channel import ChannelConfig, ManagedChannel, create_channel
from mas_orchestrator.grpc.errors import error_from_grpc_status
from mas_orchestrator.grpc.metadata import (
    CORRELATION_HEADER,
    PROTOCOL_VERSION_HEADER,
    REQUEST_ID_HEADER,
    build_outbound_metadata,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from mas_orchestrator.context.execution_context import ExecutionContext
    from mas_orchestrator.security.credentials import Secret
    from mas_orchestrator.security.identity import ServiceIdentity

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

T = TypeVar("T")

#: Default upper bound on a single outbound call.
DEFAULT_REQUEST_TIMEOUT_SECONDS: Final[float] = 10.0

#: Floor for the computed deadline: below this, a call cannot meaningfully
#: complete, so it is refused rather than attempted.
MINIMUM_USABLE_TIMEOUT_SECONDS: Final[float] = 0.05


@dataclass(frozen=True, slots=True)
class RustCoreClientConfig:
    """Configuration for :class:`RustCoreClient`.

    Attributes:
        endpoint: ``host:port`` of the Rust Core gRPC surface.
        transport: Transport security mode.
        ca_bundle_path: Optional PEM root bundle.
        client_certificate_path: Client certificate for mTLS.
        client_key_path: Client key for mTLS.
        request_timeout_seconds: Per-call deadline.
        total_timeout_seconds: Upper bound across all attempts of one call.
        max_attempts: Maximum attempts, including the first.
        keepalive_interval_seconds: Idle ping interval.
        backoff_base_seconds: Base for exponential backoff.
        backoff_max_seconds: Cap for a single backoff sleep.
        honour_retry_after: Whether to honour a provider-suggested delay when
            the error carries one.
    """

    endpoint: str = "127.0.0.1:50051"
    transport: TransportSecurity = TransportSecurity.TLS
    ca_bundle_path: str | None = None
    client_certificate_path: str | None = None
    client_key_path: str | None = None
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    total_timeout_seconds: float = 30.0
    max_attempts: int = 3
    keepalive_interval_seconds: float = 30.0
    backoff_base_seconds: float = 0.25
    backoff_max_seconds: float = 4.0
    honour_retry_after: bool = True

    def __post_init__(self) -> None:
        """Validates the configuration.

        Raises:
            ConfigurationError: If attempts or timeouts are nonsensical. The
                check that matters most is ``total >= request``: a total budget
                smaller than one attempt would guarantee a timeout on the first
                call, which is always a configuration mistake.
        """
        if self.max_attempts < 1:
            raise ConfigurationError(
                "max_attempts must be at least 1",
                details={"max_attempts": self.max_attempts},
            )
        if self.request_timeout_seconds <= 0:
            raise ConfigurationError(
                "request_timeout_seconds must be positive",
                details={"request_timeout_seconds": self.request_timeout_seconds},
            )
        if self.total_timeout_seconds < self.request_timeout_seconds:
            raise ConfigurationError(
                "total_timeout_seconds must be at least request_timeout_seconds",
                details={
                    "total_timeout_seconds": self.total_timeout_seconds,
                    "request_timeout_seconds": self.request_timeout_seconds,
                },
            )
        if self.backoff_base_seconds <= 0:
            raise ConfigurationError(
                "backoff_base_seconds must be positive",
                details={"backoff_base_seconds": self.backoff_base_seconds},
            )


class RustCoreClient:
    """Authenticated, deadline-aware client for the Rust Core gRPC surface.

    Args:
        config: Client configuration.
        environment: Running environment; gates insecure transport.
        credential: Optional bearer credential presented on every call.
        channel: Optional pre-built channel, primarily for tests. When
            omitted, :meth:`start` must be called before use.

    Typical lifecycle::

        client = RustCoreClient(config, environment=env, credential=secret)
        await client.start()
        try:
            await client.call("PublishEvent", stub.PublishEvent, request)
        finally:
            await client.aclose()
    """

    def __init__(
        self,
        config: RustCoreClientConfig,
        *,
        environment: Environment = Environment.PRODUCTION,
        credential: Secret | None = None,
        channel: ManagedChannel | None = None,
    ) -> None:
        self._config = config
        self._environment = environment
        self._credential = credential
        self._managed: ManagedChannel | None = channel
        self._owns_channel = channel is None
        self._started = channel is not None

    @property
    def is_started(self) -> bool:
        """Whether the client has a usable channel."""
        return self._started and self._managed is not None

    async def start(self, *, wait_for_ready: bool = False) -> None:
        """Creates the channel and optionally waits for it to become ready.

        Args:
            wait_for_ready: When ``True``, block until the channel is ready or
                the configured connect timeout elapses. Enable at startup when
                the service should not accept work until Rust is reachable;
                leave disabled when a brief startup race is acceptable.

        Raises:
            DependencyUnavailableError: If ``wait_for_ready`` is set and the
                channel did not become ready in time.
        """
        if self._started:
            return

        channel_config = ChannelConfig(
            endpoint=self._config.endpoint,
            transport=self._config.transport,
            ca_bundle_path=self._config.ca_bundle_path,
            client_certificate_path=self._config.client_certificate_path,
            client_key_path=self._config.client_key_path,
            keepalive_interval_seconds=self._config.keepalive_interval_seconds,
            connect_timeout_seconds=min(5.0, self._config.total_timeout_seconds),
        )
        self._managed = ManagedChannel(
            create_channel(channel_config, environment=self._environment),
            grace_seconds=10.0,
        )
        self._started = True

        if wait_for_ready:
            try:
                await self._managed.wait_until_ready(channel_config.connect_timeout_seconds)
            except DependencyUnavailableError:
                await self._close_owned_channel()
                raise

    async def aclose(self) -> None:
        """Closes the channel if this client created it.

        Safe to call more than once and safe to call when never started.
        """
        await self._close_owned_channel()

    async def _close_owned_channel(self) -> None:
        """Closes the channel only when this client owns it."""
        if self._managed is not None and self._owns_channel:
            await self._managed.close()
        self._started = False

    def _require_started(self) -> ManagedChannel:
        """Returns the managed channel, raising when the client was not started.

        Raises:
            DependencyUnavailableError: If :meth:`start` was never called. This
                is a dependency failure rather than a configuration error
                because the caller has done nothing wrong; the client is simply
                not connected yet.
        """
        if self._managed is None or not self._started:
            raise DependencyUnavailableError(
                "rust core client has not been started",
                details={"reason": "not_started"},
            )
        return self._managed

    def _metadata(
        self,
        context: ExecutionContext | None,
        *,
        extra: dict[str, str] | None = None,
    ) -> list[tuple[str, str]]:
        """Builds outbound metadata for a call.

        Args:
            context: The bound execution context, if any. Correlation and
                request identifiers are taken from it so Rust can join logs.
            extra: Additional metadata pairs to append.

        Note:
            Tenant/organization headers are deliberately never set here; see
            :mod:`mas_orchestrator.grpc.metadata`.
        """
        pairs = build_outbound_metadata(
            correlation_id=context.correlation_id if context else None,
            request_id=context.request_id if context else None,
            protocol_version=str(context.protocol_version) if context else None,
            credential=self._credential.reveal() if self._credential else None,
        )
        if extra:
            pairs.extend((key, value) for key, value in extra.items() if value)
        return pairs

    async def call(
        self,
        operation: str,
        method: Callable[[Any, float | None, list[tuple[str, str]]], Awaitable[T]],
        request: Any,
        *,
        context: ExecutionContext | None = None,
        timeout_seconds: float | None = None,
        extra_metadata: dict[str, str] | None = None,
    ) -> T:
        """Invokes a Rust Core RPC with deadline, retry and classification.

        Args:
            operation: Human-readable operation name, used in errors and logs.
            method: A callable that performs the RPC and accepts
                ``(request, timeout, metadata)``. This indirection keeps the
                client free of any dependency on generated stub types.
            request: The request message.
            context: Execution context supplying correlation metadata and any
                remaining deadline budget.
            timeout_seconds: Explicit per-call deadline. When omitted, the
                configured ``request_timeout_seconds`` is used, clamped by the
                context's remaining deadline when one applies.
            extra_metadata: Additional metadata pairs.

        Returns:
            The RPC response.

        Raises:
            ExecutionTimeoutError: If the execution's remaining budget is
                already exhausted, or is too small to be usable. No call is
                made in that case: attempting one would only consume the
                remainder and fail.
            DependencyUnavailableError: If the client was not started.
            OrchestratorError: The classified failure. Retryable failures are
                retried up to ``max_attempts`` within ``total_timeout_seconds``;
                the last error is raised if all attempts fail.
        """
        managed = self._require_started()
        del managed  # the channel is reached through `method`

        metadata = self._metadata(context, extra=extra_metadata)
        effective_timeout = self._effective_timeout(context, explicit=timeout_seconds)

        if effective_timeout < MINIMUM_USABLE_TIMEOUT_SECONDS:
            raise ExecutionTimeoutError(
                f"no usable deadline remains for {operation}",
                details={
                    "operation": operation,
                    "remaining_seconds": effective_timeout,
                },
                correlation_id=context.correlation_id if context else None,
                request_id=context.request_id if context else None,
            )

        last_error: BaseException | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            try:
                return await method(request, effective_timeout, metadata)
            except asyncio.CancelledError:
                # Never retry cancellation, and never swallow it: the caller's
                # task is being cancelled and must observe that.
                raise
            except grpc.aio.AioRpcError as error:
                typed = self._classify(error, operation, context)
                last_error = typed
                if not is_retryable(typed) or attempt == self._config.max_attempts:
                    raise typed from error
                await self._sleep_before_retry(attempt, typed)
            except TimeoutError as error:
                typed_timeout = ExecutionTimeoutError(
                    f"{operation} exceeded its deadline",
                    details={"operation": operation, "timeout_seconds": effective_timeout},
                    correlation_id=context.correlation_id if context else None,
                    request_id=context.request_id if context else None,
                )
                last_error = typed_timeout
                if attempt == self._config.max_attempts:
                    raise typed_timeout from error
                await self._sleep_before_retry(attempt, typed_timeout)

        # Every loop exit either returns or raises; this is the type checker's
        # guarantee that `last_error` is always bound when reached.
        raise (
            last_error
            if last_error is not None
            else DependencyUnavailableError(
                f"{operation} did not produce a result",
                details={"operation": operation},
            )
        )

    def _effective_timeout(
        self,
        context: ExecutionContext | None,
        *,
        explicit: float | None,
    ) -> float:
        """Computes the deadline for one call.

        The rules, in order: an explicit override wins; otherwise the
        configured per-call timeout applies; in both cases the result is
        clamped by the execution's remaining budget so a call can never outlive
        the execution it serves.
        """
        configured = explicit if explicit is not None else self._config.request_timeout_seconds
        if context is None:
            return min(configured, self._config.total_timeout_seconds)

        remaining = context.remaining_seconds()
        if remaining is None:
            return min(configured, self._config.total_timeout_seconds)
        return max(0.0, min(configured, remaining, self._config.total_timeout_seconds))

    def _classify(
        self,
        error: grpc.aio.AioRpcError,
        operation: str,
        context: ExecutionContext | None,
    ) -> BaseException:
        """Maps a gRPC error onto a typed domain error with context attached."""
        return error_from_grpc_status(
            error,
            correlation_id=context.correlation_id if context else None,
            request_id=context.request_id if context else None,
        )

    async def _sleep_before_retry(self, attempt: int, error: BaseException) -> None:
        """Sleeps with exponential backoff and full jitter before a retry.

        Full jitter (a uniform draw over the whole interval) is used rather
        than "decorrelated" variants because it is simple, well understood, and
        provably spreads retries for simultaneous callers.

        Args:
            attempt: The attempt number just failed (1-based).
            error: The classified error, consulted for a retry hint.
        """
        delay = self._backoff_delay(attempt)

        hint = getattr(error, "retry_after_seconds", None)
        if self._config.honour_retry_after and isinstance(hint, int | float) and hint > 0:
            # Honour the peer's hint, but never let it exceed our own cap:
            # a hostile or buggy peer must not be able to park us indefinitely.
            delay = min(delay + float(hint), self._config.backoff_max_seconds)

        _LOGGER.warning(
            "rust core call failed; retrying",
            extra={
                "attempt": attempt,
                "delay_seconds": round(delay, 3),
                "error_code": getattr(error, "code", "unknown"),
            },
        )
        await asyncio.sleep(delay)

    def _backoff_delay(self, attempt: int) -> float:
        """Exponential backoff with full jitter, capped by configuration."""
        ceiling = min(
            self._config.backoff_max_seconds,
            self._config.backoff_base_seconds * (2 ** (attempt - 1)),
        )
        return random.uniform(0.0, ceiling)  # noqa: S311 - not security-sensitive


def client_from_settings(
    settings: Any,
    *,
    credential: Secret | None = None,
) -> RustCoreClient:
    """Builds a :class:`RustCoreClient` from the application settings object.

    Args:
        settings: The application settings, expected to expose ``rust_core``
            and ``environment`` attributes matching the project's settings
            models. Typed as ``Any`` to avoid importing the settings module
            here; a missing attribute is reported as a configuration error.
        credential: Optional resolved service credential.

    Returns:
        A configured client. The channel is not created until
        :meth:`RustCoreClient.start` is called.

    Raises:
        ConfigurationError: If the settings object does not expose the
            expected Rust Core configuration.
    """
    rust = getattr(settings, "rust_core", None)
    if rust is None:
        raise ConfigurationError(
            "settings do not expose a rust_core section",
            details={"reason": "missing_section"},
        )

    environment = getattr(settings, "environment", Environment.PRODUCTION)
    return RustCoreClient(
        RustCoreClientConfig(
            endpoint=rust.endpoint,
            transport=rust.transport,
            ca_bundle_path=rust.ca_bundle_path,
            client_certificate_path=rust.client_cert_secret_ref,
            client_key_path=rust.client_key_secret_ref,
            request_timeout_seconds=rust.request_timeout_seconds,
            total_timeout_seconds=rust.total_timeout_seconds,
            max_attempts=rust.max_attempts,
            keepalive_interval_seconds=rust.keepalive_interval_seconds,
        ),
        environment=environment,
        credential=credential,
    )


def identity_metadata(identity: ServiceIdentity) -> list[tuple[str, str]]:
    """Metadata describing the *local* service identity.

    Used when the orchestrator registers itself with Rust. The subject is
    included so Rust can log which instance it is talking to; no credential
    material is ever placed in metadata by this helper.

    Args:
        identity: The local service identity.

    Returns:
        Metadata pairs. Empty when the identity has no subject.
    """
    if not identity.subject:
        return []
    return [("x-mas-service-subject", identity.subject)]


def correlation_headers_for_logging(metadata: list[tuple[str, str]]) -> dict[str, str]:
    """Extracts just the propagation headers from metadata, for logging.

    Args:
        metadata: Outbound metadata pairs.

    Returns:
        A dict containing only the correlation/request/protocol headers. Any
        ``authorization`` entry is deliberately excluded so this helper is safe
        to call from a logging path.
    """
    wanted = {CORRELATION_HEADER, REQUEST_ID_HEADER, PROTOCOL_VERSION_HEADER}
    return {key: value for key, value in metadata if key in wanted}
