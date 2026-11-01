"""Tool executor.

One invocation flows through the following gates, in this order. Each gate can
only stop the invocation; none of them can skip a later one.

1. **Resolve** — the tool and its current version must exist for this tenant.
2. **Bound arguments** — the model's arguments must fit the size limit.
3. **Authorise** — the permission bridge must return ``ALLOW``. The model has
   no say in this.
4. **Validate** — arguments must satisfy the declared input schema.
5. **Execute** — under a timeout, with a bounded retry policy.
6. **Bound output** — the result must fit the output limit, or be truncated
   and explicitly marked as truncated.
7. **Classify** — the outcome is mapped to a stable failure class.

The result is always a :class:`ToolResult`. An exception is never allowed to
escape into the agent loop, because an unhandled tool exception would abort an
execution that could otherwise have continued with the failure reported.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from mas_orchestrator.contracts.errors import (
    CancellationRequestedError,
    ErrorCode,
    InvalidRequestError,
    ToolFailedError,
    ToolNotFoundError,
    ToolTimeoutError,
)
from mas_orchestrator.models.retry import RetryPolicy  # noqa: TC001 - dataclass field
from mas_orchestrator.tools.types import (
    MAX_TOOL_ARGUMENT_BYTES,
    ToolDefinition,
    ToolFailureClass,
    ToolInvocationContext,
    ToolResult,
    classify_error,
)
from mas_orchestrator.tools.validation import validate_arguments, validate_output

if TYPE_CHECKING:
    from collections.abc import Mapping

    from mas_orchestrator.contracts.errors import OrchestratorError
    from mas_orchestrator.tools.permissions import ToolPermissionBridge
    from mas_orchestrator.tools.registry import ToolRegistry

DEFAULT_TOOL_TIMEOUT_SECONDS: Final[float] = 30.0
MAX_OUTPUT_PREVIEW_CHARS: Final[int] = 512

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ToolExecutorConfig:
    """Executor-wide defaults.

    Attributes:
        default_timeout_seconds: Used when neither the tool nor the call
            specifies a timeout.
        max_timeout_seconds: Hard ceiling. A tool cannot ask for more than
            this, so a misconfigured tool cannot hold a worker indefinitely.
        retry_policy: Applied to retryable tool failures.
        default_output_bytes: Output bound for tools that declare none.
    """

    default_timeout_seconds: float = DEFAULT_TOOL_TIMEOUT_SECONDS
    max_timeout_seconds: float = 120.0
    retry_policy: RetryPolicy | None = None
    default_output_bytes: int = 4 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.default_timeout_seconds <= 0:
            raise InvalidRequestError(
                "tool timeout must be positive", details={"field": "default_timeout_seconds"}
            )
        if self.max_timeout_seconds < self.default_timeout_seconds:
            raise InvalidRequestError(
                "max timeout must not be below the default timeout",
                details={
                    "field": "max_timeout_seconds",
                    "default": self.default_timeout_seconds,
                },
            )
        if self.default_output_bytes <= 0:
            raise InvalidRequestError(
                "output bound must be positive", details={"field": "default_output_bytes"}
            )

    def resolve_timeout(self, tool: ToolDefinition, requested: float | None) -> float:
        """Picks a timeout and clamps it to the ceiling.

        Precedence: explicit call override, then the tool's own default, then
        the executor default — and never above :attr:`max_timeout_seconds`.
        """
        chosen = requested or tool.default_timeout_seconds or self.default_timeout_seconds
        if chosen <= 0:
            raise InvalidRequestError(
                "tool timeout must be positive", details={"field": "timeout_seconds"}
            )
        return min(chosen, self.max_timeout_seconds)


class ToolExecutor:
    """Executes tools for one execution, enforcing every gate above."""

    def __init__(
        self,
        registry: ToolRegistry,
        permissions: ToolPermissionBridge,
        *,
        config: ToolExecutorConfig | None = None,
        logger: logging.Logger | None = None,
        cancellation: asyncio.Event | None = None,
    ) -> None:
        self._registry = registry
        self._permissions = permissions
        self._config = config or ToolExecutorConfig()
        self._logger = logger if logger is not None else _LOGGER
        self._cancellation = cancellation
        self._invocation_count = 0

    @property
    def invocation_count(self) -> int:
        """How many invocations this executor has started."""
        return self._invocation_count

    async def invoke(
        self,
        tool_id: str,
        arguments: Any,
        context: ToolInvocationContext,
        *,
        version: int | None = None,
        timeout_seconds: float | None = None,
    ) -> ToolResult:
        """Runs one tool and returns a classified result.

        This method does not raise for tool failures: failures are returned as
        a :class:`ToolResult` with ``success=False``, so the agent loop can
        decide what to do with them.

        Args:
            tool_id: Tool to invoke.
            arguments: The model's arguments, already decoded from JSON.
            context: Invocation context carrying the authoritative tenant.
            version: Specific definition version. ``None`` uses the newest.
            timeout_seconds: Override for this call only.

        Returns:
            A :class:`ToolResult`. Never ``None``.
        """
        started = time.monotonic()
        invocation_id = context.invocation_id or f"inv-{uuid.uuid4().hex}"
        self._invocation_count += 1

        # --- Gate 1: resolve ---
        try:
            definition = self._registry.get(context.tenant_id, tool_id, version)
            handler = self._registry.handler(context.tenant_id, tool_id, definition.version.number)
        except ToolNotFoundError as error:
            return self._failure(
                invocation_id=invocation_id,
                tool_id=tool_id,
                version=version or 0,
                started=started,
                error=error,
            )

        # --- Gate 2: bound arguments ---
        try:
            encoded = json.dumps(arguments, default=str).encode("utf-8")
            if len(encoded) > MAX_TOOL_ARGUMENT_BYTES:
                raise InvalidRequestError(
                    "tool arguments exceed the maximum size",
                    details={"limit": MAX_TOOL_ARGUMENT_BYTES, "size": len(encoded)},
                )
        except (TypeError, ValueError) as error:
            return self._failure(
                invocation_id=invocation_id,
                tool_id=tool_id,
                version=definition.version.number,
                started=started,
                error=InvalidRequestError(
                    "tool arguments are not serialisable", details={"tool_id": tool_id}
                ),
                cause=error,
            )

        # --- Gate 3: authorise ---
        try:
            decision = self._authorise(definition, context)
        except Exception as error:
            return self._failure(
                invocation_id=invocation_id,
                tool_id=tool_id,
                version=definition.version.number,
                started=started,
                error=error,
            )

        # --- Gate 4: validate ---
        try:
            validated = validate_arguments(definition, arguments)
        except InvalidRequestError as error:
            return self._failure(
                invocation_id=invocation_id,
                tool_id=tool_id,
                version=definition.version.number,
                started=started,
                error=error,
            )

        # --- Gate 5: execute ---
        timeout = self._config.resolve_timeout(definition, timeout_seconds)
        policy = self._retry_policy_for(definition)

        try:
            if policy is None:
                output = await self._run_once(handler, validated, context, timeout)
            else:
                output = await policy.execute(
                    f"tool:{tool_id}",
                    lambda: self._run_once(handler, validated, context, timeout),
                    idempotent=definition.idempotent,
                    cancel_event=self._cancellation,
                )
        except Exception as error:
            return self._failure(
                invocation_id=invocation_id,
                tool_id=tool_id,
                version=definition.version.number,
                started=started,
                error=error,
            )

        # --- Gate 6: bound output ---
        limit = definition.output_limit_bytes or self._config.default_output_bytes
        try:
            bounded, truncated, size = _bound_output(output, limit)
            validate_output(definition, output if not truncated else bounded)
        except InvalidRequestError as error:
            return self._failure(
                invocation_id=invocation_id,
                tool_id=tool_id,
                version=definition.version.number,
                started=started,
                error=error,
            )

        duration_ms = int((time.monotonic() - started) * 1000)
        self._log(
            "tool.invoked",
            tool_id=tool_id,
            version=definition.version.number,
            invocation_id=invocation_id,
            decision_id=decision.decision_id,
            duration_ms=duration_ms,
            truncated=truncated,
            output_bytes=size,
        )

        return ToolResult(
            invocation_id=invocation_id,
            tool_id=tool_id,
            tool_version=definition.version.number,
            success=True,
            output=bounded,
            failure_class=ToolFailureClass.NONE,
            duration_ms=duration_ms,
            truncated=truncated,
            output_bytes=size,
        )

    async def _run_once(
        self,
        handler: Any,
        arguments: Mapping[str, object],
        context: ToolInvocationContext,
        timeout: float,
    ) -> Any:
        """Runs the handler once, under a timeout and with cancellation checked.

        Raises:
            ToolTimeoutError: If the handler does not finish in time.
            CancellationRequestedError: If cancellation was requested.
        """
        if self._cancellation is not None and self._cancellation.is_set():
            raise CancellationRequestedError("execution was cancelled before the tool ran")

        remaining = context.remaining_seconds()
        if remaining is not None:
            timeout = min(timeout, remaining)
            if timeout <= 0:
                raise CancellationRequestedError(
                    "execution deadline passed before the tool ran",
                    details={"tool_id": context.tool_call_id or ""},
                )

        try:
            return await asyncio.wait_for(handler.invoke(arguments, context), timeout=timeout)
        except TimeoutError as error:
            raise ToolTimeoutError(
                "tool did not finish within its timeout",
                details={"timeout_seconds": timeout},
            ) from error
        except asyncio.CancelledError as error:
            raise CancellationRequestedError("tool invocation was cancelled") from error

    def _authorise(
        self,
        definition: ToolDefinition,
        context: ToolInvocationContext,
    ) -> Any:
        """Runs the permission check.

        Imported lazily to keep the import graph acyclic: the permissions
        module needs only the types module, not the executor.
        """
        from mas_orchestrator.tools.permissions import enforce_permission

        return enforce_permission(definition, context, self._permissions)

    def _retry_policy_for(self, definition: ToolDefinition) -> RetryPolicy | None:
        """Whether this tool may be retried.

        A non-idempotent tool is never retried automatically, because a
        repeated invocation could duplicate a durable side effect. That is not
        a limitation to work around; it is the reason the flag exists.
        """
        if not definition.idempotent:
            return None
        return self._config.retry_policy

    def _failure(
        self,
        *,
        invocation_id: str,
        tool_id: str,
        version: int,
        started: float,
        error: BaseException,
        cause: BaseException | None = None,
    ) -> ToolResult:
        """Builds a failed result from an exception.

        The failure is always classified, never swallowed. An unrecognised
        exception becomes ``INTERNAL`` so that a bug in a tool is visible as a
        bug rather than being reported as a transient provider problem.
        """
        if cause is not None and not error.__cause__:
            error.__cause__ = cause

        failure_class = classify_error(error)
        code = getattr(error, "code", None)
        if code is not None:
            code_value = code.value
        else:
            # Not one of our errors at all. Reporting it as "tool_failed"
            # would file a genuine bug under the same heading as a tool that
            # simply returned an error, which is how crashes get ignored.
            code_value = (
                ErrorCode.INTERNAL_ERROR.value
                if failure_class is ToolFailureClass.INTERNAL
                else ErrorCode.TOOL_FAILED.value
            )

        self._log(
            "tool.failed",
            tool_id=tool_id,
            version=version,
            invocation_id=invocation_id,
            error_code=code_value,
            failure_class=failure_class.value,
        )

        return ToolResult(
            invocation_id=invocation_id,
            tool_id=tool_id,
            tool_version=version,
            success=False,
            error_code=code_value,
            error_message=str(error),
            failure_class=failure_class,
            duration_ms=int((time.monotonic() - started) * 1000),
        )

    def _log(self, event: str, **fields: object) -> None:
        """Emits a structured log line.

        ``extra`` is used rather than string interpolation so the fields stay
        machine-readable for whatever log pipeline consumes them, while the
        message itself remains a stable, greppable event name.
        """
        self._logger.info("%s", event, extra={"event": event, **fields})


def _bound_output(output: Any, limit: int) -> tuple[Any, bool, int]:
    """Caps the output at ``limit`` bytes.

    Returns:
        ``(value, truncated, original_size)``. When truncation is necessary the
        value is replaced with a marker object that states the limit and shows
        a preview, so a model can see that it received partial data instead of
        silently reasoning over a cut-off string.

    Raises:
        ToolFailedError: If the output cannot be serialised at all, since its
            size is then unknown and unbounded.
    """
    if output is None:
        return None, False, 0

    try:
        encoded = json.dumps(output)
        native = True
    except (TypeError, ValueError):
        # Not a JSON-native value. Fall back to stringifying it, so that what
        # we hand back is still JSON-safe.
        #
        # Broad catch on purpose: a custom __str__ can raise anything at all,
        # and the guarantee this function makes is that output we cannot
        # measure becomes a ToolFailedError rather than propagating whatever
        # the object happened to throw.
        try:
            encoded = json.dumps(output, default=str)
        except Exception as error:
            raise ToolFailedError(
                "tool output is not serialisable and cannot be bounded",
                details={
                    "tool_output_type": type(output).__name__,
                    "cause": type(error).__name__,
                },
            ) from error
        native = False

    size = len(encoded.encode("utf-8"))
    if size <= limit:
        if native:
            return output, False, size
        # A live Python object must not reach the result: ToolResult.output is
        # forwarded to Rust and into a model's context, and a value that only
        # exists in this process's memory can be neither.
        return json.loads(encoded), False, size

    if isinstance(output, str):
        preview = output[:MAX_OUTPUT_PREVIEW_CHARS]
    else:
        preview = encoded[:MAX_OUTPUT_PREVIEW_CHARS]

    truncated_value = {
        "truncated": True,
        "reason": "output exceeded the tool output limit",
        "limit_bytes": limit,
        "actual_bytes": size,
        "preview": preview,
    }
    return truncated_value, True, size


def failed_result(
    tool_id: str,
    error: OrchestratorError,
    *,
    invocation_id: str,
    version: int = 0,
) -> ToolResult:
    """Builds a failure result outside of the executor.

    Used when a failure is detected before the executor is reached — for
    example a tool call the router already knows is unauthorised.
    """
    return ToolResult(
        invocation_id=invocation_id,
        tool_id=tool_id,
        tool_version=version,
        success=False,
        error_code=error.code.value,
        error_message=str(error),
        failure_class=classify_error(error),
    )
