"""Unit tests for the tool layer.

The properties under test are the ones from spec §10: every invocation has an
id, a version, a validated schema, a permission decision, a timeout, a bounded
output and a failure classification — and none of those can be skipped by the
model.
"""

from __future__ import annotations

import asyncio
from typing import ClassVar

import pytest

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    ToolFailedError,
    ToolPermissionDeniedError,
    ToolTimeoutError,
)
from mas_orchestrator.models.retry import RetryPolicy
from mas_orchestrator.tools.executor import ToolExecutor, ToolExecutorConfig, failed_result
from mas_orchestrator.tools.permissions import (
    AllowListPermissionBridge,
    DenyAllPermissionBridge,
    PermissionDecision,
    PermissionResult,
    enforce_permission,
)
from mas_orchestrator.tools.registry import ToolRegistry, bump
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolFailureClass,
    ToolInvocationContext,
    ToolKind,
    ToolResult,
    ToolSafety,
    ToolStatus,
    ToolVersion,
    classify_error,
)
from mas_orchestrator.tools.validation import (
    describe_unsupported_keywords,
    validate_arguments,
    validate_output,
)

TENANT = "tenant-1"
OTHER_TENANT = "tenant-2"


def _definition(
    *,
    tool_id: str = "tool-1",
    tenant_id: str = TENANT,
    version: int = 1,
    schema: dict[str, object] | None = None,
    status: ToolStatus = ToolStatus.PUBLISHED,
    safety: ToolSafety = ToolSafety.STANDARD,
    idempotent: bool = False,
    timeout: float | None = None,
    max_output_bytes: int | None = None,
) -> ToolDefinition:
    return ToolDefinition(
        tool_id=tool_id,
        name="demo",
        version=ToolVersion(version),
        kind=ToolKind.INTERNAL,
        safety=safety,
        status=status,
        input_schema=schema if schema is not None else {"type": "object"},
        tenant_id=tenant_id,
        default_timeout_seconds=timeout,
        max_output_bytes=max_output_bytes,
        idempotent=idempotent,
    )


def _context(**overrides: object) -> ToolInvocationContext:
    base: dict[str, object] = {
        "tenant_id": TENANT,
        "execution_id": "exec-1",
        "agent_id": "agent-1",
        "invocation_id": "inv-1",
    }
    base.update(overrides)
    return ToolInvocationContext(**base)  # type: ignore[arg-type]


class _Echo:
    """A handler that returns whatever it is given."""

    def __init__(self, result: object = "ok", delay: float = 0.0) -> None:
        self._result = result
        self._delay = delay
        self.calls = 0

    async def invoke(self, arguments: object, context: object) -> object:
        self.calls += 1
        if self._delay:
            await asyncio.sleep(self._delay)
        return self._result


class _Boom:
    """A handler that always fails."""

    def __init__(self, error: Exception) -> None:
        self._error = error
        self.calls = 0

    async def invoke(self, arguments: object, context: object) -> object:
        self.calls += 1
        raise self._error


class _Bridge:
    """A permission bridge with a fixed answer."""

    def __init__(
        self,
        decision: PermissionDecision = PermissionDecision.ALLOW,
        reason: str = "test",
    ) -> None:
        self._decision = decision
        self._reason = reason
        self.calls = 0

    def check(self, tool: ToolDefinition, context: ToolInvocationContext) -> PermissionResult:
        self.calls += 1
        return PermissionResult(
            decision=self._decision,
            tool_id=tool.tool_id,
            tool_version=tool.version.number,
            reason=self._reason,
            decision_id="dec-1",
            source="test",
        )


def _registry(handler: object, definition: ToolDefinition | None = None) -> ToolRegistry:
    registry = ToolRegistry(environment=Environment.DEVELOPMENT)
    registry.register(definition or _definition(), handler)  # type: ignore[arg-type]
    return registry


class TestDefinitionValidation:
    """A tool must be well-formed before it can be registered."""

    def test_valid(self) -> None:
        assert _definition().tool_id == "tool-1"

    def test_empty_id_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            _definition(tool_id=" ")

    def test_missing_tenant_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            _definition(tenant_id="")

    def test_oversized_name_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            ToolDefinition(
                tool_id="t",
                name="x" * 200,
                version=ToolVersion(1),
                kind=ToolKind.INTERNAL,
                tenant_id=TENANT,
            )

    def test_zero_version_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            ToolVersion(0)

    def test_non_object_schema_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            _definition(schema=["not", "an", "object"])  # type: ignore[arg-type]

    def test_oversized_schema_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            _definition(schema={"type": "object", "description": "x" * 300_000})

    def test_negative_timeout_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            _definition(timeout=-1)

    def test_output_limit_defaults_to_global(self) -> None:
        assert _definition().output_limit_bytes == 4 * 1024 * 1024


class TestStatus:
    """Lifecycle gating."""

    def test_published_is_invokable(self) -> None:
        assert _definition(status=ToolStatus.PUBLISHED).is_invokable is True

    def test_draft_is_not_invokable(self) -> None:
        """An unreviewed tool must not serve traffic."""
        assert _definition(status=ToolStatus.DRAFT).is_invokable is False

    def test_disabled_is_not_invokable(self) -> None:
        assert _definition(status=ToolStatus.DISABLED).is_invokable is False

    def test_deprecated_still_invokable(self) -> None:
        """Deprecated warns, it does not break running executions."""
        assert _definition(status=ToolStatus.DEPRECATED).is_invokable is True


class TestPermissionEnforcement:
    """The model cannot authorise its own calls."""

    def test_denied_status_is_refused_before_the_bridge(self) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            enforce_permission(_definition(status=ToolStatus.DRAFT), _context(), _Bridge())

    def test_cross_tenant_call_is_refused(self) -> None:
        """Even when the bridge would have allowed it."""
        with pytest.raises(ToolPermissionDeniedError) as excinfo:
            enforce_permission(_definition(tenant_id=OTHER_TENANT), _context(), _Bridge())
        assert "tenant" in str(excinfo.value)

    def test_deny_is_an_error(self) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            enforce_permission(_definition(), _context(), _Bridge(PermissionDecision.DENY))

    def test_require_approval_is_not_an_allow(self) -> None:
        """A tool awaiting human approval must not run."""
        with pytest.raises(ToolPermissionDeniedError):
            enforce_permission(
                _definition(), _context(), _Bridge(PermissionDecision.REQUIRE_APPROVAL)
            )

    def test_allow_passes_and_returns_the_decision(self) -> None:
        result = enforce_permission(_definition(), _context(), _Bridge())
        assert result.is_allowed is True
        assert result.decision_id == "dec-1"

    def test_deny_all_bridge_denies_everything(self) -> None:
        with pytest.raises(ToolPermissionDeniedError):
            enforce_permission(_definition(), _context(), DenyAllPermissionBridge())

    def test_allowlist_requires_the_tenant_pair(self) -> None:
        bridge = AllowListPermissionBridge(
            {(TENANT, "tool-1")}, environment=Environment.DEVELOPMENT
        )
        assert enforce_permission(_definition(), _context(), bridge).is_allowed is True

        with pytest.raises(ToolPermissionDeniedError):
            enforce_permission(_definition(), _context(tenant_id=OTHER_TENANT), bridge)

    def test_allowlist_refuses_production(self) -> None:
        """A permissive bridge must not be selectable in production."""
        with pytest.raises(ConfigurationError):
            AllowListPermissionBridge({(TENANT, "tool-1")}, environment=Environment.PRODUCTION)

    def test_sensitive_tool_needs_explicit_approval(self) -> None:
        bridge = AllowListPermissionBridge(
            {(TENANT, "tool-1")}, environment=Environment.DEVELOPMENT
        )
        with pytest.raises(ToolPermissionDeniedError):
            enforce_permission(_definition(safety=ToolSafety.SENSITIVE), _context(), bridge)


class TestArgumentValidation:
    """Schema validation."""

    SCHEMA: ClassVar[dict[str, object]] = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "query": {"type": "string", "minLength": 1, "maxLength": 10},
            "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        },
        "required": ["query"],
    }

    def _tool(self) -> ToolDefinition:
        return _definition(schema=self.SCHEMA)

    def test_valid_arguments(self) -> None:
        assert validate_arguments(self._tool(), {"query": "hi"})["query"] == "hi"

    def test_not_an_object(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), "query")

    def test_missing_required(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), {})

    def test_wrong_type(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), {"query": 5})

    def test_below_minimum(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), {"query": "hi", "limit": 0})

    def test_above_maximum(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), {"query": "hi", "limit": 1000})

    def test_string_too_long(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), {"query": "x" * 50})

    def test_unknown_property(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), {"query": "hi", "evil": True})

    def test_enum(self) -> None:
        tool = _definition(schema={"type": "object", "properties": {"mode": {"enum": ["a", "b"]}}})
        assert validate_arguments(tool, {"mode": "a"}) == {"mode": "a"}
        with pytest.raises(InvalidRequestError):
            validate_arguments(tool, {"mode": "c"})

    def test_boolean_is_not_an_integer(self) -> None:
        """Python's bool is an int; JSON Schema disagrees."""
        tool = _definition(schema={"type": "object", "properties": {"n": {"type": "integer"}}})
        with pytest.raises(InvalidRequestError):
            validate_arguments(tool, {"n": True})

    def test_oversized_arguments(self) -> None:
        with pytest.raises(InvalidRequestError):
            validate_arguments(self._tool(), {"query": "hi", "blob": "x" * 300_000})

    def test_output_schema_checked_when_declared(self) -> None:
        tool = ToolDefinition(
            tool_id="t",
            name="n",
            version=ToolVersion(1),
            kind=ToolKind.INTERNAL,
            tenant_id=TENANT,
            output_schema={"type": "object", "properties": {"v": {"type": "integer"}}},
        )
        assert validate_output(tool, {"v": 1}) == {"v": 1}
        with pytest.raises(InvalidRequestError):
            validate_output(tool, {"v": "one"})

    def test_unsupported_keywords_are_reported(self) -> None:
        """Silently ignoring 'pattern' would overstate what we enforce."""
        assert "pattern" in describe_unsupported_keywords({"pattern": "^a$"})


class TestExecutorGates:
    """Each gate must fire when it should."""

    @pytest.mark.asyncio
    async def test_unknown_tool_fails_classified(self) -> None:
        executor = ToolExecutor(
            ToolRegistry(environment=Environment.DEVELOPMENT),
            DenyAllPermissionBridge(),
        )
        result = await executor.invoke("nope", {}, _context())
        assert result.success is False
        assert result.failure_class is ToolFailureClass.PERMANENT

    @pytest.mark.asyncio
    async def test_denied_tool_never_reaches_the_handler(self) -> None:
        handler = _Echo()
        executor = ToolExecutor(_registry(handler), DenyAllPermissionBridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.success is False
        assert result.failure_class is ToolFailureClass.PERMISSION
        assert handler.calls == 0

    @pytest.mark.asyncio
    async def test_invalid_arguments_never_reach_the_handler(self) -> None:
        handler = _Echo()
        schema = {
            "type": "object",
            "required": ["need"],
            "properties": {"need": {"type": "string"}},
        }
        executor = ToolExecutor(_registry(handler, _definition(schema=schema)), _Bridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.success is False
        assert result.failure_class is ToolFailureClass.SCHEMA
        assert handler.calls == 0

    @pytest.mark.asyncio
    async def test_success_carries_the_serving_version(self) -> None:
        executor = ToolExecutor(_registry(_Echo(), _definition(version=7)), _Bridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.success is True
        assert result.tool_version == 7
        assert result.output == "ok"

    @pytest.mark.asyncio
    async def test_invocation_id_is_recorded(self) -> None:
        executor = ToolExecutor(_registry(_Echo()), _Bridge())
        result = await executor.invoke("tool-1", {}, _context(invocation_id="inv-xyz"))
        assert result.invocation_id == "inv-xyz"

    @pytest.mark.asyncio
    async def test_output_is_marked_untrusted(self) -> None:
        """Provenance must survive all the way to the model."""
        executor = ToolExecutor(_registry(_Echo()), _Bridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.untrusted is True
        assert "untrusted" in result.to_model_content()


class TestExecutorTimeoutAndRetry:
    """Time bounds and retry discipline."""

    @pytest.mark.asyncio
    async def test_timeout_is_classified(self) -> None:
        executor = ToolExecutor(
            _registry(_Echo(delay=1.0), _definition(timeout=0.02)),
            _Bridge(),
            config=ToolExecutorConfig(default_timeout_seconds=0.02),
        )
        result = await executor.invoke("tool-1", {}, _context())
        assert result.success is False
        assert result.failure_class is ToolFailureClass.TIMEOUT

    @pytest.mark.asyncio
    async def test_non_idempotent_tool_is_never_retried(self) -> None:
        handler = _Boom(ToolTimeoutError("slow"))
        executor = ToolExecutor(
            _registry(handler, _definition(idempotent=False)),
            _Bridge(),
            config=ToolExecutorConfig(
                retry_policy=RetryPolicy(
                    max_attempts=3, budget_seconds=5.0, backoff_base_seconds=0.001
                )
            ),
        )
        await executor.invoke("tool-1", {}, _context())
        assert handler.calls == 1

    @pytest.mark.asyncio
    async def test_idempotent_tool_is_retried(self) -> None:
        handler = _Boom(ToolTimeoutError("slow"))
        executor = ToolExecutor(
            _registry(handler, _definition(idempotent=True)),
            _Bridge(),
            config=ToolExecutorConfig(
                retry_policy=RetryPolicy(
                    max_attempts=3, budget_seconds=5.0, backoff_base_seconds=0.001
                )
            ),
        )
        result = await executor.invoke("tool-1", {}, _context())
        assert handler.calls == 3
        assert result.failure_class is ToolFailureClass.TIMEOUT

    @pytest.mark.asyncio
    async def test_deadline_already_passed(self) -> None:
        import time

        executor = ToolExecutor(_registry(_Echo()), _Bridge())
        context = _context(deadline_monotonic=time.monotonic() - 1)
        result = await executor.invoke("tool-1", {}, context)
        assert result.success is False
        assert result.failure_class is ToolFailureClass.PERMANENT

    @pytest.mark.asyncio
    async def test_cancellation_before_the_run(self) -> None:
        event = asyncio.Event()
        event.set()
        executor = ToolExecutor(_registry(_Echo()), _Bridge(), cancellation=event)
        result = await executor.invoke("tool-1", {}, _context())
        assert result.success is False


class TestBoundedOutput:
    """Output must never exceed the bound."""

    @pytest.mark.asyncio
    async def test_oversized_output_is_truncated_not_returned_whole(self) -> None:
        handler = _Echo("x" * 100_000)
        executor = ToolExecutor(_registry(handler, _definition(max_output_bytes=1_000)), _Bridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.truncated is True
        assert result.output_bytes > 1_000
        assert "preview" in result.output

    @pytest.mark.asyncio
    async def test_small_output_is_not_truncated(self) -> None:
        executor = ToolExecutor(_registry(_Echo("small")), _Bridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.truncated is False
        assert result.output == "small"

    def test_unmeasurable_output_is_a_failure(self) -> None:
        """If we cannot measure the output we cannot bound it."""
        from mas_orchestrator.tools.executor import _bound_output

        class _Unstringifiable:
            def __str__(self) -> str:
                raise RuntimeError("cannot be represented")

        with pytest.raises(ToolFailedError):
            _bound_output(_Unstringifiable(), 1024)

    @pytest.mark.asyncio
    async def test_exotic_but_stringifiable_output_survives(self) -> None:
        """A tool may return anything repr-able; it is stringified, not dropped."""
        executor = ToolExecutor(_registry(_Echo(object())), _Bridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.success is True
        assert isinstance(result.output, str)


class TestFailureClassification:
    """Mapping errors to stable classes."""

    def test_timeout(self) -> None:
        assert classify_error(ToolTimeoutError("x")) is ToolFailureClass.TIMEOUT

    def test_permission(self) -> None:
        assert classify_error(ToolPermissionDeniedError("x")) is ToolFailureClass.PERMISSION

    def test_not_found_is_permanent(self) -> None:
        from mas_orchestrator.contracts.errors import ToolNotFoundError

        assert classify_error(ToolNotFoundError("x")) is ToolFailureClass.PERMANENT

    def test_unknown_exception_is_internal(self) -> None:
        """Never assume an arbitrary exception is transient."""
        assert classify_error(RuntimeError("boom")) is ToolFailureClass.INTERNAL

    def test_only_retryable_is_retryable(self) -> None:
        assert ToolFailureClass.RETRYABLE.is_retryable is True
        assert ToolFailureClass.PERMANENT.is_retryable is False

    @pytest.mark.asyncio
    async def test_tool_exception_becomes_a_result(self) -> None:
        """An exception must not escape into the agent loop."""
        executor = ToolExecutor(_registry(_Boom(RuntimeError("boom"))), _Bridge())
        result = await executor.invoke("tool-1", {}, _context())
        assert result.success is False
        assert result.failure_class is ToolFailureClass.INTERNAL
        assert result.error_code == "internal_error"

    def test_result_rejects_contradictory_state(self) -> None:
        with pytest.raises(InvalidRequestError):
            ToolResult(
                invocation_id="i",
                tool_id="t",
                tool_version=1,
                success=True,
                error_code="boom",
            )
        with pytest.raises(InvalidRequestError):
            ToolResult(
                invocation_id="i",
                tool_id="t",
                tool_version=1,
                success=False,
            )


class TestFailedResultHelper:
    """Building a failure outside the executor."""

    def test_classifies_the_error(self) -> None:
        result = failed_result("tool-1", ToolPermissionDeniedError("no"), invocation_id="inv-1")
        assert result.failure_class is ToolFailureClass.PERMISSION
        assert result.tool_id == "tool-1"


class TestRegistry:
    """Version resolution and tenant scoping."""

    def test_newest_version_is_current(self) -> None:
        registry = ToolRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_definition(version=1), _Echo())
        registry.register(_definition(version=2), _Echo())
        assert registry.get(TENANT, "tool-1").version.number == 2

    def test_specific_version_is_retrievable(self) -> None:
        registry = ToolRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_definition(version=1), _Echo())
        registry.register(_definition(version=2), _Echo())
        assert registry.get(TENANT, "tool-1", version=1).version.number == 1

    def test_duplicate_version_refused(self) -> None:
        registry = ToolRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_definition(version=1), _Echo())
        with pytest.raises(ConfigurationError):
            registry.register(_definition(version=1), _Echo())

    def test_other_tenants_tools_are_invisible(self) -> None:
        registry = ToolRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_definition(tenant_id=OTHER_TENANT), _Echo())
        from mas_orchestrator.contracts.errors import ToolNotFoundError

        with pytest.raises(ToolNotFoundError):
            registry.get(TENANT, "tool-1")

    def test_list_tools_scoped_to_tenant(self) -> None:
        registry = ToolRegistry(environment=Environment.DEVELOPMENT)
        registry.register(_definition(tool_id="a"), _Echo())
        registry.register(_definition(tool_id="b", tenant_id=OTHER_TENANT), _Echo())
        assert [t.tool_id for t in registry.list_tools(TENANT)] == ["a"]

    def test_versions_are_sorted(self) -> None:
        registry = ToolRegistry(environment=Environment.DEVELOPMENT)
        for number in (3, 1, 2):
            registry.register(_definition(version=number), _Echo())
        assert registry.versions(TENANT, "tool-1") == (1, 2, 3)

    def test_bump_produces_the_next_version(self) -> None:
        original = _definition(version=1)
        assert bump(original).version.number == 2
        assert original.version.number == 1  # immutable

    def test_dev_only_tool_refused_in_production(self) -> None:
        registry = ToolRegistry(environment=Environment.PRODUCTION)
        with pytest.raises(ConfigurationError):
            registry.register(_definition(), _Echo(), dev_only=True)

    def test_handler_must_satisfy_the_protocol(self) -> None:
        registry = ToolRegistry(environment=Environment.DEVELOPMENT)
        with pytest.raises(ConfigurationError):
            registry.register(_definition(), object())  # type: ignore[arg-type]


class TestExecutorConfig:
    """Executor configuration bounds."""

    def test_timeout_ceiling_is_enforced(self) -> None:
        config = ToolExecutorConfig(default_timeout_seconds=5.0, max_timeout_seconds=10.0)
        assert config.resolve_timeout(_definition(timeout=999), None) == 10.0

    def test_tool_default_is_used(self) -> None:
        config = ToolExecutorConfig(default_timeout_seconds=5.0, max_timeout_seconds=10.0)
        assert config.resolve_timeout(_definition(timeout=7), None) == 7.0

    def test_request_override_wins(self) -> None:
        config = ToolExecutorConfig(default_timeout_seconds=5.0, max_timeout_seconds=10.0)
        assert config.resolve_timeout(_definition(timeout=7), 3.0) == 3.0

    def test_invalid_config_refused(self) -> None:
        with pytest.raises(InvalidRequestError):
            ToolExecutorConfig(default_timeout_seconds=0)
        with pytest.raises(InvalidRequestError):
            ToolExecutorConfig(default_timeout_seconds=10.0, max_timeout_seconds=5.0)
