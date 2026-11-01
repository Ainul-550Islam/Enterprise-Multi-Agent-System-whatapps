"""Tool layer.

Implements spec §10. Every invocation carries an identifier, a version, a
validated schema, a permission decision, an execution context, a timeout, a
retry policy, an invocation id, a bounded output, a structured result and a
failure classification. Tool output is always treated as untrusted data.

The two guarantees worth stating plainly:

* **The model cannot authorise its own tool calls.** Permission comes from an
  external :class:`~mas_orchestrator.tools.permissions.ToolPermissionBridge`,
  and the default bridge denies everything.
* **There is no second HTTP client.** Any networked tool reuses the same
  SSRF-guarded transport the model providers use, so a tool cannot become a
  bypass around the host allowlist.

Typical use:

>>> registry = ToolRegistry()  # doctest: +SKIP
>>> definition, handler = build_http_tool(  # doctest: +SKIP
...     tool_id="tool-1", name="search", tenant_id="tenant-1",
...     allowed_hosts={"api.example.com"},
... )
>>> registry.register(definition, handler)  # doctest: +SKIP
"""

from __future__ import annotations

from mas_orchestrator.tools.executor import (
    DEFAULT_TOOL_TIMEOUT_SECONDS,
    ToolExecutor,
    ToolExecutorConfig,
    failed_result,
)
from mas_orchestrator.tools.permissions import (
    AllowListPermissionBridge,
    DenyAllPermissionBridge,
    PermissionDecision,
    PermissionResult,
    ToolPermissionBridge,
    enforce_permission,
)
from mas_orchestrator.tools.registry import ToolHandler, ToolRegistry, bump
from mas_orchestrator.tools.types import (
    MAX_TOOL_ARGUMENT_BYTES,
    MAX_TOOL_CALLS_PER_EXECUTION,
    MAX_TOOL_OUTPUT_BYTES,
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

__all__ = [
    "DEFAULT_TOOL_TIMEOUT_SECONDS",
    "MAX_TOOL_ARGUMENT_BYTES",
    "MAX_TOOL_CALLS_PER_EXECUTION",
    "MAX_TOOL_OUTPUT_BYTES",
    "AllowListPermissionBridge",
    "DenyAllPermissionBridge",
    "PermissionDecision",
    "PermissionResult",
    "ToolDefinition",
    "ToolExecutor",
    "ToolExecutorConfig",
    "ToolFailureClass",
    "ToolHandler",
    "ToolInvocationContext",
    "ToolKind",
    "ToolPermissionBridge",
    "ToolRegistry",
    "ToolResult",
    "ToolSafety",
    "ToolStatus",
    "ToolVersion",
    "bump",
    "classify_error",
    "describe_unsupported_keywords",
    "enforce_permission",
    "failed_result",
    "validate_arguments",
    "validate_output",
]
