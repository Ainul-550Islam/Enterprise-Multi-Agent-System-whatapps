"""Code-execution tool.

An agent that can run code can, by construction, reach anything the process
can reach. The only defensible position is therefore that execution happens in
a **sandbox we do not implement here**, behind an adapter that must be
supplied.

Concretely, this module:

* defines the :class:`CodeSandbox` adapter interface,
* ships **no** host-execution sandbox, because ``subprocess``/shell access is
  explicitly prohibited as a production implementation,
* fails closed: with no sandbox configured, every invocation is refused with a
  typed configuration error rather than quietly running on the host.

The dangerous thing this file avoids is the plausible-looking shortcut of
"just run it locally for now". That shortcut is how a code-execution tool
becomes remote code execution.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError
from mas_orchestrator.tools.types import (
    ToolDefinition,
    ToolInvocationContext,
    ToolKind,
    ToolSafety,
    ToolVersion,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MAX_SOURCE_CHARS: Final[int] = 64 * 1024
DEFAULT_TIMEOUT_SECONDS: Final[float] = 10.0
MAX_TIMEOUT_SECONDS: Final[float] = 60.0

INPUT_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "language": {"type": "string", "enum": ["python"]},
        "source": {"type": "string", "minLength": 1, "maxLength": MAX_SOURCE_CHARS},
        "stdin": {"type": "string"},
    },
    "required": ["language", "source"],
}


@dataclass(frozen=True, slots=True)
class SandboxLimits:
    """Resource ceilings passed to the sandbox.

    These are a *request* to the sandbox, not an enforcement by this process.
    A sandbox that ignores them is not a sandbox.
    """

    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    memory_megabytes: int = 256
    cpu_seconds: int = 10
    max_output_bytes: int = 256 * 1024
    network_access: bool = False

    def __post_init__(self) -> None:
        if not 0 < self.timeout_seconds <= MAX_TIMEOUT_SECONDS:
            raise InvalidRequestError(
                "sandbox timeout is out of range",
                details={"field": "timeout_seconds", "maximum": MAX_TIMEOUT_SECONDS},
            )
        if self.memory_megabytes <= 0 or self.cpu_seconds <= 0:
            raise InvalidRequestError(
                "sandbox resource limits must be positive",
                details={"field": "memory_megabytes"},
            )
        if self.max_output_bytes <= 0:
            raise InvalidRequestError(
                "sandbox output limit must be positive", details={"field": "max_output_bytes"}
            )


@dataclass(frozen=True, slots=True)
class SandboxResult:
    """What a sandbox reports back.

    Attributes:
        exit_code: Process exit status, or ``None`` if the run was killed.
        stdout: Captured standard output.
        stderr: Captured standard error.
        timed_out: Whether the run hit its time limit.
        killed_reason: Why the run was terminated, when it was.
    """

    exit_code: int | None
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    killed_reason: str | None = None

    @property
    def succeeded(self) -> bool:
        """Whether the program exited cleanly."""
        return self.exit_code == 0 and not self.timed_out


@runtime_checkable
class CodeSandbox(Protocol):
    """Adapter for an isolated execution environment.

    Implementations might use a container runtime, a microVM, or a managed
    sandbox service. What they must **not** do is execute on the host.
    """

    async def execute(
        self,
        *,
        language: str,
        source: str,
        stdin: str,
        limits: SandboxLimits,
    ) -> SandboxResult:
        """Runs code in isolation and returns the outcome.

        Implementations must not raise to report a program failure: a nonzero
        exit status is a :class:`SandboxResult`, not an exception. An exception
        means the sandbox itself failed.
        """
        ...


class NoSandboxConfigured:
    """The fail-closed default: execution is always refused.

    This is the only sandbox implementation that ships with the codebase. It
    exists so the absence of a sandbox is a loud, typed error rather than an
    accidental fallback to running on the host.
    """

    async def execute(
        self,
        *,
        language: str,
        source: str,
        stdin: str,
        limits: SandboxLimits,
    ) -> SandboxResult:
        """Always refuses.

        Raises:
            ConfigurationError: Always.
        """
        raise ConfigurationError(
            "no code-execution sandbox is configured; refusing to run code",
            details={
                "language": language,
                "hint": "configure a container or microVM-backed CodeSandbox",
            },
        )


class CodeTool:
    """Runs model-supplied source inside an injected sandbox."""

    def __init__(
        self,
        sandbox: CodeSandbox | None = None,
        *,
        limits: SandboxLimits | None = None,
        environment: Environment = Environment.DEVELOPMENT,
    ) -> None:
        self._sandbox = sandbox
        self._limits = limits or SandboxLimits()
        self._environment = environment

    @property
    def has_sandbox(self) -> bool:
        """Whether a real sandbox is attached.

        Exposed so startup validation can refuse to boot a code tool that
        would only fail at call time.
        """
        return self._sandbox is not None

    def assert_usable(self) -> None:
        """Refuses to be used without a sandbox outside development.

        Raises:
            ConfigurationError: In production when no sandbox is attached.
        """
        if not self.has_sandbox and self._environment is Environment.PRODUCTION:
            raise ConfigurationError(
                "code tool is registered in production without a sandbox",
                details={"environment": self._environment.value},
            )

    async def invoke(
        self,
        arguments: Mapping[str, object],
        context: ToolInvocationContext,
    ) -> dict[str, Any]:
        """Executes the supplied source.

        Args:
            arguments: Validated against :data:`INPUT_SCHEMA`.
            context: Invocation context, used for logging correlation.

        Returns:
            A dict describing the run: exit code, streams and termination.

        Raises:
            ConfigurationError: If no sandbox is configured.
            InvalidRequestError: If the source exceeds the size limit.
        """
        language = str(arguments["language"])
        source = str(arguments["source"])
        stdin = str(arguments.get("stdin") or "")

        if len(source) > MAX_SOURCE_CHARS:
            raise InvalidRequestError(
                "source exceeds the maximum length",
                details={"field": "source", "limit": MAX_SOURCE_CHARS},
            )

        if self._sandbox is None:
            # Explicit refusal. Running here would turn an agent's code output
            # into host code execution, which no configuration flag can justify.
            raise ConfigurationError(
                "no code-execution sandbox is configured; refusing to run code",
                details={"tool": context.tool_call_id or "", "language": language},
            )

        _LOGGER.info(
            "code tool execution",
            extra={
                "event": "tool.code.execute",
                "invocation_id": context.invocation_id,
                "language": language,
                "source_chars": len(source),
            },
        )

        result = await self._sandbox.execute(
            language=language, source=source, stdin=stdin, limits=self._limits
        )

        return {
            "exit_code": result.exit_code,
            "stdout": result.stdout[: self._limits.max_output_bytes],
            "stderr": result.stderr[: self._limits.max_output_bytes],
            "timed_out": result.timed_out,
            "killed_reason": result.killed_reason,
            "succeeded": result.succeeded,
        }


def build_code_tool(
    *,
    tool_id: str,
    name: str,
    tenant_id: str,
    sandbox: CodeSandbox | None = None,
    description: str | None = None,
    version: int = 1,
    limits: SandboxLimits | None = None,
    environment: Environment = Environment.DEVELOPMENT,
) -> tuple[ToolDefinition, CodeTool]:
    """Builds a definition/handler pair for the code tool.

    The tool is registered with ``safety=DESTRUCTIVE`` because arbitrary code
    execution is by definition destructive-capable, and with
    ``idempotent=False`` because a program may perform side effects.
    """
    tool = CodeTool(sandbox, limits=limits, environment=environment)
    tool.assert_usable()

    definition = ToolDefinition(
        tool_id=tool_id,
        name=name,
        version=ToolVersion(version),
        kind=ToolKind.CODE,
        safety=ToolSafety.DESTRUCTIVE,
        input_schema=INPUT_SCHEMA,
        description=description or "Execute source code inside an isolated sandbox",
        tenant_id=tenant_id,
        default_timeout_seconds=tool._limits.timeout_seconds,
        idempotent=False,
    )

    return definition, tool
