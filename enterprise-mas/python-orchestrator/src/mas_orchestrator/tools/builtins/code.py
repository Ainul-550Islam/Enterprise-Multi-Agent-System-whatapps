"""Code tool builtin - factory hook.

Provides the code execution capability through the RemoteSandbox backend.
This is a factory hook that routes to the appropriate backend based on the configuration.
Code execution is always performed in an isolated sandbox with resource limits.
"""

from __future__ import annotations

import asyncio
from mas_orchestrator.tools.backends import build_backend
from mas_orchestrator.config.enums import Environment


def execute_code(
    *,
    backend_kind: str,
    config: dict,
    environment: Environment,
    code: str,
    language: str = "python",
    cpus: float | None = None,
    memory_mb: int | None = None,
    time_limit: int | None = None,
    output_size_limit: int | None = None,
) -> dict:
    """Execute code in an isolated sandbox.

    Args:
        backend_kind: The backend kind string (e.g. "sandbox_remote").
        config: Backend configuration dictionary.
        environment: Current deployment environment.
        code: The source code to execute.
        language: The programming language (currently only python supported).
        cpus: CPU limit in cores (overrides config default).
        memory_mb: Memory limit in MB (overrides config default).
        time_limit: Execution time limit in seconds (overrides config default).
        output_size_limit: Max output size in bytes (overrides config default).

    Returns:
        Dictionary with execution results including stdout, stderr, returncode, etc.

    Raises:
        ConfigurationError: If the backend is not properly configured.
    """
    backend = build_backend(
        kind=backend_kind,
        config=config,
        environment=environment,
    )
    return asyncio.run(backend.execute(
        code=code,
        language=language,
        cpus=cpus,
        memory_mb=memory_mb,
        time_limit=time_limit,
        output_size_limit=output_size_limit,
    ))
