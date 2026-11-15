"""Remote Sandbox backend.

Executes user code in an isolated sandbox environment via the sandbox-runner
HTTP service. Network is disabled, filesystem is read-only with tmpfs workspace,
and resource limits (CPU, memory, time, output size) are enforced.
All communication with the runner uses HMAC-signed requests.
"""

from __future__ import annotations

import asyncio
import json
import hmac
import hashlib
import base64
from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import resolve_secret

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Sandbox runner service base URL (configured at deployment)
DEFAULT_SANDBOX_URL = "http://localhost:8080"
#: Default timeout in seconds
DEFAULT_TIMEOUT = 30
#: HMAC secret for request signing (secret reference)
DEFAULT_HMAC_SECRET = "sandbox_runner_hmac_secret"


class RemoteSandbox:
    """Remote sandbox backend for executing user code in isolation.

    Executes code via the sandbox-runner HTTP service with the following guarantees:
    - No network access from the sandbox
    - Read-only filesystem with temporary workspace
    - Resource limits: CPU, memory, time, output size
    - HMAC-signed requests for integrity verification
    - Non-root execution environment

    Required configuration keys:
    - runner_url: Base URL of the sandbox-runner service
    - hmac_secret: Secret reference for signing requests (e.g. env:SANDBOX_HMAC_SECRET)

    Optional configuration keys:
    - timeout: request timeout in seconds
    - cpus: CPU limit (in cores)
    - memory: memory limit in MB
    - time_limit: execution time limit in seconds
    - output_size_limit: max output size in bytes
    - workspace_size: workspace size in MB
    """

    def __init__(self, *, config: dict, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._runner_url = config.get("runner_url", DEFAULT_SANDBOX_URL)
        self._hmac_secret = resolve_secret(config.get("hmac_secret", ""))
        self._timeout = float(config.get("timeout", DEFAULT_TIMEOUT))
        self._cpus = float(config.get("cpus", 0.5))
        self._memory_mb = int(config.get("memory_mb", 512))
        self._time_limit = int(config.get("time_limit", 30))
        self._output_size_limit = int(config.get("output_size_limit", 100_000))
        self._workspace_size_mb = int(config.get("workspace_size_mb", 64))

        # Validate configuration
        if not self._runner_url:
            raise ConfigurationError(
                "runner_url is required for RemoteSandbox backend",
                details={"field": "runner_url"},
            )

        if not self._hmac_secret:
            raise ConfigurationError(
                "hmac_secret (secret reference) is required for RemoteSandbox",
                details={"field": "hmac_secret"},
            )

        # Validate resource limits
        if self._cpus <= 0 or self._cpus > 32:
            _LOGGER.warning(
                "cpu limit %.2f is outside recommended range (0.032-32)",
                self._cpus
            )

        if self._memory_mb <= 0 or self._memory_mb > 8192:
            _LOGGER.warning(
                "memory limit %dMB is outside recommended range (1-8192)",
                self._memory_mb
            )

        if self._time_limit <= 0 or self._time_limit > 3600:
            _LOGGER.warning(
                "time limit %ds is outside recommended range (1-3600)",
                self._time_limit
            )

        if self._output_size_limit <= 0 or self._output_size_limit > 10_000_000:
            _LOGGER.warning(
                "output size limit %db is outside recommended range (100b-10MB)",
                self._output_size_limit
            )

        if self._workspace_size_mb <= 0 or self._workspace_size_mb > 1024:
            _LOGGER.warning(
                "workspace size %dMB is outside recommended range (1-1024)",
                self._workspace_size_mb
            )

    def _sign_request(self, payload: dict) -> dict:
        """Sign a request payload with HMAC for integrity verification.

        Args:
            payload: The request payload to sign.

        Returns:
            The signed payload with added "sig" field.
        """
        # Create a deterministic string from the payload (excluding the sig field)
        payload_copy = dict(payload)
        signature_data = json.dumps(payload_copy, sort_keys=True, separators=(',', ':'))
        
        # Compute HMAC-SHA256 signature
        sig = hmac.new(
            self._hmac_secret.encode(),
            signature_data.encode(),
            hashlib.sha256
        ).hexdigest()
        
        # Add the signature to the payload
        payload_copy["sig"] = sig
        return payload_copy

    async def execute(self, *, code: str, language: str = "python", 
                     cpus: float | None = None, memory_mb: int | None = None,
                     time_limit: int | None = None, output_size_limit: int | None = None) -> dict:
        """Execute code in the remote sandbox.

        Args:
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
            RuntimeError: If the execution fails or times out.
        """
        # Use provided overrides or config defaults
        cpus = cpus or self._cpus
        memory_mb = memory_mb or self._memory_mb
        time_limit = time_limit or self._time_limit
        output_size_limit = output_size_limit or self._output_size_limit

        # Build the request payload
        payload = {
            "code": code,
            "language": language,
            "cpus": cpus,
            "memory_mb": memory_mb,
            "time_limit": time_limit,
            "output_size_limit": output_size_limit,
            "workspace_size_mb": self._workspace_size_mb,
        }
        
        # Sign the request
        signed_payload = self._sign_request(payload)
        
        # Send to sandbox runner
        try:
            async with asyncio.timeout(self._timeout):
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        f"{self._runner_url}/run",
                        json=signed_payload,
                        timeout=self._timeout,
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        # Verify HMAC signature
                        if not self._verify_signature(result):
                            raise RuntimeError("sandbox execution: invalid HMAC signature")
                        
                        return {
                            "stdout": result.get("stdout", ""),
                            "stderr": result.get("stderr", ""),
                            "returncode": result.get("returncode", -1),
                            "execution_time": result.get("execution_time", 0.0),
                            "output_size": result.get("output_size", 0),
                            "timed_out": result.get("timed_out", False),
                            "killed": result.get("killed", False),
                        }
                    else:
                        raise RuntimeError(
                            f"sandbox runner returned {response.status_code}: {response.text}"
                        )

        except asyncio.TimeoutError:
            raise RuntimeError(f"sandbox execution timed out after {self._timeout}s")
        except httpx.ConnectError as e:
            raise RuntimeError(f"failed to connect to sandbox runner: {e}")
        except Exception as e:
            raise RuntimeError(f"sandbox execution failed: {e}")

    def _verify_signature(self, result: dict) -> bool:
        """Verify the HMAC signature of a response.

        Args:
            result: The result dictionary from the sandbox runner.

        Returns:
            True if the signature is valid, False otherwise.
        """
        # Extract the expected signature from the result
        expected_sig = result.get("sig", "")
        if not expected_sig:
            return False
        
        # Re-compute the signature
        payload_copy = {k: v for k, v in result.items() if k != "sig"}
        signature_data = json.dumps(payload_copy, sort_keys=True, separators=(',', ':'))
        expected = hmac.new(
            self._hmac_secret.encode(),
            signature_data.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return hmac.compare_digest(expected_sig, expected)


def create_backend(
    *,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> RemoteSandbox:
    """Convenient function to create a RemoteSandbox instance."""
    return RemoteSandbox(config=config, environment=environment)
