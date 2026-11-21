"""Remote sandbox backend for the code tool.

Implements :class:`~mas_orchestrator.tools.builtins.code.CodeSandbox` against a
managed sandbox service reached over the guarded transport. Code never runs in
this process: the remote side executes and reports back.

The protocol contract holds here exactly as for local adapters: a non-zero
exit (or a timeout) is a :class:`~mas_orchestrator.tools.builtins.code.SandboxResult`,
never an exception. An exception means the *sandbox itself* failed -- the
service was unreachable or answered nonsense.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any
from urllib.parse import urlsplit

from mas_orchestrator.contracts.errors import (
    DependencyUnavailableError,
    InvalidRequestError,
    OrchestratorError,
    ToolFailedError,
)
from mas_orchestrator.security.credentials import Secret
from mas_orchestrator.tools.builtins.code import SandboxLimits, SandboxResult


class RemoteSandboxBackend:
    """Executes code in a remote sandbox service.

    Args:
        transport: The platform guarded HTTP transport.
        endpoint_url: Base URL of the sandbox service; executions POST to
            ``{endpoint_url}/execute``.
        api_key: Bearer credential for the service, when it demands one.

    Raises:
        InvalidRequestError: If the configuration is malformed.
    """

    def __init__(
        self,
        *,
        transport: Any,
        endpoint_url: str,
        api_key: Secret | None = None,
        signing_secret: Secret | None = None,
    ) -> None:
        if transport is None or not hasattr(transport, "request"):
            raise InvalidRequestError(
                "remote sandbox backend requires the guarded transport",
                details={"field": "transport"},
            )
        cleaned = endpoint_url.strip().rstrip("/")
        parsed = urlsplit(cleaned)
        if parsed.scheme not in ("https", "http") or not parsed.netloc:
            raise InvalidRequestError(
                "sandbox endpoint must be an absolute http(s) URL",
                details={"field": "endpoint_url"},
            )
        if api_key is not None and not isinstance(api_key, Secret):
            raise InvalidRequestError(
                "sandbox api_key must be a Secret when supplied",
                details={"field": "api_key"},
            )
        if signing_secret is not None and not isinstance(signing_secret, Secret):
            raise InvalidRequestError(
                "sandbox signing_secret must be a Secret when supplied",
                details={"field": "signing_secret"},
            )
        self._transport = transport
        self._endpoint = cleaned
        self._api_key = api_key
        self._signing_secret = signing_secret

    async def execute(
        self,
        *,
        language: str,
        source: str,
        stdin: str,
        limits: SandboxLimits,
    ) -> SandboxResult:
        """Runs one program remotely and returns its outcome."""
        if not isinstance(limits, SandboxLimits):
            raise InvalidRequestError(
                "sandbox execution requires SandboxLimits", details={"field": "limits"}
            )
        headers: dict[str, str] = {"content-type": "application/json"}
        if self._api_key is not None:
            headers["authorization"] = f"Bearer {self._api_key.reveal()}"

        payload = {
            "language": language,
            "source": source,
            "stdin": stdin,
            "limits": {
                "timeout_seconds": limits.timeout_seconds,
                "memory_megabytes": limits.memory_megabytes,
                "cpu_seconds": limits.cpu_seconds,
                "max_output_bytes": limits.max_output_bytes,
                "network_access": limits.network_access,
            },
        }
        canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        if self._signing_secret is not None:
            digest = hmac.new(
                self._signing_secret.reveal().encode("utf-8"),
                canonical.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            headers["x-mas-signature"] = f"sha256={digest}"

        try:
            response = await self._transport.request(
                "POST",
                f"{self._endpoint}/execute",
                json_body=payload,
                headers=headers,
            )
        except OrchestratorError:
            raise
        except Exception as error:
            raise DependencyUnavailableError(
                "remote sandbox could not be reached",
                details={"tool_id": "code", "backend": "remote-sandbox"},
            ) from error

        if response.status_code not in (200, 201):
            if 500 <= response.status_code < 600:
                raise DependencyUnavailableError(
                    "remote sandbox is unavailable",
                    details={"tool_id": "code", "status_code": response.status_code},
                )
            raise ToolFailedError(
                "remote sandbox refused the execution",
                details={"tool_id": "code", "status_code": response.status_code},
            )

        try:
            document = response.json()
        except OrchestratorError as error:
            raise ToolFailedError(
                "remote sandbox returned a body that is not valid JSON",
                details={"tool_id": "code", "backend": "remote-sandbox"},
            ) from error
        if not isinstance(document, dict):
            raise ToolFailedError(
                "remote sandbox returned a malformed outcome",
                details={"tool_id": "code", "backend": "remote-sandbox"},
            )

        exit_code = document.get("exit_code")
        return SandboxResult(
            exit_code=exit_code if isinstance(exit_code, int) else None,
            stdout=str(document.get("stdout") or ""),
            stderr=str(document.get("stderr") or ""),
            timed_out=bool(document.get("timed_out", False)),
            killed_reason=(
                str(document["killed_reason"])
                if isinstance(document.get("killed_reason"), str)
                else None
            ),
        )
