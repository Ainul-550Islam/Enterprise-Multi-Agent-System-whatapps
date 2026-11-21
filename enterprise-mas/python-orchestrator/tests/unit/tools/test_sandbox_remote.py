"""RemoteSandbox: HMAC-signed HTTP, never a local subprocess."""

from __future__ import annotations

import hashlib
import hmac
import inspect
import json
from typing import Any

import pytest

from mas_orchestrator.contracts.errors import InvalidRequestError, ToolFailedError
from mas_orchestrator.providers.base_http import TransportResponse
from mas_orchestrator.security.credentials import Secret
from mas_orchestrator.tools.backends import sandbox as sandbox_module
from mas_orchestrator.tools.backends.sandbox import RemoteSandboxBackend
from mas_orchestrator.tools.builtins.code import SandboxLimits


class _RecordingTransport:
    def __init__(self, status_code: int = 200, body: bytes = b"{}") -> None:
        self.calls: list[dict[str, Any]] = []
        self._status = status_code
        self._body = body

    async def request(self, method: str, url: str, **kwargs: Any) -> TransportResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return TransportResponse(status_code=self._status, body=self._body, headers={})


def test_module_never_imports_subprocess() -> None:
    assert "subprocess" not in inspect.getsource(sandbox_module)
    assert not hasattr(sandbox_module, "subprocess")


async def test_execute_signs_the_canonical_body() -> None:
    body = b'{"exit_code":0,"stdout":"hi","stderr":"","timed_out":false}'
    transport = _RecordingTransport(200, body)
    secret = Secret("signing-secret", reference="test://sandbox")
    backend = RemoteSandboxBackend(
        transport=transport,
        endpoint_url="https://sandbox.example.com",
        signing_secret=secret,
    )
    result = await backend.execute(
        language="python", source="print(1)", stdin="", limits=SandboxLimits()
    )
    assert result.stdout == "hi"
    call = transport.calls[0]
    assert call["url"] == "https://sandbox.example.com/execute"
    payload = call["json_body"]
    canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    expected = hmac.new(b"signing-secret", canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    assert call["headers"]["x-mas-signature"] == f"sha256={expected}"


async def test_nonzero_exit_is_a_result_not_an_exception() -> None:
    body = b'{"exit_code":1,"stdout":"","stderr":"boom","timed_out":false}'
    backend = RemoteSandboxBackend(
        transport=_RecordingTransport(200, body), endpoint_url="https://sandbox.example.com"
    )
    result = await backend.execute(
        language="python", source="raise SystemExit(1)", stdin="", limits=SandboxLimits()
    )
    assert result.exit_code == 1
    assert result.stderr == "boom"


def test_relative_endpoint_is_refused() -> None:
    with pytest.raises(InvalidRequestError, match="absolute"):
        RemoteSandboxBackend(transport=_RecordingTransport(), endpoint_url="/execute")


async def test_http_refusal_is_typed() -> None:
    backend = RemoteSandboxBackend(
        transport=_RecordingTransport(400, b""), endpoint_url="https://sandbox.example.com"
    )
    with pytest.raises(ToolFailedError):
        await backend.execute(language="python", source="pass", stdin="", limits=SandboxLimits())
