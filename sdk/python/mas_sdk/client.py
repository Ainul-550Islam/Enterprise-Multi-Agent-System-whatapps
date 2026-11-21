"""The MAS API client (stdlib-only; transport is injectable for tests)."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any, Callable

Transport = Callable[[str, str, dict[str, str], bytes | None], tuple[int, str]]


class MissingScopeError(RuntimeError):
    """A scoped route was used without tenant+organization."""


@dataclass(frozen=True)
class Scope:
    """Scope headers the API requires together."""

    tenant_id: str
    organization_id: str


@dataclass(frozen=True)
class Result:
    """Decoded envelope result: branch on `ok`, never on messages."""

    ok: bool
    data: Any = None
    code: str | None = None
    message: str | None = None


def _default_transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - scheme checked by the caller
            return response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", "replace")


class MasClient:
    """The MAS API client."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        scope: Scope | None = None,
        correlation_id: str | None = None,
        transport: Transport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._scope = scope
        self._correlation_id = correlation_id or str(uuid.uuid4())
        self._transport = transport or _default_transport

    def require_scope(self) -> Scope:
        """Resolves the scope or fails locally, before the network."""
        if self._scope is None:
            raise MissingScopeError(
                "missing scope: pass tenant_id and organization_id together "
                "(MAS_TENANT_ID / MAS_ORGANIZATION_ID)"
            )
        return self._scope

    def submit_execution(
        self, *, agent_id: str, input: str, idempotency_key: str | None = None
    ) -> Result:
        self.require_scope()
        return self.request(
            "POST",
            "/v1/executions",
            {"agent_id": agent_id, "input": input},
            idempotency_key=idempotency_key or str(uuid.uuid4()),
        )

    def get_execution(self, execution_id: str) -> Result:
        self.require_scope()
        return self.request("GET", f"/v1/executions/{execution_id}")

    def signal_execution(self, execution_id: str, signal: str) -> Result:
        self.require_scope()
        return self.request("POST", f"/v1/executions/{execution_id}/signal", {"signal": signal})

    def request(
        self,
        method: str,
        path: str,
        body: Any = None,
        *,
        idempotency_key: str | None = None,
    ) -> Result:
        headers = {
            "authorization": f"Bearer {self._token}",
            "x-correlation-id": self._correlation_id,
        }
        if self._scope is not None:
            headers["x-tenant-id"] = self._scope.tenant_id
            headers["x-organization-id"] = self._scope.organization_id
        if idempotency_key is not None:
            headers["x-idempotency-key"] = idempotency_key
        payload: bytes | None = None
        if body is not None:
            headers["content-type"] = "application/json"
            payload = json.dumps(body).encode("utf-8")
        status, text = self._transport(method, f"{self._base_url}{path}", headers, payload)
        envelope = json.loads(text) if text else {}
        if status >= 400 or "error" in envelope:
            error = envelope.get("error") or {}
            return Result(
                ok=False,
                code=error.get("code", f"HTTP_{status}"),
                message=error.get("message", ""),
            )
        return Result(ok=True, data=envelope.get("data"))


def parse_event_frame(raw: str) -> dict[str, str]:
    """Parses one SSE frame into its fields (event/data/id/retry)."""
    fields: dict[str, str] = {"event": "message", "data": ""}
    data_lines: list[str] = []
    for line in raw.splitlines():
        if line.startswith("event:"):
            fields["event"] = line[len("event:") :].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())
        elif line.startswith("id:"):
            fields["id"] = line[len("id:") :].strip()
        elif line.startswith("retry:"):
            fields["retry"] = line[len("retry:") :].strip()
    fields["data"] = "\n".join(data_lines)
    return fields
