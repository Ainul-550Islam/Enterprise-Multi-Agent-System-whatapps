"""The MAS API client (stdlib-only; transport is injectable for tests).

Production defaults: bounded timeouts and a small, jittered retry budget for
429/5xx responses. Idempotent reads and mutations are safe to retry because
mutations already carry an idempotency key. The public ``Result`` shape is
unchanged; exceptions are opt-in via ``raise_on_error``.
"""

from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

from mas_sdk.errors import MasApiError, MasRateLimited, MasScopeError

Transport = Callable[[str, str, dict[str, str], bytes | None], tuple[int, str]]

# Backwards-compatible alias used by earlier SDK releases.
MissingScopeError = MasScopeError


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
    status: int = 200
    request_id: str | None = None


@dataclass(frozen=True)
class RetryPolicy:
    """Retry behaviour for transient HTTP failures."""

    attempts: int = 3
    base_delay: float = 0.05
    max_delay: float = 2.0
    jitter: float = 0.25
    statuses: frozenset[int] = field(default_factory=lambda: frozenset({429, 500, 502, 503, 504}))

    def delay(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(max(retry_after, 0.0), self.max_delay)
        exponential = min(self.base_delay * (2 ** max(attempt - 1, 0)), self.max_delay)
        spread = exponential * self.jitter
        return min(max(0.0, exponential + random.uniform(-spread, spread)), self.max_delay)


def _default_transport(
    method: str,
    url: str,
    headers: dict[str, str],
    body: bytes | None,
    timeout: float = 10.0,
) -> tuple[int, str]:
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - base URL is operator config
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
        timeout: float = 10.0,
        retry: RetryPolicy | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._scope = scope
        self._correlation_id = correlation_id or str(uuid.uuid4())
        self._transport = transport or (lambda m, u, h, b: _default_transport(m, u, h, b, timeout))
        self._timeout = timeout
        self._retry = retry or RetryPolicy()

    def require_scope(self) -> Scope:
        """Resolves the scope or fails locally, before the network."""
        if self._scope is None:
            raise MasScopeError(
                "missing scope: pass tenant_id and organization_id together "
                "(MAS_TENANT_ID / MAS_ORGANIZATION_ID)"
            )
        return self._scope

    def submit_execution(
        self,
        *,
        project_id: str,
        input: Any,
        agent_id: str | None = None,
        workflow_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> Result:
        """Submit an execution to a project using exactly one target."""
        self.require_scope()
        if (agent_id is None) == (workflow_id is None):
            raise ValueError("provide exactly one of agent_id or workflow_id")
        body: dict[str, Any] = {"input": input}
        if agent_id is not None:
            body["agent_id"] = agent_id
        if workflow_id is not None:
            body["workflow_id"] = workflow_id
        return self.request(
            "POST",
            f"/v1/projects/{_path_segment(project_id)}/executions",
            body,
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
        raise_on_error: bool = False,
        sleep: Callable[[float], None] = time.sleep,
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

        result: Result | None = None
        for attempt in range(1, self._retry.attempts + 1):
            status, text = self._transport(method, f"{self._base_url}{path}", headers, payload)
            envelope = json.loads(text) if text else {}
            error = envelope.get("error") or {}
            result = Result(
                ok=status < 400 and "error" not in envelope,
                data=envelope.get("data"),
                code=error.get("code", f"HTTP_{status}"),
                message=error.get("message", ""),
                status=status,
                request_id=envelope.get("meta", {}).get("request_id"),
            )
            if result.ok or status not in self._retry.statuses:
                break
            if attempt == self._retry.attempts:
                break
            retry_after = self._parse_retry_after(envelope)
            sleep(self._retry.delay(attempt, retry_after))

        assert result is not None  # the retry budget is at least one
        if not result.ok and raise_on_error:
            retry_after = self._parse_retry_after(envelope)
            if result.status == 429:
                raise MasRateLimited(retry_after, result.message or "rate limited")
            raise MasApiError(result.status, result.code or "HTTP_ERROR", result.message or "", result.request_id)
        return result

    @staticmethod
    def _parse_retry_after(source: Any) -> float | None:
        if not isinstance(source, dict):
            return None
        for key in ("retry_after", "retryAfter", "Retry-After"):
            value = source.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        return None

    def iter_pages(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        page_size: int = 50,
        cursor_param: str = "cursor",
        cursor_field: str = "next_cursor",
    ) -> Iterator[Any]:
        """Yields every item from a keyset-paginated collection.

        The server owns the cursor; the client never computes offsets. Pages
        follow the stable ``{ items, next_cursor }`` shape used by the API.
        """
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            query = dict(params or {})
            query["limit"] = page_size
            if cursor is not None:
                query[cursor_param] = cursor
            result = self.request("GET", f"{path}?{_encode(query)}", raise_on_error=True)
            data = result.data or {}
            for item in data.get("items", []) if isinstance(data, dict) else []:
                yield item
            cursor = data.get(cursor_field) if isinstance(data, dict) else None
            if not cursor or cursor in seen:
                return
            seen.add(cursor)


def _encode(params: dict[str, Any]) -> str:
    from urllib.parse import urlencode

    return urlencode({key: value for key, value in params.items() if value is not None})


def _path_segment(value: str) -> str:
    from urllib.parse import quote

    return quote(value, safe="")


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
