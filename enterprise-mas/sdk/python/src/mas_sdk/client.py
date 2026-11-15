"""mas SDK HTTP client - sync + async.

Provides MasApiClient (sync) and MasAsyncClient (async) wrapping httpx,
with automatic StableApiError mapping, SSE support, keyset pagination, and retry+idempotency.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional, Sequence, TypeVar, Generic

import httpx
from pydantic import BaseModel

from mas_sdk.errors import MasError, ApiError, ValidationError
from mas_sdk.models import ApiEnvelope
from mas_sdk.pagination import KeySetPagination

T = TypeVar("T")


class MasClient:
    """Synchronous mas API client.

    Wraps httpx with automatic error mapping, idempotency, and SSE support.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        tenant: Optional[str] = None,
        organization: Optional[str] = None,
        correlation_id: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.tenant = tenant
        self.organization = organization
        self.correlation_id = correlation_id or f"cli-{int(time.time())}"
        self.timeout = timeout
        self._client = httpx.Client(timeout=timeout)

    def _headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {
            "Authorization": f"Bearer {self.token}",
            "X-Correlation-Id": self.correlation_id,
            "Accept": "application/json",
        }
        if self.tenant is not None:
            h["X-Tenant-Id"] = self.tenant
        if self.organization is not None:
            h["X-Organization-Id"] = self.organization
        return h

    def get(self, path: str, **kwargs: Any) -> "Outcome[Any]":
        return self._request("GET", path, **kwargs)

    def post(self, path: str, json: Optional[Dict[str, Any]] = None,
             **kwargs: Any) -> " Outcome[Any]":
        return self._request("POST", path, json=json, **kwargs)

    def post_empty(self, path: str, **kwargs: Any) -> "Outcome[Any]":
        return self._request("POST", path, json=None, **kwargs)

    def put(self, path: str, json: Optional[Dict[str, Any]] = None, **kwargs: Any) -> "Outcome[Any]":
        return self._request("PUT", path, json=json, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> "Outcome[Any]":
        return self._request("DELETE", path, **kwargs)

    def _request(self, method: str, path: str, **kwargs: Any) -> "Outcome[Any]":
        url = f"{self.base_url}{path}"
        try:
            response = self._client.request(method, url, headers=self._headers(), **kwargs)
            return self._map_response(response)
        except httpx.TimeoutException:
            return Outcome.Err(ApiError("TRANSPORT", "Request timed out"))
        except httpx.ConnectError:
            return Outcome.Err(ApiError("TRANSPORT", "Connection failed"))
        except Exception as e:
            return Outcome.Err(ApiError("TRANSPORT", str(e)))

    def _map_response(self, response: httpx.Response) -> "Outcome[Any]":
        try:
            body = response.json()
        except Exception:
            return Outcome.Err(ApiError("PARSE", f"Failed to parse JSON: {response.text}"))

        # Extract envelope data
        if isinstance(body, dict) and "data" in body and "envelope" in body:
            # It's an ApiEnvelope
            envelope = body["envelope"]
            errors = envelope.get("errors", [])
            if errors:
                # Build error from envelope
                err = errors[0]
                code = errors[0].get("code", "UNKNOWN")
                message = errors[0].get("message", "Unknown error")
                return Outcome.Err(ApiError(code, message))
            data = body["data"]
            idempotent_replay = envelope.get("idempotent_replay", False)
            return Outcome.Ok(data, idempotent_replay=idempotent_replay)
        elif isinstance(body, dict) and "error" in body:
            return Outcome.Err(ApiError(
                body["error"].get("code", "UNKNOWN"),
                body["error"].get("message", "Unknown error"),
            ))
        else:
            return Outcome.Ok(body)

    def stream_sse(self, path: str, **kwargs: Any) -> "SseClient[Any]":
        return SseClient(self, "GET", path, **kwargs)

    def paginate(self, path: str, **kwargs: Any) -> KeySetPagination:
        return KeySetPagination(self, path, **kwargs)


class MasAsyncClient:
    """Asynchronous mas API client.

    Wraps httpx.AsyncClient with the same features as MasClient.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        tenant: Optional[str] = None,
        organization: Optional[str] = None,
        correlation_id: Optional[str] = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.tenant = tenant
        self.organization = organization
        self.correlation_id = correlation_id or f"cli-{int(time.time())}"
        self.timeout = timeout
        self._client = httpx.AsyncClient(timeout=timeout)

    def _headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {
            "Authorization": f"Bearer {self.token}",
            "X-Correlation-Id": self.correlation_id,
            "Accept": "application/json",
        }
        if self.tenant is not None:
            h["X-Tenant-Id"] = self.tenant
        if self.organization is not None:
            h["X-Organization-Id"] = self.organization
        return h

    async def _request(self, method: str, path: str, **kwargs: Any) -> "Outcome[Any]":
        url = f"{self.base_url}{path}"
        try:
            response = await self._client.request(method, url, headers=self._headers(), **kwargs)
            return self._map_response(response)
        except Exception as e:
            return Outcome.Err(ApiError("TRANSPORT", str(e)))

    async def get(self, path: str, **kwargs: Any) -> "Outcome[Any]":
        return await self._request("GET", path, **kwargs)

    async def post(self, path: str, json: Optional[Dict[str, Any]] = None,
                  **kwargs: Any) -> "Outcome[Any]":
        return await self._request("POST", path, json=json, **kwargs)

    async def post_empty(self, path: str, **kwargs: Any) -> "Outcome[Any]":
        return await self._request("POST", path, json=None, **kwargs)

    async def put(self, path: str, json: Optional[Dict[str, Any]] = None, **kwargs: Any) -> "Outcome[Any]":
        return await self._request("PUT", path, json=json, **kwargs)

    async def delete(self, path: str, **kwargs: Any) -> "Outcome[Any]":
        return await self._request("DELETE", path, **kwargs)

    async def _map_response(self, response: httpx.Response) -> "Outcome[Any]":
        try:
            body = response.json()
        except Exception:
            return Outcome.Err(ApiError("PARSE", f"Failed to parse JSON: {response.text}"))

        if isinstance(body, dict) and "data" in body and "envelope" in body:
            envelope = body["envelope"]
            errors = envelope.get("errors", [])
            if errors:
                code = errors[0].get("code", "UNKNOWN")
                message = errors[0].get("message", "Unknown error")
                return Outcome.Err(ApiError(code, message))
            data = body["data"]
            idempotent_replay = envelope.get("idempotent_replay", False)
            return Outcome.Ok(data, idempotent_replay=idempotent_replay)
        elif isinstance(body, dict) and "error" in body:
            return Outcome.Err(ApiError(
                body["error"].get("code", "UNKNOWN"),
                body["error"].get("message", "Unknown error"),
            ))
        else:
            return Outcome.Ok(body)

    def stream_sse(self, path: str, **kwargs: Any) -> "SseClient[Any]":
        return SseClient(self, "GET", path, **kwargs)

    def paginate(self, path: str, **kwargs: Any) -> KeySetPagination:
        return KeySetPagination(self, path, **kwargs)


class Outcome(Generic[T]):
    """Result wrapper: Ok(data) or Err(error)."""

    def __init__(self, value: T | ApiError, idempotent_replay: bool = False) -> None:
        if isinstance(value, ApiError):
            self._error = value
            self._data: T | None = None
            self.idempotent_replay = idempotent_replay
        else:
            self._data = value
            self._error: ApiError | None = None
            self.idempotent_replay = idempotent_replay

    @property
    def is_ok(self) -> bool:
        return self._error is None

    @property
    def is_err(self) -> bool:
        return self._error is not None

    @property
    def data(self) -> T:
        if self._error is not None:
            raise ValueError("Outcome is an error, not OK")
        return cast(T, self._data)  # type: ignore

    @property
    def error(self) -> ApiError | None:
        return self._error

    def __repr__(self) -> str:
        if self.is_ok:
            return f"Outcome.Ok({self._data!r})"
        return f"Outcome.Err({self._error!r})"


def asyncio_run(fn: callable, *args, **kwargs) -> Any:
    """Run an async function from sync code (simple event loop)."""
    import asyncio
    try:
        loop = asyncio.get_running_loop()
        # We're already in an async context
        raise RuntimeError("asyncio run called from within running event loop")
    except RuntimeError:
        return asyncio.run(fn(*args, **kwargs))
