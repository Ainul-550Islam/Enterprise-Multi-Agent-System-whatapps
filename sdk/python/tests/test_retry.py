"""Retry behaviour against an injected transport."""

from __future__ import annotations

import pytest

from mas_sdk import MasClient, MasRateLimited, RetryPolicy, Scope

SCOPE = Scope("tenant", "org")


def scripted(replies: list[tuple[int, str]]) -> tuple[object, list[int]]:
    calls: list[int] = []

    def transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
        index = min(len(calls), len(replies) - 1)
        calls.append(index)
        return replies[index]

    return transport, calls


def client(replies: list[tuple[int, str]], **kwargs: object) -> tuple[MasClient, list[int]]:
    transport, calls = scripted(replies)
    return MasClient("http://api.test", "tok", scope=SCOPE, transport=transport, **kwargs), calls  # type: ignore[arg-type]


def test_retries_429_then_succeeds() -> None:
    delays: list[float] = []
    c, calls = client([(429, '{"error":{"code":"RATE_LIMITED","message":"slow"}}'), (200, '{"data":{"ok":true}}')])
    result = c.request("GET", "/v1/quotas", sleep=delays.append)
    assert result.ok is True
    assert len(calls) == 2
    assert 0 < delays[0] <= 2.0


def test_retry_after_is_honoured() -> None:
    delays: list[float] = []
    c, _ = client(
        [(429, '{"error":{"code":"RATE_LIMITED","message":"slow"},"retry_after":0.01}'), (200, '{"data":{}}')],
        retry=RetryPolicy(base_delay=1.0, max_delay=5.0),
    )
    c.request("GET", "/v1/quotas", sleep=delays.append)
    assert delays[0] == pytest.approx(0.01)


def test_exhausted_budget_raises_typed_error() -> None:
    c, calls = client([(503, '{"error":{"code":"UNAVAILABLE","message":"down"}}')])
    with pytest.raises(Exception) as excinfo:
        c.request("GET", "/v1/quotas", raise_on_error=True, sleep=lambda _: None)
    assert len(calls) == 3
    assert excinfo.value.args[0].startswith("503 UNAVAILABLE")


def test_rate_limit_error_carries_retry_after() -> None:
    c, _ = client([(429, '{"error":{"code":"RATE_LIMITED","message":"wait"},"retry_after":0.5}')])
    with pytest.raises(MasRateLimited) as excinfo:
        c.request("GET", "/v1/quotas", raise_on_error=True, sleep=lambda _: None)
    assert excinfo.value.retry_after == 0.5


def test_client_errors_are_not_retried() -> None:
    c, calls = client([(404, '{"error":{"code":"RESOURCE_NOT_FOUND","message":"gone"}}')])
    result = c.request("GET", "/v1/agents/missing", sleep=lambda _: None)
    assert result.ok is False
    assert len(calls) == 1
