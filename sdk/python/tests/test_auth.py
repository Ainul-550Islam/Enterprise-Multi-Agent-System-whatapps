"""Scope guard and auth header behaviour (local failures only)."""

from __future__ import annotations

import pytest

from mas_sdk import MasClient, MissingScopeError


def test_scope_guard() -> None:
    def transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
        raise AssertionError("the network must not be reached without a scope")

    client = MasClient("http://api.test", "tok", transport=transport)
    with pytest.raises(MissingScopeError) as raised:
        client.require_scope()
    assert "MAS_TENANT_ID" in str(raised.value)

    # scoped routes refuse before the transport is invoked
    with pytest.raises(MissingScopeError):
        client.submit_execution(project_id="p", agent_id="a", input="i")
    with pytest.raises(MissingScopeError):
        client.get_execution("exec-1")
    with pytest.raises(MissingScopeError):
        client.signal_execution("exec-1", "cancel")


def test_bearer_and_correlation_headers_are_set() -> None:
    seen: dict[str, str] = {}

    def transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
        seen.update(headers)
        return 200, '{"data": {"status": "ok"}}'

    from mas_sdk import Scope

    client = MasClient(
        "http://api.test",
        "secret-token",
        scope=Scope(tenant_id="t", organization_id="o"),
        correlation_id="corr-1",
        transport=transport,
    )
    client.request("GET", "/v1/agents")
    assert seen["authorization"] == "Bearer secret-token"
    assert seen["x-correlation-id"] == "corr-1"
    assert seen["x-tenant-id"] == "t"
