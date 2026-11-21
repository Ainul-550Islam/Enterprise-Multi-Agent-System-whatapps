"""MasClient behaviour against an injected transport (no network)."""

from __future__ import annotations

from mas_sdk import MasClient, Scope

SCOPE = Scope(tenant_id="tenant-1", organization_id="org-1")


def make_client(reply: tuple[int, str]) -> tuple[MasClient, list[dict[str, object]]]:
    seen: list[dict[str, object]] = []

    def transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
        seen.append({"method": method, "url": url, "headers": headers, "body": body})
        return reply

    return MasClient("http://api.test", "tok", scope=SCOPE, transport=transport), seen


def test_submit_execution() -> None:
    client, seen = make_client((201, '{"data": {"execution_id": "exec-1", "status": "queued"}}'))
    result = client.submit_execution(agent_id="agent-1", input="hello")

    assert result.ok is True
    assert result.data == {"execution_id": "exec-1", "status": "queued"}
    assert len(seen) == 1
    call = seen[0]
    assert call["method"] == "POST"
    assert call["url"] == "http://api.test/v1/executions"
    headers = call["headers"]
    assert headers["x-tenant-id"] == "tenant-1"
    assert headers["x-organization-id"] == "org-1"
    assert len(headers["x-idempotency-key"]) > 8
    assert b"hello" in call["body"]


def test_error_envelopes_become_typed_results() -> None:
    client, _ = make_client((404, '{"error": {"code": "RESOURCE_NOT_FOUND", "message": "gone"}}'))
    result = client.get_execution("missing")
    assert result.ok is False
    assert result.code == "RESOURCE_NOT_FOUND"


def test_signals_use_the_signal_endpoint() -> None:
    client, seen = make_client((200, '{"data": {"ok": true}}'))
    result = client.signal_execution("exec-9", "cancel")
    assert result.ok is True
    assert seen[0]["url"] == "http://api.test/v1/executions/exec-9/signal"
