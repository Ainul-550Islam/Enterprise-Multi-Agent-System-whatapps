"""Resource clients mirror the documented operation paths."""

from __future__ import annotations

from mas_sdk import MasClient, Scope
from mas_sdk.resources import Agents, Billing, Executions, Quota, WhatsApp, Workflows

SCOPE = Scope("tenant", "org")


def client() -> tuple[MasClient, list[tuple[str, str]]]:
    calls: list[tuple[str, str]] = []

    def transport(method: str, url: str, headers: dict[str, str], body: bytes | None) -> tuple[int, str]:
        calls.append((method, url.replace("http://api.test", "")))
        return 200, '{"data":{"ok":true}}'

    return MasClient("http://api.test", "tok", scope=SCOPE, transport=transport), calls


def test_every_resource_uses_its_documented_path() -> None:
    c, calls = client()
    assert Agents(c).get("agent-1").ok
    assert Workflows(c).publish("wf-1").ok
    assert Executions(c).get("exec-1").ok
    assert WhatsApp(c).submit_template({"name": "n", "language": "en"}).ok
    assert Billing(c).checkout({"plan_code": "pro"}).ok
    assert Quota(c).quotas().ok
    assert calls == [
        ("GET", "/v1/agents/agent-1"),
        ("POST", "/v1/workflows/wf-1/publish"),
        ("GET", "/v1/executions/exec-1"),
        ("POST", "/v1/whatsapp/templates"),
        ("POST", "/v1/billing/checkout"),
        ("GET", "/v1/quotas"),
    ]


def test_whatsapp_conversation_and_text_paths() -> None:
    c, calls = client()
    WhatsApp(c).send_text("ch-1", "+8801712345678", "hello")
    WhatsApp(c).conversation("+8801712345678", "ch-1")
    assert calls == [
        ("POST", "/v1/whatsapp/channels/ch-1/messages"),
        ("GET", "/v1/whatsapp/conversations?contact=+8801712345678&channel_id=ch-1"),
    ]
