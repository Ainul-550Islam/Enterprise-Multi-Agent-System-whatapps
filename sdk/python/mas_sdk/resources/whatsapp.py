"""WhatsApp channel, inbox and template operations."""

from __future__ import annotations

from typing import Any

from mas_sdk.client import MasClient, Result


class WhatsApp:
    """Typed access to the tenant WhatsApp plane."""

    def __init__(self, client: MasClient) -> None:
        self._client = client

    def register_channel(self, body: dict[str, Any]) -> Result:
        return self._client.request("POST", "/v1/whatsapp/channels", body)

    def channels(self) -> Result:
        return self._client.request("GET", "/v1/whatsapp/channels")

    def send_text(self, channel_id: str, to: str, body: str) -> Result:
        return self._client.request(
            "POST", f"/v1/whatsapp/channels/{channel_id}/messages",
            {"to": to, "type": "text", "body": body},
        )

    def conversation(self, contact: str, channel_id: str | None = None) -> Result:
        query = f"?contact={contact}" + (f"&channel_id={channel_id}" if channel_id else "")
        return self._client.request("GET", f"/v1/whatsapp/conversations{query}")

    def human_reply(self, conversation_id: str, body: str, handoff: bool = False) -> Result:
        return self._client.request(
            "POST", f"/v1/whatsapp/conversations/{conversation_id}/messages",
            {"body": body, "handoff": handoff},
        )

    def templates(self) -> Result:
        return self._client.request("GET", "/v1/whatsapp/templates")

    def submit_template(self, body: dict[str, Any]) -> Result:
        return self._client.request("POST", "/v1/whatsapp/templates", body)

    def stats(self) -> Result:
        return self._client.request("GET", "/v1/whatsapp/stats")
