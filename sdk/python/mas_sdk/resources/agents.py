"""Agent catalogue operations (openapi: /v1/agents)."""

from __future__ import annotations

from typing import Any

from mas_sdk.client import MasClient, Result


class Agents:
    """Typed access to agent lifecycle endpoints."""

    def __init__(self, client: MasClient) -> None:
        self._client = client

    def list(self, *, limit: int = 50, cursor: str | None = None) -> Result:
        query = f"?limit={limit}" + (f"&cursor={cursor}" if cursor else "")
        return self._client.request("GET", f"/v1/agents{query}")

    def get(self, agent_id: str) -> Result:
        return self._client.request("GET", f"/v1/agents/{agent_id}")

    def create(self, body: dict[str, Any]) -> Result:
        return self._client.request("POST", "/v1/agents", body)

    def versions(self, agent_id: str) -> Result:
        return self._client.request("GET", f"/v1/agents/{agent_id}/versions")

    def publish_version(self, agent_id: str, version: str) -> Result:
        return self._client.request("POST", f"/v1/agents/{agent_id}/versions", {"version": version})

    def rollback(self, agent_id: str, version: str) -> Result:
        return self._client.request("POST", f"/v1/agents/{agent_id}/versions/{version}/rollback")

    def tool_permissions(self, agent_id: str, tool_ids: list[str]) -> Result:
        return self._client.request("PUT", f"/v1/agents/{agent_id}/tool-permissions", {"tool_ids": tool_ids})
