"""Workflow lifecycle operations (openapi: /v1/workflows)."""

from __future__ import annotations

from typing import Any

from mas_sdk.client import MasClient, Result


class Workflows:
    """Typed access to workflow lifecycle endpoints."""

    def __init__(self, client: MasClient) -> None:
        self._client = client

    def list(self, *, limit: int = 50, cursor: str | None = None) -> Result:
        query = f"?limit={limit}" + (f"&cursor={cursor}" if cursor else "")
        return self._client.request("GET", f"/v1/workflows{query}")

    def get(self, workflow_id: str) -> Result:
        return self._client.request("GET", f"/v1/workflows/{workflow_id}")

    def create(self, body: dict[str, Any]) -> Result:
        return self._client.request("POST", "/v1/workflows", body)

    def put_graph(self, workflow_id: str, graph: dict[str, Any]) -> Result:
        return self._client.request("PUT", f"/v1/workflows/{workflow_id}/graph", graph)

    def publish(self, workflow_id: str) -> Result:
        return self._client.request("POST", f"/v1/workflows/{workflow_id}/publish")

    def disable(self, workflow_id: str) -> Result:
        return self._client.request("POST", f"/v1/workflows/{workflow_id}/disable")

    def rollback(self, workflow_id: str, version: str) -> Result:
        return self._client.request("POST", f"/v1/workflows/{workflow_id}/versions/{version}/rollback")
