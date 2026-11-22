"""Execution operations (openapi: /v1/executions)."""

from __future__ import annotations

from typing import Any

from mas_sdk.client import MasClient, Result


class Executions:
    """Typed access to execution submission and lifecycle."""

    def __init__(self, client: MasClient) -> None:
        self._client = client

    def submit(self, project_id: str, body: dict[str, Any], idempotency_key: str | None = None) -> Result:
        return self._client.request(
            "POST", f"/v1/projects/{project_id}/executions", body, idempotency_key=idempotency_key
        )

    def list(self, *, limit: int = 50, cursor: str | None = None) -> Result:
        query = f"?limit={limit}" + (f"&cursor={cursor}" if cursor else "")
        return self._client.request("GET", f"/v1/executions{query}")

    def get(self, execution_id: str) -> Result:
        return self._client.request("GET", f"/v1/executions/{execution_id}")

    def steps(self, execution_id: str) -> Result:
        return self._client.request("GET", f"/v1/executions/{execution_id}/steps")

    def transition(self, execution_id: str, signal: str) -> Result:
        return self._client.request("POST", f"/v1/executions/{execution_id}/{signal}")

    def signal(self, execution_id: str, signal: str) -> Result:
        return self._client.request("POST", f"/v1/executions/{execution_id}/signal", {"signal": signal})
