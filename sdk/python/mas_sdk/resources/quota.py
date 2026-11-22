"""Quota and usage operations (openapi: /v1/quotas, /v1/usage)."""

from __future__ import annotations

from mas_sdk.client import MasClient, Result


class Quota:
    """Typed access to quota reads and usage summaries."""

    def __init__(self, client: MasClient) -> None:
        self._client = client

    def quotas(self) -> Result:
        return self._client.request("GET", "/v1/quotas")

    def usage(self) -> Result:
        return self._client.request("GET", "/v1/usage")

    def summary(self) -> Result:
        return self._client.request("GET", "/v1/usage/summary")
