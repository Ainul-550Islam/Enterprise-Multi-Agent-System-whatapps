"""Billing operations (openapi: /v1/billing/*)."""

from __future__ import annotations

from typing import Any

from mas_sdk.client import MasClient, Result


class Billing:
    """Typed access to checkout, portal and billing evidence."""

    def __init__(self, client: MasClient) -> None:
        self._client = client

    def checkout(self, body: dict[str, Any]) -> Result:
        return self._client.request("POST", "/v1/billing/checkout", body)

    def portal_session(self, body: dict[str, Any]) -> Result:
        return self._client.request("POST", "/v1/billing/portal-session", body)

    def subscription(self) -> Result:
        return self._client.request("GET", "/v1/billing/subscription")

    def entitlements(self) -> Result:
        return self._client.request("GET", "/v1/billing/entitlements")

    def usage_totals(self) -> Result:
        return self._client.request("GET", "/v1/billing/usage-totals")

    def receipts(self) -> Result:
        return self._client.request("GET", "/v1/billing/receipts")

    def receipt(self, provider: str, event_id: str) -> Result:
        return self._client.request("GET", f"/v1/billing/receipts/{provider}/{event_id}")
