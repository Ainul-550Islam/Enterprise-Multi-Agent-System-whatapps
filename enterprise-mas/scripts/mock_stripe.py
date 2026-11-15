"""Local fake Stripe API — /customers, /checkout/sessions, /portal/sessions,
/subscriptions, /invoices + signed event sender for e2e tests.

Usage (no real Stripe credentials needed):
  python scripts/mock_stripe.py

This is an in-process mock used by e2e-tests and unit tests to:
  • Respond to Stripe API endpoints with plausible JSON
  • Send signed events that pass the signature verification check
  • Simulate 200/402/429/5xx responses
  • Persist in‑memory state for a single process lifetime
"""

from __future__ import annotations

import json
import hashlib
import hmac
import time
import uuid
from typing import Any, Dict, Optional

try:
    import anyio
    HAS_ASYNC = True
except ImportError:
    HAS_ASYNC = False

# ---------------------------------------------------------------------------
# Mock server state (in‑process, no external deps)
# ---------------------------------------------------------------------------

class MockStripeServer:
    """Minimal in‑memory Stripe API mock for testing."""

    def __init__(self) -> None:
        self.customers: Dict[str, Dict[str, Any]] = {}  # customer_id → metadata
        self.checkouts: Dict[str, Dict[str, Any]] = {}  # session_id → session
        self.portals: Dict[str, Dict[str, Any]] = {}    # portal_session_id → session
        self.subscriptions: Dict[str, Dict[str, Any]] = {}  # sub_id → sub
        self.invoices: Dict[str, Dict[str, Any]] = {}   # invoice_id → invoice
        self.events: list[Dict[str, Any]] = []          # stored events
        self.fail_codes: Dict[str, int] = {}            # endpoint → HTTP code
        self.attempt_counter: int = 0

    # ---- API: POST /v1/customers ----
    async def post_customers(self, form_data: dict[str, Any]) -> Dict[str, Any]:
        self.attempt_counter += 1
        code = self.fail_codes.get("post_customers")
        if code and code != 200:
            self.attempt_counter = 0
            return {"error": {"message": "Mock failure", "code": code}}

        # Extract email from form data
        email = form_data.get("email", f"test_{self.attempt_counter}@example.com")
        name = form_data.get("name")

        cust_id = f"cus_{self.attempt_counter}_{uuid.uuid4().hex[:8]}"
        self.customers[cust_id] = {
            "id": cust_id,
            "email": email,
            "name": name or "Test Customer",
            "object": "customer",
        }
        return {"id": cust_id, "object": "customer", "email": email, "name": name or "Test Customer"}

    # ---- API: POST /v1/checkout/sessions ----
    async def post_checkout_sessions(self, form_data: dict[str, Any]) -> Dict[str, Any]:
        self.attempt_counter += 1
        code = self.fail_codes.get("post_checkout_sessions")
        if code and code != 200:
            self.attempt_counter = 0
            return {"error": {"message": "Mock failure", "code": code}}

        price_id = form_data.get("line_items", [{}])[0].get("price", "price_placeholder")
        mode = form_data.get("mode", "subscription")
        success_url = form_data.get("success_url", "https://example.com/success")
        cancel_url = form_data.get("cancel_url", "https://example.com/cancel")
        customer = form_data.get("customer", "cus_placeholder")

        session_id = f"cs_{self.attempt_counter}_{uuid.uuid4().hex[:8]}"
        self.checkouts[session_id] = {
            "id": session_id,
            "object": "checkout.session",
            "url": f"https://checkout.stripe.com/session/{session_id}",
            "url": success_url,
            "mode": mode,
            "success_url": success_url,
            "cancel_url": cancel_url,
            "customer": customer,
            "payment_status": "pending",
        }
        return {"id": session_id, "object": "checkout.session", "url": success_url, "mode": mode}

    # ---- API: POST /v1 portal sessions ----
    async def post_portal_sessions(self, form_data: dict[str, Any]) -> Dict[str, Any]:
        self.attempt_counter += 1
        code = self.fail_codes.get("post_portal_sessions")
        if code and code != 200:
            self.attempt_counter = 0
            return {"error": {"message": "Mock failure", "code": code}}

        customer = form_data.get("customer", "cus_placeholder")
        return_url = form_data.get("return_url", "https://example.com/return")

        portal_id = f"po_{self.attempt_counter}_{uuid.uuid4().hex[:4]}"
        self.portals[portal_id] = {
            "id": portal_id,
            "object": "billing_portal.session",
            "url": f"https://app.stripe.com/billing/portal/{portal_id}",
            "customer": customer,
            "return_url": return_url,
        }
        return {"id": portal_id, "object": "billing_portal.session", "url": f"https://app.stripe.com/billing/portal/{portal_id}"}

    # ---- API: GET /v1/subscriptions/{sub_id} ----
    async def get_subscriptions(self, sub_id: str) -> Dict[str, Any]:
        self.attempt_counter += 1
        code = self.fail_codes.get("get_subscriptions")
        if code and code != 200:
            self.attempt_counter = 0
            return {"error": {"message": "Mock failure", "code": code}}

        # Return existing or create a minimal one
        if sub_id not in self.subscriptions:
            self.subscriptions[sub_id] = {
                "id": sub_id,
                "object": "subscription",
                "status": "active",
                "customer": "cus_placeholder",
                "items": [{"price": "price_placeholder", "quantity": 1}],
                "current_period_start": int(time.time()),
                "current_period_end": int(time.time()) + 30 * 24 * 3600,
            }
        return self.subscriptions[sub_id]

    # ---- API: POST /v1/invoices/{invoice_id}/pay ----
    async def post_invoices_pay(self, invoice_id: str) -> Dict[str, Any]:
        self.attempt_counter += 1
        code = self.fail_codes.get("post_invoices_pay")
        if code and code != 200:
            self.attempt_counter = 0
            return {"error": {"message": "Mock failure", "code": code}}

        if invoice_id not in self.invoices:
            self.invoices[invoice_id] = {
                "id": invoice_id,
                "object": "invoice",
                "customer": "cus_placeholder",
                "status": "paid",
                "amount_paid": 1999,
                "currency": "usd",
            }
        return self.invoices[invoice_id]

    # ---- Signed event sender ----
    def compute_signature(self, secret: str, timestamp: int, body: bytes) -> str:
        """Compute the Stripe signature:  hmac_sha256(t + raw_body, secret)"""
        message = f"{timestamp}{body.decode()}".encode()
        mac = hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()
        return mac

    def send_signed_event(
        self,
        webhook_url: str,
        secret: str,
        event_type: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """Build a signed Stripe event POST.

        Returns the (event_json, signature_header) tuple that the caller
        should POST to webhook_url with header X-Stripe-Signature: t=...,v1=...
        """
        self.attempt_counter += 1
        timestamp = int(time.time())
        event_obj = {
            "id": f"evt_{self.attempt_counter}_{uuid.uuid4().hex[:8]}",
            "object": "event",
            "type": event_type,
            "data": {"object": "", "id": ""},
            "created": timestamp,
        }
        # Attach reasonable data based on event type
        if event_type == "invoice.paid":
            event_obj["data"] = {
                "object": "invoice",
                "id": f"in_{self.attempt_counter}",
                "customer": "cus_placeholder",
                "status": "paid",
                "amount_paid": 1999,
                "currency": "usd",
            }
        elif event_type == "customer.subscription.created":
            event_obj["data"] = {
                "object": "subscription",
                "id": f"sub_{self.attempt_counter}",
                "customer": "cus_placeholder",
                "status": "active",
            }
        body = json.dumps(event_obj, separators=(",", ":"))
        sig = self.compute_signature(secret, timestamp, body.encode())

        # Construct the signature header:  t=timestamp,v1=sig
        header_value = f"t={timestamp},v1={sig}"

        # In a real test, the caller would:
        #   POST to webhook_url with json=event_obj and header X-Stripe-Signature: <header_value>
        # The receiver verifies using the signature module.
        return {"event": event_obj, "signature_header": header_value, "body": body}

    # ---- Convenience: set failure modes ----
    def set_fail_code(self, endpoint: str, code: int) -> None:
        self.fail_codes[endpoint] = code

    def clear_fail_codes(self) -> None:
        self.fail_codes = {}


# Global mock instance — tests/scripts can import and mutate it
mock = MockStripeServer()

# ---------------------------------------------------------------------------
# CLI entry point (used by scripts/e2e-stripe.sh)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Mock Stripe API for WhatsApp/MAS e2e")
    parser.add_argument("--reset", action="store_true", help="Reset mock state")
    parser.add_argument(
        "--fail",
        type=str,
        metavar="ENDPOINT",
        help="Set HTTP failure code for the given endpoint (e.g. post_customers)",
    )
    parser.add_argument(
        "--upload",
        type=str,
        metavar="PATH",
        help="Not used for Stripe mock (kept for parity with WhatsApp mock)",
    )
    args = parser.parse_args()

    if args.reset:
        mock = type("Mock", (), {
            "customers": {},
            "checkouts": {},
            "portals": {},
            "subscriptions": {},
            "invoices": {},
            "events": [],
            "fail_codes": {},
            "attempt_counter": 0,
        })()
        print("Mock server reset.")
        exit(0)

    if args.fail:
        mock.set_fail_code(args.fail, int(args.fail) if args.fail.isdigit() else 500)
        print(f"Fail code set for '{args.fail}' -> {args.fail}")

    # Print current state (useful for debugging)
    print(f"Customers: {len(mock.customers)}")
    print(f"Checkouts: {len(mock.checkouts)}")
    print(f"Portals: {len(mock.portals)}")
    print(f"Subscriptions: {len(mock.subscriptions)}")
    print(f"Invoices: {len(mock.invoices)}")
    print(f"Fail codes: {mock.fail_codes}")
PYEOF
chmod +x /home/user/Enterprise-Multi-Agent-System-whatapps/enterprise-mas/scripts/mock_stripe.py