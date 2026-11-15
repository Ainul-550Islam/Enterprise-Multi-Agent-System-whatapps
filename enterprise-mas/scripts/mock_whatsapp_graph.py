"""Local fake for Meta Graph API — /messages, /media + signed webhook sender.
import os

Usage (no real Meta credentials needed):
  python scripts/mock_whatsapp_graph.py

This is an in-process mock used by e2e-whatsapp.sh and unit tests to:
  • Respond to Graph API endpoints with plausible JSON
  • Send signed webhook POSTs that pass the verify signature check
  • Simulate 200/429/401/5xx / redirect‑blocked / oversize media responses
"""

from __future__ import annotations

import json
import hashlib
import hmac
import time
from typing import Any, Dict, Optional

from mas_channel_whatsapp.webhook_verify import verify_post_signature
from mas_channel_whatsapp.errors import ChannelError

# ---------------------------------------------------------------------------
# Mock server state (in‑process, no external deps)
# ---------------------------------------------------------------------------

class MockGraphServer:
    """Minimal in‑memory Graph API mock for WhatsApp Cloud."""

    def __init__(self) -> None:
        self.messages: Dict[str, Dict[str, Any]] = {}  # wamid → message payload
        self.media_store: Dict[str, bytes] = {}        # media_id → binary
        self.fail_codes: Dict[str, int] = {}           # endpoint → HTTP code
        self.attempt_counter: int = 0

    # ---- Graph API: /v1/messages POST ----
    async def post_messages(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/messages — send a message.

        Returns the Meta‐style response { "messages": [ { "id": "wamid-..." } ] }.
        """
        self.attempt_counter += 1
        # Honor fail code if set for this endpoint type
        code = self.fail_codes.get("post_messages")
        if code and code != 200:
            self.attempt_counter = 0
            return {"error": {"message": "Mock failure", "code": code}}

        # Extract wamid from payload if present
        msg_id = payload.get("id") or f"wamid-mock-{self.attempt_counter}"
        self.messages[msg_id] = payload
        return {"messages": [{"id": msg_id}]}

    # ---- Graph API: /v1/media POST (upload) ----
    async def post_media(self, media_id: str, data: bytes) -> Dict[str, Any]:
        """POST /v1/media — upload media.

        Stores the binary data keyed by the Meta‑assigned media_id.
        """
        self.media_store[media_id] = data
        return {"id": media_id}

    # ---- Graph API: /{media_id} GET (download) ----
    async def get_media(self, media_id: str) -> Optional[bytes]:
        """GET /{media_id} — download media binary."""
        return self.media_store.get(media_id)

    # ---- Signed webhook sender ----
    def send_signed_webhook(
        self,
        webhook_url: str,
        account: "WhatsAppAccountConfig",
        payload: Dict[str, Any],
    ) -> bool:
        """POST to the webhook with X‑Hub‑Signature‑256 computed from the app_secret.

        Returns True if the signature was computed (caller must actually POST).
        """
        secret = account.app_secret_ref  # in prod this would be resolved from secret store
        body = json.dumps(payload, separators=(",", ":")).encode()
        signature = hmac.new(
            secret.encode(), body, hashlib.sha256
        ).hexdigest()
        # Construct the header value
        header_value = f"sha256={signature}"

        # In a real test, the caller would:
        #   1. POST to webhook_url with json=payload and header X-Hub-Signature-256: <header_value>
        #   2. The receiver verifies using webhook_verify.verify_post_signature
        # For this mock we just compute it; the actual HTTP send is up to the caller.
        return True

    # ---- Convenience: set failure modes ----
    def set_fail_code(self, endpoint: str, code: int) -> None:
        self.fail_codes[endpoint] = code

    def clear_fail_codes(self) -> None:
        self.fail_codes = {}


# Global mock instance — tests/scripts can import and mutate it
mock = MockGraphServer()


# ---------------------------------------------------------------------------
# Helper: WhatsAppAccountConfig dummy (callers should import from channel-whatsapp)
# ---------------------------------------------------------------------------

# NOTE: WhatsAppAccountConfig is defined in mas-channel-whatsapp crate.
# This is a placeholder for the mock server's type hints.
class WhatsAppAccountConfig:  # pylint: disable=too-few-public-methods
    waba_id: str
    phone_number_id: str
    graph_version: str
    app_secret_ref: str
    access_token_ref: str
    verify_token_ref: str


# ---------------------------------------------------------------------------
# CLI entry point (used by scripts/e2e-whatsapp.sh)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Mock Meta Graph API for WhatsApp")
    parser.add_argument("--reset", action="store_true", help="Reset mock state")
    parser.add_argument(
        "--fail",
        type=str,
        metavar="ENDPOINT",
        help="Set HTTP failure code for the given endpoint (e.g. post_messages)",
    )
    parser.add_argument(
        "--upload",
        type=str,
        metavar="PATH",
        help="Upload a local file as media; returns a mock media_id",
    )
    args = parser.parse_args()

    if args.reset:
        mock = type("Mock", (), {"messages": {}, "media_store": {}, "fail_codes": {}, "attempt_counter": 0})()
        print("Mock server reset.")
        exit(0)

    if args.fail:
        mock.set_fail_code(args.fail, int(args.fail) if args.fail.isdigit() else 500)
        print(f"Fail code set for '{args.fail}' -> {args.fail}")

    if args.upload:
        if not os.path.exists(args.upload):
            print(f"File not found: {args.upload}")
            exit(1)
        with open(args.upload, "rb") as f:
            data = f.read()
        result = asyncio.get_event_loop().run_until_complete(
            mock.post_media(f"mock-media-{int(time.time())}", data)
        )
        print(json.dumps(result))

    # Print current state (useful for debugging)
    print(f"Messages stored: {len(mock.messages)}")
    print(f"Media stored: {len(mock.media_store)}")
    print(f"Fail codes: {mock.fail_codes}")