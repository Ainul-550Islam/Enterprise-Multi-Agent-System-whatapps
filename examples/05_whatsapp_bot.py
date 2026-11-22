#!/usr/bin/env python3
"""05 — WhatsApp bot: inspect channels, templates and send a text reply.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    export MAS_WHATSAPP_CHANNEL_ID=... MAS_WHATSAPP_TO=+8801712345678
    python examples/05_whatsapp_bot.py

See examples/05_whatsapp_bot.md for the end-to-end webhook architecture.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from mas_sdk import MasClient, Scope  # noqa: E402
from mas_sdk.resources.whatsapp import WhatsApp  # noqa: E402


def main() -> int:
    scope = Scope(
        tenant_id=os.environ["MAS_TENANT_ID"],
        organization_id=os.environ["MAS_ORGANIZATION_ID"],
    )
    client = MasClient(
        os.environ.get("MAS_API_URL", "http://localhost:8080"),
        os.environ.get("MAS_API_TOKEN", "dev-token"),
        scope=scope,
    )
    whatsapp = WhatsApp(client)
    channels = whatsapp.channels()
    if not channels.ok:
        print(f"channel lookup refused: {channels.code} {channels.message}", file=sys.stderr)
        return 1
    print(f"channels: {channels.data}")

    channel_id = os.environ.get("MAS_WHATSAPP_CHANNEL_ID")
    recipient = os.environ.get("MAS_WHATSAPP_TO")
    if not channel_id or not recipient:
        print("set MAS_WHATSAPP_CHANNEL_ID and MAS_WHATSAPP_TO to send a reply")
        return 0
    sent = whatsapp.send_text(channel_id, recipient, "Hello from Enterprise MAS")
    if not sent.ok:
        print(f"send refused: {sent.code} {sent.message}", file=sys.stderr)
        return 1
    print(f"sent: {sent.data}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
