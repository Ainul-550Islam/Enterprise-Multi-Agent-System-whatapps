#!/usr/bin/env python3
"""08 — webhook trigger: register an outbound webhook and list deliveries.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    python examples/08_webhook_trigger.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from mas_sdk import MasClient, Scope  # noqa: E402


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

    created = client.request(
        "POST",
        "/v1/webhooks",
        {
            "url": "https://example.invalid/mas-webhook",
            "events": ["execution.succeeded", "execution.failed"],
        },
    )
    print(f"create webhook: ok={created.ok} code={created.code} data={created.data}")
    listed = client.request("GET", "/v1/webhooks")
    print(f"list webhooks: ok={listed.ok} data={listed.data}")
    inbound = client.request("GET", "/v1/inbound-webhooks")
    print(f"inbound webhooks: ok={inbound.ok} data={inbound.data}")
    return 0 if created.ok or listed.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
