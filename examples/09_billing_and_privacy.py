#!/usr/bin/env python3
"""09 — billing and privacy: read entitlements and submit an erasure request.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    python examples/09_billing_and_privacy.py

Provider secrets and price ids are operator-managed; this example never sends them.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from mas_sdk import MasClient, Scope  # noqa: E402
from mas_sdk.resources.billing import Billing  # noqa: E402


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
    billing = Billing(client)
    entitlements = billing.entitlements()
    if not entitlements.ok:
        print(f"entitlements refused: {entitlements.code} {entitlements.message}", file=sys.stderr)
        return 1
    print(f"entitlements: {entitlements.data}")

    contact = os.environ.get("MAS_PRIVACY_CONTACT_REF")
    if not contact:
        print("set MAS_PRIVACY_CONTACT_REF to submit an erasure request")
        return 0
    erased = client.request("POST", "/v1/privacy/erase", {"contact_ref": contact})
    if not erased.ok:
        print(f"erasure refused: {erased.code} {erased.message}", file=sys.stderr)
        return 1
    print(f"erasure request: {erased.data}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
