#!/usr/bin/env python3
"""04 — quota and usage: read the quota snapshot and usage ledger honestly.

Empty catalogue answers are truthful — this example prints exactly what the
platform reports and never fabricates numbers.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    export MAS_PROJECT_ID=<project-uuid>
    python examples/04_quota_and_usage.py
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

    quotas = client.request("GET", "/v1/quotas")
    print(f"quotas: ok={quotas.ok} data={quotas.data}")

    usage = client.request("GET", "/v1/usage")
    print(f"usage: ok={usage.ok} data={usage.data}")

    totals = client.request("GET", "/v1/billing/usage-totals")
    print(f"billing usage-totals: ok={totals.ok} data={totals.data}")

    # a refused over-quota submit is a quota answer, not a crash — show it
    demo = client.submit_execution(project_id=os.environ["MAS_PROJECT_ID"], agent_id="hello-agent", input={"prompt": "quota demo"})
    print(f"demo submit: ok={demo.ok} code={demo.code} status={(demo.data or {}).get('status')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
