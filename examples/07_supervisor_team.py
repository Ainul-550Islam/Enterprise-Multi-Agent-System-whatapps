#!/usr/bin/env python3
"""07 — supervisor team: two agents plus a workflow that sequences them.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    python examples/07_supervisor_team.py
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
    pid = os.getpid()
    planner = client.request(
        "POST",
        "/v1/agents",
        {
            "name": "Research planner",
            "slug": f"research-planner-{pid}",
            "kind": "standard",
            "description": "Breaks a question into search steps",
        },
    )
    worker = client.request(
        "POST",
        "/v1/agents",
        {
            "name": "Research worker",
            "slug": f"research-worker-{pid}",
            "kind": "standard",
            "description": "Executes one search step",
        },
    )
    if not planner.ok or not worker.ok:
        print(f"agent refused: planner={planner.code} worker={worker.code}", file=sys.stderr)
        return 1

    workflow = client.request(
        "POST",
        "/v1/workflows",
        {
            "name": "Research team",
            "slug": f"research-team-{pid}",
            "graph": {
                "nodes": [
                    {
                        "id": "plan",
                        "type": "agent_step",
                        "config": {"agent_slug": planner.data.get("slug", f"research-planner-{pid}")},
                    },
                    {
                        "id": "search",
                        "type": "agent_step",
                        "config": {"agent_slug": worker.data.get("slug", f"research-worker-{pid}")},
                    },
                ],
                "edges": [{"from": "plan", "to": "search"}],
            },
        },
    )
    print(f"workflow: ok={workflow.ok} code={workflow.code} data={workflow.data}")
    return 0 if workflow.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
