#!/usr/bin/env python3
"""02 — run a workflow: push a validated graph, run it, follow the outcome.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    python examples/02_run_workflow.py
"""

from __future__ import annotations

import os
import sys
import time
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

    workflow = client.request(
        "POST",
        "/v1/workflows",
        {
            "name": "Example Pipeline",
            "slug": f"example-pipeline-{os.getpid()}",
            "graph": {
                "nodes": [
                    {"id": "collect", "type": "agent_step", "config": {"agent_slug": "hello-agent"}},
                    {"id": "summarise", "type": "agent_step", "config": {"agent_slug": "hello-agent"}},
                ],
                "edges": [{"from": "collect", "to": "summarise"}],
            },
        },
    )
    if not workflow.ok:
        print(f"workflow refused: {workflow.code} {workflow.message}", file=sys.stderr)
        return 1
    workflow_id = workflow.data["id"]
    print(f"workflow {workflow_id} registered")

    started = client.request("POST", f"/v1/workflows/{workflow_id}/runs", {"input": "demo input"})
    if not started.ok:
        print(f"run refused: {started.code} {started.message}", file=sys.stderr)
        return 1
    run_id = started.data["run_id"]
    print(f"run {run_id} started")

    for _ in range(30):
        state = client.request("GET", f"/v1/workflows/{workflow_id}/runs/{run_id}")
        status = (state.data or {}).get("status", "unknown")
        print(f"  run status: {status}")
        if status in {"succeeded", "failed", "cancelled"}:
            return 0 if status == "succeeded" else 1
        time.sleep(1)
    print("run did not reach a terminal state in 30s", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
