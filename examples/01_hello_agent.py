#!/usr/bin/env python3
"""01 — hello agent: register an agent, submit one execution, print the result.

Run against a dev stack (`scripts/e2e-smoke.sh` first):

    export MAS_API_URL=http://localhost:8080 MAS_API_TOKEN=dev-token
    export MAS_TENANT_ID=<uuid> MAS_ORGANIZATION_ID=<uuid>
    python examples/01_hello_agent.py
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

    registered = client.request(
        "POST",
        "/v1/agents",
        {
            "name": "Hello Agent",
            "slug": f"hello-agent-{os.getpid()}",
            "kind": "standard",
            "description": "minimal runnable example",
        },
    )
    if not registered.ok:
        print(f"agent registration refused: {registered.code} {registered.message}", file=sys.stderr)
        return 1
    agent_id = registered.data["id"]
    print(f"registered agent {agent_id}")

    submitted = client.submit_execution(agent_id=agent_id, input="Say hello in one sentence.")
    if not submitted.ok:
        print(f"submit refused: {submitted.code} {submitted.message}", file=sys.stderr)
        return 1
    execution_id = submitted.data["execution_id"]
    print(f"submitted execution {execution_id} (status={submitted.data['status']})")

    result = client.get_execution(execution_id)
    print(f"execution state: {result.data}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
