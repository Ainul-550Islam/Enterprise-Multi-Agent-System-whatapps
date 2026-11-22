#!/usr/bin/env python3
"""03 — stream events: follow the SSE stream while an execution runs.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    export MAS_PROJECT_ID=<project-uuid>
    python examples/03_stream_events.py
"""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sdk" / "python"))

from mas_sdk import MasClient, Scope  # noqa: E402
from mas_sdk.client import parse_event_frame  # noqa: E402


def main() -> int:
    scope = Scope(
        tenant_id=os.environ["MAS_TENANT_ID"],
        organization_id=os.environ["MAS_ORGANIZATION_ID"],
    )
    token = os.environ.get("MAS_API_TOKEN", "dev-token")
    base = os.environ.get("MAS_API_URL", "http://localhost:8080").rstrip("/")
    client = MasClient(base, token, scope=scope)

    submitted = client.submit_execution(project_id=os.environ["MAS_PROJECT_ID"], agent_id="hello-agent", input={"prompt": "stream me"})
    if not submitted.ok:
        print(f"submit refused: {submitted.code} {submitted.message}", file=sys.stderr)
        return 1
    execution_id = submitted.data["id"]
    print(f"following execution {execution_id} on /v1/stream")

    request = urllib.request.Request(
        f"{base}/v1/stream?execution_id={execution_id}",
        headers={
            "authorization": f"Bearer {token}",
            "x-tenant-id": scope.tenant_id,
            "x-organization-id": scope.organization_id,
            "accept": "text/event-stream",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
        buffer = ""
        while True:
            chunk = response.read(256).decode("utf-8", "replace")
            if not chunk:
                break
            buffer += chunk
            while "\n\n" in buffer:
                raw, buffer = buffer.split("\n\n", 1)
                frame = parse_event_frame(raw)
                print(f"event={frame['event']} data={frame['data'][:120]}")
                if frame["event"] in {"execution.finished", "execution.failed"}:
                    payload = json.loads(frame["data"] or "{}")
                    print(f"terminal event for {payload.get('execution_id', execution_id)}")
                    return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
