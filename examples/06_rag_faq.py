#!/usr/bin/env python3
"""06 — RAG FAQ: register a retriever-shaped agent and search the catalogue.

Empty retrieval is printed as empty. This example never fabricates hits.

    export MAS_API_URL=... MAS_API_TOKEN=... MAS_TENANT_ID=... MAS_ORGANIZATION_ID=...
    python examples/06_rag_faq.py
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

    sources = client.request("GET", "/v1/data-sources")
    print(f"data-sources: ok={sources.ok} data={sources.data}")

    hits = client.request("GET", "/v1/search?q=refund%20policy")
    print(f"search: ok={hits.ok} code={hits.code} data={hits.data}")

    agent = client.request(
        "POST",
        "/v1/agents",
        {
            "name": "RAG FAQ",
            "slug": f"rag-faq-{os.getpid()}",
            "kind": "standard",
            "description": "Answers only from retrieved documents",
        },
    )
    if not agent.ok:
        print(f"agent registration refused: {agent.code} {agent.message}", file=sys.stderr)
        return 1
    submitted = client.submit_execution(
        agent_id=agent.data["id"],
        input="What is the refund policy? Cite documents or say you do not know.",
    )
    print(f"submit: ok={submitted.ok} code={submitted.code} data={submitted.data}")
    return 0 if submitted.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
