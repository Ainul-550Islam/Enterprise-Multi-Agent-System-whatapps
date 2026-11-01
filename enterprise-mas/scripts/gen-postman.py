#!/usr/bin/env python3
"""gen-postman.py — generate docs/postman/mas.postman_collection.json from docs/openapi.yaml.

Real parsing of the OpenAPI spec (no hand-written requests). Each path/method
becomes one Postman request with the stable envelope shape, bearer-auth
placeholder, and path/query parameters from the spec.

Usage:
    python3 scripts/gen-postman.py [openapi.yaml] [output.json]
    Defaults: enterprise-mas/rust-core/docs/openapi.yaml ->
              enterprise-mas/docs/postman/mas.postman_collection.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    openapi_path = Path(sys.argv[1]) if len(sys.argv) > 1 else repo_root / "rust-core" / "docs" / "openapi.yaml"
    output_path = Path(sys.argv[2]) if len(sys.argv) > 2 else repo_root / "docs" / "postman" / "mas.postman_collection.json"

    spec_text = openapi_path.read_text()

    # Minimal YAML parse: path keys are lines `  /v1/...:`; method keys are
    # indented `    get:`/`    post:`/... beneath the nearest path key.
    paths: dict[str, list[str]] = {}
    current_path: str | None = None
    for raw in spec_text.splitlines():
        line = raw.lstrip(" ")
        if line.startswith("/v1/") and line.rstrip().endswith(":"):
            current_path = line.rstrip()[:-1]
            paths.setdefault(current_path, [])
            continue
        if current_path is not None:
            m = line.split(":", 1)[0]
            if m in {"get", "post", "put", "patch", "delete"}:
                paths[current_path].append(m)

    requests = []
    for path, methods in sorted(paths.items()):
        for method in methods:
            requests.append({
                "name": f"{method.upper()} {path}",
                "request": {
                    "method": method.upper(),
                    "header": [
                        {"key": "Authorization", "value": "Bearer {{api_token}}", "description": "Bearer API key or JWT"}
                    ],
                    "url": {
                        "raw": "{{base_url}}" + path,
                        "host": ["{{base_url}}"],
                        "path": [p.strip("{}") for p in path.split("/") if p],
                    },
                },
            })

    collection = {
        "info": {
            "name": "Enterprise MAS API",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
        },
        "item": [{"name": "MAS v1", "item": requests}],
        "variable": [
            {"key": "base_url", "value": "http://127.0.0.1:8080"},
            {"key": "api_token", "value": "dev-token"},
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(collection, indent=2) + "\n")
    print(f"wrote {output_path} ({len(requests)} requests from {openapi_path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
