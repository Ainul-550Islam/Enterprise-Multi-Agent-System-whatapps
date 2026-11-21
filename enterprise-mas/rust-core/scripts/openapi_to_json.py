#!/usr/bin/env python3
"""Regenerate docs/openapi.json from the edited source of truth docs/openapi.yaml.

Usage:  python3 rust-core/scripts/openapi_to_json.py

The API serves the generated JSON at GET /v1/openapi.json (see
crates/api/src/openapi_bundle.rs). After editing docs/openapi.yaml, run this
script and commit both files together — the `openapi_parity` integration test
fails if the YAML, the JSON bundle, and the router disagree.
"""
import json
import pathlib
import sys

try:
    import yaml
except ImportError:  # pragma: no cover
    sys.exit("pyyaml is required: pip install pyyaml")

root = pathlib.Path(__file__).resolve().parents[1]
src = root / "docs" / "openapi.yaml"
dst = root / "docs" / "openapi.json"
spec = yaml.safe_load(src.read_text())
dst.write_text(json.dumps(spec, indent=2, ensure_ascii=False) + "\n")
print(f"wrote {dst}")
