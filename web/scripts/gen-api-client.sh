#!/usr/bin/env bash
# PURPOSE: generate src/lib/api/schema.d.ts from rust-core/docs/openapi.yaml
# via openapi-typescript. A throwaway copy rewrites the dangling
# `#/components/responses/Forbidden` refs (the spec names that response
# `Error`) so the generator can run; the committed OpenAPI files are not
# rewritten here.
set -euo pipefail

WEB="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="$(cd "${WEB}/.." && pwd)"
SPEC="${ROOT}/enterprise-mas/rust-core/docs/openapi.yaml"
OUT="${WEB}/src/lib/api/schema.d.ts"
BIN="${WEB}/node_modules/.bin/openapi-typescript"

if [ ! -f "${SPEC}" ]; then
  echo "gen-api-client: openapi spec missing at ${SPEC}" >&2
  exit 1
fi
if [ ! -x "${BIN}" ]; then
  echo "gen-api-client: openapi-typescript is not installed (run npm ci)" >&2
  exit 1
fi

mkdir -p "$(dirname "${OUT}")"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
python3 - "${SPEC}" "${TMP}/openapi.yaml" <<'PY'
from pathlib import Path
import sys
src, dest = sys.argv[1], sys.argv[2]
text = Path(src).read_text()
text = text.replace("#/components/responses/Forbidden", "#/components/responses/Error")
Path(dest).write_text(text)
PY

"${BIN}" "${TMP}/openapi.yaml" -o "${OUT}"
echo "gen-api-client: wrote ${OUT}"
