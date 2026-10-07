#!/usr/bin/env bash
#
# Regenerates the Python protobuf bindings from the authoritative Rust
# protobuf contract.
#
# The `.proto` file lives in the Rust workspace because Rust owns the wire
# contract. Python is a consumer: it must never hand-edit the generated
# bindings, and this script is the only sanctioned way to refresh them.
#
# Generated files are committed (see .gitignore exceptions) so that a Python
# build does not require a Rust checkout or a protoc toolchain.
#
# Usage:
#   ./scripts/generate_proto.sh [path/to/rust-core]
#
# Exits non-zero if generation fails or if the proto cannot be located; it
# never leaves a half-written binding behind.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# The Python package and Rust workspace are siblings in the repository.
# Preserve the old packaged-checkout location as a fallback for existing builds.
DEFAULT_RUST_CORE="${ROOT_DIR}/../rust-core"
LEGACY_RUST_CORE="${ROOT_DIR}/../repo/enterprise-mas/rust-core"
if [[ -n "${1:-}" ]]; then
  RUST_CORE="$1"
elif [[ -d "${DEFAULT_RUST_CORE}" ]]; then
  RUST_CORE="${DEFAULT_RUST_CORE}"
else
  RUST_CORE="${LEGACY_RUST_CORE}"
fi

PROTO_REL="crates/api/proto/mas/api/v1/mas.proto"
PROTO_PATH="${RUST_CORE}/${PROTO_REL}"
PROTO_ROOT="${RUST_CORE}/crates/api/proto"
OUT_DIR="${ROOT_DIR}/src/mas_orchestrator/grpc/generated"

if [[ ! -f "${PROTO_PATH}" ]]; then
  echo "error: authoritative proto not found at ${PROTO_PATH}" >&2
  echo "hint: pass the rust-core directory explicitly:" >&2
  echo "  ${0} /path/to/enterprise-mas/rust-core" >&2
  exit 1
fi

if ! python3 -c "import grpc_tools.protoc" >/dev/null 2>&1; then
  echo "error: grpcio-tools is not installed" >&2
  echo "hint: python3 -m pip install 'grpcio-tools>=1.84'" >&2
  exit 1
fi

echo "==> generating from ${PROTO_PATH}"
mkdir -p "${OUT_DIR}"

# Generate into a staging directory first: if protoc fails partway through we
# keep the previous, known-good bindings instead of shipping a corrupt module.
STAGING="$(mktemp -d)"
trap 'rm -rf "${STAGING}"' EXIT

python3 -m grpc_tools.protoc \
  --proto_path="${PROTO_ROOT}" \
  --python_out="${STAGING}" \
  --grpc_python_out="${STAGING}" \
  --pyi_out="${STAGING}" \
  "${PROTO_PATH}"

# protoc preserves the proto package path; flatten into the generated package.
find "${STAGING}" -name '*.py' -o -name '*.pyi' | while read -r generated; do
  cp "${generated}" "${OUT_DIR}/$(basename "${generated}")"
done

# protoc emits imports that follow the proto package path
# (`from mas.api.v1 import mas_pb2 ...`). Because we flatten the tree into a
# single package, those absolute imports must become package-relative or the
# generated service stubs are unimportable. This rewrite is mechanical and is
# re-applied on every regeneration, so the generated files stay untouched
# by hand.
find "${OUT_DIR}" -name '*_pb2_grpc.py' -print0 | while IFS= read -r -d '' stub; do
  python3 - "${stub}" <<'REWRITE'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
# `from mas.api.v1 import mas_pb2 as mas__pb2` -> `from . import mas_pb2 as mas__pb2`
rewritten = re.sub(
    r"^from\s+[\w.]+\s+import\s+(\w+_pb2)\s+as\s+",
    r"from . import \1 as ",
    text,
    flags=re.MULTILINE,
)
if rewritten != text:
    path.write_text(rewritten, encoding="utf-8")
    print(f"rewrote package imports in {path.name}")
REWRITE
done

# The generated package needs an __init__.py so it is importable and so the
# mypy/ruff overrides for `mas_orchestrator.grpc.generated.*` apply.
# protoc names the module after the file (`mas.proto` -> `mas_pb2.py`), while
# the documented contract name is `mas_api_pb2`. Rather than hand-editing the
# generated file (which would be lost on regeneration), the package re-exports
# it under the documented alias. BASE is read back from what protoc produced.
BASE="$(find "${STAGING}" -name '*_pb2.py' -exec basename {} .py \; | head -1)"
if [[ -z "${BASE}" ]]; then
  echo "error: protoc produced no *_pb2.py module" >&2
  exit 1
fi
SERVICE_BASE="$(find "${STAGING}" -name '*_pb2_grpc.py' -exec basename {} .py \; | head -1)"

cat >"${OUT_DIR}/__init__.py" <<PY
"""Generated protobuf bindings — DO NOT EDIT.

Produced by \`\`scripts/generate_proto.sh\`\` from the authoritative Rust
protobuf contract at \`\`crates/api/proto/mas/api/v1/mas.proto\`\`.

Rust owns the wire contract. To change these definitions, change the \`\`.proto\`\`
in the Rust workspace and re-run the generator; hand edits will be lost on the
next regeneration and will silently break protocol compatibility.

Lint and type-check rules are relaxed for this package because the code is
machine-generated and does not follow project style.
"""

from __future__ import annotations

from . import ${BASE} as mas_api_pb2
from . import ${SERVICE_BASE} as mas_api_pb2_grpc

__all__ = ["mas_api_pb2", "mas_api_pb2_grpc"]
PY

echo "==> generated:"
ls -1 "${OUT_DIR}"
echo "==> done"
