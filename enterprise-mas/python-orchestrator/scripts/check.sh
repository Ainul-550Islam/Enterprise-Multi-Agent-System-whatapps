#!/usr/bin/env bash
#
# Runs every check the Python orchestrator must pass, in the order CI runs them.
#
# Usage:
#   bash scripts/check.sh           # lint + format + types + tests
#   bash scripts/check.sh --fast    # skip the type-check pass
#
# Exits non-zero on the first failing stage. Every stage is meaningful on its
# own, so a failure names the stage rather than dumping four tool outputs.
#
# The protobuf bindings are *verified*, not regenerated: if `mas.proto` changed
# without re-running `scripts/generate_proto.sh`, the bindings are stale and
# that must fail the build here rather than surface as a wire mismatch in
# production.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

FAST=0
if [[ "${1:-}" == "--fast" ]]; then
  FAST=1
fi

# Prefer the venv `bootstrap.sh` created, but fall back to whatever is on PATH
# so the script works in a container that installed the package directly.
if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  PY="${ROOT_DIR}/.venv/bin/python"
else
  PY="${PYTHON_BIN:-python3}"
fi

export PYTHONPATH="${ROOT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

stage() { printf '\n==> %s\n' "$1"; }

stage "ruff (lint)"
"${PY}" -m ruff check src tests

stage "ruff (format)"
"${PY}" -m ruff format --check src tests

if [[ "${FAST}" -eq 0 ]]; then
  stage "mypy (strict)"
  "${PY}" -m mypy src
fi

stage "protobuf bindings are current"
# Regenerating into a temp directory and diffing catches a stale checked-in
# binding without needing the Rust checkout to be buildable; when the proto is
# unreachable the check is skipped with a warning rather than failing spuriously
# on a machine that only has the Python half.
if command -v protoc >/dev/null 2>&1 || "${PY}" -c "import grpc_tools.protoc" >/dev/null 2>&1; then
  BEFORE="$(mktemp)"
  cat src/mas_orchestrator/grpc/generated/mas_pb2.py src/mas_orchestrator/grpc/generated/mas_pb2_grpc.py >"${BEFORE}"
  bash "${SCRIPT_DIR}/generate_proto.sh" >/dev/null
  AFTER="$(mktemp)"
  cat src/mas_orchestrator/grpc/generated/mas_pb2.py src/mas_orchestrator/grpc/generated/mas_pb2_grpc.py >"${AFTER}"
  if ! diff -q "${BEFORE}" "${AFTER}" >/dev/null; then
    echo "error: generated protobuf bindings were stale; they have been regenerated." >&2
    echo "hint: commit src/mas_orchestrator/grpc/generated/ and re-run." >&2
    rm -f "${BEFORE}" "${AFTER}"
    exit 1
  fi
  rm -f "${BEFORE}" "${AFTER}"
  echo "    bindings match mas.proto"
else
  echo "    skipped: no protoc available (install grpcio-tools to enable)"
fi

stage "pytest"
# `--continue-on-collection-errors` so one broken module does not hide the
# results of the other sixty; the non-zero exit still fails the stage.
"${PY}" -m pytest tests -q --continue-on-collection-errors

printf '\n==> all checks passed\n'
