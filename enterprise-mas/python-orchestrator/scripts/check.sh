#!/usr/bin/env bash
# PURPOSE:
# Runs every check the project promises, and refuses to hide a failure.
#
# Format, lint, strict type-check, tests and byte-compilation.
#
# Every stage runs even when an earlier one fails, so a single invocation
# reports the full picture; the exit status is non-zero if any stage failed.
# There is deliberately no `|| true` anywhere in this script.

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

overall=0

run_stage() {
  local name="$1"
  shift
  echo
  echo "=== ${name} ==="
  if "$@"; then
    echo "--- ${name}: PASS"
  else
    echo "--- ${name}: FAIL"
    overall=1
  fi
}

run_stage "ruff format (check)" python3 -m ruff format --check src tests
run_stage "ruff lint" python3 -m ruff check src tests
run_stage "mypy (strict)" env PYTHONPATH=src python3 -m mypy src/mas_orchestrator
run_stage "pytest" env PYTHONPATH=src python3 -m pytest tests -q
run_stage "compileall" python3 -m compileall -q src tests


echo
if [[ "${overall}" -ne 0 ]]; then
  echo "FAILURE: one or more checks failed"
  exit 1
fi
echo "SUCCESS: all checks passed"
