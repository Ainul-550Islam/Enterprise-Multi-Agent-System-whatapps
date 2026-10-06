#!/usr/bin/env bash
# PURPOSE:
# Runs every check the project promises, and refuses to hide a failure.
#
# Format, lint, strict type-check, tests, byte-compilation, and finally a
# completeness check that proves the published deliverable still contains each
# file verbatim rather than a shortened copy of it.
#
# Runs format, lint, type-check and test checks without hiding failures.
#
# Every stage runs even when an earlier one fails, so a single invocation
# reports the full picture; the exit status is non-zero if any stage failed.
# There is deliberately no `|| true` anywhere in this script.
#
# The completeness stage belongs here because this script is where the
# "no shortening" rule is enforced. A stage that only grepped for "..." would
# have passed the truncated document it exists to catch; comparing every file
# against its source is the only check that can actually fail.
#
# The completeness stage is why this script is the right place for the rule
# "no shortening". A stage that only greps for "..." would have passed the
# truncated document it was written to catch; comparing each file against its
# source is the only check that can fail.

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
run_stage "deliverable completeness" python3 verify_deliverable_completeness.py

echo
if [[ "${overall}" -ne 0 ]]; then
  echo "FAILURE: one or more checks failed"
  exit 1
fi
echo "SUCCESS: all checks passed"
