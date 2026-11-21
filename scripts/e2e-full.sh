#!/usr/bin/env bash
# PURPOSE: full e2e sweep — smoke, whatsapp lane, then the CLI round trip.
# Runs the individual e2e suites in order and stops at the first failure so
# the log ends on the exact check that broke.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "e2e-full: starting full sweep"

bash "${ROOT}/scripts/e2e-smoke.sh"
echo "e2e-full: smoke ok"

bash "${ROOT}/scripts/e2e-whatsapp.sh"
echo "e2e-full: whatsapp ok"

# CLI round trip against the same stack (status must answer 'live: ok').
# The output is captured whole before grepping: piping straight into
# `grep -q` closes stdout early and a blocked-stdio writer dies on SIGPIPE.
cli_status() {
  if command -v mas >/dev/null 2>&1; then
    mas status 2>&1
  elif [ -x "${ROOT}/enterprise-mas/rust-core/target/debug/mas" ]; then
    "${ROOT}/enterprise-mas/rust-core/target/debug/mas" status 2>&1
  elif command -v cargo >/dev/null 2>&1 && [ -d "${ROOT}/enterprise-mas/rust-core" ]; then
    (cd "${ROOT}/enterprise-mas/rust-core" && cargo run -q -p mas-cli -- status 2>&1)
  else
    return 127
  fi
}
set +e
CLI_OUT="$(cli_status)"
CLI_RC=$?
set -e
if [ "${CLI_RC}" = "127" ]; then
  echo "e2e-full: cli unavailable on PATH and no cargo — cli step NOT_RUN" >&2
elif [ "${CLI_RC}" != "0" ]; then
  echo "e2e-full: cli status exited ${CLI_RC}: ${CLI_OUT}" >&2; exit 1
elif printf '%s' "${CLI_OUT}" | grep -q "live: ok"; then
  echo "e2e-full: cli ok"
else
  echo "e2e-full: cli status did not report 'live: ok': ${CLI_OUT}" >&2; exit 1
fi

echo "e2e-full: PASS"
