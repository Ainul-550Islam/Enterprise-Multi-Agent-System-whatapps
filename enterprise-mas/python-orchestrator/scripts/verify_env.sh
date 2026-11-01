#!/usr/bin/env bash
#
# Validates the runtime/build environment without ever printing a secret.
#
# Used in CI and before a production deploy to fail fast on a missing
# interpreter, dependency, or required configuration variable. It deliberately
# checks only for *presence* of secret-bearing variables: echoing their values
# would leak them into build logs.

set -uo pipefail

failures=0

check_command() {
  if command -v "$1" >/dev/null 2>&1; then
    printf 'ok    %-28s %s\n' "$1" "$(command -v "$1")"
  else
    printf 'FAIL  %-28s not found\n' "$1"
    failures=$((failures + 1))
  fi
}

check_python_module() {
  if python3 -c "import $1" >/dev/null 2>&1; then
    printf 'ok    %-28s import %s\n' "python:$1" "$1"
  else
    printf 'FAIL  %-28s module %s not importable\n' "python:$1" "$1"
    failures=$((failures + 1))
  fi
}

# Presence-only check: the name is printed, never the value.
check_env_present() {
  if [[ -n "${!1:-}" ]]; then
    printf 'ok    %-28s set (value hidden)\n' "$1"
  else
    printf 'WARN  %-28s not set\n' "$1"
  fi
}

echo "=== interpreter ==="
check_command python3
python3 --version 2>/dev/null | sed 's/^/      /'

echo
echo "=== python modules ==="
check_python_module pydantic
check_python_module pydantic_settings
check_python_module grpc
check_python_module httpx
check_python_module structlog

echo
echo "=== optional python modules ==="
python3 -c "import opentelemetry" >/dev/null 2>&1 \
  && printf 'ok    %-28s import opentelemetry\n' "python:opentelemetry" \
  || printf 'WARN  %-28s opentelemetry not installed (optional)\n' "python:opentelemetry"

echo
echo "=== secret references (presence only) ==="
check_env_present MAS_ORCHESTRATOR_RUST_BEARER_TOKEN
check_env_present MAS_ORCHESTRATOR_SERVICE_TOKEN

echo
if [[ "${failures}" -gt 0 ]]; then
  echo "FAILURE: ${failures} required check(s) failed"
  exit 1
fi
echo "SUCCESS: environment verified"
