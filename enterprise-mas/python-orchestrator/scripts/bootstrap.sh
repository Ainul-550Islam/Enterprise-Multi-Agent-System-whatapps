#!/usr/bin/env bash
#
# Installs the Python orchestrator's development toolchain.
#
# This is the one script CI runs before `scripts/check.sh`, and it is also what
# a developer runs after cloning. Keeping both paths identical means a check
# that passes locally passes in CI for the same reason rather than by accident.
#
# Usage:
#   bash scripts/bootstrap.sh [--force]
#
# Idempotent: re-running against an existing environment is a no-op reinstall.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="${PYTHON_BIN:-python3}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "error: ${PYTHON_BIN} is not on PATH" >&2
  echo "hint: set PYTHON_BIN=/path/to/python3.13 and re-run" >&2
  exit 1
fi

# The checked-in protobuf bindings require >= 3.11 (StrEnum, `Self` in
# protocols). Failing here with a clear message beats failing 200 tests in with
# ImportError.
PY_VERSION="$("${PYTHON_BIN}" -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
PY_MAJOR="${PY_VERSION%%.*}"
PY_MINOR="${PY_VERSION##*.}"
if [[ "${PY_MAJOR}" -lt 3 ]] || { [[ "${PY_MAJOR}" -eq 3 ]] && [[ "${PY_MINOR}" -lt 11 ]]; }; then
  echo "error: Python >= 3.11 is required, found ${PY_VERSION}" >&2
  exit 1
fi

echo "==> bootstrapping with Python ${PY_VERSION} (${PYTHON_BIN})"

# A venv keeps this script from mutating whatever interpreter the runner or the
# developer happens to have, and gives `check.sh` a predictable path.
VENV_DIR="${VENV_DIR:-${ROOT_DIR}/.venv}"
if [[ ! -d "${VENV_DIR}" ]]; then
  echo "==> creating virtualenv at ${VENV_DIR}"
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

python -m pip install --upgrade pip >/dev/null

echo "==> installing the package with its dev dependency group"
# `-e` so the installed console script and the working tree are the same code;
# installing a copy would let `check.sh` pass against a stale build.
python -m pip install --editable ".[otel,metrics]" >/dev/null
python -m pip install "pytest>=8.2" "pytest-asyncio>=0.24" "ruff>=0.6" "mypy>=1.11" \
  "grpcio-tools>=1.84" "defusedxml>=0.7.1,<1" "types-defusedxml" >/dev/null

echo "==> toolchain ready"
python - <<'PY'
import grpc, pydantic, structlog, defusedxml  # noqa: F401
import mas_orchestrator  # noqa: F401

print(f"    grpcio={grpc.__version__} pydantic={pydantic.VERSION}")
PY
