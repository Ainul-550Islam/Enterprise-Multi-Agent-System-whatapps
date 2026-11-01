#!/usr/bin/env sh
# PURPOSE:
# Restores the complete Python toolchain after the sandbox loses it.
#
# Installs the pinned development dependencies AND the package itself, so a
# fresh session can run `scripts/check.sh` green with one command. Installing
# the package matters, not just its dependencies: `pyproject.toml` sets
# `pythonpath = ["src"]`, which lets pytest import `mas_orchestrator` but does
# nothing for a test that spawns a bare `python3 -c "import ..."` subprocess.
# Without the install, that one test goes red for reasons that look like a
# product bug and are not.
#
# Usage:  ./scripts/bootstrap.sh
#
# The sandbox can lose installed packages between sessions while /home/user
# survives. Running this script restores the toolchain without touching any
# source file.
#
# Usage:  ./scripts/bootstrap.sh
set -eu

python3 -m pip install -q --disable-pip-version-check \
    "pydantic>=2.9,<3" \
    "pydantic-settings>=2.5,<3" \
    "structlog>=24.1" \
    "httpx>=0.27" \
    "grpcio>=1.84" \
    "grpcio-tools>=1.84" \
    "protobuf>=7.35.1,<8" \
    "beautifulsoup4>=4.12,<5" \
    "defusedxml>=0.7.1,<1" \
    "pgvector>=0.3,<1" \
    "pypdf>=5,<7" \
    "psycopg[binary,pool]>=3.2,<4" \
    "python-docx>=1.1,<2" \
    "pytest>=8.2" \
    "pytest-asyncio>=0.24" \
    "ruff>=0.6" \
    "mypy>=1.11" \
    "types-defusedxml>=0.7.0"

# Install the package itself, not only its dev dependencies.
#
# `pyproject.toml` sets `pythonpath = ["src"]`, which is enough for pytest to
# import `mas_orchestrator` but NOT enough for a test that spawns a bare
# `python3 -c "import mas_orchestrator"` subprocess: that child inherits no
# pytest path setup. The fingerprint-stability test does exactly that, because
# a hash that differs between processes is not a fingerprint at all. Restoring
# only the tools therefore leaves one test red after a reset, which looks like
# a product bug and is not.
python3 -m pip install -q --disable-pip-version-check --no-deps -e .

echo "toolchain restored"
python3 -m ruff --version
python3 -m mypy --version
python3 -m pytest --version
