#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# check.sh — the CI gate. Runs the same four gates every phase has been held
# to since crate #1, in this order (failure stops the run):
#
#   1. cargo fmt --check
#   2. cargo clippy --workspace --all-targets -- -D warnings
#   3. cargo test  --workspace --locked
#   4. cargo doc   --workspace --no-deps  (warnings denied via RUSTDOCFLAGS)
#
# Env: MAS_ENV=test is exported so config layering resolves the fast test
# overlay (the loaders tolerate its absence, config/test.toml ships anyway).
#
# Usage: scripts/check.sh [--quick]   (#quick runs tests in --release? no —
#   #quick skips doc, meant for pre-push sanity only)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
cd "$(dirname "$0")/.."

# Toolchain: the committed .cargo-env points at the shared install; fall back
# to whatever cargo is on PATH (CI images pin via rust-toolchain.toml).
if [ -f .cargo-env ]; then
    # shellcheck disable=SC1091
    source .cargo-env
fi
if ! command -v cargo >/dev/null; then
    echo "check.sh: cargo missing — run scripts/bootstrap-toolchain.sh first" >&2
    exit 127
fi

export MAS_ENV="${MAS_ENV:-test}"
QUICK="${1:-}"

step() { printf '\n==> %s\n' "$*"; }

step "1/4 cargo fmt --check"
cargo fmt --check

step "2/4 cargo clippy --workspace --all-targets -- -D warnings"
cargo clippy --workspace --all-targets --locked -- -D warnings

step "3/4 cargo test --workspace --locked"
cargo test --workspace --locked

if [ "$QUICK" != "--quick" ]; then
    step "4/4 cargo doc --workspace --no-deps (warnings denied)"
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
else
    step "4/4 cargo doc — SKIPPED (--quick)"
fi

printf '\n✔ all gates green\n'
