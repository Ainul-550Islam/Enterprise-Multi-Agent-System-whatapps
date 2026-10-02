#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# bootstrap-toolchain.sh — (re)builds the shared Rust toolchain under
# /home/user/.toolchains/rust per rust-toolchain.toml. The sandbox keeps
# wiping heavy directories between sessions (ship-of-Theseus problem — the
# project survives, the compilers don't), so this script is the deterministic
# recovery. Idempotent: rerunning over an existing install is a no-op unless
# the requested channel differs.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
cd "$(dirname "$0")/.."

HOME_DIR="${HOME:-/home/user}"
export RUSTUP_HOME="$HOME_DIR/.toolchains/rust/rustup"
export CARGO_HOME="$HOME_DIR/.toolchains/rust/cargo"

CHANNEL="$(grep -E 'channel *=' rust-toolchain.toml | sed 's/.*= *"//;s/".*//')"
CHANNEL="${CHANNEL:-1.98.1}"

if [ -x "$CARGO_HOME/bin/cargo" ]; then
    INSTALLED="$("$CARGO_HOME/bin/rustc" --version 2>/dev/null | awk '{print $2}')"
    if [ "$INSTALLED" = "$CHANNEL" ]; then
        echo "toolchain $CHANNEL already present at $CARGO_HOME"
        exit 0
    fi
fi

if ! command -v curl >/dev/null; then
    echo "bootstrap-toolchain.sh: curl required" >&2
    exit 127
fi

echo "installing rust $CHANNEL into $RUSTUP_HOME (this takes a minute)…"
curl -sSf https://sh.rustup.rs -o /tmp/rustup-init-mas.sh
sh /tmp/rustup-init-mas.sh -y --profile minimal \
    --default-toolchain "$CHANNEL" --component rustfmt,clippy
rm -f /tmp/rustup-init-mas.sh

cat > .cargo-env <<EOF
# Toolchain environment for this workspace (kept beside the project so it
# survives sandbox reattaches; the toolchain itself lives under the user's
# .toolchains/ and is rebuilt by scripts/bootstrap-toolchain.sh when wiped).
export RUSTUP_HOME=$RUSTUP_HOME
export CARGO_HOME=$CARGO_HOME
export PATH="\$CARGO_HOME/bin:\$PATH"
EOF

echo "toolchain ready: $("$CARGO_HOME/bin/cargo" --version)"
