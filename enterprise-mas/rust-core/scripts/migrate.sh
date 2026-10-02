#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# migrate.sh — verified apply/export of migrations/NNNN_*/up.sql through the
# SQL-driven runner in crux (sql at crates/persistence/src/migrations.rs).
# Two modes, matching doctrine:
#
#   scripts/migrate.sh up               → stable-order apply with checksums,
#                                          PostgreSQL advisory lock
#   scripts/migrate.sh plan             → SHA-256 manifest preview (no DB
#                                          contact) — what production must
#                                          export BEFORE deploying (runbook
#                                          requirement: DBAs apply by hand)
#   scripts/migrate.sh status           → read-only reconciliation against
#                                          the database (drift detectable)
#
# Env: DATABASE_URL (postgres://…), MAS_DB_MAX_CONNS optional.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .cargo-env ]; then source .cargo-env; fi
cargo build -p mas-persistence --quiet

CMD="${1:-status}"
if [ -z "${DATABASE_URL:-}" ] && [ "$CMD" != "plan" ]; then
    echo "migrate.sh: DATABASE_URL required for '$CMD'" >&2
    exit 2
fi

cargo run -p mas-persistence --quiet --example mas_migrate -- "$CMD" "${MIGRATIONS_DIR:-migrations}"
