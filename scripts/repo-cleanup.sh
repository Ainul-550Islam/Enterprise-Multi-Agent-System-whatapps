#!/usr/bin/env bash
# scripts/repo-cleanup.sh — idempotent repo hygiene for the Batch 03 sale-readiness
# delivery. DRY-RUN by default; pass --apply to execute.
#
#   git rm: .cargo/bin/rustup, .profile, Module1_Full_Source.zip,
#           module1-full-source/  (verify pure duplicate first; abort if unique files)
#   git mv: root *_REPORT.md / *report.md / RUST_PYTHON_BRIDGE_PHASE16_FILES.md /
#           uploads/ / enterprise-mas/reports/* -> docs/internal/
#
# Prints what it would do in dry-run; only mutates with --apply.

set -euo pipefail

APPLY=0
if [[ "${1:-}" == "--apply" ]]; then
  APPLY=1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

log() { if [[ $APPLY -eq 1 ]]; then echo "APPLY: $*"; else echo "DRY-RUN: $*"; fi }
fail() { echo "ABORT: $*" >&2; exit 1; }

# ─── Verify duplicates before touching anything ───
# module1-full-source must be a pure duplicate of enterprise-mas; abort if it
# contains unique files not present in enterprise-mas.
if [[ -d module1-full-source && -d enterprise-mas ]]; then
  unique=$(diff -rq module1-full-source enterprise-mas 2>/dev/null \
    | grep -v "Only in enterprise-mas" \
    | grep "^Only in module1-full-source" || true)
  if [[ -n "$unique" ]]; then
    fail "module1-full-source contains unique files not in enterprise-mas; refusing to delete:\n$unique"
  fi
  log "module1-full-source is a pure duplicate of enterprise-mas -> remove"
  [[ $APPLY -eq 1 ]] && git rm -r --cached --ignore-unmatch module1-full-source . 2>/dev/null || true
  [[ $APPLY -eq 1 ]] && rm -rf module1-full-source
fi

# ─── git rm candidates ───
rm_candidates=(
  ".cargo/bin/rustup"
  ".profile"
  "Module1_Full_Source.zip"
)
for f in "${rm_candidates[@]}"; do
  if [[ -e "$f" ]]; then
    log "git rm $f"
    [[ $APPLY -eq 1 ]] && git rm --cached --ignore-unmatch "$f" 2>/dev/null || true
    [[ $APPLY -eq 1 ]] && rm -f "$f"
  else
    log "skip (absent): $f"
  fi
done

# ─── git mv candidates into docs/internal/ ───
DEST="docs/internal"
[[ $APPLY -eq 1 ]] && mkdir -p "$DEST"

mv_candidates=(
  $(ls *_REPORT.md 2>/dev/null || true)
  $(ls *report.md 2>/dev/null || true)
  "RUST_PYTHON_BRIDGE_PHASE16_FILES.md"
  "uploads"
  "enterprise-mas/reports"
)
for src in "${mv_candidates[@]}"; do
  [[ -z "$src" ]] && continue
  if [[ -e "$src" ]]; then
    log "git mv $src -> $DEST/"
    [[ $APPLY -eq 1 ]] && git mv -k "$src" "$DEST/" 2>/dev/null || true
  else
    log "skip (absent): $src"
  fi
done

echo
if [[ $APPLY -eq 1 ]]; then
  echo "repo-cleanup: applied."
else
  echo "repo-cleanup: dry-run complete. Re-run with --apply to execute."
fi
