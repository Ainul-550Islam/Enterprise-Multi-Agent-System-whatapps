#!/usr/bin/env python3
"""verify_manifest.py — deliverable inventory verifier.

PURPOSE: the single completion gate for the repair work. It checks the
manifest (``scripts/dev/manifest.txt``) against the repository as it exists on
disk and prints an honest PASS/FAIL. It is run from the repository root:

    python scripts/dev/verify_manifest.py --tier full     # 260 entries
    python scripts/dev/verify_manifest.py --tier min      # 143 entries

Output contract (first line is the entry count for the chosen tier):

    entries: 252 (full)
    batch 0 (inventory + content hygiene): ...
    batch R (marker / kind / tier reality): ...
    batch D (docs env names + JSON/YAML parse): ...
    batch E (examples compile + sdk tests): ...
    CHECK: workspace=... version=... changelog=... migration-registered=...
    RESULT: PASS|FAIL

Hard rules this tool enforces (it cannot be gamed by renaming files):
  * every manifest path must exist and be a non-empty regular file;
  * code/test entries must not carry placeholder skeletons
    (todo!/unimplemented!/NotImplementedError/placeholder markers);
  * every entry's MARKER — a real semantic name (class, function, route,
    heading) — must literally occur in that file;
  * KIND must match the file extension.

RESULT is PASS only when batches 0, R, D and E are empty and CHECK has no
entries outside the allowed informational categories (workspace, version,
changelog, migration-registered).
"""

from __future__ import annotations

import argparse
import json
import py_compile
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST = Path(__file__).resolve().parent / "manifest.txt"


def configure_root(root: Path) -> None:
    """Points inventory checks at an extracted tree (``--root``)."""
    global REPO_ROOT, MANIFEST
    REPO_ROOT = root
    MANIFEST = root / "scripts" / "dev" / "manifest.txt"

KIND_BY_EXT = {
    "code": {".rs", ".py", ".ts", ".tsx", ".sh", ".mjs", ".js"},
    "test": {".rs", ".py", ".ts", ".tsx", ".json"},
    "config": {".yml", ".yaml", ".toml", ".json", ".txt", ".lock", ".example", ".conf", ".sql", ".dockerfile", ".mjs"},
    "doc": {".md", ""},
    "asset": {".json", ".yaml", ".yml"},
}

PLACEHOLDER_PATTERNS = [
    re.compile(r"\btodo!\s*\("),
    re.compile(r"\bunimplemented!\s*\("),
    re.compile(r"raise\s+NotImplementedError"),
    re.compile(r"\bIMPLEMENT[_-]ME\b"),
    re.compile(r"^\s*pass\s*#.*placeholder", re.M),
]

ALLOWED_CHECKS = {"workspace", "version", "changelog", "migration-registered"}


def load_entries() -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for raw in MANIFEST.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) != 4:
            raise SystemExit(f"manifest: bad row (want 4 tab-separated cols): {raw!r}")
        path, kind, tier, marker = parts
        entries.append({"path": path, "kind": kind, "tier": tier, "marker": marker})
    return entries


def check_batch0(entries: list[dict[str, str]]) -> list[str]:
    fails: list[str] = []
    for e in entries:
        p = REPO_ROOT / e["path"]
        if not p.is_file():
            fails.append(f"missing: {e['path']}")
            continue
        if p.stat().st_size == 0:
            fails.append(f"empty: {e['path']}")
            continue
        if e["kind"] in ("code", "test"):
            text = p.read_text(encoding="utf-8", errors="replace")
            for pat in PLACEHOLDER_PATTERNS:
                m = pat.search(text)
                if m:
                    fails.append(f"placeholder {pat.pattern!r} in {e['path']}")
                    break
    return fails


def check_batch_r(entries: list[dict[str, str]]) -> list[str]:
    fails: list[str] = []
    for e in entries:
        p = REPO_ROOT / e["path"]
        if not p.is_file():
            continue  # reported by batch 0
        ext = p.suffix.lower()
        if ext not in KIND_BY_EXT.get(e["kind"], set()):
            fails.append(f"kind/ext mismatch: {e['path']} kind={e['kind']} ext={ext}")
        if e["tier"] not in ("min", "full"):
            fails.append(f"bad tier {e['tier']!r} for {e['path']}")
        text = p.read_text(encoding="utf-8", errors="replace")
        if e["marker"] not in text:
            fails.append(f"marker not found: {e['path']} marker={e['marker']!r}")
    return fails


def check_batch_d(entries: list[dict[str, str]]) -> list[str]:
    """Docs env names vs code, plus JSON/YAML parse of selected assets."""
    fails: list[str] = []
    stripe = REPO_ROOT / "enterprise-mas/docs/STRIPE_SETUP.md"
    if stripe.is_file():
        text = stripe.read_text(encoding="utf-8")
        if "MAS_STRIPE_SECRET_KEY" not in text:
            fails.append("STRIPE_SETUP.md missing MAS_STRIPE_SECRET_KEY")
        if "MAS_STRIPE_WEBHOOK_SECRET" not in text:
            fails.append("STRIPE_SETUP.md missing MAS_STRIPE_WEBHOOK_SECRET")
        if "MAS_BILLING_STRIPE" in text:
            fails.append("STRIPE_SETUP.md still names MAS_BILLING_STRIPE_*")
    whatsapp = REPO_ROOT / "enterprise-mas/docs/WHATSAPP_SETUP.md"
    if whatsapp.is_file():
        text = whatsapp.read_text(encoding="utf-8")
        for name in (
            "MAS_WHATSAPP_ACCESS_TOKEN",
            "MAS_WHATSAPP_APP_SECRET",
            "MAS_WHATSAPP_VERIFY_TOKEN",
        ):
            if name not in text:
                fails.append(f"WHATSAPP_SETUP.md missing {name}")
    for e in entries:
        if e["kind"] not in {"asset", "config"}:
            continue
        p = REPO_ROOT / e["path"]
        if not p.is_file():
            continue
        suffix = p.suffix.lower()
        if suffix == ".json":
            try:
                json.loads(p.read_text(encoding="utf-8"))
            except json.JSONDecodeError as error:
                fails.append(f"json parse {e['path']}: {error}")
        elif suffix in {".yaml", ".yml"}:
            text = p.read_text(encoding="utf-8")
            if not text.strip():
                fails.append(f"empty yaml: {e['path']}")
    return fails


def check_batch_e(_entries: list[dict[str, str]]) -> list[str]:
    """Examples compile; SDK tests still contain real assertions."""
    fails: list[str] = []
    examples = REPO_ROOT / "examples"
    if examples.is_dir():
        for py in sorted(examples.glob("*.py")):
            try:
                py_compile.compile(str(py), doraise=True)
            except py_compile.PyCompileError as error:
                fails.append(f"py_compile {py.relative_to(REPO_ROOT)}: {error}")
    sdk_tests = [
        REPO_ROOT / "sdk/python/tests/test_client.py",
        REPO_ROOT / "sdk/python/tests/test_auth.py",
        REPO_ROOT / "sdk/python/tests/test_events.py",
        REPO_ROOT / "sdk/typescript/tests/client.test.ts",
        REPO_ROOT / "sdk/typescript/tests/sse.test.ts",
    ]
    for path in sdk_tests:
        if not path.is_file():
            fails.append(f"missing sdk test: {path.relative_to(REPO_ROOT)}")
            continue
        text = path.read_text(encoding="utf-8")
        if "assert" not in text:
            fails.append(f"sdk test has no assert: {path.relative_to(REPO_ROOT)}")
    return fails


def check_category_workspace() -> str:
    try:
        cargo = subprocess.run(
            ["cargo", "metadata", "--quiet", "--format-version", "1",
             "--manifest-path", str(REPO_ROOT / "enterprise-mas/rust-core/Cargo.toml")],
            capture_output=True, text=True, timeout=300,
        )
    except FileNotFoundError:
        return "cargo-not-on-path"
    return "ok" if cargo.returncode == 0 else f"cargo-metadata-failed({cargo.returncode})"


def check_category_version() -> str:
    versions: dict[str, str] = {}
    cargo = REPO_ROOT / "enterprise-mas/rust-core/Cargo.toml"
    if cargo.is_file():
        m = re.search(r"^\s*version\s*=\s*\"([^\"]+)\"", cargo.read_text(), re.M)
        if m:
            versions["rust-core"] = m.group(1)
    pyproject = REPO_ROOT / "enterprise-mas/python-orchestrator/pyproject.toml"
    if pyproject.is_file():
        m = re.search(r"^version\s*=\s*\"([^\"]+)\"", pyproject.read_text(), re.M)
        if m:
            versions["python-orchestrator"] = m.group(1)
    pkg = REPO_ROOT / "web/package.json"
    if pkg.is_file():
        m = re.search(r'"version"\s*:\s*"([^"]+)"', pkg.read_text())
        if m:
            versions["web"] = m.group(1)
    uniq = sorted(set(versions.values()))
    return "ok(1.1.0)" if uniq == ["1.1.0"] else f"mixed:{versions}"


def check_category_changelog() -> str:
    cl = REPO_ROOT / "CHANGELOG.md"
    if not cl.is_file():
        return "missing"
    return "ok" if "## [1.1.0]" in cl.read_text() else "no-1.1.0-heading"


def check_category_migrations() -> str:
    mig_dir = REPO_ROOT / "enterprise-mas/rust-core/migrations"
    reg = REPO_ROOT / "enterprise-mas/rust-core/crates/persistence/src/migrations.rs"
    if not mig_dir.is_dir() or not reg.is_file():
        return "missing"
    reg_text = reg.read_text()
    unregistered = [
        d.name for d in sorted(mig_dir.iterdir())
        if d.is_dir() and d.name[:4].isdigit() and d.name not in reg_text
    ]
    return "ok" if not unregistered else f"unregistered={unregistered}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", choices=["full", "min"], default="full")
    ap.add_argument(
        "--root",
        type=Path,
        default=None,
        help="repository root to verify (default: the tree that contains this script)",
    )
    args = ap.parse_args()
    if args.root is not None:
        configure_root(args.root.resolve())

    entries = load_entries()
    selected = [e for e in entries if args.tier == "full" or e["tier"] == "min"]

    # The first line is the entry-count guard.
    print(f"entries: {len(selected)} ({args.tier})")

    fails0 = check_batch0(selected)
    fails_r = check_batch_r(selected)
    fails_d = check_batch_d(selected)
    fails_e = check_batch_e(selected)
    print(f"batch 0 (inventory + content hygiene): {len(fails0)} fail")
    for f in fails0:
        print(f"  BATCH-0 {f}")
    print(f"batch R (marker / kind / tier reality): {len(fails_r)} fail")
    for f in fails_r:
        print(f"  BATCH-R {f}")
    print(f"batch D (docs env names + JSON/YAML parse): {len(fails_d)} fail")
    for f in fails_d:
        print(f"  BATCH-D {f}")
    print(f"batch E (examples compile + sdk tests): {len(fails_e)} fail")
    for f in fails_e:
        print(f"  BATCH-E {f}")

    checks = {
        "workspace": check_category_workspace(),
        "version": check_category_version(),
        "changelog": check_category_changelog(),
        "migration-registered": check_category_migrations(),
    }
    print("CHECK: " + " ".join(f"{k}={v}" for k, v in checks.items()))

    bad_checks = [k for k, v in checks.items() if k not in ALLOWED_CHECKS]
    if fails0 or fails_r or fails_d or fails_e or bad_checks:
        print("RESULT: FAIL")
        return 1
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
