#!/usr/bin/env python3
# PURPOSE:
# Assembles the Phase 16 deliverable: every file created or modified during
# the Rust <-> Python AgentBridge integration, emitted as FILE: + PURPOSE: +
# complete content, with no omissions and no elision markers.
#
# It extracts each file's PURPOSE from its own leading `# PURPOSE:` (Python)
# or `// PURPOSE:` (Rust) block, so the deliverable cannot drift from the
# source: if a file has no purpose comment, the build FAILS rather than
# emitting a file without one.

"""Generates the Phase 16 output file.

The user's requirement is that every modified file appears with its purpose
and its *complete* content. Doing that by hand risks exactly the failure the
requirement exists to prevent — a truncated body, an "omitted" marker, or a
purpose comment that describes what the file used to do.

So this script:

* reads the file list from a single explicit table;
* refuses to run if any listed file is missing;
* refuses to run if any listed file lacks a ``PURPOSE:`` marker;
* refuses to run if any emitted body contains a common elision marker.

The last check is the important one: it makes "no omissions" a property the
build enforces rather than a promise the author makes.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path("/home/user/repo/enterprise-mas/rust-core")
PY = Path("/home/user/python-orchestrator")
OUT = Path("/home/user/RUST_PYTHON_BRIDGE_PHASE16_FILES.md")

# (display path, absolute path, language)
FILES: list[tuple[str, Path, str]] = [
    # --- Rust: wire contract -------------------------------------------------
    (
        "crates/api/proto/mas/api/v1/mas.proto",
        REPO / "crates/api/proto/mas/api/v1/mas.proto",
        "proto",
    ),
    # --- Rust: execution port -------------------------------------------------
    ("crates/execution/src/agent_bridge.rs", REPO / "crates/execution/src/agent_bridge.rs", "rust"),
    ("crates/execution/src/lib.rs", REPO / "crates/execution/src/lib.rs", "rust"),
    ("crates/execution/Cargo.toml", REPO / "crates/execution/Cargo.toml", "toml"),
    # --- Rust: gRPC adapter (new crate) ---------------------------------------
    ("crates/ai-bridge/Cargo.toml", REPO / "crates/ai-bridge/Cargo.toml", "toml"),
    ("crates/ai-bridge/build.rs", REPO / "crates/ai-bridge/build.rs", "rust"),
    ("crates/ai-bridge/src/lib.rs", REPO / "crates/ai-bridge/src/lib.rs", "rust"),
    # --- Rust: production composition root ------------------------------------
    (
        "crates/worker-service/src/agent_execution.rs",
        REPO / "crates/worker-service/src/agent_execution.rs",
        "rust",
    ),
    ("crates/worker-service/src/main.rs", REPO / "crates/worker-service/src/main.rs", "rust"),
    ("crates/worker-service/Cargo.toml", REPO / "crates/worker-service/Cargo.toml", "toml"),
    # --- Rust: pre-existing lint fixed in passing -----------------------------
    ("crates/messaging/src/nats.rs", REPO / "crates/messaging/src/nats.rs", "rust"),
    # --- Python: inbound trust boundary ---------------------------------------
    (
        "src/mas_orchestrator/grpc/agent_bridge_servicer.py",
        PY / "src/mas_orchestrator/grpc/agent_bridge_servicer.py",
        "python",
    ),
    ("src/mas_orchestrator/grpc/server.py", PY / "src/mas_orchestrator/grpc/server.py", "python"),
    ("src/mas_orchestrator/grpc/errors.py", PY / "src/mas_orchestrator/grpc/errors.py", "python"),
    (
        "src/mas_orchestrator/grpc/__init__.py",
        PY / "src/mas_orchestrator/grpc/__init__.py",
        "python",
    ),
    # --- Python: production composition ---------------------------------------
    (
        "src/mas_orchestrator/services/composition.py",
        PY / "src/mas_orchestrator/services/composition.py",
        "python",
    ),
    (
        "src/mas_orchestrator/agents/tenant_registry.py",
        PY / "src/mas_orchestrator/agents/tenant_registry.py",
        "python",
    ),
    ("src/mas_orchestrator/main.py", PY / "src/mas_orchestrator/main.py", "python"),
    (
        "src/mas_orchestrator/config/settings.py",
        PY / "src/mas_orchestrator/config/settings.py",
        "python",
    ),
    # --- Python: telemetry -----------------------------------------------------
    (
        "src/mas_orchestrator/observability/bridge_metrics.py",
        PY / "src/mas_orchestrator/observability/bridge_metrics.py",
        "python",
    ),
    (
        "src/mas_orchestrator/observability/__init__.py",
        PY / "src/mas_orchestrator/observability/__init__.py",
        "python",
    ),
    # --- Python: configuration -------------------------------------------------
    ("pyproject.toml", PY / "pyproject.toml", "toml"),
    # --- Tests -------------------------------------------------------------------
    ("tests/bridge/conftest.py", PY / "tests/bridge/conftest.py", "python"),
    (
        "tests/bridge/test_identity_and_scope.py",
        PY / "tests/bridge/test_identity_and_scope.py",
        "python",
    ),
    (
        "tests/bridge/test_deadline_and_cancellation.py",
        PY / "tests/bridge/test_deadline_and_cancellation.py",
        "python",
    ),
    ("tests/bridge/test_error_mapping.py", PY / "tests/bridge/test_error_mapping.py", "python"),
    (
        "tests/bridge/test_production_composition.py",
        PY / "tests/bridge/test_production_composition.py",
        "python",
    ),
    ("tests/bridge/test_telemetry.py", PY / "tests/bridge/test_telemetry.py", "python"),
    (
        "tests/bridge/test_cross_language_contract.py",
        PY / "tests/bridge/test_cross_language_contract.py",
        "python",
    ),
    (
        "tests/contract/test_proto_contract.py",
        PY / "tests/contract/test_proto_contract.py",
        "python",
    ),
    ("tests/unit/test_events_delivery.py", PY / "tests/unit/test_events_delivery.py", "python"),
    ("scripts/bootstrap.sh", PY / "scripts/bootstrap.sh", "shell"),
    (
        "crates/ai-bridge/tests/e2e_python.rs",
        REPO / "crates/ai-bridge/tests/e2e_python.rs",
        "rust",
    ),
    ("tests/bridge/stub_server.py", PY / "tests/bridge/stub_server.py", "python"),
    (
        "verify_deliverable_completeness.py",
        PY / "verify_deliverable_completeness.py",
        "python",
    ),
    ("scripts/check.sh", PY / "scripts/check.sh", "shell"),
]

#: Markers that would mean the body was abridged. The build refuses to emit
#: a file containing any of these inside a code fence.
ELISION_MARKERS = (
    "... omitted",
    "# Rest of the code",
    "// existing code omitted",
    "# same as above",
    "<truncated>",
    "content omitted",
    "# ... rest unchanged",
)

#: Files exempt from the marker scan, by exact displayed path.
#
#: The completeness verifier has to *name* every marker it hunts for, so its
#: own source contains all of them and would otherwise fail the build it is
#: part of. Exempting one named file is auditable in a way that weakening the
#: marker list would not be; keep this set empty unless a file's job is
#: literally to define the markers.
MARKER_SCAN_EXEMPT: frozenset[str] = frozenset({"verify_deliverable_completeness.py"})


def _strip_comment(line: str, sigils: tuple[str, ...]) -> str | None:
    """Removes a leading comment sigil from *line*.

    Args:
        line: A stripped source line.
        sigils: The comment markers valid for this language.

    Returns:
        The line's text with the sigil removed, or ``None`` when the line is
        not a comment of a recognised kind.
    """
    for sigil in sorted(sigils, key=len, reverse=True):
        if line.startswith(sigil):
            return line[len(sigil) :].strip()
    return None


def extract_purpose(text: str, language: str) -> str:
    """Pulls the PURPOSE block out of a file.

    Args:
        text: The file's contents.
        language: One of ``rust``, ``python``, ``proto``, ``toml``, ``shell``.

    Returns:
        The purpose text, with comment markers stripped.

    Raises:
        SystemExit: If the file has no ``PURPOSE:`` marker. Emitting a file
            without its purpose would defeat the point of the deliverable, so
            this fails the build rather than shipping a blank.
    """
    sigils: tuple[str, ...] = {
        "rust": ("//!", "//"),
        "python": ("#",),
        "proto": ("//",),
        "toml": ("#",),
        "shell": ("#",),
    }[language]

    lines = text.splitlines()
    start_index: int | None = None
    first_body = ""

    for index, raw in enumerate(lines):
        stripped = raw.strip()
        body = _strip_comment(stripped, sigils)
        if body is None:
            continue
        if body.startswith("PURPOSE:"):
            start_index = index
            first_body = body.removeprefix("PURPOSE:").strip()
            break

    if start_index is None:
        raise SystemExit(f"FATAL: no PURPOSE: marker found (language={language})")

    out: list[str] = [first_body]

    # Collect the contiguous run of comment lines that follows the marker.
    for raw in lines[start_index + 1 :]:
        stripped = raw.strip()
        if not stripped:
            out.append("")
            continue
        body = _strip_comment(stripped, sigils)
        if body is None:
            break
        out.append(body)

    # Stop at the last non-blank line: trailing blank comment lines are noise.
    while out and not out[-1]:
        out.pop()
    return "\n".join(out)


def main() -> int:
    """Builds the deliverable.

    Returns:
        ``0`` on success, ``1`` if a guard failed.
    """
    problems: list[str] = []

    for display, path, _language in FILES:
        if not path.is_file():
            problems.append(f"missing file: {display}")
            continue
        text = path.read_text(encoding="utf-8")
        lowered = text.lower()
        if display not in MARKER_SCAN_EXEMPT:
            for marker in ELISION_MARKERS:
                if marker.lower() in lowered:
                    problems.append(f"elision marker {marker!r} in {display}")
        if "PURPOSE:" not in text:
            problems.append(f"no PURPOSE comment: {display}")

    if problems:
        for problem in problems:
            print(f"FATAL: {problem}", file=sys.stderr)
        return 1

    parts: list[str] = []
    parts.append(
        "# Phase 16 — Complete File Output\n\n"
        "Rust ↔ Python Production AgentBridge Integration\n\n"
        "Every file created or modified in this phase, with its purpose and its\n"
        "**complete** content. This document is generated by\n"
        "`python-orchestrator/build_phase16_deliverable.py`, which refuses to emit\n"
        "a file that is missing, lacks a `PURPOSE:` comment, or contains a common\n"
        'elision marker — so "no omissions" is enforced by the build rather than\n'
        "promised by hand.\n"
    )

    for display, path, language in FILES:
        text = path.read_text(encoding="utf-8")
        purpose = extract_purpose(text, language)
        fence = {
            "rust": "rust",
            "python": "python",
            "proto": "proto",
            "toml": "toml",
            "shell": "sh",
        }[language]
        parts.append(f"\n---\n\n## FILE: `{display}`\n")
        parts.append(f"### PURPOSE\n\n{purpose}\n")
        parts.append(f"\n### CONTENT\n\n```{fence}\n{text}\n```\n")

    OUT.write_text("".join(parts), encoding="utf-8")
    total = sum(len(path.read_text(encoding="utf-8").splitlines()) for _, path, _ in FILES)
    print(f"wrote {OUT} — {len(FILES)} files, {total} source lines, no omissions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
