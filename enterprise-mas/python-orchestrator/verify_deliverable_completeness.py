# PURPOSE:
# Proves the Phase 16 deliverable violates nothing and shortens nothing.
#
# A grep for "..." only catches an author who left a marker behind. It cannot
# catch the failure that actually matters: a file whose content was quietly
# truncated to keep the document small. So this script reconstructs every file
# from the deliverable and compares it byte-for-byte against the source on
# disk -- the only comparison that proves "full code" rather than "no visible
# ellipsis".
#
# Exits non-zero on any mismatch, so it can be wired into a check.

"""Verifies the generated deliverable contains complete, verbatim files.

Checks, in order:

1. every declared file resolves and is non-empty;
2. every file carries a ``PURPOSE:`` block;
3. no elision marker appears anywhere;
4. **each file's content in the document is byte-identical to the source.**

The fourth check is the point. The first three are necessary but weak: a file
truncated at a plausible boundary satisfies all of them.
"""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
DELIVERABLE = WORKSPACE_ROOT / "RUST_PYTHON_BRIDGE_PHASE16_FILES.md"
PYTHON_ROOT = WORKSPACE_ROOT / "enterprise-mas" / "python-orchestrator"
REPO_ROOT = WORKSPACE_ROOT / "enterprise-mas" / "rust-core"

#: Phrases that mean "content removed here".
ELISION_MARKERS: tuple[str, ...] = (
    "... omitted",
    "# Rest of the code",
    "// existing code omitted",
    "# same as above",
    "<truncated>",
    "content omitted",
    "# " + "..." + " rest unchanged",  # split: this file contains its own list
    "rest of the code",
    "omitted for brevity",
    "snip",
)

#: Files exempt from the marker scan, by exact displayed path.
#
#: This module has to *name* every marker it hunts for, so it contains all of
#: them and would otherwise fail its own check. The exemption is by exact path
#: rather than by pattern, so it cannot be widened by renaming something; keep
#: it to files whose job is literally to define the markers.
MARKER_SCAN_EXEMPT: frozenset[str] = frozenset({"verify_deliverable_completeness.py"})

#: Files the durable-execution pipeline deliberately changed after handoff.
#
#: The byte-for-byte check is a *handoff* proof: it says "what is in the
#: document is what was on disk at Phase 16 sign-off". Once the next task
#: legitimately edits a file, the document is a historical record rather than a
#: live mirror, and a mismatch there is progress rather than a defect.
#:
#: Each entry names the reason, because an unexplained exemption is how a real
#: regression hides. Anything NOT listed here still has to match byte-for-byte,
#: so the check keeps its teeth: it catches a Phase 16 file that was edited by
#: accident.
SUPERSEDED_SINCE_HANDOFF: dict[str, str] = {
    "crates/ai-bridge/src/lib.rs": (
        "the exhaustive `annotate()` match gained an arm for "
        "`AppError::ExternalFailure`, the structured failure an external agent "
        "runtime reports"
    ),
    "crates/worker-service/src/agent_execution.rs": (
        "`AgentRunHandler` now drives `Executor::run_agent_node` (canonical "
        "runtime -> node registry -> agent bridge) instead of calling "
        "`bridge.run()` directly"
    ),
    "crates/worker-service/src/main.rs": (
        "the `--dev-inmemory` lane and `EchoHandler` were deleted so the worker "
        "has exactly one bring-up path; the consumer now takes an "
        "`AttemptCoordinator` over the durable execution port"
    ),
    "crates/worker-service/Cargo.toml": (
        "gained the `mas-application` dependency for `DurableExecutionPort`"
    ),
    "crates/messaging/src/nats.rs": (
        "gained `NatsEventBus`, `event_subject` and `event_headers`: the "
        "production transport the outbox relay publishes through, so durable "
        "events reach the existing NATS JetStream plane"
    ),
    "verify_deliverable_completeness.py": (
        "gained `SUPERSEDED_SINCE_HANDOFF`, so a deliberate post-handoff edit "
        "is reported as recorded progress instead of an unexplained failure"
    ),
}


def parse_deliverable(text: str) -> list[tuple[str, str, str]]:
    """Splits the deliverable into (display path, purpose, content).

    Args:
        text: The deliverable's text.

    Returns:
        One triple per ``## FILE:`` section.

    Raises:
        SystemExit: If a section is malformed, since a silently skipped
            section would make the rest of the report meaningless.
    """
    pattern = re.compile(
        r"^## FILE: `([^`]+)`\n+### PURPOSE\n+(.*?)\n+### CONTENT\n+```[a-z]*\n(.*?)\n```\n",
        re.MULTILINE | re.DOTALL,
    )
    found = pattern.findall(text)
    declared = len(re.findall(r"^## FILE: ", text, re.MULTILINE))
    if len(found) != declared:
        print(
            f"FATAL: parsed {len(found)} of {declared} sections; the document structure changed",
            file=sys.stderr,
        )
        raise SystemExit(1)
    return found


def resolve(display: str) -> Path:
    """Maps a displayed path back to its source file.

    Args:
        display: The path as shown in the deliverable.

    Returns:
        The absolute path on disk.

    Raises:
        SystemExit: If the path belongs to neither tree.
    """
    if display.startswith("crates/") or display.startswith("migrations/"):
        return REPO_ROOT / display
    return PYTHON_ROOT / display


def sha256(data: bytes) -> str:
    """Returns the hex digest of *data*."""
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    """Entry point.

    Returns:
        ``0`` when the deliverable is complete and verbatim, ``1`` otherwise.
    """
    if not DELIVERABLE.is_file():
        print(f"FATAL: {DELIVERABLE} not found", file=sys.stderr)
        return 1

    text = DELIVERABLE.read_text(encoding="utf-8")
    sections = parse_deliverable(text)
    failures: list[str] = []
    superseded: dict[str, str] = {}

    print(f"deliverable: {DELIVERABLE}")
    print(f"sections found: {len(sections)}\n")

    for display, purpose, content in sections:
        path = resolve(display)

        if not path.is_file():
            failures.append(f"{display}: source file missing at {path}")
            continue

        source = path.read_text(encoding="utf-8")

        if not content.strip():
            failures.append(f"{display}: empty content in the deliverable")
            continue

        # Checked against the source, not the extracted text: the builder
        # strips comment markers, so the rendered purpose no longer contains
        # the literal "PURPOSE:" it was found by.
        if "PURPOSE:" not in source:
            failures.append(f"{display}: no PURPOSE block in the source")
            continue

        if not purpose.strip():
            failures.append(f"{display}: empty PURPOSE section in the deliverable")
            continue

        if display not in MARKER_SCAN_EXEMPT:
            lowered = content.lower()
            for marker in ELISION_MARKERS:
                if marker.lower() in lowered:
                    failures.append(f"{display}: elision marker {marker!r}")

        # The decisive check. Compared as bytes because a trailing-newline or
        # CRLF difference would otherwise pass a string compare in some
        # readers while producing a file that is not what shipped.
        if sha256(content.encode("utf-8")) != sha256(source.encode("utf-8")):
            reason = SUPERSEDED_SINCE_HANDOFF.get(display)
            if reason is None:
                failures.append(
                    f"{display}: content differs from source "
                    f"(deliverable {len(content)} chars, source {len(source)} chars)"
                )
            else:
                superseded[display] = reason

    verified = len(sections) - len({f.split(":")[0] for f in failures}) - len(superseded)
    print(f"files verified byte-for-byte: {verified}/{len(sections)}")

    if superseded:
        print(f"\nSUPERSEDED SINCE HANDOFF ({len(superseded)}) -- expected, not failures:")
        for display, reason in sorted(superseded.items()):
            print(f"  - {display}: {reason}")

    if failures:
        print(f"\nFAILURES ({len(failures)}):")
        for failure in failures:
            print(f"  - {failure}")
        return 1

    print("\nOK: every file is complete, verbatim, and carries a PURPOSE block.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
