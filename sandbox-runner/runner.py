#!/usr/bin/env python3
"""sandbox-runner — execute untrusted snippets under hard resource limits.

The runner enforces four ceilings before anything else is trusted:

* **memory** (``enforce_limits``): address-space limit via RLIMIT_AS — an
  allocation bomb dies with a memory exit instead of taking the host down;
* **cpu**: RLIMIT_CPU — an infinite loop is killed at the deadline;
* **output**: RLIMIT_FSIZE — a huge-output program is cut off at the cap and
  the truncated output is reported, not silently dropped;
* **nproc**: RLIMIT_NPROC — a fork bomb cannot spawn unbounded children.

The HTTP ``POST /execute`` surface (the contract
``RemoteSandboxBackend`` posts to) refuses unsigned bodies. The HMAC is
computed the same way the orchestrator does: canonical JSON
(``separators=(',', ':'), sort_keys=True``) then ``sha256=`` hex in
``x-mas-signature``. The key is ``MAS_SANDBOX_HMAC_KEY``. With no key
configured the server answers 503 rather than running unsigned code.

The seccomp profile (``seccomp.json``) is a default-deny allowlist applied by
the container runtime (``--security-opt seccomp=...``); the runner validates
the profile is well-formed and rejects profiles that allowlisted escape
hatches (mount, ptrace, reboot, pivot_root, chroot, unshare, setns, bpf,
kexec, module load). ``socket``/``clone``/``fork`` stay in the container
profile because this process is the HTTP server — snippet isolation for
egress is the operator's internal network, not a syscall the server can
drop and still listen.

Usage::

    python sandbox-runner/runner.py --file snippet.py
    python sandbox-runner/runner.py --serve --bind 127.0.0.1 --port 8090
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import resource
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

DEFAULT_MEMORY_BYTES = 64 * 1024 * 1024
DEFAULT_CPU_SECONDS = 2
DEFAULT_OUTPUT_BYTES = 64 * 1024
DEFAULT_NPROC = 32
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8090
HMAC_ENV = "MAS_SANDBOX_HMAC_KEY"
MAX_EXECUTE_BYTES = 256 * 1024

# Syscalls the profile must never allow. The runner itself still needs
# socket/clone/fork to serve HTTP and spawn the limited child.
DENIED_SYSCALLS = frozenset(
    {
        "mount",
        "umount",
        "umount2",
        "ptrace",
        "reboot",
        "pivot_root",
        "chroot",
        "unshare",
        "setns",
        "bpf",
        "init_module",
        "finit_module",
        "delete_module",
        "kexec_load",
        "kexec_file_load",
        "swapon",
        "swapoff",
        "perf_event_open",
        "userfaultfd",
        "process_vm_readv",
        "process_vm_writev",
        "syslog",
        "io_uring_setup",
        "io_uring_enter",
        "io_uring_register",
    }
)


@dataclass(frozen=True)
class Limits:
    """Ceilings applied to one snippet run."""

    memory_bytes: int = DEFAULT_MEMORY_BYTES
    cpu_seconds: int = DEFAULT_CPU_SECONDS
    output_bytes: int = DEFAULT_OUTPUT_BYTES
    nproc: int = DEFAULT_NPROC
    network_access: bool = False


@dataclass(frozen=True)
class RunResult:
    """Outcome of one sandboxed run."""

    exit_code: int
    stdout: str
    stderr: str
    truncated: bool
    timed_out: bool = False


def enforce_limits(limits: Limits) -> None:
    """Applies rlimits in the *current* process (called in the child)."""
    resource.setrlimit(resource.RLIMIT_AS, (limits.memory_bytes, limits.memory_bytes))
    resource.setrlimit(resource.RLIMIT_CPU, (limits.cpu_seconds, limits.cpu_seconds))
    resource.setrlimit(resource.RLIMIT_FSIZE, (limits.output_bytes, limits.output_bytes))
    resource.setrlimit(resource.RLIMIT_NPROC, (limits.nproc, limits.nproc))


def load_seccomp_profile(path: Path) -> dict:
    """Validates the seccomp profile: strict JSON, default-deny, allowlist."""
    profile = json.loads(path.read_text())
    if profile.get("defaultAction") != "SCMP_ACT_ERRNO":
        raise ValueError("seccomp profile must default-deny with SCMP_ACT_ERRNO")
    names = profile.get("syscalls", [])
    if not names or not all(entry.get("action") == "SCMP_ACT_ALLOW" for entry in names):
        raise ValueError("seccomp profile must carry an explicit SCMP_ACT_ALLOW syscall allowlist")
    allowed: set[str] = set()
    for entry in names:
        allowed.update(entry.get("names") or [])
    blocked = sorted(allowed & DENIED_SYSCALLS)
    if blocked:
        raise ValueError(f"seccomp profile must not allow {blocked}")
    return profile


def canonical_json(payload: Any) -> bytes:
    """Matches RemoteSandboxBackend's HMAC input."""
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def sign_payload(payload: Any, key: str) -> str:
    """Returns the ``sha256=<hex>`` header value for ``payload``."""
    digest = hmac.new(key.encode("utf-8"), canonical_json(payload), sha256).hexdigest()
    return f"sha256={digest}"


def verify_request_hmac(*, body: bytes, header: str, key: str) -> None:
    """Verifies ``x-mas-signature`` over the canonical form of ``body``.

    Raises:
        PermissionError: missing key, missing/forged signature, or a body
            that is not JSON (a non-JSON body cannot have been signed by
            the orchestrator).
    """
    if not key:
        raise PermissionError("sandbox HMAC key is not configured")
    if not header or not header.startswith("sha256="):
        raise PermissionError("invalid hmac")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PermissionError("invalid hmac body") from error
    expected = sign_payload(payload, key)
    if not hmac.compare_digest(expected, header):
        raise PermissionError("invalid hmac")


def limits_from_document(document: dict[str, Any]) -> Limits:
    """Maps the RemoteSandboxBackend ``limits`` object onto rlimits."""
    raw = document.get("limits") or {}
    memory_mb = int(raw.get("memory_megabytes") or (DEFAULT_MEMORY_BYTES // (1024 * 1024)))
    cpu_seconds = int(raw.get("cpu_seconds") or DEFAULT_CPU_SECONDS)
    output_bytes = int(raw.get("max_output_bytes") or DEFAULT_OUTPUT_BYTES)
    network_access = bool(raw.get("network_access", False))
    timeout_seconds = raw.get("timeout_seconds")
    if timeout_seconds is not None:
        cpu_seconds = max(cpu_seconds, int(float(timeout_seconds)))
    return Limits(
        memory_bytes=max(1, memory_mb) * 1024 * 1024,
        cpu_seconds=max(1, cpu_seconds),
        output_bytes=max(1, output_bytes),
        network_access=network_access,
    )


def run_snippet(
    code: str,
    *,
    limits: Limits | None = None,
    python: str | None = None,
    stdin: str = "",
) -> RunResult:
    """Runs `code` in a limited child process and returns the outcome."""
    limits = limits or Limits()
    if limits.network_access:
        raise PermissionError("network_access is disabled for sandboxed snippets")
    python = python or sys.executable
    timed_out = False
    with tempfile.TemporaryDirectory(prefix="sandbox-run-") as tmp:
        script = Path(tmp) / "snippet.py"
        script.write_text(code)
        try:
            child = subprocess.run(
                [python, str(script)],
                input=stdin.encode("utf-8"),
                capture_output=True,
                timeout=limits.cpu_seconds + 5,
                preexec_fn=lambda: enforce_limits(limits),
                env={"PATH": os.environ.get("PATH", "/usr/bin"), "LANG": "C.UTF-8"},
            )
            exit_code = child.returncode
            stdout_b, stderr_b = child.stdout, child.stderr
        except subprocess.TimeoutExpired as error:
            timed_out = True
            exit_code = -1
            stdout_b = error.stdout or b""
            stderr_b = error.stderr or b""
    stdout = stdout_b.decode("utf-8", "replace")
    stderr = stderr_b.decode("utf-8", "replace")
    truncated = len(stdout) > limits.output_bytes or len(stderr) > limits.output_bytes
    stdout = stdout[: limits.output_bytes]
    stderr = stderr[: limits.output_bytes]
    return RunResult(
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        truncated=truncated,
        timed_out=timed_out,
    )


def execute_document(document: dict[str, Any]) -> dict[str, Any]:
    """Runs one ``/execute`` document and returns the sandbox JSON body."""
    language = str(document.get("language") or "")
    if language != "python":
        raise PermissionError("only language=python is accepted")
    source = document.get("source")
    if not isinstance(source, str) or not source:
        raise PermissionError("source must be a non-empty string")
    stdin = document.get("stdin") if isinstance(document.get("stdin"), str) else ""
    limits = limits_from_document(document)
    result = run_snippet(source, limits=limits, stdin=stdin)
    killed_reason = None
    if result.timed_out:
        killed_reason = "timeout"
    elif result.exit_code != 0:
        killed_reason = "nonzero_exit"
    return {
        "exit_code": result.exit_code,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "timed_out": result.timed_out,
        "killed_reason": killed_reason,
        "truncated": result.truncated,
    }


class ExecuteHandler(BaseHTTPRequestHandler):
    """HMAC-gated ``POST /execute``."""

    server_version = "mas-sandbox/1.1.0"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        sys.stderr.write("sandbox-runner: " + (format % args) + "\n")

    def do_POST(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] != "/execute":
            self._send_json(404, {"error": "not found"})
            return
        key = os.environ.get(HMAC_ENV, "")
        if not key:
            self._send_json(503, {"error": "MAS_SANDBOX_HMAC_KEY is not set"})
            return
        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0 or length > MAX_EXECUTE_BYTES:
            self._send_json(413, {"error": "body too large"})
            return
        body = self.rfile.read(length)
        try:
            verify_request_hmac(
                body=body,
                header=self.headers.get("x-mas-signature", ""),
                key=key,
            )
            document = json.loads(body.decode("utf-8"))
            if not isinstance(document, dict):
                raise PermissionError("execute body must be an object")
            result = execute_document(document)
        except PermissionError as error:
            self._send_json(401, {"error": str(error)})
            return
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_json(400, {"error": "body is not JSON"})
            return
        self._send_json(200, result)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def serve(bind: str, port: int) -> None:
    """Serves ``POST /execute`` until killed."""
    httpd = ThreadingHTTPServer((bind, port), ExecuteHandler)
    sys.stderr.write(f"sandbox-runner: listening on {bind}:{port}\n")
    httpd.serve_forever()


def main() -> int:
    parser = argparse.ArgumentParser(description="run a snippet under hard limits")
    parser.add_argument("--file", type=Path)
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--bind", default=DEFAULT_BIND)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--memory-bytes", type=int, default=DEFAULT_MEMORY_BYTES)
    parser.add_argument("--cpu-seconds", type=int, default=DEFAULT_CPU_SECONDS)
    parser.add_argument("--output-bytes", type=int, default=DEFAULT_OUTPUT_BYTES)
    parser.add_argument("--nproc", type=int, default=DEFAULT_NPROC)
    args = parser.parse_args()
    if args.serve:
        serve(args.bind, args.port)
        return 0
    if args.file is None:
        parser.error("either --file or --serve is required")
    limits = Limits(
        memory_bytes=args.memory_bytes,
        cpu_seconds=args.cpu_seconds,
        output_bytes=args.output_bytes,
        nproc=args.nproc,
    )
    result = run_snippet(args.file.read_text(), limits=limits)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
