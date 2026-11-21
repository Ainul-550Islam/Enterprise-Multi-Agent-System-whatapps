"""Seccomp profile contract plus HMAC gating for POST /execute."""

from __future__ import annotations

import json
import os
import sys
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runner import (  # noqa: E402
    DENIED_SYSCALLS,
    ExecuteHandler,
    HMAC_ENV,
    load_seccomp_profile,
    sign_payload,
    verify_request_hmac,
)

PROFILE = Path(__file__).resolve().parents[1] / "seccomp.json"


def test_seccomp_default_deny() -> None:
    profile = load_seccomp_profile(PROFILE)
    assert profile["defaultAction"] == "SCMP_ACT_ERRNO"
    assert profile["defaultErrnoRet"] != 0, "denial must be an error, not success"
    allow = [entry for entry in profile["syscalls"] if entry["action"] == "SCMP_ACT_ALLOW"]
    assert allow, "the allowlist must be explicit"
    assert all(entry["action"] == "SCMP_ACT_ALLOW" for entry in profile["syscalls"])
    names = set(allow[0]["names"])
    for syscall in (
        "mount",
        "ptrace",
        "reboot",
        "pivot_root",
        "chroot",
        "unshare",
        "setns",
        "bpf",
        "kexec_load",
        "init_module",
        "io_uring_setup",
    ):
        assert syscall not in names
    assert names.isdisjoint(DENIED_SYSCALLS)


def test_profile_is_strict_json_without_comments() -> None:
    raw = PROFILE.read_text()
    assert "#" not in raw, "seccomp.json must not carry comment markers"
    json.loads(raw)


def test_broken_profiles_are_refused(tmp_path: Path) -> None:
    missing = tmp_path / "missing-action.json"
    missing.write_text('{"syscalls": []}')
    with pytest.raises(ValueError, match="default-deny"):
        load_seccomp_profile(missing)
    no_allow = tmp_path / "no-allow.json"
    no_allow.write_text('{"defaultAction": "SCMP_ACT_ERRNO", "syscalls": []}')
    with pytest.raises(ValueError, match="allowlist"):
        load_seccomp_profile(no_allow)
    escape = tmp_path / "escape.json"
    escape.write_text(
        json.dumps(
            {
                "defaultAction": "SCMP_ACT_ERRNO",
                "syscalls": [{"action": "SCMP_ACT_ALLOW", "names": ["read", "pivot_root"]}],
            }
        )
    )
    with pytest.raises(ValueError, match="must not allow"):
        load_seccomp_profile(escape)


def test_hmac_rejects_a_forged_tag() -> None:
    payload = {"language": "python", "source": "print(1)"}
    body = json.dumps(payload).encode("utf-8")
    with pytest.raises(PermissionError, match="invalid hmac"):
        verify_request_hmac(body=body, header="sha256=deadbeef", key="secret")


def test_hmac_accepts_the_canonical_signature() -> None:
    payload = {"language": "python", "source": "print(1)", "stdin": ""}
    body = json.dumps(payload).encode("utf-8")
    header = sign_payload(payload, "secret")
    verify_request_hmac(body=body, header=header, key="secret")


def test_execute_http_requires_hmac(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(HMAC_ENV, "test-hmac-key")
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), ExecuteHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    url = f"http://{host}:{port}/execute"
    payload = {"language": "python", "source": "print('hmac-ok')"}
    body = json.dumps(payload).encode("utf-8")
    try:
        forged = Request(
            url,
            data=body,
            method="POST",
            headers={"content-type": "application/json", "x-mas-signature": "sha256=00"},
        )
        with pytest.raises(HTTPError) as forged_error:
            urlopen(forged, timeout=5)  # noqa: S310
        assert forged_error.value.code == 401

        signed = Request(
            url,
            data=body,
            method="POST",
            headers={
                "content-type": "application/json",
                "x-mas-signature": sign_payload(payload, "test-hmac-key"),
            },
        )
        with urlopen(signed, timeout=10) as response:  # noqa: S310
            status = response.status
            document = json.loads(response.read().decode("utf-8"))
        assert status == 200
        assert document["exit_code"] == 0
        assert "hmac-ok" in document["stdout"]
    finally:
        httpd.shutdown()


def test_execute_http_without_a_key_is_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(HMAC_ENV, raising=False)
    assert HMAC_ENV not in os.environ
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), ExecuteHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[:2]
    try:
        request = Request(
            f"http://{host}:{port}/execute",
            data=b'{"language":"python","source":"print(1)"}',
            method="POST",
            headers={"content-type": "application/json"},
        )
        with pytest.raises(HTTPError) as error:
            urlopen(request, timeout=5)  # noqa: S310
        assert error.value.code == 503
    finally:
        httpd.shutdown()
