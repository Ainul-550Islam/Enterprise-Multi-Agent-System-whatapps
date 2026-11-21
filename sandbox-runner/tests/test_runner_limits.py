"""Resource-limit enforcement: every limit gets a real runaway program and a
real assertion (memory, infinite loop, huge output, fork bomb, network flag).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from runner import Limits, load_seccomp_profile, run_snippet  # noqa: E402

TIGHT = Limits(memory_bytes=32 * 1024 * 1024, cpu_seconds=1, output_bytes=8 * 1024, nproc=8)


def test_memory_limit_kills_an_allocation_bomb() -> None:
    result = run_snippet(
        "x = bytearray(256 * 1024 * 1024)\nprint('allocated', len(x))\n",
        limits=TIGHT,
    )
    assert result.exit_code != 0, "an allocation bomb must not succeed"
    assert "allocated" not in result.stdout, "the bomb must die before printing"


def test_infinite_loop_is_killed_by_the_cpu_limit() -> None:
    result = run_snippet("while True:\n    pass\n", limits=TIGHT)
    assert result.exit_code != 0, "an infinite loop must not succeed"
    assert result.stdout == "", "the loop must be killed before producing output"


def test_huge_output_is_truncated_at_the_output_cap() -> None:
    result = run_snippet(
        "import sys\nfor _ in range(100000):\n    sys.stdout.write('A' * 1000)\n",
        limits=TIGHT,
    )
    assert len(result.stdout) == TIGHT.output_bytes, "output must be cut at the cap"
    assert result.stdout.startswith("A"), "truncated output keeps its head"


def test_a_normal_snippet_runs_to_completion() -> None:
    result = run_snippet("print('hello sandbox')\n", limits=TIGHT)
    assert result.exit_code == 0
    assert result.stdout.strip() == "hello sandbox"


def test_seccomp_profile_is_default_deny() -> None:
    profile = load_seccomp_profile(Path(__file__).resolve().parents[1] / "seccomp.json")
    assert profile["defaultAction"] == "SCMP_ACT_ERRNO"


def test_fork_bomb_is_contained_by_nproc() -> None:
    result = run_snippet(
        "import os, sys\n"
        "for i in range(1000):\n"
        "    try:\n"
        "        pid = os.fork()\n"
        "    except OSError:\n"
        "        print('fork-limited', i)\n"
        "        sys.exit(0)\n"
        "    if pid == 0:\n"
        "        os._exit(0)\n"
        "print('unlimited')\n"
        "sys.exit(1)\n",
        limits=TIGHT,
    )
    assert result.timed_out is False, "the bomb must not run to the wall-clock timeout"
    assert "unlimited" not in result.stdout
    assert result.exit_code == 0
    assert "fork-limited" in result.stdout
    count = int(result.stdout.strip().split()[-1])
    assert count < 64, f"nproc ceiling leaked ({count} children)"


def test_network_access_flag_is_refused() -> None:
    with pytest.raises(PermissionError, match="network_access"):
        run_snippet("print(1)\n", limits=Limits(network_access=True))
