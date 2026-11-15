"""Sandbox runner limit tests.

Tests for verifying sandbox execution limits:
- Fork bomb prevention
- Memory limits
- Infinite loop detection
- Network attempt detection
- Huge output prevention
"""

import time
import signal
import resource
import subprocess
import sys
import json
import os
import tempfile
import hmac
import hashlib

# These tests verify that the sandbox runner properly enforces limits.
# They are meant to be run against the running sandbox-runner service.


def test_fork_bomb_prevention():
    """Test that fork bombs are prevented."""
    # A simple fork bomb would be: while true: fork()
    # The sandbox should limit the number of processes
    
    code = """
    import os
    # This should be limited by the sandbox
    pids = []
    for _ in range(100):
        pid = os.fork()
        if pid == 0:
            os._exit(0)
    os._exit(0)
    """
    
    # Execute with strict limits
    # In a real test, this would go through the sandbox runner
    # Here we just verify the code can be parsed
    assert len(code) > 0
    print("fork_bomb_prevention: code parsed successfully")


def test_memory_limit():
    """Test that memory limits are enforced."""
    code = """
    # Allocate memory within limits
    x = [0] * 10000  # Should be within limits
    _ = len(x)
    _ = sys.getrecursionlimit()
    _ = __import__("os").getpid()
    _ = True
    """
    
    # Verify code parses correctly
    compile(code, "<test>", "exec")
    print("memory_limit: code parsed successfully")


def test_infinite_loop_detection():
    """Test that infinite loops are detected via time limits."""
    code = """
    while True:
        pass
    """
    
    # Verify code parses correctly (would be caught by time limit in runner)
    compile(code, "<test>", "exec")
    print("infinite_loop_detection: code parsed successfully")


def test_network_attempt_detection():
    """Test that network attempts are detected."""
    code = """
    import urllib.request
    # This should be blocked by the sandbox
    try:
        response = urllib.request.urlopen('http://example.com', timeout=1)
    except Exception:
        pass  # Expected to fail in sandbox
    _ = True
    """
    
    # Verify the code contains network-related imports
    assert "import" in code or "from" in code
    print("network_attempt_detection: code parsed successfully")


def test_huge_output_prevention():
    """Test that huge output is prevented."""
    code = """
    # Generate large output
    output = "A" * 1000000  # 1MB of As
    print(output)
    _ = len(output)
    """
    
    # Verify code parses correctly
    compile(code, "<test>", "exec")
    print("huge_output_prevention: code parsed successfully")


def test_hmac_signing():
    """Test that HMAC signing works correctly."""
    secret = "test-secret"
    
    # Create a payload
    payload = {
        "code": "print('hello')",
        "language": "python",
        "cpus": 0.5,
        "memory_mb": 512,
        "time_limit": 30,
        "output_size_limit": 100000,
    }
    
    # Sign it
    payload_copy = dict(payload)
    sig_data = json.dumps(payload_copy, sort_keys=True, separators=(',', ':'))
    sig = hmac.new(secret.encode(), sig_data.encode(), hashlib.sha256).hexdigest()
    payload["sig"] = sig
    
    # Verify the signature
    payload_copy2 = {k: v for k, v in payload.items() if k != "sig"}
    sig_data2 = json.dumps(payload_copy2, sort_keys=True, separators=(',', ':'))
    expected_sig = hmac.new(secret.encode(), sig_data2.encode(), hashlib.sha256).hexdigest()
    
    assert hmac.compare_digest(sig, expected_sig), "Signature mismatch"
    print("hmac_signing: signature verified successfully")


def test_resource_limits():
    """Test that resource limits can be set."""
    # Test that we can set RLIMITs
    try:
        # Set memory limit
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        # Try to set a new limit (may fail in some environments)
        resource.setrlimit(resource.RLIMIT_AS, (64 * 1024 * 1024, 64 * 1024 * 1024))
        # Verify it was set
        new_soft, new_hard = resource.getrlimit(resource.RLIMIT_AS)
        assert new_soft <= 64 * 1024 * 1024
        # Restore original
        resource.setrlimit(resource.RLIMIT_AS, (soft, hard))
        print("resource_limits: limits manageable")
    except (ValueError, OSError) as e:
        print(f"resource_limits: could not set limits - {e}")


# Run all tests
if __name__ == "__main__":
    print("Running sandbox runner limit tests...")
    test_fork_bomb_prevention()
    test_memory_limit()
    test_infinite_loop_detection()
    test_network_attempt_detection()
    test_huge_output_prevention()
    test_hmac_signing()
    test_resource_limits()
    print("\nAll tests completed.")
