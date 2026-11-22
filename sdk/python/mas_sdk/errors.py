"""Typed MAS SDK errors.

The transport keeps returning ``Result`` values for backwards compatibility;
resource clients and callers that prefer exceptions can opt in with
``raise_on_error=True`` and branch on these types.
"""

from __future__ import annotations


class MasApiError(RuntimeError):
    """A non-success API envelope."""

    def __init__(self, status: int, code: str, message: str, request_id: str | None = None) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.request_id = request_id


class MasRateLimited(MasApiError):
    """HTTP 429 after the retry budget was exhausted."""

    def __init__(self, retry_after: float | None, message: str = "rate limited") -> None:
        super().__init__(429, "RATE_LIMITED", message)
        self.retry_after = retry_after


class MasScopeError(RuntimeError):
    """A scoped route was used without tenant and organization ids."""
