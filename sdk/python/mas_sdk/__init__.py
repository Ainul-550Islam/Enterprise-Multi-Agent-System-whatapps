"""mas-sdk — Python client for the Enterprise MAS API.

Scope guards fail locally; mutations carry idempotency keys; responses are
unwrapped from the stable envelope. Typed exceptions are available for callers
that prefer exceptions over ``Result`` branching.
"""

from __future__ import annotations

from mas_sdk.client import MasClient, MissingScopeError, Result, RetryPolicy, Scope
from mas_sdk.errors import MasApiError, MasRateLimited, MasScopeError

__version__ = "1.1.0"

__all__ = [
    "MasClient",
    "MasApiError",
    "MasRateLimited",
    "MasScopeError",
    "MissingScopeError",
    "Result",
    "RetryPolicy",
    "Scope",
    "__version__",
]
