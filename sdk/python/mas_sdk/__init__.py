"""mas-sdk — Python client for the Enterprise MAS API.

Scope guards fail locally (before any network call); mutations carry
idempotency keys; responses are unwrapped from the stable envelope so callers
branch on result objects, never on message text.
"""

from __future__ import annotations

from mas_sdk.client import MasClient, MissingScopeError, Scope

__all__ = ["MasClient", "MissingScopeError", "Scope"]
