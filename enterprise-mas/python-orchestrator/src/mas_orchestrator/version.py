"""Application and version metadata for the Python AI Orchestrator.

This module is intentionally side-effect free: importing it must never start
I/O, read configuration, or touch the network. Diagnostics endpoints, the
``User-Agent`` presented to Rust Core, and compatibility reporting all read
from here.

The values are kept in one place so that ``pyproject.toml`` (packaging) and
runtime reporting cannot drift apart silently: CI verifies that
``pyproject.toml``'s ``project.version`` matches :data:`__version__`.
"""

from __future__ import annotations

from importlib import metadata

#: Semantic version of the Python AI Orchestrator.
__version__: str = "0.1.0"

#: Logical service name reported to Rust Core, logs, and metrics.
SERVICE_NAME: str = "mas-orchestrator"

#: Version of the Rust Core gRPC wire contract this build was written against.
#:
#: This is a *compatibility statement*, not a negotiation: the orchestrator
#: refuses to serve traffic whose major protocol version differs from
#: :data:`AI_PROTOCOL_MAJOR` (see ``contracts/protocol_version.py``).
AI_PROTOCOL_VERSION: str = "1.0.0"

#: Major component of :data:`AI_PROTOCOL_VERSION`, used for compatibility gates.
AI_PROTOCOL_MAJOR: int = 1

#: Minimum Rust Core ``mas-api`` minor release whose ``ExecutionApi`` surface
#: this orchestrator is compatible with. Reported in health diagnostics so an
#: operator can see a mismatch before it turns into runtime errors.
MIN_RUST_API_VERSION: str = "0.1.0"


def installed_version() -> str:
    """Returns the installed distribution version, falling back to source.

    In an editable/source checkout ``importlib.metadata`` may not resolve a
    distribution. Rather than crash during diagnostics, fall back to the
    version constant baked into this module.

    Returns:
        The installed distribution version string, or :data:`__version__`.
    """
    try:
        return metadata.version("mas-orchestrator")
    except metadata.PackageNotFoundError:
        return __version__


def user_agent() -> str:
    """Builds the ``User-Agent`` value presented on outbound Rust Core calls.

    Returns:
        A short, non-secret identity string, e.g.
        ``mas-orchestrator/0.1.0 (ai-protocol/1.0.0)``.
    """
    return f"{SERVICE_NAME}/{installed_version()} (ai-protocol/{AI_PROTOCOL_VERSION})"


def build_info() -> dict[str, str]:
    """Returns a diagnostics dictionary safe to expose on health endpoints.

    Deliberately excludes anything environment- or secret-derived: no host
    names, no credentials, no configuration values.

    Returns:
        A mapping of stable diagnostic keys to their values.
    """
    return {
        "service": SERVICE_NAME,
        "version": installed_version(),
        "ai_protocol_version": AI_PROTOCOL_VERSION,
        "min_rust_api_version": MIN_RUST_API_VERSION,
    }
