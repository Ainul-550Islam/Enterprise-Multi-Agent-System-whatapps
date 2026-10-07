"""Explicit protocol compatibility and version negotiation rules.

The Python orchestrator and the Rust Core evolve independently, so every
inbound request declares the wire contract it was built against and the
orchestrator either accepts it or rejects it with
:class:`~mas_orchestrator.contracts.errors.ProtocolError`.

Rules implemented here:

* Versions are ``MAJOR.MINOR.PATCH`` triples compared **numerically**, never
  lexically (``"1.10.0"`` must sort above ``"1.9.0"``).
* Only an exact ``MAJOR`` match is accepted. A major bump is a breaking change
  and requires a coordinated rollout, not silent tolerance.
* The orchestrator accepts callers on a **lower or equal minor** within the
  same major, and tolerates callers on a **higher minor** only when that minor
  is explicitly listed as supported — a newer caller may use fields we do not
  understand, which is unsafe to assume.
* Negotiation never guesses. An unparseable or absent version is rejected; it
  is not defaulted to the current version, because doing so would let a
  genuinely incompatible caller through.

The value sent by Rust is defined by ``AI_PROTOCOL_VERSION`` in
``crates/ai-bridge/src/lib.rs``; both sides must be updated together.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from mas_orchestrator.contracts.errors import ProtocolError

__all__ = [
    "CURRENT_PROTOCOL_VERSION",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "ProtocolVersion",
    "negotiate_protocol_version",
    "parse_protocol_version",
]


@dataclass(frozen=True, slots=True, order=True)
class ProtocolVersion:
    """A parsed semantic-ish wire version.

    Ordering is lexicographic over the ``(major, minor, patch)`` tuple, which
    matches numeric ordering because every component is an ``int``.
    """

    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        """Renders the canonical ``MAJOR.MINOR.PATCH`` form."""
        return f"{self.major}.{self.minor}.{self.patch}"

    @property
    def is_stable(self) -> bool:
        """A ``0.x`` major is pre-stable and never negotiated automatically."""
        return self.major > 0


#: The version this build emits and prefers.
CURRENT_PROTOCOL_VERSION: Final[ProtocolVersion] = ProtocolVersion(1, 1, 0)

#: Every version this build will accept. Rust ships one of these in the
#: ``x-ai-protocol-version`` metadata header and in the request body. The 1.1
#: minor adds the Data Hub bridge without changing AgentBridge wire semantics.
SUPPORTED_PROTOCOL_VERSIONS: Final[frozenset[ProtocolVersion]] = frozenset(
    {
        ProtocolVersion(1, 0, 0),
        ProtocolVersion(1, 1, 0),
    }
)


def parse_protocol_version(raw: str) -> ProtocolVersion:
    """Parses ``MAJOR.MINOR.PATCH`` into a :class:`ProtocolVersion`.

    Args:
        raw: The version string exactly as supplied by the caller.

    Raises:
        ProtocolError: If the string is empty, has fewer or more than three
            components, or contains a non-numeric or negative component.
            Pre-release/build suffixes (``1.0.0-rc1``) are rejected: the wire
            contract has no defined semantics for them.
    """
    if not raw:
        raise ProtocolError(
            "protocol version is required",
            details={"reason": "empty"},
        )

    parts = raw.strip().split(".")
    if len(parts) != 3:
        raise ProtocolError(
            "protocol version must be MAJOR.MINOR.PATCH",
            details={"reason": "component_count", "components": len(parts)},
        )

    numbers: list[int] = []
    for part in parts:
        if not part.isdigit():
            raise ProtocolError(
                "protocol version components must be non-negative integers",
                details={"reason": "non_numeric", "component": part},
            )
        numbers.append(int(part))

    version = ProtocolVersion(numbers[0], numbers[1], numbers[2])
    if not version.is_stable:
        raise ProtocolError(
            "protocol version major zero is not a supported wire contract",
            details={"reason": "unstable_major", "version": str(version)},
        )
    return version


def negotiate_protocol_version(raw: str | None) -> ProtocolVersion:
    """Validates an inbound protocol version and returns the negotiated one.

    Args:
        raw: The caller-declared version, or ``None`` when absent.

    Returns:
        The accepted :class:`ProtocolVersion`.

    Raises:
        ProtocolError: If the version is absent, unparseable, outside the
            supported set, or belongs to a different major line. The error
            details always explain *why* negotiation failed so an operator can
            fix the mismatch without reading logs from both sides.
    """
    if raw is None or not raw.strip():
        raise ProtocolError(
            "missing protocol version: the caller must declare its wire contract",
            details={
                "reason": "absent",
                "supported": sorted(str(v) for v in SUPPORTED_PROTOCOL_VERSIONS),
            },
        )

    requested = parse_protocol_version(raw)

    if requested in SUPPORTED_PROTOCOL_VERSIONS:
        return requested

    supported_majors = {version.major for version in SUPPORTED_PROTOCOL_VERSIONS}
    if requested.major not in supported_majors:
        raise ProtocolError(
            "incompatible protocol major version",
            details={
                "reason": "major_mismatch",
                "requested": str(requested),
                "supported_majors": sorted(supported_majors),
                "supported": sorted(str(v) for v in SUPPORTED_PROTOCOL_VERSIONS),
            },
        )

    # Same major, but a minor we do not implement. A caller ahead of us may be
    # sending fields we would silently ignore; a caller behind us is simply
    # unknown to this build. Both are refused rather than guessed at.
    raise ProtocolError(
        "unsupported protocol version",
        details={
            "reason": "unsupported_minor",
            "requested": str(requested),
            "supported": sorted(str(v) for v in SUPPORTED_PROTOCOL_VERSIONS),
        },
    )
