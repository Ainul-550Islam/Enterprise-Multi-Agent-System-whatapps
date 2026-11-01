"""The sandbox boundary.

:mod:`~mas_orchestrator.tools.builtins.code` runs code; this module defines
what "sandboxed" has to mean for that to be safe, and refuses configurations
that do not meet it.

The rule this exists to enforce is that a code tool with no isolation is not a
weaker code tool, it is a remote shell. So the boundary is stated as an
explicit contract (:class:`SandboxContract`) that a backend must satisfy before
it is accepted, and the checks are the ones that actually matter:

* **No host execution.** A sandbox that runs on the host is refused outright.
* **No ambient network.** Network access is off by default and, if granted, is
  restricted to named hosts — a sandboxed program that can reach the metadata
  service is not sandboxed.
* **Bounded resources.** CPU, memory, wall clock and output all have ceilings,
  and every ceiling is mandatory. An unbounded limit is refused rather than
  defaulted, because "no limit" is the value that causes the incident.
* **No durable writes.** The filesystem is ephemeral.

This module does not implement a sandbox. It decides whether one may be used.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from mas_orchestrator.contracts.errors import ConfigurationError, InvalidRequestError


class NetworkMode(StrEnum):
    """Whether a sandboxed program may open sockets.

    ``NONE`` is the default and the only mode that is safe without review. A
    program that can reach the network from inside a sandbox can reach the
    cloud metadata service, which is usually how a sandbox escape becomes a
    credential theft.
    """

    NONE = "none"
    ALLOWLISTED = "allowlisted"
    FULL = "full"


class FilesystemMode(StrEnum):
    """What the program may write."""

    EPHEMERAL = "ephemeral"
    MOUNTED = "mounted"
    HOST = "host"


class IsolationKind(StrEnum):
    """How isolation is achieved."""

    CONTAINER = "container"
    MICROVM = "microvm"
    WASM = "wasm"
    REMOTE_SERVICE = "remote_service"
    #: Not isolation. Present so a backend can be honest, and so the validator
    #: can refuse it rather than treat an unknown value as acceptable.
    HOST = "host"


#: Isolation kinds that are accepted at all. Anything else is refused.
ACCEPTED_ISOLATION: Final[frozenset[IsolationKind]] = frozenset(
    {
        IsolationKind.CONTAINER,
        IsolationKind.MICROVM,
        IsolationKind.WASM,
        IsolationKind.REMOTE_SERVICE,
    }
)


@dataclass(frozen=True, slots=True)
class SandboxContract:
    """What a sandbox backend guarantees.

    Attributes:
        isolation: How the program is isolated.
        network: Whether sockets may be opened.
        allowed_hosts: Hosts reachable when the mode is ``ALLOWLISTED``.
        filesystem: What may be written.
        max_cpu_seconds: CPU budget. Must be positive.
        max_memory_mb: Memory ceiling. Must be positive.
        max_wall_clock_seconds: Wall-clock ceiling. Must be positive.
        max_output_bytes: Output ceiling. Must be positive.
        max_processes: Process/thread ceiling. Must be positive.
        runs_on_host: Whether execution happens on the orchestrator host. Must
            be ``False``; a ``True`` value is refused.
    """

    isolation: IsolationKind
    network: NetworkMode = NetworkMode.NONE
    allowed_hosts: frozenset[str] = frozenset()
    filesystem: FilesystemMode = FilesystemMode.EPHEMERAL
    max_cpu_seconds: float = 5.0
    max_memory_mb: int = 256
    max_wall_clock_seconds: float = 30.0
    max_output_bytes: int = 1_048_576
    max_processes: int = 64
    runs_on_host: bool = False

    def __post_init__(self) -> None:
        if self.runs_on_host:
            raise ConfigurationError(
                "a sandbox that executes on the orchestrator host is not a sandbox: refusing",
                details={"isolation": self.isolation.value},
            )
        if self.network is NetworkMode.ALLOWLISTED and not self.allowed_hosts:
            raise ConfigurationError(
                "allowlisted network mode requires at least one allowed host",
                details={"field": "allowed_hosts"},
            )
        for field_name in (
            "max_cpu_seconds",
            "max_memory_mb",
            "max_wall_clock_seconds",
            "max_output_bytes",
            "max_processes",
        ):
            value = getattr(self, field_name)
            if value is None or value <= 0:
                raise ConfigurationError(
                    f"{field_name} must be a positive value, not {value!r}: an "
                    "unbounded resource is the default that causes incidents",
                    details={"field": field_name, "value": value},
                )

    @property
    def network_is_constrained(self) -> bool:
        """Whether network access is limited to named hosts or denied."""
        return self.network is not NetworkMode.FULL

    @property
    def writes_are_durable(self) -> bool:
        """Whether the program can leave anything behind."""
        return self.filesystem is not FilesystemMode.EPHEMERAL


def validate_contract(contract: SandboxContract, *, environment: str) -> None:
    """Checks a contract against deployment strictness.

    Args:
        contract: The guarantees a backend claims.
        environment: Deployment environment.

    Raises:
        ConfigurationError: If the contract is unacceptable. In production this
            includes full network access, host filesystem mounts, and any
            isolation kind outside :data:`ACCEPTED_ISOLATION`.
    """
    from mas_orchestrator.config.enums import Environment

    if contract.isolation not in ACCEPTED_ISOLATION:
        raise ConfigurationError(
            "isolation kind is not accepted: code execution must run in a "
            "container, microVM, WASM runtime or remote service",
            details={"isolation": contract.isolation.value},
        )

    if environment != Environment.PRODUCTION.value:
        return

    if contract.network is NetworkMode.FULL:
        raise ConfigurationError(
            "unrestricted network access is not permitted in production: a "
            "sandboxed program could reach the metadata service",
            details={"network": contract.network.value},
        )
    if contract.filesystem is FilesystemMode.HOST:
        raise ConfigurationError(
            "host filesystem access is not permitted in production",
            details={"filesystem": contract.filesystem.value},
        )
    if contract.max_wall_clock_seconds > 120.0:
        raise ConfigurationError(
            "wall-clock ceiling is too generous for production",
            details={"max_wall_clock_seconds": contract.max_wall_clock_seconds},
        )


def assert_may_execute(
    contract: SandboxContract | None,
    *,
    environment: str,
    language: str,
) -> SandboxContract:
    """The single gate a code tool consults before running anything.

    Args:
        contract: The backend's guarantees, or ``None`` for "no sandbox".
        environment: Deployment environment.
        language: The language, for the error detail.

    Returns:
        The validated contract.

    Raises:
        ConfigurationError: If there is no sandbox, or the contract fails
            validation. There is no return path that permits host execution.
    """
    if contract is None:
        raise ConfigurationError(
            "no sandbox is configured; refusing to run code",
            details={
                "language": language,
                "hint": "configure a container or microVM-backed sandbox",
            },
        )
    validate_contract(contract, environment=environment)
    return contract


def deny_reason(contract: SandboxContract | None) -> str | None:
    """Why a contract is unusable, or ``None`` if it is acceptable.

    A query rather than a raise, so startup validation can report every problem
    at once instead of stopping at the first.
    """
    if contract is None:
        return "no sandbox configured"
    try:
        validate_contract(contract, environment="production")
    except ConfigurationError as error:
        return str(error)
    return None


def is_safe_language(language: str, *, allowed: frozenset[str]) -> bool:
    """Whether a language is on the allowlist."""
    return language.strip().lower() in {item.lower() for item in allowed}


#: Languages accepted by default. Anything else needs explicit allowance,
#: because an interpreter the platform has not reviewed is an interpreter with
#: unknown capabilities.
DEFAULT_ALLOWED_LANGUAGES: Final[frozenset[str]] = frozenset(
    {"python", "python3", "javascript", "node", "bash", "sh"}
)


def require_allowed_language(
    language: str,
    *,
    allowed: frozenset[str] = DEFAULT_ALLOWED_LANGUAGES,
) -> str:
    """Validates a language name.

    Raises:
        InvalidRequestError: If the language is not allowed.
    """
    normalised = language.strip().lower()
    if not normalised:
        raise InvalidRequestError("a language is required", details={"field": "language"})
    if not is_safe_language(normalised, allowed=allowed):
        raise InvalidRequestError(
            "language is not on the allowlist",
            details={"language": language, "allowed": sorted(allowed)},
        )
    return normalised
