"""Liveness and readiness.

Spec §18 forbids echoing secrets in health endpoints. That is enforced by
:func:`safe_status`, which names dependencies and reports their state without
ever including a value that could be a credential.

Two endpoints, with different meanings:

* **liveness** — the process is running. It must not depend on anything
  external, or a dependency outage would trigger a restart loop that makes the
  outage worse.
* **readiness** — the service can do work. This is where dependencies are
  checked.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from collections.abc import Callable

from mas_orchestrator.config.secrets import safe_environment_summary

#: How long a dependency check may take before the dependency is reported
#: degraded rather than blocking readiness forever.
CHECK_TIMEOUT_SECONDS: Final[float] = 2.0


class HealthState(StrEnum):
    """Whether a component is usable."""

    OK = "ok"
    DEGRADED = "degraded"
    FAILING = "failing"


@dataclass(frozen=True, slots=True)
class DependencyStatus:
    """The state of one dependency.

    Attributes:
        name: Dependency name.
        state: Whether it is usable.
        detail: A short, content-free explanation.
        latency_ms: How long the check took, when measured.
    """

    name: str
    state: HealthState
    detail: str = ""
    latency_ms: int | None = None

    def as_dict(self) -> dict[str, Any]:
        """A JSON-serialisable view."""
        payload: dict[str, Any] = {"name": self.name, "state": self.state.value}
        if self.detail:
            payload["detail"] = self.detail
        if self.latency_ms is not None:
            payload["latency_ms"] = self.latency_ms
        return payload


@dataclass(frozen=True, slots=True)
class HealthReport:
    """A readiness or liveness answer.

    Attributes:
        state: Overall state.
        dependencies: Per-dependency states.
        checked_epoch_ms: When the check ran.
    """

    state: HealthState
    dependencies: tuple[DependencyStatus, ...] = ()
    checked_epoch_ms: int = 0

    @property
    def ready(self) -> bool:
        """Whether the service should receive traffic.

        ``degraded`` still accepts traffic: a partially-impaired dependency is
        better handled by the service's own retries than by removing the
        service entirely.
        """
        return self.state is not HealthState.FAILING

    def as_dict(self) -> dict[str, Any]:
        """A JSON-serialisable view."""
        return {
            "state": self.state.value,
            "ready": self.ready,
            "checked_epoch_ms": self.checked_epoch_ms,
            "dependencies": [dep.as_dict() for dep in self.dependencies],
        }


def overall_state(states: list[HealthState]) -> HealthState:
    """Aggregates dependency states.

    Any failing dependency fails the whole; any degraded one degrades it. A
    failing dependency is not averaged away by healthy ones.
    """
    if not states:
        return HealthState.OK
    if any(state is HealthState.FAILING for state in states):
        return HealthState.FAILING
    if any(state is HealthState.DEGRADED for state in states):
        return HealthState.DEGRADED
    return HealthState.OK


class HealthChecker:
    """Runs registered dependency checks."""

    def __init__(self, *, service_name: str = "mas-orchestrator") -> None:
        self._service_name = service_name
        self._checks: list[tuple[str, Callable[[], HealthState | None]]] = []

    def register(self, name: str, check: Callable[[], HealthState | None]) -> None:
        """Registers a dependency check.

        A check returns a :class:`HealthState`, or ``None`` to mean "no
        opinion". ``None`` is reported as degraded rather than OK: a check that
        cannot tell is not evidence of health.
        """
        self._checks.append((name, check))

    def check(self) -> HealthReport:
        """Runs every check and aggregates.

        A check that raises is reported as failing, not swallowed: an
        exception from a dependency probe is the clearest signal there is.
        """
        import time

        results: list[DependencyStatus] = []
        states: list[HealthState] = []

        for name, check in self._checks:
            started = time.monotonic()
            try:
                result = check()
            except Exception as error:
                results.append(
                    DependencyStatus(
                        name=name,
                        state=HealthState.FAILING,
                        detail=f"check raised {type(error).__name__}",
                    )
                )
                states.append(HealthState.FAILING)
                continue

            latency = int((time.monotonic() - started) * 1000)
            if result is None:
                state = HealthState.DEGRADED
                detail = "check reported no state"
            else:
                state = result
                detail = ""
            results.append(
                DependencyStatus(name=name, state=state, detail=detail, latency_ms=latency)
            )
            states.append(state)

        return HealthReport(
            state=overall_state(states),
            dependencies=tuple(results),
            checked_epoch_ms=int(time.time() * 1000),
        )

    def liveness(self) -> HealthReport:
        """Whether the process is running.

        Deliberately checks nothing: liveness must not be coupled to a
        dependency, or a dependency outage becomes a restart loop.
        """
        import time

        return HealthReport(state=HealthState.OK, checked_epoch_ms=int(time.time() * 1000))


def safe_status(
    *,
    service_name: str,
    environment: str,
    report: HealthReport | None = None,
    version: str = "unknown",
) -> dict[str, Any]:
    """Builds a health payload that cannot leak configuration.

    The environment summary reports names and lengths only, and anything whose
    name looks sensitive is redacted outright. Spec §18 forbids echoing
    secrets; this is the endpoint where that rule is easiest to break.

    Args:
        service_name: Service name.
        environment: Deployment environment.
        report: Health report, when available.
        version: Build version.

    Returns:
        A JSON-serialisable status payload.
    """
    payload: dict[str, Any] = {
        "service": service_name,
        "environment": environment,
        "version": version,
        "status": "ok",
    }
    if report is not None:
        payload["health"] = report.as_dict()
        payload["status"] = "ready" if report.ready else "not_ready"

    # Names and lengths only. Never values.
    payload["environment_variables"] = safe_environment_summary()
    return payload


_DEFAULT: Final[HealthChecker] = HealthChecker()


def default_checker() -> HealthChecker:
    """The process-wide checker, for wiring in one place."""
    return _DEFAULT


def reset_default_checker() -> None:
    """Clears registered checks. Intended for tests."""
    _DEFAULT._checks.clear()
