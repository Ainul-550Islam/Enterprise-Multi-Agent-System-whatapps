"""Health endpoints.

Two endpoints with genuinely different meanings, which is the whole point:

* **liveness** — the process is running. It checks nothing, by design. If
  liveness depended on a dependency, a dependency outage would restart the
  service repeatedly and make the outage worse.
* **readiness** — the service can do work. Dependencies are checked here, and
  the result decides whether the service receives traffic.

Spec §18 forbids echoing secrets in health endpoints, so both go through
:func:`~mas_orchestrator.observability.health.safe_status`, which reports
environment configuration by name and length only and redacts anything whose
name looks sensitive. A health endpoint is usually the least protected route in
a service, which is exactly why it must not carry configuration.
"""

from __future__ import annotations

import logging
from typing import Any, Final

from mas_orchestrator.observability.health import (
    HealthChecker,
    HealthState,
    safe_status,
)

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Routes that are always served, even when the service is draining.
ALWAYS_AVAILABLE: Final[frozenset[str]] = frozenset({"/healthz", "/readyz"})


def liveness(
    checker: HealthChecker,
    *,
    service_name: str,
    environment: str,
    version: str = "unknown",
) -> dict[str, Any]:
    """Answers a liveness probe.

    Checks nothing. Returns ``ok`` whenever the process can serve this
    response, which is the definition of alive.
    """
    report = checker.liveness()
    return {
        "status": "ok",
        "service": service_name,
        "environment": environment,
        "version": version,
        "state": report.state.value,
        "checked_epoch_ms": report.checked_epoch_ms,
    }


def readiness(
    checker: HealthChecker,
    *,
    service_name: str,
    environment: str,
    version: str = "unknown",
) -> dict[str, Any]:
    """Answers a readiness probe.

    Runs every registered dependency check and reports the aggregate. A
    dependency that raises is reported as failing rather than swallowed: an
    exception from a probe is the clearest signal there is.
    """
    report = checker.check()
    payload = safe_status(
        service_name=service_name,
        environment=environment,
        report=report,
        version=version,
    )
    if not report.ready:
        _LOGGER.warning(
            "readiness check failed",
            extra={
                "event": "api.not_ready",
                "state": report.state.value,
                "dependencies": [dep.name for dep in report.dependencies],
            },
        )
    return payload


def startup(checker: HealthChecker) -> HealthState:
    """Runs a startup probe.

    Checked once at boot, before the service accepts work. A service that
    starts while a mandatory dependency is unavailable spends its first minutes
    failing requests it could have refused at startup.
    """
    report = checker.check()
    return report.state


def is_always_available(path: str) -> bool:
    """Whether a path is served even while draining."""
    return path in ALWAYS_AVAILABLE
