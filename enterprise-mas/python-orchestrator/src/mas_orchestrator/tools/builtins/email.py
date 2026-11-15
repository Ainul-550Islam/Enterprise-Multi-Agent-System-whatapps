"""Email tool builtin - factory hook.

Provides the email sending capability through the configured backend.
This is a factory hook that routes to the appropriate backend
based on the configuration.
"""

from __future__ import annotations

from mas_orchestrator.tools.backends import build_backend
from mas_orchestrator.config.enums import Environment


def send_email(
    *,
    backend_kind: str,
    config: dict,
    environment: Environment,
    to: str,
    subject: str,
    body: str,
    html: str | None = None,
) -> bool:
    """Send an email using the configured backend.

    Args:
        backend_kind: The backend kind string (e.g. "smtp", "sendgrid").
        config: Backend configuration dictionary.
        environment: Current deployment environment.
        to: Recipient email address.
        subject: Email subject line.
        body: Plain-text body content.
        html: Optional HTML body content.

    Returns:
        True if the email was sent successfully.

    Raises:
        ConfigurationError: If the backend is not properly configured.
    """
    backend = build_backend(
        kind=backend_kind,
        config=config,
        environment=environment,
    )
    return asyncio.run(backend.send(
        to=to,
        subject=subject,
        body=body,
        html=html,
    ))
