"""SMTP email backend with STARTTLS and rate limiting.

Sends email via an SMTP server using STARTTLS for encryption.
Enforces host allowlist to prevent SSRF and rate limits to prevent abuse.
All credentials must be resolved from secrets (env:NAME or file:PATH) — no plaintext keys.
"""

from __future__ import annotations

import asyncio
import smtplib
import time
from email.message import EmailMessage
from typing import TYPE_CHECKING

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import resolve_secret

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: Default rate limit: max 10 emails per 60-second window per tenant
DEFAULT_RATE_LIMIT = 10
#: Default cooldown between emails in seconds
DEFAULT_COOLDOWN = 6.0


class SmtpEmailBackend:
    """SMTP email backend with STARTTLS and rate limiting.

    Requires the following configuration keys:
    - smtp_host: SMTP server hostname (allowlisted)
    - smtp_port: SMTP server port (typically 587 for STARTTLS)
    - smtp_user: SMTP username (secret reference)
    - smtp_password: SMTP password (secret reference)
    - from_email: Default sender email address

    Optional configuration keys:
    - rate_limit: max emails per 60-second window
    - cooldown: seconds between individual emails
    - timeout: connection timeout in seconds
    - verify_certs: whether to verify TLS certificates
    """

    def __init__(self, *, config: dict, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._host = config.get("smtp_host")
        self._port = int(config.get("smtp_port", 587))
        self._user = resolve_secret(config.get("smtp_user", ""))
        self._password = resolve_secret(config.get("smtp_password", ""))
        self._from_email = config.get("from_email", "")
        self._timeout = float(config.get("timeout", 30.0))
        self._verify_certs = config.get("verify_certs", True)

        # Rate limiting
        self._rate_limit = int(config.get("rate_limit", DEFAULT_RATE_LIMIT))
        self._cooldown = float(config.get("cooldown", DEFAULT_COOLDOWN))

        # Per-tenant rate limit tracking
        self._last_email: dict[str, float] = {}

        # Validate configuration
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate the backend configuration.

        Raises:
            ConfigurationError: If required fields are missing or invalid.
        """
        if not self._host:
            raise ConfigurationError(
                "smtp_host is required for the SMTP backend",
                details={"field": "smtp_host"},
            )

        # Host allowlist: only permit approved providers
        allowed_hosts = [
            "smtp.gmail.com",
            "smtp.sendgrid.net",
            "smtp.mailgun.org",
            "smtp.zoho.com",
        ]
        if self._host not in allowed_hosts:
            raise ConfigurationError(
                f"SMTP host '{self._host}' is not in the allowed host list",
                details={"host": self._host, "allowed": allowed_hosts},
            )

        if not (1 <= self._port <= 65535):
            raise ConfigurationError(
                f"SMTP port {self._port} is not in valid range 1-65535",
                details={"port": self._port},
            )

        if not self._user:
            raise ConfigurationError(
                "smtp_user (secret reference) is required",
                details={"field": "smtp_user"},
            )

        if not self._password:
            raise ConfigurationError(
                "smtp_password (secret reference) is required",
                details={"field": "smtp_password"},
            )

    async def send(self, *, to: str, subject: str, body: str, html: str | None = None) -> bool:
        """Send an email message.

        Args:
            to: Recipient email address.
            subject: Email subject line.
            body: Plain-text body content.
            html: Optional HTML body content.

        Returns:
            True if the email was sent successfully.

        Raises:
            ConfigurationError: If the backend is not properly configured.
            RuntimeError: If the send fails.
        """
        # Rate limiting check
        tenant_id = getattr(self, "_tenant_id", "unknown")
        now = time.time()

        if tenant_id in self._last_email:
            elapsed = now - self._last_email[tenant_id]
            if elapsed < self._cooldown:
                wait_time = self._cooldown - elapsed
                _LOGGER.info(
                    "rate limiting email send, waiting %.1fs", wait_time
                )
                await asyncio.sleep(wait_time)

        # Enforce rate limit window
        window_start = now - 60
        emails_in_window = sum(
            1 for t in self._last_email if t > window_start
        )

        if emails_in_window >= self._rate_limit:
            raise ConfigurationError(
                f"rate limit exceeded: {self._rate_limit} emails per 60s"
                , details={"rate_limit": self._rate_limit},
            )

        # Build the email message
        msg = EmailMessage()
        msg["From"] = self._from_email or "noreply@enterprise-mas.local"
        msg["To"] = to
        msg["Subject"] = subject
        msg.set_content(body)

        if html:
            msg.add_alternative(html, subtype="html")

        # Send via SMTP with STARTTLS
        try:
            async with asyncio.timeout(self._timeout):
                await self._send_smtp(msg)
            
            # Track the send time
            self._last_email[tenant_id] = time.time()

            _LOGGER.info(
                "email sent successfully to %s", to
            )
            return True

        except Exception as e:
            _LOGGER.error("failed to send email to %s: %s", to, e)
            raise RuntimeError(f"failed to send email to {to}: {e}")

    async def _send_smtp(self, msg: EmailMessage) -> None:
        """Send email via SMTP with STARTTLS."""
        inbox = smtplib.SMTP(self._host, self._port, timeout=self._timeout)
        try:
            # Start TLS
            inbox.starttls()
            # Login
            inbox.login(self._user, self._password)
            # Send message
            inbox.send_message(msg)
        finally:
            inbox.quit()


def create_backend(
    *,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> SmtpEmailBackend:
    """Convenient function to create an SmtpEmailBackend instance."""
    return SmtpEmailBackend(config=config, environment=environment)
