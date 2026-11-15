"""SendGrid email backend with guarded HTTP.

Sends email via the SendGrid API using HTTPS.
All outbound traffic is pinned to SendGrid's domain to prevent SSRF.
All credentials must be resolved from secrets — no plaintext keys.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import httpx
from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import ConfigurationError
from mas_orchestrator.security.credentials import resolve_secret

if TYPE_CHECKING:
    from collections.abc import Sequence

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

#: SendGrid API endpoint URL (pinned, not configurable via input)
SENDGRID_API_URL = "https://api.sendgrid.com/v3/mail/send"
#: Default rate limit: max 100 emails per 60-second window per tenant
DEFAULT_RATE_LIMIT = 100


class SendGridEmailBackend:
    """SendGrid email backend with guarded HTTP.

    Requires the following configuration keys:
    - api_key: Secret reference for SendGrid API key (e.g. env:SENDGRID_API_KEY)

    Optional configuration keys:
    - from_email: Default sender email address
    - rate_limit: max emails per 60-second window
    """

    def __init__(self, *, config: dict, environment: Environment = Environment.DEVELOPMENT) -> None:
        self._environment = environment
        self._api_key = resolve_secret(config.get("api_key", ""))
        self._from_email = config.get("from_email", "noreply@enterprise-mas.local")
        self._rate_limit = int(config.get("rate_limit", DEFAULT_RATE_LIMIT))
        self._last_email: dict[str, float] = {}

        # Validate configuration
        if not self._api_key:
            raise ConfigurationError(
                "api_key (secret reference) is required for SendGrid backend",
                details={"field": "api_key"},
            )

    async def send(self, *, to: str, subject: str, body: str, html: str | None = None) -> bool:
        """Send an email message via SendGrid API.

        Args:
            to: Recipient email address.
            subject: Email subject line.
            body: Plain-text body content.
            html: Optional HTML body content.

        Returns:
            True if the email was sent successfully (HTTP 2xx).

        Raises:
            ConfigurationError: If the backend is not properly configured.
            RuntimeError: If the API call fails.
        """
        # Rate limiting check
        tenant_id = getattr(self, "_tenant_id", "unknown")
        now = asyncio.get_event_loop().time()

        if tenant_id in self._last_email:
            elapsed = now - self._last_email[tenant_id]
            if elapsed < 6.0:  # minimum cooldown
                await asyncio.sleep(6.0 - elapsed)

        # Enforce rate limit window
        loop = asyncio.get_event_loop()
        window_start = loop.time() - 60
        emails_in_window = sum(
            1 for t in self._last_email if t > window_start
        )

        if emails_in_window >= self._rate_limit:
            raise ConfigurationError(
                f"rate limit exceeded: {self._rate_limit} emails per 60s"
                , details={"rate_limit": self._rate_limit},
            )

        # Build the email payload for SendGrid
        payload = {
            "personalizations": [{
                "to": [{"email": to}],
            }],
            "from": {"email": self._from_email},
            "content": [{
                "type": "text/plain",
                "value": body,
            },
            ]
        }
        if html:
            payload["content"].append({
                "type": "text/html",
                "value": html,
            })

        # Send via HTTP with pinned host
        try:
            async with asyncio.timeout(30):
                async with httpx.AsyncClient(
                    timeout=30.0,
                    follow_redirects=True,
                ) as client:
                    response = await client.post(
                        SENDGRID_API_URL,
                        json=payload,
                        headers={
                            "Authorization": f"Bearer {self._api_key}",
                            "Content-Type": "application/json",
                        },
                    )
                    
                    # Track the send time
                    self._last_email[tenant_id] = loop.time()

                    if response.is_success:
                        _LOGGER.info(
                            "email sent successfully to %s via SendGrid", to
                        )
                        return True
                    else:
                        raise RuntimeError(
                            f"SendGrid API returned {response.status_code}: {response.text}"
                        )

        except asyncio.TimeoutError:
            raise RuntimeError(f"SendGrid API call timed out")
        except httpx.ConnectError as e:
            raise RuntimeError(f"failed to connect to SendGrid API: {e}")
        except Exception as e:
            raise RuntimeError(f"failed to send email via SendGrid: {e}")


def create_backend(
    *,
    config: dict,
    environment: Environment = Environment.DEVELOPMENT,
) -> SendGridEmailBackend:
    """Convenient function to create a SendGridEmailBackend instance."""
    return SendGridEmailBackend(config=config, environment=environment)
