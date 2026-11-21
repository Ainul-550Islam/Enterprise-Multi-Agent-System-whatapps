"""Email delivery backends: SMTP and SendGrid.

Both implement
:class:`~mas_orchestrator.tools.builtins.email.EmailBackend`. Neither retries:
the executor owns delivery accounting, and a hidden retry is how one message
becomes two.

Failure taxonomy (shared by both adapters):

* bad input -- :class:`~mas_orchestrator.contracts.errors.InvalidRequestError`;
* the provider refused the message (HTTP 4xx / SMTP 5xx) --
  :class:`~mas_orchestrator.contracts.errors.ToolFailedError`;
* the provider could not be reached --
  :class:`~mas_orchestrator.contracts.errors.DependencyUnavailableError`.
"""

from __future__ import annotations

import asyncio
import ipaddress
import smtplib
import time
from email.message import EmailMessage
from email.utils import make_msgid
from typing import Any

from mas_orchestrator.contracts.errors import (
    DependencyUnavailableError,
    InvalidRequestError,
    ToolFailedError,
)
from mas_orchestrator.security.credentials import Secret

#: SendGrid (and SMTP body) hard cap so a tool cannot ship an unbounded payload.
MAX_EMAIL_BODY_BYTES: int = 256 * 1024


def _require_positive_timeout(timeout_seconds: float) -> None:
    if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
        raise InvalidRequestError(
            "delivery timeout must be positive",
            details={"field": "timeout_seconds"},
        )


def _require_body_size(body: str) -> None:
    if len(body.encode("utf-8")) > MAX_EMAIL_BODY_BYTES:
        raise InvalidRequestError(
            "email body exceeds the delivery size cap",
            details={"field": "body", "max_bytes": MAX_EMAIL_BODY_BYTES},
        )


def _refuse_private_host(host: str, *, allow_private: bool) -> None:
    """Operator hosts may be private only when explicitly allowlisted."""
    cleaned = host.strip().lower().rstrip(".")
    if cleaned in {"localhost", "localhost.localdomain"} and not allow_private:
        raise InvalidRequestError(
            "smtp host is loopback; set allow_private_hosts for operator relays",
            details={"field": "host"},
        )
    try:
        address = ipaddress.ip_address(cleaned)
    except ValueError:
        return
    from mas_orchestrator.providers.base_http import is_blocked_address

    if is_blocked_address(str(address)) and not allow_private:
        raise InvalidRequestError(
            "smtp host is a private or loopback address; set allow_private_hosts",
            details={"field": "host"},
        )


class SmtpEmailBackend:
    """Delivers mail through an SMTP relay.

    One call opens one authenticated session and sends exactly one message.
    The synchronous SMTP conversation runs on a worker thread so the event
    loop never blocks on the network.

    Args:
        host: Relay hostname.
        port: Relay port (587 for STARTTLS, 465 for implicit TLS).
        from_address: Envelope and header sender.
        username: Relay login, when the relay demands one.
        password: Relay password; required when ``username`` is set and never
            stored outside :class:`~mas_orchestrator.security.credentials.Secret`.
        use_tls: Whether to upgrade with STARTTLS before authenticating.
        smtp_factory: Injection point for tests; defaults to
            :class:`smtplib.SMTP`.

    Raises:
        InvalidRequestError: If the configuration is incomplete or malformed.
    """

    def __init__(
        self,
        *,
        host: str,
        port: int,
        from_address: str,
        username: str | None = None,
        password: Secret | None = None,
        use_tls: bool = True,
        smtp_factory: Any = None,
        allow_private_hosts: bool = False,
        min_interval_seconds: float = 0.0,
    ) -> None:
        if not host.strip():
            raise InvalidRequestError("smtp host must not be empty", details={"field": "host"})
        if not 0 < int(port) < 65536:
            raise InvalidRequestError("smtp port is out of range", details={"field": "port"})
        if "@" not in from_address:
            raise InvalidRequestError(
                "smtp from_address must be a valid address", details={"field": "from_address"}
            )
        if (username is None) != (password is None):
            raise InvalidRequestError(
                "smtp username and password must be supplied together",
                details={"field": "password"},
            )
        if not use_tls:
            raise InvalidRequestError(
                "smtp STARTTLS is mandatory",
                details={"field": "use_tls"},
            )
        if min_interval_seconds < 0:
            raise InvalidRequestError(
                "smtp min_interval_seconds must not be negative",
                details={"field": "min_interval_seconds"},
            )
        _refuse_private_host(host, allow_private=allow_private_hosts)
        self._host = host.strip()
        self._port = int(port)
        self._from_address = from_address.strip()
        self._username = username
        self._password = password
        self._use_tls = True
        self._smtp_factory = smtp_factory or smtplib.SMTP
        self._min_interval_seconds = float(min_interval_seconds)
        self._last_send_monotonic: float | None = None

    async def send(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        reply_to: str | None,
        timeout_seconds: float,
    ) -> str:
        """Sends one message and returns its ``Message-ID``."""
        _require_positive_timeout(timeout_seconds)
        _require_body_size(body)
        if not to:
            raise InvalidRequestError("smtp send requires a recipient", details={"field": "to"})
        if self._min_interval_seconds > 0 and self._last_send_monotonic is not None:
            elapsed = time.monotonic() - self._last_send_monotonic
            if elapsed < self._min_interval_seconds:
                raise ToolFailedError(
                    "smtp rate limit exceeded",
                    details={"tool_id": "email", "backend": "smtp"},
                )

        message = EmailMessage()
        message["From"] = self._from_address
        message["To"] = ", ".join(to)
        message["Subject"] = subject
        message_id = make_msgid(domain=self._from_address.rsplit("@", 1)[-1])
        message["Message-ID"] = message_id
        if reply_to is not None:
            message["Reply-To"] = reply_to
        message.set_content(body)

        await asyncio.to_thread(self._deliver, message, to, float(timeout_seconds))
        self._last_send_monotonic = time.monotonic()
        return message_id

    def _deliver(self, message: EmailMessage, to: list[str], timeout_seconds: float) -> None:
        """Runs one SMTP session. Any relay failure is a typed error."""
        try:
            session = self._smtp_factory(self._host, self._port, timeout=timeout_seconds)
            with session:
                session.ehlo()
                if self._use_tls:
                    session.starttls()
                    session.ehlo()
                if self._username is not None and self._password is not None:
                    session.login(self._username, self._password.reveal())
                session.send_message(message, from_addr=self._from_address, to_addrs=to)
        except smtplib.SMTPRecipientsRefused as error:
            raise ToolFailedError(
                "smtp relay refused every recipient",
                details={"tool_id": "email", "backend": "smtp"},
            ) from error
        except smtplib.SMTPResponseException as error:
            # A 5xx refusal is a provider verdict; anything else is reachability.
            if 500 <= error.smtp_code < 600:
                raise ToolFailedError(
                    "smtp relay refused the message",
                    details={"tool_id": "email", "backend": "smtp", "smtp_code": error.smtp_code},
                ) from error
            raise DependencyUnavailableError(
                "smtp relay conversation failed",
                details={"tool_id": "email", "backend": "smtp", "smtp_code": error.smtp_code},
            ) from error
        except (smtplib.SMTPException, OSError) as error:
            raise DependencyUnavailableError(
                "smtp relay could not be reached",
                details={"tool_id": "email", "backend": "smtp"},
            ) from error


class SendGridEmailBackend:
    """Delivers mail through the SendGrid v3 API over the guarded transport.

    Args:
        transport: The platform guarded HTTP transport.
        api_key: The SendGrid API key, never stored outside a
            :class:`~mas_orchestrator.security.credentials.Secret`.
        from_address: Verified sender address.

    Raises:
        InvalidRequestError: If the configuration is incomplete or malformed.
    """

    _ENDPOINT = "https://api.sendgrid.com/v3/mail/send"

    def __init__(
        self,
        *,
        transport: Any,
        api_key: Secret,
        from_address: str,
    ) -> None:
        if transport is None or not hasattr(transport, "request"):
            raise InvalidRequestError(
                "sendgrid backend requires the guarded transport",
                details={"field": "transport"},
            )
        if not isinstance(api_key, Secret):
            raise InvalidRequestError(
                "sendgrid api_key must be a Secret", details={"field": "api_key"}
            )
        if "@" not in from_address:
            raise InvalidRequestError(
                "sendgrid from_address must be a valid address", details={"field": "from_address"}
            )
        self._transport = transport
        self._api_key = api_key
        self._from_address = from_address.strip()

    async def send(
        self,
        *,
        to: list[str],
        subject: str,
        body: str,
        reply_to: str | None,
        timeout_seconds: float,
    ) -> str:
        """Sends one message and returns the provider message id."""
        _require_positive_timeout(timeout_seconds)
        _require_body_size(body)
        if not to:
            raise InvalidRequestError("sendgrid send requires a recipient", details={"field": "to"})

        payload: dict[str, Any] = {
            "personalizations": [{"to": [{"email": address} for address in to]}],
            "from": {"email": self._from_address},
            "subject": subject,
            "content": [{"type": "text/plain", "value": body}],
        }
        if reply_to is not None:
            payload["reply_to"] = {"email": reply_to}

        try:
            response = await self._transport.request(
                "POST",
                self._ENDPOINT,
                json_body=payload,
                headers={
                    "authorization": f"Bearer {self._api_key.reveal()}",
                    "content-type": "application/json",
                },
            )
        except (InvalidRequestError, ToolFailedError, DependencyUnavailableError):
            raise
        except Exception as error:
            raise DependencyUnavailableError(
                "sendgrid could not be reached",
                details={"tool_id": "email", "backend": "sendgrid"},
            ) from error

        if response.status_code in (200, 202):
            provider_id = dict(response.headers).get("x-message-id")
            return provider_id or make_msgid(domain=self._from_address.rsplit("@", 1)[-1])
        if 500 <= response.status_code < 600:
            raise DependencyUnavailableError(
                "sendgrid is unavailable",
                details={"tool_id": "email", "backend": "sendgrid"},
            )
        raise ToolFailedError(
            "sendgrid refused the message",
            details={
                "tool_id": "email",
                "backend": "sendgrid",
                "status_code": response.status_code,
            },
        )
