"""SmtpEmailBackend and SendGridEmailBackend: STARTTLS, allowlist, rate, size."""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from mas_orchestrator.contracts.errors import (
    DependencyUnavailableError,
    InvalidRequestError,
    ToolFailedError,
)
from mas_orchestrator.providers.base_http import TransportResponse
from mas_orchestrator.security.credentials import Secret
from mas_orchestrator.tools.backends.email import (
    MAX_EMAIL_BODY_BYTES,
    SendGridEmailBackend,
    SmtpEmailBackend,
)


class _RecordingSmtp:
    instances: ClassVar[list[_RecordingSmtp]] = []

    def __init__(self, host: str, port: int, timeout: float | None = None) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.steps: list[str] = []
        self.__class__.instances.append(self)

    def __enter__(self) -> _RecordingSmtp:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def ehlo(self) -> None:
        self.steps.append("ehlo")

    def starttls(self) -> None:
        self.steps.append("starttls")

    def login(self, user: str, password: str) -> None:
        self.steps.append("login")

    def send_message(
        self, message: Any, from_addr: str | None = None, to_addrs: Any = None
    ) -> dict[str, Any]:
        self.steps.append("send")
        return {}


class _RecordingTransport:
    def __init__(
        self, status_code: int = 202, body: bytes = b"", headers: dict[str, str] | None = None
    ) -> None:
        self.calls: list[dict[str, Any]] = []
        self._status = status_code
        self._body = body
        self._headers = headers or {"x-message-id": "sg-1"}

    async def request(self, method: str, url: str, **kwargs: Any) -> TransportResponse:
        self.calls.append({"method": method, "url": url, **kwargs})
        return TransportResponse(status_code=self._status, body=self._body, headers=self._headers)


def test_starttls_is_mandatory() -> None:
    with pytest.raises(InvalidRequestError, match="STARTTLS"):
        SmtpEmailBackend(
            host="relay.example.com",
            port=587,
            from_address="ops@example.com",
            use_tls=False,
        )


def test_loopback_host_is_refused_without_operator_flag() -> None:
    with pytest.raises(InvalidRequestError, match="loopback"):
        SmtpEmailBackend(host="127.0.0.1", port=587, from_address="ops@example.com")
    with pytest.raises(InvalidRequestError, match="loopback"):
        SmtpEmailBackend(host="localhost", port=587, from_address="ops@example.com")


def test_operator_may_allow_private_relay() -> None:
    backend = SmtpEmailBackend(
        host="10.0.0.8",
        port=587,
        from_address="ops@example.com",
        allow_private_hosts=True,
        smtp_factory=_RecordingSmtp,
    )
    assert backend._host == "10.0.0.8"


async def test_smtp_starttls_runs_before_auth() -> None:
    _RecordingSmtp.instances.clear()
    backend = SmtpEmailBackend(
        host="relay.example.com",
        port=587,
        from_address="ops@example.com",
        username="bot",
        password=Secret("s3cret", reference="test://email"),
        smtp_factory=_RecordingSmtp,
    )
    await backend.send(
        to=["u@example.com"], subject="s", body="b", reply_to=None, timeout_seconds=2.0
    )
    assert _RecordingSmtp.instances[-1].steps == ["ehlo", "starttls", "ehlo", "login", "send"]


async def test_smtp_rate_limit_refuses_a_burst() -> None:
    backend = SmtpEmailBackend(
        host="relay.example.com",
        port=587,
        from_address="ops@example.com",
        smtp_factory=_RecordingSmtp,
        min_interval_seconds=60.0,
    )
    await backend.send(
        to=["u@example.com"], subject="s", body="b", reply_to=None, timeout_seconds=2.0
    )
    with pytest.raises(ToolFailedError, match="rate limit"):
        await backend.send(
            to=["u@example.com"], subject="s", body="b", reply_to=None, timeout_seconds=2.0
        )


async def test_oversize_body_is_refused() -> None:
    backend = SmtpEmailBackend(
        host="relay.example.com",
        port=587,
        from_address="ops@example.com",
        smtp_factory=_RecordingSmtp,
    )
    with pytest.raises(InvalidRequestError, match="size cap"):
        await backend.send(
            to=["u@example.com"],
            subject="s",
            body="x" * (MAX_EMAIL_BODY_BYTES + 1),
            reply_to=None,
            timeout_seconds=2.0,
        )


async def test_sendgrid_posts_through_the_guarded_transport() -> None:
    transport = _RecordingTransport()
    backend = SendGridEmailBackend(
        transport=transport,
        api_key=Secret("sg", reference="test://email"),
        from_address="ops@example.com",
    )
    message_id = await backend.send(
        to=["u@example.com"], subject="hello", body="world", reply_to=None, timeout_seconds=2.0
    )
    assert message_id == "sg-1"
    assert transport.calls[0]["url"] == "https://api.sendgrid.com/v3/mail/send"
    assert transport.calls[0]["headers"]["authorization"] == "Bearer sg"


async def test_sendgrid_timeout_must_be_positive() -> None:
    backend = SendGridEmailBackend(
        transport=_RecordingTransport(),
        api_key=Secret("sg", reference="test://email"),
        from_address="ops@example.com",
    )
    with pytest.raises(InvalidRequestError, match="timeout"):
        await backend.send(
            to=["u@example.com"], subject="s", body="b", reply_to=None, timeout_seconds=0
        )


async def test_sendgrid_unavailable_is_typed() -> None:
    transport = _RecordingTransport(status_code=503)
    backend = SendGridEmailBackend(
        transport=transport,
        api_key=Secret("sg", reference="test://email"),
        from_address="ops@example.com",
    )
    with pytest.raises(DependencyUnavailableError):
        await backend.send(
            to=["u@example.com"], subject="s", body="b", reply_to=None, timeout_seconds=2.0
        )
