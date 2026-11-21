"""Tests for the production tool backends.

Parity with ``tests/unit/test_builtins.py``: every adapter is asked the same
question the built-in tool tests ask -- can a provider response, a malformed
configuration, or a failing dependency make this backend do something it was
not configured to do? Each test drives a recording fake (transport, SMTP
session, pool), never a network.
"""

from __future__ import annotations

import smtplib
from typing import Any, ClassVar

import pytest

from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    DependencyUnavailableError,
    InvalidRequestError,
    ToolFailedError,
)
from mas_orchestrator.providers.base_http import TransportResponse
from mas_orchestrator.security.credentials import Secret
from mas_orchestrator.tools.backends.database import PostgresReadOnlyBackend
from mas_orchestrator.tools.backends.email import SendGridEmailBackend, SmtpEmailBackend
from mas_orchestrator.tools.backends.sandbox import RemoteSandboxBackend
from mas_orchestrator.tools.backends.search import (
    BraveSearchBackend,
    SearxngSearchBackend,
    TavilySearchBackend,
)
from mas_orchestrator.tools.builtins.code import SandboxLimits
from mas_orchestrator.tools.builtins.email import EmailTool, EmailToolConfig
from mas_orchestrator.tools.builtins.search import SearchTool, SearchToolConfig
from mas_orchestrator.tools.types import ToolInvocationContext


class _RecordingTransport:
    """A guarded transport stand-in that records every request."""

    def __init__(
        self, status_code: int = 200, body: bytes = b"{}", headers: dict[str, str] | None = None
    ) -> None:
        self._responses = [(status_code, body, headers or {})]
        self.calls: list[dict[str, Any]] = []

    def queue(
        self, status_code: int, body: bytes = b"{}", headers: dict[str, str] | None = None
    ) -> None:
        self._responses.append((status_code, body, headers or {}))

    async def request(
        self,
        method: str,
        url: str,
        *,
        json_body: object | None = None,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
        max_response_bytes: int | None = None,
    ) -> TransportResponse:
        self.calls.append(
            {"method": method, "url": url, "json_body": json_body, "headers": dict(headers or {})}
        )
        status_code, body, response_headers = self._responses.pop(0)
        return TransportResponse(status_code=status_code, body=body, headers=response_headers)


class _RecordingSmtp:
    """An smtplib stand-in that records the whole conversation."""

    instances: ClassVar[list[_RecordingSmtp]] = []

    def __init__(self, host: str, port: int, timeout: float | None = None) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.steps: list[str] = []
        self.sent: list[tuple[Any, Any, Any]] = []
        self._fail_send: type[Exception] | None = None
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
        self.sent.append((message, from_addr, to_addrs))
        return {}


class _FailingSmtp(_RecordingSmtp):
    def send_message(
        self, message: Any, from_addr: str | None = None, to_addrs: Any = None
    ) -> dict[str, Any]:
        raise smtplib.SMTPRecipientsRefused({"a@example.com": (550, b"no such user")})


class _RejectingSmtp(_RecordingSmtp):
    def send_message(
        self, message: Any, from_addr: str | None = None, to_addrs: Any = None
    ) -> dict[str, Any]:
        raise smtplib.SMTPResponseException(550, b"mailbox unavailable")


class _UnreachableSmtp(_RecordingSmtp):
    def __init__(self, host: str, port: int, timeout: float | None = None) -> None:
        raise OSError("connection refused")


def _context() -> ToolInvocationContext:
    return ToolInvocationContext(
        tenant_id="tenant-alpha",
        execution_id="exec-1",
        agent_id="worker",
        invocation_id="inv-1",
    )


class TestSmtpEmailBackend:
    def test_config_requires_host_port_and_sender(self) -> None:
        with pytest.raises(InvalidRequestError):
            SmtpEmailBackend(host="", port=587, from_address="ops@example.com")
        with pytest.raises(InvalidRequestError):
            SmtpEmailBackend(host="relay.example.com", port=0, from_address="ops@example.com")
        with pytest.raises(InvalidRequestError):
            SmtpEmailBackend(host="relay.example.com", port=587, from_address="not-an-address")

    def test_username_requires_password(self) -> None:
        with pytest.raises(InvalidRequestError):
            SmtpEmailBackend(
                host="relay.example.com", port=587, from_address="ops@example.com", username="u"
            )

    async def test_send_builds_one_message_and_returns_its_id(self) -> None:
        _RecordingSmtp.instances.clear()
        backend = SmtpEmailBackend(
            host="relay.example.com",
            port=587,
            from_address="ops@example.com",
            username="bot",
            password=Secret("s3cret", reference="test://backends"),
            smtp_factory=_RecordingSmtp,
        )
        message_id = await backend.send(
            to=["u@example.com"],
            subject="hello",
            body="world",
            reply_to="reply@example.com",
            timeout_seconds=5.0,
        )
        assert message_id.startswith("<") and message_id.endswith(">")
        session = _RecordingSmtp.instances[-1]
        assert session.steps == ["ehlo", "starttls", "ehlo", "login", "send"]
        assert session.timeout == 5.0
        message, from_addr, to_addrs = session.sent[0]
        assert from_addr == "ops@example.com"
        assert to_addrs == ["u@example.com"]
        assert message["Subject"] == "hello"
        assert message["Reply-To"] == "reply@example.com"

    async def test_recipient_refusal_is_a_typed_failure(self) -> None:
        backend = SmtpEmailBackend(
            host="relay.example.com",
            port=587,
            from_address="ops@example.com",
            smtp_factory=_FailingSmtp,
        )
        with pytest.raises(ToolFailedError):
            await backend.send(
                to=["u@example.com"],
                subject="s",
                body="b",
                reply_to=None,
                timeout_seconds=5.0,
            )

    async def test_relay_5xx_is_a_typed_failure(self) -> None:
        backend = SmtpEmailBackend(
            host="relay.example.com",
            port=587,
            from_address="ops@example.com",
            smtp_factory=_RejectingSmtp,
        )
        with pytest.raises(ToolFailedError):
            await backend.send(
                to=["u@example.com"],
                subject="s",
                body="b",
                reply_to=None,
                timeout_seconds=5.0,
            )

    async def test_unreachable_relay_is_a_dependency_error(self) -> None:
        backend = SmtpEmailBackend(
            host="relay.example.com",
            port=587,
            from_address="ops@example.com",
            smtp_factory=_UnreachableSmtp,
        )
        with pytest.raises(DependencyUnavailableError):
            await backend.send(
                to=["u@example.com"],
                subject="s",
                body="b",
                reply_to=None,
                timeout_seconds=5.0,
            )

    async def test_empty_recipients_are_refused(self) -> None:
        backend = SmtpEmailBackend(
            host="relay.example.com", port=587, from_address="ops@example.com"
        )
        with pytest.raises(InvalidRequestError):
            await backend.send(to=[], subject="s", body="b", reply_to=None, timeout_seconds=5.0)


class TestSendGridEmailBackend:
    def test_config_rejects_a_plain_string_key(self) -> None:
        with pytest.raises(InvalidRequestError):
            SendGridEmailBackend(
                transport=_RecordingTransport(),
                api_key="not-a-secret",  # type: ignore[arg-type]
                from_address="ops@example.com",
            )

    async def test_send_posts_the_payload_and_returns_the_provider_id(self) -> None:
        transport = _RecordingTransport(202, b"", {"x-message-id": "sg-42"})
        backend = SendGridEmailBackend(
            transport=transport,
            api_key=Secret("key-1", reference="test://backends"),
            from_address="ops@example.com",
        )
        message_id = await backend.send(
            to=["u@example.com"],
            subject="hello",
            body="world",
            reply_to=None,
            timeout_seconds=5.0,
        )
        assert message_id == "sg-42"
        call = transport.calls[0]
        assert call["method"] == "POST"
        assert call["url"].startswith("https://api.sendgrid.com/")
        assert call["headers"]["authorization"] == "Bearer key-1"
        payload = call["json_body"]
        assert payload["personalizations"] == [{"to": [{"email": "u@example.com"}]}]
        assert payload["from"] == {"email": "ops@example.com"}
        assert payload["content"] == [{"type": "text/plain", "value": "world"}]
        assert "reply_to" not in payload

    async def test_a_refusal_never_retries(self) -> None:
        transport = _RecordingTransport(401, b'{"errors":[]}')
        backend = SendGridEmailBackend(
            transport=transport,
            api_key=Secret("key-1", reference="test://backends"),
            from_address="ops@example.com",
        )
        with pytest.raises(ToolFailedError):
            await backend.send(
                to=["u@example.com"], subject="s", body="b", reply_to=None, timeout_seconds=5.0
            )
        assert len(transport.calls) == 1

    async def test_5xx_is_a_dependency_error(self) -> None:
        transport = _RecordingTransport(503, b"")
        backend = SendGridEmailBackend(
            transport=transport,
            api_key=Secret("key-1", reference="test://backends"),
            from_address="ops@example.com",
        )
        with pytest.raises(DependencyUnavailableError):
            await backend.send(
                to=["u@example.com"], subject="s", body="b", reply_to=None, timeout_seconds=5.0
            )

    async def test_backend_drives_the_email_tool(self) -> None:
        transport = _RecordingTransport(202, b"", {"x-message-id": "sg-7"})
        backend = SendGridEmailBackend(
            transport=transport,
            api_key=Secret("key-1", reference="test://backends"),
            from_address="ops@example.com",
        )
        tool = EmailTool(
            backend,
            config=EmailToolConfig(allowed_domains=frozenset({"example.com"})),
        )
        result = await tool.invoke(
            {"to": "u@example.com", "subject": "s", "body": "b"},
            _context(),
        )
        assert result["sent"] is True
        assert result["message_id"] == "sg-7"


class TestBraveSearchBackend:
    async def test_maps_web_results(self) -> None:
        body = (
            b'{"web":{"results":['
            b'{"title":"T","url":"https://example.com/a","description":"D","relevance":0.9},'
            b'{"title":"","url":"https://example.com/b","description":"E"}'
            b"]}}"
        )
        transport = _RecordingTransport(200, body)
        backend = BraveSearchBackend(
            transport=transport, api_key=Secret("brave-key", reference="test://backends")
        )
        results = await backend.search("hello world", limit=5, timeout_seconds=5.0)
        assert [r.url for r in results] == ["https://example.com/a", "https://example.com/b"]
        assert results[0].title == "T"
        assert results[0].snippet == "D"
        assert transport.calls[0]["headers"]["x-subscription-token"] == "brave-key"

    async def test_empty_query_and_bad_limit_are_refused(self) -> None:
        backend = BraveSearchBackend(
            transport=_RecordingTransport(), api_key=Secret("k", reference="test://backends")
        )
        with pytest.raises(InvalidRequestError):
            await backend.search("   ", limit=5, timeout_seconds=5.0)
        with pytest.raises(InvalidRequestError):
            await backend.search("q", limit=0, timeout_seconds=5.0)

    async def test_provider_refusal_is_typed(self) -> None:
        transport = _RecordingTransport(422, b"")
        backend = BraveSearchBackend(
            transport=transport, api_key=Secret("k", reference="test://backends")
        )
        with pytest.raises(ToolFailedError):
            await backend.search("q", limit=3, timeout_seconds=5.0)
        transport2 = _RecordingTransport(500, b"")
        backend2 = BraveSearchBackend(
            transport=transport2, api_key=Secret("k", reference="test://backends")
        )
        with pytest.raises(DependencyUnavailableError):
            await backend2.search("q", limit=3, timeout_seconds=5.0)

    async def test_a_response_without_results_is_a_failure(self) -> None:
        transport = _RecordingTransport(200, b'{"web":{}}')
        backend = BraveSearchBackend(
            transport=transport, api_key=Secret("k", reference="test://backends")
        )
        with pytest.raises(ToolFailedError):
            await backend.search("q", limit=3, timeout_seconds=5.0)


class TestTavilySearchBackend:
    async def test_maps_results_and_authenticates(self) -> None:
        body = (
            b'{"results":[{"title":"T","url":"https://example.com/a","content":"C","score":0.7}]}'
        )
        transport = _RecordingTransport(200, body)
        backend = TavilySearchBackend(
            transport=transport, api_key=Secret("tv-key", reference="test://backends")
        )
        results = await backend.search("q", limit=3, timeout_seconds=5.0)
        assert len(results) == 1
        assert results[0].score == 0.7
        call = transport.calls[0]
        assert call["headers"]["authorization"] == "Bearer tv-key"
        assert call["json_body"] == {"query": "q", "max_results": 3}

    async def test_malformed_body_is_a_failure(self) -> None:
        transport = _RecordingTransport(200, b'{"nope":true}')
        backend = TavilySearchBackend(
            transport=transport, api_key=Secret("k", reference="test://backends")
        )
        with pytest.raises(ToolFailedError):
            await backend.search("q", limit=3, timeout_seconds=5.0)

    async def test_results_are_truncated_to_the_limit(self) -> None:
        items = b",".join(
            b'{"title":"t","url":"https://example.com/%d","content":"c"}' % i for i in range(5)
        )
        transport = _RecordingTransport(200, b'{"results":[' + items + b"]}")
        backend = TavilySearchBackend(
            transport=transport, api_key=Secret("k", reference="test://backends")
        )
        results = await backend.search("q", limit=2, timeout_seconds=5.0)
        assert len(results) == 2


class TestSearxngSearchBackend:
    def test_requires_an_absolute_url(self) -> None:
        with pytest.raises(InvalidRequestError):
            SearxngSearchBackend(transport=_RecordingTransport(), base_url="search.example.com")

    async def test_maps_results(self) -> None:
        body = b'{"results":[{"title":"T","url":"https://example.com/a","content":"C"}]}'
        transport = _RecordingTransport(200, body)
        backend = SearxngSearchBackend(transport=transport, base_url="https://search.example.com/")
        results = await backend.search("q", limit=3, timeout_seconds=5.0)
        assert results[0].url == "https://example.com/a"
        assert transport.calls[0]["url"].startswith("https://search.example.com/search?q=q")


class _FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]], names: list[str]) -> None:
        self._rows = rows
        self.description = [type("Col", (), {"name": name})() for name in names]
        self.closed = False

    async def fetchmany(self, count: int) -> list[tuple[Any, ...]]:
        return self._rows[:count]

    async def close(self) -> None:
        self.closed = True


class _FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]], names: list[str]) -> None:
        self.statements: list[str] = []
        self.params: list[list[Any]] = []
        self._rows = rows
        self._names = names

    def transaction(self) -> Any:
        connection = self

        class _Tx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *args: object) -> None:
                return None

        del connection
        return _Tx()

    async def execute(self, statement: str, parameters: list[Any] | None = None) -> _FakeCursor:
        self.statements.append(statement)
        self.params.append(list(parameters or []))
        if statement.strip().upper().startswith(("SET",)):
            return _FakeCursor([], [])
        return _FakeCursor(self._rows, self._names)


class _FakePool:
    def __init__(self, rows: list[tuple[Any, ...]], names: list[str]) -> None:
        self.connection_obj = _FakeConnection(rows, names)

    def connection(self) -> Any:
        pool = self

        class _Ctx:
            async def __aenter__(self) -> _FakeConnection:
                return pool.connection_obj

            async def __aexit__(self, *args: object) -> None:
                return None

        return _Ctx()


class TestPostgresReadOnlyBackend:
    def test_requires_a_pool(self) -> None:
        with pytest.raises(InvalidRequestError):
            PostgresReadOnlyBackend(pool=None)

    def test_multi_statement_input_is_refused(self) -> None:
        backend = PostgresReadOnlyBackend(pool=_FakePool([], []))
        with pytest.raises(ToolFailedError):
            backend._refuse_unsafe("select 1; drop table users")

    @pytest.mark.parametrize(
        "statement",
        [
            "insert into t values (1)",
            "update t set x = 1",
            "delete from t",
            "create table t (id int)",
            "drop table t",
            "select * from (delete from t returning *) x",
            "with x as (insert into t values (1) returning *) select * from x",
        ],
    )
    def test_write_shapes_are_refused(self, statement: str) -> None:
        backend = PostgresReadOnlyBackend(pool=_FakePool([], []))
        with pytest.raises(ToolFailedError):
            backend._refuse_unsafe(statement)

    async def test_select_runs_read_only_with_a_timeout_and_maps_rows(self) -> None:
        pool = _FakePool([(1, "a"), (2, "b"), (3, "c")], ["id", "name"])
        backend = PostgresReadOnlyBackend(pool=pool)
        rows = await backend.fetch(
            "select id, name from t where id = %s",
            parameters=[1],
            limit=2,
            timeout_seconds=1.5,
        )
        assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        connection = pool.connection_obj
        assert connection.statements[0] == "SET TRANSACTION READ ONLY"
        assert connection.statements[1] == "SET LOCAL statement_timeout = 1500"
        assert connection.statements[2].startswith("select id, name")
        assert connection.params[2] == [1]

    async def test_bad_limit_or_timeout_is_refused(self) -> None:
        backend = PostgresReadOnlyBackend(pool=_FakePool([], []))
        with pytest.raises(InvalidRequestError):
            await backend.fetch("select 1", parameters=[], limit=0, timeout_seconds=1.0)
        with pytest.raises(InvalidRequestError):
            await backend.fetch("select 1", parameters=[], limit=1, timeout_seconds=0.0)


class TestRemoteSandboxBackend:
    def test_requires_an_absolute_endpoint(self) -> None:
        with pytest.raises(InvalidRequestError):
            RemoteSandboxBackend(transport=_RecordingTransport(), endpoint_url="sandbox.local")

    async def test_execute_maps_the_outcome(self) -> None:
        body = b'{"exit_code":0,"stdout":"hi","stderr":"","timed_out":false}'
        transport = _RecordingTransport(200, body)
        backend = RemoteSandboxBackend(
            transport=transport,
            endpoint_url="https://sandbox.example.com",
            api_key=Secret("sb", reference="test://backends"),
        )
        result = await backend.execute(
            language="python",
            source="print('hi')",
            stdin="",
            limits=SandboxLimits(),
        )
        assert result.succeeded
        assert result.stdout == "hi"
        call = transport.calls[0]
        assert call["url"] == "https://sandbox.example.com/execute"
        assert call["headers"]["authorization"] == "Bearer sb"
        assert call["json_body"]["limits"]["memory_megabytes"] == 256

    async def test_a_failing_program_is_a_result_not_an_error(self) -> None:
        body = b'{"exit_code":1,"stdout":"","stderr":"boom","timed_out":false}'
        transport = _RecordingTransport(200, body)
        backend = RemoteSandboxBackend(
            transport=transport, endpoint_url="https://sandbox.example.com"
        )
        result = await backend.execute(
            language="python", source="raise SystemExit(1)", stdin="", limits=SandboxLimits()
        )
        assert not result.succeeded
        assert result.exit_code == 1
        assert result.stderr == "boom"

    async def test_a_timeout_is_a_result(self) -> None:
        body = (
            b'{"exit_code":null,"stdout":"","stderr":"",'
            b'"timed_out":true,"killed_reason":"wall clock"}'
        )
        transport = _RecordingTransport(200, body)
        backend = RemoteSandboxBackend(
            transport=transport, endpoint_url="https://sandbox.example.com"
        )
        result = await backend.execute(
            language="python", source="while True: pass", stdin="", limits=SandboxLimits()
        )
        assert result.timed_out
        assert result.killed_reason == "wall clock"

    async def test_service_failures_are_typed(self) -> None:
        transport = _RecordingTransport(400, b"")
        backend = RemoteSandboxBackend(
            transport=transport, endpoint_url="https://sandbox.example.com"
        )
        with pytest.raises(ToolFailedError):
            await backend.execute(
                language="python", source="pass", stdin="", limits=SandboxLimits()
            )
        transport2 = _RecordingTransport(502, b"")
        backend2 = RemoteSandboxBackend(
            transport=transport2, endpoint_url="https://sandbox.example.com"
        )
        with pytest.raises(DependencyUnavailableError):
            await backend2.execute(
                language="python", source="pass", stdin="", limits=SandboxLimits()
            )

    async def test_a_malformed_body_is_a_failure(self) -> None:
        transport = _RecordingTransport(200, b"not-json")
        backend = RemoteSandboxBackend(
            transport=transport, endpoint_url="https://sandbox.example.com"
        )
        with pytest.raises(ToolFailedError):
            await backend.execute(
                language="python", source="pass", stdin="", limits=SandboxLimits()
            )


class TestSearchStack:
    async def test_brave_backend_drives_the_search_tool(self) -> None:
        body = (
            b'{"web":{"results":[{"title":"T","url":"https://example.com/a","description":"D"}]}}'
        )
        transport = _RecordingTransport(200, body)
        backend = BraveSearchBackend(
            transport=transport, api_key=Secret("k", reference="test://backends")
        )
        tool = SearchTool(backend, config=SearchToolConfig())
        result = await tool.invoke({"query": "hello"}, _context())
        assert result["count"] == 1


class TestMemoryStoreFactory:
    """memory.backends.build_memory_store routes by backend name."""

    def test_postgres_routes_to_postgres_memory_store(self) -> None:
        from mas_orchestrator.memory.backends import build_memory_store
        from mas_orchestrator.memory.postgres_store import PostgresMemoryStore

        pool = object()
        store = build_memory_store("postgres", pool=pool)
        assert isinstance(store, PostgresMemoryStore)

    def test_postgres_without_pool_fails_closed(self) -> None:
        from mas_orchestrator.memory.backends import build_memory_store

        with pytest.raises(ConfigurationError) as excinfo:
            build_memory_store("postgres")
        assert excinfo.value.details == {"backend": "postgres"}

    def test_in_process_is_the_default(self) -> None:
        from mas_orchestrator.memory.backends import (
            InProcessMemoryStore,
            build_memory_store,
        )

        assert isinstance(build_memory_store(""), InProcessMemoryStore)
        assert isinstance(build_memory_store("in-process"), InProcessMemoryStore)

    def test_unknown_backend_fails_closed(self) -> None:
        from mas_orchestrator.memory.backends import build_memory_store

        with pytest.raises(ConfigurationError) as excinfo:
            build_memory_store("mysql")
        assert excinfo.value.details["backend"] == "mysql"
