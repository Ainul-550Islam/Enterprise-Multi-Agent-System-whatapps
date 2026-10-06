"""Tests for the HTTP API boundary.

The claims that matter, all from spec §23:

* a request cannot name its own tenant;
* identity headers outside the trusted namespace are ignored, not honoured;
* a typed error keeps its identity as a status and a code;
* health answers liveness without touching dependencies, and readiness without
  leaking configuration.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from mas_orchestrator.api.app import MAX_BODY_BYTES, AppConfig, Application, create_app
from mas_orchestrator.api.dependencies import (
    HEADER_AGENT_ID,
    HEADER_CORRELATION_ID,
    HEADER_EXECUTION_ID,
    HEADER_ORGANIZATION_ID,
    HEADER_TENANT_ID,
    TRUSTED_HEADER_PREFIX,
    UNTRUSTED_TENANT_HEADERS,
    RequestScope,
    assert_same_tenant,
    build_scope,
    context_from_headers,
    filter_trusted,
    is_trusted_header,
    new_correlation_id,
)
from mas_orchestrator.api.errors import (
    ErrorBody,
    is_retryable_status,
    status_for,
    to_body,
)
from mas_orchestrator.api.health import is_always_available, liveness, readiness
from mas_orchestrator.api.routes import API_VERSION, Method, RouteTable, build_default_table
from mas_orchestrator.api.schemas import (
    MAX_REQUEST_TEXT_CHARS,
    ExecutionRequestSchema,
    MemoryWriteSchema,
    RetrievalRequestSchema,
    request_from_schema,
)
from mas_orchestrator.contracts.errors import (
    AgentNotFoundError,
    AuthenticationError,
    InvalidRequestError,
    ModelRateLimitedError,
    PolicyDeniedError,
    TenantContextError,
)
from mas_orchestrator.contracts.protocol_version import CURRENT_PROTOCOL_VERSION
from mas_orchestrator.security.identity import ServiceIdentity

TENANT = "tenant-alpha"


def _headers(**overrides: str) -> dict[str, str]:
    base = {
        HEADER_TENANT_ID: TENANT,
        HEADER_ORGANIZATION_ID: "org-1",
        HEADER_EXECUTION_ID: "exec-1",
        HEADER_AGENT_ID: "worker",
        HEADER_CORRELATION_ID: "corr-1",
    }
    base.update(overrides)
    return base


def _caller() -> ServiceIdentity:
    return ServiceIdentity(subject="rust-core", issuer="static")


class TestTrustedHeaders:
    def test_trusted_prefix_is_honoured(self) -> None:
        assert is_trusted_header(HEADER_TENANT_ID)

    def test_untrusted_prefix_is_not(self) -> None:
        assert not is_trusted_header("x-tenant-id")

    def test_filter_drops_untrusted(self) -> None:
        trusted = filter_trusted({**_headers(), "x-tenant-id": "attacker"})
        assert "x-tenant-id" not in trusted
        assert trusted[HEADER_TENANT_ID] == TENANT

    def test_untrusted_tenant_headers_are_known(self) -> None:
        # Recognised so the refusal is explainable rather than mysterious.
        assert "x-tenant-id" in UNTRUSTED_TENANT_HEADERS

    def test_prefix_constant_is_a_prefix(self) -> None:
        assert HEADER_TENANT_ID.startswith(TRUSTED_HEADER_PREFIX)


class TestRequestScope:
    def test_correlation_id_is_supplied_or_generated(self) -> None:
        assert build_scope(_headers()).correlation_id == "corr-1"
        assert new_correlation_id()

    def test_scope_records_elapsed_time(self) -> None:
        assert build_scope(_headers()).elapsed_ms >= 0

    def test_scope_marks_authentication(self) -> None:
        assert not RequestScope("c").authenticated


class TestContextFromHeaders:
    def test_builds_context_from_trusted_headers(self) -> None:
        context = context_from_headers(
            _headers(),
            protocol_version=CURRENT_PROTOCOL_VERSION,
            caller=_caller(),
        )
        assert context.tenant_id == TENANT
        assert context.execution_id == "exec-1"

    def test_unauthenticated_request_is_refused(self) -> None:
        # An unauthenticated caller must not be able to name a tenant at all.
        with pytest.raises(AuthenticationError):
            context_from_headers(
                _headers(),
                authenticated=False,
                protocol_version=CURRENT_PROTOCOL_VERSION,
                caller=_caller(),
            )

    def test_missing_caller_is_refused(self) -> None:
        with pytest.raises(AuthenticationError):
            context_from_headers(_headers(), protocol_version=CURRENT_PROTOCOL_VERSION)

    def test_missing_identity_headers_are_refused(self) -> None:
        with pytest.raises(TenantContextError):
            context_from_headers(
                {HEADER_AGENT_ID: "worker"},
                protocol_version=CURRENT_PROTOCOL_VERSION,
                caller=_caller(),
            )

    def test_tenant_comes_only_from_the_trusted_header(self) -> None:
        # The unprefixed header is ignored, so the tenant is the real one and
        # not the one the caller tried to supply.
        context = context_from_headers(
            {**_headers(), "x-tenant-id": "attacker"},
            protocol_version=CURRENT_PROTOCOL_VERSION,
            caller=_caller(),
        )
        assert context.tenant_id == TENANT

    def test_cross_tenant_assertion(self) -> None:
        context = context_from_headers(
            _headers(), protocol_version=CURRENT_PROTOCOL_VERSION, caller=_caller()
        )
        with pytest.raises(TenantContextError):
            assert_same_tenant(context, "tenant-other")


class TestErrorMapping:
    def test_typed_errors_keep_their_identity(self) -> None:
        # A caller must be able to tell "you may not" from "we broke".
        assert status_for(PolicyDeniedError("no")) == 403
        assert status_for(AgentNotFoundError("no")) == 404
        assert status_for(InvalidRequestError("no")) == 400

    def test_unmapped_error_is_a_server_error(self) -> None:
        assert status_for(RuntimeError("boom")) == 500

    def test_rate_limit_is_retryable(self) -> None:
        assert is_retryable_status(status_for(ModelRateLimitedError("no")))

    def test_safe_errors_describe_themselves(self) -> None:
        body = to_body(PolicyDeniedError("policy says no"))
        assert "no" in body.message

    def test_unsafe_errors_are_generalised(self) -> None:
        # The real message may contain provider or tool content that must not
        # leave the service.
        body = to_body(RuntimeError("token sk-abc123 leaked"), correlation_id="c1")
        assert "sk-" not in body.message
        assert body.correlation_id == "c1"

    def test_body_carries_a_stable_code(self) -> None:
        assert to_body(PolicyDeniedError("no")).code

    def test_body_is_serialisable(self) -> None:
        assert "error" in ErrorBody("c", "m", 400).as_dict()


class TestSchemas:
    def test_request_has_no_tenant_field(self) -> None:
        # Structural, not documentary: without the field there is nothing to
        # defend.
        assert "tenant_id" not in ExecutionRequestSchema.model_fields

    def test_request_rejects_unknown_fields(self) -> None:
        with pytest.raises(ValidationError):
            ExecutionRequestSchema(request="hi", tenant_id=TENANT)  # type: ignore[call-arg]

    def test_request_bounds_text(self) -> None:
        with pytest.raises(ValidationError):
            ExecutionRequestSchema(request="x" * (MAX_REQUEST_TEXT_CHARS + 1))

    def test_request_is_immutable(self) -> None:
        schema = ExecutionRequestSchema(request="hi")
        with pytest.raises(ValidationError):
            schema.request = "other"  # type: ignore[misc]

    def test_metadata_is_bounded(self) -> None:
        with pytest.raises(ValidationError):
            ExecutionRequestSchema(request="hi", metadata={f"k{i}": "v" for i in range(100)})

    def test_request_from_schema_does_not_forward_metadata(self) -> None:
        # Caller metadata must not become runtime context: the context is built
        # from authenticated metadata alone.
        schema = ExecutionRequestSchema(request="hi", metadata={"role": "admin"})
        assert "metadata" not in request_from_schema(schema)

    def test_memory_scope_is_checked(self) -> None:
        with pytest.raises(ValidationError):
            MemoryWriteSchema(content="x", record_id="r", scope="forever")

    def test_retrieval_top_k_is_bounded(self) -> None:
        with pytest.raises(ValidationError):
            RetrievalRequestSchema(query="q", top_k=10_000)


class TestRoutes:
    def test_work_routes_are_versioned(self) -> None:
        table = RouteTable()
        table.add(Method.POST, "/executions", _noop)
        assert table.paths() == (f"/{API_VERSION}/executions",)

    def test_health_routes_are_unversioned(self) -> None:
        table = RouteTable()
        table.add(Method.GET, "/healthz", _noop)
        assert table.paths() == ("/healthz",)

    def test_duplicate_route_is_refused(self) -> None:
        table = RouteTable()
        table.add(Method.GET, "/healthz", _noop)
        with pytest.raises(ValueError, match="already registered"):
            table.add(Method.GET, "/healthz", _noop)

    def test_default_table_marks_health_as_always_served(self) -> None:
        table = build_default_table(execute=_noop, health=_noop, ready=_noop)
        health = table.get("GET", "/healthz")
        executions = table.get("POST", f"/{API_VERSION}/executions")
        assert health is not None and health.served_while_draining
        assert executions is not None and not executions.served_while_draining

    def test_health_routes_need_no_authentication(self) -> None:
        table = build_default_table(execute=_noop, health=_noop, ready=_noop)
        assert table.get("GET", "/readyz") is not None
        assert not table.get("GET", "/readyz").authenticated  # type: ignore[union-attr]


async def _noop(payload: dict[str, object]) -> dict[str, object]:
    return {"ok": True}


class TestApplication:
    def test_body_limit_is_bounded(self) -> None:
        assert 1024 <= MAX_BODY_BYTES <= 8 * 1024 * 1024

    async def test_unknown_route_returns_404(self) -> None:
        app = create_app(RouteTable(), AppConfig())
        status, _ = await _request(app, "GET", "/nope")
        assert status == 404

    async def test_health_route_serves_without_authentication(self) -> None:
        table = RouteTable()
        table.add(Method.GET, "/healthz", _noop, authenticated=False)
        app = create_app(table, AppConfig())
        status, body = await _request(app, "GET", "/healthz")
        assert status == 200
        assert body == {"ok": True}

    async def test_draining_refuses_new_work(self) -> None:
        # Accepting work during a drain guarantees abandoned executions.
        table = RouteTable()
        table.add(Method.POST, "/executions", _noop)
        app = create_app(table, AppConfig(draining=True))
        status, body = await _request(app, "POST", f"/{API_VERSION}/executions")
        assert status == 503
        assert body["error"]["retryable"]  # type: ignore[index]

    async def test_health_is_served_while_draining(self) -> None:
        table = RouteTable()
        table.add(Method.GET, "/healthz", _noop, served_while_draining=True, authenticated=False)
        app = create_app(table, AppConfig(draining=True))
        status, _ = await _request(app, "GET", "/healthz")
        assert status == 200

    async def test_oversized_body_is_refused(self) -> None:
        table = RouteTable()
        table.add(Method.POST, "/executions", _noop, authenticated=False)
        app = create_app(table, AppConfig())
        status, _ = await _request(
            app, "POST", f"/{API_VERSION}/executions", body=b"x" * (MAX_BODY_BYTES + 1)
        )
        assert status == 400

    async def test_malformed_json_is_refused(self) -> None:
        table = RouteTable()
        table.add(Method.POST, "/executions", _noop, authenticated=False)
        app = create_app(table, AppConfig())
        status, _ = await _request(app, "POST", f"/{API_VERSION}/executions", body=b"{not json")
        assert status == 400

    async def test_handler_failure_is_mapped_not_raised(self) -> None:
        async def boom(payload: dict[str, object]) -> dict[str, object]:
            raise InvalidRequestError("bad request")

        table = RouteTable()
        table.add(Method.POST, "/executions", boom, authenticated=False)
        app = create_app(table, AppConfig())
        status, body = await _request(app, "POST", f"/{API_VERSION}/executions", body=b"{}")
        assert status == 400
        assert body["error"]["code"]  # type: ignore[index]


class TestHealth:
    def test_liveness_checks_nothing(self) -> None:
        from mas_orchestrator.observability.health import HealthChecker

        payload = liveness(HealthChecker(), service_name="svc", environment="development")
        assert payload["status"] == "ok"

    def test_readiness_reports_dependency_state(self) -> None:
        from mas_orchestrator.observability.health import HealthChecker

        payload = readiness(HealthChecker(), service_name="svc", environment="development")
        assert "health" in payload
        # Dependencies are nested under the report, and environment values are
        # reported by length only — a health route must not leak configuration.
        assert "dependencies" in payload["health"]

    def test_health_paths_are_always_available(self) -> None:
        assert is_always_available("/healthz")
        assert not is_always_available("/v1/executions")


async def _request(
    app: Application,
    method: str,
    path: str,
    *,
    body: bytes = b"{}",
) -> tuple[int, object]:
    """Drives one ASGI request and returns the status and decoded body."""
    import json

    messages = [
        {"type": "http.request", "body": body, "more_body": False},
    ]
    sent: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    await app(
        {"type": "http", "method": method, "path": path, "headers": []},
        receive,
        send,
    )
    status = int(sent[0]["status"])  # type: ignore[arg-type]
    raw = sent[-1].get("body") or b"{}"
    return status, json.loads(bytes(raw))  # type: ignore[arg-type]
