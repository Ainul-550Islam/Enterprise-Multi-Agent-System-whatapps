"""The internal AI-service HTTP API.

Optional by design: this service exists to serve Rust over gRPC, and the HTTP
API is a convenience for internal callers that cannot speak gRPC. Nothing in
:mod:`mas_orchestrator` depends on this package, so it can be removed without
touching the runtime.

Three rules govern everything here.

**Tenancy is never taken from the request.** Identity arrives in headers under
a trusted namespace, set by the layer that authenticated the caller, and never
from a body field. There is no ``tenant_id`` on any request schema in
:mod:`.schemas`, and there is no parameter that overrides the tenant in
:mod:`.dependencies`. A request that cannot be attributed to a tenant is
refused rather than defaulted — a default tenant would mean an unauthenticated
call runs as somebody.

**Errors keep their identity.** :mod:`.errors` maps each typed error to a
specific status and a stable code. A caller can tell "you may not" from "we
broke", which is the distinction that determines whether retrying is sensible.
Exception text is never echoed, because it can contain provider or tool
content.

**Health tells the truth without leaking.** :mod:`.health` separates liveness
from readiness, and reports configuration by name and length only. A health
endpoint is usually the least protected route in a service, which is why it
must not carry secrets.

The transport is a small ASGI implementation in :mod:`.app` rather than a web
framework, because spec §21 treats a dependency as a decision. The runtime
accepts JSON and returns JSON, and that is the whole surface.

During shutdown every work route returns ``503`` while health routes keep
answering. Accepting work during a drain guarantees abandoned executions, and
reporting unhealthy would tell an orchestrator to restart a service that is
shutting down on purpose.
"""

from __future__ import annotations

from mas_orchestrator.api.app import (
    MAX_BODY_BYTES,
    AppConfig,
    Application,
    create_app,
)
from mas_orchestrator.api.dependencies import (
    HEADER_AGENT_ID,
    HEADER_AGENT_VERSION_ID,
    HEADER_CORRELATION_ID,
    HEADER_EXECUTION_ID,
    HEADER_ORGANIZATION_ID,
    HEADER_REQUEST_ID,
    HEADER_TASK_ATTEMPT_ID,
    HEADER_TASK_ID,
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
    STATUS_MAP,
    ErrorBody,
    is_retryable_status,
    log_error,
    status_for,
    to_body,
)
from mas_orchestrator.api.health import (
    ALWAYS_AVAILABLE,
    is_always_available,
    liveness,
    readiness,
    startup,
)
from mas_orchestrator.api.routes import (
    API_VERSION,
    UNVERSIONED_PATHS,
    Method,
    Route,
    RouteTable,
    build_default_table,
)
from mas_orchestrator.api.schemas import (
    MAX_METADATA_KEYS,
    MAX_METADATA_VALUE_CHARS,
    MAX_REQUEST_TEXT_CHARS,
    ErrorResponseSchema,
    ExecutionRequestSchema,
    ExecutionResponseSchema,
    HealthResponseSchema,
    MemoryWriteSchema,
    ReadinessResponseSchema,
    RetrievalRequestSchema,
    request_from_schema,
)

__all__ = [
    "ALWAYS_AVAILABLE",
    "API_VERSION",
    "HEADER_AGENT_ID",
    "HEADER_AGENT_VERSION_ID",
    "HEADER_CORRELATION_ID",
    "HEADER_EXECUTION_ID",
    "HEADER_ORGANIZATION_ID",
    "HEADER_REQUEST_ID",
    "HEADER_TASK_ATTEMPT_ID",
    "HEADER_TASK_ID",
    "HEADER_TENANT_ID",
    "MAX_BODY_BYTES",
    "MAX_METADATA_KEYS",
    "MAX_METADATA_VALUE_CHARS",
    "MAX_REQUEST_TEXT_CHARS",
    "STATUS_MAP",
    "TRUSTED_HEADER_PREFIX",
    "UNTRUSTED_TENANT_HEADERS",
    "UNVERSIONED_PATHS",
    "AppConfig",
    "Application",
    "ErrorBody",
    "ErrorResponseSchema",
    "ExecutionRequestSchema",
    "ExecutionResponseSchema",
    "HealthResponseSchema",
    "MemoryWriteSchema",
    "Method",
    "ReadinessResponseSchema",
    "RequestScope",
    "RetrievalRequestSchema",
    "Route",
    "RouteTable",
    "assert_same_tenant",
    "build_default_table",
    "build_scope",
    "context_from_headers",
    "create_app",
    "filter_trusted",
    "is_always_available",
    "is_retryable_status",
    "is_trusted_header",
    "liveness",
    "log_error",
    "new_correlation_id",
    "readiness",
    "request_from_schema",
    "startup",
    "status_for",
    "to_body",
]
