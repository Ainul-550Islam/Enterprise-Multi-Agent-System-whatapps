# PURPOSE:
# Public surface of the orchestrator's gRPC layer.
#
# Re-exports the metadata helpers that establish the trust boundary, the status
# translation used in both directions, the bridge servicer that receives Rust's
# calls, and the server bootstrap that exposes it.

"""Rust Core gRPC integration.

This package contains the transport between Python and the Rust Core:

* :mod:`mas_orchestrator.grpc.channel` — secure channel creation and lifecycle.
* :mod:`mas_orchestrator.grpc.client` — authenticated, deadline-aware client.
* :mod:`mas_orchestrator.grpc.metadata` — correlation/tenancy propagation rules.
* :mod:`mas_orchestrator.grpc.errors` — bidirectional status<->error mapping.
* :mod:`mas_orchestrator.grpc.agent_bridge_servicer` — production
  ``AgentBridgeApi`` servicer: the inbound Rust → Python trust boundary.
* :mod:`mas_orchestrator.grpc.data_hub_servicer` — optional
  ``DataHubBridgeApi`` servicer, reusing that same authenticated trust boundary.
* :mod:`mas_orchestrator.grpc.server` — server bootstrap, message bounds and
  orderly shutdown for both surfaces.
* :mod:`mas_orchestrator.grpc.generated` — protobuf bindings generated from the
  authoritative Rust ``.proto``. Never hand-edited.

The inbound direction (Rust calling Python) is served from
:mod:`mas_orchestrator.api`, which uses these same metadata and error rules so
that both directions agree on the contract.
"""

from __future__ import annotations

from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.channel import (
    DEFAULT_MAX_MESSAGE_BYTES,
    ChannelConfig,
    ManagedChannel,
    create_channel,
)
from mas_orchestrator.grpc.client import (
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    RustCoreClient,
    RustCoreClientConfig,
    client_from_settings,
)
from mas_orchestrator.grpc.data_hub_servicer import DataHubBridgeServicer
from mas_orchestrator.grpc.errors import (
    CODE_TO_STATUS,
    STATUS_TO_CODE,
    abort_with_error,
    details_from_abort,
    error_from_grpc_status,
    is_retryable_status,
    to_grpc_status,
)
from mas_orchestrator.grpc.metadata import (
    AUTHORIZATION_HEADER,
    CORRELATION_HEADER,
    ORGANIZATION_HEADER,
    PROTOCOL_VERSION_HEADER,
    REQUEST_ID_HEADER,
    TENANT_HEADER,
    RustCallMetadata,
    build_outbound_metadata,
    extract_call_metadata,
    parse_grpc_timeout,
    reconcile_tenant_scope,
)
from mas_orchestrator.grpc.server import (
    GrpcServer,
    GrpcServerConfig,
)

__all__ = [
    "AUTHORIZATION_HEADER",
    "CODE_TO_STATUS",
    "CORRELATION_HEADER",
    "DEFAULT_MAX_MESSAGE_BYTES",
    "DEFAULT_REQUEST_TIMEOUT_SECONDS",
    "ORGANIZATION_HEADER",
    "PROTOCOL_VERSION_HEADER",
    "REQUEST_ID_HEADER",
    "STATUS_TO_CODE",
    "TENANT_HEADER",
    "AgentBridgeServicer",
    "AgentBridgeServicerConfig",
    "ChannelConfig",
    "DataHubBridgeServicer",
    "GrpcServer",
    "GrpcServerConfig",
    "ManagedChannel",
    "RustCallMetadata",
    "RustCoreClient",
    "RustCoreClientConfig",
    "abort_with_error",
    "build_outbound_metadata",
    "client_from_settings",
    "create_channel",
    "details_from_abort",
    "error_from_grpc_status",
    "extract_call_metadata",
    "is_retryable_status",
    "parse_grpc_timeout",
    "reconcile_tenant_scope",
    "to_grpc_status",
]
