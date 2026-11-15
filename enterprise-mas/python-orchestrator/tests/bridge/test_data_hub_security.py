"""Data Hub bridge security must delegate to the AgentBridge boundary."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from mas_orchestrator.grpc.data_hub_servicer import DataHubBridgeServicer
from mas_orchestrator.grpc.generated import mas_api_pb2
from mas_orchestrator.security.identity import ServiceScope


class _AgentBridgeBoundary:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, Any, ServiceScope | None]] = []

    async def validate_call_context(
        self,
        request: Any,
        context: Any,
        *,
        required_scope: ServiceScope | None = None,
    ) -> tuple[Any, Any]:
        self.calls.append((request, context, required_scope))
        return (
            SimpleNamespace(protocol_version="1.1.0"),
            SimpleNamespace(subject="rust-core"),
        )


class _AbortContext:
    async def abort(self, *args: Any) -> None:
        raise RuntimeError(f"aborted: {args[1]}")


@pytest.mark.asyncio
async def test_data_hub_validation_reuses_agent_bridge_identity_and_scope() -> None:
    boundary = _AgentBridgeBoundary()
    servicer = DataHubBridgeServicer(
        service=object(),  # The private validation method does not touch storage.
        agent_bridge=boundary,  # type: ignore[arg-type]
    )
    request = mas_api_pb2.SyncSourceRequest(
        protocol_version="1.1.0",
        deadline_rfc3339=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        identity=mas_api_pb2.ServiceIdentityMsg(subject="rust-core"),
    )
    context = object()

    metadata, identity = await servicer._validate(
        request,
        context,  # type: ignore[arg-type]
        ServiceScope.DATA_HUB_SYNC,
    )

    assert boundary.calls == [(request, context, ServiceScope.DATA_HUB_SYNC)]
    assert metadata.protocol_version == "1.1.0"
    assert identity.subject == "rust-core"


@pytest.mark.asyncio
async def test_data_hub_does_not_accept_a_missing_body_identity() -> None:
    boundary = _AgentBridgeBoundary()
    servicer = DataHubBridgeServicer(
        service=object(),
        agent_bridge=boundary,  # type: ignore[arg-type]
    )
    request = mas_api_pb2.SyncSourceRequest(
        protocol_version="1.1.0",
        deadline_rfc3339=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
    )

    with pytest.raises(RuntimeError, match="aborted"):
        await servicer._validate(
            request,
            _AbortContext(),  # type: ignore[arg-type]
            ServiceScope.DATA_HUB_SYNC,
        )

    assert len(boundary.calls) == 1
    assert boundary.calls[0][2] is ServiceScope.DATA_HUB_SYNC
