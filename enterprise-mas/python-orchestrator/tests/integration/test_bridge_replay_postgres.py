"""Live gRPC and PostgreSQL replay regression using the insert-only role.

Requires MAS_TEST_REPLAY_DATABASE_URL for an isolated, migrated database and
its mas_app role. Two actual bridge listeners share the PostgreSQL receipt
store, modelling separate replicas without external model calls.
"""

from __future__ import annotations

import asyncio
import os

import grpc
import pytest

from mas_orchestrator.grpc.agent_bridge_servicer import (
    AgentBridgeServicer,
    AgentBridgeServicerConfig,
)
from mas_orchestrator.grpc.generated import mas_api_pb2_grpc
from mas_orchestrator.grpc.server import GrpcServer, GrpcServerConfig
from mas_orchestrator.security.identity import ServiceScope
from mas_orchestrator.security.internal_replay import PostgresReplayGuard
from tests.bridge.conftest import (
    SIGNING_SECRET,
    RecordingExecutionService,
    internal_signature_pairs,
    metadata,
    request,
    resolver,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="function")]


async def test_signed_request_executes_once_across_bridge_replicas() -> None:
    url = os.environ["MAS_TEST_REPLAY_DATABASE_URL"]
    service = RecordingExecutionService()
    servers = [
        GrpcServer(
            servicer=AgentBridgeServicer(
                execution_service=service,  # type: ignore[arg-type]
                identity_resolver=resolver(),
                replay_guard=PostgresReplayGuard(url),
                config=AgentBridgeServicerConfig(
                    require_scope=ServiceScope.EXECUTE_AGENT,
                    internal_service_secret=SIGNING_SECRET,
                ),
            ),
            config=GrpcServerConfig(host="127.0.0.1", port=0),
        )
        for _ in range(2)
    ]
    ports = []
    channels = []
    try:
        for server in servers:
            ports.append(await server.start())
        channels = [grpc.aio.insecure_channel(f"127.0.0.1:{port}") for port in ports]
        message = request()
        pairs = metadata() + internal_signature_pairs(message)

        async def call(channel: grpc.aio.Channel) -> str:
            try:
                await mas_api_pb2_grpc.AgentBridgeApiStub(channel).RunAgent(message, metadata=pairs)
            except grpc.aio.AioRpcError as error:
                assert error.code() is grpc.StatusCode.UNAUTHENTICATED
                return "rejected"
            return "accepted"

        results = await asyncio.gather(*(call(channels[index % 2]) for index in range(8)))
        assert results.count("accepted") == 1
        assert results.count("rejected") == 7
        assert service.call_count == 1
    finally:
        for channel in channels:
            await channel.close()
        for server in servers:
            await server.stop()
