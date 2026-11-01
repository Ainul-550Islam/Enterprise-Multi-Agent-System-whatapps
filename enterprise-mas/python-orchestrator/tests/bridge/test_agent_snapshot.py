"""Rust-owned immutable agent snapshots at the Python AgentBridge boundary."""

from __future__ import annotations

import json

import grpc
import pytest
from tests.bridge.conftest import (
    AGENT,
    AGENT_VERSION,
    ORGANIZATION,
    PROJECT,
    TENANT,
    BridgeHarness,
    metadata,
    request,
    snapshot_json,
)

pytestmark = pytest.mark.asyncio(loop_scope="function")


async def test_exact_snapshot_agent_spec_is_dispatched(bridge: BridgeHarness) -> None:
    """The runtime receives the signed Rust snapshot, not a catalog lookup."""
    stub, channel = bridge.stub()
    try:
        reply = await stub.RunAgent(request(), metadata=metadata())
    finally:
        await channel.close()

    assert reply.WhichOneof("verdict") == "succeeded"
    assert bridge.service.call_count == 1
    runtime_spec = bridge.service.runs[0].agent_spec
    assert runtime_spec is not None
    assert runtime_spec.agent_id == AGENT
    assert runtime_spec.tenant_id == TENANT
    assert runtime_spec.agent_version_id == AGENT_VERSION
    assert runtime_spec.model_hint == "provider/model-test"
    assert runtime_spec.system_prompt == "Rust-published test instructions"
    assert runtime_spec.limits.max_steps == 25


async def test_missing_snapshot_is_rejected_before_runtime_dispatch(bridge: BridgeHarness) -> None:
    stub, channel = bridge.stub()
    try:
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(request(agent_snapshot_json=""), metadata=metadata())
    finally:
        await channel.close()

    assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    assert bridge.service.call_count == 0


async def test_snapshot_from_another_tenant_is_rejected(bridge: BridgeHarness) -> None:
    foreign_snapshot = snapshot_json(tenant_id="22222222-2222-4222-8222-222222222222")
    stub, channel = bridge.stub()
    try:
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(
                request(agent_snapshot_json=foreign_snapshot),
                metadata=metadata(),
            )
    finally:
        await channel.close()

    assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    assert bridge.service.call_count == 0


async def test_disabled_snapshot_is_rejected_before_dispatch(bridge: BridgeHarness) -> None:
    disabled_snapshot = snapshot_json(status="disabled")
    stub, channel = bridge.stub()
    try:
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(
                request(agent_snapshot_json=disabled_snapshot),
                metadata=metadata(),
            )
    finally:
        await channel.close()

    assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    assert bridge.service.call_count == 0


async def test_each_published_version_materializes_its_own_agent_spec(
    bridge: BridgeHarness,
) -> None:
    """Version selection in Rust survives Python dispatch without substitution."""
    version_one = "77777777-7777-4777-8777-777777777777"
    version_two = "88888888-8888-4888-8888-888888888888"
    first_snapshot = snapshot_json(
        agent_version_id=version_one,
        version_number=1,
        model_hint="provider/model-v1",
        system_prompt="version one immutable prompt",
    )
    second_snapshot = snapshot_json(
        agent_version_id=version_two,
        version_number=2,
        model_hint="provider/model-v2",
        system_prompt="version two immutable prompt",
    )
    stub, channel = bridge.stub()
    try:
        first = await stub.RunAgent(
            request(
                execution_id="execution-v1",
                agent_version=version_one,
                agent_snapshot_json=first_snapshot,
            ),
            metadata=metadata(),
        )
        second = await stub.RunAgent(
            request(
                execution_id="execution-v2",
                agent_version=version_two,
                agent_snapshot_json=second_snapshot,
            ),
            metadata=metadata(),
        )
    finally:
        await channel.close()

    assert first.WhichOneof("verdict") == "succeeded"
    assert second.WhichOneof("verdict") == "succeeded"
    assert bridge.service.call_count == 2
    first_spec, second_spec = (run.agent_spec for run in bridge.service.runs)
    assert first_spec is not None and second_spec is not None
    assert first_spec.agent_version_id == version_one
    assert first_spec.model_hint == "provider/model-v1"
    assert first_spec.system_prompt == "version one immutable prompt"
    assert second_spec.agent_version_id == version_two
    assert second_spec.model_hint == "provider/model-v2"
    assert second_spec.system_prompt == "version two immutable prompt"


async def test_unknown_snapshot_fields_are_rejected(bridge: BridgeHarness) -> None:
    payload = json.loads(
        snapshot_json(
            agent_id=AGENT,
            tenant_id=TENANT,
            organization_id=ORGANIZATION,
            project_id=PROJECT,
            agent_version_id=AGENT_VERSION,
        )
    )
    payload["api_key"] = "must-not-be-forwarded"
    stub, channel = bridge.stub()
    try:
        with pytest.raises(grpc.aio.AioRpcError) as raised:
            await stub.RunAgent(
                request(agent_snapshot_json=json.dumps(payload)),
                metadata=metadata(),
            )
    finally:
        await channel.close()

    assert raised.value.code() is grpc.StatusCode.INVALID_ARGUMENT
    assert bridge.service.call_count == 0
