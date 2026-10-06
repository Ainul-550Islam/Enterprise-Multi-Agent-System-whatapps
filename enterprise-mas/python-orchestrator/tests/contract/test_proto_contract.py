# PURPOSE:
# Contract tests pinning the Python protobuf bindings to the Rust proto.
#
# Asserts the exact field set of every message Rust sends and decodes, so a
# proto change that would desynchronise the two sides fails here instead of
# surfacing as a silent decode error in production.

"""Contract tests between the generated protobuf bindings and the Rust contract.

These tests exist because the Python bindings are generated from the Rust
``.proto``, and a silent regeneration against a *different* proto would break
the service at runtime rather than at build time. Pinning the exact field names
and shapes turns that class of failure into a test failure.

The authoritative contract is:

* ``crates/api/proto/mas/api/v1/mas.proto`` in the Rust workspace
* ``crates/ai-bridge/src/lib.rs`` — the Rust client that calls us

If a field is renamed on the Rust side, this suite fails and the mismatch must
be resolved deliberately rather than by loosening the assertions.
"""

from __future__ import annotations

import pytest

from mas_orchestrator.grpc.generated import mas_api_pb2 as pb
from mas_orchestrator.grpc.generated import mas_api_pb2_grpc as pbg

# The wire protocol version both sides must agree on.
EXPECTED_PROTOCOL_VERSION = "1.0.0"


class TestAgentBridgeServiceShape:
    """The service Python must implement for Rust's GrpcAgentBridge."""

    def test_agent_bridge_api_service_exists(self) -> None:
        assert hasattr(pbg, "AgentBridgeApiServicer")

    def test_servicer_exposes_run_agent(self) -> None:
        assert hasattr(pbg.AgentBridgeApiServicer, "RunAgent")

    def test_servicer_exposes_health(self) -> None:
        assert hasattr(pbg.AgentBridgeApiServicer, "Health")

    def test_expected_service_methods_only(self) -> None:
        """Guard against a proto change adding an unimplemented method."""
        exposed = {
            name
            for name in dir(pbg.AgentBridgeApiServicer)
            if not name.startswith("_") and name[:1].isupper()
        }
        assert exposed == {"RunAgent", "Health"}

    def test_client_stub_matches_the_servicer(self) -> None:
        """Rust's GrpcAgentBridge calls exactly these methods."""
        assert hasattr(pbg, "AgentBridgeApiStub")


class TestAgentRunRequestShape:
    """The request Rust sends; must match `AgentRunRequest` in ai-bridge."""

    def test_field_names_match_the_rust_client(self) -> None:
        actual = [field.name for field in pb.AgentRunRequest.DESCRIPTOR.fields]
        assert actual == [
            "agent_id",
            "agent_version",
            "input_json",
            "correlation_id",
            "request_id",
            "execution_id",
            "task_id",
            "tenant_id",
            "organization_id",
            "project_id",
            "protocol_version",
            "deadline_rfc3339",
            "limits",
            "token_budget_max",
            "idempotency_key",
            "identity",
        ]

    def test_identity_fields_are_strings(self) -> None:
        """Tenant/organization are authoritative strings supplied by Rust."""
        field = pb.AgentRunRequest.DESCRIPTOR.fields_by_name["tenant_id"]
        other = pb.AgentRunRequest.DESCRIPTOR.fields_by_name["organization_id"]
        assert field.type == other.type
        assert field.type == field.TYPE_STRING

    def test_project_id_is_optional(self) -> None:
        """Rust's proto declares `optional string project_id`."""
        field = pb.AgentRunRequest.DESCRIPTOR.fields_by_name["project_id"]
        assert field.has_presence is True

    def test_required_identity_fields_are_not_optional(self) -> None:
        """Mandatory identity is a plain field, not `optional`, in proto3."""
        for name in ("tenant_id", "organization_id", "execution_id", "agent_id"):
            field = pb.AgentRunRequest.DESCRIPTOR.fields_by_name[name]
            assert field.has_presence is False, name

    def test_token_budget_is_a_64_bit_integer(self) -> None:
        field = pb.AgentRunRequest.DESCRIPTOR.fields_by_name["token_budget_max"]
        assert field.type == field.TYPE_UINT64

    def test_round_trip_preserves_values(self) -> None:
        request = pb.AgentRunRequest(
            agent_id="agent-1",
            agent_version="v2",
            input_json='{"prompt":"hi"}',
            correlation_id="corr-1",
            request_id="req-1",
            execution_id="exec-1",
            task_id="task-1",
            tenant_id="tenant-1",
            organization_id="org-1",
            project_id="proj-1",
            protocol_version=EXPECTED_PROTOCOL_VERSION,
            deadline_rfc3339="2099-01-01T00:00:00.000Z",
            token_budget_max=4096,
        )
        decoded = pb.AgentRunRequest.FromString(request.SerializeToString())
        assert decoded.agent_id == "agent-1"
        assert decoded.tenant_id == "tenant-1"
        assert decoded.organization_id == "org-1"
        assert decoded.project_id == "proj-1"
        assert decoded.protocol_version == EXPECTED_PROTOCOL_VERSION
        assert decoded.token_budget_max == 4096

    def test_input_is_json_not_structured_data(self) -> None:
        """`input_json` is a string: the payload shape is Python's concern."""
        field = pb.AgentRunRequest.DESCRIPTOR.fields_by_name["input_json"]
        assert field.type == field.TYPE_STRING


class TestResourceLimitsShape:
    """Mirrors `domain::ResourceLimits`."""

    def test_field_names_match(self) -> None:
        actual = [field.name for field in pb.ResourceLimitsMsg.DESCRIPTOR.fields]
        assert actual == [
            "timeout_ms",
            "max_steps",
            "max_parallel_tasks",
            "max_tool_calls",
            "max_payload_bytes",
            "max_retries",
        ]

    def test_round_trip(self) -> None:
        limits = pb.ResourceLimitsMsg(
            timeout_ms=30_000,
            max_steps=12,
            max_parallel_tasks=8,
            max_tool_calls=25,
            max_payload_bytes=1_048_576,
            max_retries=3,
        )
        decoded = pb.ResourceLimitsMsg.FromString(limits.SerializeToString())
        assert decoded.timeout_ms == 30_000
        assert decoded.max_steps == 12
        assert decoded.max_tool_calls == 25


class TestAgentRunReplyShape:
    """The verdict oneof Rust decodes into `AgentRunVerdict`."""

    def test_verdict_is_a_oneof(self) -> None:
        names = [oneof.name for oneof in pb.AgentRunReply.DESCRIPTOR.oneofs]
        assert names == ["verdict"]

    def test_verdict_has_exactly_two_branches(self) -> None:
        """Succeeded | Failed — there is no third, implicit 'unknown'."""
        oneof = pb.AgentRunReply.DESCRIPTOR.oneofs_by_name["verdict"]
        assert sorted(field.name for field in oneof.fields) == ["failed", "succeeded"]

    def test_success_carries_output_and_usage(self) -> None:
        fields = {field.name for field in pb.AgentSucceeded.DESCRIPTOR.fields}
        assert fields == {"output_json", "usage", "model_key", "finish_reason"}

    def test_failure_carries_code_message_retryable_usage(self) -> None:
        fields = {field.name for field in pb.AgentFailed.DESCRIPTOR.fields}
        assert fields == {
            "code",
            "message",
            "retryable",
            "usage",
            "correlation_id",
            "request_id",
        }

    def test_success_round_trip(self) -> None:
        reply = pb.AgentRunReply(
            succeeded=pb.AgentSucceeded(
                output_json='{"answer":42}',
                usage=pb.AgentUsageMsg(
                    tokens_input=10, tokens_output=20, tool_calls=1, steps=2, active_time_ms=33
                ),
            )
        )
        decoded = pb.AgentRunReply.FromString(reply.SerializeToString())
        assert decoded.WhichOneof("verdict") == "succeeded"
        assert decoded.succeeded.usage.tokens_input == 10

    def test_failure_round_trip(self) -> None:
        reply = pb.AgentRunReply(
            failed=pb.AgentFailed(code="model_timeout", message="timed out", retryable=True)
        )
        decoded = pb.AgentRunReply.FromString(reply.SerializeToString())
        assert decoded.WhichOneof("verdict") == "failed"
        assert decoded.failed.retryable is True

    def test_empty_verdict_is_distinguishable(self) -> None:
        """Python must treat 'no verdict set' as a protocol violation."""
        decoded = pb.AgentRunReply.FromString(pb.AgentRunReply().SerializeToString())
        assert decoded.WhichOneof("verdict") is None


class TestUsageShape:
    """Mirrors `AgentUsage` in `crates/execution/src/agent_bridge.rs`."""

    def test_field_names_match(self) -> None:
        actual = [field.name for field in pb.AgentUsageMsg.DESCRIPTOR.fields]
        assert actual == [
            "tokens_input",
            "tokens_output",
            "tool_calls",
            "steps",
            "active_time_ms",
        ]


class TestHealthContract:
    """The readiness probe Rust calls before accepting traffic."""

    def test_health_reply_shape(self) -> None:
        fields = {field.name for field in pb.BridgeHealthReply.DESCRIPTOR.fields}
        assert fields == {"healthy", "detail"}

    def test_health_request_is_empty(self) -> None:
        request = pb.Empty()
        assert request.SerializeToString() == b""

    def test_unhealthy_reply_round_trip(self) -> None:
        reply = pb.BridgeHealthReply(healthy=False, detail="model provider unreachable")
        decoded = pb.BridgeHealthReply.FromString(reply.SerializeToString())
        assert decoded.healthy is False
        assert decoded.detail == "model provider unreachable"


class TestProtocolVersionAgreement:
    """Both sides must claim the same wire version."""

    @pytest.mark.parametrize("version", ["1.0.0"])
    def test_supported_version_is_carried(self, version: str) -> None:
        request = pb.AgentRunRequest(protocol_version=version)
        assert request.protocol_version == version

    def test_python_negotiates_the_version_rust_sends(self) -> None:
        """The generated proto and the negotiation table must not disagree."""
        from mas_orchestrator.contracts.protocol_version import (
            SUPPORTED_PROTOCOL_VERSIONS,
            negotiate_protocol_version,
        )

        sent_by_rust = pb.AgentRunRequest(protocol_version=EXPECTED_PROTOCOL_VERSION)
        negotiated = negotiate_protocol_version(sent_by_rust.protocol_version)
        assert negotiated in SUPPORTED_PROTOCOL_VERSIONS
