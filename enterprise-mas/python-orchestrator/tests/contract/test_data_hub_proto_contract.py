"""Data Hub bridge wire contract shared by Rust and Python."""

import asyncio

from mas_orchestrator.grpc.generated import mas_api_pb2 as pb
from mas_orchestrator.security.identity import ServiceScope, StaticTokenServiceIdentityResolver


def test_data_hub_service_has_exact_methods() -> None:
    service = pb.DESCRIPTOR.services_by_name["DataHubBridgeApi"]
    assert [
        (method.name, method.input_type.name, method.output_type.name) for method in service.methods
    ] == [
        ("SyncSource", "SyncSourceRequest", "SyncSourceReply"),
        ("IngestDocument", "IngestDocumentRequest", "DocumentIngestionReply"),
        ("PurgeDocument", "PurgeDocumentRequest", "PurgeDocumentReply"),
        ("Health", "Empty", "DataHubHealthReply"),
    ]


def test_stateful_requests_include_authoritative_scope_and_identity() -> None:
    for cls in (pb.SyncSourceRequest, pb.IngestDocumentRequest, pb.PurgeDocumentRequest):
        fields = cls.DESCRIPTOR.fields_by_name
        assert {
            "tenant_id",
            "organization_id",
            "project_id",
            "source_id",
            "identity",
            "correlation_id",
            "request_id",
            "protocol_version",
            "deadline_rfc3339",
        } <= set(fields)
        assert fields["identity"].message_type.full_name == "mas.api.v1.ServiceIdentityMsg"
    assert pb.SyncSourceRequest.DESCRIPTOR.fields_by_name["credential_secret_ref"].has_presence
    assert pb.IngestDocumentRequest.DESCRIPTOR.fields_by_name["content_sha256"].has_presence
    assert pb.SyncSourceReply.DESCRIPTOR.fields_by_name["next_cursor"].has_presence


def test_request_roundtrip_preserves_bytes_scope_and_secret_reference() -> None:
    request = pb.IngestDocumentRequest(
        tenant_id="tenant",
        organization_id="org",
        project_id="project",
        source_id="source",
        document_id="document",
        source_kind="upload",
        content=b"\x00content\xff",
        identity=pb.ServiceIdentityMsg(subject="mas-api-data-hub", scopes=["data_hub_ingest"]),
        content_sha256="a" * 64,
    )
    decoded = pb.IngestDocumentRequest.FromString(request.SerializeToString(deterministic=True))
    assert decoded == request
    assert decoded.content == b"\x00content\xff"
    assert decoded.HasField("content_sha256")
    assert not decoded.HasField("source_uri")


def test_data_hub_capabilities_are_not_granted_to_default_identity() -> None:
    resolver = StaticTokenServiceIdentityResolver("test-only-token")
    identity = asyncio.run(resolver.resolve("test-only-token"))
    assert not identity.has_scope(ServiceScope.DATA_HUB_SYNC)
    assert not identity.has_scope(ServiceScope.DATA_HUB_INGEST)
    assert not identity.has_scope(ServiceScope.DATA_HUB_PURGE)
