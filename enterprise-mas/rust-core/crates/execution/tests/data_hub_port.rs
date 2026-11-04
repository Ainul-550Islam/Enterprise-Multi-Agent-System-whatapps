//! Behavioral tests for the explicit in-memory Data Hub lane.

use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_execution::data_hub_bridge::{
    DataHubBridgePort, DataHubCallContext, DataHubIngestDocumentRequest,
    DataHubPurgeDocumentRequest, DataHubSourceKind, InMemoryDataHubBridge,
};
use uuid::Uuid;

fn context(tenant: TenantId) -> DataHubCallContext {
    DataHubCallContext::new(
        tenant,
        OrganizationId::new(),
        ProjectId::new(),
        "correlation-test",
        None,
        Timestamp::now(),
    )
    .expect("scoped context")
}

fn request(
    context: DataHubCallContext,
    source: Uuid,
    document: Uuid,
) -> DataHubIngestDocumentRequest {
    DataHubIngestDocumentRequest {
        context,
        source_id: source,
        document_id: document,
        source_kind: DataHubSourceKind::Upload,
        title: "document".to_owned(),
        mime_type: "text/plain".to_owned(),
        content: b"test content".to_vec(),
        source_uri: None,
        content_sha256: None,
        required_roles: Vec::new(),
        denied_principals: Vec::new(),
        metadata: serde_json::json!({}),
    }
}

#[test]
fn rejects_nil_tenant_and_empty_correlation() {
    assert!(DataHubCallContext::new(
        TenantId::nil(),
        OrganizationId::new(),
        ProjectId::new(),
        "correlation",
        None,
        Timestamp::now()
    )
    .is_err());
    assert!(DataHubCallContext::new(
        TenantId::new(),
        OrganizationId::new(),
        ProjectId::new(),
        " ",
        None,
        Timestamp::now()
    )
    .is_err());
}

#[tokio::test]
async fn bad_content_digest_is_rejected_without_persisting_a_chunk() {
    let bridge = InMemoryDataHubBridge::new();
    let tenant = TenantId::new();
    let source = Uuid::new_v4();
    let document = Uuid::new_v4();
    let scope = context(tenant);
    let mut input = request(scope.clone(), source, document);
    input.content_sha256 = Some("incorrect".to_owned());
    assert!(bridge.ingest_document(input).await.is_err());
    let result = bridge
        .purge_document(DataHubPurgeDocumentRequest {
            context: scope,
            source_id: source,
            document_id: document,
        })
        .await
        .expect("purge");
    assert_eq!(result.removed_chunks, 0);
}

#[tokio::test]
async fn a_different_tenant_cannot_purge_an_ingested_document() {
    let bridge = InMemoryDataHubBridge::new();
    let tenant = TenantId::new();
    let source = Uuid::new_v4();
    let document = Uuid::new_v4();
    let scope = context(tenant);
    bridge
        .ingest_document(request(scope.clone(), source, document))
        .await
        .expect("ingest");
    let foreign = bridge
        .purge_document(DataHubPurgeDocumentRequest {
            context: context(TenantId::new()),
            source_id: source,
            document_id: document,
        })
        .await
        .expect("foreign purge");
    assert_eq!(foreign.removed_chunks, 0);
    let own = bridge
        .purge_document(DataHubPurgeDocumentRequest {
            context: scope,
            source_id: source,
            document_id: document,
        })
        .await
        .expect("own purge");
    assert_eq!(own.removed_chunks, 1);
}

#[tokio::test]
async fn wrong_source_or_project_cannot_purge_a_document() {
    let bridge = InMemoryDataHubBridge::new();
    let tenant = TenantId::new();
    let scope = context(tenant);
    let source = Uuid::new_v4();
    let document = Uuid::new_v4();
    bridge
        .ingest_document(request(scope.clone(), source, document))
        .await
        .expect("ingest");
    let wrong_source = bridge
        .purge_document(DataHubPurgeDocumentRequest {
            context: scope.clone(),
            source_id: Uuid::new_v4(),
            document_id: document,
        })
        .await
        .expect("wrong source");
    assert_eq!(wrong_source.removed_chunks, 0);
    let mut wrong_project = scope.clone();
    wrong_project.project_id = ProjectId::new();
    let wrong_project_result = bridge
        .purge_document(DataHubPurgeDocumentRequest {
            context: wrong_project,
            source_id: source,
            document_id: document,
        })
        .await
        .expect("wrong project");
    assert_eq!(wrong_project_result.removed_chunks, 0);
    let mut wrong_organization = scope.clone();
    wrong_organization.organization_id = OrganizationId::new();
    let wrong_organization_result = bridge
        .purge_document(DataHubPurgeDocumentRequest {
            context: wrong_organization,
            source_id: source,
            document_id: document,
        })
        .await
        .expect("wrong organization");
    assert_eq!(wrong_organization_result.removed_chunks, 0);
    let correct = bridge
        .purge_document(DataHubPurgeDocumentRequest {
            context: scope,
            source_id: source,
            document_id: document,
        })
        .await
        .expect("correct scope");
    assert_eq!(correct.removed_chunks, 1);
}
