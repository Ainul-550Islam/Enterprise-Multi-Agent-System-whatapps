//! Tenant-scoped Rust-owned Data Hub catalogue and ingestion use cases.
//!
//! Rust owns source/document metadata, project scope, authorization and audit.
//! Document bytes are accepted separately, bounded and digested here, then sent
//! only through the authenticated `DataHubBridgePort`. No vector/index storage
//! is accessed from this service.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::ids::{DataSourceId, DocumentId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_contracts::data_hub::{
    CreateDataSourceRequest, CreateDocumentRequest, DataSourceDto, DataSourceSyncDto, DocumentDto,
};
use mas_domain::{
    AuditActorKind, AuditOutcome, DataSource, DataSourceKind, DataSourceStatus, Document, Project,
    ProjectStatus,
};
use mas_execution::data_hub_bridge::{
    DataHubBridgePort, DataHubCallContext, DataHubDocumentIngestionResult,
    DataHubIngestDocumentRequest, DataHubPurgeDocumentRequest, DataHubSourceKind,
    DataHubSyncSourceRequest,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::audit::{record_mutation, AuditSinkPort};
use crate::context::ServiceContext;
use crate::stores::{DataSourceStorePort, DocumentStorePort, ProjectStorePort};

/// Maximum caller-visible source/document rows returned by one list use case.
pub const MAX_DATA_HUB_LIST_ITEMS: usize = 1_000;
/// Default payload ceiling leaves protobuf metadata headroom under the 16 MiB
/// default gRPC message limit.
pub const DEFAULT_MAX_UPLOAD_BYTES: u64 = 15 * 1024 * 1024;
const MAX_BRIDGE_TIMEOUT: Duration = Duration::from_secs(900);

/// Request limits applied before sending content to the Python ingestion plane.
#[derive(Debug, Clone, Copy)]
pub struct DataHubLimits {
    /// Maximum separately transported upload/document payload.
    pub max_upload_bytes: u64,
    /// End-to-end bridge deadline for ingestion and source sync.
    pub bridge_timeout: Duration,
}

impl Default for DataHubLimits {
    fn default() -> Self {
        Self {
            max_upload_bytes: DEFAULT_MAX_UPLOAD_BYTES,
            bridge_timeout: Duration::from_secs(300),
        }
    }
}

impl DataHubLimits {
    /// Validates local limits against the bridge protocol's supported bounds.
    pub fn validate(self) -> Result<Self> {
        if self.max_upload_bytes == 0 || self.max_upload_bytes > 64 * 1024 * 1024 {
            return Err(AppError::invalid_field(
                "max_upload_bytes",
                "out_of_range",
                "must be between 1 byte and 64 MiB",
            ));
        }
        if self.bridge_timeout.is_zero() || self.bridge_timeout > MAX_BRIDGE_TIMEOUT {
            return Err(AppError::invalid_field(
                "bridge_timeout",
                "out_of_range",
                "must be positive and no greater than 900 seconds",
            ));
        }
        Ok(self)
    }
}

/// Application facade for tenant-bound Data Hub metadata and bridge operations.
#[derive(Debug, Clone)]
pub struct DataHubApplicationService {
    sources: Arc<dyn DataSourceStorePort>,
    documents: Arc<dyn DocumentStorePort>,
    projects: Arc<dyn ProjectStorePort>,
    bridge: Arc<dyn DataHubBridgePort>,
    audit: Arc<dyn AuditSinkPort>,
    limits: DataHubLimits,
}

impl DataHubApplicationService {
    /// Wires Rust-owned metadata stores, the authenticated Python bridge and
    /// the mandatory audit sink.
    pub fn new(
        sources: Arc<dyn DataSourceStorePort>,
        documents: Arc<dyn DocumentStorePort>,
        projects: Arc<dyn ProjectStorePort>,
        bridge: Arc<dyn DataHubBridgePort>,
        audit: Arc<dyn AuditSinkPort>,
        limits: DataHubLimits,
    ) -> Result<Self> {
        Ok(Self {
            sources,
            documents,
            projects,
            bridge,
            audit,
            limits: limits.validate()?,
        })
    }

    /// Registers a project-scoped source using only non-secret configuration.
    pub async fn create_source(
        &self,
        ctx: &ServiceContext,
        request: &CreateDataSourceRequest,
    ) -> Result<DataSourceDto> {
        request.validate()?;
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let project = self
            .owned_project(tenant, organization, request.project_id)
            .await?;
        if project.status != ProjectStatus::Active {
            return Err(AppError::conflict(
                "archived projects cannot add data sources",
            ));
        }
        let creator = acting_user(ctx)?;
        if request.credential_id.is_some_and(|id| id.is_nil()) {
            return Err(AppError::invalid_field(
                "credential_id",
                "required",
                "credential metadata id must be concrete",
            ));
        }
        let kind = request.kind.parse::<DataSourceKind>()?;
        let source = DataSource::register(
            tenant,
            project.id,
            request.name.clone(),
            kind,
            request.config.clone(),
            request.credential_id,
            creator,
        )?;
        self.sources.save(&source).await?;
        self.audit(
            ctx,
            "data_source.register",
            "data_source",
            Some(source.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(source_dto(&source))
    }

    /// Lists sources only within the authenticated tenant and requested project.
    pub async fn list_sources(
        &self,
        ctx: &ServiceContext,
        project_id: ProjectId,
    ) -> Result<Vec<DataSourceDto>> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        self.owned_project(tenant, organization, project_id).await?;
        let mut sources = self.sources.list(tenant, project_id).await?;
        sources.sort_by(|left, right| {
            left.name
                .to_ascii_lowercase()
                .cmp(&right.name.to_ascii_lowercase())
                .then_with(|| left.id.cmp(&right.id))
        });
        sources.truncate(MAX_DATA_HUB_LIST_ITEMS);
        Ok(sources.iter().map(source_dto).collect())
    }

    /// Returns one source while hiding whether a foreign-tenant id exists.
    pub async fn get_source(
        &self,
        ctx: &ServiceContext,
        id: DataSourceId,
    ) -> Result<DataSourceDto> {
        let source = self.owned_source(ctx, id).await?;
        Ok(source_dto(&source))
    }

    /// Synchronizes an active, tenant-owned source through the Python bridge.
    /// The opaque cursor is committed only after a valid bridge response.
    pub async fn sync_source(
        &self,
        ctx: &ServiceContext,
        id: DataSourceId,
    ) -> Result<DataSourceSyncDto> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let mut source = self.owned_source(ctx, id).await?;
        let bridge_context = self.bridge_context(ctx, tenant, organization, source.project_id)?;
        let required_roles = access_labels(&source.config, "required_roles")?;
        let denied_principals = access_labels(&source.config, "denied_principals")?;
        let credential_secret_ref = source
            .config
            .get("credential_secret_ref")
            .and_then(Value::as_str)
            .map(str::to_owned);
        source.activate()?;
        source.begin_sync()?;
        self.sources.save(&source).await?;
        let bridge_request = DataHubSyncSourceRequest {
            context: bridge_context,
            source_id: *source.id.as_uuid(),
            source_name: source.name.clone(),
            source_kind: bridge_source_kind(source.kind),
            configuration: source.config.clone(),
            credential_secret_ref,
            cursor: source.sync_cursor.clone(),
            required_roles,
            denied_principals,
        };

        let reply = match self.bridge.sync_source(bridge_request).await {
            Ok(reply) if reply.source_id == *source.id.as_uuid() => reply,
            Ok(_) => {
                return self
                    .fail_sync(ctx, source, "invalid_bridge_response", "source sync failed")
                    .await;
            },
            Err(_) => {
                return self
                    .fail_sync(ctx, source, "bridge_error", "source sync failed")
                    .await;
            },
        };

        if source
            .complete_sync(reply.next_cursor.clone(), Timestamp::now())
            .is_err()
        {
            return self
                .fail_sync(ctx, source, "invalid_bridge_response", "source sync failed")
                .await;
        }
        self.sources.save(&source).await?;
        self.audit(
            ctx,
            "data_source.sync",
            "data_source",
            Some(source.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(DataSourceSyncDto {
            source_id: source.id,
            job_id: None,
            status: if reply.failed_count == 0 {
                "completed".to_owned()
            } else {
                "partial".to_owned()
            },
        })
    }

    /// Persists document metadata, validates the exact bytes, and indexes them
    /// through the authenticated bridge. Raw content never enters a DTO/store.
    pub async fn ingest_document(
        &self,
        ctx: &ServiceContext,
        request: &CreateDocumentRequest,
        content: Vec<u8>,
    ) -> Result<DocumentDto> {
        request.validate()?;
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        if u64::try_from(content.len()).unwrap_or(u64::MAX) > self.limits.max_upload_bytes {
            return Err(AppError::invalid_field(
                "content",
                "too_large",
                "document exceeds the configured upload limit",
            ));
        }
        let actual_size = u64::try_from(content.len()).map_err(|_| {
            AppError::invalid_field("content", "too_large", "document size is not representable")
        })?;
        if request.size_bytes != actual_size {
            return Err(AppError::invalid_field(
                "size_bytes",
                "size_mismatch",
                "declared document size does not match the uploaded bytes",
            ));
        }
        let digest = hex::encode(Sha256::digest(&content));
        if digest != request.content_sha256 {
            return Err(AppError::invalid_field(
                "content_sha256",
                "digest_mismatch",
                "uploaded content does not match the declared SHA-256 digest",
            ));
        }

        let source = self.owned_source(ctx, request.source_id).await?;
        if source.status == DataSourceStatus::Disabled {
            return Err(AppError::conflict(
                "disabled data sources cannot ingest documents",
            ));
        }
        let bridge_context = self.bridge_context(ctx, tenant, organization, source.project_id)?;
        let creator = acting_user(ctx)?;
        let mut document = Document::new(
            tenant,
            source.project_id,
            source.id,
            request.title.clone(),
            request.mime_type.clone(),
            actual_size,
            digest.clone(),
            creator,
            request.access_policy.clone(),
            request.metadata.clone(),
        )?;
        let required_roles = access_labels(&document.access_policy, "required_roles")?;
        let denied_principals = access_labels(&document.access_policy, "denied_principals")?;
        self.documents.save(&document).await?;
        document.begin_processing()?;
        self.documents.save(&document).await?;

        let bridge_request = DataHubIngestDocumentRequest {
            context: bridge_context,
            source_id: *source.id.as_uuid(),
            document_id: *document.id.as_uuid(),
            source_kind: bridge_source_kind(source.kind),
            title: document.title.clone(),
            mime_type: document.mime_type.clone(),
            content,
            source_uri: None,
            content_sha256: Some(digest),
            required_roles,
            denied_principals,
            metadata: Value::Object(document.metadata.clone()),
        };
        let reply = match self.bridge.ingest_document(bridge_request).await {
            Ok(reply) if valid_ingestion_reply(&reply, &document) => reply,
            Ok(_) => {
                document.mark_failed("invalid_bridge_response")?;
                self.documents.save(&document).await?;
                self.audit(
                    ctx,
                    "document.ingest",
                    "document",
                    Some(document.id.to_string()),
                    AuditOutcome::Failure,
                )
                .await?;
                return Err(AppError::external_service(
                    "data-hub",
                    "document ingestion failed",
                ));
            },
            Err(_) => {
                document.mark_failed("bridge_error")?;
                self.documents.save(&document).await?;
                self.audit(
                    ctx,
                    "document.ingest",
                    "document",
                    Some(document.id.to_string()),
                    AuditOutcome::Failure,
                )
                .await?;
                return Err(AppError::external_service(
                    "data-hub",
                    "document ingestion failed",
                ));
            },
        };

        document.mark_ready(reply.chunk_count)?;
        self.documents.save(&document).await?;
        self.audit(
            ctx,
            "document.ingest",
            "document",
            Some(document.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(document_dto(&document))
    }

    /// Lists metadata for one source after proving tenant, organization and
    /// project ownership. Deleted rows remain visible as tombstones.
    pub async fn list_documents(
        &self,
        ctx: &ServiceContext,
        source_id: DataSourceId,
    ) -> Result<Vec<DocumentDto>> {
        let source = self.owned_source(ctx, source_id).await?;
        let tenant = ctx.require_tenant()?;
        let mut documents = self.documents.list(tenant, source.id).await?;
        documents.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        documents.truncate(MAX_DATA_HUB_LIST_ITEMS);
        Ok(documents.iter().map(document_dto).collect())
    }

    /// Returns document metadata only; content and vectors are never exposed.
    pub async fn get_document(&self, ctx: &ServiceContext, id: DocumentId) -> Result<DocumentDto> {
        let document = self.owned_document(ctx, id).await?;
        Ok(document_dto(&document))
    }

    /// Tombstones a document before asking Python to purge its derived index.
    /// A retry repeats the idempotent purge if the previous RPC failed.
    pub async fn delete_document(&self, ctx: &ServiceContext, id: DocumentId) -> Result<()> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let mut document = self.owned_document(ctx, id).await?;
        let source = self.owned_source(ctx, document.source_id).await?;
        let bridge_context = self.bridge_context(ctx, tenant, organization, source.project_id)?;
        document.mark_deleted()?;
        self.documents.save(&document).await?;

        let purge = self
            .bridge
            .purge_document(DataHubPurgeDocumentRequest {
                context: bridge_context,
                source_id: *source.id.as_uuid(),
                document_id: *document.id.as_uuid(),
            })
            .await;
        match purge {
            Ok(reply) if reply.document_id == *document.id.as_uuid() => {
                self.audit(
                    ctx,
                    "document.delete",
                    "document",
                    Some(document.id.to_string()),
                    AuditOutcome::Success,
                )
                .await
            },
            _ => {
                self.audit(
                    ctx,
                    "document.delete",
                    "document",
                    Some(document.id.to_string()),
                    AuditOutcome::Failure,
                )
                .await?;
                Err(AppError::external_service(
                    "data-hub",
                    "document removal is pending; retry the request",
                ))
            },
        }
    }

    async fn owned_project(
        &self,
        tenant: TenantId,
        organization: mas_common::ids::OrganizationId,
        id: ProjectId,
    ) -> Result<Project> {
        self.projects
            .get(id)
            .await?
            .filter(|project| {
                project.tenant_id == tenant && project.organization_id == organization
            })
            .ok_or_else(|| AppError::not_found("project", id.to_string()))
    }

    async fn owned_source(&self, ctx: &ServiceContext, id: DataSourceId) -> Result<DataSource> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let source = self
            .sources
            .get(id)
            .await?
            .filter(|source| source.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("data source", id.to_string()))?;
        self.owned_project(tenant, organization, source.project_id)
            .await?;
        Ok(source)
    }

    async fn owned_document(&self, ctx: &ServiceContext, id: DocumentId) -> Result<Document> {
        let tenant = ctx.require_tenant()?;
        let document = self
            .documents
            .get(id)
            .await?
            .filter(|document| document.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("document", id.to_string()))?;
        let source = self.owned_source(ctx, document.source_id).await?;
        if source.project_id != document.project_id {
            return Err(AppError::not_found("document", id.to_string()));
        }
        Ok(document)
    }

    fn bridge_context(
        &self,
        ctx: &ServiceContext,
        tenant: TenantId,
        organization: mas_common::ids::OrganizationId,
        project: ProjectId,
    ) -> Result<DataHubCallContext> {
        let deadline = Timestamp::now()
            .checked_add(self.limits.bridge_timeout)
            .ok_or_else(|| AppError::validation("Data Hub deadline is out of range"))?;
        DataHubCallContext::new(
            tenant,
            organization,
            project,
            ctx.correlation_id.clone(),
            Some(ctx.request_id.to_string()),
            deadline,
        )
    }

    async fn fail_sync<T>(
        &self,
        ctx: &ServiceContext,
        mut source: DataSource,
        failure_code: &str,
        public_message: &str,
    ) -> Result<T> {
        source.fail_sync(failure_code)?;
        self.sources.save(&source).await?;
        self.audit(
            ctx,
            "data_source.sync",
            "data_source",
            Some(source.id.to_string()),
            AuditOutcome::Failure,
        )
        .await?;
        Err(AppError::external_service("data-hub", public_message))
    }

    async fn audit(
        &self,
        ctx: &ServiceContext,
        action: &str,
        resource_type: &str,
        resource_id: Option<String>,
        outcome: AuditOutcome,
    ) -> Result<()> {
        record_mutation(
            ctx,
            self.audit.as_ref(),
            action,
            resource_type,
            resource_id,
            outcome,
        )
        .await
    }
}

fn acting_user(ctx: &ServiceContext) -> Result<UserId> {
    if ctx.actor.kind != AuditActorKind::User {
        return Err(AppError::forbidden(
            "this Data Hub operation requires a user principal",
        ));
    }
    let user_id = UserId::parse_str(&ctx.actor.id)
        .map_err(|_| AppError::forbidden("the authenticated user id is invalid"))?;
    if user_id.is_nil() {
        return Err(AppError::forbidden("the authenticated user id is invalid"));
    }
    Ok(user_id)
}

fn bridge_source_kind(kind: DataSourceKind) -> DataHubSourceKind {
    match kind {
        DataSourceKind::Upload => DataHubSourceKind::Upload,
        DataSourceKind::ObjectStorage => DataHubSourceKind::ObjectStorage,
        DataSourceKind::Web => DataHubSourceKind::Web,
    }
}

fn access_labels(policy: &Value, field: &str) -> Result<Vec<String>> {
    let Some(value) = policy.get(field) else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or_else(|| {
        AppError::invalid_field(
            field,
            "invalid_format",
            "access labels must be supplied as an array of strings",
        )
    })?;
    if values.len() > 128 {
        return Err(AppError::invalid_field(
            field,
            "too_many",
            "at most 128 access labels are allowed",
        ));
    }
    let mut labels = BTreeSet::new();
    for value in values {
        let label = value.as_str().ok_or_else(|| {
            AppError::invalid_field(
                field,
                "invalid_format",
                "access labels must contain only strings",
            )
        })?;
        let label = label.trim();
        if label.is_empty() || label.len() > 128 || label.chars().any(char::is_control) {
            return Err(AppError::invalid_field(
                field,
                "invalid_format",
                "access labels must be non-empty, bounded, and contain no control characters",
            ));
        }
        labels.insert(label.to_owned());
    }
    Ok(labels.into_iter().collect())
}

fn source_dto(source: &DataSource) -> DataSourceDto {
    DataSourceDto {
        id: source.id,
        project_id: source.project_id,
        name: source.name.clone(),
        kind: source.kind.to_string(),
        status: source.status.to_string(),
        config: redact_reference_values(&source.config),
        has_credential: source.credential_id.is_some() || contains_reference(&source.config),
        last_synced_at: source.last_synced_at,
        last_failure_code: source.last_failure_code.clone(),
        updated_at: source.updated_at,
    }
}

fn document_dto(document: &Document) -> DocumentDto {
    DocumentDto {
        id: document.id,
        source_id: document.source_id,
        title: document.title.clone(),
        mime_type: document.mime_type.clone(),
        size_bytes: document.size_bytes,
        status: document.status.to_string(),
        chunk_count: document.chunk_count,
        failure_code: document.failure_code.clone(),
        created_at: document.created_at,
        updated_at: document.updated_at,
    }
}

fn valid_ingestion_reply(reply: &DataHubDocumentIngestionResult, document: &Document) -> bool {
    reply.document_id == *document.id.as_uuid()
        && reply.sha256.eq_ignore_ascii_case(&document.content_sha256)
        && reply.byte_count == document.size_bytes
        && reply.embedded_count == reply.chunk_count
        && reply.skipped_reason.is_none()
}

fn contains_reference(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| is_reference_key(key) || contains_reference(value)),
        Value::Array(items) => items.iter().any(contains_reference),
        _ => false,
    }
}

fn redact_reference_values(value: &Value) -> Value {
    match value {
        Value::Object(fields) => {
            let redacted = fields
                .iter()
                .map(|(key, child)| {
                    let value = if is_reference_key(key) {
                        Value::String("[configured]".to_owned())
                    } else {
                        redact_reference_values(child)
                    };
                    (key.clone(), value)
                })
                .collect::<Map<String, Value>>();
            Value::Object(redacted)
        },
        Value::Array(items) => Value::Array(items.iter().map(redact_reference_values).collect()),
        _ => value.clone(),
    }
}

fn is_reference_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace('-', "_");
    normalized.ends_with("_secret_ref")
        || normalized.ends_with("_credential_ref")
        || normalized == "secret_reference"
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_common::timestamps::Timestamp;
    use mas_contracts::data_hub::CreateDocumentRequest;
    use mas_domain::DocumentStatus;
    use mas_domain::{AuditActor, AuditEvent};
    use mas_execution::data_hub_bridge::{
        DataHubBridgePort, DataHubDocumentIngestionResult, DataHubIngestDocumentRequest,
        DataHubPurgeDocumentRequest, DataHubPurgeDocumentResult, DataHubSyncSourceRequest,
        DataHubSyncSourceResult,
    };
    use serde_json::Map;

    use super::*;
    use crate::stores::{InMemoryServices, ProjectStorePort};

    #[derive(Debug, Default)]
    struct FakeBridge {
        ingested: AtomicUsize,
    }

    #[async_trait]
    impl DataHubBridgePort for FakeBridge {
        fn name(&self) -> &'static str {
            "fake-data-hub"
        }

        async fn sync_source(
            &self,
            request: DataHubSyncSourceRequest,
        ) -> Result<DataHubSyncSourceResult> {
            Ok(DataHubSyncSourceResult {
                source_id: request.source_id,
                discovered_count: 0,
                changed_count: 0,
                indexed_count: 0,
                failed_count: 0,
                next_cursor: request.cursor,
                errors: Vec::new(),
            })
        }

        async fn ingest_document(
            &self,
            request: DataHubIngestDocumentRequest,
        ) -> Result<DataHubDocumentIngestionResult> {
            self.ingested.fetch_add(1, Ordering::SeqCst);
            Ok(DataHubDocumentIngestionResult {
                document_id: request.document_id,
                sha256: request.content_sha256.expect("digest"),
                byte_count: request.content.len() as u64,
                chunk_count: 2,
                embedded_count: 2,
                skipped_reason: None,
            })
        }

        async fn purge_document(
            &self,
            request: DataHubPurgeDocumentRequest,
        ) -> Result<DataHubPurgeDocumentResult> {
            Ok(DataHubPurgeDocumentResult {
                document_id: request.document_id,
                removed_chunks: 0,
            })
        }

        async fn health(&self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct TestAuditSink;

    #[async_trait]
    impl AuditSinkPort for TestAuditSink {
        async fn record(&self, _event: &AuditEvent) -> Result<()> {
            Ok(())
        }
    }

    fn context(tenant: TenantId, organization: OrganizationId, user: UserId) -> ServiceContext {
        ServiceContext::new(
            AuditActor::new(AuditActorKind::User, user.to_string()).expect("actor"),
            Some(tenant),
            Some(organization),
            "data-hub-test-correlation",
            &Timestamp::now(),
        )
        .expect("context")
    }

    async fn setup() -> (
        DataHubApplicationService,
        Arc<InMemoryServices>,
        Arc<FakeBridge>,
        ServiceContext,
        DataSourceId,
    ) {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let user = UserId::new();
        let project = Project::create(
            tenant,
            organization,
            "Research",
            "research".parse().expect("slug"),
        )
        .expect("project");
        let project_id = project.id;
        let store = Arc::new(InMemoryServices::new());
        ProjectStorePort::save(store.as_ref(), &project)
            .await
            .expect("save project");
        let bridge = Arc::new(FakeBridge::default());
        let service = DataHubApplicationService::new(
            store.clone(),
            store.clone(),
            store.clone(),
            bridge.clone(),
            Arc::new(TestAuditSink),
            DataHubLimits::default(),
        )
        .expect("service");
        let ctx = context(tenant, organization, user);
        let source = service
            .create_source(
                &ctx,
                &CreateDataSourceRequest {
                    project_id,
                    name: "Uploaded files".to_owned(),
                    kind: "upload".to_owned(),
                    config: serde_json::json!({}),
                    credential_id: None,
                },
            )
            .await
            .expect("source");
        (service, store, bridge, ctx, source.id)
    }

    #[tokio::test]
    async fn ingest_persists_only_metadata_and_checks_uploaded_digest() {
        let (service, store, bridge, ctx, source_id) = setup().await;
        let bytes = b"hello data hub".to_vec();
        let digest = hex::encode(Sha256::digest(&bytes));
        let request = CreateDocumentRequest {
            source_id,
            title: "Notes".to_owned(),
            mime_type: "text/plain".to_owned(),
            size_bytes: bytes.len() as u64,
            content_sha256: digest,
            access_policy: serde_json::json!({"required_roles": ["reader"]}),
            metadata: Map::new(),
        };
        let dto = service
            .ingest_document(&ctx, &request, bytes)
            .await
            .expect("ingest");
        assert_eq!(dto.status, "ready");
        assert_eq!(dto.chunk_count, 2);
        assert_eq!(bridge.ingested.load(Ordering::SeqCst), 1);
        let saved = DocumentStorePort::get(store.as_ref(), dto.id)
            .await
            .expect("get")
            .expect("saved");
        assert!(saved.metadata.is_empty());
        assert_eq!(saved.status, DocumentStatus::Ready);
    }

    #[tokio::test]
    async fn rejects_foreign_tenant_source_before_bridge_call() {
        let (service, _store, bridge, ctx, source_id) = setup().await;
        let foreign = context(TenantId::new(), OrganizationId::new(), UserId::new());
        let result = service.get_source(&foreign, source_id).await;
        assert!(matches!(result, Err(AppError::NotFound { .. })));
        assert_eq!(bridge.ingested.load(Ordering::SeqCst), 0);
        assert!(ctx.tenant_id.is_some());
    }
}
