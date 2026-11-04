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
/// Default matches the Python Data Hub upload and document byte ceilings.
pub const DEFAULT_MAX_UPLOAD_BYTES: u64 = 2 * 1024 * 1024;
/// Maximum allowed by the Python Data Hub settings schema.
pub const MAX_UPLOAD_BYTES: u64 = 8 * 1024 * 1024;
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
        if self.max_upload_bytes == 0 || self.max_upload_bytes > MAX_UPLOAD_BYTES {
            return Err(AppError::invalid_field(
                "max_upload_bytes",
                "out_of_range",
                "must be between 1 byte and 8 MiB",
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

    /// Returns the configured byte ceiling used by the HTTP upload adapter to
    /// reject oversized multipart fields before it retains them for bridge
    /// transport. The service re-checks the same bound before ingestion.
    #[must_use]
    pub const fn max_upload_bytes(&self) -> u64 {
        self.limits.max_upload_bytes
    }

    /// Registers a project-scoped source using only non-secret configuration.
    pub async fn create_source(
        &self,
        ctx: &ServiceContext,
        request: &CreateDataSourceRequest,
    ) -> Result<DataSourceDto> {
        request.validate()?;
        access_labels(&request.config, "required_roles")?;
        access_labels(&request.config, "denied_principals")?;
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
        if sources
            .iter()
            .any(|source| source.tenant_id != tenant || source.project_id != project_id)
        {
            return Err(AppError::database(
                "data source listing returned a row outside its scope",
            ));
        }
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
        // Legacy rows can predate strict registration checks. Do not sync
        // them with unknown or misspelled access-policy settings.
        mas_contracts::data_hub::validate_source_config(source.kind.as_ref(), &source.config)?;
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

        let counts_valid = reply.changed_count <= reply.discovered_count
            && reply.indexed_count <= reply.changed_count
            && reply.failed_count <= reply.changed_count - reply.indexed_count
            && (reply.failed_count != 0 || reply.indexed_count == reply.changed_count);
        if !counts_valid {
            return self
                .fail_sync(ctx, source, "invalid_bridge_response", "source sync failed")
                .await;
        }
        let partial = reply.failed_count != 0;
        let transition = if partial {
            source.fail_sync_with_cursor(reply.next_cursor, "partial_sync")
        } else {
            source.complete_sync(reply.next_cursor, Timestamp::now())
        };
        if transition.is_err() {
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
            if partial {
                AuditOutcome::Failure
            } else {
                AuditOutcome::Success
            },
        )
        .await?;
        Ok(DataSourceSyncDto {
            source_id: source.id,
            job_id: None,
            status: if partial { "partial" } else { "completed" }.to_owned(),
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
        mas_contracts::data_hub::validate_source_config(source.kind.as_ref(), &source.config)?;
        let (required_roles, denied_principals) =
            effective_access_labels(&source.config, &request.access_policy)?;
        let effective_policy = serde_json::json!({
            "required_roles": required_roles,
            "denied_principals": denied_principals,
        });
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
            effective_policy,
            request.metadata.clone(),
        )?;
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
        if documents.iter().any(|document| {
            document.tenant_id != tenant
                || document.source_id != source.id
                || document.project_id != source.project_id
        }) {
            return Err(AppError::database(
                "document listing returned a row outside its scope",
            ));
        }
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
                project.id == id
                    && project.tenant_id == tenant
                    && project.organization_id == organization
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
            .filter(|source| source.id == id && source.tenant_id == tenant)
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
            .filter(|document| document.id == id && document.tenant_id == tenant)
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

// Both source and upload policy apply. Our RAG policy encodes an OR over
// roles, not an AND of role groups. Intersection is conservative; disjoint
// nonempty groups are not expressible and must never become open access.
fn effective_access_labels(source: &Value, document: &Value) -> Result<(Vec<String>, Vec<String>)> {
    let source_roles: BTreeSet<_> = access_labels(source, "required_roles")?
        .into_iter()
        .collect();
    let document_roles: BTreeSet<_> = access_labels(document, "required_roles")?
        .into_iter()
        .collect();
    let roles = if source_roles.is_empty() {
        document_roles
    } else if document_roles.is_empty() {
        source_roles
    } else {
        let intersection: BTreeSet<_> = source_roles
            .intersection(&document_roles)
            .cloned()
            .collect();
        if intersection.is_empty() {
            return Err(AppError::invalid_field(
                "access_policy",
                "incompatible_roles",
                "source and document role restrictions cannot be combined safely",
            ));
        }
        intersection
    };
    let mut denied: BTreeSet<_> = access_labels(source, "denied_principals")?
        .into_iter()
        .collect();
    denied.extend(access_labels(document, "denied_principals")?);
    // Both inputs are bounded separately; the merged list is also subject to
    // the bridge's per-field bound.
    if denied.len() > 128 {
        return Err(AppError::invalid_field(
            "denied_principals",
            "too_many",
            "combined denial list exceeds 128 labels",
        ));
    }
    Ok((roles.into_iter().collect(), denied.into_iter().collect()))
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
        && reply.chunk_count > 0
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
                    let value = if is_reference_key(key)
                        || matches!(key.as_str(), "url" | "endpoint_url")
                    {
                        Value::String("[configured]".to_owned())
                    } else if key == "start_urls" {
                        match child {
                            Value::Array(urls) => Value::Array(
                                urls.iter()
                                    .map(|_| Value::String("[configured]".to_owned()))
                                    .collect(),
                            ),
                            _ => Value::String("[configured]".to_owned()),
                        }
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

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
        empty_reply: AtomicBool,
        sync_result: std::sync::Mutex<Option<DataHubSyncSourceResult>>,
        last_access: std::sync::Mutex<Option<(Vec<String>, Vec<String>)>>,
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
            if let Some(result) = self.sync_result.lock().expect("sync result mutex").clone() {
                return Ok(result);
            }
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
            *self.last_access.lock().expect("last access mutex") = Some((
                request.required_roles.clone(),
                request.denied_principals.clone(),
            ));
            let chunk_count = if self.empty_reply.load(Ordering::SeqCst) {
                0
            } else {
                2
            };
            Ok(DataHubDocumentIngestionResult {
                document_id: request.document_id,
                sha256: request.content_sha256.expect("digest"),
                byte_count: request.content.len() as u64,
                chunk_count,
                embedded_count: chunk_count,
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

    #[derive(Debug)]
    struct MisbehavingSourceStore(DataSource);

    #[async_trait]
    impl DataSourceStorePort for MisbehavingSourceStore {
        async fn save(&self, _source: &DataSource) -> Result<()> {
            unreachable!("read-only test adapter")
        }

        async fn get(&self, _id: DataSourceId) -> Result<Option<DataSource>> {
            Ok(Some(self.0.clone()))
        }

        async fn list(&self, _tenant: TenantId, _project: ProjectId) -> Result<Vec<DataSource>> {
            Ok(vec![self.0.clone()])
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
    async fn partial_sync_persists_retry_cursor_without_claiming_success() {
        let (mut service, store, bridge, ctx, source_id) = setup().await;
        let audit = Arc::new(crate::audit::InMemoryAuditSink::new());
        service.audit = audit.clone();
        let long_cursor = "a".repeat(5000);
        *bridge.sync_result.lock().expect("mutex") = Some(DataHubSyncSourceResult {
            source_id: *source_id.as_uuid(),
            discovered_count: 3,
            changed_count: 2,
            indexed_count: 1,
            failed_count: 1,
            next_cursor: Some(long_cursor.clone()),
            errors: vec![],
        });
        let response = service
            .sync_source(&ctx, source_id)
            .await
            .expect("partial response");
        assert_eq!(response.status, "partial");
        let source = DataSourceStorePort::get(store.as_ref(), source_id)
            .await
            .expect("read")
            .expect("source");
        assert_eq!(source.status, DataSourceStatus::Error);
        assert_eq!(source.sync_cursor.as_deref(), Some(long_cursor.as_str()));
        assert_eq!(source.last_failure_code.as_deref(), Some("partial_sync"));
        assert!(source.last_synced_at.is_none());
        assert_eq!(audit.events()[0].outcome, AuditOutcome::Failure);
        // Explicit retry can resume and records successful completion only then.
        *bridge.sync_result.lock().expect("mutex") = None;
        let result = service.sync_source(&ctx, source_id).await.expect("retry");
        assert_eq!(result.status, "completed");
        let source = DataSourceStorePort::get(store.as_ref(), source_id)
            .await
            .expect("read")
            .expect("source");
        assert_eq!(source.sync_cursor.as_deref(), Some(long_cursor.as_str()));
        assert!(source.last_synced_at.is_some());
        assert_eq!(source.status, DataSourceStatus::Active);
    }

    #[tokio::test]
    async fn impossible_bridge_counts_do_not_advance_source() {
        let (service, store, bridge, ctx, source_id) = setup().await;
        *bridge.sync_result.lock().expect("mutex") = Some(DataHubSyncSourceResult {
            source_id: *source_id.as_uuid(),
            discovered_count: 1,
            changed_count: 1,
            indexed_count: 0,
            failed_count: 0,
            next_cursor: Some("bad".into()),
            errors: vec![],
        });
        assert!(service.sync_source(&ctx, source_id).await.is_err());
        let source = DataSourceStorePort::get(store.as_ref(), source_id)
            .await
            .expect("read")
            .expect("source");
        assert_eq!(source.status, DataSourceStatus::Error);
        assert!(source.sync_cursor.is_none());
        assert_eq!(
            source.last_failure_code.as_deref(),
            Some("invalid_bridge_response")
        );
    }

    #[test]
    fn source_projection_never_returns_urls_or_secret_references() {
        let config = serde_json::json!({
            "url": "https://user:password@site.test/path?token=secret",
            "start_urls": ["https://site.test/private-token", "https://site.test/x"],
            "endpoint_url": "https://storage.test/secret-path",
            "access_key_secret_ref": "env:EXAMPLE_KEY",
            "max_pages": 4,
        });
        let projected = redact_reference_values(&config);
        let output = projected.to_string();
        for secret in [
            "password",
            "token=secret",
            "private-token",
            "secret-path",
            "EXAMPLE_KEY",
        ] {
            assert!(
                !output.contains(secret),
                "source projection leaked {secret}"
            );
        }
        assert_eq!(projected["start_urls"].as_array().expect("array").len(), 2);
        assert_eq!(projected["max_pages"], 4);
    }

    #[test]
    fn upload_limits_match_the_orchestrator_configuration_bounds() {
        assert_eq!(DataHubLimits::default().max_upload_bytes, 2 * 1024 * 1024);
        assert!(DataHubLimits {
            max_upload_bytes: MAX_UPLOAD_BYTES,
            ..DataHubLimits::default()
        }
        .validate()
        .is_ok());
        assert!(DataHubLimits {
            max_upload_bytes: MAX_UPLOAD_BYTES + 1,
            ..DataHubLimits::default()
        }
        .validate()
        .is_err());
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
    async fn upload_combines_source_and_document_access_before_indexing() {
        let (service, store, bridge, ctx, source_id) = setup().await;
        let mut source = DataSourceStorePort::get(store.as_ref(), source_id)
            .await
            .expect("read")
            .expect("source");
        source.config =
            serde_json::json!({"required_roles": ["admin", "reader"], "denied_principals": ["u1"]});
        DataSourceStorePort::save(store.as_ref(), &source)
            .await
            .expect("save source");
        let bytes = b"confidential".to_vec();
        let mut request = CreateDocumentRequest {
            source_id,
            title: "Secret".to_owned(),
            mime_type: "text/plain".to_owned(),
            size_bytes: bytes.len() as u64,
            content_sha256: hex::encode(Sha256::digest(&bytes)),
            access_policy: serde_json::json!({"required_roles": ["reader", "finance"], "denied_principals": ["u2"]}),
            metadata: Map::new(),
        };
        let saved = service
            .ingest_document(&ctx, &request, bytes.clone())
            .await
            .expect("ingest");
        let expected = (
            vec!["reader".to_owned()],
            vec!["u1".to_owned(), "u2".to_owned()],
        );
        assert_eq!(
            bridge.last_access.lock().expect("mutex").as_ref(),
            Some(&expected)
        );
        let doc = DocumentStorePort::get(store.as_ref(), saved.id)
            .await
            .expect("read")
            .expect("saved");
        assert_eq!(
            doc.access_policy,
            serde_json::json!({"required_roles": ["reader"], "denied_principals": ["u1", "u2"]})
        );
        request.access_policy = serde_json::json!({"required_roles": ["finance"]});
        assert!(service
            .ingest_document(&ctx, &request, bytes.clone())
            .await
            .is_err());
        assert_eq!(bridge.ingested.load(Ordering::SeqCst), 1);
        request.access_policy = serde_json::json!({});
        service
            .ingest_document(&ctx, &request, bytes)
            .await
            .expect("inherits source policy");
        assert_eq!(
            bridge.last_access.lock().expect("mutex").as_ref(),
            Some(&(
                vec!["admin".to_owned(), "reader".to_owned()],
                vec!["u1".to_owned()]
            ))
        );
    }

    #[tokio::test]
    async fn unknown_or_malformed_source_policy_cannot_be_saved_or_synced() {
        let (service, store, _bridge, ctx, source_id) = setup().await;
        let source = DataSourceStorePort::get(store.as_ref(), source_id)
            .await
            .expect("read")
            .expect("source");
        for config in [
            serde_json::json!({"roles": ["admin"]}),
            serde_json::json!({"required_role": ["admin"]}),
            serde_json::json!({"required_roles": "admin"}),
        ] {
            let request = CreateDataSourceRequest {
                project_id: source.project_id,
                name: "invalid".to_owned(),
                kind: "upload".to_owned(),
                config: config.clone(),
                credential_id: None,
            };
            assert!(service.create_source(&ctx, &request).await.is_err());
            let mut legacy = source.clone();
            legacy.config = config;
            DataSourceStorePort::save(store.as_ref(), &legacy)
                .await
                .expect("legacy row");
            assert!(service.sync_source(&ctx, source_id).await.is_err());
        }
    }

    #[tokio::test]
    async fn empty_upload_is_rejected_before_persistence_or_bridge() {
        let (service, store, bridge, ctx, source_id) = setup().await;
        let request = CreateDocumentRequest {
            source_id,
            title: "Empty".to_owned(),
            mime_type: "text/plain".to_owned(),
            size_bytes: 0,
            content_sha256: hex::encode(Sha256::digest([])),
            access_policy: serde_json::json!({}),
            metadata: Map::new(),
        };
        assert!(service
            .ingest_document(&ctx, &request, vec![])
            .await
            .is_err());
        assert_eq!(bridge.ingested.load(Ordering::SeqCst), 0);
        assert!(DocumentStorePort::list(
            store.as_ref(),
            ctx.require_tenant().expect("tenant"),
            source_id
        )
        .await
        .expect("documents")
        .is_empty());
    }

    #[tokio::test]
    async fn unsupported_access_policy_fields_never_reach_storage_or_index() {
        let (service, store, bridge, ctx, source_id) = setup().await;
        let bytes = b"sensitive content".to_vec();
        for policy in [
            serde_json::json!({"roles": ["admin"]}),
            serde_json::json!({"required_role": ["admin"]}),
            serde_json::json!({"required_roles": ["admin"], "deny": ["user"]}),
        ] {
            let request = CreateDocumentRequest {
                source_id,
                title: "Notes".to_owned(),
                mime_type: "text/plain".to_owned(),
                size_bytes: bytes.len() as u64,
                content_sha256: hex::encode(Sha256::digest(&bytes)),
                access_policy: policy,
                metadata: Map::new(),
            };
            assert!(service
                .ingest_document(&ctx, &request, bytes.clone())
                .await
                .is_err());
        }
        assert_eq!(bridge.ingested.load(Ordering::SeqCst), 0);
        assert!(DocumentStorePort::list(
            store.as_ref(),
            ctx.require_tenant().expect("tenant"),
            source_id
        )
        .await
        .expect("documents")
        .is_empty());
    }

    #[tokio::test]
    async fn empty_bridge_reply_records_failure_instead_of_a_ready_document() {
        let (service, _store, bridge, ctx, source_id) = setup().await;
        bridge.empty_reply.store(true, Ordering::SeqCst);
        let bytes = b"content".to_vec();
        let request = CreateDocumentRequest {
            source_id,
            title: "Notes".to_owned(),
            mime_type: "text/plain".to_owned(),
            size_bytes: bytes.len() as u64,
            content_sha256: hex::encode(Sha256::digest(&bytes)),
            access_policy: serde_json::json!({}),
            metadata: Map::new(),
        };
        assert!(matches!(
            service.ingest_document(&ctx, &request, bytes).await,
            Err(AppError::ExternalService { .. })
        ));
        let documents = service.list_documents(&ctx, source_id).await.expect("list");
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].status, "failed");
        assert_eq!(
            documents[0].failure_code.as_deref(),
            Some("invalid_bridge_response")
        );
    }

    #[tokio::test]
    async fn stale_ingestion_cannot_revive_a_deleted_document_in_memory() {
        let (service, store, _bridge, ctx, source_id) = setup().await;
        let bytes = b"content".to_vec();
        let request = CreateDocumentRequest {
            source_id,
            title: "Notes".to_owned(),
            mime_type: "text/plain".to_owned(),
            size_bytes: bytes.len() as u64,
            content_sha256: hex::encode(Sha256::digest(&bytes)),
            access_policy: serde_json::json!({}),
            metadata: Map::new(),
        };
        let dto = service
            .ingest_document(&ctx, &request, bytes)
            .await
            .expect("ingest");
        let stale = DocumentStorePort::get(store.as_ref(), dto.id)
            .await
            .expect("get")
            .expect("document");
        service.delete_document(&ctx, dto.id).await.expect("delete");
        assert!(DocumentStorePort::save(store.as_ref(), &stale)
            .await
            .is_err());
        service
            .delete_document(&ctx, dto.id)
            .await
            .expect("retry delete");
        let current = DocumentStorePort::get(store.as_ref(), dto.id)
            .await
            .expect("get")
            .expect("document");
        assert_eq!(current.status, DocumentStatus::Deleted);
    }

    #[tokio::test]
    async fn rejects_source_store_rows_outside_requested_scope() {
        let (mut service, store, _bridge, ctx, source_id) = setup().await;
        let mut source = DataSourceStorePort::get(store.as_ref(), source_id)
            .await
            .expect("get source")
            .expect("source");
        let project_id = source.project_id;
        source.project_id = ProjectId::new();
        service.sources = Arc::new(MisbehavingSourceStore(source.clone()));
        assert!(matches!(
            service.list_sources(&ctx, project_id).await,
            Err(AppError::Database { .. })
        ));
        source.project_id = project_id;
        service.sources = Arc::new(MisbehavingSourceStore(source));
        assert!(matches!(
            service.get_source(&ctx, DataSourceId::new()).await,
            Err(AppError::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_inconsistent_document_project_in_source_listing() {
        let (service, store, _bridge, ctx, source_id) = setup().await;
        let tenant = ctx.tenant_id.expect("tenant");
        let document = Document::new(
            tenant,
            ProjectId::new(),
            source_id,
            "foreign project document",
            "text/plain",
            4,
            hex::encode(Sha256::digest(b"data")),
            UserId::new(),
            serde_json::json!({}),
            Map::new(),
        )
        .expect("document");
        DocumentStorePort::save(store.as_ref(), &document)
            .await
            .expect("seed inconsistent row");
        assert!(matches!(
            service.get_document(&ctx, document.id).await,
            Err(AppError::NotFound { .. })
        ));
        assert!(matches!(
            service.list_documents(&ctx, source_id).await,
            Err(AppError::Database { .. })
        ));
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
