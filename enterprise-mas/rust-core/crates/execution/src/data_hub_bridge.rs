//! PURPOSE: Typed port for Rust-owned Data Hub operations delegated to Python.
//!
//! Owning layer: `mas-execution`.
//!
//! Rust remains authoritative for tenant scope, source/document metadata,
//! authorization and audit. This port carries only a validated snapshot to the
//! Python ingestion/RAG plane; it does not grant Python SQL access to any
//! Rust-owned table. Production transport lives in `mas-ai-bridge`.

use std::fmt;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_orchestration::cancellation::CancellationToken;
use serde_json::Value;
use uuid::Uuid;

/// The source adapters implemented by the Python ingestion plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataHubSourceKind {
    /// A file uploaded through the Rust API and placed in authorized storage.
    Upload,
    /// An S3-compatible object storage source.
    ObjectStorage,
    /// A bounded, allow-listed web crawler.
    Web,
}

impl DataHubSourceKind {
    /// Stable protobuf/configuration representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Upload => "upload",
            Self::ObjectStorage => "object_storage",
            Self::Web => "web",
        }
    }
}

impl fmt::Display for DataHubSourceKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Authoritative scope and execution bounds shared by every Data Hub RPC.
#[derive(Debug, Clone)]
pub struct DataHubCallContext {
    /// Tenant proven by Rust's request-scope resolver.
    pub tenant_id: TenantId,
    /// Organization proven by Rust's request-scope resolver.
    pub organization_id: OrganizationId,
    /// Project that owns the source/document.
    pub project_id: ProjectId,
    /// End-to-end correlation id.
    pub correlation_id: String,
    /// Platform request id, when one exists.
    pub request_id: Option<String>,
    /// Absolute RPC and ingestion deadline.
    pub deadline: Timestamp,
    /// Cooperative cancellation propagated from the owning execution/request.
    pub cancellation: CancellationToken,
}

impl DataHubCallContext {
    /// Creates a trusted bridge context with a fresh cooperative cancellation token.
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        correlation_id: impl Into<String>,
        request_id: Option<String>,
        deadline: Timestamp,
    ) -> Result<Self> {
        let context = Self {
            tenant_id,
            organization_id,
            project_id,
            correlation_id: correlation_id.into(),
            request_id,
            deadline,
            cancellation: CancellationToken::new(),
        };
        context.require_authoritative_scope()?;
        Ok(context)
    }

    /// Rejects unscoped or malformed calls before they cross the trust boundary.
    pub fn require_authoritative_scope(&self) -> Result<()> {
        if self.tenant_id.is_nil() || self.organization_id.is_nil() || self.project_id.is_nil() {
            return Err(AppError::validation(
                "Data Hub request is missing authoritative tenant, organization, or project scope",
            ));
        }
        if self.correlation_id.trim().is_empty() {
            return Err(AppError::validation(
                "Data Hub request is missing its correlation id",
            ));
        }
        Ok(())
    }
}

/// Request to enumerate and ingest source changes.
#[derive(Debug, Clone)]
pub struct DataHubSyncSourceRequest {
    /// Trusted request scope and cancellation/deadline.
    pub context: DataHubCallContext,
    /// Rust-owned source identifier.
    pub source_id: Uuid,
    /// Display name, not an authorization input.
    pub source_name: String,
    /// Selected adapter kind.
    pub source_kind: DataHubSourceKind,
    /// Non-secret connector configuration. Secret material is forbidden;
    /// credentials cross this boundary only as a secret-manager reference.
    pub configuration: Value,
    /// Secret reference resolved by the Python secret resolver, never a secret.
    pub credential_secret_ref: Option<String>,
    /// Opaque cursor persisted by Rust between syncs.
    pub cursor: Option<String>,
    /// Roles required to retrieve indexed chunks.
    pub required_roles: Vec<String>,
    /// Principals explicitly denied access to indexed chunks.
    pub denied_principals: Vec<String>,
}

/// Aggregate result for one bounded source sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataHubSyncSourceResult {
    /// Rust-owned source identifier echoed by Python.
    pub source_id: Uuid,
    /// Number of source entries observed.
    pub discovered_count: u32,
    /// Number of changed entries.
    pub changed_count: u32,
    /// Number of successfully indexed documents.
    pub indexed_count: u32,
    /// Number of per-document failures.
    pub failed_count: u32,
    /// Opaque cursor to persist with the Rust source record.
    pub next_cursor: Option<String>,
    /// Sanitized document-id/error-code summaries only.
    pub errors: Vec<String>,
}

/// Request to ingest bytes from the already-authorized upload/source path.
#[derive(Clone)]
pub struct DataHubIngestDocumentRequest {
    /// Trusted request scope and cancellation/deadline.
    pub context: DataHubCallContext,
    /// Rust-owned source identifier.
    pub source_id: Uuid,
    /// Rust-owned document identifier.
    pub document_id: Uuid,
    /// Selected adapter kind.
    pub source_kind: DataHubSourceKind,
    /// Display title.
    pub title: String,
    /// Validated MIME type.
    pub mime_type: String,
    /// Content read by the Rust-authorized storage path.
    pub content: Vec<u8>,
    /// Optional source URI after SSRF and authorization validation.
    pub source_uri: Option<String>,
    /// Expected digest if Rust has already computed one.
    pub content_sha256: Option<String>,
    /// Roles required to retrieve indexed chunks.
    pub required_roles: Vec<String>,
    /// Principals explicitly denied access to indexed chunks.
    pub denied_principals: Vec<String>,
    /// Non-secret document metadata.
    pub metadata: Value,
}

impl fmt::Debug for DataHubIngestDocumentRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DataHubIngestDocumentRequest")
            .field("context", &self.context)
            .field("source_id", &self.source_id)
            .field("document_id", &self.document_id)
            .field("source_kind", &self.source_kind)
            .field("title", &self.title)
            .field("mime_type", &self.mime_type)
            .field(
                "content",
                &format_args!("[REDACTED; {} bytes]", self.content.len()),
            )
            .field(
                "source_uri",
                &self.source_uri.as_ref().map(|_| "[REDACTED]"),
            )
            .field("content_sha256", &self.content_sha256)
            .field("required_roles", &self.required_roles)
            .field("denied_principals", &self.denied_principals)
            .field("metadata", &"[REDACTED]")
            .finish()
    }
}

/// A completed or intentionally skipped document ingestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataHubDocumentIngestionResult {
    /// Rust-owned document identifier echoed by Python.
    pub document_id: Uuid,
    /// SHA-256 digest of the ingested bytes.
    pub sha256: String,
    /// Exact byte count observed by Python.
    pub byte_count: u64,
    /// Number of chunks produced.
    pub chunk_count: u32,
    /// Number of vectors embedded and written.
    pub embedded_count: u32,
    /// Safe skip reason, when the source item was intentionally ignored.
    pub skipped_reason: Option<String>,
}

/// Request to purge derived chunks for a deleted Rust-owned document.
#[derive(Debug, Clone)]
pub struct DataHubPurgeDocumentRequest {
    /// Trusted request scope and cancellation/deadline.
    pub context: DataHubCallContext,
    /// Rust-owned source identifier.
    pub source_id: Uuid,
    /// Rust-owned document identifier.
    pub document_id: Uuid,
}

/// Purge acknowledgement from the isolated Python RAG plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataHubPurgeDocumentResult {
    /// Rust-owned document identifier echoed by Python.
    pub document_id: Uuid,
    /// Number of derived chunk rows removed.
    pub removed_chunks: u64,
}

/// Port for the Rust-to-Python Data Hub bridge.
///
/// Implementations must preserve the resolved identity, bind authentication to
/// the exact encoded body, enforce the request deadline, observe cancellation,
/// and map transport failures through the same canonical error taxonomy as the
/// AgentBridge adapter.
#[async_trait]
pub trait DataHubBridgePort: Send + Sync + fmt::Debug {
    /// Stable adapter name for health, logs and metrics.
    fn name(&self) -> &'static str;

    /// Lists and processes bounded source changes.
    async fn sync_source(
        &self,
        request: DataHubSyncSourceRequest,
    ) -> Result<DataHubSyncSourceResult>;

    /// Ingests one authorized document payload.
    async fn ingest_document(
        &self,
        request: DataHubIngestDocumentRequest,
    ) -> Result<DataHubDocumentIngestionResult>;

    /// Removes only the derived vectors/chunks for one scoped document.
    async fn purge_document(
        &self,
        request: DataHubPurgeDocumentRequest,
    ) -> Result<DataHubPurgeDocumentResult>;

    /// Readiness of the Python Data Hub ingestion plane.
    async fn health(&self) -> Result<()>;
}
