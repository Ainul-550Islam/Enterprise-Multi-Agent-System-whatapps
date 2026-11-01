//! PURPOSE: Rust-owned Data Hub document metadata and ingestion lifecycle.
//!
//! Document bytes are processed over the authenticated Rust-to-Python bridge;
//! vector chunks and embeddings are owned only by the isolated Python RAG
//! plane. This aggregate holds metadata and never contains vector data.

use mas_common::error::AppError;
use mas_common::ids::{DataSourceId, DocumentId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

string_enum! {
    /// Document indexing lifecycle.
    DocumentStatus {
        /// Created but not yet sent to the ingestion plane.
        Pending => "pending",
        /// Python is parsing, chunking or embedding the document.
        Processing => "processing",
        /// All produced chunks are available for authorized retrieval.
        Ready => "ready",
        /// The last ingestion attempt failed with a stable failure code.
        Failed => "failed",
        /// Tombstoned; derived vectors have been purged or are being purged.
        Deleted => "deleted",
    }
}

/// Rust-owned metadata for one source document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub source_id: DataSourceId,
    pub title: String,
    pub mime_type: String,
    pub size_bytes: u64,
    /// Lowercase SHA-256 digest of the exact content bytes.
    pub content_sha256: String,
    pub status: DocumentStatus,
    pub chunk_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    /// Retrieval access policy; contains role/principal labels, never secrets.
    #[serde(default)]
    pub access_policy: Value,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    pub created_by: UserId,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Document {
    /// Registers metadata for a pending document before bridge ingestion.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant_id: TenantId,
        project_id: ProjectId,
        source_id: DataSourceId,
        title: impl Into<String>,
        mime_type: impl Into<String>,
        size_bytes: u64,
        content_sha256: impl Into<String>,
        created_by: UserId,
        access_policy: Value,
        metadata: Map<String, Value>,
    ) -> Result<Self> {
        if tenant_id.is_nil() || project_id.is_nil() || source_id.is_nil() || created_by.is_nil() {
            return Err(AppError::invalid_field(
                "document",
                "required",
                "tenant, project, source and creator identifiers must be concrete",
            ));
        }
        let title = title.into();
        let mime_type = mime_type.into();
        let content_sha256 = content_sha256.into();
        validate_title_and_mime(&title, &mime_type)?;
        validate_digest(&content_sha256)?;
        if !access_policy.is_object() {
            return Err(AppError::invalid_field(
                "access_policy",
                "not_object",
                "access policy must be a JSON object",
            ));
        }
        let now = Timestamp::now();
        Ok(Self {
            id: DocumentId::new(),
            tenant_id,
            project_id,
            source_id,
            title,
            mime_type,
            size_bytes,
            content_sha256,
            status: DocumentStatus::Pending,
            chunk_count: 0,
            failure_code: None,
            access_policy,
            metadata,
            created_by,
            created_at: now,
            updated_at: now,
        })
    }

    /// Revalidates persisted invariants after hydration.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_nil()
            || self.tenant_id.is_nil()
            || self.project_id.is_nil()
            || self.source_id.is_nil()
            || self.created_by.is_nil()
        {
            return Err(AppError::database("document contains a nil identifier"));
        }
        validate_title_and_mime(&self.title, &self.mime_type)
            .map_err(|_| AppError::database("document title or MIME type is invalid"))?;
        validate_digest(&self.content_sha256)
            .map_err(|_| AppError::database("document content digest is invalid"))?;
        if !self.access_policy.is_object() {
            return Err(AppError::database(
                "document access policy is not an object",
            ));
        }
        if self.status == DocumentStatus::Ready && self.failure_code.is_some() {
            return Err(AppError::database(
                "ready document cannot retain a failure code",
            ));
        }
        if self.status == DocumentStatus::Failed && self.failure_code.is_none() {
            return Err(AppError::database(
                "failed document is missing its stable failure code",
            ));
        }
        Ok(())
    }

    /// Pending|Failed → Processing.
    pub fn begin_processing(&mut self) -> Result<()> {
        match self.status {
            DocumentStatus::Pending | DocumentStatus::Failed => {
                self.status = DocumentStatus::Processing;
                self.failure_code = None;
                self.touch();
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "document in status '{other}' cannot begin processing"
            ))),
        }
    }

    /// Processing → Ready after the RAG plane confirms indexing.
    pub fn mark_ready(&mut self, chunk_count: u32) -> Result<()> {
        self.require_processing("complete")?;
        self.chunk_count = chunk_count;
        self.failure_code = None;
        self.status = DocumentStatus::Ready;
        self.touch();
        Ok(())
    }

    /// Pending|Processing → Failed with a stable code, never provider text.
    pub fn mark_failed(&mut self, failure_code: impl Into<String>) -> Result<()> {
        if !matches!(
            self.status,
            DocumentStatus::Pending | DocumentStatus::Processing
        ) {
            return Err(AppError::conflict(format!(
                "document in status '{}' cannot fail ingestion",
                self.status
            )));
        }
        let failure_code = failure_code.into();
        validate_failure_code(&failure_code)?;
        self.failure_code = Some(failure_code);
        self.status = DocumentStatus::Failed;
        self.touch();
        Ok(())
    }

    /// Tombstones a document before the application asks Python to purge its
    /// derived chunks. Repeated deletes are idempotent.
    pub fn mark_deleted(&mut self) -> Result<()> {
        if self.status == DocumentStatus::Deleted {
            return Ok(());
        }
        self.status = DocumentStatus::Deleted;
        self.failure_code = None;
        self.touch();
        Ok(())
    }

    /// Replaces the tenant-approved retrieval labels before indexing.
    pub fn set_access_policy(&mut self, access_policy: Value) -> Result<()> {
        if matches!(
            self.status,
            DocumentStatus::Processing | DocumentStatus::Ready | DocumentStatus::Deleted
        ) {
            return Err(AppError::conflict(
                "access policy can only be changed before indexing or after retrying a failed document",
            ));
        }
        if !access_policy.is_object() {
            return Err(AppError::invalid_field(
                "access_policy",
                "not_object",
                "access policy must be a JSON object",
            ));
        }
        self.access_policy = access_policy;
        self.touch();
        Ok(())
    }

    fn require_processing(&self, action: &str) -> Result<()> {
        if self.status != DocumentStatus::Processing {
            return Err(AppError::conflict(format!(
                "cannot {action} a document while status is '{}'",
                self.status
            )));
        }
        Ok(())
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}

fn validate_title_and_mime(title: &str, mime_type: &str) -> Result<()> {
    validation::validate_length("title", title, 1, 512)?;
    validation::validate_length("mime_type", mime_type, 1, 255)?;
    if title.trim().is_empty() || mime_type.trim().is_empty() {
        return Err(AppError::invalid_field(
            "document",
            "required",
            "title and MIME type must not be empty",
        ));
    }
    if title.chars().any(char::is_control) || mime_type.chars().any(char::is_control) {
        return Err(AppError::invalid_field(
            "document",
            "invalid_format",
            "title and MIME type must not contain control characters",
        ));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AppError::invalid_field(
            "content_sha256",
            "invalid_format",
            "content SHA-256 must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_failure_code(code: &str) -> Result<()> {
    validation::validate_length("failure_code", code, 1, 128)?;
    if !code
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return Err(AppError::invalid_field(
            "failure_code",
            "invalid_format",
            "failure code must be a stable identifier, not provider text",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document() -> Document {
        Document::new(
            TenantId::new(),
            ProjectId::new(),
            DataSourceId::new(),
            "Architecture guide",
            "text/markdown",
            42,
            "a".repeat(64),
            UserId::new(),
            serde_json::json!({"required_roles": ["reader"]}),
            Map::new(),
        )
        .expect("document")
    }

    #[test]
    fn document_processing_lifecycle_records_chunk_count() {
        let mut document = document();
        document.begin_processing().expect("begin");
        document.mark_ready(4).expect("ready");
        assert_eq!(document.status, DocumentStatus::Ready);
        assert_eq!(document.chunk_count, 4);
        assert!(document.failure_code.is_none());
        document.validate().expect("valid row");
    }

    #[test]
    fn failure_codes_are_stable_identifiers_not_raw_error_text() {
        let mut document = document();
        let error = document.mark_failed("provider timed out with key sk-live");
        assert!(matches!(error, Err(AppError::Validation { .. })));
    }

    #[test]
    fn deleted_document_is_terminal_and_idempotent() {
        let mut document = document();
        document.mark_deleted().expect("delete");
        document.mark_deleted().expect("repeat delete");
        assert!(document.begin_processing().is_err());
        assert_eq!(document.status, DocumentStatus::Deleted);
    }
}
