//! Rust-owned Data Hub metadata contracts; bytes and vectors stay in their plane.

use mas_common::ids::{CredentialId, DataSourceId, DocumentId, ProjectId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Register a tenant-scoped source with non-secret adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDataSourceRequest {
    /// Owning project.
    pub project_id: ProjectId,
    /// Display name.
    pub name: String,
    /// `upload`, `object_storage`, or `web`.
    pub kind: String,
    /// Non-secret adapter settings only.
    #[serde(default)]
    pub config: Value,
    /// Existing credential metadata id, never a secret value.
    #[serde(default)]
    pub credential_id: Option<CredentialId>,
}

impl CreateDataSourceRequest {
    /// Checks required fields and rejects secret-bearing configuration.
    pub fn validate(&self) -> Result<()> {
        validation::validate_length("name", &self.name, 1, 200)?;
        if self.name.trim().is_empty() {
            return Err(mas_common::error::AppError::invalid_field(
                "name",
                "required",
                "source name must not be blank",
            ));
        }
        match self.kind.as_str() {
            "upload" | "object_storage" | "web" => {},
            other => {
                return Err(mas_common::error::AppError::invalid_field(
                    "kind",
                    "invalid_enum_value",
                    format!("unsupported data source kind '{other}'"),
                ));
            },
        }
        if !self.config.is_object() {
            return Err(mas_common::error::AppError::invalid_field(
                "config",
                "not_object",
                "source config must be an object",
            ));
        }
        reject_secrets(&self.config)?;
        Ok(())
    }
}

fn reject_secrets(value: &Value) -> Result<()> {
    match value {
        Value::Object(fields) => {
            for (key, child) in fields {
                let normalized = key.to_ascii_lowercase().replace('-', "_");
                let reference = normalized.ends_with("_secret_ref")
                    || normalized.ends_with("_credential_ref")
                    || normalized == "secret_reference";
                let secretish = [
                    "password",
                    "secret",
                    "token",
                    "api_key",
                    "apikey",
                    "access_key",
                    "client_key",
                    "private_key",
                    "authorization",
                    "credential",
                ]
                .iter()
                .any(|needle| normalized.contains(needle));
                if secretish && !reference && !child.is_null() {
                    return Err(mas_common::error::AppError::invalid_field(
                        "config",
                        "secret_in_config",
                        format!("config field '{key}' may contain secret material"),
                    ));
                }
                reject_secrets(child)?;
            }
        },
        Value::Array(items) => {
            for item in items {
                reject_secrets(item)?;
            }
        },
        _ => {},
    }
    Ok(())
}

/// Begin ingestion of one document; upload content is transported separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDocumentRequest {
    /// Source whose adapter owns this content.
    pub source_id: DataSourceId,
    /// Display title.
    pub title: String,
    /// Exact MIME type of the bytes.
    pub mime_type: String,
    /// Byte count before upload.
    pub size_bytes: u64,
    /// Lowercase SHA-256 of the exact bytes.
    pub content_sha256: String,
    /// Retrieval filters.
    #[serde(default)]
    pub access_policy: Value,
    /// Non-secret searchable metadata.
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl CreateDocumentRequest {
    /// Validates metadata before a separately bounded upload begins.
    pub fn validate(&self) -> Result<()> {
        validation::validate_length("title", &self.title, 1, 512)?;
        validation::validate_length("mime_type", &self.mime_type, 1, 128)?;
        if self.title.trim().is_empty() || !self.access_policy.is_object() {
            return Err(mas_common::error::AppError::invalid_field(
                "document",
                "invalid_format",
                "title and object-shaped access policy are required",
            ));
        }
        if self.content_sha256.len() != 64
            || !self
                .content_sha256
                .chars()
                .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
        {
            return Err(mas_common::error::AppError::invalid_field(
                "content_sha256",
                "invalid_format",
                "content hash must be lowercase SHA-256 hex",
            ));
        }
        reject_secrets(&self.access_policy)?;
        reject_secrets(&Value::Object(self.metadata.clone()))?;
        Ok(())
    }
}

/// Safe data-source projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceDto {
    /// Source id.
    pub id: DataSourceId,
    /// Owning project.
    pub project_id: ProjectId,
    /// Display name.
    pub name: String,
    /// Adapter kind.
    pub kind: String,
    /// Lifecycle state.
    pub status: String,
    /// Non-secret settings.
    pub config: Value,
    /// Whether an external credential is linked.
    pub has_credential: bool,
    /// Last successful sync timestamp.
    pub last_synced_at: Option<Timestamp>,
    /// Stable failure code only.
    pub last_failure_code: Option<String>,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// Safe document projection. No content bytes or vectors are included.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentDto {
    /// Document id.
    pub id: DocumentId,
    /// Source id.
    pub source_id: DataSourceId,
    /// Title.
    pub title: String,
    /// MIME type.
    pub mime_type: String,
    /// Content size in bytes.
    pub size_bytes: u64,
    /// Ingestion status.
    pub status: String,
    /// Indexed chunk count.
    pub chunk_count: u32,
    /// Stable ingestion failure code.
    pub failure_code: Option<String>,
    /// Creation timestamp.
    pub created_at: Timestamp,
    /// Last update timestamp.
    pub updated_at: Timestamp,
}

/// Result of asking the Python bridge to sync a source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceSyncDto {
    /// Source id.
    pub source_id: DataSourceId,
    /// Bridge-side job identifier, if asynchronous.
    pub job_id: Option<String>,
    /// Accepted status.
    pub status: String,
}
