//! Rust-owned Data Hub metadata contracts; bytes and vectors stay in their plane.

use mas_common::ids::{CredentialId, DataSourceId, DocumentId, ProjectId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Register a tenant-scoped source with non-secret adapter configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
        validate_source_config(&self.kind, &self.config)?;
        reject_secrets(&self.config)?;
        Ok(())
    }
}

/// Rejects unsupported source configuration, including misspelled policy
/// keys, rather than silently ingesting content with an unintended open ACL.
pub fn validate_source_config(kind: &str, config: &Value) -> Result<()> {
    if !matches!(kind, "upload" | "web" | "object_storage") {
        return Err(mas_common::error::AppError::invalid_field(
            "kind",
            "invalid_enum_value",
            "unsupported data source kind",
        ));
    }
    let fields = config.as_object().ok_or_else(|| {
        mas_common::error::AppError::invalid_field(
            "config",
            "not_object",
            "source config must be an object",
        )
    })?;
    for key in fields.keys() {
        let common = matches!(
            key.as_str(),
            "required_roles" | "denied_principals" | "credential_secret_ref"
        );
        let supported = match kind {
            "upload" => false,
            "web" => matches!(
                key.as_str(),
                "url" | "start_urls" | "max_depth" | "max_pages"
            ),
            "object_storage" => matches!(
                key.as_str(),
                "endpoint_url"
                    | "bucket"
                    | "prefix"
                    | "region"
                    | "access_key_secret_ref"
                    | "secret_key_secret_ref"
            ),
            _ => {
                return Err(mas_common::error::AppError::invalid_field(
                    "kind",
                    "invalid_enum_value",
                    "unsupported data source kind",
                ))
            },
        };
        if !common && !supported {
            return Err(mas_common::error::AppError::invalid_field(
                "config",
                "unknown_field",
                "source config contains an unsupported field",
            ));
        }
    }
    // URL components can conceal inline tokens even though the surrounding
    // config key is innocuous. Source URLs must not carry credentials, query
    // parameters, or fragments; secret values belong in approved references.
    let url_fields: &[&str] = match kind {
        "web" => &["url", "start_urls"],
        "object_storage" => &["endpoint_url"],
        _ => &[],
    };
    for field in url_fields {
        if let Some(value) = fields.get(*field) {
            match value {
                Value::String(url) => validate_source_url(url)?,
                Value::Array(urls) if *field == "start_urls" => {
                    if urls.is_empty() || urls.len() > 25 {
                        return Err(mas_common::error::AppError::validation(
                            "invalid source URL list",
                        ));
                    }
                    for url in urls {
                        let url = url.as_str().ok_or_else(|| {
                            mas_common::error::AppError::validation("source URLs must be strings")
                        })?;
                        validate_source_url(url)?;
                    }
                },
                _ => {
                    return Err(mas_common::error::AppError::validation(
                        "source URL must be a string or URL list",
                    ))
                },
            }
        }
    }
    Ok(())
}

fn validate_source_url(raw: &str) -> Result<()> {
    let invalid = || {
        mas_common::error::AppError::validation(
            "source URL must be HTTPS and must not contain credentials, query, or fragment",
        )
    };
    if raw.len() > 2048 || raw.trim() != raw || raw.chars().any(char::is_control) {
        return Err(invalid());
    }
    let (_, authority) = raw.split_once("://").ok_or_else(invalid)?;
    if authority
        .split(['/', '?', '#'])
        .next()
        .is_none_or(|part| part.contains('@'))
    {
        return Err(invalid());
    }
    let url = url::Url::parse(raw).map_err(|_| invalid())?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port().is_some_and(|port| port != 443)
    {
        return Err(invalid());
    }
    Ok(())
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
#[serde(deny_unknown_fields)]
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
        if self.size_bytes == 0 {
            return Err(mas_common::error::AppError::invalid_field(
                "size_bytes",
                "empty",
                "document content must not be empty",
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
        // An unknown policy field must never be silently ignored: a caller
        // could otherwise believe an unenforced restriction protects content.
        let policy = self.access_policy.as_object().ok_or_else(|| {
            mas_common::error::AppError::invalid_field(
                "access_policy",
                "not_object",
                "access policy must be an object",
            )
        })?;
        if policy
            .keys()
            .any(|key| key != "required_roles" && key != "denied_principals")
        {
            return Err(mas_common::error::AppError::invalid_field(
                "access_policy",
                "unknown_field",
                "access policy contains an unsupported field",
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

#[cfg(test)]
mod strict_request_tests {
    use super::*;

    #[test]
    fn source_urls_reject_embedded_credentials_and_queries() {
        for url in [
            "https://user:pass@docs.example.test/root",
            "https://@docs.example.test/root",
            "https:docs.example.test/root",
            "https://docs.example.test/root?",
            "https://docs.example.test/root#",
            "https://docs.example.test/\nsecret",
            "https://docs.example.test/root?token=secret",
            "https://docs.example.test/root#secret",
            "http://docs.example.test/root",
            "https://docs.example.test:8443/root",
        ] {
            assert!(validate_source_config("web", &serde_json::json!({"url": url})).is_err());
            assert!(
                validate_source_config("web", &serde_json::json!({"start_urls": [url]})).is_err()
            );
            assert!(validate_source_config(
                "object_storage",
                &serde_json::json!({"endpoint_url": url})
            )
            .is_err());
        }
        assert!(validate_source_config(
            "web",
            &serde_json::json!({"start_urls": ["https://docs.example.test/root"]})
        )
        .is_ok());
        assert!(validate_source_config("web", &serde_json::json!({"start_urls": [42]})).is_err());
    }

    #[test]
    fn top_level_policy_typos_are_rejected_by_deserialization() {
        let source = serde_json::json!({
            "project_id": ProjectId::new(), "name": "s", "kind": "upload",
            "config": {}, "access_policy": {"required_roles": ["admin"]}
        });
        assert!(serde_json::from_value::<CreateDataSourceRequest>(source).is_err());
        let document = serde_json::json!({
            "source_id": DataSourceId::new(), "title": "t", "mime_type": "text/plain",
            "size_bytes": 1, "content_sha256": "a".repeat(64),
            "access_policy": {}, "required_roles": ["admin"]
        });
        assert!(serde_json::from_value::<CreateDocumentRequest>(document).is_err());
    }
}
