//! PURPOSE: Rust-owned Data Hub source metadata and sync lifecycle.
//!
//! Source configuration contains no secret bytes. Credentials are linked via
//! metadata identifiers, source discovery cursors are opaque, and all ingestion
//! bytes/vector operations remain behind the Python Data Hub bridge.

use mas_common::error::AppError;
use mas_common::ids::{CredentialId, DataSourceId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use serde_json::Value;

string_enum! {
    /// Supported Data Hub source adapter.
    DataSourceKind {
        /// Uploaded file content read through Rust-authorized storage.
        Upload => "upload",
        /// S3-compatible object storage adapter.
        ObjectStorage => "object_storage",
        /// Bounded, allow-listed web crawler.
        Web => "web",
    }
}

string_enum! {
    /// Data source lifecycle.
    DataSourceStatus {
        /// Newly registered but not yet enabled.
        Draft => "draft",
        /// Ready for explicit or scheduled synchronization.
        Active => "active",
        /// A single sync operation is in flight.
        Syncing => "syncing",
        /// The last sync failed; retry requires an explicit transition.
        Error => "error",
        /// Disabled source; no new syncs may start.
        Disabled => "disabled",
    }
}

/// One tenant-scoped source whose metadata is owned by Rust.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataSource {
    pub id: DataSourceId,
    pub tenant_id: TenantId,
    pub project_id: ProjectId,
    pub name: String,
    pub kind: DataSourceKind,
    pub status: DataSourceStatus,
    /// Non-secret adapter configuration only.
    #[serde(default)]
    pub config: Value,
    /// Credential metadata link; raw secret material never enters this object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<CredentialId>,
    /// User that registered this source.
    pub created_by: UserId,
    /// Opaque adapter cursor persisted by Rust.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_synced_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure_code: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl DataSource {
    /// Registers a source in `Draft`.
    pub fn register(
        tenant_id: TenantId,
        project_id: ProjectId,
        name: impl Into<String>,
        kind: DataSourceKind,
        config: Value,
        credential_id: Option<CredentialId>,
        created_by: UserId,
    ) -> Result<Self> {
        if tenant_id.is_nil() || project_id.is_nil() || created_by.is_nil() {
            return Err(AppError::invalid_field(
                "data_source",
                "required",
                "tenant, project and creator identifiers must be concrete",
            ));
        }
        let name = name.into();
        validation::validate_length("name", &name, 1, 200)?;
        if name.trim().is_empty() {
            return Err(AppError::invalid_field(
                "name",
                "required",
                "must not be empty",
            ));
        }
        validate_config(&config)?;
        let now = Timestamp::now();
        Ok(Self {
            id: DataSourceId::new(),
            tenant_id,
            project_id,
            name,
            kind,
            status: DataSourceStatus::Draft,
            config,
            credential_id,
            created_by,
            sync_cursor: None,
            last_synced_at: None,
            last_failure_code: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Revalidates persisted state after hydration.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_nil() || self.tenant_id.is_nil() || self.project_id.is_nil() {
            return Err(AppError::database("data source contains a nil identifier"));
        }
        validation::validate_length("name", &self.name, 1, 200)
            .map_err(|_| AppError::database("data source name violates its persisted invariant"))?;
        validate_config(&self.config)
            .map_err(|_| AppError::database("data source config contains an invalid value"))?;
        Ok(())
    }

    /// Draft|Error → Active.
    pub fn activate(&mut self) -> Result<()> {
        match self.status {
            DataSourceStatus::Draft | DataSourceStatus::Error => {
                self.status = DataSourceStatus::Active;
                self.last_failure_code = None;
                self.touch();
                Ok(())
            },
            DataSourceStatus::Active => Ok(()),
            other => Err(AppError::conflict(format!(
                "data source in status '{other}' cannot be activated"
            ))),
        }
    }

    /// Active → Syncing.
    pub fn begin_sync(&mut self) -> Result<()> {
        match self.status {
            DataSourceStatus::Active => {
                self.status = DataSourceStatus::Syncing;
                self.last_failure_code = None;
                self.touch();
                Ok(())
            },
            other => Err(AppError::conflict(format!(
                "data source in status '{other}' cannot start a sync"
            ))),
        }
    }

    /// Syncing → Active and persist the opaque adapter cursor.
    pub fn complete_sync(&mut self, cursor: Option<String>, at: Timestamp) -> Result<()> {
        self.require_syncing("complete")?;
        if let Some(cursor) = cursor.as_deref() {
            validation::validate_length("sync_cursor", cursor, 0, 4096)?;
        }
        self.sync_cursor = cursor;
        self.last_synced_at = Some(at);
        self.last_failure_code = None;
        self.status = DataSourceStatus::Active;
        self.touch();
        Ok(())
    }

    /// Syncing → Error with a sanitized, stable failure code.
    pub fn fail_sync(&mut self, failure_code: impl Into<String>) -> Result<()> {
        self.require_syncing("fail")?;
        let failure_code = failure_code.into();
        validation::validate_length("failure_code", &failure_code, 1, 128)?;
        if !failure_code
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
        {
            return Err(AppError::invalid_field(
                "failure_code",
                "invalid_format",
                "failure code must be a stable identifier, not provider text",
            ));
        }
        self.last_failure_code = Some(failure_code);
        self.status = DataSourceStatus::Error;
        self.touch();
        Ok(())
    }

    /// Disables the source. An in-flight sync must settle before it is disabled.
    pub fn disable(&mut self) -> Result<()> {
        match self.status {
            DataSourceStatus::Syncing => Err(AppError::conflict(
                "a syncing data source must settle before it can be disabled",
            )),
            DataSourceStatus::Disabled => Ok(()),
            _ => {
                self.status = DataSourceStatus::Disabled;
                self.touch();
                Ok(())
            },
        }
    }

    /// Changes non-secret configuration only when the source is not syncing.
    pub fn update_configuration(&mut self, config: Value) -> Result<()> {
        if self.status == DataSourceStatus::Syncing || self.status == DataSourceStatus::Disabled {
            return Err(AppError::conflict(
                "syncing or disabled data sources cannot be reconfigured",
            ));
        }
        validate_config(&config)?;
        self.config = config;
        self.touch();
        Ok(())
    }

    /// Binds credential metadata without accepting secret material.
    pub fn bind_credential(&mut self, credential_id: CredentialId) -> Result<()> {
        if credential_id.is_nil() {
            return Err(AppError::invalid_field(
                "credential_id",
                "required",
                "must be concrete",
            ));
        }
        if self.status == DataSourceStatus::Syncing || self.status == DataSourceStatus::Disabled {
            return Err(AppError::conflict(
                "syncing or disabled data sources cannot change credentials",
            ));
        }
        self.credential_id = Some(credential_id);
        self.touch();
        Ok(())
    }

    fn require_syncing(&self, action: &str) -> Result<()> {
        if self.status != DataSourceStatus::Syncing {
            return Err(AppError::conflict(format!(
                "cannot {action} a data source sync while status is '{}'",
                self.status
            )));
        }
        Ok(())
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}

fn validate_config(config: &Value) -> Result<()> {
    if !config.is_object() {
        return Err(AppError::invalid_field(
            "config",
            "not_object",
            "source configuration must be a JSON object",
        ));
    }
    validate_config_value(config)
}

fn validate_config_value(value: &Value) -> Result<()> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let key = key.to_ascii_lowercase().replace('-', "_");
                let reference = key.ends_with("_secret_ref")
                    || key.ends_with("_credential_ref")
                    || key == "secret_reference";
                if !reference
                    && [
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
                    .any(|fragment| key.contains(fragment))
                {
                    return Err(AppError::invalid_field(
                        "config",
                        "secret_material_forbidden",
                        "source configuration may contain a secret reference, never secret material",
                    ));
                }
                validate_config_value(value)?;
            }
            Ok(())
        },
        Value::Array(values) => {
            for value in values {
                validate_config_value(value)?;
            }
            Ok(())
        },
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{CredentialId, UserId};

    fn source() -> DataSource {
        DataSource::register(
            TenantId::new(),
            ProjectId::new(),
            "Docs",
            DataSourceKind::Web,
            serde_json::json!({"start_url": "https://docs.example.test"}),
            Some(CredentialId::new()),
            UserId::new(),
        )
        .expect("source")
    }

    #[test]
    fn source_lifecycle_records_cursor_and_failure_code() {
        let mut source = source();
        source.activate().expect("activate");
        source.begin_sync().expect("begin");
        source
            .complete_sync(Some("cursor-v2".to_owned()), Timestamp::now())
            .expect("complete");
        assert_eq!(source.status, DataSourceStatus::Active);
        assert_eq!(source.sync_cursor.as_deref(), Some("cursor-v2"));
        source.begin_sync().expect("retry");
        source.fail_sync("upstream_timeout").expect("failed");
        assert_eq!(source.status, DataSourceStatus::Error);
        assert_eq!(
            source.last_failure_code.as_deref(),
            Some("upstream_timeout")
        );
    }

    #[test]
    fn source_configuration_rejects_raw_secret_values() {
        let error = DataSource::register(
            TenantId::new(),
            ProjectId::new(),
            "Private bucket",
            DataSourceKind::ObjectStorage,
            serde_json::json!({"access_key": "raw-secret"}),
            None,
            UserId::new(),
        )
        .expect_err("raw credential configuration must not be stored");
        assert!(matches!(error, AppError::Validation { .. }));
    }

    #[test]
    fn disabled_sources_cannot_be_reactivated_by_a_sync_result() {
        let mut source = source();
        source.disable().expect("disable");
        assert!(source.begin_sync().is_err());
    }
}
