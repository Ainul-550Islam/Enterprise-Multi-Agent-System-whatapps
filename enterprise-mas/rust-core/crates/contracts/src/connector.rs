//! Connector DTOs.

use mas_common::enums::ConnectorStatus;
use mas_common::ids::ConnectorId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

/// Request to register a connector. Secret material is NEVER accepted here —
/// credentials are exchanged through the secret-store flow and referenced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterConnectorRequest {
    /// Provider key (e.g. `slack`, `salesforce`).
    pub provider: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// `none` | `oauth2` | `api_key` | `basic`
    pub auth_mode: String,
    /// Non-secret configuration (instance URL, region, scopes). Must not
    /// contain keys matching the sensitive-key policy.
    #[serde(default)]
    pub config: serde_json::Map<String, serde_json::Value>,
}

impl RegisterConnectorRequest {
    pub fn validate(&self) -> Result<()> {
        validation::validate_length("provider", &self.provider, 1, 64)?;
        if !self
            .provider
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
        {
            return Err(mas_common::error::AppError::invalid_field(
                "provider",
                "invalid_format",
                "provider keys may contain [A-Za-z0-9._-] only",
            ));
        }
        validation::validate_resource_name("name", &self.name)?;
        match self.auth_mode.as_str() {
            "none" | "oauth2" | "api_key" | "basic" => {},
            other => {
                return Err(mas_common::error::AppError::invalid_field(
                    "auth_mode",
                    "invalid_enum_value",
                    format!("unknown auth mode '{other}'"),
                ));
            },
        }
        // Reject payloads that smuggle secrets into config.
        for key in self.config.keys() {
            if mas_common::redaction::is_sensitive_key(key) {
                return Err(mas_common::error::AppError::invalid_field(
                    "config",
                    "secret_in_config",
                    format!(
                        "config key '{key}' looks sensitive; secrets must go through the credential flow"
                    ),
                ));
            }
        }
        Ok(())
    }
}

/// Request to test connectivity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestConnectorRequest {
    pub connector_id: ConnectorId,
}

impl TestConnectorRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

/// Wire representation of a connector (never contains secrets).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorResponse {
    pub id: ConnectorId,
    pub tenant_id: mas_common::ids::TenantId,
    pub provider: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub auth_mode: String,
    pub status: ConnectorStatus,
    pub has_credential: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tested_at: Option<Timestamp>,
    /// Safe failure summary of the last connectivity test.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Outcome of a connectivity test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorStatusResponse {
    pub connector_id: ConnectorId,
    pub status: ConnectorStatus,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub latency_ms: Option<u64>,
    pub tested_at: Timestamp,
}
