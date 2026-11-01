//! Connector aggregate: configuration for talking to an external system.
//!
//! Contains *metadata only*. Live connectivity (`test_connection`) is
//! executed by the integrations layer; here we model the contract and record
//! its outcomes.

use mas_common::enums::ConnectorStatus;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, CredentialId, OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

string_enum! {
    /// How the connector authenticates to the external system.
    ConnectorAuthMode {
        None => "none",
        OAuth2 => "oauth2",
        ApiKey => "api_key",
        Basic => "basic",
    }
}

/// The connector aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connector {
    pub id: ConnectorId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    /// Provider key (e.g. `slack`, `salesforce`, `postgres`).
    pub provider: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub auth_mode: ConnectorAuthMode,
    pub status: ConnectorStatus,
    /// Non-secret credential metadata link (`None` until bound).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<CredentialId>,
    /// Non-secret provider configuration (instance URL, region, …).
    /// MUST NOT contain secret material — enforced by redaction in writes.
    #[serde(default)]
    pub config: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_tested_at: Option<Timestamp>,
    /// Safe failure description of the last connectivity test.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Connector {
    /// Creates a connector in `PendingVerification`.
    pub fn register(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        provider: impl Into<String>,
        name: impl Into<String>,
        auth_mode: ConnectorAuthMode,
    ) -> Result<Self> {
        let provider = provider.into();
        let name = name.into();
        validation::validate_length("provider", &provider, 1, 64)?;
        if !provider
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
        {
            return Err(AppError::invalid_field(
                "provider",
                "invalid_format",
                "provider keys may contain [A-Za-z0-9._-] only",
            ));
        }
        validation::validate_resource_name("name", &name)?;
        let now = Timestamp::now();
        Ok(Self {
            id: ConnectorId::new(),
            tenant_id,
            organization_id,
            provider,
            name,
            display_name: None,
            auth_mode,
            status: ConnectorStatus::PendingVerification,
            credential_id: None,
            config: serde_json::Map::new(),
            last_tested_at: None,
            last_error: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Binds credential metadata. Requires matching auth mode necessity.
    pub fn bind_credential(&mut self, credential_id: CredentialId) -> Result<()> {
        if self.auth_mode == ConnectorAuthMode::None {
            return Err(AppError::invalid_field(
                "credential_id",
                "not_applicable",
                "connectors with auth mode 'none' cannot bind credentials",
            ));
        }
        self.credential_id = Some(credential_id);
        self.touch();
        Ok(())
    }

    /// Records the outcome of a connectivity test (performed by integrations).
    pub fn record_test_result(&mut self, ok: bool, error: Option<String>) -> Result<()> {
        if self.status == ConnectorStatus::Disabled {
            return Err(AppError::conflict("disabled connectors cannot be tested"));
        }
        self.last_tested_at = Some(Timestamp::now());
        if ok {
            if self.auth_mode != ConnectorAuthMode::None && self.credential_id.is_none() {
                return Err(AppError::conflict(
                    "connector requires bound credentials before it can become active",
                ));
            }
            self.status = ConnectorStatus::Active;
            self.last_error = None;
        } else {
            self.status = ConnectorStatus::Error;
            let mut error = error.unwrap_or_else(|| "connection test failed".to_owned());
            error.truncate(1024);
            self.last_error = Some(error);
        }
        self.touch();
        Ok(())
    }

    pub fn disable(&mut self) -> Result<()> {
        match self.status {
            ConnectorStatus::Disabled => Ok(()),
            _ => {
                self.status = ConnectorStatus::Disabled;
                self.touch();
                Ok(())
            },
        }
    }

    /// Runtime usability gate used by tool/connector services.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.status == ConnectorStatus::Active
    }

    fn touch(&mut self) {
        self.updated_at = Timestamp::now();
    }
}
