//! Credential metadata: everything about a credential *except* the secret.
//!
//! **Invariant:** raw secret values never enter domain objects. Only the
//! [`SecretReferenceId`] pointing at the secret store is stored here.

use mas_common::ids::{ConnectorId, CredentialId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Kind of credential material referenced.
    CredentialType {
        OAuth2Token => "oauth2_token",
        ApiKey => "api_key",
        BasicAuth => "basic_auth",
        Certificate => "certificate",
    }
}

/// Rotation bookkeeping for a credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RotationMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rotated_at: Option<Timestamp>,
    /// Rotate at least every N days (`None` = manual rotation only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_period_days: Option<u32>,
}

impl Default for RotationMetadata {
    fn default() -> Self {
        Self {
            last_rotated_at: None,
            rotation_period_days: Some(90),
        }
    }
}

/// Non-secret metadata describing a stored credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialMetadata {
    pub id: CredentialId,
    pub tenant_id: TenantId,
    /// Connector this credential serves (if bound).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_id: Option<ConnectorId>,
    pub credential_type: CredentialType,
    /// Provider key (e.g. `slack`, `github`).
    pub provider: String,
    /// Pointer to the actual secret material in the secret store.
    pub secret_reference_id: SecretReferenceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    #[serde(default)]
    pub rotation: RotationMetadata,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl CredentialMetadata {
    pub fn new(
        tenant_id: TenantId,
        connector_id: Option<ConnectorId>,
        credential_type: CredentialType,
        provider: impl Into<String>,
        secret_reference_id: SecretReferenceId,
        expires_at: Option<Timestamp>,
    ) -> Result<Self> {
        let provider = provider.into();
        validation::validate_non_empty("provider", &provider)?;
        let now = Timestamp::now();
        Ok(Self {
            id: CredentialId::new(),
            tenant_id,
            connector_id,
            credential_type,
            provider,
            secret_reference_id,
            expires_at,
            rotation: RotationMetadata::default(),
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether the credential is past its expiry.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|expiry| !expiry.is_future())
    }

    /// Whether rotation is due based on the configured period.
    #[must_use]
    pub fn needs_rotation(&self) -> bool {
        let Some(period_days) = self.rotation.rotation_period_days else {
            return false;
        };
        let Some(last) = self.rotation.last_rotated_at else {
            return true; // never rotated
        };
        let due = last.checked_add(std::time::Duration::from_secs(
            u64::from(period_days) * 86_400,
        ));
        due.is_some_and(|due| !due.is_future())
    }

    /// Points the credential at the rotated secret version.
    pub fn mark_rotated(&mut self, new_reference: SecretReferenceId) -> Result<()> {
        self.secret_reference_id = new_reference;
        self.rotation.last_rotated_at = Some(Timestamp::now());
        self.updated_at = Timestamp::now();
        Ok(())
    }

    /// Records a refreshed expiry (e.g. after OAuth token refresh).
    pub fn set_expiry(&mut self, expires_at: Option<Timestamp>) {
        self.expires_at = expires_at;
        self.updated_at = Timestamp::now();
    }
}
