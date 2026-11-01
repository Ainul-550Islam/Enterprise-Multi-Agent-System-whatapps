//! Secret references: typed pointers into external secret stores.
//!
//! The reference itself is not sensitive — it contains provider, path and
//! version metadata, never material.

use mas_common::ids::SecretReferenceId;
use mas_common::result::Result;
use mas_common::string_enum;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Backing secret provider.
    SecretProviderKind {
        Vault => "vault",
        Kms => "kms",
        AwsSecretsManager => "aws_secrets_manager",
        GcpSecretManager => "gcp_secret_manager",
        AzureKeyVault => "azure_key_vault",
        /// Process environment (dev/test only).
        Environment => "environment",
    }
}

string_enum! {
    /// Rotation lifecycle of the referenced secret version.
    SecretRotationState {
        /// The version applications should use.
        Current => "current",
        /// A new version is being rolled out; read current, write both.
        Rotating => "rotating",
        /// Superseded; kept for in-flight consumers only.
        Retired => "retired",
    }
}

string_enum! {
    /// Whether the referenced secret can currently be read.
    SecretAvailability {
        Available => "available",
        Unavailable => "unavailable",
        Deleted => "deleted",
    }
}

/// A typed pointer to secret material in an external store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretReference {
    pub id: SecretReferenceId,
    pub provider: SecretProviderKind,
    /// Provider-specific path/URI (e.g. `secret/data/tenants/t1/slack`).
    pub path: String,
    /// Key version within the store (`None` = store default/latest).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_version: Option<String>,
    pub rotation_state: SecretRotationState,
    pub availability: SecretAvailability,
}

impl SecretReference {
    pub fn new(
        provider: SecretProviderKind,
        path: impl Into<String>,
        key_version: Option<String>,
    ) -> Result<Self> {
        let path = path.into();
        mas_common::validation::validate_non_empty("path", &path)?;
        if path.len() > 1024 || path.chars().any(char::is_control) {
            return Err(mas_common::error::AppError::invalid_field(
                "path",
                "invalid_format",
                "secret paths must be 1..=1024 chars without control characters",
            ));
        }
        Ok(Self {
            id: SecretReferenceId::new(),
            provider,
            path,
            key_version,
            rotation_state: SecretRotationState::Current,
            availability: SecretAvailability::Available,
        })
    }

    /// Marks the reference unreadable (provider outage / lease loss).
    pub fn mark_unavailable(&mut self) {
        self.availability = SecretAvailability::Unavailable;
    }

    pub fn mark_available(&mut self) {
        if self.availability == SecretAvailability::Unavailable {
            self.availability = SecretAvailability::Available;
        }
    }

    pub fn mark_deleted(&mut self) {
        self.availability = SecretAvailability::Deleted;
    }

    /// Current → Rotating (a replacement version is being rolled in).
    pub fn begin_rotation(&mut self) -> Result<()> {
        match self.rotation_state {
            SecretRotationState::Current => {
                self.rotation_state = SecretRotationState::Rotating;
                Ok(())
            },
            SecretRotationState::Rotating => Ok(()),
            SecretRotationState::Retired => Err(mas_common::error::AppError::conflict(
                "retired secret references cannot rotate",
            )),
        }
    }

    /// Rotating → Current with the new key version.
    pub fn complete_rotation(&mut self, new_key_version: Option<String>) -> Result<()> {
        match self.rotation_state {
            SecretRotationState::Rotating => {
                self.key_version = new_key_version;
                self.rotation_state = SecretRotationState::Current;
                Ok(())
            },
            other => Err(mas_common::error::AppError::conflict(format!(
                "cannot complete rotation from state '{other}'"
            ))),
        }
    }

    /// Current|Rotating → Retired.
    pub fn retire(&mut self) {
        self.rotation_state = SecretRotationState::Retired;
    }

    /// Whether readers may use this reference right now.
    #[must_use]
    pub fn is_readable(&self) -> bool {
        self.availability == SecretAvailability::Available
            && self.rotation_state != SecretRotationState::Retired
    }
}
