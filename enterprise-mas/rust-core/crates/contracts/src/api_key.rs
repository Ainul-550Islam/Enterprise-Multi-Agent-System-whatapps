//! API-key management DTOs. Raw key material is returned only by create/rotate.

use mas_common::ids::ApiKeyId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Issue an API key within the authenticated tenant and principal role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateApiKeyRequest {
    /// Human-recognizable key name.
    pub name: String,
    /// Requested least-privilege scopes.
    pub scopes: Vec<String>,
    /// Optional expiry; absence means no scheduled expiry.
    pub expires_at: Option<Timestamp>,
}

impl CreateApiKeyRequest {
    /// Validates names, scopes, expiry and bounds before key issuance.
    pub fn validate(&self) -> Result<()> {
        validation::validate_resource_name("name", &self.name)?;
        if self.scopes.is_empty() || self.scopes.len() > 64 {
            return Err(mas_common::error::AppError::invalid_field(
                "scopes",
                "out_of_range",
                "an API key must request between 1 and 64 scopes",
            ));
        }
        for scope in &self.scopes {
            validation::validate_length("scope", scope, 1, 128)?;
            mas_security_scope_validate(scope)?;
        }
        if self.expires_at.is_some_and(|value| !value.is_future()) {
            return Err(mas_common::error::AppError::invalid_field(
                "expires_at",
                "in_past",
                "API key expiry must be in the future",
            ));
        }
        Ok(())
    }
}

/// Reuses the security crate's canonical scope grammar without accepting
/// malformed strings into persisted key metadata.
fn mas_security_scope_validate(scope: &str) -> Result<()> {
    mas_common::validation::validate_length("scope", scope, 1, 128)?;
    if scope == "*" {
        return Ok(());
    }
    let Some((resource, action)) = scope.split_once(':') else {
        return Err(mas_common::error::AppError::invalid_field(
            "scope",
            "invalid_format",
            "scope must be resource:action, resource:*, or *",
        ));
    };
    let valid_part = |part: &str, allow_wildcard: bool| {
        !part.is_empty()
            && part.len() <= 64
            && part.chars().enumerate().all(|(index, ch)| {
                ch.is_ascii_alphanumeric()
                    || matches!(ch, '-' | '_')
                    || (allow_wildcard && ch == '*' && index == part.len() - 1)
            })
    };
    if scope.matches(':').count() != 1 || !valid_part(resource, false) || !valid_part(action, true)
    {
        return Err(mas_common::error::AppError::invalid_field(
            "scope",
            "invalid_format",
            "scope must be resource:action, resource:*, or *",
        ));
    }
    Ok(())
}

/// Non-secret metadata shown when listing keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyMetadataDto {
    /// Key identifier.
    pub id: ApiKeyId,
    /// User-visible name.
    pub name: String,
    /// Display-safe prefix; never includes the key secret.
    pub prefix: String,
    /// Granted scopes.
    pub scopes: Vec<String>,
    /// Optional expiry.
    pub expires_at: Option<Timestamp>,
    /// Latest successful use.
    pub last_used_at: Option<Timestamp>,
    /// Whether the key has been revoked.
    pub revoked: bool,
    /// Issuance timestamp.
    pub created_at: Timestamp,
}

/// Create/rotate response. `key` is plaintext and MUST be shown once only.
#[derive(Clone, Serialize, Deserialize)]
pub struct CreatedApiKeyDto {
    /// Safe key metadata.
    pub metadata: ApiKeyMetadataDto,
    /// Raw key material; do not persist or log this response.
    pub key: String,
}

impl fmt::Debug for CreatedApiKeyDto {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreatedApiKeyDto")
            .field("metadata", &self.metadata)
            .field("key", &mas_common::redaction::REDACTED)
            .finish()
    }
}

/// Body for a revocation request.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RevokeApiKeyRequest {
    /// Optional operator reason recorded in the audit trail.
    pub reason: Option<String>,
}

/// Body for key rotation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateApiKeyRequest {
    /// Optional grace interval in seconds; capped by the security service.
    pub grace_seconds: Option<u64>,
}

impl RotateApiKeyRequest {
    /// Rejects unbounded/negative-equivalent grace values.
    pub fn validate(&self) -> Result<()> {
        if self.grace_seconds.is_some_and(|seconds| seconds > 86_400) {
            return Err(mas_common::error::AppError::invalid_field(
                "grace_seconds",
                "out_of_range",
                "API key rotation grace may not exceed one day",
            ));
        }
        Ok(())
    }
}
