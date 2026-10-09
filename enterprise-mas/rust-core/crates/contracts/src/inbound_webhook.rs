//! PURPOSE: Stable authenticated-management and public-ingress DTOs for
//! canonical inbound webhooks.
//!
//! Owning layer: `mas-contracts` public API/gRPC boundary.
//!
//! Production role: define bounded endpoint registration/listing shapes and a
//! minimal public intake acknowledgement without exposing tenant identifiers,
//! signing-secret material, Vault paths, receipt payloads, or replay details.
//!
//! Security boundary: these DTOs validate syntax only. Authorization, route
//! lookup, HMAC verification, receipt idempotency, and outbox ordering remain
//! server-side and cannot be inferred from a DTO.
//!
//! Data ownership: DTOs own wire compatibility only; endpoint/receipt/outbox
//! stores own durable state; the secret manager owns signing-key bytes.
//!
//! Prohibited responsibilities: carrying raw secrets, accepting a tenant id
//! for public ingress, serializing received payloads, or making delivery
//! success claims before durable receipt processing.

use mas_common::error::AppError;
use mas_common::ids::{InboundWebhookId, SecretReferenceId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Authenticated endpoint registration request. The secret pointer must already
/// exist in the caller's tenant; the server enforces that with a scoped FK.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterInboundWebhookRequest {
    pub source: String,
    pub signing_secret_reference_id: SecretReferenceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_key_version: Option<String>,
    /// Bounded current/previous provider `kid` allowlist used during rotation.
    #[serde(default)]
    pub accepted_key_versions: BTreeSet<String>,
}

impl RegisterInboundWebhookRequest {
    pub fn validate(&self) -> Result<()> {
        validate_source(&self.source)?;
        if self.signing_secret_reference_id.is_nil() {
            return Err(AppError::invalid_field(
                "signing_secret_reference_id",
                "required",
                "a concrete signing secret reference is required",
            ));
        }
        if self
            .required_key_version
            .as_deref()
            .is_some_and(|value| !is_safe_key_version(value))
            || self.accepted_key_versions.len() > 3
            || self
                .accepted_key_versions
                .iter()
                .any(|value| !is_safe_key_version(value))
        {
            return Err(AppError::invalid_field(
                "key_versions",
                "invalid_format",
                "inbound key versions must be at most three bounded safe tokens",
            ));
        }
        if let Some(required) = self.required_key_version.as_deref() {
            if !self.accepted_key_versions.is_empty()
                && !self.accepted_key_versions.contains(required)
            {
                return Err(AppError::invalid_field(
                    "key_versions",
                    "inconsistent",
                    "required key version must be included in accepted key versions",
                ));
            }
        }
        Ok(())
    }
}

/// Bounded keyset list query for tenant inbound endpoint metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InboundWebhookQuery {
    pub limit: Option<u16>,
    pub cursor: Option<String>,
}

impl InboundWebhookQuery {
    pub fn validate(&self) -> Result<()> {
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "inbound webhook endpoint page limit must be between 1 and 200",
            ));
        }
        if let Some(cursor) = &self.cursor {
            if cursor.is_empty()
                || cursor.len() > 512
                || !cursor
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(AppError::invalid_field(
                    "cursor",
                    "invalid_format",
                    "inbound webhook endpoint cursor must be URL-safe base64 without padding",
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn page_limit(&self) -> u16 {
        match self.limit {
            Some(limit) => limit,
            None => 50,
        }
    }
}

/// Safe endpoint metadata visible only through authenticated tenant routes.
/// It deliberately confirms signing-key configuration without exposing a
/// reference identifier, manager path, version content, or key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundWebhookDto {
    pub id: InboundWebhookId,
    pub source: String,
    pub has_signing_secret: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_key_version: Option<String>,
    #[serde(default)]
    pub accepted_key_versions: BTreeSet<String>,
    pub enabled: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Stable keyset page of endpoint metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundWebhookPageDto {
    pub items: Vec<InboundWebhookDto>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Public acknowledgement after an authenticated receipt operation. The value
/// intentionally reveals neither tenant nor whether a duplicate row exists.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundWebhookIngestResponse {
    /// `accepted`, `deferred`, or `duplicate`.
    pub disposition: String,
}

fn validate_source(source: &str) -> Result<()> {
    if source.is_empty()
        || source.len() > 128
        || !source
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(AppError::invalid_field(
            "source",
            "invalid_format",
            "inbound webhook source must be a 1..=128 character safe token",
        ));
    }
    Ok(())
}

fn is_safe_key_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_and_cursor_validation_fail_closed() {
        let valid = RegisterInboundWebhookRequest {
            source: "provider.events".to_owned(),
            signing_secret_reference_id: SecretReferenceId::new(),
            required_key_version: Some("v1".to_owned()),
            accepted_key_versions: BTreeSet::from(["v1".to_owned()]),
        };
        assert!(valid.validate().is_ok());
        assert!(RegisterInboundWebhookRequest {
            source: "../../bad".to_owned(),
            signing_secret_reference_id: SecretReferenceId::new(),
            required_key_version: None,
            accepted_key_versions: BTreeSet::new(),
        }
        .validate()
        .is_err());
        assert!(InboundWebhookQuery {
            limit: Some(201),
            cursor: None,
        }
        .validate()
        .is_err());
    }
}
