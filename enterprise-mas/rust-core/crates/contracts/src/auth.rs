//! Authentication/authorization contract DTOs.

use mas_common::ids::{OrganizationId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

/// How the caller presented credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthnMethod {
    BearerToken,
    ApiKey,
    ServiceToService,
}

/// Inbound authentication request.
///
/// The credential material itself is redacted from `Debug` output to keep it
/// out of logs (defense in depth on top of the logging redactor).
#[derive(Clone, Serialize, Deserialize)]
pub struct AuthenticateRequest {
    pub method: AuthnMethod,
    /// Raw credential (JWT or API key). Never logged.
    pub credential: String,
    /// Optional tenant hint (`X-Tenant-Id`); verified against the credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_hint: Option<TenantId>,
}

impl fmt::Debug for AuthenticateRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthenticateRequest")
            .field("method", &self.method)
            .field("credential", &mas_common::redaction::REDACTED)
            .field("tenant_hint", &self.tenant_hint)
            .finish()
    }
}

impl AuthenticateRequest {
    pub fn validate(&self) -> Result<()> {
        validation::validate_non_empty("credential", &self.credential)?;
        if self.credential.len() > mas_common::constants::MAX_JWT_SIZE_BYTES {
            return Err(mas_common::error::AppError::invalid_field(
                "credential",
                "too_long",
                "credential exceeds maximum allowed size",
            ));
        }
        Ok(())
    }
}

/// Verified claims extracted from a credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationClaims {
    /// Subject (canonical user id string for user credentials).
    pub subject: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    pub issuer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    pub expires_at: Timestamp,
    pub issued_at: Timestamp,
}

impl AuthenticationClaims {
    pub fn validate(&self) -> Result<()> {
        validation::validate_non_empty("subject", &self.subject)?;
        validation::validate_non_empty("issuer", &self.issuer)?;
        if !self.expires_at.is_future() {
            return Err(mas_common::error::AppError::unauthorized(
                "credential has expired",
            ));
        }
        if self.issued_at.is_after(&self.expires_at) {
            return Err(mas_common::error::AppError::unauthorized(
                "credential issued-at is after expiry",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.contains(scope) || self.scopes.contains("*")
    }
}

/// Fully authenticated actor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthenticatedActor {
    User {
        user_id: UserId,
    },
    ApiKey {
        api_key_id: mas_common::ids::ApiKeyId,
    },
    Service {
        service_name: String,
    },
}

/// Security context handed to authorization/policy checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizationContext {
    pub actor: AuthenticatedActor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
}

impl AuthorizationContext {
    #[must_use]
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.contains(scope) || self.scopes.contains("*")
    }

    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }

    /// Derives a context from verified user claims.
    #[must_use]
    pub fn from_claims(claims: &AuthenticationClaims) -> Option<Self> {
        let actor = AuthenticatedActor::User {
            user_id: claims.user_id?,
        };
        Some(Self {
            actor,
            tenant_id: claims.tenant_id,
            organization_id: claims.organization_id,
            scopes: claims.scopes.clone(),
            roles: claims.roles.clone(),
        })
    }
}

/// Identity claims of an internal service (mTLS/JWT service tokens).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceIdentityClaims {
    /// Registered service name (e.g. `rust-api`, `python-orchestrator`).
    pub service_name: String,
    /// Instance/pod identity for audit, if available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    /// Services this identity is permitted to call.
    #[serde(default)]
    pub allowed_targets: BTreeSet<String>,
    pub expires_at: Timestamp,
}

impl ServiceIdentityClaims {
    pub fn validate(&self) -> Result<()> {
        validation::validate_non_empty("service_name", &self.service_name)?;
        if !self.expires_at.is_future() {
            return Err(mas_common::error::AppError::unauthorized(
                "service credential has expired",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub fn may_call(&self, target: &str) -> bool {
        self.allowed_targets.contains(target) || self.allowed_targets.contains("*")
    }
}
