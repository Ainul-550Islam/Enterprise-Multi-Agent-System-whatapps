//! API key metadata.
//!
//! **Invariant:** the raw key value is never persisted in this object — only
//! the short visible prefix (for UX/lookup) plus scopes/lifecycle metadata.
//! Hash verification happens in `security::api_keys`.
//!
//! **Tenant binding (security Phase 8):** a key is minted *inside* one tenant
//! and one organization, and that binding is stored here, not supplied by the
//! caller at authentication time. Whoever presents a key therefore becomes a
//! principal of the key's tenant — presenting a valid key cannot make you a
//! member of some other tenant, and no request header can move it.
//!
//! **Role:** the key carries the membership role its bearer acts under. It is
//! minted by an administrator and is not self-elevatable: a key cannot widen
//! its own role, and there is no API that changes a key's role in place
//! (rotation preserves identity, revocation ends it).

use crate::MembershipRole;
use mas_common::ids::{ApiKeyId, OrganizationId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Non-secret view used when listing keys to users.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyMetadataView {
    pub id: ApiKeyId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub name: String,
    pub prefix: String,
    pub scopes: Vec<String>,
    pub expires_at: Option<Timestamp>,
    pub last_used_at: Option<Timestamp>,
    pub revoked: bool,
    pub created_at: Timestamp,
}

/// API key metadata aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: ApiKeyId,
    pub tenant_id: TenantId,
    /// The organization the tenant belongs to. Both halves of the binding are
    /// stored so authentication never has to guess one from the other.
    pub organization_id: OrganizationId,
    /// The membership role the bearer of this key acts under.
    #[serde(default = "default_key_role")]
    pub role: MembershipRole,
    /// Creating/owning user (`None` for service keys).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<UserId>,
    pub name: String,
    /// First characters of the key (e.g. `mas_a1b2c3d4`), safe to display.
    pub prefix: String,
    /// Granted scopes (e.g. `agents:read`, `executions:*`).
    pub scopes: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<Timestamp>,
    pub created_at: Timestamp,
}

/// The role assumed by a key serialized before keys carried a role.
///
/// Least privilege on upgrade: an old key does not silently become an
/// administrator row because a new field appeared.
const fn default_key_role() -> MembershipRole {
    MembershipRole::ServiceAccount
}

impl ApiKey {
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        user_id: Option<UserId>,
        role: MembershipRole,
        name: impl Into<String>,
        prefix: impl Into<String>,
        scopes: BTreeSet<String>,
        expires_at: Option<Timestamp>,
    ) -> Result<Self> {
        let name = name.into();
        let prefix = prefix.into();
        validation::validate_resource_name("name", &name)?;
        if !prefix.starts_with(mas_common::constants::API_KEY_PREFIX)
            || prefix.len()
                > mas_common::constants::API_KEY_PREFIX.len()
                    + mas_common::constants::API_KEY_VISIBLE_PREFIX_LEN
        {
            return Err(mas_common::error::AppError::invalid_field(
                "prefix",
                "invalid_format",
                "prefix must be the visible key prefix only (never the full key)",
            ));
        }
        for scope in &scopes {
            validation::validate_length("scope", scope, 1, 128)?;
        }
        if expires_at.is_some_and(|expiry| !expiry.is_future()) {
            return Err(mas_common::error::AppError::invalid_field(
                "expires_at",
                "out_of_range",
                "expiration must be in the future",
            ));
        }
        Ok(Self {
            id: ApiKeyId::new(),
            tenant_id,
            organization_id,
            role,
            user_id,
            name,
            prefix,
            scopes,
            expires_at,
            last_used_at: None,
            revoked_at: None,
            created_at: Timestamp::now(),
        })
    }

    /// Whether the key may authenticate right now.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.revoked_at.is_none() && !matches!(self.expires_at, Some(expiry) if !expiry.is_future())
    }

    /// Exact match or wildcard coverage (`*` or `prefix:*`).
    #[must_use]
    pub fn has_scope(&self, requested: &str) -> bool {
        if self.scopes.contains(requested) || self.scopes.contains("*") {
            return true;
        }
        match requested.split_once(':') {
            Some((namespace, _)) => self.scopes.contains(&format!("{namespace}:*")),
            None => false,
        }
    }

    /// Records a successful authentication at `at`. Rate-limit metadata is
    /// kept out of the aggregate on purpose (quota layer owns it).
    pub fn record_use(&mut self, at: Timestamp) {
        self.last_used_at = Some(at);
    }

    /// The tenant this key binds its bearer to.
    #[must_use]
    pub const fn tenant_binding(&self) -> TenantId {
        self.tenant_id
    }

    /// The organization this key binds its bearer to.
    #[must_use]
    pub const fn organization_binding(&self) -> OrganizationId {
        self.organization_id
    }

    /// Immediately revokes the key (irreversible).
    pub fn revoke(&mut self) {
        if self.revoked_at.is_none() {
            self.revoked_at = Some(Timestamp::now());
        }
    }

    #[must_use]
    pub fn is_revoked(&self) -> bool {
        self.revoked_at.is_some()
    }

    /// Public-safe projection of this key.
    #[must_use]
    pub fn as_metadata_view(&self) -> ApiKeyMetadataView {
        ApiKeyMetadataView {
            id: self.id,
            tenant_id: self.tenant_id,
            organization_id: self.organization_id,
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            scopes: self.scopes.iter().cloned().collect(),
            expires_at: self.expires_at,
            last_used_at: self.last_used_at,
            revoked: self.is_revoked(),
            created_at: self.created_at,
        }
    }
}
