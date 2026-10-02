//! Entitlements: tenant-level feature access derived from plan/contract.

use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

string_enum! {
    /// Where an entitlement came from.
    EntitlementSource {
        /// From the subscription plan.
        Plan => "plan",
        /// Explicit commercial override.
        ContractOverride => "contract_override",
        /// Time-limited trial grant.
        Trial => "trial",
        /// Internal/manual grant (support, migration).
        Manual => "manual",
    }
}

/// Stable feature identifiers. String-backed so new features don't require
/// schema changes, with well-known constants for the common ones.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeatureKey(Cow<'static, str>);

impl FeatureKey {
    pub const ADVANCED_WORKFLOWS: FeatureKey = FeatureKey(Cow::Borrowed("advanced_workflows"));
    pub const CUSTOM_CONNECTORS: FeatureKey = FeatureKey(Cow::Borrowed("custom_connectors"));
    pub const SSO_SCIM: FeatureKey = FeatureKey(Cow::Borrowed("sso_scim"));
    pub const AUDIT_EXPORT: FeatureKey = FeatureKey(Cow::Borrowed("audit_export"));
    pub const SANDBOXED_CODE: FeatureKey = FeatureKey(Cow::Borrowed("sandboxed_code"));
    pub const DEDICATED_ISOLATION: FeatureKey = FeatureKey(Cow::Borrowed("dedicated_isolation"));
    pub const PRIORITY_SUPPORT: FeatureKey = FeatureKey(Cow::Borrowed("priority_support"));

    pub fn new(key: impl Into<Cow<'static, str>>) -> Result<Self> {
        let key = key.into();
        mas_common::validation::validate_length("feature_key", &key, 1, 64)?;
        if !key
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        {
            return Err(mas_common::error::AppError::invalid_field(
                "feature_key",
                "invalid_format",
                "feature keys use snake_case [a-z0-9_]",
            ));
        }
        Ok(Self(key))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for FeatureKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One feature grant for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entitlement {
    pub tenant_id: TenantId,
    pub feature: FeatureKey,
    pub enabled: bool,
    /// Optional numeric cap associated with the feature (e.g. seats, agents).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    pub source: EntitlementSource,
    /// Grant expiry (trials/contract terms); `None` = open-ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Entitlement {
    pub fn grant(
        tenant_id: TenantId,
        feature: FeatureKey,
        limit: Option<u64>,
        source: EntitlementSource,
        expires_at: Option<Timestamp>,
    ) -> Result<Self> {
        if limit == Some(0) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "limits must be positive when set",
            ));
        }
        let now = Timestamp::now();
        Ok(Self {
            tenant_id,
            feature,
            enabled: true,
            limit,
            source,
            expires_at,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether the grant confers access right now.
    #[must_use]
    pub fn is_active(&self, now: &Timestamp) -> bool {
        self.enabled && self.expires_at.is_none_or(|expiry| expiry.is_after(now))
    }

    /// Remaining amount against the feature limit, if limited.
    #[must_use]
    pub const fn remaining(&self, used: u64) -> Option<u64> {
        match self.limit {
            Some(limit) => Some(limit.saturating_sub(used)),
            None => None,
        }
    }

    /// Whether `used + amount` stays within the feature limit
    /// (`None` limits mean unbounded).
    #[must_use]
    pub const fn allows(&self, used: u64, amount: u64) -> bool {
        match self.limit {
            Some(limit) => used.saturating_add(amount) <= limit,
            None => true,
        }
    }

    pub fn disable(&mut self) {
        self.enabled = false;
        self.updated_at = Timestamp::now();
    }

    pub fn adjust_limit(&mut self, limit: Option<u64>) -> Result<()> {
        if limit == Some(0) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "limits must be positive when set",
            ));
        }
        self.limit = limit;
        self.updated_at = Timestamp::now();
        Ok(())
    }
}
