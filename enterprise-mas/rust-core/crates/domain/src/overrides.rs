//! PURPOSE: Enterprise overrides — a negotiated deviation from what the plan
//! grants, recorded as a first-class row rather than as an edit to the plan.
//!
//! Owning layer: `mas-domain`.
//!
//! Production responsibility: let one enterprise customer hold a limit the
//! catalogue does not sell, without changing what every other tenant on that
//! tier receives. An override names the tenant, the value, **why**, who
//! authorised it, and how long it lasts.
//!
//! Data ownership: overrides are tenant-scoped and sit under RLS. They are
//! Rust-owned platform state.
//!
//! It must NOT:
//!   • exist without a reason or an actor — an override nobody can justify is
//!     an override nobody can safely remove;
//!   • be used to modify a plan definition (plans are shared reference data);
//!   • be trusted from a caller: an override is read server-side from the
//!     tenant's own row, never accepted in a request body;
//!   • overlap in time for the same (tenant, target) — two live overrides
//!     would make the effective limit depend on row order.

use mas_common::error::AppError;
use mas_common::ids::{OverrideId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::entitlement::FeatureKey;
use crate::quota::{EnforcementMode, QuotaDimension, QuotaPeriod};

string_enum! {
    /// What kind of principal authorised an override.
    ActorKind {
        /// A named human operator (support engineer, account manager).
        User => "user",
        /// An API key acting on behalf of an integration.
        ApiKey => "api_key",
        /// A platform service (provisioning, migration).
        Service => "service",
    }
}

/// Who made an override, recorded verbatim for the audit trail.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OverrideActor {
    pub kind: ActorKind,
    /// Canonical id string (user id, key id, or service name).
    pub id: String,
}

impl OverrideActor {
    /// Builds an actor, rejecting an empty identifier.
    pub fn new(kind: ActorKind, id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        validation::validate_non_empty("actor_id", &id)?;
        Ok(Self { kind, id })
    }

    /// The platform provisioning actor.
    #[must_use]
    pub fn system() -> Self {
        Self {
            kind: ActorKind::Service,
            id: "system".to_owned(),
        }
    }
}

impl std::fmt::Display for OverrideActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.kind, self.id)
    }
}

/// A feature-level override for one tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitlementOverride {
    pub id: OverrideId,
    pub tenant_id: TenantId,
    pub feature: FeatureKey,
    pub enabled: bool,
    /// Optional cap; `None` = unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_limit: Option<u64>,
    /// Why this exists. Mandatory and non-empty.
    pub reason: String,
    pub actor: OverrideActor,
    pub effective_from: Timestamp,
    /// `None` = open-ended; otherwise the override stops applying at this
    /// instant and the plan's own grant takes over again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_to: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl EntitlementOverride {
    /// Creates an override. A blank reason is refused rather than trimmed into
    /// something plausible: the reason is the only thing that lets the next
    /// engineer decide whether this override may be removed.
    pub fn new(
        tenant_id: TenantId,
        feature: FeatureKey,
        enabled: bool,
        numeric_limit: Option<u64>,
        reason: impl Into<String>,
        actor: OverrideActor,
        effective_from: Timestamp,
        effective_to: Option<Timestamp>,
    ) -> Result<Self> {
        let reason = reason.into();
        // A reason of spaces is not a reason. The audit trail for an override
        // is the only thing that lets the next operator know why a tenant has
        // a limit its plan does not grant, so it has to say something.
        validation::validate_non_empty("reason", &reason)?;
        validation::validate_length("reason", reason.trim(), 1, 500)?;
        let reason = reason.trim().to_owned();
        if numeric_limit == Some(0) {
            return Err(AppError::invalid_field(
                "numeric_limit",
                "out_of_range",
                "an override limit must be positive when set; omit it for unbounded",
            ));
        }
        if let Some(until) = effective_to {
            if !until.is_after(&effective_from) {
                return Err(AppError::invalid_field(
                    "effective_to",
                    "invalid_period",
                    "effective_to must be strictly after effective_from",
                ));
            }
        }
        let now = Timestamp::now();
        Ok(Self {
            id: OverrideId::new(),
            tenant_id,
            feature,
            enabled,
            numeric_limit,
            reason,
            actor,
            effective_from,
            effective_to,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether the override is in force at `at`.
    ///
    /// Half-open `[effective_from, effective_to)`: an override that expires at
    /// exactly the instant it is consulted is expired. Two overrides for the
    /// same target would otherwise both be "live" at the boundary instant.
    #[must_use]
    pub fn is_effective_at(&self, at: &Timestamp) -> bool {
        if at.is_before(&self.effective_from) {
            return false;
        }
        match self.effective_to {
            Some(until) => at.is_before(&until),
            None => true,
        }
    }

    /// Ends the override now, keeping the audit trail intact.
    pub fn end_now(&mut self, at: &Timestamp) -> Result<()> {
        if !at.is_after(&self.effective_from) {
            return Err(AppError::invalid_field(
                "effective_to",
                "invalid_period",
                "cannot end an override at or before the instant it began",
            ));
        }
        self.effective_to = Some(*at);
        self.updated_at = *at;
        Ok(())
    }
}

/// A quota-ceiling override for one tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaOverride {
    pub id: OverrideId,
    pub tenant_id: TenantId,
    pub dimension: QuotaDimension,
    pub period: QuotaPeriod,
    pub max_value: u64,
    pub enforcement: EnforcementMode,
    /// Why this exists. Mandatory and non-empty.
    pub reason: String,
    pub actor: OverrideActor,
    pub effective_from: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_to: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl QuotaOverride {
    /// Creates a quota override.
    pub fn new(
        tenant_id: TenantId,
        dimension: QuotaDimension,
        period: QuotaPeriod,
        max_value: u64,
        enforcement: EnforcementMode,
        reason: impl Into<String>,
        actor: OverrideActor,
        effective_from: Timestamp,
        effective_to: Option<Timestamp>,
    ) -> Result<Self> {
        let reason = reason.into();
        // A reason of spaces is not a reason. The audit trail for an override
        // is the only thing that lets the next operator know why a tenant has
        // a limit its plan does not grant, so it has to say something.
        validation::validate_non_empty("reason", &reason)?;
        validation::validate_length("reason", reason.trim(), 1, 500)?;
        let reason = reason.trim().to_owned();
        if matches!(
            dimension,
            QuotaDimension::ConcurrentRuns | QuotaDimension::StorageBytes
        ) && period != QuotaPeriod::Unbounded
        {
            return Err(AppError::invalid_field(
                "period",
                "incoherent_period",
                "concurrent_runs and storage_bytes are gauges; use the 'unbounded' period",
            ));
        }
        if let Some(until) = effective_to {
            if !until.is_after(&effective_from) {
                return Err(AppError::invalid_field(
                    "effective_to",
                    "invalid_period",
                    "effective_to must be strictly after effective_from",
                ));
            }
        }
        let now = Timestamp::now();
        Ok(Self {
            id: OverrideId::new(),
            tenant_id,
            dimension,
            period,
            max_value,
            enforcement,
            reason,
            actor,
            effective_from,
            effective_to,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether the override is in force at `at` (half-open interval).
    #[must_use]
    pub fn is_effective_at(&self, at: &Timestamp) -> bool {
        if at.is_before(&self.effective_from) {
            return false;
        }
        match self.effective_to {
            Some(until) => at.is_before(&until),
            None => true,
        }
    }

    /// Ends the override now.
    pub fn end_now(&mut self, at: &Timestamp) -> Result<()> {
        if !at.is_after(&self.effective_from) {
            return Err(AppError::invalid_field(
                "effective_to",
                "invalid_period",
                "cannot end an override at or before the instant it began",
            ));
        }
        self.effective_to = Some(*at);
        self.updated_at = *at;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entitlement::FeatureKey;

    fn actor() -> OverrideActor {
        OverrideActor::new(ActorKind::User, "user-1").expect("actor")
    }

    #[test]
    fn a_blank_reason_is_refused() {
        let tenant = TenantId::new();
        let now = Timestamp::now();
        assert!(EntitlementOverride::new(
            tenant,
            FeatureKey::AUDIT_EXPORT,
            true,
            None,
            "   ",
            actor(),
            now,
            None,
        )
        .is_err());
        assert!(QuotaOverride::new(
            tenant,
            QuotaDimension::Executions,
            QuotaPeriod::Daily,
            10,
            EnforcementMode::Enforce,
            "",
            actor(),
            now,
            None,
        )
        .is_err());
    }

    #[test]
    fn the_effective_interval_is_half_open() {
        let tenant = TenantId::new();
        let start = Timestamp::from_unix_seconds(1_000).expect("start");
        let end = Timestamp::from_unix_seconds(2_000).expect("end");
        let after = Timestamp::from_unix_seconds(2_001).expect("after");
        let before = Timestamp::from_unix_seconds(999).expect("before");
        let override_ = EntitlementOverride::new(
            tenant,
            FeatureKey::AUDIT_EXPORT,
            true,
            Some(5),
            "contractual",
            actor(),
            start,
            Some(end),
        )
        .expect("override");
        assert!(!override_.is_effective_at(&before), "not yet started");
        assert!(override_.is_effective_at(&start), "inclusive start");
        assert!(!override_.is_effective_at(&end), "exclusive end");
        assert!(!override_.is_effective_at(&after), "expired");
    }

    #[test]
    fn an_open_ended_override_never_expires() {
        let tenant = TenantId::new();
        let start = Timestamp::from_unix_seconds(1_000).expect("start");
        let override_ = QuotaOverride::new(
            tenant,
            QuotaDimension::Tokens,
            QuotaPeriod::Monthly,
            1_000,
            EnforcementMode::Enforce,
            "negotiated",
            actor(),
            start,
            None,
        )
        .expect("override");
        let far_future = Timestamp::from_unix_seconds(4_000_000_000).expect("future");
        assert!(override_.is_effective_at(&far_future));
    }

    #[test]
    fn gauge_dimensions_reject_a_resetting_period() {
        let tenant = TenantId::new();
        let now = Timestamp::now();
        assert!(QuotaOverride::new(
            tenant,
            QuotaDimension::ConcurrentRuns,
            QuotaPeriod::Daily,
            4,
            EnforcementMode::Enforce,
            "more concurrency",
            actor(),
            now,
            None,
        )
        .is_err());
    }

    #[test]
    fn ending_an_override_records_the_instant() {
        let tenant = TenantId::new();
        let start = Timestamp::from_unix_seconds(1_000).expect("start");
        let end = Timestamp::from_unix_seconds(2_000).expect("end");
        let mut override_ = EntitlementOverride::new(
            tenant,
            FeatureKey::SSO_SCIM,
            true,
            None,
            "security review",
            actor(),
            start,
            None,
        )
        .expect("override");
        override_.end_now(&end).expect("end");
        assert_eq!(override_.effective_to, Some(end));
        assert!(!override_.is_effective_at(&end));
        // Ending before it began is refused rather than silently clamped.
        assert!(override_.end_now(&start).is_err());
    }
}
