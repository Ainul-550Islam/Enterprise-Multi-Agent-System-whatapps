//! PURPOSE: The plan aggregate — a durable, data-defined commercial tier and
//! the two things it actually controls: which features are switched on, and
//! how much of each metered dimension the tenant may consume.
//!
//! Owning layer: `mas-domain`.
//!
//! Production responsibility: make a plan *data*. Before this aggregate a plan
//! was a `plan_code` string compared against a matrix compiled into the
//! process, so adding a tier or renegotiating one customer's token ceiling
//! was a code change and a deploy. The enforcement path reads these rows now.
//!
//! Data ownership: plans are platform-wide reference data. There is no
//! `tenant_id` on `Plan`, and that is deliberate — a tenant-writable plan is a
//! tenant that can mint its own limits. Per-tenant deviation is expressed by
//! an override (see `overrides.rs`), never by editing a plan.
//!
//! It must NOT:
//!   • be consulted for pricing or presentation — `spec` is opaque metadata
//!     and enforcement must never branch on it;
//!   • let a caller supply its own plan or entitlement claims; a plan is
//!     resolved from the subscription, server-side;
//!   • hard-code tier names in execution code — `PLAN_CODES` exists only so
//!     seed data and tests have stable identifiers, not so the runtime can
//!     say `if plan == "enterprise"`.

use mas_common::error::AppError;
use mas_common::ids::PlanId;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::entitlement::FeatureKey;
use crate::quota::{EnforcementMode, QuotaDimension, QuotaPeriod};

string_enum! {
    /// Lifecycle of a plan.
    ///
    /// A plan that is no longer sold keeps its `Retired` row: historical
    /// subscriptions still point at it, and deleting the row would leave
    /// those tenants with no resolvable entitlement set at all — which, under
    /// deny-by-default, means they lose features they have paid for.
    PlanStatus {
        Active => "active",
        /// No longer sold to new tenants; existing subscriptions keep working.
        Retired => "retired",
    }
}

/// Stable identifiers for the plans the platform seeds.
///
/// These exist so migrations, fixtures and tests agree on spelling. The
/// enforcement path compares against the *database*, never against these.
pub mod plan_codes {
    /// Entry tier: enough to evaluate the platform.
    pub const FREE: &str = "free";
    /// Standard paid tier.
    pub const TEAM: &str = "team";
    /// Paid tier with concurrency and governance features.
    pub const BUSINESS: &str = "business";
    /// Highest tier; per-tenant overrides are expected here.
    pub const ENTERPRISE: &str = "enterprise";
}

/// A plan: a named, versioned bundle of features and metering ceilings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    pub id: PlanId,
    /// Stable machine code (`free`, `team`, …). Unique across the platform.
    pub code: String,
    /// Human-facing name.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Opaque presentation/billing metadata. Never read by enforcement.
    #[serde(default)]
    pub spec: serde_json::Value,
    pub status: PlanStatus,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Plan {
    /// Creates a plan, validating the code format and the human name.
    pub fn new(
        code: impl Into<String>,
        name: impl Into<String>,
        description: Option<String>,
    ) -> Result<Self> {
        let code = code.into();
        validate_plan_code(&code)?;
        let name = name.into();
        validation::validate_length("name", &name, 1, 120)?;
        let now = Timestamp::now();
        Ok(Self {
            id: PlanId::new(),
            code,
            name,
            description,
            spec: serde_json::Value::Object(serde_json::Map::new()),
            status: PlanStatus::Active,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether the plan may be attached to a *new* subscription.
    #[must_use]
    pub const fn is_sellable(&self) -> bool {
        matches!(self.status, PlanStatus::Active)
    }

    /// Marks the plan as no longer sold. Existing subscriptions are untouched.
    pub fn retire(&mut self) {
        self.status = PlanStatus::Retired;
        self.updated_at = Timestamp::now();
    }
}

/// Validates a plan code (`snake_case`, 1..=64).
fn validate_plan_code(code: &str) -> Result<()> {
    validation::validate_length("code", code, 1, 64)?;
    if !code
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(AppError::invalid_field(
            "code",
            "invalid_format",
            "plan codes use snake_case [a-z0-9_]",
        ));
    }
    Ok(())
}

/// One feature a plan unlocks, with its optional numeric cap.
///
/// `limit = None` means *the feature is on and unbounded*; an absent row means
/// *the feature is off*. Deny-by-default follows from the row's absence, not
/// from a boolean that someone has to remember to set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanEntitlement {
    pub plan_id: PlanId,
    pub feature: FeatureKey,
    pub enabled: bool,
    /// Optional cap (seats, agents, retention days…). `None` = unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numeric_limit: Option<u64>,
    pub created_at: Timestamp,
}

impl PlanEntitlement {
    /// Grants a feature on a plan.
    pub fn grant(plan_id: PlanId, feature: FeatureKey, numeric_limit: Option<u64>) -> Result<Self> {
        if numeric_limit == Some(0) {
            return Err(AppError::invalid_field(
                "numeric_limit",
                "out_of_range",
                "a plan limit must be positive when set; omit it for unbounded",
            ));
        }
        Ok(Self {
            plan_id,
            feature,
            enabled: true,
            numeric_limit,
            created_at: Timestamp::now(),
        })
    }

    /// The effective cap, and whether the grant is on at all.
    #[must_use]
    pub const fn effective(&self) -> Option<Option<u64>> {
        if self.enabled {
            Some(self.numeric_limit)
        } else {
            None
        }
    }
}

/// One metering ceiling a plan imposes.
///
/// Keyed by `(plan_id, dimension, period)`: the plan's allowance for that
/// meter in that window. A tenant's effective ceiling is this row unless an
/// override says otherwise.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanQuota {
    pub plan_id: PlanId,
    pub dimension: QuotaDimension,
    pub period: QuotaPeriod,
    pub max_value: u64,
    pub enforcement: EnforcementMode,
    pub created_at: Timestamp,
}

impl PlanQuota {
    /// Defines a plan ceiling for one dimension.
    pub fn new(
        plan_id: PlanId,
        dimension: QuotaDimension,
        period: QuotaPeriod,
        max_value: u64,
        enforcement: EnforcementMode,
    ) -> Result<Self> {
        // Gauge dimensions never reset, so a period would be incoherent. The
        // same invariant is enforced in `Quota::new`; repeating it here stops
        // a plan row from expressing something the runtime cannot evaluate.
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
        Ok(Self {
            plan_id,
            dimension,
            period,
            max_value,
            enforcement,
            created_at: Timestamp::now(),
        })
    }
}

/// A plan resolved together with everything it grants — the shape the
/// entitlement resolver and the quota registry actually consume.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResolvedPlan {
    pub plan: Option<Plan>,
    /// Feature → cap. `Some(None)` = on and unbounded.
    #[serde(default)]
    pub features: BTreeMap<FeatureKey, Option<u64>>,
    /// Dimension+period → ceiling.
    #[serde(default)]
    pub quotas: BTreeMap<String, PlanQuota>,
}

impl ResolvedPlan {
    /// Stable map key for a (dimension, period) pair.
    #[must_use]
    pub fn quota_key(dimension: &QuotaDimension, period: &QuotaPeriod) -> String {
        format!("{dimension}:{period}")
    }

    /// The ceiling a plan sets for one dimension/period, if any.
    #[must_use]
    pub fn quota_for(
        &self,
        dimension: &QuotaDimension,
        period: &QuotaPeriod,
    ) -> Option<&PlanQuota> {
        self.quotas.get(&Self::quota_key(dimension, period))
    }

    /// The plan code, for audit lines and error messages.
    #[must_use]
    pub fn code(&self) -> &str {
        self.plan.as_ref().map_or("none", |plan| plan.code.as_str())
    }
}
