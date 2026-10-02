//! Subscription state relevant to authorization.
//!
//! This models *only* what runtime authorization needs (state, seats, plan
//! code). Payment-provider APIs, invoices, dunning etc. live outside
//! rust-core entirely.

use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Lifecycle of the commercial subscription.
    SubscriptionState {
        Trialing => "trialing",
        Active => "active",
        /// Payment issues; grace period, runtime keeps working.
        PastDue => "past_due",
        /// Deliberately paused; workloads stop.
        Suspended => "suspended",
        /// Ended; read-only access only.
        Cancelled => "cancelled",
    }
}

/// Authorization-facing subscription snapshot for a tenant.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub tenant_id: TenantId,
    /// Plan identifier (e.g. `team`, `enterprise`). Resolved to entitlements
    /// by the entitlement layer; never interpreted here.
    pub plan_code: String,
    pub state: SubscriptionState,
    /// Purchased seats, when the plan is seat-based.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seats: Option<u32>,
    /// Next renewal/reevaluation point in time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renews_at: Option<Timestamp>,
    pub updated_at: Timestamp,
}

impl Subscription {
    pub fn new(
        tenant_id: TenantId,
        plan_code: impl Into<String>,
        state: SubscriptionState,
        seats: Option<u32>,
        renews_at: Option<Timestamp>,
    ) -> Result<Self> {
        let plan_code = plan_code.into();
        mas_common::validation::validate_non_empty("plan_code", &plan_code)?;
        Ok(Self {
            tenant_id,
            plan_code,
            state,
            seats,
            renews_at,
            updated_at: Timestamp::now(),
        })
    }

    /// May the tenant use paid functionality right now?
    #[must_use]
    pub const fn is_entitled(&self) -> bool {
        matches!(
            self.state,
            SubscriptionState::Trialing | SubscriptionState::Active | SubscriptionState::PastDue
        )
    }

    /// May new work be *started* (stricter than `is_entitled`)?
    #[must_use]
    pub const fn can_start_work(&self) -> bool {
        matches!(
            self.state,
            SubscriptionState::Trialing | SubscriptionState::Active
        )
    }

    /// Deterministic state transition (billing events feed this).
    pub fn transition_to(&mut self, next: SubscriptionState) -> Result<()> {
        use SubscriptionState as S;
        let legal = matches!(
            (self.state, next),
            (S::Trialing, S::Active)
                | (S::Trialing, S::Cancelled)
                | (S::Active, S::PastDue)
                | (S::Active, S::Suspended)
                | (S::Active, S::Cancelled)
                | (S::PastDue, S::Active)
                | (S::PastDue, S::Suspended)
                | (S::PastDue, S::Cancelled)
                | (S::Suspended, S::Active)
                | (S::Suspended, S::Cancelled)
        );
        if !legal {
            return Err(mas_common::error::AppError::conflict(format!(
                "illegal subscription transition {} → {next}",
                self.state
            )));
        }
        self.state = next;
        self.updated_at = Timestamp::now();
        Ok(())
    }

    /// Checks seat capacity for adding `additional` members.
    #[must_use]
    pub fn has_seat_capacity(&self, current_members: u32, additional: u32) -> bool {
        match self.seats {
            Some(seats) => current_members.saturating_add(additional) <= seats,
            None => true,
        }
    }
}
