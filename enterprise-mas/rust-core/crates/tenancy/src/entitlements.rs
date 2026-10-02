//! Entitlement gating: subscription → feature surface (and caps).
//!
//! * The store holds the platform-agnostic records (Subscription,
//!   Entitlement rows) — the *plan matrix* is a pluggable source.
//! * Explicit entitlement rows override the plan (feature flags, custom
//!   contracts), including the limit cap (None = inherit).
//! * Suspended/cancelled subscriptions refuse work **here** (fail-closed),
//!   before the quota system ever sees a request.

use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_domain::{Entitlement, FeatureKey, Subscription, SubscriptionState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

/// Persistence port for subscription + entitlement rows.
#[async_trait::async_trait]
pub trait EntitlementStorePort: Send + Sync + fmt::Debug {
    async fn get_subscription(&self, tenant_id: TenantId) -> Result<Option<Subscription>>;
    async fn upsert_subscription(&self, subscription: &Subscription) -> Result<()>;
    async fn list_entitlements(&self, tenant_id: TenantId) -> Result<Vec<Entitlement>>;
    async fn upsert_entitlement(&self, entitlement: &Entitlement) -> Result<()>;
}

/// Plan-matrix source: plan_code → the features it buys + generic caps.
#[async_trait::async_trait]
pub trait PlanMatrixPort: Send + Sync + fmt::Debug {
    async fn features_for_plan(&self, plan_code: &str) -> Result<Option<PlanGrant>>;
}

/// What a plan grants (features with optional platform-wide defaults).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanGrant {
    #[serde(default)]
    pub features: BTreeSet<FeatureKey>,
    /// Default caps by feature key (None-cap = unbounded).
    #[serde(default)]
    pub default_limits: BTreeMap<String, u64>,
}

/// One tenant's effective feature profile (the rendered view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitlementProfile {
    pub tenant_id: TenantId,
    pub plan_code: String,
    pub subscription_state: SubscriptionState,
    /// Enabled features with their effective cap (None = unbounded).
    pub features: BTreeMap<FeatureKey, Option<u64>>,
    #[serde(default)]
    pub seats: Option<u32>,
}

impl EntitlementProfile {
    #[must_use]
    pub fn has_feature(&self, key: &FeatureKey) -> bool {
        self.features.contains_key(key)
    }

    #[must_use]
    pub fn cap(&self, key: &FeatureKey) -> Option<u64> {
        self.features.get(key).copied().flatten()
    }
}

/// In-memory entitlement store.
#[derive(Debug, Default)]
pub struct InMemoryEntitlementStore {
    subscriptions: Mutex<BTreeMap<TenantId, Subscription>>,
    entitlements: Mutex<BTreeMap<(TenantId, String), Entitlement>>,
}

impl InMemoryEntitlementStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl EntitlementStorePort for InMemoryEntitlementStore {
    async fn get_subscription(&self, tenant_id: TenantId) -> Result<Option<Subscription>> {
        Ok(self
            .subscriptions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&tenant_id)
            .cloned())
    }
    async fn upsert_subscription(&self, subscription: &Subscription) -> Result<()> {
        self.subscriptions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(subscription.tenant_id, subscription.clone());
        Ok(())
    }
    async fn list_entitlements(&self, tenant_id: TenantId) -> Result<Vec<Entitlement>> {
        Ok(self
            .entitlements
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range((tenant_id, String::new())..=(tenant_id, "￿".to_owned()))
            .map(|(_, entitlement)| entitlement.clone())
            .collect())
    }
    async fn upsert_entitlement(&self, entitlement: &Entitlement) -> Result<()> {
        self.entitlements
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                (
                    entitlement.tenant_id,
                    entitlement.feature.as_str().to_owned(),
                ),
                entitlement.clone(),
            );
        Ok(())
    }
}

/// Static in-process plan matrix (tenant bootstrap, dev, tests).
#[derive(Debug, Default)]
pub struct StaticPlanMatrix {
    plans: Mutex<BTreeMap<String, PlanGrant>>,
}

impl StaticPlanMatrix {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_grant(self, plan_code: impl Into<String>, grant: PlanGrant) -> Self {
        self.plans
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(plan_code.into(), grant);
        self
    }
}

#[async_trait::async_trait]
impl PlanMatrixPort for StaticPlanMatrix {
    async fn features_for_plan(&self, plan_code: &str) -> Result<Option<PlanGrant>> {
        Ok(self
            .plans
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(plan_code)
            .cloned())
    }
}

/// The gating service.
#[derive(Debug)]
pub struct EntitlementService {
    store: Arc<dyn EntitlementStorePort>,
    plans: Arc<dyn PlanMatrixPort>,
}

impl EntitlementService {
    pub fn new(store: Arc<dyn EntitlementStorePort>, plans: Arc<dyn PlanMatrixPort>) -> Self {
        Self { store, plans }
    }

    /// Builds the effective profile for a tenant (None → no plan at all).
    pub async fn profile(&self, tenant_id: TenantId) -> Result<Option<EntitlementProfile>> {
        let Some(subscription) = self.store.get_subscription(tenant_id).await? else {
            return Ok(None);
        };
        let grant = self
            .plans
            .features_for_plan(&subscription.plan_code)
            .await?
            .unwrap_or_default();
        let mut features: BTreeMap<FeatureKey, Option<u64>> = grant
            .features
            .into_iter()
            .map(|feature| {
                let cap = grant.default_limits.get(feature.as_str()).copied();
                (feature, cap)
            })
            .collect();
        for entitlement in self.store.list_entitlements(tenant_id).await? {
            if entitlement.enabled {
                features.insert(entitlement.feature.clone(), entitlement.limit);
            } else {
                features.remove(&entitlement.feature);
            }
        }
        Ok(Some(EntitlementProfile {
            tenant_id,
            plan_code: subscription.plan_code,
            subscription_state: subscription.state,
            features,
            seats: subscription.seats,
        }))
    }

    /// The feature gate callers use (returns an upgrade-required error).
    pub async fn require_feature(&self, tenant_id: TenantId, feature: &FeatureKey) -> Result<()> {
        match self.profile(tenant_id).await? {
            None => Err(AppError::forbidden(format!(
                "tenant has no subscription; feature '{feature}' is unavailable"
            ))),
            Some(profile) => {
                Self::require_state_allows(&profile.subscription_state)?;
                if profile.has_feature(feature) {
                    Ok(())
                } else {
                    Err(AppError::forbidden(format!(
                        "plan '{}' does not include feature '{feature}'",
                        profile.plan_code
                    )))
                }
            },
        }
    }

    /// Numeric cap accessor for quota integration (`None` = unbounded).
    pub async fn feature_cap(
        &self,
        tenant_id: TenantId,
        feature: &FeatureKey,
    ) -> Result<Option<u64>> {
        match self.profile(tenant_id).await? {
            None => Ok(None),
            Some(profile) => {
                Self::require_state_allows(&profile.subscription_state)?;
                Ok(profile
                    .has_feature(feature)
                    .then(|| profile.cap(feature))
                    .flatten())
            },
        }
    }

    /// Subscription-state gate shared by both checks.
    fn require_state_allows(state: &SubscriptionState) -> Result<()> {
        match state {
            SubscriptionState::Active
            | SubscriptionState::Trialing
            | SubscriptionState::PastDue => Ok(()),
            SubscriptionState::Suspended => Err(AppError::forbidden(
                "subscription is suspended; features are unavailable",
            )),
            SubscriptionState::Cancelled => Err(AppError::forbidden(
                "subscription is cancelled; read-only access only",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_domain::EntitlementSource;

    fn world(
        plan_features: Vec<FeatureKey>,
    ) -> (EntitlementService, Arc<InMemoryEntitlementStore>) {
        let store = Arc::new(InMemoryEntitlementStore::new());
        let matrix = Arc::new(StaticPlanMatrix::new().with_grant(
            "team",
            PlanGrant {
                features: plan_features.iter().cloned().collect(),
                default_limits: BTreeMap::from([(
                    FeatureKey::SANDBOXED_CODE.as_str().to_owned(),
                    100,
                )]),
            },
        ));
        (EntitlementService::new(store.clone(), matrix), store)
    }

    #[tokio::test]
    async fn missing_plan_unknown_feature_grants_and_fails_closed() {
        let (service, store) = world(vec![FeatureKey::SANDBOXED_CODE]);
        let tenant = TenantId::new();
        assert!(service
            .require_feature(tenant, &FeatureKey::AUDIT_EXPORT)
            .await
            .is_err());

        store
            .upsert_subscription(
                &Subscription::new(tenant, "team", SubscriptionState::Active, Some(10), None)
                    .expect("sub"),
            )
            .await
            .expect("upsert");
        service
            .require_feature(tenant, &FeatureKey::SANDBOXED_CODE)
            .await
            .expect("granted");
        assert_eq!(
            service
                .feature_cap(tenant, &FeatureKey::SANDBOXED_CODE)
                .await
                .expect("cap"),
            Some(100)
        );
        assert!(service
            .require_feature(tenant, &FeatureKey::CUSTOM_CONNECTORS)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn explicit_entitlements_override_plan_both_ways() {
        let (service, store) = world(vec![FeatureKey::ADVANCED_WORKFLOWS]);
        let tenant = TenantId::new();
        store
            .upsert_subscription(
                &Subscription::new(tenant, "team", SubscriptionState::Active, None, None)
                    .expect("sub"),
            )
            .await
            .expect("upsert");
        // disable a plan feature via a contract override
        let mut disabled = Entitlement::grant(
            tenant,
            FeatureKey::ADVANCED_WORKFLOWS,
            None,
            EntitlementSource::ContractOverride,
            None,
        )
        .expect("gent");
        disabled.enabled = false;
        store.upsert_entitlement(&disabled).await.expect("disable");
        assert!(service
            .require_feature(tenant, &FeatureKey::ADVANCED_WORKFLOWS)
            .await
            .is_err());
        // grant a non-plan feature with a cap
        store
            .upsert_entitlement(
                &Entitlement::grant(
                    tenant,
                    FeatureKey::AUDIT_EXPORT,
                    Some(5),
                    EntitlementSource::ContractOverride,
                    None,
                )
                .expect("grant"),
            )
            .await
            .expect("enable");
        service
            .require_feature(tenant, &FeatureKey::AUDIT_EXPORT)
            .await
            .expect("enabled");
        assert_eq!(
            service
                .feature_cap(tenant, &FeatureKey::AUDIT_EXPORT)
                .await
                .expect("cap"),
            Some(5)
        );
    }

    #[tokio::test]
    async fn suspended_or_cancelled_subscriptions_deny_everything() {
        let (service, store) = world(vec![FeatureKey::SANDBOXED_CODE]);
        let tenant = TenantId::new();
        for state in [SubscriptionState::Suspended, SubscriptionState::Cancelled] {
            store
                .upsert_subscription(
                    &Subscription::new(tenant, "team", state, None, None).expect("s"),
                )
                .await
                .expect("upsert");
            assert!(
                service
                    .require_feature(tenant, &FeatureKey::SANDBOXED_CODE)
                    .await
                    .is_err(),
                "state {state:?} must deny"
            );
        }
        // Trial and past_due keep working.
        for state in [SubscriptionState::Trialing, SubscriptionState::PastDue] {
            store
                .upsert_subscription(
                    &Subscription::new(tenant, "team", state, None, None).expect("s"),
                )
                .await
                .expect("upsert");
            assert!(
                service
                    .require_feature(tenant, &FeatureKey::SANDBOXED_CODE)
                    .await
                    .is_ok(),
                "state {state:?} must allow"
            );
        }
    }
}
