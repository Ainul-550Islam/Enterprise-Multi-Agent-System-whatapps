//! PURPOSE: The plan catalogue and the plan matrix port — where "what does a
//! plan grant" is answered from rows instead of from compiled-in constants.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: hold the plan catalogue as data and expose it
//! through `PlanMatrixPort`, which is the only thing the entitlement resolver
//! asks. Adding a tier, or changing what a tier includes, is an INSERT now.
//!
//! Security boundary: plans are deliberately **not** tenant-scoped and carry
//! no RLS policy. A tenant reads the plan its subscription points at; a tenant
//! that could write plan rows could mint itself any limit. Plan administration
//! is a platform-admin operation protected at the API layer, not here.
//!
//! Data ownership: Rust-owned platform reference data.
//!
//! It must NOT:
//!   • accept a plan code from a request and treat it as the tenant's plan —
//!     the plan is resolved from the subscription, server-side;
//!   • let enforcement branch on `spec` (presentation/billing metadata);
//!   • delete a plan that live subscriptions point at.

use mas_common::error::AppError;
use mas_common::ids::PlanId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::entitlement::FeatureKey;
use mas_domain::plan::{Plan, PlanEntitlement, PlanQuota, PlanStatus, ResolvedPlan};
use mas_domain::quota::{EnforcementMode, QuotaDimension, QuotaPeriod};
use mas_tenancy::entitlements::PlanMatrixPort;
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::map_sqlx;

/// One plan row as stored.
#[derive(Debug, Clone)]
pub struct PlanRow {
    /// Plan id.
    pub id: uuid::Uuid,
    /// Stable machine code.
    pub code: String,
    /// Human-facing name.
    pub name: String,
    /// Long-form description.
    pub description: Option<String>,
    /// Opaque presentation/billing metadata.
    pub spec: serde_json::Value,
    /// Lifecycle state.
    pub status: String,
    /// Creation instant.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last mutation.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PlanRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            code: row.try_get("code")?,
            name: row.try_get("name")?,
            description: row.try_get("description")?,
            spec: row.try_get("spec")?,
            status: row.try_get("status")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl PlanRow {
    /// Hydrates into the domain aggregate.
    pub fn into_domain(self) -> Result<Plan> {
        let status = self
            .status
            .parse::<PlanStatus>()
            .map_err(|_| AppError::database(format!("unknown plan status '{}'", self.status)))?;
        Ok(Plan {
            id: PlanId::from_uuid(self.id),
            code: self.code,
            name: self.name,
            description: self.description,
            spec: self.spec,
            status,
            created_at: Timestamp::from_datetime(self.created_at),
            updated_at: Timestamp::from_datetime(self.updated_at),
        })
    }
}

/// One plan entitlement row.
#[derive(Debug, Clone)]
pub struct PlanEntitlementRow {
    /// Plan id.
    pub plan_id: uuid::Uuid,
    /// Feature key.
    pub feature: String,
    /// Whether the grant is on.
    pub enabled: bool,
    /// Optional numeric cap.
    pub numeric_limit: Option<i64>,
    /// Creation instant.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PlanEntitlementRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            plan_id: row.try_get("plan_id")?,
            feature: row.try_get("feature")?,
            enabled: row.try_get("enabled")?,
            numeric_limit: row.try_get("numeric_limit")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl PlanEntitlementRow {
    /// Hydrates into the domain aggregate.
    pub fn into_domain(self) -> Result<PlanEntitlement> {
        let feature = FeatureKey::new(self.feature.clone()).map_err(|_| {
            AppError::database(format!(
                "invalid feature key '{}' in plan row",
                self.feature
            ))
        })?;
        Ok(PlanEntitlement {
            plan_id: PlanId::from_uuid(self.plan_id),
            feature,
            enabled: self.enabled,
            numeric_limit: self.numeric_limit.and_then(|v| u64::try_from(v).ok()),
            created_at: Timestamp::from_datetime(self.created_at),
        })
    }
}

/// One plan quota row.
#[derive(Debug, Clone)]
pub struct PlanQuotaRow {
    /// Plan id.
    pub plan_id: uuid::Uuid,
    /// Metered dimension.
    pub dimension: String,
    /// Reset window.
    pub period: String,
    /// Ceiling.
    pub max_value: i64,
    /// What happens at the ceiling.
    pub enforcement: String,
    /// Creation instant.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PlanQuotaRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            plan_id: row.try_get("plan_id")?,
            dimension: row.try_get("dimension")?,
            period: row.try_get("period")?,
            max_value: row.try_get("max_value")?,
            enforcement: row.try_get("enforcement")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl PlanQuotaRow {
    /// Hydrates into the domain aggregate.
    pub fn into_domain(self) -> Result<PlanQuota> {
        let dimension = self.dimension.parse::<QuotaDimension>().map_err(|_| {
            AppError::database(format!("unknown quota dimension '{}'", self.dimension))
        })?;
        let period = self
            .period
            .parse::<QuotaPeriod>()
            .map_err(|_| AppError::database(format!("unknown quota period '{}'", self.period)))?;
        let enforcement = self.enforcement.parse::<EnforcementMode>().map_err(|_| {
            AppError::database(format!("unknown enforcement mode '{}'", self.enforcement))
        })?;
        PlanQuota::new(
            PlanId::from_uuid(self.plan_id),
            dimension,
            period,
            u64::try_from(self.max_value).unwrap_or(0),
            enforcement,
        )
    }
}

/// The plan catalogue and the `PlanMatrixPort` implementation.
#[derive(Debug, Clone)]
pub struct PostgresPlanStore {
    pool: PgPool,
}

impl PostgresPlanStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a plan. The unique index on `code` is what stops two
    /// provisioning runs from creating two plans with the same identity.
    pub async fn create(&self, plan: &Plan) -> Result<Plan> {
        sqlx::query(
            "INSERT INTO plans (id, code, name, description, spec, status)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (code) DO NOTHING",
        )
        .bind(plan.id.into_uuid())
        .bind(&plan.code)
        .bind(&plan.name)
        .bind(&plan.description)
        .bind(&plan.spec)
        .bind(plan.status.as_str())
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        self.by_code(&plan.code).await?.ok_or_else(|| {
            AppError::internal(format!("plan '{}' vanished after insert", plan.code))
        })
    }

    /// Reads a plan by code.
    pub async fn by_code(&self, code: &str) -> Result<Option<Plan>> {
        let row: Option<PlanRow> = sqlx::query_as::<_, PlanRow>(
            "SELECT id, code, name, description, spec, status, created_at, updated_at
               FROM plans WHERE code = $1",
        )
        .bind(code)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(PlanRow::into_domain).transpose()
    }

    /// Reads a plan by id.
    pub async fn by_id(&self, id: PlanId) -> Result<Option<Plan>> {
        let row: Option<PlanRow> = sqlx::query_as::<_, PlanRow>(
            "SELECT id, code, name, description, spec, status, created_at, updated_at
               FROM plans WHERE id = $1",
        )
        .bind(id.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(PlanRow::into_domain).transpose()
    }

    /// Every plan, active first.
    pub async fn list(&self) -> Result<Vec<Plan>> {
        let rows: Vec<PlanRow> = sqlx::query_as::<_, PlanRow>(
            "SELECT id, code, name, description, spec, status, created_at, updated_at
               FROM plans
              ORDER BY (status <> 'active'), code",
        )
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(PlanRow::into_domain).collect()
    }

    /// Attaches a feature grant to a plan.
    pub async fn grant_feature(&self, grant: &PlanEntitlement) -> Result<()> {
        sqlx::query(
            "INSERT INTO plan_entitlements (plan_id, feature, enabled, numeric_limit)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (plan_id, feature) DO UPDATE
                SET enabled = EXCLUDED.enabled,
                    numeric_limit = EXCLUDED.numeric_limit",
        )
        .bind(grant.plan_id.into_uuid())
        .bind(grant.feature.as_str())
        .bind(grant.enabled)
        .bind(
            grant
                .numeric_limit
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
        )
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Sets a metering ceiling on a plan.
    pub async fn set_quota(&self, quota: &PlanQuota) -> Result<()> {
        sqlx::query(
            "INSERT INTO plan_quotas (plan_id, dimension, period, max_value, enforcement)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (plan_id, dimension, period) DO UPDATE
                SET max_value = EXCLUDED.max_value,
                    enforcement = EXCLUDED.enforcement",
        )
        .bind(quota.plan_id.into_uuid())
        .bind(quota.dimension.as_str())
        .bind(quota.period.as_str())
        .bind(i64::try_from(quota.max_value).unwrap_or(i64::MAX))
        .bind(quota.enforcement.as_str())
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Retires a plan: it stops being sold, existing subscriptions keep it.
    pub async fn retire(&self, id: PlanId) -> Result<Plan> {
        let row: Option<PlanRow> = sqlx::query_as::<_, PlanRow>(
            "UPDATE plans SET status = 'retired', updated_at = now()
              WHERE id = $1
          RETURNING id, code, name, description, spec, status, created_at, updated_at",
        )
        .bind(id.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(PlanRow::into_domain)
            .transpose()?
            .ok_or_else(|| AppError::not_found("plan", id.to_string()))
    }
}

#[async_trait::async_trait]
impl PlanMatrixPort for PostgresPlanStore {
    async fn features_for_plan(
        &self,
        plan_code: &str,
    ) -> Result<Option<mas_tenancy::entitlements::PlanGrant>> {
        use mas_tenancy::entitlements::PlanGrant;
        use std::collections::{BTreeMap, BTreeSet};

        let Some(plan) = self.by_code(plan_code).await? else {
            // An unknown plan code is not a crash and not a silent empty
            // grant: `None` propagates so the resolver can fail closed with a
            // message naming the code.
            return Ok(None);
        };

        let entitlement_rows: Vec<PlanEntitlementRow> = sqlx::query_as::<_, PlanEntitlementRow>(
            "SELECT plan_id, feature, enabled, numeric_limit, created_at
               FROM plan_entitlements
              WHERE plan_id = $1",
        )
        .bind(plan.id.into_uuid())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        let mut features: BTreeSet<FeatureKey> = BTreeSet::new();
        let mut default_limits: BTreeMap<String, u64> = BTreeMap::new();
        for row in entitlement_rows {
            let grant = row.into_domain()?;
            // A disabled grant is absent from the feature set, so
            // deny-by-default holds without an explicit "off" lookup.
            if !grant.enabled {
                continue;
            }
            if let Some(limit) = grant.numeric_limit {
                default_limits.insert(grant.feature.as_str().to_owned(), limit);
            }
            features.insert(grant.feature);
        }

        Ok(Some(PlanGrant {
            features,
            default_limits,
        }))
    }
}

/// A plan resolved with both of its grant tables (the quota half the
/// `PlanMatrixPort` does not cover).
#[derive(Debug, Clone)]
pub struct PostgresPlanResolver {
    pool: PgPool,
}

impl PostgresPlanResolver {
    /// Builds a resolver over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Resolves a plan code into everything it grants.
    ///
    /// An unknown code yields a [`ResolvedPlan`] with `plan: None` rather than
    /// an error: the caller decides whether that is fatal, and for a tenant on
    /// a plan that has since been deleted the correct answer is usually
    /// "no features", not "500".
    pub async fn resolve(&self, plan_code: &str) -> Result<ResolvedPlan> {
        let store = PostgresPlanStore::new(self.pool.clone());
        let Some(plan) = store.by_code(plan_code).await? else {
            return Ok(ResolvedPlan::default());
        };

        let entitlement_rows: Vec<PlanEntitlementRow> = sqlx::query_as::<_, PlanEntitlementRow>(
            "SELECT plan_id, feature, enabled, numeric_limit, created_at
               FROM plan_entitlements WHERE plan_id = $1",
        )
        .bind(plan.id.into_uuid())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        let quota_rows: Vec<PlanQuotaRow> = sqlx::query_as::<_, PlanQuotaRow>(
            "SELECT plan_id, dimension, period, max_value, enforcement, created_at
               FROM plan_quotas WHERE plan_id = $1",
        )
        .bind(plan.id.into_uuid())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        let mut features = std::collections::BTreeMap::new();
        for row in entitlement_rows {
            let grant = row.into_domain()?;
            if grant.enabled {
                features.insert(grant.feature, grant.numeric_limit);
            }
        }
        let mut quotas = std::collections::BTreeMap::new();
        for row in quota_rows {
            let quota = row.into_domain()?;
            quotas.insert(
                ResolvedPlan::quota_key(&quota.dimension, &quota.period),
                quota,
            );
        }

        Ok(ResolvedPlan {
            plan: Some(plan),
            features,
            quotas,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_status_in_a_row_is_a_loud_error() {
        let row = PlanRow {
            id: uuid::Uuid::new_v4(),
            code: "team".to_owned(),
            name: "Team".to_owned(),
            description: None,
            spec: serde_json::Value::Null,
            status: "discontinued".to_owned(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        assert!(
            row.into_domain().is_err(),
            "schema drift must fail loudly, not default to a status"
        );
    }

    #[test]
    fn the_quota_map_key_is_dimension_scoped_by_period() {
        let key = ResolvedPlan::quota_key(&QuotaDimension::Tokens, &QuotaPeriod::Monthly);
        assert_eq!(key, "tokens:monthly");
        let hourly = ResolvedPlan::quota_key(&QuotaDimension::Tokens, &QuotaPeriod::Hourly);
        assert_ne!(key, hourly, "the same dimension may carry several periods");
    }
}
