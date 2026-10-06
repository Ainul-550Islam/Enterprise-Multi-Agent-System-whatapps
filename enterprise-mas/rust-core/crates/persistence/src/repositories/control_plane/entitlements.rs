//! PURPOSE: Entitlement persistence and the canonical override layer.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: store what a tenant is entitled to — the
//! subscription-scoped rows — and the negotiated deviations from its plan.
//! Both are read server-side during resolution; neither is ever accepted from
//! a request body.
//!
//! Security boundary: tenant-scoped and RLS-protected. The override methods
//! require an actor and a reason on every write, and the partial unique index
//! over live overrides means two concurrent grants of the same override
//! cannot both land.
//!
//! Data ownership: Rust-owned platform state.
//!
//! It must NOT:
//!   • let a caller submit an entitlement claim — resolution reads these rows;
//!   • modify a plan to accommodate one tenant (that is what overrides are
//!     for, and the reason is why the two tables are separate);
//!   • allow two live overrides for the same (tenant, target);
//!   • let an expired override still apply — `list_effective` filters on the
//!     clock, and the query is half-open so a boundary instant is unambiguous.

use mas_common::error::AppError;
use mas_common::ids::{OverrideId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::entitlement::{Entitlement, EntitlementSource, FeatureKey};
use mas_domain::overrides::{EntitlementOverride, OverrideActor, QuotaOverride};
use mas_domain::quota::{EnforcementMode, QuotaDimension, QuotaPeriod};
use mas_domain::subscription::Subscription;
use mas_tenancy::entitlements::EntitlementStorePort;
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::map_sqlx;

/// One entitlement row as stored.
/// One `entitlement_overrides` row, as the database returns it.
///
/// A named alias rather than an inline type: the same shape is selected in
/// three places, and spelling it out each time made the query the least
/// readable part of the function.
type EntitlementOverrideRow = (
    uuid::Uuid,
    String,
    bool,
    Option<i64>,
    String,
    String,
    String,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);

/// One `quota_overrides` row, as the database returns it.
type QuotaOverrideRow = (
    uuid::Uuid,
    String,
    String,
    i64,
    String,
    String,
    String,
    String,
    chrono::DateTime<chrono::Utc>,
    Option<chrono::DateTime<chrono::Utc>>,
    chrono::DateTime<chrono::Utc>,
    chrono::DateTime<chrono::Utc>,
);

/// One persisted entitlement row.
pub struct EntitlementRow {
    /// Owning tenant.
    pub tenant_id: uuid::Uuid,
    /// Feature key.
    pub feature: String,
    /// Whether the grant is on.
    pub enabled: bool,
    /// Optional numeric cap.
    pub numeric_limit: Option<i64>,
    /// Provenance.
    pub source: String,
    /// Expiry.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Creation instant.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last mutation.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for EntitlementRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            tenant_id: row.try_get("tenant_id")?,
            feature: row.try_get("feature")?,
            enabled: row.try_get("enabled")?,
            numeric_limit: row.try_get("numeric_limit")?,
            source: row.try_get("source")?,
            expires_at: row.try_get("expires_at")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl EntitlementRow {
    /// Hydrates into the domain aggregate.
    pub fn into_domain(self) -> Result<Entitlement> {
        let feature = FeatureKey::new(self.feature.clone())
            .map_err(|_| AppError::database(format!("invalid feature key '{}'", self.feature)))?;
        let source = self.source.parse::<EntitlementSource>().map_err(|_| {
            AppError::database(format!("unknown entitlement source '{}'", self.source))
        })?;
        Ok(Entitlement {
            tenant_id: TenantId::from_uuid(self.tenant_id),
            feature,
            enabled: self.enabled,
            limit: self.numeric_limit.and_then(|v| u64::try_from(v).ok()),
            source,
            expires_at: self.expires_at.map(Timestamp::from_datetime),
            created_at: Timestamp::from_datetime(self.created_at),
            updated_at: Timestamp::from_datetime(self.updated_at),
        })
    }
}

/// The entitlement store: subscriptions, entitlements, and both override
/// tables behind one handle.
#[derive(Debug, Clone)]
pub struct PostgresEntitlementStore {
    pool: PgPool,
}

impl PostgresEntitlementStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Grants or replaces a tenant-level entitlement row.
    pub async fn grant(&self, entitlement: &Entitlement) -> Result<()> {
        sqlx::query(
            "INSERT INTO entitlements
                (tenant_id, feature, enabled, numeric_limit, source, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (tenant_id, feature) DO UPDATE
                SET enabled = EXCLUDED.enabled,
                    numeric_limit = EXCLUDED.numeric_limit,
                    source = EXCLUDED.source,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = now()",
        )
        .bind(entitlement.tenant_id.into_uuid())
        .bind(entitlement.feature.as_str())
        .bind(entitlement.enabled)
        .bind(
            entitlement
                .limit
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
        )
        .bind(entitlement.source.as_str())
        .bind(entitlement.expires_at.map(Timestamp::into_datetime))
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    /// Every entitlement row for a tenant, expired ones included.
    pub async fn list_rows(&self, tenant: TenantId) -> Result<Vec<Entitlement>> {
        let rows: Vec<EntitlementRow> = sqlx::query_as::<_, EntitlementRow>(
            "SELECT tenant_id, feature, enabled, numeric_limit, source, expires_at,
                    created_at, updated_at
               FROM entitlements
              WHERE tenant_id = $1
              ORDER BY feature",
        )
        .bind(tenant.into_uuid())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(EntitlementRow::into_domain).collect()
    }
}

#[async_trait::async_trait]
impl EntitlementStorePort for PostgresEntitlementStore {
    async fn get_subscription(&self, tenant_id: TenantId) -> Result<Option<Subscription>> {
        type Row = (
            String,
            String,
            Option<i64>,
            Option<chrono::DateTime<chrono::Utc>>,
            chrono::DateTime<chrono::Utc>,
        );
        let row: Option<Row> = sqlx::query_as(
            "SELECT plan, state, (spec->>'seats')::bigint, renews_at, updated_at
               FROM subscriptions
              WHERE tenant_id = $1
              ORDER BY created_at DESC
              LIMIT 1",
        )
        .bind(tenant_id.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        let Some((plan_code, state, seats, renews_at, updated_at)) = row else {
            return Ok(None);
        };
        let state = SubscriptionStateOrError::parse(&state)?;
        Ok(Some(Subscription {
            tenant_id,
            plan_code,
            state,
            seats: seats.and_then(|s| u32::try_from(s).ok()),
            renews_at: renews_at.map(Timestamp::from_datetime),
            updated_at: Timestamp::from_datetime(updated_at),
        }))
    }

    async fn upsert_subscription(&self, subscription: &Subscription) -> Result<()> {
        // A plain INSERT on purpose. The partial unique index over live states
        // is what rejects a second live subscription, and it does so without
        // the caller having to invent a conflict target whose predicate has to
        // match the index text character for character.
        let result = sqlx::query(
            "INSERT INTO subscriptions (tenant_id, plan, state, spec)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(subscription.tenant_id.into_uuid())
        .bind(&subscription.plan_code)
        .bind(subscription.state.as_str())
        .bind(serde_json::json!({}))
        .execute(executor(&self.pool).await?.connection())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if crate::error::is_unique_violation(&error) => Err(AppError::conflict(
                format!(
                    "tenant {} already has a live subscription; transition it instead of creating another",
                    subscription.tenant_id
                ),
            )),
            Err(error) => Err(map_sqlx(error)),
        }
    }

    async fn list_entitlements(&self, tenant_id: TenantId) -> Result<Vec<Entitlement>> {
        self.list_rows(tenant_id).await
    }

    async fn upsert_entitlement(&self, entitlement: &Entitlement) -> Result<()> {
        self.grant(entitlement).await
    }
}

/// Small helper so a bad state string produces a database error rather than a
/// panic or a silent default.
struct SubscriptionStateOrError;

impl SubscriptionStateOrError {
    fn parse(raw: &str) -> Result<mas_domain::subscription::SubscriptionState> {
        raw.parse::<mas_domain::subscription::SubscriptionState>()
            .map_err(|_| AppError::database(format!("unknown subscription state '{raw}'")))
    }
}

// ---------------------------------------------------------------------------
// Overrides
// ---------------------------------------------------------------------------

/// The override store: negotiated deviations, both feature and quota.
#[derive(Debug, Clone)]
pub struct PostgresOverrideStore {
    pool: PgPool,
}

impl PostgresOverrideStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Records a feature override.
    ///
    /// A second live override for the same (tenant, feature) is refused by the
    /// partial unique index. That is deliberate: two live values would make
    /// the effective limit depend on row order, and silently replacing one
    /// would destroy the record of who granted the first.
    pub async fn grant_feature(&self, override_: &EntitlementOverride) -> Result<OverrideId> {
        let id = override_.id;
        let result = sqlx::query(
            "INSERT INTO entitlement_overrides
                (id, tenant_id, feature, enabled, numeric_limit, reason,
                 actor_id, actor_kind, effective_from, effective_to)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(id.into_uuid())
        .bind(override_.tenant_id.into_uuid())
        .bind(override_.feature.as_str())
        .bind(override_.enabled)
        .bind(
            override_
                .numeric_limit
                .map(|v| i64::try_from(v).unwrap_or(i64::MAX)),
        )
        .bind(&override_.reason)
        .bind(&override_.actor.id)
        .bind(override_.actor.kind.as_str())
        .bind(override_.effective_from.into_datetime())
        .bind(override_.effective_to.map(Timestamp::into_datetime))
        .execute(executor(&self.pool).await?.connection())
        .await;
        match result {
            Ok(_) => Ok(id),
            Err(error) if crate::error::is_unique_violation(&error) => Err(AppError::conflict(
                format!(
                    "tenant {} already has a live override for feature '{}'; end it before granting another",
                    override_.tenant_id, override_.feature
                ),
            )),
            Err(error) => Err(map_sqlx(error)),
        }
    }

    /// Records a quota override.
    pub async fn grant_quota(&self, override_: &QuotaOverride) -> Result<OverrideId> {
        let id = override_.id;
        let result = sqlx::query(
            "INSERT INTO quota_overrides
                (id, tenant_id, dimension, period, max_value, enforcement, reason,
                 actor_id, actor_kind, effective_from, effective_to)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(id.into_uuid())
        .bind(override_.tenant_id.into_uuid())
        .bind(override_.dimension.as_str())
        .bind(override_.period.as_str())
        .bind(i64::try_from(override_.max_value).unwrap_or(i64::MAX))
        .bind(override_.enforcement.as_str())
        .bind(&override_.reason)
        .bind(&override_.actor.id)
        .bind(override_.actor.kind.as_str())
        .bind(override_.effective_from.into_datetime())
        .bind(override_.effective_to.map(Timestamp::into_datetime))
        .execute(executor(&self.pool).await?.connection())
        .await;
        match result {
            Ok(_) => Ok(id),
            Err(error) if crate::error::is_unique_violation(&error) => Err(AppError::conflict(
                format!(
                    "tenant {} already has a live override for {}/{}; end it before granting another",
                    override_.tenant_id, override_.dimension, override_.period
                ),
            )),
            Err(error) => Err(map_sqlx(error)),
        }
    }

    /// Ends a feature override at `at`.
    pub async fn end_feature(
        &self,
        tenant: TenantId,
        feature: &FeatureKey,
        at: &Timestamp,
    ) -> Result<()> {
        let affected = sqlx::query(
            // Ending early is allowed; ending *late* would extend the grant,
            // and that is the one outcome an operator must not be able to
            // reach by accident. `effective_to IS NULL OR effective_to > $3`
            // admits the first and refuses the second.
            "UPDATE entitlement_overrides
                SET effective_to = $3, updated_at = now()
              WHERE tenant_id = $1 AND feature = $2
                AND (effective_to IS NULL OR effective_to > $3)",
        )
        .bind(tenant.into_uuid())
        .bind(feature.as_str())
        .bind(at.into_datetime())
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        if affected.rows_affected() == 0 {
            return Err(AppError::not_found(
                "entitlement_override",
                format!("{tenant}/{feature}"),
            ));
        }
        Ok(())
    }

    /// Ends a quota override at `at`.
    pub async fn end_quota(
        &self,
        tenant: TenantId,
        dimension: QuotaDimension,
        period: QuotaPeriod,
        at: &Timestamp,
    ) -> Result<()> {
        let affected = sqlx::query(
            "UPDATE quota_overrides
                SET effective_to = $4, updated_at = now()
              WHERE tenant_id = $1 AND dimension = $2 AND period = $3
                AND effective_to IS NULL",
        )
        .bind(tenant.into_uuid())
        .bind(dimension.as_str())
        .bind(period.as_str())
        .bind(at.into_datetime())
        .execute(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        if affected.rows_affected() == 0 {
            return Err(AppError::not_found(
                "quota_override",
                format!("{tenant}/{dimension}/{period}"),
            ));
        }
        Ok(())
    }

    /// Every feature override in force at `at`, newest first.
    ///
    /// The interval is half-open in SQL exactly as it is in the domain type,
    /// so an override that expires at the instant of evaluation is already
    /// gone and cannot race with its replacement.
    pub async fn effective_features(
        &self,
        tenant: TenantId,
        at: &Timestamp,
    ) -> Result<Vec<EntitlementOverride>> {
        let rows: Vec<EntitlementOverrideRow> = sqlx::query_as(
            "SELECT id, feature, enabled, numeric_limit, reason, actor_id, actor_kind,
                    effective_from, effective_to, created_at, updated_at
               FROM entitlement_overrides
              WHERE tenant_id = $1
                AND effective_from <= $2
                AND (effective_to IS NULL OR $2 < effective_to)
              ORDER BY effective_from DESC",
        )
        .bind(tenant.into_uuid())
        .bind(at.into_datetime())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        rows.into_iter()
            .map(
                |(
                    id,
                    feature,
                    enabled,
                    numeric_limit,
                    reason,
                    actor_id,
                    actor_kind,
                    effective_from,
                    effective_to,
                    created_at,
                    updated_at,
                )| {
                    let actor_kind = actor_kind
                        .parse::<mas_domain::overrides::ActorKind>()
                        .unwrap_or(mas_domain::overrides::ActorKind::Service);
                    Ok(EntitlementOverride {
                        id: OverrideId::from_uuid(id),
                        tenant_id: tenant,
                        feature: FeatureKey::new(feature)?,
                        enabled,
                        numeric_limit: numeric_limit.and_then(|v| u64::try_from(v).ok()),
                        reason,
                        actor: OverrideActor::new(actor_kind, actor_id)?,
                        effective_from: Timestamp::from_datetime(effective_from),
                        effective_to: effective_to.map(Timestamp::from_datetime),
                        created_at: Timestamp::from_datetime(created_at),
                        updated_at: Timestamp::from_datetime(updated_at),
                    })
                },
            )
            .collect()
    }

    /// Every quota override in force at `at`, newest first.
    pub async fn effective_quotas(
        &self,
        tenant: TenantId,
        at: &Timestamp,
    ) -> Result<Vec<QuotaOverride>> {
        let rows: Vec<QuotaOverrideRow> = sqlx::query_as(
            "SELECT id, dimension, period, max_value, enforcement, reason, actor_id,
                    actor_kind, effective_from, effective_to, created_at, updated_at
               FROM quota_overrides
              WHERE tenant_id = $1
                AND effective_from <= $2
                AND (effective_to IS NULL OR $2 < effective_to)
              ORDER BY effective_from DESC",
        )
        .bind(tenant.into_uuid())
        .bind(at.into_datetime())
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        rows.into_iter()
            .map(
                |(
                    id,
                    dimension,
                    period,
                    max_value,
                    enforcement,
                    reason,
                    actor_id,
                    actor_kind,
                    effective_from,
                    effective_to,
                    created_at,
                    updated_at,
                )| {
                    let dimension = dimension.parse::<QuotaDimension>().map_err(|_| {
                        AppError::database(format!("unknown quota dimension '{dimension}'"))
                    })?;
                    let period = period.parse::<QuotaPeriod>().map_err(|_| {
                        AppError::database(format!("unknown quota period '{period}'"))
                    })?;
                    let enforcement = enforcement.parse::<EnforcementMode>().map_err(|_| {
                        AppError::database(format!("unknown enforcement mode '{enforcement}'"))
                    })?;
                    let actor_kind = actor_kind
                        .parse::<mas_domain::overrides::ActorKind>()
                        .unwrap_or(mas_domain::overrides::ActorKind::Service);
                    Ok(QuotaOverride {
                        id: OverrideId::from_uuid(id),
                        tenant_id: tenant,
                        dimension,
                        period,
                        max_value: u64::try_from(max_value).unwrap_or(0),
                        enforcement,
                        reason,
                        actor: OverrideActor::new(actor_kind, actor_id)?,
                        effective_from: Timestamp::from_datetime(effective_from),
                        effective_to: effective_to.map(Timestamp::from_datetime),
                        created_at: Timestamp::from_datetime(created_at),
                        updated_at: Timestamp::from_datetime(updated_at),
                    })
                },
            )
            .collect()
    }

    /// Every override a tenant has ever had, including ended ones (audit view).
    pub async fn feature_history(
        &self,
        tenant: TenantId,
        limit: i64,
    ) -> Result<Vec<EntitlementOverride>> {
        let rows: Vec<EntitlementOverrideRow> = sqlx::query_as(
            "SELECT id, feature, enabled, numeric_limit, reason, actor_id, actor_kind,
                    effective_from, effective_to, created_at, updated_at
               FROM entitlement_overrides
              WHERE tenant_id = $1
              ORDER BY effective_from DESC
              LIMIT $2",
        )
        .bind(tenant.into_uuid())
        .bind(limit.max(1))
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(
                |(
                    id,
                    feature,
                    enabled,
                    numeric_limit,
                    reason,
                    actor_id,
                    actor_kind,
                    effective_from,
                    effective_to,
                    created_at,
                    updated_at,
                )| {
                    let actor_kind = actor_kind
                        .parse::<mas_domain::overrides::ActorKind>()
                        .unwrap_or(mas_domain::overrides::ActorKind::Service);
                    Ok(EntitlementOverride {
                        id: OverrideId::from_uuid(id),
                        tenant_id: tenant,
                        feature: FeatureKey::new(feature)?,
                        enabled,
                        numeric_limit: numeric_limit.and_then(|v| u64::try_from(v).ok()),
                        reason,
                        actor: OverrideActor::new(actor_kind, actor_id)?,
                        effective_from: Timestamp::from_datetime(effective_from),
                        effective_to: effective_to.map(Timestamp::from_datetime),
                        created_at: Timestamp::from_datetime(created_at),
                        updated_at: Timestamp::from_datetime(updated_at),
                    })
                },
            )
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_source_in_a_row_is_a_loud_error() {
        let row = EntitlementRow {
            tenant_id: TenantId::new().into_uuid(),
            feature: "audit_export".to_owned(),
            enabled: true,
            numeric_limit: None,
            source: "vibes".to_owned(),
            expires_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        assert!(
            row.into_domain().is_err(),
            "schema drift must fail loudly, never default a source"
        );
    }

    #[test]
    fn a_row_hydrates_with_its_cap_and_provenance() {
        let row = EntitlementRow {
            tenant_id: TenantId::new().into_uuid(),
            feature: "audit_export".to_owned(),
            enabled: true,
            numeric_limit: Some(5),
            source: "contract_override".to_owned(),
            expires_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let entitlement = row.into_domain().expect("hydrate");
        assert_eq!(entitlement.limit, Some(5));
        assert_eq!(entitlement.source, EntitlementSource::ContractOverride);
        assert!(entitlement.is_active(&Timestamp::now()));
    }
}
