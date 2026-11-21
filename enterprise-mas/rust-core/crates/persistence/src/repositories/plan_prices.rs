//! PURPOSE: The plan <-> billing-provider price catalogue repository.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: answer "which provider price charges for this
//! plan on this interval" for the checkout flow, and record the mapping
//! idempotently. Amounts are integer minor units; a `NULL` amount stays
//! `None` rather than becoming a fabricated zero.
//!
//! Security boundary: every statement binds `tenant_id` inside the request
//! scope. The pre-scope webhook path never reads this table -- it resolves
//! prices only after the tenant is known.
//!
//! Data ownership: provider price ids are opaque handles; no provider secret
//! or key is ever stored here.
//!
//! It must NOT:
//!   * return a price whose `active` flag is false for a new checkout;
//!   * let one provider price id map to two rows (the unique index is the
//!     control);
//!   * convert money to or from a float anywhere in this module.

use mas_common::error::AppError;
use mas_common::ids::{PlanId, PlanPriceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::billing::{BillingInterval, PlanPrice};
use sqlx::PgPool;
use sqlx::Row;

use crate::error::map_sqlx;
use crate::scoped_request;

/// Postgres implementation of the plan price catalogue.
#[derive(Debug, Clone)]
pub struct PostgresPlanPriceStore {
    _pool: PgPool,
}

impl PostgresPlanPriceStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }

    /// Records a mapping, refusing a second row for the same slot.
    ///
    /// Idempotent on `(tenant_id, provider, plan_id, billing_interval)`:
    /// re-registering the same price returns the existing row. A different
    /// provider price id for an occupied slot is a [`AppError::conflict`] --
    /// silently replacing it would re-point live subscriptions.
    ///
    /// # Errors
    ///
    /// [`AppError::conflict`] on a slot already mapped to another provider
    /// price; [`AppError::database`] on storage failure.
    pub async fn upsert(&self, price: &PlanPrice) -> Result<PlanPrice> {
        let scope = scoped_request::current_scope()?;
        if scope.tenant_id() != price.tenant_id {
            return Err(AppError::forbidden(
                "plan price upsert outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let existing = sqlx::query(
            "SELECT provider_price_id FROM plan_prices \
             WHERE tenant_id = $1 AND provider = $2 AND plan_id = $3 AND billing_interval = $4",
        )
        .bind(price.tenant_id.as_uuid())
        .bind(&price.provider)
        .bind(price.plan_id.as_uuid())
        .bind(price.billing_interval.as_str())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if let Some(row) = existing {
            let recorded: String = row.try_get("provider_price_id").map_err(map_sqlx)?;
            if recorded != price.provider_price_id {
                return Err(AppError::conflict(
                    "plan price slot is already mapped to a different provider price",
                ));
            }
            return self
                .get(price.tenant_id, price.id)
                .await?
                .ok_or_else(|| AppError::database("plan price row vanished after upsert"));
        }
        sqlx::query(
            "INSERT INTO plan_prices \
             (id, tenant_id, plan_id, provider, provider_price_id, currency, unit_amount, billing_interval, active) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
        )
        .bind(price.id.as_uuid())
        .bind(price.tenant_id.as_uuid())
        .bind(price.plan_id.as_uuid())
        .bind(&price.provider)
        .bind(&price.provider_price_id)
        .bind(&price.currency)
        .bind(price.unit_amount)
        .bind(price.billing_interval.as_str())
        .bind(price.active)
        .execute(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        drop(executor);
        self.get(price.tenant_id, price.id)
            .await?
            .ok_or_else(|| AppError::database("plan price insert did not round-trip"))
    }

    /// Tenant-scoped get by row id.
    pub async fn get(&self, tenant: TenantId, id: PlanPriceId) -> Result<Option<PlanPrice>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "plan price read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT bp.id, bp.tenant_id, bp.plan_id, p.code AS plan_code, bp.provider, \
                    bp.provider_price_id, bp.currency, bp.unit_amount, \
             billing_interval, active, created_at, updated_at \
             FROM plan_prices WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_price(&row)).transpose()
    }

    /// The active price for a plan slot, if one is registered.
    pub async fn get_active(
        &self,
        tenant: TenantId,
        provider: &str,
        plan_id: PlanId,
        interval: BillingInterval,
    ) -> Result<Option<PlanPrice>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "plan price read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT bp.id, bp.tenant_id, bp.plan_id, p.code AS plan_code, bp.provider, \
                    bp.provider_price_id, bp.currency, bp.unit_amount, \
             billing_interval, active, created_at, updated_at \
             FROM plan_prices \
             WHERE tenant_id = $1 AND provider = $2 AND plan_id = $3 AND billing_interval = $4 AND active = true",
        )
        .bind(tenant.as_uuid())
        .bind(provider)
        .bind(plan_id.as_uuid())
        .bind(interval.as_str())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_price(&row)).transpose()
    }

    /// Tenant-scoped lookup by provider price handle.
    pub async fn get_by_provider_price(
        &self,
        tenant: TenantId,
        provider: &str,
        provider_price_id: &str,
    ) -> Result<Option<PlanPrice>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "plan price read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT bp.id, bp.tenant_id, bp.plan_id, p.code AS plan_code, bp.provider, \
                    bp.provider_price_id, bp.currency, bp.unit_amount, \
             billing_interval, active, created_at, updated_at \
             FROM plan_prices bp JOIN plans p ON p.id = bp.plan_id \
             WHERE bp.tenant_id = $1 AND bp.provider = $2 AND bp.provider_price_id = $3",
        )
        .bind(tenant.as_uuid())
        .bind(provider)
        .bind(provider_price_id)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_price(&row)).transpose()
    }
}

fn row_to_price(row: &sqlx::postgres::PgRow) -> Result<PlanPrice> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let plan: uuid::Uuid = row.try_get("plan_id").map_err(map_sqlx)?;
    let interval_raw: String = row.try_get("billing_interval").map_err(map_sqlx)?;
    let interval: BillingInterval = interval_raw
        .parse()
        .map_err(|_| AppError::database("plan_prices.billing_interval drifted from the enum"))?;
    let plan_code: String = row.try_get("plan_code").map_err(map_sqlx)?;
    Ok(PlanPrice {
        id: PlanPriceId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        plan_id: PlanId::from_uuid(plan),
        plan_code,
        provider: row.try_get("provider").map_err(map_sqlx)?,
        provider_price_id: row.try_get("provider_price_id").map_err(map_sqlx)?,
        currency: row.try_get("currency").map_err(map_sqlx)?,
        unit_amount: row.try_get("unit_amount").map_err(map_sqlx)?,
        billing_interval: interval,
        active: row.try_get("active").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    })
}

use mas_application::billing_service::PlanPriceStorePort as PlanPricePortTrait;

#[async_trait::async_trait]
impl PlanPricePortTrait for PostgresPlanPriceStore {
    async fn get_active(
        &self,
        tenant: TenantId,
        provider: &str,
        plan_id: PlanId,
        interval: BillingInterval,
    ) -> Result<Option<PlanPrice>> {
        PostgresPlanPriceStore::get_active(self, tenant, provider, plan_id, interval).await
    }

    async fn get_by_provider_price(
        &self,
        tenant: TenantId,
        provider: &str,
        provider_price_id: &str,
    ) -> Result<Option<PlanPrice>> {
        PostgresPlanPriceStore::get_by_provider_price(self, tenant, provider, provider_price_id)
            .await
    }
}
