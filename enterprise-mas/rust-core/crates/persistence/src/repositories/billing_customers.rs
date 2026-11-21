//! PURPOSE: The tenant <-> billing-provider customer mapping repository.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: give the checkout flow an idempotent way to
//! record "this tenant is customer `cus_...` at this provider", and give the
//! webhook path the *only* sanctioned way to answer "which tenant does
//! `cus_...` belong to". The second question is answered by the
//! `mas_resolve_billing_tenant` SECURITY DEFINER function (migration 0057),
//! because the webhook has no tenant scope yet -- exactly the 0015 pattern:
//! confine the exception instead of widening the rule. Everything else runs
//! under the request scope with `tenant_id` bound in SQL.
//!
//! Security boundary: the tenant lookup is the cross-tenant control on the
//! webhook path. It returns a tenant id from platform truth only; a caller
//! that gets `None` must reject the event rather than trust a tenant id from
//! the body.
//!
//! Data ownership: provider identifiers are opaque handles; no provider
//! secret or key is ever stored here.
//!
//! It must NOT:
//!   * expose an unscoped list or scan of customer mappings;
//!   * let one provider customer map to two tenants (the unique index is the
//!     control and the code never works around it);
//!   * read the environment or configuration.

use mas_common::error::AppError;
use mas_common::ids::{BillingCustomerId, OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::billing::BillingCustomer;
use sqlx::PgPool;
use sqlx::Row;

use crate::error::map_sqlx;
use crate::scoped_request;

/// Postgres implementation of the billing customer boundary.
#[derive(Debug, Clone)]
pub struct PostgresBillingCustomerStore {
    pool: PgPool,
}

impl PostgresBillingCustomerStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Resolves which tenant (and organization) owns a provider customer --
    /// **pre-scope**.
    ///
    /// Runs the migration-0057 SECURITY DEFINER function, which is the only
    /// unscoped read of this table. `None` means no platform row claims this
    /// customer; the caller must then reject the event. The organization is
    /// returned alongside the tenant so the caller can open an authoritative
    /// scope.
    ///
    /// # Errors
    ///
    /// [`AppError::database`] when the lookup itself fails.
    pub async fn resolve_owner(
        &self,
        provider: &str,
        provider_customer_id: &str,
    ) -> Result<Option<(TenantId, OrganizationId)>> {
        let row: Option<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
            "SELECT tenant_id, organization_id FROM mas_resolve_billing_tenant($1, $2)",
        )
        .bind(provider)
        .bind(provider_customer_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(row.map(|(tenant, org)| (TenantId::from_uuid(tenant), OrganizationId::from_uuid(org))))
    }

    /// Records the mapping, or returns the existing one.
    ///
    /// Idempotent on `(tenant_id, provider)`: a retried checkout finds the
    /// customer it already created. A conflict on `(provider,
    /// provider_customer_id)` -- the same provider customer claimed by another
    /// tenant -- is refused, never silently reassigned.
    ///
    /// # Errors
    ///
    /// [`AppError::conflict`] when the provider customer belongs to a
    /// different tenant; [`AppError::database`] on storage failure.
    pub async fn create_or_get(&self, customer: &BillingCustomer) -> Result<BillingCustomer> {
        let scope = scoped_request::current_scope()?;
        if scope.tenant_id() != customer.tenant_id {
            return Err(AppError::forbidden(
                "billing customer create outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let inserted = sqlx::query(
            "INSERT INTO billing_customers (id, tenant_id, provider, provider_customer_id, email) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (tenant_id, provider) DO NOTHING \
             RETURNING id, tenant_id, provider, provider_customer_id, email, created_at, updated_at",
        )
        .bind(customer.id.as_uuid())
        .bind(customer.tenant_id.as_uuid())
        .bind(&customer.provider)
        .bind(&customer.provider_customer_id)
        .bind(&customer.email)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_conflict_or_sqlx)?;
        if let Some(row) = inserted {
            return row_to_customer(&row);
        }
        // The mapping already existed for this tenant: return it as-is. The
        // provider handle recorded at creation wins; a different handle in the
        // new request is a caller mistake, not a re-link.
        drop(executor);
        self.get_for_tenant_provider(customer.tenant_id, &customer.provider)
            .await?
            .ok_or_else(|| {
                AppError::database("billing customer insert conflicted but the row is gone")
            })
    }

    /// Tenant-scoped get by row id.
    pub async fn get(
        &self,
        tenant: TenantId,
        id: BillingCustomerId,
    ) -> Result<Option<BillingCustomer>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "billing customer read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, provider, provider_customer_id, email, created_at, updated_at \
             FROM billing_customers WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_customer(&row)).transpose()
    }

    /// Tenant-scoped lookup by provider handle.
    pub async fn get_by_provider_customer(
        &self,
        tenant: TenantId,
        provider: &str,
        provider_customer_id: &str,
    ) -> Result<Option<BillingCustomer>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "billing customer read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, provider, provider_customer_id, email, created_at, updated_at \
             FROM billing_customers WHERE tenant_id = $1 AND provider = $2 AND provider_customer_id = $3",
        )
        .bind(tenant.as_uuid())
        .bind(provider)
        .bind(provider_customer_id)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_customer(&row)).transpose()
    }

    /// Tenant-scoped lookup by provider label (the one-customer-per-provider
    /// invariant).
    pub async fn get_for_tenant_provider(
        &self,
        tenant: TenantId,
        provider: &str,
    ) -> Result<Option<BillingCustomer>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "billing customer read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, provider, provider_customer_id, email, created_at, updated_at \
             FROM billing_customers WHERE tenant_id = $1 AND provider = $2",
        )
        .bind(tenant.as_uuid())
        .bind(provider)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_customer(&row)).transpose()
    }
}

fn map_conflict_or_sqlx(error: sqlx::Error) -> AppError {
    if crate::error::is_unique_violation(&error) {
        AppError::conflict("the provider customer is already linked to a different tenant")
    } else {
        map_sqlx(error)
    }
}

fn row_to_customer(row: &sqlx::postgres::PgRow) -> Result<BillingCustomer> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    Ok(BillingCustomer {
        id: BillingCustomerId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        provider: row.try_get("provider").map_err(map_sqlx)?,
        provider_customer_id: row.try_get("provider_customer_id").map_err(map_sqlx)?,
        email: row.try_get("email").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    })
}

use mas_application::billing_service::BillingCustomerStorePort as BillingCustomerPortTrait;

#[async_trait::async_trait]
impl BillingCustomerPortTrait for PostgresBillingCustomerStore {
    async fn resolve_owner(
        &self,
        provider: &str,
        provider_customer_id: &str,
    ) -> Result<Option<(TenantId, OrganizationId)>> {
        PostgresBillingCustomerStore::resolve_owner(self, provider, provider_customer_id).await
    }

    async fn create_or_get(&self, customer: &BillingCustomer) -> Result<BillingCustomer> {
        PostgresBillingCustomerStore::create_or_get(self, customer).await
    }

    async fn get_for_tenant_provider(
        &self,
        tenant: TenantId,
        provider: &str,
    ) -> Result<Option<BillingCustomer>> {
        PostgresBillingCustomerStore::get_for_tenant_provider(self, tenant, provider).await
    }
}
