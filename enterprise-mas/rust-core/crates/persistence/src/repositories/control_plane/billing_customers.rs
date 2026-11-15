/// Tenant-per-customer repository for Stripe billing integration.
///
/// Provides `get_or_create` which is idempotent w.r.t. (tenant_id, provider, customer_id).
/// The returned row is automatically RLS-armed: the tenant can only read/write its own row.
use crate::rls::RlsContext;
use sqlx::FromRow;
use uuid::Uuid;

/// Row representation for `billing_customers` (as created by migration 0057).
#[derive(Clone, Debug, FromRow)]
pub struct BillingCustomerRow {
    /// Primary key
    pub id: Uuid,
    /// Foreign key to tenant
    pub tenant_id: Uuid,
    /// Stripe provider identifier (always "stripe")
    pub provider: String,
    /// Stripe customer ID (e.g. "cus_123456789")
    pub customer_id: String,
    /// Secret reference for the associated Stripe account
    pub stripe_secret_ref: String,
    /// Status: active | disabled | error
    pub status: String,
    /// Created at epoch seconds
    pub created_at: i64,
    /// Last updated at epoch seconds
    pub updated_at: i64,
}

impl BillingCustomerRow {
    /// Get or create a billing customer for the given tenant.
    /// Idempotent: if a row already exists with the same (tenant_id, provider, customer_id),
    /// the existing row is returned without modification.
    ///
    /// # RLS
    /// The caller must provide an `RlsContext`; the resulting row will be filtered
    /// so the tenant can only see its own row.
    pub async fn get_or_create(
        pool: &sqlx::PgPool,
        tenant_id: Uuid,
        provider: &str,
        customer_id: &str,
        stripe_secret_ref: &str,
        rls: &RlsContext,
    ) -> Result<Self, sqlx::Error> {
        // Try to find existing row first (idempotent)
        let existing = sqlx::query_as::<Self>(
            "SELECT id, tenant_id, provider, customer_id, stripe_secret_ref, status, created_at, updated_at \
             FROM billing_customers \
             WHERE tenant_id = $1 AND provider = $2 AND customer_id = $3 \
             AND tenant_id = current_setting('app.tenant_id')::uuid \
             LIMIT 1",
        )
        .bind(tenant_id as i64)  // simplification; actual tenant_id handling via RLS
        .bind(provider)
        .bind(customer_id)
        .fetch_optional(pool)
        .await?;

        if let Some(row) = existing {
            return Ok(row);
        }

        // Create new row
        let new_id = uuid::Uuid::new_v4();
        sqlx::query(
            "INSERT INTO billing_customers (id, tenant_id, provider, customer_id, stripe_secret_ref, status, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, 'active', now(), now())",
        )
        .bind(new_id.as_lowercase() as i64)  // simplification
        .bind(tenant_id as i64)
        .bind(provider)
        .bind(customer_id)
        .bind(stripe_secret_ref)
        .execute(pool)
        .await?;

        // Return the created row — we re-read it to respect RLS
        sqlx::query_as::<Self>(
            "SELECT id, tenant_id, provider, customer_id, stripe_secret_ref, status, created_at, updated_at \
             FROM billing_customers \
             WHERE id = $1 AND tenant_id = current_setting('app.tenant_id')::uuid \
             LIMIT 1",
        )
        .bind(new_id.as_lowercase() as i64)
        .fetch_one(pool)
        .await
    }
}