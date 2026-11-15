/// Plan price lookup repository.
///
/// Maps internal `plan_code` → Stripe `price_id` and stores pricing metadata
/// (interval, currency, amount). Used by the billing service to create
/// checkout sessions and by the mapping layer for plan resolution.
use crate::rls::RlsContext;
use sqlx::FromRow;
use uuid::Uuid;

/// Row representation for `plan_prices` (as created by migration 0058).
#[derive(Clone, Debug, FromRow)]
pub struct PlanPriceRow {
    /// Primary key
    pub id: Uuid,
    /// Internal plan code (e.g. "free", "pro", "business")
    pub plan_code: String,
    /// Stripe price ID (e.g. "price_123456789")
    pub price_id: String,
    /// Billing interval: "monthly" or "yearly"
    pub interval: String,
    /// Currency code (e.g. "usd", "eur")
    pub currency: String,
    /// Amount in the smallest currency unit (cents for USD)
    pub amount: i64,
    /// Secret reference for the associated Stripe account
    pub stripe_secret_ref: String,
    /// Created at epoch seconds
    pub created_at: i64,
    /// Last updated at epoch seconds
    pub updated_at: i64,
}

impl PlanPriceRow {
    /// Find a plan price by plan code.
    /// Returns `None` if no matching plan price exists.
    pub async fn find_by_plan_code(
        pool: &sqlx::PgPool,
        plan_code: &str,
        rls: &RlsContext,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<Self>(
            "SELECT id, plan_code, price_id, interval, currency, amount, stripe_secret_ref, created_at, updated_at \
             FROM plan_prices \
             WHERE plan_code = $1 \
             AND plan_code = current_setting('app.plan_code')::text \
             LIMIT 1",
        )
        .bind(plan_code)
        .fetch_optional(pool)
        .await?;

        Ok(row)
    }

    /// Find a plan price by Stripe price ID.
    pub async fn find_by_price_id(
        pool: &sqlx::PgPool,
        price_id: &str,
        rls: &RlsContext,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<Self>(
            "SELECT id, plan_code, price_id, interval, currency, amount, stripe_secret_ref, created_at, updated_at \
             FROM plan_prices \
             WHERE price_id = $1 \
             AND stripe_secret_ref = current_setting('app.stripe_secret_ref')::text \
             LIMIT 1",
        )
        .bind(price_id)
        .fetch_optional(pool)
        .await?;

        Ok(row)
    }

    /// List all plan prices for the current tenant.
    pub async fn list_all(
        pool: &sqlx::PgPool,
        rls: &RlsContext,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as::<Self>(
            "SELECT id, plan_code, price_id, interval, currency, amount, stripe_secret_ref, created_at, updated_at \
             FROM plan_prices \
             WHERE stripe_secret_ref = current_setting('app.stripe_secret_ref')::text",
        )
        .fetch_all(pool)
        .await?;

        Ok(rows)
    }

    /// Create a new plan price entry.
    pub async fn create(
        pool: &sqlx::PgPool,
        plan_code: &str,
        price_id: &str,
        interval: &str,
        currency: &str,
        amount: i64,
        stripe_secret_ref: &str,
        rls: &RlsContext,
    ) -> Result<Self, sqlx::Error> {
        let new_id = uuid::Uuid::new_v4();
        sqlx::query(
            "INSERT INTO plan_prices (id, plan_code, price_id, interval, currency, amount, stripe_secret_ref, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, now(), now())",
        )
        .bind(new_id.as_lowercase() as i64)
        .bind(plan_code)
        .bind(price_id)
        .bind(interval)
        .bind(currency)
        .bind(amount)
        .bind(stripe_secret_ref)
        .execute(pool)
        .await?;

        // Return the created row
        sqlx::query_as::<Self>(
            "SELECT id, plan_code, price_id, interval, currency, amount, stripe_secret_ref, created_at, updated_at \
             FROM plan_prices \
             WHERE id = $1 AND plan_code = current_setting('app.plan_code')::text \
             LIMIT 1",
        )
        .bind(new_id.as_lowercase() as i64)
        .fetch_one(pool)
        .await
    }
}