//! PURPOSE: Durable billing checkout sessions and the per-tenant event
//! application claim.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: make checkout exactly-once (one provider
//! session per `(tenant, idempotency_key)`) and make the *effect* of a
//! provider webhook event exactly-once per tenant. The claim is the
//! `INSERT ... ON CONFLICT DO NOTHING` on `billing_event_applications`: the row
//! that inserts wins the right to apply the effect; a conflict means the
//! effect already happened and the caller must not repeat it.
//!
//! Security boundary: every statement binds `tenant_id` inside the request
//! scope. The webhook resolves its tenant first (via
//! `mas_resolve_billing_tenant`) and then opens the scope before touching
//! anything here.
//!
//! Data ownership: provider session and event ids are opaque handles.
//!
//! It must NOT:
//!   * open a second session for a repeated idempotency key;
//!   * update or delete an application claim -- claims are append-only
//!     evidence (the schema enforces this too);
//!   * treat a claim conflict as an error -- it is the exactly-once success
//!     signal for a replay.

use mas_common::error::AppError;
use mas_common::ids::{
    BillingReceiptId, CheckoutSessionId, EventApplicationId, PlanId, PlanPriceId, TenantId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::billing::{CheckoutSessionRecord, CheckoutState, EventApplication};
use sqlx::PgPool;
use sqlx::Row;

use crate::error::map_sqlx;
use crate::scoped_request;

/// Postgres implementation of the checkout session log.
#[derive(Debug, Clone)]
pub struct PostgresCheckoutSessionStore {
    _pool: PgPool,
}

impl PostgresCheckoutSessionStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }

    /// Creates a session record, or returns the one the idempotency key
    /// already created.
    ///
    /// Returns `(record, created)` -- `created` is `false` when an earlier
    /// request with the same key already recorded this checkout, in which
    /// case the caller must reuse that provider session rather than open a
    /// second one.
    ///
    /// # Errors
    ///
    /// [`AppError::database`] on storage failure.
    pub async fn create_or_get(
        &self,
        record: &CheckoutSessionRecord,
    ) -> Result<(CheckoutSessionRecord, bool)> {
        let scope = scoped_request::current_scope()?;
        if scope.tenant_id() != record.tenant_id {
            return Err(AppError::forbidden(
                "checkout session create outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let inserted = sqlx::query(
            "INSERT INTO billing_checkout_sessions \
             (id, tenant_id, idempotency_key, plan_id, plan_code, plan_price_id, provider, provider_session_id, state, success_url, cancel_url) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (tenant_id, idempotency_key) DO NOTHING \
             RETURNING id, tenant_id, idempotency_key, plan_id, plan_code, plan_price_id, provider, \
                       provider_session_id, state, success_url, cancel_url, created_at, updated_at",
        )
        .bind(record.id.as_uuid())
        .bind(record.tenant_id.as_uuid())
        .bind(&record.idempotency_key)
        .bind(record.plan_id.as_uuid())
        .bind(&record.plan_code)
        .bind(record.plan_price_id.map(PlanPriceId::into_uuid))
        .bind(&record.provider)
        .bind(&record.provider_session_id)
        .bind(record.state.as_str())
        .bind(&record.success_url)
        .bind(&record.cancel_url)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if let Some(row) = inserted {
            return Ok((row_to_session(&row)?, true));
        }
        drop(executor);
        let existing = self
            .get_by_idempotency_key(record.tenant_id, &record.idempotency_key)
            .await?
            .ok_or_else(|| {
                AppError::database("checkout session insert conflicted but the row is gone")
            })?;
        Ok((existing, false))
    }

    /// Tenant-scoped lookup by idempotency key.
    pub async fn get_by_idempotency_key(
        &self,
        tenant: TenantId,
        idempotency_key: &str,
    ) -> Result<Option<CheckoutSessionRecord>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "checkout session read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, idempotency_key, plan_id, plan_code, plan_price_id, provider, \
                    provider_session_id, state, success_url, cancel_url, created_at, updated_at \
             FROM billing_checkout_sessions WHERE tenant_id = $1 AND idempotency_key = $2",
        )
        .bind(tenant.as_uuid())
        .bind(idempotency_key)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_session(&row)).transpose()
    }

    /// Tenant-scoped lookup by provider session handle.
    ///
    /// This is how a webhook naming `cs_...` is matched to a row this platform
    /// owns -- never trusted from the body alone.
    pub async fn get_by_provider_session(
        &self,
        tenant: TenantId,
        provider: &str,
        provider_session_id: &str,
    ) -> Result<Option<CheckoutSessionRecord>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "checkout session read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, idempotency_key, plan_id, plan_code, plan_price_id, provider, \
                    provider_session_id, state, success_url, cancel_url, created_at, updated_at \
             FROM billing_checkout_sessions \
             WHERE tenant_id = $1 AND provider = $2 AND provider_session_id = $3",
        )
        .bind(tenant.as_uuid())
        .bind(provider)
        .bind(provider_session_id)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_session(&row)).transpose()
    }

    /// Records the provider session handle on a created row.
    ///
    /// # Errors
    ///
    /// [`AppError::not_found`] when no created row carries this idempotency
    /// key in the scope's tenant.
    pub async fn attach_provider_session(
        &self,
        tenant: TenantId,
        idempotency_key: &str,
        provider_session_id: &str,
    ) -> Result<CheckoutSessionRecord> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "checkout session update outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "UPDATE billing_checkout_sessions \
             SET provider_session_id = $3 \
             WHERE tenant_id = $1 AND idempotency_key = $2 AND state = 'created' \
             RETURNING id, tenant_id, idempotency_key, plan_id, plan_price_id, provider, \
                       provider_session_id, state, success_url, cancel_url, created_at, updated_at",
        )
        .bind(tenant.as_uuid())
        .bind(idempotency_key)
        .bind(provider_session_id)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_session(&row))
            .transpose()?
            .ok_or_else(|| AppError::not_found("checkout session", idempotency_key))
    }

    /// Records a checkout lifecycle transition.
    pub async fn set_state(
        &self,
        tenant: TenantId,
        id: CheckoutSessionId,
        state: CheckoutState,
    ) -> Result<Option<CheckoutSessionRecord>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "checkout session update outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "UPDATE billing_checkout_sessions \
             SET state = $3 \
             WHERE tenant_id = $1 AND id = $2 \
             RETURNING id, tenant_id, idempotency_key, plan_id, plan_price_id, provider, \
                       provider_session_id, state, success_url, cancel_url, created_at, updated_at",
        )
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .bind(state.as_str())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_session(&row)).transpose()
    }
}

/// Postgres implementation of the per-tenant event application claim.
#[derive(Debug, Clone)]
pub struct PostgresEventApplicationStore {
    _pool: PgPool,
}

impl PostgresEventApplicationStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }

    /// Claims the right to apply one provider event to the scope's tenant.
    ///
    /// Returns `true` when the claim inserted -- the caller now owns the
    /// effect and must apply it in the same transaction. Returns `false` when
    /// the `(tenant, provider, event_id)` was already claimed -- a replay, or
    /// a concurrent delivery from another replica -- and the caller must
    /// report success without re-applying.
    ///
    /// # Errors
    ///
    /// [`AppError::database`] on storage failure. A claim conflict is *not*
    /// an error; it is the exactly-once signal.
    pub async fn claim(&self, application: &EventApplication) -> Result<bool> {
        let scope = scoped_request::current_scope()?;
        if scope.tenant_id() != application.tenant_id {
            return Err(AppError::forbidden(
                "event application claim outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let inserted = sqlx::query(
            "INSERT INTO billing_event_applications \
             (id, tenant_id, provider, event_id, receipt_id, event_type, effect) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, provider, event_id) DO NOTHING",
        )
        .bind(application.id.as_uuid())
        .bind(application.tenant_id.as_uuid())
        .bind(&application.provider)
        .bind(&application.event_id)
        .bind(application.receipt_id.map(BillingReceiptId::into_uuid))
        .bind(&application.event_type)
        .bind(&application.effect)
        .execute(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(inserted.rows_affected() == 1)
    }

    /// Tenant-scoped lookup of an existing claim.
    pub async fn get(
        &self,
        tenant: TenantId,
        provider: &str,
        event_id: &str,
    ) -> Result<Option<EventApplication>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "event application read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, provider, event_id, receipt_id, event_type, effect, applied_at \
             FROM billing_event_applications \
             WHERE tenant_id = $1 AND provider = $2 AND event_id = $3",
        )
        .bind(tenant.as_uuid())
        .bind(provider)
        .bind(event_id)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_application(&row)).transpose()
    }
}

fn row_to_session(row: &sqlx::postgres::PgRow) -> Result<CheckoutSessionRecord> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let plan: uuid::Uuid = row.try_get("plan_id").map_err(map_sqlx)?;
    let price: Option<uuid::Uuid> = row.try_get("plan_price_id").map_err(map_sqlx)?;
    let state_raw: String = row.try_get("state").map_err(map_sqlx)?;
    let state: CheckoutState = state_raw
        .parse()
        .map_err(|_| AppError::database("billing_checkout_sessions.state drifted from the enum"))?;
    let plan_code: String = row.try_get("plan_code").map_err(map_sqlx)?;
    Ok(CheckoutSessionRecord {
        id: CheckoutSessionId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        idempotency_key: row.try_get("idempotency_key").map_err(map_sqlx)?,
        plan_id: PlanId::from_uuid(plan),
        plan_code,
        plan_price_id: price.map(PlanPriceId::from_uuid),
        provider: row.try_get("provider").map_err(map_sqlx)?,
        provider_session_id: row.try_get("provider_session_id").map_err(map_sqlx)?,
        state,
        success_url: row.try_get("success_url").map_err(map_sqlx)?,
        cancel_url: row.try_get("cancel_url").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    })
}

fn row_to_application(row: &sqlx::postgres::PgRow) -> Result<EventApplication> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let receipt: Option<uuid::Uuid> = row.try_get("receipt_id").map_err(map_sqlx)?;
    Ok(EventApplication {
        id: EventApplicationId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        provider: row.try_get("provider").map_err(map_sqlx)?,
        event_id: row.try_get("event_id").map_err(map_sqlx)?,
        receipt_id: receipt.map(BillingReceiptId::from_uuid),
        event_type: row.try_get("event_type").map_err(map_sqlx)?,
        effect: row.try_get("effect").map_err(map_sqlx)?,
        applied_at: Timestamp::from_datetime(row.try_get("applied_at").map_err(map_sqlx)?),
    })
}

use mas_application::billing_service::{
    CheckoutSessionStorePort as CheckoutStoreTrait, EventApplicationStorePort,
};

#[async_trait::async_trait]
impl CheckoutStoreTrait for PostgresCheckoutSessionStore {
    async fn create_or_get(
        &self,
        record: &CheckoutSessionRecord,
    ) -> Result<(CheckoutSessionRecord, bool)> {
        PostgresCheckoutSessionStore::create_or_get(self, record).await
    }

    async fn attach_provider_session(
        &self,
        tenant: TenantId,
        idempotency_key: &str,
        provider_session_id: &str,
    ) -> Result<CheckoutSessionRecord> {
        PostgresCheckoutSessionStore::attach_provider_session(
            self,
            tenant,
            idempotency_key,
            provider_session_id,
        )
        .await
    }

    async fn get_by_provider_session(
        &self,
        tenant: TenantId,
        provider: &str,
        provider_session_id: &str,
    ) -> Result<Option<CheckoutSessionRecord>> {
        PostgresCheckoutSessionStore::get_by_provider_session(
            self,
            tenant,
            provider,
            provider_session_id,
        )
        .await
    }

    async fn set_state(
        &self,
        tenant: TenantId,
        id: mas_common::ids::CheckoutSessionId,
        state: mas_domain::billing::CheckoutState,
    ) -> Result<Option<CheckoutSessionRecord>> {
        PostgresCheckoutSessionStore::set_state(self, tenant, id, state).await
    }
}

#[async_trait::async_trait]
impl EventApplicationStorePort for PostgresEventApplicationStore {
    async fn claim(&self, application: &EventApplication) -> Result<bool> {
        PostgresEventApplicationStore::claim(self, application).await
    }

    async fn get(
        &self,
        tenant: TenantId,
        provider: &str,
        event_id: &str,
    ) -> Result<Option<EventApplication>> {
        PostgresEventApplicationStore::get(self, tenant, provider, event_id).await
    }
}
