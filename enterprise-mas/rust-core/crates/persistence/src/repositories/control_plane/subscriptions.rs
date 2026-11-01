//! PURPOSE: Subscription lifecycle persistence and its append-only history.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: own the state machine's durable side. Transitions
//! are validated by the domain aggregate and applied with a
//! compare-and-swap on `version`, so two concurrent updates cannot both win;
//! every accepted transition writes a history row in the *same transaction*,
//! which is what stops the history from drifting from the state.
//!
//! Security boundary: tenant-scoped and RLS-protected. The external
//! provider reference (`external_subscription_id`) is looked up without a
//! tenant filter in exactly one place — its one tenant-unfiltered read — and
//! that method exists only for the authenticated webhook path, which resolves
//! the tenant from the row it finds instead of from the callback's body.
//!
//! Data ownership: Rust-owned platform state. Provider identifiers are opaque
//! handles; no provider secret is ever stored here.
//!
//! It must NOT:
//!   • accept a state transition the domain aggregate would refuse;
//!   • let two live subscriptions coexist for one tenant (the partial unique
//!     index enforces it, and a duplicate is reported as a conflict, not
//!     silently merged);
//!   • trust a tenant id that arrived in an unsigned provider callback.

use mas_common::error::AppError;
use mas_common::ids::{SubscriptionEventId, SubscriptionId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::subscription::{Subscription, SubscriptionState};
use sqlx::Row;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};

use super::executor::executor;
use crate::error::map_sqlx;

/// One subscription row as stored.
#[derive(Debug, Clone)]
pub struct SubscriptionRow {
    /// Subscription id.
    pub id: uuid::Uuid,
    /// Owning tenant.
    pub tenant_id: uuid::Uuid,
    /// Plan code, denormalised for reads that do not need the plan row.
    pub plan: String,
    /// Plan id, when the plan exists in the catalogue.
    pub plan_id: Option<uuid::Uuid>,
    /// Lifecycle state.
    pub state: String,
    /// Why the current state was entered.
    pub state_reason: String,
    /// Optimistic-concurrency version.
    pub version: i64,
    /// Billing period payload from 0008.
    pub period: Option<serde_json::Value>,
    /// Opaque spec payload from 0008.
    pub spec: serde_json::Value,
    /// When the subscription became effective.
    pub effective_from: chrono::DateTime<chrono::Utc>,
    /// When it stops, if scheduled.
    pub effective_to: Option<chrono::DateTime<chrono::Utc>>,
    /// Start of the current billing period.
    pub current_period_start: Option<chrono::DateTime<chrono::Utc>>,
    /// End of the current billing period.
    pub current_period_end: Option<chrono::DateTime<chrono::Utc>>,
    /// Next renewal instant.
    pub renews_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Trial expiry.
    pub trial_ends_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Cancellation instant.
    pub cancelled_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Provider customer handle.
    pub external_customer_id: Option<String>,
    /// Provider subscription handle.
    pub external_subscription_id: Option<String>,
    /// Creation instant.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last mutation.
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for SubscriptionRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            tenant_id: row.try_get("tenant_id")?,
            plan: row.try_get("plan")?,
            plan_id: row.try_get("plan_id")?,
            state: row.try_get("state")?,
            state_reason: row.try_get("state_reason")?,
            version: row.try_get("version")?,
            period: row.try_get("period")?,
            spec: row.try_get("spec")?,
            effective_from: row.try_get("effective_from")?,
            effective_to: row.try_get("effective_to")?,
            current_period_start: row.try_get("current_period_start")?,
            current_period_end: row.try_get("current_period_end")?,
            renews_at: row.try_get("renews_at")?,
            trial_ends_at: row.try_get("trial_ends_at")?,
            cancelled_at: row.try_get("cancelled_at")?,
            external_customer_id: row.try_get("external_customer_id")?,
            external_subscription_id: row.try_get("external_subscription_id")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl SubscriptionRow {
    /// Hydrates into the domain aggregate.
    ///
    /// The domain `Subscription` is deliberately narrow — it holds only what
    /// authorization needs. Everything else in this row is billing metadata
    /// and stays here, so a change to the billing columns cannot change an
    /// authorization decision by accident.
    pub fn into_domain(self) -> Result<Subscription> {
        let state = self.state.parse::<SubscriptionState>().map_err(|_| {
            AppError::database(format!("unknown subscription state '{}'", self.state))
        })?;
        Ok(Subscription {
            tenant_id: TenantId::from_uuid(self.tenant_id),
            plan_code: self.plan,
            state,
            seats: self
                .spec
                .get("seats")
                .and_then(serde_json::Value::as_u64)
                .and_then(|seats| u32::try_from(seats).ok()),
            renews_at: self.renews_at.map(Timestamp::from_datetime),
            updated_at: Timestamp::from_datetime(self.updated_at),
        })
    }
}

/// One append-only lifecycle event.
#[derive(Debug, Clone)]
pub struct SubscriptionEventRow {
    /// Event id.
    pub id: uuid::Uuid,
    /// Subscription the event belongs to.
    pub subscription_id: uuid::Uuid,
    /// Owning tenant.
    pub tenant_id: uuid::Uuid,
    /// Previous state (`NULL` on creation).
    pub from_state: Option<String>,
    /// New state.
    pub to_state: String,
    /// Why.
    pub reason: String,
    /// Who.
    pub actor_id: String,
    /// What kind of actor.
    pub actor_kind: String,
    /// Provider event that caused it, when any.
    pub external_event_id: Option<String>,
    /// Correlation id for tracing.
    pub correlation_id: String,
    /// When.
    pub occurred_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for SubscriptionEventRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            subscription_id: row.try_get("subscription_id")?,
            tenant_id: row.try_get("tenant_id")?,
            from_state: row.try_get("from_state")?,
            to_state: row.try_get("to_state")?,
            reason: row.try_get("reason")?,
            actor_id: row.try_get("actor_id")?,
            actor_kind: row.try_get("actor_kind")?,
            external_event_id: row.try_get("external_event_id")?,
            correlation_id: row.try_get("correlation_id")?,
            occurred_at: row.try_get("occurred_at")?,
        })
    }
}

/// The subscription store.
#[derive(Debug, Clone)]
pub struct PostgresSubscriptionStore {
    pool: PgPool,
}

/// The transaction a transition runs in.
///
/// Inside a request this is the request's own transaction — the one the RLS
/// GUCs are pinned to — because a second connection taken from the pool would
/// carry no GUC at all and would therefore see no subscription row to lock.
/// Outside a request there is nothing to join, so the transition opens a
/// transaction of its own: the state change and its history row must land
/// together or not at all.
enum TransitionUnit {
    /// The request's transaction. It commits with the request; this method
    /// must not commit it early.
    Request(crate::scoped_request::ScopedExecutor),
    /// A transaction this call opened and is responsible for committing.
    Own(Transaction<'static, Postgres>),
}

impl TransitionUnit {
    /// The connection, ready to be used as a `sqlx` executor.
    fn connection(&mut self) -> &mut PgConnection {
        match self {
            Self::Request(scoped) => scoped.connection(),
            Self::Own(transaction) => transaction,
        }
    }
}

impl PostgresSubscriptionStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Creates a subscription.
    ///
    /// The partial unique index over live states is what prevents a second
    /// live subscription for one tenant. It is enforced by the database rather
    /// than by a `SELECT` first, because two concurrent creations would both
    /// read "none exists" and both insert.
    pub async fn create(&self, subscription: &Subscription) -> Result<SubscriptionId> {
        let id = SubscriptionId::new();
        let result = sqlx::query(
            "INSERT INTO subscriptions (id, tenant_id, plan, state, spec, version)
             VALUES ($1, $2, $3, $4, $5, 1)",
        )
        .bind(id.into_uuid())
        .bind(subscription.tenant_id.into_uuid())
        .bind(&subscription.plan_code)
        .bind(subscription.state.as_str())
        .bind(serde_json::json!({}))
        .execute(executor(&self.pool).await?.connection())
        .await;
        match result {
            Ok(_) => Ok(id),
            Err(error) => {
                // 23505 = unique_violation. It covers both the live-subscription
                // index and the external-reference index, and the caller needs
                // to know that the subscription already exists rather than that
                // "the database is broken".
                if crate::error::is_unique_violation(&error) {
                    Err(AppError::conflict(format!(
                        "tenant {} already has a live subscription",
                        subscription.tenant_id
                    )))
                } else {
                    Err(map_sqlx(error))
                }
            },
        }
    }

    /// Reads the tenant's live subscription, if any.
    pub async fn get(&self, tenant: TenantId) -> Result<Option<Subscription>> {
        let row: Option<SubscriptionRow> = sqlx::query_as::<_, SubscriptionRow>(
            "SELECT id, tenant_id, plan, plan_id, state, state_reason, version, period, spec,
                    effective_from, effective_to, current_period_start, current_period_end,
                    renews_at, trial_ends_at, cancelled_at, external_customer_id,
                    external_subscription_id, created_at, updated_at
               FROM subscriptions
              WHERE tenant_id = $1
                AND state IN ('trialing', 'active', 'past_due', 'suspended', 'cancelled')
              ORDER BY created_at DESC
              LIMIT 1",
        )
        .bind(tenant.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        row.map(SubscriptionRow::into_domain).transpose()
    }

    /// Reads the raw row, for the billing/admin surface that needs the
    /// lifecycle columns the authorization aggregate does not carry.
    pub async fn get_row(&self, tenant: TenantId) -> Result<Option<SubscriptionRow>> {
        sqlx::query_as::<_, SubscriptionRow>(
            "SELECT id, tenant_id, plan, plan_id, state, state_reason, version, period, spec,
                    effective_from, effective_to, current_period_start, current_period_end,
                    renews_at, trial_ends_at, cancelled_at, external_customer_id,
                    external_subscription_id, created_at, updated_at
               FROM subscriptions
              WHERE tenant_id = $1
              ORDER BY created_at DESC
              LIMIT 1",
        )
        .bind(tenant.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }

    /// Applies a state transition, stamping the history row in the same
    /// transaction.
    ///
    /// `expected_version` is the optimistic-concurrency guard: two concurrent
    /// transitions both read the same version and only one UPDATE matches, so
    /// the loser is told to re-read rather than silently overwriting.
    #[allow(clippy::too_many_arguments)]
    pub async fn transition(
        &self,
        tenant: TenantId,
        next: SubscriptionState,
        reason: &str,
        actor_id: &str,
        actor_kind: &str,
        external_event_id: Option<&str>,
        correlation_id: &str,
        expected_version: i64,
        at: &Timestamp,
    ) -> Result<SubscriptionRow> {
        let mut unit = if crate::scoped_request::has_scope() {
            TransitionUnit::Request(crate::scoped_request::connection().await?)
        } else {
            TransitionUnit::Own(self.pool.begin().await.map_err(map_sqlx)?)
        };

        let current: SubscriptionRow =
            sqlx::query_as::<_, SubscriptionRow>(
                "SELECT id, tenant_id, plan, plan_id, state, state_reason, version, period, spec,
                        effective_from, effective_to, current_period_start, current_period_end,
                        renews_at, trial_ends_at, cancelled_at, external_customer_id,
                        external_subscription_id, created_at, updated_at
                   FROM subscriptions
                  WHERE tenant_id = $1 AND version = $2
                   FOR UPDATE",
            )
            .bind(tenant.into_uuid())
            .bind(expected_version)
            .fetch_optional(unit.connection())
            .await
            .map_err(map_sqlx)?
            .ok_or_else(|| {
                AppError::conflict(format!(
                    "subscription for tenant {tenant} moved since version {expected_version}; re-read and retry"
                ))
            })?;

        // The domain aggregate owns the transition table. Applying it here
        // rather than re-deriving the legality rules keeps one authority.
        let mut aggregate = current.clone().into_domain()?;
        aggregate.transition_to(next).map_err(|error| match error {
            // An illegal transition is a conflict, not a 500: the state is
            // known and the request is simply wrong for it.
            AppError::Conflict(message) => AppError::conflict(message),
            other => other,
        })?;

        let updated: SubscriptionRow = sqlx::query_as::<_, SubscriptionRow>(
            "UPDATE subscriptions
                SET state = $3,
                    state_reason = $4,
                    version = version + 1,
                    updated_at = now(),
                    cancelled_at = CASE WHEN $3::text = 'cancelled'
                                        THEN now() ELSE cancelled_at END
              WHERE id = $1 AND version = $2
          RETURNING id, tenant_id, plan, plan_id, state, state_reason, version, period, spec,
                    effective_from, effective_to, current_period_start, current_period_end,
                    renews_at, trial_ends_at, cancelled_at, external_customer_id,
                    external_subscription_id, created_at, updated_at",
        )
        .bind(current.id)
        .bind(expected_version)
        .bind(next.as_str())
        .bind(reason)
        .fetch_optional(unit.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::conflict("subscription version moved during transition"))?;

        sqlx::query(
            "INSERT INTO subscription_events
                (id, subscription_id, tenant_id, from_state, to_state, reason,
                 actor_id, actor_kind, external_event_id, correlation_id, occurred_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
        )
        .bind(SubscriptionEventId::new().into_uuid())
        .bind(current.id)
        .bind(tenant.into_uuid())
        .bind(&current.state)
        .bind(next.as_str())
        .bind(reason)
        .bind(actor_id)
        .bind(actor_kind)
        .bind(external_event_id)
        .bind(correlation_id)
        .bind(at.into_datetime())
        .execute(unit.connection())
        .await
        .map_err(map_sqlx)?;

        // Only the transaction this call opened. Committing the request's
        // transaction here would end the request's unit of work halfway
        // through the handler and leave the rest of it uncommitted.
        if let TransitionUnit::Own(transaction) = unit {
            transaction.commit().await.map_err(map_sqlx)?;
        }
        Ok(updated)
    }

    /// The append-only history for one tenant, newest first.
    pub async fn history(&self, tenant: TenantId, limit: i64) -> Result<Vec<SubscriptionEventRow>> {
        sqlx::query_as::<_, SubscriptionEventRow>(
            "SELECT id, subscription_id, tenant_id, from_state, to_state, reason,
                    actor_id, actor_kind, external_event_id, correlation_id, occurred_at
               FROM subscription_events
              WHERE tenant_id = $1
              ORDER BY occurred_at DESC, seq DESC
              LIMIT $2",
        )
        .bind(tenant.into_uuid())
        .bind(limit.max(1))
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }

    /// Resolves a subscription by the provider's own identifier.
    ///
    /// This is the only tenant-unfiltered read in the module, and it exists
    /// for the webhook path. It does **not** mean the webhook is trusted: the
    /// caller must have already verified the signature, and it takes the
    /// tenant from *this row* rather than from anything in the callback body.
    pub async fn find_by_external_id(
        &self,
        provider: &str,
        external_subscription_id: &str,
    ) -> Result<Option<SubscriptionRow>> {
        let _ = provider;
        sqlx::query_as::<_, SubscriptionRow>(
            "SELECT id, tenant_id, plan, plan_id, state, state_reason, version, period, spec,
                    effective_from, effective_to, current_period_start, current_period_end,
                    renews_at, trial_ends_at, cancelled_at, external_customer_id,
                    external_subscription_id, created_at, updated_at
               FROM subscriptions
              WHERE external_subscription_id = $1",
        )
        .bind(external_subscription_id)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }

    /// Attaches provider identifiers to a subscription.
    pub async fn link_external(
        &self,
        tenant: TenantId,
        external_customer_id: Option<&str>,
        external_subscription_id: Option<&str>,
    ) -> Result<SubscriptionRow> {
        sqlx::query_as::<_, SubscriptionRow>(
            "UPDATE subscriptions
                SET external_customer_id = COALESCE($2, external_customer_id),
                    external_subscription_id = COALESCE($3, external_subscription_id),
                    updated_at = now()
              WHERE tenant_id = $1
          RETURNING id, tenant_id, plan, plan_id, state, state_reason, version, period, spec,
                    effective_from, effective_to, current_period_start, current_period_end,
                    renews_at, trial_ends_at, cancelled_at, external_customer_id,
                    external_subscription_id, created_at, updated_at",
        )
        .bind(tenant.into_uuid())
        .bind(external_customer_id)
        .bind(external_subscription_id)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("subscription", tenant.to_string()))
    }

    /// Moves the billing window forward.
    pub async fn advance_period(
        &self,
        tenant: TenantId,
        start: &Timestamp,
        end: &Timestamp,
    ) -> Result<SubscriptionRow> {
        sqlx::query_as::<_, SubscriptionRow>(
            "UPDATE subscriptions
                SET current_period_start = $2,
                    current_period_end = $3,
                    renews_at = $3,
                    updated_at = now()
              WHERE tenant_id = $1
          RETURNING id, tenant_id, plan, plan_id, state, state_reason, version, period, spec,
                    effective_from, effective_to, current_period_start, current_period_end,
                    renews_at, trial_ends_at, cancelled_at, external_customer_id,
                    external_subscription_id, created_at, updated_at",
        )
        .bind(tenant.into_uuid())
        .bind(start.into_datetime())
        .bind(end.into_datetime())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("subscription", tenant.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_state_in_a_row_is_a_loud_error() {
        let row = SubscriptionRow {
            id: uuid::Uuid::new_v4(),
            tenant_id: TenantId::new().into_uuid(),
            plan: "team".to_owned(),
            plan_id: None,
            state: "dunning".to_owned(),
            state_reason: String::new(),
            version: 1,
            period: None,
            spec: serde_json::Value::Null,
            effective_from: chrono::Utc::now(),
            effective_to: None,
            current_period_start: None,
            current_period_end: None,
            renews_at: None,
            trial_ends_at: None,
            cancelled_at: None,
            external_customer_id: None,
            external_subscription_id: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        assert!(
            row.into_domain().is_err(),
            "schema drift must fail loudly, never default to a state"
        );
    }

    #[test]
    fn a_live_row_hydrates_into_the_authorization_aggregate() {
        let tenant = TenantId::new();
        let row = SubscriptionRow {
            id: uuid::Uuid::new_v4(),
            tenant_id: tenant.into_uuid(),
            plan: "business".to_owned(),
            plan_id: None,
            state: "active".to_owned(),
            state_reason: String::new(),
            version: 3,
            period: None,
            spec: serde_json::json!({"seats": 25}),
            effective_from: chrono::Utc::now(),
            effective_to: None,
            current_period_start: None,
            current_period_end: None,
            renews_at: None,
            trial_ends_at: None,
            cancelled_at: None,
            external_customer_id: Some("cus_1".to_owned()),
            external_subscription_id: Some("sub_1".to_owned()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let subscription = row.into_domain().expect("hydrate");
        assert_eq!(subscription.plan_code, "business");
        assert_eq!(subscription.seats, Some(25));
        assert!(subscription.is_entitled());
        assert!(subscription.can_start_work());
        assert_eq!(subscription.tenant_id, tenant);
    }
}
