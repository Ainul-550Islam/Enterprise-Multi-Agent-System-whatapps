//! PURPOSE: PostgreSQL durable recipient-notification repository.
//!
//! Owning layer: `mas-persistence` request-plane adapter.
//!
//! Production role: implement canonical request-plane notification
//! deduplication/lifecycle/keyset persistence and a separately constructed,
//! lease-fenced delivery-plane store over PostgreSQL.
//!
//! Security boundary: request statements include a tenant predicate and use the
//! task-local scoped connection. Recipient filters are passed by the application
//! service only after actor/administrator authorization. The delivery store is
//! explicitly constructed with the least-privilege `mas_notifications` pool;
//! it can claim/settle rows but has no request CRUD API.
//!
//! Data ownership: PostgreSQL owns deduplication, archive/read/delivery state,
//! leases, fences, and ordering. The adapter never sends a channel message or
//! renders a body.
//!
//! Prohibited responsibilities: outbound channel delivery, template rendering,
//! raw secret persistence, offset pagination, unscoped request-plane listing,
//! or arbitrary SQL access through the delivery pool.

use std::time::Duration;

use chrono::{DateTime, Utc};
use mas_application::notification_service::{NotificationCursor, NotificationLifecycleStorePort};
use mas_application::stores::NotificationStorePort;
use mas_common::error::AppError;
use mas_common::ids::{TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{DeliveryState, Notification, NotificationChannel};
use mas_integrations::durable_notifications::{ClaimedNotification, NotificationDeliveryLeasePort};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const NOTIFICATION_COLUMNS: &str = "id, tenant_id, recipient_id, channel, deduplication_key, \
    destination, subject, body, delivery_state, attempts, max_attempts, next_attempt_at, \
    last_error, read_at, archived_at, created_at, updated_at";

/// Canonical scoped notification adapter.
#[derive(Debug, Clone)]
pub struct PostgresNotificationStore {
    pool: PgPool,
}

impl PostgresNotificationStore {
    /// Binds the adapter to the normal application pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool used by composition to establish request RLS scopes.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Cross-tenant delivery-plane store isolated from request-plane CRUD.
///
/// The caller must use a pool authenticated as `mas_notifications`, which has
/// only `SELECT, UPDATE` on `notifications` (migration 0039). This type has no
/// create/read/list API deliberately: a background worker must not become a
/// general tenant data access path.
#[derive(Debug, Clone)]
pub struct PostgresNotificationDeliveryStore {
    pool: PgPool,
}

impl PostgresNotificationDeliveryStore {
    /// Binds the delivery port to its dedicated `mas_notifications` pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Exposes the pool only for delivery-plane health/metrics composition.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl NotificationLifecycleStorePort for PostgresNotificationStore {
    async fn create_or_get(&self, notification: &Notification) -> Result<Notification> {
        validate_notification(notification)?;
        ensure_current_tenant(notification.tenant_id)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let status = legacy_status(&notification.state);
        let inserted = sqlx::query(&format!(
            "INSERT INTO notifications (
                 id, tenant_id, recipient_id, channel, template, payload, status,
                 deduplication_key, destination, subject, body, delivery_state,
                 attempts, max_attempts, next_attempt_at, last_error, read_at,
                 archived_at, delivered_at, created_at, updated_at
             ) VALUES (
                 $1, $2, $3, $4, 'canonical', '{{}}'::jsonb, $5,
                 $6, $7, $8, $9, $10,
                 $11, $12, $13, $14, $15, $16,
                 CASE WHEN $10 = 'delivered' THEN now() ELSE NULL END, now(), now()
             )
             ON CONFLICT (tenant_id, recipient_id, channel, deduplication_key) DO NOTHING
             RETURNING {NOTIFICATION_COLUMNS}"
        ))
        .bind(notification.id)
        .bind(notification.tenant_id.into_uuid())
        .bind(notification.recipient_id.into_uuid())
        .bind(notification.channel.to_string())
        .bind(status)
        .bind(&notification.deduplication_key)
        .bind(&notification.destination)
        .bind(&notification.subject)
        .bind(&notification.body)
        .bind(notification.state.to_string())
        .bind(i32::try_from(notification.attempts).map_err(|_| {
            AppError::invalid_field("attempts", "out_of_range", "attempts are too large")
        })?)
        .bind(i32::try_from(notification.max_attempts).map_err(|_| {
            AppError::invalid_field("max_attempts", "out_of_range", "max attempts are too large")
        })?)
        .bind(notification.next_attempt_at.map(Timestamp::into_datetime))
        .bind(&notification.last_error)
        .bind(notification.read_at.map(Timestamp::into_datetime))
        .bind(notification.archived_at.map(Timestamp::into_datetime))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if let Some(row) = inserted {
            return notification_from_row(row);
        }
        let row = sqlx::query(&format!(
            "SELECT {NOTIFICATION_COLUMNS}
             FROM notifications
             WHERE tenant_id = $1
               AND recipient_id = $2
               AND channel = $3
               AND deduplication_key = $4"
        ))
        .bind(notification.tenant_id.into_uuid())
        .bind(notification.recipient_id.into_uuid())
        .bind(notification.channel.to_string())
        .bind(&notification.deduplication_key)
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::database("notification dedup conflict row disappeared"))?;
        notification_from_row(row)
    }

    async fn get(&self, tenant: TenantId, id: Uuid) -> Result<Option<Notification>> {
        ensure_current_tenant(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {NOTIFICATION_COLUMNS}
             FROM notifications
             WHERE tenant_id = $1 AND id = $2"
        ))
        .bind(tenant.into_uuid())
        .bind(id)
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(notification_from_row).transpose()
    }

    async fn save(&self, notification: &Notification) -> Result<()> {
        validate_notification(notification)?;
        ensure_current_tenant(notification.tenant_id)?;
        let mut scoped = crate::scoped_request::connection().await?;
        // This request-plane method is intentionally limited to recipient
        // lifecycle columns. A read/archive race must never replay a stale
        // delivery state over a worker's lease-fenced settlement.
        let result = sqlx::query(
            "UPDATE notifications
             SET read_at = COALESCE(read_at, $3),
                 archived_at = COALESCE(archived_at, $4),
                 updated_at = now()
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(notification.tenant_id.into_uuid())
        .bind(notification.id)
        .bind(notification.read_at.map(Timestamp::into_datetime))
        .bind(notification.archived_at.map(Timestamp::into_datetime))
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found(
                "notification",
                notification.id.to_string(),
            ));
        }
        Ok(())
    }

    async fn list_page(
        &self,
        tenant: TenantId,
        recipient: UserId,
        include_archived: bool,
        unread_only: bool,
        after: Option<NotificationCursor>,
        limit: u16,
    ) -> Result<Vec<Notification>> {
        ensure_current_tenant(tenant)?;
        if !(1..=201).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "notification storage page limit must be between 1 and 201",
            ));
        }
        let mut scoped = crate::scoped_request::connection().await?;
        // Boolean predicates make the prepared statement stable while retaining
        // index-friendly tenant/recipient/keyset constraints.
        let rows = sqlx::query(&format!(
            "SELECT {NOTIFICATION_COLUMNS}
             FROM notifications
             WHERE tenant_id = $1
               AND recipient_id = $2
               AND ($3::boolean OR archived_at IS NULL)
               AND (NOT $4::boolean OR read_at IS NULL)
               AND (
                    $5::timestamptz IS NULL
                    OR created_at < $5
                    OR (created_at = $5 AND id < $6)
               )
             ORDER BY created_at DESC, id DESC
             LIMIT $7"
        ))
        .bind(tenant.into_uuid())
        .bind(recipient.into_uuid())
        .bind(include_archived)
        .bind(unread_only)
        .bind(after.map(|cursor| cursor.created_at.into_datetime()))
        .bind(after.map(|cursor| cursor.id))
        .bind(i64::from(limit))
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(notification_from_row).collect()
    }
}

#[async_trait::async_trait]
impl NotificationStorePort for PostgresNotificationStore {
    async fn save(&self, notification: &Notification) -> Result<()> {
        <Self as NotificationLifecycleStorePort>::save(self, notification).await
    }

    async fn get(&self, id: Uuid) -> Result<Option<Notification>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        <Self as NotificationLifecycleStorePort>::get(self, tenant, id).await
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Notification>> {
        ensure_current_tenant(tenant)?;
        // Existing broad application port has no recipient/page filter. Keep
        // it bounded and canonical callers use NotificationLifecycleStorePort.
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {NOTIFICATION_COLUMNS}
             FROM notifications
             WHERE tenant_id = $1
             ORDER BY created_at DESC, id DESC
             LIMIT 1001"
        ))
        .bind(tenant.into_uuid())
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(notification_from_row).collect()
    }
}

#[async_trait::async_trait]
impl NotificationDeliveryLeasePort for PostgresNotificationDeliveryStore {
    async fn claim_due(
        &self,
        worker: &str,
        _now: &Timestamp,
        lease: Duration,
        limit: u32,
    ) -> Result<Vec<ClaimedNotification>> {
        validate_delivery_claim_arguments(worker, lease, limit)?;
        let lease_millis = i64::try_from(lease.as_millis()).map_err(|_| {
            AppError::invalid_field(
                "lease",
                "out_of_range",
                "notification delivery lease is too large for PostgreSQL interval storage",
            )
        })?;
        // The database clock is deliberate. A skewed worker clock cannot retain
        // or steal another replica's claim. An expired `sending` row is exposed
        // to the domain worker as Pending: delivery is at-least-once, and a
        // crashed worker cannot leave a row permanently unsendable.
        let rows = sqlx::query(
            "WITH due AS (
                SELECT notification.id,
                       CASE
                           WHEN notification.delivery_state = 'sending' THEN 'pending'
                           ELSE notification.delivery_state
                       END AS prior_state
                FROM notifications AS notification
                WHERE (
                        notification.delivery_state = 'pending'
                     OR (notification.delivery_state = 'failed'
                         AND notification.next_attempt_at <= now())
                     OR (notification.delivery_state = 'sending'
                         AND notification.lease_expires_at <= now())
                )
                ORDER BY COALESCE(notification.next_attempt_at, notification.created_at) ASC,
                         notification.id ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
             ), claimed AS (
                UPDATE notifications AS notification
                SET delivery_state = 'sending',
                    lease_owner = $1,
                    lease_expires_at = now() + ($3 * interval '1 millisecond'),
                    fence = notification.fence + 1,
                    updated_at = now()
                FROM due
                WHERE notification.id = due.id
                RETURNING notification.*
             )
             SELECT claimed.id,
                    claimed.tenant_id,
                    claimed.recipient_id,
                    claimed.channel,
                    claimed.deduplication_key,
                    claimed.destination,
                    claimed.subject,
                    claimed.body,
                    due.prior_state AS delivery_state,
                    claimed.attempts,
                    claimed.max_attempts,
                    claimed.next_attempt_at,
                    claimed.last_error,
                    claimed.read_at,
                    claimed.archived_at,
                    claimed.created_at,
                    claimed.updated_at,
                    claimed.fence
             FROM claimed
             JOIN due ON due.id = claimed.id
             ORDER BY COALESCE(claimed.next_attempt_at, claimed.created_at) ASC,
                      claimed.id ASC",
        )
        .bind(worker)
        .bind(i64::from(limit))
        .bind(lease_millis)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.into_iter()
            .map(claimed_notification_from_row)
            .collect()
    }

    async fn settle_claim(
        &self,
        claim: &ClaimedNotification,
        notification: &Notification,
    ) -> Result<bool> {
        validate_notification(notification)?;
        if notification.id != claim.notification.id
            || notification.tenant_id != claim.notification.tenant_id
            || notification.recipient_id != claim.notification.recipient_id
            || notification.channel != claim.notification.channel
            || notification.deduplication_key != claim.notification.deduplication_key
        {
            return Err(AppError::invalid_field(
                "notification",
                "mismatched_claim",
                "notification settlement does not match its claimed durable row",
            ));
        }
        if !matches!(
            notification.state,
            DeliveryState::Delivered | DeliveryState::Failed | DeliveryState::DeadLettered
        ) {
            return Err(AppError::invalid_field(
                "delivery_state",
                "invalid_settlement",
                "notification claim must settle as delivered, failed, or dead lettered",
            ));
        }
        let attempts = i32::try_from(notification.attempts).map_err(|_| {
            AppError::invalid_field(
                "attempts",
                "out_of_range",
                "notification attempts are too large for PostgreSQL integer storage",
            )
        })?;
        let result = sqlx::query(
            "UPDATE notifications
             SET status = $3,
                 destination = $4,
                 subject = $5,
                 body = $6,
                 delivery_state = $7,
                 attempts = $8,
                 max_attempts = $9,
                 next_attempt_at = $10,
                 last_error = $11,
                 read_at = $12,
                 archived_at = $13,
                 delivered_at = CASE
                     WHEN $7 = 'delivered' THEN COALESCE(delivered_at, now())
                     ELSE delivered_at
                 END,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 updated_at = now()
             WHERE id = $1
               AND fence = $2
               AND delivery_state = 'sending'
               AND lease_expires_at > now()",
        )
        .bind(notification.id)
        .bind(claim.fence)
        .bind(legacy_status(&notification.state))
        .bind(&notification.destination)
        .bind(&notification.subject)
        .bind(&notification.body)
        .bind(notification.state.to_string())
        .bind(attempts)
        .bind(i32::try_from(notification.max_attempts).map_err(|_| {
            AppError::invalid_field(
                "max_attempts",
                "out_of_range",
                "notification maximum attempts are too large for PostgreSQL integer storage",
            )
        })?)
        .bind(notification.next_attempt_at.map(Timestamp::into_datetime))
        .bind(&notification.last_error)
        .bind(notification.read_at.map(Timestamp::into_datetime))
        .bind(notification.archived_at.map(Timestamp::into_datetime))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }

    async fn release_claim(&self, claim: &ClaimedNotification) -> Result<bool> {
        let state = claim.notification.state.to_string();
        let result = sqlx::query(
            "UPDATE notifications
             SET delivery_state = $3,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 updated_at = now()
             WHERE id = $1
               AND fence = $2
               AND delivery_state = 'sending'",
        )
        .bind(claim.notification.id)
        .bind(claim.fence)
        .bind(state)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }
}

fn validate_delivery_claim_arguments(worker: &str, lease: Duration, limit: u32) -> Result<()> {
    if worker.trim().is_empty() || worker.len() > 128 || worker.chars().any(char::is_control) {
        return Err(AppError::invalid_field(
            "worker",
            "invalid_format",
            "notification delivery worker must be 1..=128 printable characters",
        ));
    }
    if !(1..=1_000).contains(&limit) {
        return Err(AppError::invalid_field(
            "limit",
            "out_of_range",
            "notification delivery claim limit must be between 1 and 1000",
        ));
    }
    if lease < Duration::from_secs(1) || lease > Duration::from_secs(300) {
        return Err(AppError::invalid_field(
            "lease",
            "out_of_range",
            "notification delivery lease must be between 1 and 300 seconds",
        ));
    }
    Ok(())
}

fn claimed_notification_from_row(row: sqlx::postgres::PgRow) -> Result<ClaimedNotification> {
    let fence: i64 = row.try_get("fence").map_err(map_sqlx)?;
    if fence <= 0 {
        return Err(AppError::database(
            "claimed notification has an invalid fence",
        ));
    }
    Ok(ClaimedNotification {
        notification: notification_from_row(row)?,
        fence,
    })
}

fn ensure_current_tenant(tenant: TenantId) -> Result<()> {
    if crate::scoped_request::current_scope()?.tenant_id() != tenant {
        return Err(AppError::not_found("tenant", tenant.to_string()));
    }
    Ok(())
}

fn legacy_status(state: &DeliveryState) -> &'static str {
    match state {
        DeliveryState::Pending | DeliveryState::Sending => "queued",
        DeliveryState::Delivered => "sent",
        DeliveryState::Failed | DeliveryState::DeadLettered => "failed",
        DeliveryState::Cancelled => "cancelled",
    }
}

fn validate_notification(notification: &Notification) -> Result<()> {
    if notification.id.is_nil()
        || notification.tenant_id.is_nil()
        || notification.recipient_id.is_nil()
    {
        return Err(AppError::invalid_field(
            "notification",
            "required",
            "notification IDs and tenant/recipient scope must be concrete",
        ));
    }
    if notification.attempts > notification.max_attempts
        || !(1..=64).contains(&notification.max_attempts)
    {
        return Err(AppError::invalid_field(
            "attempts",
            "out_of_range",
            "notification attempt counts violate durable bounds",
        ));
    }
    if notification.state == DeliveryState::Failed && notification.next_attempt_at.is_none() {
        return Err(AppError::invalid_field(
            "next_attempt_at",
            "required",
            "retryable failed notifications require a next attempt time",
        ));
    }
    Ok(())
}

fn notification_from_row(row: sqlx::postgres::PgRow) -> Result<Notification> {
    let channel: String = row.try_get("channel").map_err(map_sqlx)?;
    let state: String = row.try_get("delivery_state").map_err(map_sqlx)?;
    let notification = Notification {
        id: row.try_get("id").map_err(map_sqlx)?,
        tenant_id: TenantId::from_uuid(row.try_get::<Uuid, _>("tenant_id").map_err(map_sqlx)?),
        recipient_id: UserId::from_uuid(row.try_get::<Uuid, _>("recipient_id").map_err(map_sqlx)?),
        channel: channel
            .parse::<NotificationChannel>()
            .map_err(|_| AppError::database("stored notification channel is invalid"))?,
        deduplication_key: row.try_get("deduplication_key").map_err(map_sqlx)?,
        destination: row.try_get("destination").map_err(map_sqlx)?,
        subject: row.try_get("subject").map_err(map_sqlx)?,
        body: row.try_get("body").map_err(map_sqlx)?,
        state: state
            .parse::<DeliveryState>()
            .map_err(|_| AppError::database("stored notification delivery state is invalid"))?,
        attempts: nonnegative_u32(&row, "attempts")?,
        max_attempts: nonnegative_u32(&row, "max_attempts")?,
        next_attempt_at: optional_timestamp(&row, "next_attempt_at")?,
        last_error: row.try_get("last_error").map_err(map_sqlx)?,
        read_at: optional_timestamp(&row, "read_at")?,
        archived_at: optional_timestamp(&row, "archived_at")?,
        created_at: timestamp(&row, "created_at")?,
        updated_at: timestamp(&row, "updated_at")?,
    };
    validate_notification(&notification)
        .map_err(|_| AppError::database("stored notification violates domain invariants"))?;
    Ok(notification)
}

fn timestamp(row: &sqlx::postgres::PgRow, column: &str) -> Result<Timestamp> {
    row.try_get::<DateTime<Utc>, _>(column)
        .map(Timestamp::from_datetime)
        .map_err(map_sqlx)
}

fn optional_timestamp(row: &sqlx::postgres::PgRow, column: &str) -> Result<Option<Timestamp>> {
    row.try_get::<Option<DateTime<Utc>>, _>(column)
        .map(|value| value.map(Timestamp::from_datetime))
        .map_err(map_sqlx)
}

fn nonnegative_u32(row: &sqlx::postgres::PgRow, column: &str) -> Result<u32> {
    let value: i32 = row.try_get(column).map_err(map_sqlx)?;
    u32::try_from(value).map_err(|_| AppError::database(format!("stored {column} is negative")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_status_maps_new_terminal_dead_letter_explicitly() {
        assert_eq!(legacy_status(&DeliveryState::DeadLettered), "failed");
        assert_eq!(legacy_status(&DeliveryState::Delivered), "sent");
    }
}
