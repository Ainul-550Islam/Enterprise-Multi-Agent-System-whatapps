//! PURPOSE: PostgreSQL durable outbound-webhook relay repository.
//!
//! Owning layer: `mas-persistence` infrastructure adapter.
//!
//! Production role: implement the integration layer's fenced delivery-lease
//! port using PostgreSQL `FOR UPDATE SKIP LOCKED`, authoritative database time,
//! monotonically increasing fences, atomic delivery/endpoint settlement, and
//! durable dead-letter records. It is constructed with the dedicated
//! `mas_integrations` pool, never the normal request pool.
//!
//! Security boundary: the relay is intentionally cross-tenant, so its database
//! role has a tiny BYPASSRLS grant described in migration 0039. Every claimed
//! row carries tenant ownership and joins through tenant equality. This module
//! neither resolves secret material nor provides a raw HTTP path; it exposes a
//! `SecretReferenceId` to the worker and the worker uses guarded egress.
//!
//! Data ownership: PostgreSQL owns lease, fence, endpoint outcome, payload,
//! delivery, and dead-letter state. This adapter retains no local queue or
//! secret cache, so a process restart cannot lose a pending job.
//!
//! Prohibited responsibilities: tenant-request CRUD, direct webhook sending,
//! raw signing/OAuth/credential material, authorization decisions, and
//! best-effort in-memory persistence.

use std::time::Duration;

use chrono::{DateTime, Utc};
use mas_common::error::AppError;
use mas_common::ids::{EventId, SecretReferenceId, TenantId, WebhookId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::notification::DeliveryState;
use mas_domain::value_objects::SafeUrl;
use mas_domain::webhook::{
    SignatureAlgorithm, SignatureConfig, WebhookDelivery, WebhookEndpoint, WebhookRetryPolicy,
};
use mas_integrations::durable_webhooks::{ClaimedWebhookDelivery, WebhookDeliveryLeasePort};
use mas_integrations::webhooks::StoredEvent;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// The dedicated relay-plane PostgreSQL store.
///
/// The caller must supply a pool authenticated as `mas_integrations`; using a
/// normal `mas_app` pool deliberately yields no cross-tenant queue rows under
/// FORCE RLS and is treated as an operational wiring error rather than a
/// fallback mode.
#[derive(Debug, Clone)]
pub struct PostgresWebhookRelayStore {
    pool: PgPool,
}

impl PostgresWebhookRelayStore {
    /// Binds the durable relay port to its dedicated database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Exposes the pool only for relay-plane health checks/metrics wiring.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl WebhookDeliveryLeasePort for PostgresWebhookRelayStore {
    async fn claim_due(
        &self,
        worker: &str,
        _now: &Timestamp,
        lease: Duration,
        limit: u32,
    ) -> Result<Vec<ClaimedWebhookDelivery>> {
        validate_claim_arguments(worker, lease, limit)?;
        let lease_ms = i64::try_from(lease.as_millis()).map_err(|_| {
            AppError::invalid_field(
                "lease",
                "out_of_range",
                "webhook delivery lease is too large for PostgreSQL interval storage",
            )
        })?;
        let limit = i64::from(limit);

        // This CTE is one atomic claim operation: selection locks candidates
        // with SKIP LOCKED, then the same statement stamps a new fence/lease.
        // The database clock is deliberate: a worker's skewed wall clock must
        // not determine another replica's ownership.
        let rows = sqlx::query(
            "WITH due AS (
                SELECT delivery.id, delivery.status AS prior_state
                FROM webhook_deliveries AS delivery
                JOIN webhooks AS endpoint
                  ON endpoint.id = delivery.webhook_id
                 AND endpoint.tenant_id = delivery.tenant_id
                JOIN webhook_payloads AS payload
                  ON payload.id = delivery.payload_id
                 AND payload.tenant_id = delivery.tenant_id
                JOIN secret_references AS signing_secret
                  ON signing_secret.id = endpoint.signing_secret_id
                 AND signing_secret.tenant_id = endpoint.tenant_id
                WHERE endpoint.enabled
                  AND signing_secret.availability = 'available'
                  AND signing_secret.rotation_state <> 'retired' 
                  AND delivery.payload_hash = payload.body_sha256
                  AND (
                        (delivery.status IN ('pending', 'failed')
                         AND delivery.next_attempt_at <= now())
                     OR (delivery.status = 'leased'
                         AND delivery.lease_expires_at <= now())
                  )
                ORDER BY delivery.next_attempt_at ASC, delivery.id ASC
                LIMIT $2
                FOR UPDATE OF delivery SKIP LOCKED
             ), claimed AS (
                UPDATE webhook_deliveries AS delivery
                SET status = 'leased',
                    lease_owner = $1,
                    lease_expires_at = now() + ($3 * interval '1 millisecond'),
                    fence = delivery.fence + 1,
                    updated_at = now()
                FROM due
                WHERE delivery.id = due.id
                RETURNING delivery.*
             )
             SELECT
                 delivery.id AS delivery_id,
                 delivery.webhook_id AS endpoint_id,
                 delivery.event_id,
                 due.prior_state AS delivery_state,
                 delivery.attempts AS attempt_count,
                 delivery.http_status AS last_status_code,
                 delivery.next_attempt_at,
                 delivery.payload_hash,
                 delivery.signing_key_version AS delivery_signing_key_version,
                 delivery.last_error,
                 delivery.created_at AS delivery_created_at,
                 delivery.updated_at AS delivery_updated_at,
                 delivery.fence,
                 endpoint.tenant_id,
                 endpoint.url,
                 endpoint.description,
                 endpoint.subscribed_events,
                 endpoint.signing_secret_id,
                 endpoint.signing_key_version AS endpoint_signing_key_version,
                 endpoint.enabled,
                 endpoint.retry_max_attempts,
                 endpoint.retry_initial_backoff_ms,
                 endpoint.consecutive_failures,
                 endpoint.created_at AS endpoint_created_at,
                 endpoint.updated_at AS endpoint_updated_at,
                 payload.event_type,
                 payload.body
             FROM claimed AS delivery
             JOIN due
               ON due.id = delivery.id
             JOIN webhooks AS endpoint
               ON endpoint.id = delivery.webhook_id
              AND endpoint.tenant_id = delivery.tenant_id
             JOIN webhook_payloads AS payload
               ON payload.id = delivery.payload_id
              AND payload.tenant_id = delivery.tenant_id
             ORDER BY delivery.next_attempt_at ASC, delivery.id ASC",
        )
        .bind(worker)
        .bind(limit)
        .bind(lease_ms)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;

        rows.iter().map(claim_from_row).collect()
    }

    async fn settle_claim(
        &self,
        claim: &ClaimedWebhookDelivery,
        delivery: &WebhookDelivery,
        endpoint: &WebhookEndpoint,
    ) -> Result<bool> {
        validate_settlement(claim, delivery, endpoint)?;
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        let state = delivery.state.to_string();
        let last_status = delivery.last_status_code.map(i32::from);
        let attempts = i32::try_from(delivery.attempt_count).map_err(|_| {
            AppError::invalid_field(
                "attempt_count",
                "out_of_range",
                "webhook attempt count does not fit PostgreSQL integer storage",
            )
        })?;
        let terminal_dead_letter = delivery.state == DeliveryState::DeadLettered;

        // Fence AND lease expiry are both required. A new owner increments the
        // fence; an old owner whose lease has elapsed is also forbidden from
        // committing even if no replacement has claimed yet.
        let settled = sqlx::query(
            "UPDATE webhook_deliveries
             SET status = $3,
                 attempts = $4,
                 http_status = $5,
                 next_attempt_at = $6,
                 last_error = $7,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 dead_lettered_at = CASE WHEN $8 THEN now() ELSE dead_lettered_at END,
                 updated_at = now()
             WHERE id = $1
               AND fence = $2
               AND status = 'leased'
               AND lease_expires_at > now()
             RETURNING tenant_id, webhook_id",
        )
        .bind(delivery.id)
        .bind(claim.fence)
        .bind(state)
        .bind(attempts)
        .bind(last_status)
        .bind(delivery.next_attempt_at.map(Timestamp::into_datetime))
        .bind(&delivery.last_error)
        .bind(terminal_dead_letter)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_sqlx)?;

        let Some(settled) = settled else {
            transaction.rollback().await.map_err(map_sqlx)?;
            return Ok(false);
        };
        let tenant_id: Uuid = settled.try_get("tenant_id").map_err(map_sqlx)?;
        let endpoint_id: Uuid = settled.try_get("webhook_id").map_err(map_sqlx)?;
        if endpoint_id != endpoint.id.into_uuid() || tenant_id != endpoint.tenant_id.into_uuid() {
            transaction.rollback().await.map_err(map_sqlx)?;
            return Err(AppError::database(
                "webhook settlement returned an endpoint outside its claimed tenant scope",
            ));
        }

        // Update only outcome statistics atomically against the current row.
        // In particular, a concurrent administrator disable remains disabled;
        // a successful stale-ish outcome never re-enables an endpoint.
        let delivered = delivery.state == DeliveryState::Delivered;
        let endpoint_update = sqlx::query(
            "UPDATE webhooks
             SET consecutive_failures = CASE
                     WHEN $3 THEN 0
                     ELSE LEAST(consecutive_failures + 1, 2147483647)
                 END,
                 enabled = CASE
                     WHEN $3 THEN enabled
                     WHEN consecutive_failures + 1 >= 25 THEN false
                     ELSE enabled
                 END,
                 updated_at = now()
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(endpoint.id.into_uuid())
        .bind(endpoint.tenant_id.into_uuid())
        .bind(delivered)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        if endpoint_update.rows_affected() != 1 {
            transaction.rollback().await.map_err(map_sqlx)?;
            return Err(AppError::database(
                "claimed webhook endpoint disappeared before fenced settlement",
            ));
        }

        if terminal_dead_letter {
            let error = delivery
                .last_error
                .as_deref()
                .filter(|message| !message.is_empty())
                .unwrap_or("webhook delivery attempts exhausted");
            let dead_letter = sqlx::query(
                "INSERT INTO webhook_dead_letters (
                    delivery_id, tenant_id, webhook_id, event_id, payload_id,
                    payload_hash, final_status, attempt_count, error, final_fence
                 )
                 SELECT id, tenant_id, webhook_id, event_id, payload_id,
                        payload_hash, http_status, attempts, $2, fence
                 FROM webhook_deliveries
                 WHERE id = $1
                 ON CONFLICT (delivery_id) DO NOTHING",
            )
            .bind(delivery.id)
            .bind(error)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
            if dead_letter.rows_affected() != 1 {
                transaction.rollback().await.map_err(map_sqlx)?;
                return Err(AppError::database(
                    "webhook dead-letter evidence was unexpectedly already present",
                ));
            }
        }

        transaction.commit().await.map_err(map_sqlx)?;
        Ok(true)
    }

    async fn release_claim(&self, claim: &ClaimedWebhookDelivery) -> Result<bool> {
        let original_state = claim.delivery.state.to_string();
        let result = sqlx::query(
            "UPDATE webhook_deliveries
             SET status = $3,
                 lease_owner = NULL,
                 lease_expires_at = NULL,
                 updated_at = now()
             WHERE id = $1
               AND fence = $2
               AND status = 'leased'",
        )
        .bind(claim.delivery.id)
        .bind(claim.fence)
        .bind(original_state)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }
}

fn validate_claim_arguments(worker: &str, lease: Duration, limit: u32) -> Result<()> {
    if worker.trim().is_empty() || worker.len() > 128 || worker.chars().any(char::is_control) {
        return Err(AppError::invalid_field(
            "worker",
            "invalid_format",
            "webhook relay worker id must be 1..=128 printable characters",
        ));
    }
    if lease < Duration::from_secs(1) || lease > Duration::from_secs(300) {
        return Err(AppError::invalid_field(
            "lease",
            "out_of_range",
            "webhook relay lease must be between 1 and 300 seconds",
        ));
    }
    if !(1..=1_000).contains(&limit) {
        return Err(AppError::invalid_field(
            "limit",
            "out_of_range",
            "webhook relay claim limit must be between 1 and 1000",
        ));
    }
    Ok(())
}

fn validate_settlement(
    claim: &ClaimedWebhookDelivery,
    delivery: &WebhookDelivery,
    endpoint: &WebhookEndpoint,
) -> Result<()> {
    if delivery.id != claim.delivery.id
        || delivery.endpoint_id != claim.delivery.endpoint_id
        || delivery.event_id != claim.delivery.event_id
        || endpoint.id != claim.endpoint.id
        || endpoint.tenant_id != claim.endpoint.tenant_id
    {
        return Err(AppError::conflict(
            "webhook relay refuses settlement for an aggregate other than its claim",
        ));
    }
    if !matches!(
        delivery.state,
        DeliveryState::Delivered | DeliveryState::Failed | DeliveryState::DeadLettered
    ) {
        return Err(AppError::conflict(
            "webhook relay can settle only delivered, retryable-failed, or dead-lettered jobs",
        ));
    }
    Ok(())
}

fn claim_from_row(row: &sqlx::postgres::PgRow) -> Result<ClaimedWebhookDelivery> {
    let url: String = row.try_get("url").map_err(map_sqlx)?;
    let url = SafeUrl::parse(&url).map_err(|_| {
        AppError::database("persisted webhook URL no longer satisfies tenant-safe URL invariants")
    })?;
    let subscribed_events: Vec<String> = row.try_get("subscribed_events").map_err(map_sqlx)?;
    if subscribed_events.is_empty() {
        return Err(AppError::database(
            "persisted webhook endpoint has no subscribed events",
        ));
    }
    let retry_max_attempts = required_u32(row, "retry_max_attempts")?;
    let retry_initial_backoff_ms = required_u64(row, "retry_initial_backoff_ms")?;
    let attempt_count = required_u32(row, "attempt_count")?;
    let last_status_code = row
        .try_get::<Option<i32>, _>("last_status_code")
        .map_err(map_sqlx)?
        .map(|status| {
            u16::try_from(status).map_err(|_| AppError::database("invalid stored HTTP status"))
        })
        .transpose()?;
    let state: String = row.try_get("delivery_state").map_err(map_sqlx)?;
    let state = state.parse().map_err(|_| {
        AppError::database("stored webhook delivery state is not a valid delivery lifecycle state")
    })?;
    if state != DeliveryState::Pending && state != DeliveryState::Failed {
        return Err(AppError::database(
            "claimed webhook delivery had an invalid pre-claim state",
        ));
    }
    let endpoint = WebhookEndpoint {
        id: WebhookId::from_uuid(row.try_get("endpoint_id").map_err(map_sqlx)?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(map_sqlx)?),
        url,
        description: row.try_get("description").map_err(map_sqlx)?,
        subscribed_events: subscribed_events.into_iter().collect(),
        signature: SignatureConfig {
            algorithm: SignatureAlgorithm::HmacSha256,
            secret_reference_id: SecretReferenceId::from_uuid(
                row.try_get("signing_secret_id").map_err(map_sqlx)?,
            ),
            key_version: row
                .try_get::<Option<String>, _>("endpoint_signing_key_version")
                .map_err(map_sqlx)?,
        },
        enabled: row.try_get("enabled").map_err(map_sqlx)?,
        retry_policy: WebhookRetryPolicy {
            max_attempts: retry_max_attempts,
            initial_backoff_ms: retry_initial_backoff_ms,
        },
        consecutive_failures: required_u32(row, "consecutive_failures")?,
        created_at: timestamp(row, "endpoint_created_at")?,
        updated_at: timestamp(row, "endpoint_updated_at")?,
    };
    let payload_hash: String = row.try_get("payload_hash").map_err(map_sqlx)?;
    let delivery = WebhookDelivery {
        id: row.try_get("delivery_id").map_err(map_sqlx)?,
        endpoint_id: endpoint.id,
        event_id: EventId::from_uuid(row.try_get("event_id").map_err(map_sqlx)?),
        // The row has just been put into a lease. Domain state intentionally
        // stays Pending/Failed because `record_attempt` is the transition that
        // increments attempt count after the actual outbound I/O completes.
        state,
        attempt_count,
        last_status_code,
        next_attempt_at: optional_timestamp(row, "next_attempt_at")?,
        payload_hash,
        signing_key_version: row
            .try_get::<Option<String>, _>("delivery_signing_key_version")
            .map_err(map_sqlx)?,
        last_error: row.try_get("last_error").map_err(map_sqlx)?,
        created_at: timestamp(row, "delivery_created_at")?,
        updated_at: timestamp(row, "delivery_updated_at")?,
    };
    let event = StoredEvent {
        event_type: row.try_get("event_type").map_err(map_sqlx)?,
        body: row.try_get("body").map_err(map_sqlx)?,
    };
    Ok(ClaimedWebhookDelivery {
        delivery,
        endpoint,
        event,
        fence: row.try_get("fence").map_err(map_sqlx)?,
    })
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

fn required_u32(row: &sqlx::postgres::PgRow, column: &str) -> Result<u32> {
    let raw: i32 = row.try_get(column).map_err(map_sqlx)?;
    u32::try_from(raw).map_err(|_| AppError::database(format!("stored {column} is negative")))
}

fn required_u64(row: &sqlx::postgres::PgRow, column: &str) -> Result<u64> {
    let raw: i64 = row.try_get(column).map_err(map_sqlx)?;
    u64::try_from(raw).map_err(|_| AppError::database(format!("stored {column} is negative")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_arguments_reject_unbounded_or_opaque_worker_identity() {
        assert!(validate_claim_arguments("relay-a", Duration::from_secs(30), 100).is_ok());
        assert!(validate_claim_arguments("", Duration::from_secs(30), 100).is_err());
        assert!(validate_claim_arguments("bad\nworker", Duration::from_secs(30), 100).is_err());
        assert!(validate_claim_arguments("relay", Duration::ZERO, 100).is_err());
        assert!(validate_claim_arguments("relay", Duration::from_secs(30), 0).is_err());
    }
}
