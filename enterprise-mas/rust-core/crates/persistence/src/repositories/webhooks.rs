//! PURPOSE: Tenant-scoped PostgreSQL webhook endpoint and durable fanout store.
//!
//! Owning layer: `mas-persistence` request-plane adapter.
//!
//! Production role: persist the canonical endpoint model under request RLS and
//! atomically store exact event payload bytes plus idempotent endpoint delivery
//! rows. It captures signing-key version at enqueue so a retry remains stable
//! across secret rotation.
//!
//! Security boundary: every request method requires the task-local authoritative
//! tenant scope before issuing SQL. No raw secret material enters this adapter:
//! endpoints point only to `secret_references`, payloads are bounded exact
//! event bytes, and URL parsing is tenant-safe before egress later occurs.
//!
//! Data ownership: `webhooks`, `webhook_payloads`, and `webhook_deliveries`
//! rows belong to PostgreSQL. This adapter never uses an in-memory queue or
//! direct delivery shortcut.
//!
//! Prohibited responsibilities: cross-tenant relay claiming, secret resolution,
//! HMAC signing, HTTP I/O, callback verification, authorization policy, and
//! rendering notification payloads.

use chrono::{DateTime, Utc};
use mas_application::stores::{WebhookEndpointCursor, WebhookStorePort};
use mas_common::error::AppError;
use mas_common::ids::{EventId, SecretReferenceId, TenantId, WebhookId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::value_objects::SafeUrl;
use mas_domain::webhook::{
    SignatureAlgorithm, SignatureConfig, WebhookEndpoint, WebhookRetryPolicy,
};
use mas_integrations::durable_webhooks::OutboundWebhookEnqueuePort;
use serde_json::json;
use sha2::{Digest as _, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

const ENDPOINT_COLUMNS: &str =
    "endpoint.id, endpoint.tenant_id, endpoint.url, endpoint.description, \
    endpoint.subscribed_events, endpoint.signing_secret_id, endpoint.signing_key_version, \
    endpoint.enabled, endpoint.retry_max_attempts, endpoint.retry_initial_backoff_ms, \
    endpoint.consecutive_failures, endpoint.created_at, endpoint.updated_at";

/// The normal request-plane webhook persistence adapter.
#[derive(Debug, Clone)]
pub struct PostgresWebhookStore {
    pool: PgPool,
}

impl PostgresWebhookStore {
    /// Binds request-plane operations to the application's scoped pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Exposes the pool only so composition can open the authoritative RLS
    /// scope. Repository methods themselves use `scoped_request` exclusively.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl WebhookStorePort for PostgresWebhookStore {
    async fn save(&self, endpoint: &WebhookEndpoint) -> Result<()> {
        validate_endpoint(endpoint)?;
        let scope = crate::scoped_request::current_scope()?;
        if scope.tenant_id() != endpoint.tenant_id {
            return Err(AppError::not_found("webhook", endpoint.id.to_string()));
        }
        let mut scoped = crate::scoped_request::connection().await?;
        let secret_exists = sqlx::query(
            "SELECT 1
             FROM secret_references
             WHERE tenant_id = $1
               AND id = $2
               AND availability = 'available'
               AND rotation_state <> 'retired'",
        )
        .bind(endpoint.tenant_id.into_uuid())
        .bind(endpoint.signature.secret_reference_id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?
        .is_some();
        if !secret_exists {
            return Err(AppError::invalid_field(
                "signature.secret_reference_id",
                "unavailable",
                "webhook signing secret reference is not available in this tenant",
            ));
        }

        // Legacy columns are mirrored only for an online rolling migration;
        // all reads below use canonical columns. The generated legacy name is
        // deterministic and never user-visible API authority.
        let legacy_name = format!("endpoint-{}", endpoint.id);
        let subscribed_events: Vec<&str> = endpoint
            .subscribed_events
            .iter()
            .map(String::as_str)
            .collect();
        let status = if endpoint.enabled {
            "active"
        } else {
            "disabled"
        };
        let result = sqlx::query(
            "INSERT INTO webhooks (
                 id, tenant_id, name, target_url, event_filter, signing_ref, status,
                 url, description, subscribed_events, signature_algorithm,
                 signing_secret_id, signing_key_version, enabled,
                 retry_max_attempts, retry_initial_backoff_ms, consecutive_failures,
                 created_at, updated_at
             ) VALUES (
                 $1, $2, $3, $4, $5, $6, $7,
                 $4, $8, $9, 'hmac_sha256',
                 $10, $11, $12, $13, $14, $15, $16, $17
             )
             ON CONFLICT (id) DO UPDATE SET
                 target_url = EXCLUDED.target_url,
                 event_filter = EXCLUDED.event_filter,
                 signing_ref = EXCLUDED.signing_ref,
                 status = EXCLUDED.status,
                 url = EXCLUDED.url,
                 description = EXCLUDED.description,
                 subscribed_events = EXCLUDED.subscribed_events,
                 signature_algorithm = EXCLUDED.signature_algorithm,
                 signing_secret_id = EXCLUDED.signing_secret_id,
                 signing_key_version = EXCLUDED.signing_key_version,
                 enabled = EXCLUDED.enabled,
                 retry_max_attempts = EXCLUDED.retry_max_attempts,
                 retry_initial_backoff_ms = EXCLUDED.retry_initial_backoff_ms,
                 consecutive_failures = EXCLUDED.consecutive_failures,
                 updated_at = EXCLUDED.updated_at
             WHERE webhooks.tenant_id = EXCLUDED.tenant_id",
        )
        .bind(endpoint.id.into_uuid())
        .bind(endpoint.tenant_id.into_uuid())
        .bind(legacy_name)
        .bind(endpoint.url.as_str())
        .bind(json!(subscribed_events))
        .bind(json!({"id": endpoint.signature.secret_reference_id.to_string()}))
        .bind(status)
        .bind(&endpoint.description)
        .bind(&subscribed_events)
        .bind(endpoint.signature.secret_reference_id.into_uuid())
        .bind(&endpoint.signature.key_version)
        .bind(endpoint.enabled)
        .bind(
            i32::try_from(endpoint.retry_policy.max_attempts).map_err(|_| {
                AppError::invalid_field("retry.max_attempts", "out_of_range", "value is too large")
            })?,
        )
        .bind(
            i64::try_from(endpoint.retry_policy.initial_backoff_ms).map_err(|_| {
                AppError::invalid_field(
                    "retry.initial_backoff_ms",
                    "out_of_range",
                    "value is too large",
                )
            })?,
        )
        .bind(i32::try_from(endpoint.consecutive_failures).map_err(|_| {
            AppError::invalid_field("consecutive_failures", "out_of_range", "value is too large")
        })?)
        .bind(endpoint.created_at.into_datetime())
        .bind(endpoint.updated_at.into_datetime())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found("webhook", endpoint.id.to_string()));
        }
        Ok(())
    }

    async fn get(&self, id: WebhookId) -> Result<Option<WebhookEndpoint>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(&format!(
            "SELECT {ENDPOINT_COLUMNS}
             FROM webhooks AS endpoint
             WHERE endpoint.tenant_id = $1 AND endpoint.id = $2"
        ))
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(endpoint_from_row).transpose()
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<WebhookEndpoint>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::not_found("tenant", tenant.to_string()));
        }
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {ENDPOINT_COLUMNS}
             FROM webhooks AS endpoint
             WHERE endpoint.tenant_id = $1
             ORDER BY endpoint.created_at DESC, endpoint.id DESC
             LIMIT 1001"
        ))
        .bind(tenant.into_uuid())
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(endpoint_from_row).collect()
    }

    async fn list_page(
        &self,
        tenant: TenantId,
        after: Option<WebhookEndpointCursor>,
        limit: u16,
    ) -> Result<Vec<WebhookEndpoint>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant {
            return Ok(Vec::new());
        }
        let limit = i64::from(limit.clamp(1, 201));
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(&format!(
            "SELECT {ENDPOINT_COLUMNS}
             FROM webhooks AS endpoint
             WHERE endpoint.tenant_id = $1
               AND (
                    $2::timestamptz IS NULL
                    OR endpoint.created_at < $2
                    OR (endpoint.created_at = $2 AND endpoint.id < $3)
               )
             ORDER BY endpoint.created_at DESC, endpoint.id DESC
             LIMIT $4"
        ))
        .bind(tenant.into_uuid())
        .bind(after.map(|cursor| cursor.created_at.into_datetime()))
        .bind(after.map(|cursor| cursor.id.into_uuid()))
        .bind(limit)
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(endpoint_from_row).collect()
    }
}

#[async_trait::async_trait]
impl OutboundWebhookEnqueuePort for PostgresWebhookStore {
    async fn enqueue_event(
        &self,
        tenant_id: TenantId,
        event_id: EventId,
        event_type: &str,
        body: &[u8],
    ) -> Result<u64> {
        validate_event(event_type, body)?;
        if crate::scoped_request::current_scope()?.tenant_id() != tenant_id {
            return Err(AppError::not_found("tenant", tenant_id.to_string()));
        }
        let payload_hash = hex::encode(Sha256::digest(body));
        let mut scoped = crate::scoped_request::connection().await?;

        // The request's task-local transaction is the atomicity boundary. A
        // duplicate event id with different bytes is corruption/replay and is
        // rejected, never silently merged.
        let inserted = sqlx::query(
            "INSERT INTO webhook_payloads (
                 id, tenant_id, event_id, event_type, body, body_sha256
             ) VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)
             ON CONFLICT (tenant_id, event_id) DO NOTHING
             RETURNING id, body_sha256",
        )
        .bind(tenant_id.into_uuid())
        .bind(event_id.into_uuid())
        .bind(event_type)
        .bind(body)
        .bind(&payload_hash)
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        let (payload_id, stored_hash): (Uuid, String) = if let Some(row) = inserted {
            (
                row.try_get("id").map_err(map_sqlx)?,
                row.try_get("body_sha256").map_err(map_sqlx)?,
            )
        } else {
            let existing = sqlx::query(
                "SELECT id, body_sha256
                 FROM webhook_payloads
                 WHERE tenant_id = $1 AND event_id = $2",
            )
            .bind(tenant_id.into_uuid())
            .bind(event_id.into_uuid())
            .fetch_optional(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?
            .ok_or_else(|| AppError::database("webhook payload conflict row disappeared"))?;
            (
                existing.try_get("id").map_err(map_sqlx)?,
                existing.try_get("body_sha256").map_err(map_sqlx)?,
            )
        };
        if stored_hash != payload_hash {
            return Err(AppError::conflict(
                "webhook event id was already recorded with different payload bytes",
            ));
        }

        let deliveries = sqlx::query(
            "INSERT INTO webhook_deliveries (
                 id, tenant_id, webhook_id, event_id, status, attempts,
                 next_attempt_at, payload_id, payload_hash, signing_key_version,
                 created_at, updated_at
             )
             SELECT
                 gen_random_uuid(), endpoint.tenant_id, endpoint.id, $2,
                 'pending', 0, now(), $1, $3,
                 COALESCE(endpoint.signing_key_version, signing_secret.key_version),
                 now(), now()
             FROM webhooks AS endpoint
             JOIN secret_references AS signing_secret
               ON signing_secret.id = endpoint.signing_secret_id
              AND signing_secret.tenant_id = endpoint.tenant_id
             WHERE endpoint.tenant_id = $4
               AND endpoint.enabled
               AND signing_secret.availability = 'available'
               AND signing_secret.rotation_state <> 'retired'
               AND ($5 = ANY(endpoint.subscribed_events)
                    OR '*' = ANY(endpoint.subscribed_events))
             ON CONFLICT (webhook_id, event_id) DO NOTHING",
        )
        .bind(payload_id)
        .bind(event_id.into_uuid())
        .bind(&payload_hash)
        .bind(tenant_id.into_uuid())
        .bind(event_type)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(deliveries.rows_affected())
    }
}

fn validate_endpoint(endpoint: &WebhookEndpoint) -> Result<()> {
    if endpoint.id.is_nil()
        || endpoint.tenant_id.is_nil()
        || endpoint.signature.secret_reference_id.is_nil()
    {
        return Err(AppError::invalid_field(
            "webhook",
            "required",
            "webhook identifiers and signing secret reference must be concrete",
        ));
    }
    if endpoint.url.is_internal_endpoint() {
        return Err(AppError::invalid_field(
            "url",
            "internal_endpoint_forbidden",
            "tenant webhook endpoints cannot target operator-only internal endpoints",
        ));
    }
    if endpoint.subscribed_events.is_empty() {
        return Err(AppError::invalid_field(
            "subscribed_events",
            "required",
            "webhook endpoints must subscribe to at least one event",
        ));
    }
    if !(1..=64).contains(&endpoint.retry_policy.max_attempts)
        || !(1..=300_000).contains(&endpoint.retry_policy.initial_backoff_ms)
    {
        return Err(AppError::invalid_field(
            "retry_policy",
            "out_of_range",
            "webhook retry policy is outside durable database bounds",
        ));
    }
    Ok(())
}

fn validate_event(event_type: &str, body: &[u8]) -> Result<()> {
    if event_type.is_empty()
        || event_type.len() > 128
        || !event_type.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '*')
        })
    {
        return Err(AppError::invalid_field(
            "event_type",
            "invalid_format",
            "webhook event type must be 1..=128 safe ASCII characters",
        ));
    }
    if body.is_empty() || body.len() > 1_048_576 {
        return Err(AppError::invalid_field(
            "body",
            "out_of_range",
            "webhook payload must be between 1 byte and 1 MiB",
        ));
    }
    Ok(())
}

fn endpoint_from_row(row: &sqlx::postgres::PgRow) -> Result<WebhookEndpoint> {
    let url: String = row.try_get("url").map_err(map_sqlx)?;
    let url = SafeUrl::parse(&url).map_err(|_| {
        AppError::database("persisted webhook URL no longer satisfies tenant-safe URL invariants")
    })?;
    let subscribed_events: Vec<String> = row.try_get("subscribed_events").map_err(map_sqlx)?;
    let endpoint = WebhookEndpoint {
        id: WebhookId::from_uuid(row.try_get("id").map_err(map_sqlx)?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(map_sqlx)?),
        url,
        description: row.try_get("description").map_err(map_sqlx)?,
        subscribed_events: subscribed_events.into_iter().collect(),
        signature: SignatureConfig {
            algorithm: SignatureAlgorithm::HmacSha256,
            secret_reference_id: SecretReferenceId::from_uuid(
                row.try_get("signing_secret_id").map_err(map_sqlx)?,
            ),
            key_version: row.try_get("signing_key_version").map_err(map_sqlx)?,
        },
        enabled: row.try_get("enabled").map_err(map_sqlx)?,
        retry_policy: WebhookRetryPolicy {
            max_attempts: non_negative_u32(row, "retry_max_attempts")?,
            initial_backoff_ms: non_negative_u64(row, "retry_initial_backoff_ms")?,
        },
        consecutive_failures: non_negative_u32(row, "consecutive_failures")?,
        created_at: timestamp(row, "created_at")?,
        updated_at: timestamp(row, "updated_at")?,
    };
    validate_endpoint(&endpoint)
        .map_err(|_| AppError::database("stored webhook endpoint violates domain invariants"))?;
    Ok(endpoint)
}

fn timestamp(row: &sqlx::postgres::PgRow, column: &str) -> Result<Timestamp> {
    row.try_get::<DateTime<Utc>, _>(column)
        .map(Timestamp::from_datetime)
        .map_err(map_sqlx)
}

fn non_negative_u32(row: &sqlx::postgres::PgRow, column: &str) -> Result<u32> {
    let value: i32 = row.try_get(column).map_err(map_sqlx)?;
    u32::try_from(value).map_err(|_| AppError::database(format!("stored {column} is negative")))
}

fn non_negative_u64(row: &sqlx::postgres::PgRow, column: &str) -> Result<u64> {
    let value: i64 = row.try_get(column).map_err(map_sqlx)?;
    u64::try_from(value).map_err(|_| AppError::database(format!("stored {column} is negative")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_input_is_bounded_and_does_not_allow_control_type_names() {
        assert!(validate_event("execution.completed", b"{}").is_ok());
        assert!(validate_event("", b"{}").is_err());
        assert!(validate_event("execution\ncompleted", b"{}").is_err());
        assert!(validate_event("event", &[]).is_err());
        assert!(validate_event("event", &vec![0_u8; 1_048_577]).is_err());
    }
}
