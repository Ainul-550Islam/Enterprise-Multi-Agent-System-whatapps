//! PURPOSE: PostgreSQL implementation of verified inbound webhook receipt,
//! sequence-ordering, replay-prevention, and transactional-outbox persistence.
//!
//! Owning layer: `mas-persistence` callback/relay adapter.
//!
//! Production role: after the integration ingress has verified an HMAC, record
//! a tenant-scoped receipt exactly once, serialize ordered sources by a locked
//! cursor, defer sequence gaps deterministically, and insert each newly
//! contiguous event into the transactional outbox in the same transaction.
//!
//! Security boundary: this adapter requires the dedicated `mas_inbound` pool
//! defined by migration 0041. It never receives an unverified request, raw
//! signing key, or caller-selected SQL scope, and does not log payloads.
//!
//! Data ownership: receipt rows own verified event evidence/order state;
//! cursor rows own source progress; outbox rows own relay state. The external
//! provider owns event origin and the secret manager owns signing key bytes.
//!
//! Prohibited responsibilities: signature verification, route lookup, tenant
//! discovery from public input, secret resolution, direct broker publication,
//! or fake duplicate/accepted outcomes.

use chrono::{DateTime, Utc};
use mas_common::error::AppError;
use mas_common::ids::{InboundWebhookId, OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_events::{EventEnvelope, OutboxRecord};
use mas_integrations::inbound_webhooks::{
    InboundWebhookDisposition, InboundWebhookEndpoint, InboundWebhookEndpointCursor,
    InboundWebhookEndpointStorePort, InboundWebhookReceiptPort, InboundWebhookRoute,
    InboundWebhookRoutePort, VerifiedInboundWebhook,
};
use serde_json::json;
use sqlx::{PgConnection, PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::repositories::outbox::PostgresOutboxStore;

const INBOUND_EVENT_TYPE: &str = "integration.inbound_webhook.received";
const INBOUND_EVENT_VERSION: u32 = 1;
const INBOUND_AGGREGATE_TYPE: &str = "inbound_webhook";

/// Dedicated callback-pool adapter. The pool must authenticate as `mas_inbound`;
/// normal request-scoped `mas_app` pools must not be substituted because public
/// webhook callbacks have no authenticated user scope to set RLS GUCs.
#[derive(Debug, Clone)]
pub struct PostgresInboundWebhookReceiptStore {
    inbound_pool: PgPool,
}

impl PostgresInboundWebhookReceiptStore {
    /// Creates the receipt adapter over the dedicated callback-role pool.
    #[must_use]
    pub const fn new(inbound_pool: PgPool) -> Self {
        Self { inbound_pool }
    }

    /// Exposed only for bootstrap readiness checks; repository operations do
    /// not expose arbitrary SQL or permit a caller-supplied executor.
    #[must_use]
    pub const fn inbound_pool(&self) -> &PgPool {
        &self.inbound_pool
    }
}

/// Split request/public-route adapter for canonical inbound endpoint metadata.
/// The public lookup pool must authenticate as `mas_inbound`; it has a
/// column-level grant limited to routing fields and cannot read secret paths.
#[derive(Debug, Clone)]
pub struct PostgresInboundWebhookEndpointStore {
    request_pool: PgPool,
    inbound_pool: PgPool,
}

impl PostgresInboundWebhookEndpointStore {
    /// Creates the endpoint adapter with isolated request and public-route pools.
    #[must_use]
    pub const fn new(request_pool: PgPool, inbound_pool: PgPool) -> Self {
        Self {
            request_pool,
            inbound_pool,
        }
    }

    /// Request-pool composition accessor. Endpoint writes still require the
    /// task-local authoritative RLS transaction.
    #[must_use]
    pub const fn request_pool(&self) -> &PgPool {
        &self.request_pool
    }

    /// Dedicated public-ingress route lookup pool accessor for readiness only.
    #[must_use]
    pub const fn inbound_pool(&self) -> &PgPool {
        &self.inbound_pool
    }
}

#[async_trait::async_trait]
impl InboundWebhookEndpointStorePort for PostgresInboundWebhookEndpointStore {
    async fn save(&self, endpoint: &InboundWebhookEndpoint) -> Result<()> {
        endpoint.validate()?;
        let scope = crate::scoped_request::current_scope()?;
        if scope.tenant_id() != endpoint.tenant_id
            || scope.organization_id() != endpoint.organization_id
        {
            return Err(AppError::not_found(
                "inbound_webhook_endpoint",
                endpoint.id.to_string(),
            ));
        }
        let accepted_key_versions: Vec<String> =
            endpoint.accepted_key_versions.iter().cloned().collect();
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "INSERT INTO inbound_webhook_endpoints (
                 id, tenant_id, organization_id, source,
                 signing_secret_reference_id, required_key_version,
                 accepted_key_versions, enabled, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), now())
             ON CONFLICT (id) DO UPDATE SET
                 source = EXCLUDED.source,
                 signing_secret_reference_id = EXCLUDED.signing_secret_reference_id,
                 required_key_version = EXCLUDED.required_key_version,
                 accepted_key_versions = EXCLUDED.accepted_key_versions,
                 enabled = EXCLUDED.enabled,
                 updated_at = now()
             WHERE inbound_webhook_endpoints.tenant_id = EXCLUDED.tenant_id
               AND inbound_webhook_endpoints.organization_id = EXCLUDED.organization_id",
        )
        .bind(endpoint.id.into_uuid())
        .bind(endpoint.tenant_id.into_uuid())
        .bind(endpoint.organization_id.into_uuid())
        .bind(&endpoint.source)
        .bind(endpoint.signing_secret_reference_id.into_uuid())
        .bind(&endpoint.required_key_version)
        .bind(&accepted_key_versions)
        .bind(endpoint.enabled)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found(
                "inbound_webhook_endpoint",
                endpoint.id.to_string(),
            ));
        }
        Ok(())
    }

    async fn get(
        &self,
        tenant_id: TenantId,
        endpoint_id: InboundWebhookId,
    ) -> Result<Option<InboundWebhookEndpoint>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant_id {
            return Ok(None);
        }
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, organization_id, source,
                    signing_secret_reference_id, required_key_version,
                    accepted_key_versions, enabled, created_at, updated_at
             FROM inbound_webhook_endpoints
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant_id.into_uuid())
        .bind(endpoint_id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(inbound_endpoint_from_row).transpose()
    }

    async fn list_page(
        &self,
        tenant_id: TenantId,
        after: Option<InboundWebhookEndpointCursor>,
        limit: u16,
    ) -> Result<Vec<InboundWebhookEndpoint>> {
        if crate::scoped_request::current_scope()?.tenant_id() != tenant_id {
            return Ok(Vec::new());
        }
        let limit = i64::from(limit.clamp(1, 201));
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, organization_id, source,
                    signing_secret_reference_id, required_key_version,
                    accepted_key_versions, enabled, created_at, updated_at
             FROM inbound_webhook_endpoints
             WHERE tenant_id = $1
               AND (
                    $2::timestamptz IS NULL
                    OR created_at < $2
                    OR (created_at = $2 AND id < $3)
               )
             ORDER BY created_at DESC, id DESC
             LIMIT $4",
        )
        .bind(tenant_id.into_uuid())
        .bind(after.map(|cursor| cursor.created_at.into_datetime()))
        .bind(after.map(|cursor| cursor.id.into_uuid()))
        .bind(limit)
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter().map(inbound_endpoint_from_row).collect()
    }
}

#[async_trait::async_trait]
impl InboundWebhookRoutePort for PostgresInboundWebhookEndpointStore {
    async fn route(&self, opaque_route_id: &str) -> Result<Option<InboundWebhookRoute>> {
        let Ok(endpoint_id) = opaque_route_id.parse::<InboundWebhookId>() else {
            return Ok(None);
        };
        let row = sqlx::query(
            "SELECT tenant_id, organization_id, source,
                    signing_secret_reference_id, required_key_version,
                    accepted_key_versions
             FROM inbound_webhook_endpoints
             WHERE id = $1 AND enabled = true",
        )
        .bind(endpoint_id.into_uuid())
        .fetch_optional(&self.inbound_pool)
        .await
        .map_err(map_sqlx)?;
        row.map(|row| {
            let route = InboundWebhookRoute {
                tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(map_sqlx)?),
                organization_id: Some(OrganizationId::from_uuid(
                    row.try_get("organization_id").map_err(map_sqlx)?,
                )),
                source: row.try_get("source").map_err(map_sqlx)?,
                signing_secret_reference_id: mas_common::ids::SecretReferenceId::from_uuid(
                    row.try_get("signing_secret_reference_id")
                        .map_err(map_sqlx)?,
                ),
                required_key_version: row.try_get("required_key_version").map_err(map_sqlx)?,
                accepted_key_versions: accepted_key_versions_from_row(&row)?,
            };
            route.validate()?;
            Ok(route)
        })
        .transpose()
    }
}

fn accepted_key_versions_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<std::collections::BTreeSet<String>> {
    let stored = row
        .try_get::<Vec<String>, _>("accepted_key_versions")
        .map_err(map_sqlx)?;
    let values = stored
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if values.len() != stored.len() {
        return Err(AppError::database(
            "stored inbound endpoint has duplicate accepted key versions",
        ));
    }
    Ok(values)
}

fn inbound_endpoint_from_row(row: sqlx::postgres::PgRow) -> Result<InboundWebhookEndpoint> {
    let endpoint = InboundWebhookEndpoint {
        id: InboundWebhookId::from_uuid(row.try_get("id").map_err(map_sqlx)?),
        tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(map_sqlx)?),
        organization_id: OrganizationId::from_uuid(
            row.try_get("organization_id").map_err(map_sqlx)?,
        ),
        source: row.try_get("source").map_err(map_sqlx)?,
        signing_secret_reference_id: mas_common::ids::SecretReferenceId::from_uuid(
            row.try_get("signing_secret_reference_id")
                .map_err(map_sqlx)?,
        ),
        required_key_version: row.try_get("required_key_version").map_err(map_sqlx)?,
        accepted_key_versions: accepted_key_versions_from_row(&row)?,
        enabled: row.try_get("enabled").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(
            row.try_get::<DateTime<Utc>, _>("created_at")
                .map_err(map_sqlx)?,
        ),
        updated_at: Timestamp::from_datetime(
            row.try_get::<DateTime<Utc>, _>("updated_at")
                .map_err(map_sqlx)?,
        ),
    };
    endpoint
        .validate()
        .map_err(|_| AppError::database("stored inbound webhook endpoint violates invariants"))?;
    Ok(endpoint)
}

#[derive(Debug)]
struct ReceiptRow {
    id: Uuid,
    tenant_id: TenantId,
    organization_id: Option<OrganizationId>,
    source: String,
    external_event_id: String,
    sequence_no: Option<i64>,
    payload: serde_json::Value,
    payload_sha256: String,
    signature_key_version: Option<String>,
    received_at: Timestamp,
    processing_state: String,
}

impl ReceiptRow {
    fn from_row(row: sqlx::postgres::PgRow) -> Result<Self> {
        Ok(Self {
            id: row.try_get("id").map_err(map_sqlx)?,
            tenant_id: TenantId::from_uuid(row.try_get("tenant_id").map_err(map_sqlx)?),
            organization_id: row
                .try_get::<Option<Uuid>, _>("organization_id")
                .map_err(map_sqlx)?
                .map(OrganizationId::from_uuid),
            source: row.try_get("source").map_err(map_sqlx)?,
            external_event_id: row.try_get("external_event_id").map_err(map_sqlx)?,
            sequence_no: row.try_get("sequence_no").map_err(map_sqlx)?,
            payload: row.try_get("payload").map_err(map_sqlx)?,
            payload_sha256: row.try_get("payload_sha256").map_err(map_sqlx)?,
            signature_key_version: row.try_get("signature_key_version").map_err(map_sqlx)?,
            received_at: Timestamp::from_datetime(
                row.try_get::<DateTime<Utc>, _>("received_at")
                    .map_err(map_sqlx)?,
            ),
            processing_state: row.try_get("processing_state").map_err(map_sqlx)?,
        })
    }

    fn validate_stored(&self) -> Result<()> {
        if self.id.is_nil()
            || self.tenant_id.is_nil()
            || self.source.is_empty()
            || self.source.len() > 128
            || self.external_event_id.is_empty()
            || self.external_event_id.len() > 256
            || self.sequence_no.is_some_and(|value| value < 0)
            || self.payload_sha256.len() != 64
            || !self
                .payload_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || !matches!(
                self.processing_state.as_str(),
                "accepted" | "deferred" | "processed"
            )
        {
            return Err(AppError::database(
                "stored inbound webhook receipt violates canonical invariants",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl InboundWebhookReceiptPort for PostgresInboundWebhookReceiptStore {
    async fn record_verified(
        &self,
        webhook: &VerifiedInboundWebhook,
    ) -> Result<InboundWebhookDisposition> {
        webhook.validate()?;
        let mut transaction = self.inbound_pool.begin().await.map_err(map_sqlx)?;
        // Fail boundedly when a hot source is currently advancing. Retrying a
        // verified delivery is safe because its receipt key is idempotent.
        sqlx::query("SET LOCAL lock_timeout = '5s'")
            .execute(transaction.as_mut())
            .await
            .map_err(map_sqlx)?;
        let outcome = record_verified_in(transaction.as_mut(), webhook).await?;
        transaction.commit().await.map_err(map_sqlx)?;
        Ok(outcome)
    }
}

async fn record_verified_in(
    connection: &mut PgConnection,
    webhook: &VerifiedInboundWebhook,
) -> Result<InboundWebhookDisposition> {
    let inserted = sqlx::query(
        "INSERT INTO inbound_webhook_receipts (
             id, tenant_id, organization_id, source, external_event_id,
             sequence_no, payload, payload_sha256, signature_key_version,
             received_at, processing_state
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), 'accepted')
         ON CONFLICT DO NOTHING
         RETURNING id, tenant_id, organization_id, source, external_event_id,
                   sequence_no, payload, payload_sha256, signature_key_version,
                   received_at, processing_state",
    )
    .bind(Uuid::now_v7())
    .bind(webhook.tenant_id.into_uuid())
    .bind(webhook.organization_id.map(|id| id.into_uuid()))
    .bind(&webhook.source)
    .bind(&webhook.external_event_id)
    .bind(webhook.sequence_no)
    .bind(&webhook.payload)
    .bind(&webhook.payload_sha256)
    .bind(&webhook.signature_key_version)
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;

    let Some(receipt) = inserted.map(ReceiptRow::from_row).transpose()? else {
        return existing_conflict_disposition(connection, webhook).await;
    };
    receipt.validate_stored()?;

    let Some(sequence_no) = receipt.sequence_no else {
        emit_receipt(connection, &receipt).await?;
        return Ok(InboundWebhookDisposition::Accepted);
    };

    let last_processed_sequence =
        lock_cursor(connection, receipt.tenant_id, &receipt.source).await?;
    let expected = last_processed_sequence
        .checked_add(1)
        .ok_or_else(|| AppError::conflict("inbound webhook sequence space is exhausted"))?;
    if sequence_no > expected {
        set_deferred(connection, receipt.id).await?;
        return Ok(InboundWebhookDisposition::Deferred);
    }
    if sequence_no < expected {
        // A sequence can only be below the cursor when the unique sequence
        // index should already have found a prior receipt. Treat any anomaly
        // as a safe conflict instead of attempting to reorder an event.
        return Err(AppError::conflict(
            "inbound webhook sequence is behind the durable source cursor",
        ));
    }

    emit_receipt(connection, &receipt).await?;
    advance_cursor(connection, receipt.tenant_id, &receipt.source, sequence_no).await?;

    // The source cursor lock serializes all writers for this source. Release
    // each precisely next deferred receipt in one transaction so a gap close
    // atomically produces a deterministic contiguous outbox prefix.
    let mut next_sequence = sequence_no
        .checked_add(1)
        .ok_or_else(|| AppError::conflict("inbound webhook sequence space is exhausted"))?;
    while let Some(deferred) = deferred_at(
        connection,
        receipt.tenant_id,
        &receipt.source,
        next_sequence,
    )
    .await?
    {
        emit_receipt(connection, &deferred).await?;
        advance_cursor(
            connection,
            receipt.tenant_id,
            &receipt.source,
            next_sequence,
        )
        .await?;
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| AppError::conflict("inbound webhook sequence space is exhausted"))?;
    }
    Ok(InboundWebhookDisposition::Accepted)
}

async fn existing_conflict_disposition(
    connection: &mut PgConnection,
    webhook: &VerifiedInboundWebhook,
) -> Result<InboundWebhookDisposition> {
    let existing = sqlx::query(
        "SELECT id, tenant_id, organization_id, source, external_event_id,
                sequence_no, payload, payload_sha256, signature_key_version,
                received_at, processing_state
         FROM inbound_webhook_receipts
         WHERE tenant_id = $1 AND source = $2 AND external_event_id = $3",
    )
    .bind(webhook.tenant_id.into_uuid())
    .bind(&webhook.source)
    .bind(&webhook.external_event_id)
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if let Some(existing) = existing.map(ReceiptRow::from_row).transpose()? {
        existing.validate_stored()?;
        if existing.payload_sha256 == webhook.payload_sha256
            && existing.sequence_no == webhook.sequence_no
            && existing.organization_id == webhook.organization_id
            && existing.signature_key_version == webhook.signature_key_version
        {
            return Ok(InboundWebhookDisposition::Duplicate);
        }
        return Err(AppError::conflict(
            "inbound webhook external event id conflicts with existing verified evidence",
        ));
    }

    if let Some(sequence_no) = webhook.sequence_no {
        let collision = sqlx::query(
            "SELECT 1
             FROM inbound_webhook_receipts
             WHERE tenant_id = $1 AND source = $2 AND sequence_no = $3",
        )
        .bind(webhook.tenant_id.into_uuid())
        .bind(&webhook.source)
        .bind(sequence_no)
        .fetch_optional(&mut *connection)
        .await
        .map_err(map_sqlx)?;
        if collision.is_some() {
            return Err(AppError::conflict(
                "inbound webhook sequence conflicts with existing verified evidence",
            ));
        }
    }
    Err(AppError::database(
        "inbound webhook receipt insert conflicted without a visible canonical receipt",
    ))
}

async fn lock_cursor(
    connection: &mut PgConnection,
    tenant_id: TenantId,
    source: &str,
) -> Result<i64> {
    sqlx::query(
        "INSERT INTO inbound_webhook_order_cursors (
             tenant_id, source, last_processed_sequence, updated_at
         ) VALUES ($1, $2, -1, now())
         ON CONFLICT (tenant_id, source) DO NOTHING",
    )
    .bind(tenant_id.into_uuid())
    .bind(source)
    .execute(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    let row = sqlx::query(
        "SELECT last_processed_sequence
         FROM inbound_webhook_order_cursors
         WHERE tenant_id = $1 AND source = $2
         FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(source)
    .fetch_one(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    row.try_get("last_processed_sequence").map_err(map_sqlx)
}

async fn set_deferred(connection: &mut PgConnection, receipt_id: Uuid) -> Result<()> {
    let result = sqlx::query(
        "UPDATE inbound_webhook_receipts
         SET processing_state = 'deferred'
         WHERE id = $1 AND processing_state = 'accepted'",
    )
    .bind(receipt_id)
    .execute(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if result.rows_affected() != 1 {
        return Err(AppError::database(
            "new inbound webhook receipt could not transition to deferred",
        ));
    }
    Ok(())
}

async fn deferred_at(
    connection: &mut PgConnection,
    tenant_id: TenantId,
    source: &str,
    sequence_no: i64,
) -> Result<Option<ReceiptRow>> {
    let row = sqlx::query(
        "SELECT id, tenant_id, organization_id, source, external_event_id,
                sequence_no, payload, payload_sha256, signature_key_version,
                received_at, processing_state
         FROM inbound_webhook_receipts
         WHERE tenant_id = $1
           AND source = $2
           AND sequence_no = $3
           AND processing_state = 'deferred'
         FOR UPDATE",
    )
    .bind(tenant_id.into_uuid())
    .bind(source)
    .bind(sequence_no)
    .fetch_optional(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    row.map(ReceiptRow::from_row).transpose()
}

async fn emit_receipt(connection: &mut PgConnection, receipt: &ReceiptRow) -> Result<()> {
    receipt.validate_stored()?;
    if !matches!(receipt.processing_state.as_str(), "accepted" | "deferred") {
        return Err(AppError::database(
            "attempted to emit an inbound webhook receipt outside an emit-ready state",
        ));
    }
    let payload = json!({
        "source": &receipt.source,
        "external_event_id": &receipt.external_event_id,
        "sequence_no": receipt.sequence_no,
        "payload_sha256": &receipt.payload_sha256,
        "signature_key_version": &receipt.signature_key_version,
        "payload": &receipt.payload,
    });
    let mut builder = EventEnvelope::builder(
        INBOUND_EVENT_TYPE,
        INBOUND_EVENT_VERSION,
        INBOUND_AGGREGATE_TYPE,
        receipt.id.to_string(),
        receipt.tenant_id,
    )?
    .occurred_at(receipt.received_at)
    .with_correlation(format!("inbound-webhook:{}", receipt.id))?
    .with_payload(payload)
    .with_header("source", receipt.source.clone())
    .with_header("payload-sha256", receipt.payload_sha256.clone());
    if let Some(organization_id) = receipt.organization_id {
        builder = builder.with_organization(organization_id);
    }
    let envelope = builder.build()?;
    let event_id = envelope.id;
    let inserted =
        PostgresOutboxStore::enqueue_in(connection, &OutboxRecord::new(envelope)).await?;
    if !inserted {
        return Err(AppError::conflict(
            "inbound webhook outbox event unexpectedly already exists",
        ));
    }
    let result = sqlx::query(
        "UPDATE inbound_webhook_receipts
         SET processing_state = 'processed', outbox_event_id = $2, processed_at = now()
         WHERE id = $1 AND processing_state IN ('accepted', 'deferred')",
    )
    .bind(receipt.id)
    .bind(event_id.into_uuid())
    .execute(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if result.rows_affected() != 1 {
        return Err(AppError::database(
            "inbound webhook receipt lost its emit-ready state during settlement",
        ));
    }
    Ok(())
}

async fn advance_cursor(
    connection: &mut PgConnection,
    tenant_id: TenantId,
    source: &str,
    sequence_no: i64,
) -> Result<()> {
    let result = sqlx::query(
        "UPDATE inbound_webhook_order_cursors
         SET last_processed_sequence = $3, updated_at = now()
         WHERE tenant_id = $1
           AND source = $2
           AND last_processed_sequence < $3",
    )
    .bind(tenant_id.into_uuid())
    .bind(source)
    .bind(sequence_no)
    .execute(&mut *connection)
    .await
    .map_err(map_sqlx)?;
    if result.rows_affected() != 1 {
        return Err(AppError::conflict(
            "inbound webhook source cursor was not owned by this transaction",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_receipt_rejects_noncanonical_hash_and_state() {
        let row = ReceiptRow {
            id: Uuid::now_v7(),
            tenant_id: TenantId::new(),
            organization_id: None,
            source: "provider.events".to_owned(),
            external_event_id: "evt-1".to_owned(),
            sequence_no: Some(0),
            payload: json!({"ok": true}),
            payload_sha256: "A".repeat(64),
            signature_key_version: None,
            received_at: Timestamp::now(),
            processing_state: "accepted".to_owned(),
        };
        assert!(row.validate_stored().is_err());
    }

    #[test]
    fn inbound_event_type_remains_canonical() {
        assert_eq!(INBOUND_EVENT_TYPE, "integration.inbound_webhook.received");
        assert!(mas_common::ids::EventId::new().into_uuid() != Uuid::nil());
    }
}
