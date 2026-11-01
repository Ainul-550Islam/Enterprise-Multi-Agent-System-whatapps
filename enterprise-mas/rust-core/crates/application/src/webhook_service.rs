//! PURPOSE: Tenant-scoped outbound webhook delivery inspection and dead-letter
//! replay use cases.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: validate bounded keyset queries, hide foreign
//! endpoint/delivery ids, allow replay only for durable dead letters, preserve
//! event idempotency, and audit every replay.
//!
//! Data ownership: persistence owns delivery, payload, dead-letter, and replay
//! facts; this service owns authorization-independent use-case sequencing.
//!
//! MUST NOT: read or return event payload bytes, resolve signing keys, sign
//! requests, dispatch HTTP, or retry a non-dead-lettered delivery.

use mas_common::error::AppError;
use mas_common::ids::{EventId, TenantId, UserId, WebhookId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;

/// Stable keyset position for a delivery-history page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryCursor {
    pub created_at: Timestamp,
    pub id: uuid::Uuid,
}

/// Redacted durable delivery projection; it never carries payload bytes or a
/// signing reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryRecord {
    pub id: uuid::Uuid,
    pub tenant_id: TenantId,
    pub endpoint_id: WebhookId,
    pub event_id: EventId,
    pub state: String,
    pub attempt_count: u32,
    pub last_status_code: Option<u16>,
    pub next_attempt_at: Option<Timestamp>,
    pub payload_hash: String,
    pub last_error: Option<String>,
    pub created_at: Timestamp,
}

/// Bounded delivery history page.
#[derive(Debug, Clone)]
pub struct DeliveryPage {
    pub items: Vec<DeliveryRecord>,
    pub next_cursor: Option<String>,
}

/// Persistence port for redacted webhook administration.
#[async_trait::async_trait]
pub trait WebhookAdminPort: std::fmt::Debug + Send + Sync {
    /// Reads one bounded page of an endpoint's delivery history.
    ///
    /// Contract (mirrored by the PostgreSQL implementation): an endpoint that
    /// is not visible under `tenant` is `RESOURCE_NOT_FOUND` — never an empty
    /// page, so a foreign id cannot be told apart from an empty own id.
    async fn list_deliveries(
        &self,
        tenant: TenantId,
        endpoint: WebhookId,
        after: Option<DeliveryCursor>,
        limit: u16,
    ) -> Result<Vec<DeliveryRecord>>;
    async fn replay_dead_letter(
        &self,
        tenant: TenantId,
        endpoint: WebhookId,
        delivery: uuid::Uuid,
        actor: Option<UserId>,
    ) -> Result<DeliveryRecord>;
}

/// Webhook delivery administration service.
#[derive(Debug)]
pub struct WebhookService {
    store: Box<dyn WebhookAdminPort>,
    audit: Box<dyn AuditSinkPort>,
}

impl WebhookService {
    /// Composes tenant-scoped persistence and mandatory audit.
    pub fn new(store: Box<dyn WebhookAdminPort>, audit: Box<dyn AuditSinkPort>) -> Self {
        Self { store, audit }
    }

    /// Reads a bounded descending delivery history page.
    pub async fn deliveries(
        &self,
        ctx: &ServiceContext,
        endpoint: WebhookId,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<DeliveryPage> {
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "delivery page limit must be between 1 and 200",
            ));
        }
        let tenant = ctx.require_tenant()?;
        let after = cursor.map(decode_cursor).transpose()?;
        let mut items = self
            .store
            .list_deliveries(tenant, endpoint, after, limit.saturating_add(1))
            .await?;
        let more = items.len() > usize::from(limit);
        if more {
            items.truncate(usize::from(limit));
        }
        let next_cursor = if more {
            items.last().map(encode_cursor).transpose()?
        } else {
            None
        };
        Ok(DeliveryPage { items, next_cursor })
    }

    /// Requeues exactly one dead-lettered delivery. The durable store preserves
    /// its event id and prevents a second concurrent replay from creating work.
    pub async fn replay(
        &self,
        ctx: &ServiceContext,
        endpoint: WebhookId,
        delivery: uuid::Uuid,
    ) -> Result<DeliveryRecord> {
        let tenant = ctx.require_tenant()?;
        let actor = if ctx.actor.kind == mas_domain::AuditActorKind::User {
            UserId::parse_str(&ctx.actor.id).ok()
        } else {
            None
        };
        let result = self
            .store
            .replay_dead_letter(tenant, endpoint, delivery, actor)
            .await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "webhook.delivery.replay",
            "webhook_delivery",
            Some(delivery.to_string()),
            mas_domain::AuditOutcome::Success,
        )
        .await?;
        Ok(result)
    }
}

fn encode_cursor(record: &DeliveryRecord) -> Result<String> {
    use base64::Engine as _;
    // Serialization of two scalar columns cannot fail in practice, but this
    // runs inside a live request (keyset paging) — an error must travel the
    // envelope, not panic the worker.
    let bytes =
        serde_json::to_vec(&serde_json::json!({"created_at":record.created_at,"id":record.id}))
            .map_err(|error| {
                AppError::serialization(format!("delivery cursor serialization: {error}"))
            })?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(encoded: &str) -> Result<DeliveryCursor> {
    use base64::Engine as _;
    #[derive(Deserialize)]
    struct Wire {
        created_at: Timestamp,
        id: uuid::Uuid,
    }
    if encoded.is_empty()
        || encoded.len() > 512
        || !encoded
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'-' | b'_'))
    {
        return Err(AppError::invalid_field(
            "cursor",
            "invalid_format",
            "delivery cursor is invalid",
        ));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| {
            AppError::invalid_field("cursor", "invalid_format", "delivery cursor is invalid")
        })?;
    let wire: Wire = serde_json::from_slice(&bytes).map_err(|_| {
        AppError::invalid_field("cursor", "invalid_format", "delivery cursor is invalid")
    })?;
    Ok(DeliveryCursor {
        created_at: wire.created_at,
        id: wire.id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_is_bounded_and_tamper_checked_for_wire_shape() {
        let record = DeliveryRecord {
            id: uuid::Uuid::now_v7(),
            tenant_id: TenantId::new(),
            endpoint_id: WebhookId::new(),
            event_id: EventId::new(),
            state: "dead_lettered".to_owned(),
            attempt_count: 1,
            last_status_code: None,
            next_attempt_at: None,
            payload_hash: "a".repeat(64),
            last_error: None,
            created_at: Timestamp::now(),
        };
        let encoded = encode_cursor(&record).expect("cursor encode");
        let decoded = decode_cursor(&encoded).expect("cursor");
        assert_eq!(decoded.id, record.id);
        assert!(decode_cursor("../../secret").is_err());
    }

    #[derive(Debug)]
    struct RejectNonDeadLetter;

    #[async_trait::async_trait]
    impl WebhookAdminPort for RejectNonDeadLetter {
        async fn list_deliveries(
            &self,
            _tenant: TenantId,
            endpoint: WebhookId,
            _after: Option<DeliveryCursor>,
            _limit: u16,
        ) -> Result<Vec<DeliveryRecord>> {
            // This fixture exposes no endpoints; the port contract requires a
            // non-visible endpoint to be `not_found`, not an empty page.
            Err(AppError::not_found("webhook", endpoint.to_string()))
        }

        async fn replay_dead_letter(
            &self,
            _tenant: TenantId,
            _endpoint: WebhookId,
            _delivery: uuid::Uuid,
            _actor: Option<UserId>,
        ) -> Result<DeliveryRecord> {
            Err(AppError::conflict(
                "only dead-lettered webhook deliveries may be replayed",
            ))
        }
    }

    #[tokio::test]
    async fn a_non_dead_lettered_delivery_is_refused_without_a_success_audit() {
        let audit = std::sync::Arc::new(audit::InMemoryAuditSink::new());
        let service = WebhookService::new(Box::new(RejectNonDeadLetter), Box::new(audit.clone()));
        let tenant = TenantId::new();
        let context =
            ServiceContext::for_service("webhook-test", "non-dead-letter", &Timestamp::now())
                .expect("context")
                .with_scope(tenant, mas_common::ids::OrganizationId::new());
        let result = service
            .replay(&context, WebhookId::new(), uuid::Uuid::now_v7())
            .await;
        assert!(result.is_err());
        assert!(
            audit.is_empty(),
            "refused replay must not be recorded as a successful mutation"
        );
    }
}
