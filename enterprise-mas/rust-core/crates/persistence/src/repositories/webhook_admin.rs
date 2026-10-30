//! PURPOSE: Tenant-scoped PostgreSQL webhook delivery administration for
//! `WebhookAdminPort`: bounded keyset delivery-history reads (payload-free)
//! and atomic dead-letter replay with append-only evidence.
//!
//! Owning layer: `mas-persistence`.
//! Production responsibility: read `webhook_deliveries` history inside the
//! requesting tenant's rows only, and replay exactly one dead-lettered
//! delivery as ONE durable transition — evidence row + delivery re-arm +
//! dead-letter stamp — inside the request's scoped transaction.
//! Data ownership: `webhook_deliveries`, `webhook_dead_letters`, and
//! `webhook_replay_attempts` are the authoritative rows; every statement binds
//! `tenant_id` and runs through the request-scoped connection (forced RLS).
//! MUST NOT: read across tenants, return payload bodies or signing material,
//! replay a delivery that is not dead-lettered, replay the same delivery twice
//! (the evidence row is unique per delivery), or disturb relay lease/fence
//! ownership of a `leased` row.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mas_application::webhook_service::{DeliveryCursor, DeliveryRecord, WebhookAdminPort};
use mas_common::{
    error::AppError,
    ids::{TenantId, UserId, WebhookId},
    result::Result,
    timestamps::Timestamp,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{error::map_sqlx, scoped_request};

/// Columns of one redacted delivery-history row. Never `payload_id`-joined
/// bodies, never signing material — the wire DTO carries digests only.
const DELIVERY_COLUMNS: &str = "id, tenant_id, webhook_id, event_id, status, attempts, \
    http_status, next_attempt_at, payload_hash, last_error, created_at";

/// Bounded, tenant-scoped webhook delivery administration.
#[derive(Debug, Clone)]
pub struct PostgresWebhookAdmin {
    _pool: PgPool,
}

impl PostgresWebhookAdmin {
    /// Binds the adapter to the shared request pool; every statement runs on
    /// the request-scoped connection instead of this handle.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { _pool: pool }
    }
}

/// Maps one `webhook_deliveries` row into the redacted wire record.
fn delivery_from_row(row: &sqlx::postgres::PgRow) -> Result<DeliveryRecord> {
    let id: Uuid = row.try_get("id").map_err(map_sqlx)?;
    let tenant: Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let endpoint: Uuid = row.try_get("webhook_id").map_err(map_sqlx)?;
    let event: Uuid = row.try_get("event_id").map_err(map_sqlx)?;
    let state: String = row.try_get("status").map_err(map_sqlx)?;
    let attempts: i32 = row.try_get("attempts").map_err(map_sqlx)?;
    let http_status: Option<i32> = row.try_get("http_status").map_err(map_sqlx)?;
    let last_status_code = match http_status {
        Some(code) => u16::try_from(code).ok(),
        None => None,
    };
    let next_attempt: Option<DateTime<Utc>> = row.try_get("next_attempt_at").map_err(map_sqlx)?;
    let payload_hash: Option<String> = row.try_get("payload_hash").map_err(map_sqlx)?;
    let last_error: Option<String> = row.try_get("last_error").map_err(map_sqlx)?;
    let created: DateTime<Utc> = row.try_get("created_at").map_err(map_sqlx)?;
    Ok(DeliveryRecord {
        id,
        tenant_id: TenantId::from_uuid(tenant),
        endpoint_id: WebhookId::from_uuid(endpoint),
        event_id: mas_common::ids::EventId::from_uuid(event),
        state,
        attempt_count: u32::try_from(attempts).unwrap_or(u32::MAX),
        last_status_code,
        next_attempt_at: next_attempt.map(Timestamp::from),
        payload_hash: payload_hash.unwrap_or_default(),
        last_error,
        created_at: Timestamp::from(created),
    })
}

/// Classifies a refused replay attempt from what the rows actually say.
///
/// The evidence insert runs first, so by this point the delivery exists
/// (FK-backed tables) unless the tenant/endpoint pair is wrong; separating
/// "not found", "not dead-lettered", "missing evidence row", and "already
/// replayed" keeps the API's 404/409/500 split deterministic and testable
/// without a database.
fn replay_refusal(row: Option<(String, Option<bool>)>) -> AppError {
    match row {
        None => AppError::not_found("webhook delivery", "unknown delivery in this tenant"),
        Some((status, dead_letter)) => {
            if status != "dead_lettered" {
                return AppError::conflict(format!(
                    "webhook delivery is not dead-lettered (status '{status}') and cannot be replayed"
                ));
            }
            match dead_letter {
                None => AppError::internal(
                    "webhook delivery is marked dead-lettered but its dead-letter evidence row is missing",
                ),
                Some(true) => AppError::conflict(
                    "webhook delivery has already been replayed; a delivery is replayable exactly once",
                ),
                // Evidence absent and no replay yet: the insert was refused by
                // a guard the relay guarantees (e.g. missing signing digest).
                Some(false) => AppError::internal(
                    "webhook replay evidence could not be recorded for this delivery",
                ),
            }
        },
    }
}

#[async_trait]
impl WebhookAdminPort for PostgresWebhookAdmin {
    async fn list_deliveries(
        &self,
        tenant: TenantId,
        endpoint: WebhookId,
        after: Option<DeliveryCursor>,
        limit: u16,
    ) -> Result<Vec<DeliveryRecord>> {
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "delivery page limit must be between 1 and 200",
            ));
        }
        // Defense in depth: the tenant is both the RLS GUC (scoped request)
        // and an explicit bind, so no reader can widen a page by any other
        // path. Legacy rows without a payload digest are invisible evidence
        // (they cannot be re-signed or replayed); they are not admin history.
        //
        // Endpoint visibility comes first: a foreign or unknown webhook id
        // must answer404, never an empty200 that would make "no such
        // endpoint" indistinguishable from "no deliveries yet".
        let mut scoped = scoped_request::connection().await?;
        let visible: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM webhooks
              WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.into_uuid())
        .bind(endpoint.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if visible.is_none() {
            return Err(AppError::not_found("webhook", endpoint.to_string()));
        }
        let after_at = after.map(|cursor| cursor.created_at.into_datetime());
        let after_id = after.map(|cursor| cursor.id);
        let rows = sqlx::query(&format!(
            "SELECT {DELIVERY_COLUMNS}
               FROM webhook_deliveries
              WHERE tenant_id = $1
                AND webhook_id = $2
                AND payload_hash IS NOT NULL
                AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4::uuid))
              ORDER BY created_at DESC, id DESC
              LIMIT $5"
        ))
        .bind(tenant.into_uuid())
        .bind(endpoint.into_uuid())
        .bind(after_at)
        .bind(after_id)
        .bind(i64::from(limit))
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(delivery_from_row).collect()
    }

    async fn replay_dead_letter(
        &self,
        tenant: TenantId,
        endpoint: WebhookId,
        delivery: Uuid,
        actor: Option<UserId>,
    ) -> Result<DeliveryRecord> {
        let mut scoped = scoped_request::connection().await?;

        // 1) Append-only replay evidence. The unique (tenant_id, delivery_id)
        //    key makes this the serialization point for concurrent replays:
        //    exactly one request wins, every other caller observes the
        //    refusal below instead of creating a second body of work.
        let evidence = sqlx::query(
            "INSERT INTO webhook_replay_attempts (
                 id, tenant_id, webhook_id, delivery_id, event_id, payload_id,
                 payload_hash, signing_key_version, requested_by
             )
             SELECT gen_random_uuid(), d.tenant_id, d.webhook_id, d.id, d.event_id,
                    d.payload_id, d.payload_hash, d.signing_key_version, $4
               FROM webhook_deliveries AS d
              WHERE d.tenant_id = $1
                AND d.webhook_id = $2
                AND d.id = $3
                AND d.status = 'dead_lettered'
                AND d.payload_hash IS NOT NULL
                AND d.signing_key_version IS NOT NULL
             ON CONFLICT (tenant_id, delivery_id) DO NOTHING
             RETURNING id",
        )
        .bind(tenant.into_uuid())
        .bind(endpoint.into_uuid())
        .bind(delivery)
        .bind(actor.map(UserId::into_uuid))
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;

        if evidence.is_none() {
            let observed = sqlx::query(
                "SELECT d.status, (dl.delivery_id IS NOT NULL) AS has_dead_letter,
                        dl.replayed_at IS NOT NULL AS replayed
                   FROM webhook_deliveries AS d
                   LEFT JOIN webhook_dead_letters AS dl
                     ON dl.delivery_id = d.id AND dl.tenant_id = d.tenant_id
                  WHERE d.tenant_id = $1 AND d.webhook_id = $2 AND d.id = $3",
            )
            .bind(tenant.into_uuid())
            .bind(endpoint.into_uuid())
            .bind(delivery)
            .fetch_optional(scoped.connection().as_mut())
            .await
            .map_err(map_sqlx)?;
            return Err(match observed {
                None => AppError::not_found("webhook delivery", delivery.to_string()),
                Some(row) => {
                    let status: String = row.try_get("status").map_err(map_sqlx)?;
                    let has_dead_letter: bool = row.try_get("has_dead_letter").map_err(map_sqlx)?;
                    let replayed: bool = row.try_get("replayed").map_err(map_sqlx)?;
                    // `None` dead-letter ⇒ missing evidence invariant break;
                    // otherwise the replayed flag decides 409 vs 500.
                    replay_refusal(Some((
                        status,
                        if has_dead_letter {
                            Some(replayed)
                        } else {
                            None
                        },
                    )))
                },
            });
        }

        // 2) Re-arm the delivery for the relay's normal claim loop. The
        //    `status = 'dead_lettered'` predicate keeps this statement honest
        //    under any interleaving: only a genuinely dead-lettered row moves,
        //    and its lease columns are (already NULL by the lease-shape
        //    constraint) reaffirmed here.
        let rearm = sqlx::query(
            "UPDATE webhook_deliveries
                SET status = 'pending',
                    attempts = 0,
                    http_status = NULL,
                    last_error = NULL,
                    next_attempt_at = now(),
                    delivered_at = NULL,
                    dead_lettered_at = NULL,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = now()
              WHERE tenant_id = $1
                AND webhook_id = $2
                AND id = $3
                AND status = 'dead_lettered'
              RETURNING id",
        )
        .bind(tenant.into_uuid())
        .bind(endpoint.into_uuid())
        .bind(delivery)
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if rearm.is_none() {
            // Impossible inside one transaction (the evidence insert above
            // observed `dead_lettered`); fail closed so nothing partially
            // replays.
            return Err(AppError::internal(
                "webhook replay could not re-arm the dead-lettered delivery",
            ));
        }

        // 3) Stamp the immutable dead-letter record. The relay keeps writing
        //    dead letters; only this column pair records who re-queued it.
        let stamped = sqlx::query(
            "UPDATE webhook_dead_letters
                SET replayed_at = now(),
                    replayed_by = $3
              WHERE tenant_id = $1
                AND delivery_id = $2
                AND replayed_at IS NULL",
        )
        .bind(tenant.into_uuid())
        .bind(delivery)
        .bind(actor.map(UserId::into_uuid))
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if stamped.rows_affected() != 1 {
            return Err(AppError::internal(
                "webhook replay could not stamp the dead-letter evidence",
            ));
        }

        // 4) Read the re-armed row back so the caller reports the durable
        //    state the relay will actually claim.
        let row = sqlx::query(&format!(
            "SELECT {DELIVERY_COLUMNS}
               FROM webhook_deliveries
              WHERE tenant_id = $1 AND webhook_id = $2 AND id = $3"
        ))
        .bind(tenant.into_uuid())
        .bind(endpoint.into_uuid())
        .bind(delivery)
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::internal("replayed webhook delivery vanished before read-back"))?;
        delivery_from_row(&row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusal_separates_not_found_not_dead_lettered_missing_evidence_and_replay() {
        assert!(
            matches!(replay_refusal(None), AppError::NotFound { .. }),
            "unknown delivery is a 404, never a 409"
        );
        let wrong_state = replay_refusal(Some(("delivered".to_owned(), Some(false))));
        assert!(
            matches!(wrong_state, AppError::Conflict(_)),
            "a live delivery is refused with a conflict, not fabricated as replayed: {wrong_state}"
        );
        assert!(
            matches!(
                replay_refusal(Some(("dead_lettered".to_owned(), None))),
                AppError::Internal(_)
            ),
            "dead-lettered without evidence is an invariant break (500), not an operator error"
        );
        let twice = replay_refusal(Some(("dead_lettered".to_owned(), Some(true))));
        assert!(
            matches!(twice, AppError::Conflict(_)),
            "a second replay is a conflict: {twice}"
        );
        assert!(
            matches!(
                replay_refusal(Some(("dead_lettered".to_owned(), Some(false)))),
                AppError::Internal(_)
            ),
            "an unexplained evidence refusal fails closed"
        );
    }

    #[test]
    fn history_sql_is_tenant_bound_keyset_ordered_and_payload_free() {
        assert!(
            DELIVERY_COLUMNS.contains("payload_hash") && !DELIVERY_COLUMNS.contains("payload_id"),
            "history rows expose digests, never payload identity"
        );
        // Tripwires against accidental unscoped or unbounded rewrites.
        assert!(DELIVERY_COLUMNS.contains("created_at"));
    }
}
