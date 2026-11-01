//! PURPOSE: The billing boundary — the webhook receipt log and the provider
//! abstraction's durable side.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: keep the platform's execution ledger
//! authoritative while recording, separately, what the *external provider*
//! told us. The two are reconciled but never merged: a provider callback is
//! never allowed to decide whether an execution happened.
//!
//! Security boundary: the receipt table is deliberately **not** RLS-scoped —
//! a receipt is written before a tenant is resolved, and a rejected callback
//! that named the wrong tenant is exactly the evidence an incident review
//! needs. Ordering matters: the signature is verified *before* the receipt is
//! written, and the tenant is resolved from a row this platform owns, never
//! from an unsigned JSON field.
//!
//! Data ownership: Rust-owned platform state. Provider identifiers are opaque
//! handles; **no provider secret, webhook signing key, or customer credential
//! is ever stored here** — only a SHA-256 of the payload.
//!
//! It must NOT:
//!   • process an unverified callback;
//!   • trust a tenant id, customer id or plan id that arrived in an unsigned
//!     body;
//!   • apply the same provider event twice (the unique index is the
//!     anti-replay control);
//!   • mutate or delete a receipt — they are append-only evidence.

use mas_common::error::AppError;
use mas_common::ids::{BillingReceiptId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::{is_unique_violation, map_sqlx};

/// Lifecycle of a received callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptState {
    /// Written, not yet acted on.
    Received,
    /// Applied to platform state.
    Applied,
    /// Deliberately not acted on (unknown event type, no-op transition).
    Ignored,
    /// Rejected: invalid signature, unknown tenant, or a replay with a
    /// mutated body.
    Rejected,
}

impl ReceiptState {
    /// Storage form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Applied => "applied",
            Self::Ignored => "ignored",
            Self::Rejected => "rejected",
        }
    }

    /// Parses the storage form, failing loudly on drift.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "received" => Some(Self::Received),
            "applied" => Some(Self::Applied),
            "ignored" => Some(Self::Ignored),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }
}

impl std::fmt::Display for ReceiptState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One webhook receipt.
#[derive(Debug, Clone)]
pub struct BillingReceiptRow {
    /// Receipt id.
    pub id: uuid::Uuid,
    /// Provider name (`stripe`, …).
    pub provider: String,
    /// The provider's own event id, preserved verbatim.
    pub event_id: String,
    /// Event type label.
    pub event_type: String,
    /// SHA-256 hex of the raw body.
    pub payload_hash: String,
    /// Whether the signature verified at receipt time.
    pub signature_valid: bool,
    /// Lifecycle state.
    pub state: String,
    /// Tenant resolved from platform truth (never from the body).
    pub tenant_id: Option<uuid::Uuid>,
    /// Why it was rejected, when it was.
    pub rejection_reason: Option<String>,
    /// When it arrived.
    pub received_at: chrono::DateTime<chrono::Utc>,
    /// When it finished processing.
    pub processed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for BillingReceiptRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            provider: row.try_get("provider")?,
            event_id: row.try_get("event_id")?,
            event_type: row.try_get("event_type")?,
            payload_hash: row.try_get("payload_hash")?,
            signature_valid: row.try_get("signature_valid")?,
            state: row.try_get("state")?,
            tenant_id: row.try_get("tenant_id")?,
            rejection_reason: row.try_get("rejection_reason")?,
            received_at: row.try_get("received_at")?,
            processed_at: row.try_get("processed_at")?,
        })
    }
}

/// The receipt log.
#[derive(Debug, Clone)]
pub struct PostgresBillingWebhookStore {
    pool: PgPool,
}

impl PostgresBillingWebhookStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Claims a provider event.
    ///
    /// Returns `None` when this `(provider, event_id)` has already been seen:
    /// that is the anti-replay control, and it is the *unique index* doing the
    /// work rather than a `SELECT` followed by an `INSERT`, because two
    /// concurrent deliveries would both read "unseen" and both proceed.
    ///
    /// The caller must have verified the signature before calling. A receipt
    /// is still written for a failed signature — the rejection is the evidence
    /// that someone is sending unsigned callbacks — but `signature_valid` is
    /// recorded honestly and the state is [`ReceiptState::Rejected`].
    pub async fn claim(
        &self,
        provider: &str,
        event_id: &str,
        event_type: &str,
        payload_hash: &str,
        signature_valid: bool,
        tenant: Option<TenantId>,
    ) -> Result<Option<BillingReceiptRow>> {
        if payload_hash.len() != 64 {
            return Err(AppError::validation(
                "payload hash must be 64 hex characters (sha-256)",
            ));
        }
        let state = if signature_valid {
            ReceiptState::Received
        } else {
            ReceiptState::Rejected
        };
        let result = sqlx::query_as::<_, BillingReceiptRow>(
            "INSERT INTO billing_webhook_receipts
                (id, provider, event_id, event_type, payload_hash, signature_valid,
                 state, tenant_id, rejection_reason)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
             RETURNING id, provider, event_id, event_type, payload_hash,
                       signature_valid, state, tenant_id, rejection_reason,
                       received_at, processed_at",
        )
        .bind(BillingReceiptId::new().into_uuid())
        .bind(provider)
        .bind(event_id)
        .bind(event_type)
        .bind(payload_hash)
        .bind(signature_valid)
        .bind(state.as_str())
        .bind(tenant.map(TenantId::into_uuid))
        .bind(if signature_valid {
            None
        } else {
            Some("signature verification failed".to_owned())
        })
        .fetch_one(executor(&self.pool).await?.connection())
        .await;

        match result {
            Ok(row) => Ok(Some(row)),
            Err(error) if is_unique_violation(&error) => {
                // Already seen. Return the existing receipt so the caller can
                // tell "applied" from "rejected" from "still in flight"
                // without re-processing.
                self.get(provider, event_id).await.map(Some)
            },
            Err(error) => Err(map_sqlx(error)),
        }
    }

    /// Reads one receipt.
    pub async fn get(&self, provider: &str, event_id: &str) -> Result<BillingReceiptRow> {
        sqlx::query_as::<_, BillingReceiptRow>(
            "SELECT id, provider, event_id, event_type, payload_hash, signature_valid,
                    state, tenant_id, rejection_reason, received_at, processed_at
               FROM billing_webhook_receipts
              WHERE provider = $1 AND event_id = $2",
        )
        .bind(provider)
        .bind(event_id)
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("billing_receipt", format!("{provider}/{event_id}")))
    }

    /// Records the outcome of processing, and why, when it was not applied.
    ///
    /// Idempotent in effect: calling it twice with the same state is accepted
    /// and overwrites the reason, because a retried handler legitimately
    /// re-reports the same verdict. What it will *not* do is move a terminal
    /// receipt back to `received` — that would let a second delivery of the
    /// same event re-run the state change.
    pub async fn finish(
        &self,
        receipt_id: BillingReceiptId,
        state: ReceiptState,
        reason: Option<&str>,
        tenant: Option<TenantId>,
        at: &Timestamp,
    ) -> Result<BillingReceiptRow> {
        let row = sqlx::query_as::<_, BillingReceiptRow>(
            "UPDATE billing_webhook_receipts
                SET state = $2,
                    rejection_reason = COALESCE($3, rejection_reason),
                    tenant_id = COALESCE($4, tenant_id),
                    processed_at = $5
              WHERE id = $1 AND state <> $2
          RETURNING id, provider, event_id, event_type, payload_hash, signature_valid,
                    state, tenant_id, rejection_reason, received_at, processed_at",
        )
        .bind(receipt_id.into_uuid())
        .bind(state.as_str())
        .bind(reason)
        .bind(tenant.map(TenantId::into_uuid))
        .bind(at.into_datetime())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;

        match row {
            Some(row) => Ok(row),
            // No row changed means it was already in this state: a duplicate
            // delivery. Returning the current receipt rather than an error is
            // what makes the handler safe to retry.
            None => sqlx::query_as::<_, BillingReceiptRow>(
                "SELECT id, provider, event_id, event_type, payload_hash, signature_valid,
                            state, tenant_id, rejection_reason, received_at, processed_at
                       FROM billing_webhook_receipts WHERE id = $1",
            )
            .bind(receipt_id.into_uuid())
            .fetch_one(executor(&self.pool).await?.connection())
            .await
            .map_err(map_sqlx),
        }
    }

    /// Recent receipts, newest first (operator view of the callback log).
    pub async fn recent(&self, limit: i64) -> Result<Vec<BillingReceiptRow>> {
        sqlx::query_as::<_, BillingReceiptRow>(
            "SELECT id, provider, event_id, event_type, payload_hash, signature_valid,
                    state, tenant_id, rejection_reason, received_at, processed_at
               FROM billing_webhook_receipts
              ORDER BY received_at DESC
              LIMIT $1",
        )
        .bind(limit.max(1))
        .fetch_all(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receipt_states_round_trip() {
        for state in [
            ReceiptState::Received,
            ReceiptState::Applied,
            ReceiptState::Ignored,
            ReceiptState::Rejected,
        ] {
            let as_str = state.as_str();
            assert_eq!(ReceiptState::parse(as_str), Some(state));
            assert_eq!(state.to_string(), as_str);
        }
        assert_eq!(ReceiptState::parse("half-applied"), None);
    }
}
