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
//!   • treat a receipt as an exclusive processing lease or as proof that
//!     separately committed business effects were applied atomically.

use mas_common::error::AppError;
use mas_common::ids::{BillingReceiptId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use sqlx::PgPool;
use sqlx::Row;

use super::executor::executor;
use crate::error::map_sqlx;

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

/// Whether this delivery created the unique receipt or matched an existing one.
/// A duplicate is not a grant to apply the event again; processing needs its
/// own transactional claim/recovery protocol.
#[derive(Debug, Clone)]
pub enum BillingClaim {
    /// This delivery inserted the receipt.
    New(BillingReceiptRow),
    /// A previous verified delivery already inserted the identical receipt.
    Existing(BillingReceiptRow),
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

    /// Claims a Stripe event authenticated by the Stripe-specific verifier.
    ///
    /// A receipt is not a processing lease or an atomic billing effect; a
    /// future production ingress must provide those separately.
    pub async fn claim_verified_stripe(
        &self,
        event: &mas_integrations::stripe_webhooks::VerifiedStripeEvent,
    ) -> Result<BillingClaim> {
        self.claim(
            "stripe",
            event.id(),
            event.event_type(),
            event.payload_hash(),
            true,
            None,
        )
        .await
    }

    /// Claims an authenticated provider event without poisoning its id on
    /// invalid signatures. A duplicate is distinguished from a new insert and
    /// must match the original event type and raw-body hash.
    ///
    /// A receipt alone is not an exclusive processing lease. Callers must
    /// atomically record business effects and use a crash-recovery protocol.
    pub async fn claim(
        &self,
        provider: &str,
        event_id: &str,
        event_type: &str,
        payload_hash: &str,
        signature_valid: bool,
        tenant: Option<TenantId>,
    ) -> Result<BillingClaim> {
        if !signature_valid {
            return Err(AppError::unauthorized(
                "billing callback signature was not verified",
            ));
        }
        if tenant.is_some_and(|id| id.is_nil()) {
            return Err(AppError::validation(
                "billing receipt tenant must be concrete",
            ));
        }
        for (field, value, max) in [
            ("provider", provider, 64),
            ("event_id", event_id, 256),
            ("event_type", event_type, 256),
        ] {
            if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
                return Err(AppError::invalid_field(
                    field,
                    "invalid_format",
                    "invalid billing event identifier",
                ));
            }
        }
        if payload_hash.len() != 64
            || !payload_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(AppError::validation(
                "payload hash must be lowercase SHA-256 hex",
            ));
        }
        // ON CONFLICT avoids aborting the surrounding request transaction,
        // unlike catching a PostgreSQL unique-violation after the fact.
        let row = sqlx::query_as::<_, BillingReceiptRow>(
            "INSERT INTO billing_webhook_receipts
                (id, provider, event_id, event_type, payload_hash, signature_valid,
                 state, tenant_id, rejection_reason)
             VALUES ($1,$2,$3,$4,$5,true,'received',$6,NULL)
             ON CONFLICT (provider, event_id) DO NOTHING
             RETURNING id, provider, event_id, event_type, payload_hash,
                       signature_valid, state, tenant_id, rejection_reason,
                       received_at, processed_at",
        )
        .bind(BillingReceiptId::new().into_uuid())
        .bind(provider)
        .bind(event_id)
        .bind(event_type)
        .bind(payload_hash)
        .bind(tenant.map(TenantId::into_uuid))
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?;
        if let Some(row) = row {
            return Ok(BillingClaim::New(row));
        }
        let existing = self.get(provider, event_id).await?;
        if !existing.signature_valid
            || existing.event_type != event_type
            || existing.payload_hash != payload_hash
            || tenant.is_some_and(|id| existing.tenant_id != Some(id.into_uuid()))
        {
            return Err(AppError::conflict(
                "billing event id conflicts with an existing receipt",
            ));
        }
        Ok(BillingClaim::Existing(existing))
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
    /// A terminal verdict can be retried unchanged, but never changed to a
    /// different verdict or tenant. Receipt finalization alone does not make
    /// separately performed business effects atomic.
    pub async fn finish(
        &self,
        receipt_id: BillingReceiptId,
        state: ReceiptState,
        reason: Option<&str>,
        tenant: Option<TenantId>,
        at: &Timestamp,
    ) -> Result<BillingReceiptRow> {
        if state == ReceiptState::Received {
            return Err(AppError::validation(
                "received is not a terminal billing receipt state",
            ));
        }
        if tenant.is_some_and(|id| id.is_nil()) {
            return Err(AppError::validation(
                "billing receipt tenant must be concrete",
            ));
        }
        let row = sqlx::query_as::<_, BillingReceiptRow>(
            "UPDATE billing_webhook_receipts
                SET state = $2,
                    rejection_reason = COALESCE($3, rejection_reason),
                    tenant_id = COALESCE($4, tenant_id),
                    processed_at = $5
              WHERE id = $1 AND state = 'received' AND signature_valid
                AND (tenant_id IS NULL OR $4::uuid IS NULL OR tenant_id = $4::uuid)
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

        if let Some(row) = row {
            return Ok(row);
        }
        let existing = sqlx::query_as::<_, BillingReceiptRow>(
            "SELECT id, provider, event_id, event_type, payload_hash, signature_valid,
                    state, tenant_id, rejection_reason, received_at, processed_at
               FROM billing_webhook_receipts WHERE id = $1",
        )
        .bind(receipt_id.into_uuid())
        .fetch_optional(executor(&self.pool).await?.connection())
        .await
        .map_err(map_sqlx)?
        .ok_or_else(|| AppError::not_found("billing_receipt", receipt_id.to_string()))?;
        if existing.signature_valid
            && existing.state == state.as_str()
            && tenant.is_none_or(|id| existing.tenant_id == Some(id.into_uuid()))
            && reason.is_none_or(|value| existing.rejection_reason.as_deref() == Some(value))
        {
            Ok(existing)
        } else {
            Err(AppError::conflict(
                "billing receipt is already finalized or has a different owner",
            ))
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
