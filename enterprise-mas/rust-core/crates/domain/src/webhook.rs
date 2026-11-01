//! Webhook endpoints and durable delivery aggregates.

use mas_common::constants;
use mas_common::error::AppError;
use mas_common::ids::{EventId, SecretReferenceId, TenantId, WebhookId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::notification::DeliveryState;
use crate::value_objects::SafeUrl;

string_enum! {
    /// Signature algorithm for outbound webhook payloads.
    SignatureAlgorithm {
        HmacSha256 => "hmac_sha256",
    }
}

/// Signature configuration (metadata only; the secret itself is referenced).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureConfig {
    pub algorithm: SignatureAlgorithm,
    /// Reference to the shared signing secret.
    pub secret_reference_id: SecretReferenceId,
    /// Current key version used for newly enqueued deliveries. Existing jobs
    /// capture this version so rotation cannot change a retry's signature
    /// unexpectedly midway through a delivery stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_version: Option<String>,
}

/// Delivery retry behavior of an endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookRetryPolicy {
    pub max_attempts: u32,
    /// First backoff in milliseconds; doubles per attempt (capped at 5 min).
    pub initial_backoff_ms: u64,
}

impl Default for WebhookRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: constants::DEFAULT_MAX_RETRIES + 1,
            initial_backoff_ms: constants::DEFAULT_RETRY_BACKOFF_MS,
        }
    }
}

/// A registered outbound webhook endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookEndpoint {
    pub id: WebhookId,
    pub tenant_id: TenantId,
    /// Tenant-influenced endpoint URLs are always parsed with `SafeUrl::parse`,
    /// never the operator-only internal endpoint parser.
    pub url: SafeUrl,
    pub description: Option<String>,
    /// Event type names (e.g. `execution.completed`) this endpoint receives.
    pub subscribed_events: BTreeSet<String>,
    pub signature: SignatureConfig,
    pub enabled: bool,
    pub retry_policy: WebhookRetryPolicy,
    /// Statistics, updated by the delivery pipeline.
    #[serde(default)]
    pub consecutive_failures: u32,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl WebhookEndpoint {
    /// Registers a tenant-owned outbound endpoint after its URL has passed the
    /// tenant-safe `SafeUrl` parser and its secret has been created elsewhere.
    pub fn register(
        tenant_id: TenantId,
        url: SafeUrl,
        subscribed_events: BTreeSet<String>,
        signature: SignatureConfig,
    ) -> Result<Self> {
        if url.is_internal_endpoint() {
            return Err(AppError::invalid_field(
                "url",
                "internal_endpoint_forbidden",
                "tenant webhook endpoints cannot target operator-only internal endpoints",
            ));
        }
        if signature.key_version.as_deref().is_some_and(|version| {
            version.is_empty()
                || version.len() > 128
                || !version
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        }) {
            return Err(AppError::invalid_field(
                "signature.key_version",
                "invalid_format",
                "signing key versions must be 1..=128 ASCII token characters",
            ));
        }
        if subscribed_events.is_empty() {
            return Err(AppError::invalid_field(
                "subscribed_events",
                "required",
                "a webhook must subscribe to at least one event type",
            ));
        }
        for event in &subscribed_events {
            if event.is_empty()
                || event.len() > 128
                || !event.chars().all(|ch| {
                    ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | ':' | '*')
                })
            {
                return Err(AppError::invalid_field(
                    "subscribed_events",
                    "invalid_format",
                    format!("invalid event type name {event:?}"),
                ));
            }
        }
        let now = Timestamp::now();
        Ok(Self {
            id: WebhookId::new(),
            tenant_id,
            url,
            description: None,
            subscribed_events,
            signature,
            enabled: true,
            retry_policy: WebhookRetryPolicy::default(),
            consecutive_failures: 0,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether an event of `event_type` should be delivered here.
    #[must_use]
    pub fn subscribes_to(&self, event_type: &str) -> bool {
        self.enabled
            && (self.subscribed_events.contains("*") || self.subscribed_events.contains(event_type))
    }

    pub fn disable(&mut self) {
        self.enabled = false;
        self.updated_at = Timestamp::now();
    }

    pub fn enable(&mut self) {
        self.enabled = true;
        self.updated_at = Timestamp::now();
    }

    /// Circuit breaker: auto-disable after 25 consecutive failures.
    pub fn record_outcome(&mut self, delivered: bool) {
        if delivered {
            self.consecutive_failures = 0;
        } else {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            if self.consecutive_failures >= 25 {
                self.enabled = false;
            }
        }
        self.updated_at = Timestamp::now();
    }
}

/// One durable delivery stream of an event to an endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDelivery {
    pub id: uuid::Uuid,
    pub endpoint_id: WebhookId,
    pub event_id: EventId,
    pub state: DeliveryState,
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_status_code: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<Timestamp>,
    /// SHA-256 (hex) of the signed payload body, for replay integrity.
    pub payload_hash: String,
    /// Version captured at enqueue. `None` means the secret manager's current
    /// version only for legacy data; new delivery creation captures explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_key_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl WebhookDelivery {
    /// Creates a delivery without a key version for backwards-compatible
    /// callers. New production enqueue paths use [`Self::new_with_key_version`].
    pub fn new(
        endpoint_id: WebhookId,
        event_id: EventId,
        payload_hash: impl Into<String>,
    ) -> Result<Self> {
        Self::new_with_key_version(endpoint_id, event_id, payload_hash, None)
    }

    /// Creates a delivery and captures the endpoint's signing key version.
    pub fn new_with_key_version(
        endpoint_id: WebhookId,
        event_id: EventId,
        payload_hash: impl Into<String>,
        signing_key_version: Option<String>,
    ) -> Result<Self> {
        let payload_hash = payload_hash.into();
        if payload_hash.len() != 64 || !payload_hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
            return Err(AppError::invalid_field(
                "payload_hash",
                "invalid_format",
                "payload hash must be 64 hex characters (SHA-256)",
            ));
        }
        if signing_key_version.as_deref().is_some_and(|version| {
            version.is_empty()
                || version.len() > 128
                || !version
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        }) {
            return Err(AppError::invalid_field(
                "signing_key_version",
                "invalid_format",
                "signing key versions must be 1..=128 ASCII token characters",
            ));
        }
        let now = Timestamp::now();
        Ok(Self {
            id: uuid::Uuid::now_v7(),
            endpoint_id,
            event_id,
            state: DeliveryState::Pending,
            attempt_count: 0,
            last_status_code: None,
            next_attempt_at: None,
            payload_hash,
            signing_key_version,
            last_error: None,
            created_at: now,
            updated_at: now,
        })
    }

    /// Whether another attempt is allowed under `policy`.
    #[must_use]
    pub fn should_retry(&self, policy: &WebhookRetryPolicy) -> bool {
        matches!(self.state, DeliveryState::Failed | DeliveryState::Pending)
            && self.attempt_count < policy.max_attempts
    }

    /// Records one completed HTTP attempt and updates retry/dead-letter state.
    pub fn record_attempt(
        &mut self,
        status_code: Option<u16>,
        policy: &WebhookRetryPolicy,
        error: Option<String>,
    ) {
        self.attempt_count = self.attempt_count.saturating_add(1);
        self.last_status_code = status_code;
        let delivered = status_code.is_some_and(|code| (200..300).contains(&code));
        if delivered {
            self.state = DeliveryState::Delivered;
            self.last_error = None;
            self.next_attempt_at = None;
        } else if self.attempt_count < policy.max_attempts {
            self.state = DeliveryState::Failed;
            let mut message = error.unwrap_or_else(|| {
                format!(
                    "endpoint returned {}",
                    status_code.map_or("no response".to_owned(), |code| code.to_string())
                )
            });
            message.truncate(1024);
            self.last_error = Some(message);
            // Exponential backoff: initial * 2^(attempt-1), capped at 5 min.
            let factor = 1_u64 << (self.attempt_count.saturating_sub(1).min(6));
            let backoff = policy
                .initial_backoff_ms
                .saturating_mul(factor)
                .min(300_000);
            self.next_attempt_at =
                Timestamp::now().checked_add(std::time::Duration::from_millis(backoff));
        } else {
            self.state = DeliveryState::DeadLettered;
            self.next_attempt_at = None;
            self.last_error = Some("delivery attempts exhausted".to_owned());
        }
        self.updated_at = Timestamp::now();
    }

    /// Marks work cancelled only before a terminal outcome. Worker ownership
    /// fencing is enforced by persistence; this aggregate enforces lifecycle.
    pub fn cancel(&mut self, reason: impl Into<String>) -> Result<()> {
        if self.state.is_terminal() {
            return Err(AppError::conflict(
                "terminal webhook deliveries cannot be cancelled",
            ));
        }
        let mut reason = reason.into();
        reason.truncate(1024);
        self.state = DeliveryState::Cancelled;
        self.next_attempt_at = None;
        self.last_error = Some(reason);
        self.updated_at = Timestamp::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash() -> String {
        "a".repeat(64)
    }

    #[test]
    fn retries_end_in_explicit_dead_letter_state() {
        let endpoint = WebhookId::new();
        let event = EventId::new();
        let mut delivery =
            WebhookDelivery::new_with_key_version(endpoint, event, hash(), Some("v2".to_owned()))
                .expect("delivery");
        delivery.record_attempt(
            Some(500),
            &WebhookRetryPolicy {
                max_attempts: 1,
                initial_backoff_ms: 1,
            },
            None,
        );
        assert_eq!(delivery.state, DeliveryState::DeadLettered);
        assert!(delivery.next_attempt_at.is_none());
        assert_eq!(delivery.signing_key_version.as_deref(), Some("v2"));
    }
}
