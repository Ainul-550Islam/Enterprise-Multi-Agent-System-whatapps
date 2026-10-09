//! PURPOSE: Durable, lease-fenced outbound webhook worker for the canonical
//! integration plane.
//!
//! Owning layer: `mas-integrations` application/infrastructure boundary.
//!
//! Production role: claim due webhook deliveries from a durable store, resolve
//! only the captured signing-secret version at send time, dispatch through the
//! canonical guarded HTTP transport, and settle delivery/endpoint state only
//! while the worker still owns the lease fence. It applies bounded jittered
//! retries, honours bounded Retry-After hints, and retains exhausted jobs as
//! explicit dead letters.
//!
//! Security boundary: it never accepts a caller URL, secret bytes, tenant id,
//! or delivery state as authority. URLs and endpoints arrive from the durable
//! store, secret material arrives from `WebhookSecretPort`, and every socket
//! opens beneath `GuardedHttpClient`. A stale worker may send at-least-once
//! after a crash, but cannot overwrite a newer worker's durable settlement.
//!
//! Data ownership: the store owns endpoint, payload, delivery, lease, and
//! fencing rows. This worker owns no durable state and reconstructs every
//! action from a claimed row after restart.
//!
//! Prohibited responsibilities: request authentication/authorization, webhook
//! registration, raw secret persistence, inbound verification, domain event
//! production, or a direct/unprotected HTTP path.

use std::time::Duration;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::notification::DeliveryState;
use mas_domain::webhook::{WebhookDelivery, WebhookEndpoint, WebhookRetryPolicy};
use sha2::{Digest as _, Sha256};

use crate::http_guard::{GuardedHttpClient, HttpClientPort, HttpRequest};
use crate::webhooks::{
    sign_header_with_key_version, StoredEvent, WebhookSecretPort, DELIVERY_ID_HEADER,
    EVENT_ID_HEADER, EVENT_TYPE_HEADER, MAX_RETRY_AFTER_SECS, SIGNATURE_HEADER,
};

/// A delivery claimed under a durable lease/fence.
#[derive(Debug, Clone)]
pub struct ClaimedWebhookDelivery {
    /// Current delivery aggregate snapshot.
    pub delivery: WebhookDelivery,
    /// Endpoint snapshot joined by the claim statement.
    pub endpoint: WebhookEndpoint,
    /// Exact payload retained with the durable event.
    pub event: StoredEvent,
    /// Monotonic fence incremented each time ownership changes.
    pub fence: i64,
}

/// Application-side atomic fanout port.
///
/// Implementations persist the exact payload bytes once and create one
/// idempotent delivery per matching endpoint in the same transaction. The
/// endpoint signing key version is captured at enqueue, never looked up again
/// from mutable endpoint configuration during a retry.
#[async_trait]
pub trait OutboundWebhookEnqueuePort: std::fmt::Debug + Send + Sync {
    /// Stores an event and fanouts it to enabled subscribed endpoints.
    /// Returns the number of newly created delivery rows; duplicate event ids
    /// converge to the original durable payload/deliveries rather than sending
    /// an event twice.
    async fn enqueue_event(
        &self,
        tenant_id: mas_common::ids::TenantId,
        event_id: mas_common::ids::EventId,
        event_type: &str,
        body: &[u8],
    ) -> Result<u64>;
}

/// Persistent delivery-lease port.
///
/// Every implementation must use one transactional claim statement such as
/// `FOR UPDATE SKIP LOCKED` plus an incremented fence. `settle_claim` must
/// predicate its update on both the delivery id and fence; a lease expiration
/// alone is not sufficient fencing because a slow stale worker could otherwise
/// overwrite a newer worker's outcome.
#[async_trait]
pub trait WebhookDeliveryLeasePort: std::fmt::Debug + Send + Sync {
    /// Claims at most `limit` due jobs for `worker` until `lease` expires.
    async fn claim_due(
        &self,
        worker: &str,
        now: &Timestamp,
        lease: Duration,
        limit: u32,
    ) -> Result<Vec<ClaimedWebhookDelivery>>;

    /// Commits endpoint + delivery state iff `claim.fence` still owns it.
    /// Returns `false` when ownership moved; callers must not retry settlement.
    async fn settle_claim(
        &self,
        claim: &ClaimedWebhookDelivery,
        delivery: &WebhookDelivery,
        endpoint: &WebhookEndpoint,
    ) -> Result<bool>;

    /// Releases an undecided claim iff the caller still owns its fence.
    async fn release_claim(&self, claim: &ClaimedWebhookDelivery) -> Result<bool>;
}

/// Low-cardinality outcomes from one worker pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DurableWebhookCycle {
    /// Claimed rows for which this worker attempted an HTTP delivery.
    pub attempted: u64,
    /// Successful 2xx deliveries.
    pub delivered: u64,
    /// Retryable outcomes rescheduled durably.
    pub requeued: u64,
    /// Explicit terminal dead letters.
    pub dead_lettered: u64,
    /// A worker that lost its lease/fence before settlement.
    pub stale: u64,
    /// Claims that could not be decided and were returned to the pool.
    pub released: u64,
}

impl DurableWebhookCycle {
    /// Rows claimed in this pass, including stale/released outcomes.
    #[must_use]
    pub const fn total(&self) -> u64 {
        self.attempted + self.stale + self.released
    }
}

/// Lease and retry limits for [`DurableWebhookWorker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableWebhookWorkerConfig {
    /// Unique process/replica identity persisted in lease rows.
    pub worker_id: String,
    /// Maximum jobs one pass claims.
    pub batch_size: u32,
    /// Lease duration; must exceed the outbound request timeout budget.
    pub lease: Duration,
}

impl Default for DurableWebhookWorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: "integration-worker".to_owned(),
            batch_size: 100,
            lease: Duration::from_secs(30),
        }
    }
}

impl DurableWebhookWorkerConfig {
    /// Validates operator-supplied worker configuration.
    pub fn validate(&self) -> Result<()> {
        if self.worker_id.trim().is_empty() || self.worker_id.len() > 128 {
            return Err(AppError::invalid_field(
                "worker_id",
                "invalid_format",
                "webhook worker id must be 1..=128 characters",
            ));
        }
        if !(1..=1_000).contains(&self.batch_size) {
            return Err(AppError::invalid_field(
                "batch_size",
                "out_of_range",
                "webhook worker batch size must be between 1 and 1000",
            ));
        }
        if self.lease < Duration::from_secs(1) || self.lease > Duration::from_secs(300) {
            return Err(AppError::invalid_field(
                "lease",
                "out_of_range",
                "webhook worker lease must be between 1 and 300 seconds",
            ));
        }
        Ok(())
    }
}

/// Production-safe outbound delivery worker.
#[derive(Debug)]
pub struct DurableWebhookWorker<S, K, C>
where
    S: WebhookDeliveryLeasePort,
    K: WebhookSecretPort,
    C: HttpClientPort,
{
    store: S,
    secrets: K,
    client: GuardedHttpClient<C>,
    config: DurableWebhookWorkerConfig,
}

impl<S, K, C> DurableWebhookWorker<S, K, C>
where
    S: WebhookDeliveryLeasePort,
    K: WebhookSecretPort,
    C: HttpClientPort,
{
    /// Creates a durable worker after validating its bounded lease settings.
    pub fn new(
        store: S,
        secrets: K,
        client: GuardedHttpClient<C>,
        config: DurableWebhookWorkerConfig,
    ) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            store,
            secrets,
            client,
            config,
        })
    }

    /// Runs one bounded durable claim/send/settle cycle.
    pub async fn run_once(&self, now: &Timestamp) -> Result<DurableWebhookCycle> {
        let claims = self
            .store
            .claim_due(
                &self.config.worker_id,
                now,
                self.config.lease,
                self.config.batch_size,
            )
            .await?;
        let mut cycle = DurableWebhookCycle::default();
        for claim in claims {
            self.deliver_one(claim, now, &mut cycle).await;
        }
        Ok(cycle)
    }

    /// Exposes the active guarded egress policy for composition assertions.
    #[must_use]
    pub const fn client(&self) -> &GuardedHttpClient<C> {
        &self.client
    }

    async fn deliver_one(
        &self,
        claim: ClaimedWebhookDelivery,
        now: &Timestamp,
        cycle: &mut DurableWebhookCycle,
    ) {
        let mut delivery = claim.delivery.clone();
        let mut endpoint = claim.endpoint.clone();

        // SQL metadata equality is not evidence that the bytes selected for
        // this attempt are the bytes that were originally enqueued. Hash the
        // fetched payload again before signing/sending so a corrupt join, row,
        // or storage mutation becomes a durable terminal failure rather than
        // an authenticated delivery of altered content.
        if !payload_hash_matches(&claim.event.body, &delivery.payload_hash) {
            cycle.attempted = cycle.attempted.saturating_add(1);
            self.record_failure(
                &claim,
                &mut delivery,
                &mut endpoint,
                now,
                None,
                None,
                false,
                Some("webhook payload integrity verification failed".to_owned()),
                cycle,
            )
            .await;
            return;
        }

        let secret = match self
            .secrets
            .resolve_version(
                endpoint.signature.secret_reference_id,
                delivery.signing_key_version.as_deref(),
            )
            .await
        {
            Ok(secret) => secret,
            Err(_) => {
                cycle.attempted = cycle.attempted.saturating_add(1);
                self.record_failure(
                    &claim,
                    &mut delivery,
                    &mut endpoint,
                    now,
                    None,
                    None,
                    true,
                    Some("webhook signing secret unavailable".to_owned()),
                    cycle,
                )
                .await;
                return;
            },
        };

        let signature = sign_header_with_key_version(
            now,
            &claim.event.body,
            secret.expose(),
            delivery.signing_key_version.as_deref(),
        );
        let request = HttpRequest::post(endpoint.url.clone(), claim.event.body.clone())
            .with_header("content-type", "application/json")
            .with_header(SIGNATURE_HEADER, &signature)
            .with_header(EVENT_TYPE_HEADER, &claim.event.event_type)
            .with_header(EVENT_ID_HEADER, &delivery.event_id.to_string())
            .with_header(DELIVERY_ID_HEADER, &delivery.id.to_string());

        cycle.attempted = cycle.attempted.saturating_add(1);
        match self.client.send(&request).await {
            Ok(response) if response.is_success() => {
                delivery.record_attempt(Some(response.status), &endpoint.retry_policy, None);
                endpoint.record_outcome(true);
                self.settle(&claim, &delivery, &endpoint, cycle, Outcome::Delivered)
                    .await;
            },
            Ok(response) => {
                let retryable = is_retryable_status(response.status);
                let retry_after = response.retry_after_seconds();
                self.record_failure(
                    &claim,
                    &mut delivery,
                    &mut endpoint,
                    now,
                    Some(response.status),
                    retry_after.filter(|_| matches!(response.status, 429 | 503)),
                    retryable,
                    None,
                    cycle,
                )
                .await;
            },
            Err(_) => {
                self.record_failure(
                    &claim,
                    &mut delivery,
                    &mut endpoint,
                    now,
                    None,
                    None,
                    true,
                    Some("webhook transport failed".to_owned()),
                    cycle,
                )
                .await;
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_failure(
        &self,
        claim: &ClaimedWebhookDelivery,
        delivery: &mut WebhookDelivery,
        endpoint: &mut WebhookEndpoint,
        now: &Timestamp,
        status: Option<u16>,
        retry_after_seconds: Option<u64>,
        retryable: bool,
        error: Option<String>,
        cycle: &mut DurableWebhookCycle,
    ) {
        if !retryable {
            // Drive the aggregate to its explicit dead-letter terminal state
            // without pretending a permanent 4xx is a transient retry.
            delivery.attempt_count = endpoint.retry_policy.max_attempts.saturating_sub(1);
        }
        delivery.record_attempt(status, &endpoint.retry_policy, error);
        if delivery.state == DeliveryState::Failed {
            delivery.next_attempt_at = now.checked_add(jittered_backoff(
                &endpoint.retry_policy,
                delivery.attempt_count,
                delivery.id,
            ));
            if let Some(retry_after_seconds) = retry_after_seconds {
                honor_retry_after(delivery, now, retry_after_seconds);
            }
        }
        endpoint.record_outcome(false);
        let outcome = if delivery.state == DeliveryState::DeadLettered {
            Outcome::DeadLettered
        } else {
            Outcome::Requeued
        };
        self.settle(claim, delivery, endpoint, cycle, outcome).await;
    }

    async fn settle(
        &self,
        claim: &ClaimedWebhookDelivery,
        delivery: &WebhookDelivery,
        endpoint: &WebhookEndpoint,
        cycle: &mut DurableWebhookCycle,
        outcome: Outcome,
    ) {
        match self.store.settle_claim(claim, delivery, endpoint).await {
            Ok(true) => match outcome {
                Outcome::Delivered => cycle.delivered = cycle.delivered.saturating_add(1),
                Outcome::Requeued => cycle.requeued = cycle.requeued.saturating_add(1),
                Outcome::DeadLettered => {
                    cycle.dead_lettered = cycle.dead_lettered.saturating_add(1)
                },
            },
            Ok(false) => {
                cycle.stale = cycle.stale.saturating_add(1);
                tracing::warn!(
                    outcome = "stale_fence",
                    "webhook delivery settlement lost ownership"
                );
            },
            Err(_) => {
                // The store has not confirmed a durable outcome. Releasing is
                // safer than holding a lease until crash expiry, but a release
                // failure means another replica may already own it.
                tracing::warn!(
                    outcome = "settlement_error",
                    "webhook delivery settlement failed"
                );
                match self.store.release_claim(claim).await {
                    Ok(true) => cycle.released = cycle.released.saturating_add(1),
                    Ok(false) | Err(_) => cycle.stale = cycle.stale.saturating_add(1),
                }
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Delivered,
    Requeued,
    DeadLettered,
}

/// Recomputes a payload digest over the actual bytes fetched for this lease.
/// A malformed/noncanonical stored digest is also a mismatch, never a reason
/// to relax integrity checks.
fn payload_hash_matches(body: &[u8], expected: &str) -> bool {
    expected.len() == 64
        && expected
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && hex::encode(Sha256::digest(body)) == expected
}

/// 408/425/429 and server failures are plausibly transient. All other 4xx
/// outcomes are dead-lettered immediately so a malformed payload cannot burn
/// a worker's retry budget indefinitely.
fn is_retryable_status(status: u16) -> bool {
    matches!(status, 408 | 425 | 429) || status >= 500
}

/// Computes deterministic full jitter without keeping process-local RNG state.
///
/// The value is derived from a delivery UUID and attempt number, so a retry
/// after a process restart has the same schedule while different deliveries do
/// not synchronize into a thundering herd.
fn jittered_backoff(
    policy: &WebhookRetryPolicy,
    attempt: u32,
    delivery_id: uuid::Uuid,
) -> Duration {
    let factor = 1_u64 << attempt.saturating_sub(1).min(6);
    let ceiling_ms = policy
        .initial_backoff_ms
        .saturating_mul(factor)
        .clamp(1, 300_000);
    let mut hasher = Sha256::new();
    hasher.update(delivery_id.as_bytes());
    hasher.update(attempt.to_be_bytes());
    let digest = hasher.finalize();
    let entropy = u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("first eight SHA-256 bytes always form u64"),
    );
    Duration::from_millis(entropy % (ceiling_ms.saturating_add(1)))
}

/// Extends an already-computed retry schedule for a provider's bounded
/// `Retry-After` hint. Kept public for stores/workers that need to preserve
/// deterministic settlement semantics around their own response adapters.
pub fn honor_retry_after(
    delivery: &mut WebhookDelivery,
    now: &Timestamp,
    retry_after_seconds: u64,
) {
    let delay = Duration::from_secs(retry_after_seconds.min(MAX_RETRY_AFTER_SECS));
    let Some(suggested) = now.checked_add(delay) else {
        return;
    };
    if delivery
        .next_attempt_at
        .is_none_or(|current| suggested.is_after(&current))
    {
        delivery.next_attempt_at = Some(suggested);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_is_deterministic_per_delivery_and_bounded() {
        let policy = WebhookRetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1_000,
        };
        let id = uuid::Uuid::now_v7();
        let first = jittered_backoff(&policy, 2, id);
        let second = jittered_backoff(&policy, 2, id);
        assert_eq!(first, second);
        assert!(first <= Duration::from_millis(2_000));
    }

    #[test]
    fn fetched_payload_bytes_must_match_the_durable_digest() {
        let body = br#"{"event":"finished"}"#;
        let hash = hex::encode(Sha256::digest(body));
        assert!(payload_hash_matches(body, &hash));
        assert!(!payload_hash_matches(br#"{"event":"tampered"}"#, &hash));
        assert!(!payload_hash_matches(body, &hash.to_ascii_uppercase()));
    }

    #[test]
    fn only_explicitly_transient_http_statuses_retry() {
        assert!(is_retryable_status(408));
        assert!(is_retryable_status(429));
        assert!(is_retryable_status(503));
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(404));
    }
}
