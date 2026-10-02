//! Webhook plumbing.
//!
//! **Inbound**: HMAC-SHA256 signature verification for delivered payloads,
//! in the `t=<unix>,v1=<hex>` header shape with a two-sided replay window
//! and constant-time comparison. Verification is fail-closed: malformed
//! headers, stale or future-dated timestamps and signature mismatches all
//! reject.
//!
//! **Outbound**: a store-and-forward dispatcher driving
//! [`mas_domain::WebhookEndpoint`]/[`mas_domain::WebhookDelivery`]:
//! enqueue (idempotent per endpoint+event) → sign → POST through the
//! hardened egress client → record attempt (backoff, `Retry-After`
//! honouring, exhaustion dead-letter) → endpoint circuit breaker via
//! `record_outcome`.
//!
//! Signing secrets are resolved through [`WebhookSecretPort`] at dispatch
//! time and never cached inside any dispatch object.

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use mas_common::error::AppError;
use mas_common::ids::{EventId, SecretReferenceId, TenantId, WebhookId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::notification::DeliveryState;
use mas_domain::webhook::{WebhookDelivery, WebhookEndpoint};
use sha2::Sha256;
use std::sync::Mutex;
use std::time::Duration;

use crate::http_guard::{GuardedHttpClient, HttpClientPort, HttpRequest};

/// Signature header emitted/consumed by the platform.
pub const SIGNATURE_HEADER: &str = "x-mas-signature";
/// Event type header on outbound deliveries.
pub const EVENT_TYPE_HEADER: &str = "x-mas-event";
/// Event id header on outbound deliveries.
pub const EVENT_ID_HEADER: &str = "x-mas-event-id";
/// Delivery id header on outbound deliveries.
pub const DELIVERY_ID_HEADER: &str = "x-mas-delivery";

/// Default two-sided replay tolerance for inbound signatures (5 minutes).
pub const DEFAULT_REPLAY_TOLERANCE: Duration = Duration::from_secs(300);
/// Maximum `Retry-After` honouring for outbound retries (1 hour).
pub const MAX_RETRY_AFTER_SECS: u64 = 3600;
/// Cap on stored payloads awaiting dispatch (back-pressure).
pub const MAX_STORED_PAYLOADS: usize = 4096;
/// Maximum payload bytes accepted for dispatch (1 MiB).
pub const MAX_PAYLOAD_BYTES: usize = 1 << 20;

type HmacSha256 = Hmac<Sha256>;

/// Computes the hex HMAC-SHA256 over `"{timestamp}.{payload}"`.
#[must_use]
pub fn compute_signature(timestamp: i64, payload: &[u8], secret: &[u8]) -> String {
    let mut input = timestamp.to_string().into_bytes();
    input.push(b'.');
    input.extend_from_slice(payload);
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret)
        .expect("HMAC-SHA256 accepts keys of any length");
    mac.update(&input);
    hex::encode(mac.finalize().into_bytes())
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Parsed inbound signature header (`t=<unix>,v1=<hex>[,kid=<version>]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundSignature {
    pub timestamp: i64,
    /// Hex-encoded HMAC-SHA256, exactly 64 lowercase hex characters.
    pub signature: String,
    pub key_version: Option<String>,
}

impl InboundSignature {
    /// Parses a signature header, fail-closed on any malformed component.
    pub fn parse(header: &str) -> Result<Self> {
        let mut timestamp = None;
        let mut signature = None;
        let mut key_version = None;
        for part in header.split(',') {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| AppError::unauthorized("malformed webhook signature header"))?;
            match key.trim() {
                "t" => {
                    if timestamp.is_some() {
                        return Err(AppError::unauthorized(
                            "duplicate timestamp in signature header",
                        ));
                    }
                    timestamp = Some(value.trim().parse::<i64>().map_err(|_| {
                        AppError::unauthorized("non-numeric timestamp in signature header")
                    })?);
                },
                "v1" => {
                    if signature.is_some() {
                        return Err(AppError::unauthorized(
                            "duplicate signature in signature header",
                        ));
                    }
                    let value = value.trim();
                    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
                        return Err(AppError::unauthorized(
                            "signature must be 64 lowercase hex characters",
                        ));
                    }
                    signature = Some(value.to_ascii_lowercase());
                },
                "kid" => key_version = Some(value.trim().chars().take(128).collect()),
                _ => {},
            }
        }
        match (timestamp, signature) {
            (Some(t), Some(v1)) => Ok(Self {
                timestamp: t,
                signature: v1,
                key_version,
            }),
            _ => Err(AppError::unauthorized(
                "signature header must contain t= and v1= components",
            )),
        }
    }
}

/// Builds the outbound signature header for `payload` signed `at`.
#[must_use]
pub fn sign_header(at: &Timestamp, payload: &[u8], secret: &[u8]) -> String {
    let ts = at.to_unix_seconds();
    format!("t={ts},v1={}", compute_signature(ts, payload, secret))
}

/// Verifies a parsed inbound signature against the raw `payload`:
/// 1. two-sided replay window `|now - t| <= tolerance` (future-dated and
///    stale signatures both reject),
/// 2. constant-time comparison against the expected tag.
pub fn verify_inbound(
    parts: &InboundSignature,
    payload: &[u8],
    secret: &[u8],
    now: &Timestamp,
    tolerance: Duration,
) -> Result<()> {
    let skew = now.to_unix_seconds().saturating_sub(parts.timestamp);
    if skew.unsigned_abs() > tolerance.as_secs() {
        return Err(AppError::unauthorized(
            "stale or future-dated webhook signature rejected",
        ));
    }
    let expected = hex::decode(compute_signature(parts.timestamp, payload, secret))
        .map_err(|_| AppError::internal("hex encoding of HMAC failed"))?;
    let candidate = hex::decode(&parts.signature)
        .map_err(|_| AppError::unauthorized("webhook signature is not valid hex"))?;
    if !ct_eq(&expected, &candidate) {
        return Err(AppError::unauthorized(
            "webhook signature verification failed",
        ));
    }
    Ok(())
}

/// Event payload retained between enqueue and dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub event_type: String,
    pub body: Vec<u8>,
}

/// Resolves webhook signing secrets for endpoints.
#[async_trait]
pub trait WebhookSecretPort: std::fmt::Debug + Send + Sync {
    /// Returns the raw bytes of the referenced secret. Resolution failures
    /// are supported (unavailable store, retired version, …).
    async fn resolve(&self, reference: SecretReferenceId) -> Result<Vec<u8>>;
}

/// Persistence port for endpoints, deliveries and retained payloads.
#[async_trait]
pub trait WebhookStorePort: std::fmt::Debug + Send + Sync {
    /// All endpoints registered by `tenant`.
    async fn endpoints_for(&self, tenant: TenantId) -> Result<Vec<WebhookEndpoint>>;
    /// Endpoint lookup by id (any tenant; the caller enforces scoping).
    async fn get_endpoint(&self, id: WebhookId) -> Result<Option<WebhookEndpoint>>;
    /// Upserts an endpoint (keyed by `endpoint.id`).
    async fn save_endpoint(&self, endpoint: &WebhookEndpoint) -> Result<()>;
    /// Upserts a delivery (keyed by `delivery.id`).
    async fn save_delivery(&self, delivery: &WebhookDelivery) -> Result<()>;
    /// All deliveries for an endpoint (any state).
    async fn deliveries_for(&self, endpoint_id: WebhookId) -> Result<Vec<WebhookDelivery>>;
    /// Up to `limit` deliveries ready for an attempt at `now`
    /// (non-terminal, `next_attempt_at` absent or due), oldest first.
    async fn due_deliveries(&self, now: &Timestamp, limit: u32) -> Result<Vec<WebhookDelivery>>;
    /// Retains an event payload for later dispatch. Existing ids overwrite;
    /// store-full rejects with `Conflict`.
    async fn store_payload(&self, event_id: EventId, stored: StoredEvent) -> Result<()>;
    /// Fetches a retained payload.
    async fn payload_for(&self, event_id: EventId) -> Result<Option<StoredEvent>>;
    /// Removes an endpoint (individuated from `save_endpoint` so tests and
    /// admin flows can retire endpoints). Returns whether it existed.
    async fn remove_endpoint(&self, id: WebhookId) -> Result<bool>;
}

/// In-memory reference implementation of [`WebhookStorePort`]. Delivery
/// keys are UUIDv7, so iteration is already oldest-first.
#[derive(Debug, Default)]
pub struct InMemoryWebhookStore {
    endpoints: Mutex<std::collections::BTreeMap<WebhookId, WebhookEndpoint>>,
    deliveries: Mutex<std::collections::BTreeMap<uuid::Uuid, WebhookDelivery>>,
    payloads: Mutex<std::collections::BTreeMap<EventId, StoredEvent>>,
}

impl InMemoryWebhookStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience for tests/seed: registers an endpoint.
    pub fn insert_endpoint(&self, endpoint: WebhookEndpoint) {
        self.endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.id, endpoint);
    }

    /// Outstanding (non-terminal) delivery count.
    #[must_use]
    pub fn outstanding(&self) -> usize {
        self.deliveries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|d| !d.state.is_terminal())
            .count()
    }
}

#[async_trait]
impl WebhookStorePort for InMemoryWebhookStore {
    async fn endpoints_for(&self, tenant: TenantId) -> Result<Vec<WebhookEndpoint>> {
        Ok(self
            .endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|ep| ep.tenant_id == tenant)
            .cloned()
            .collect())
    }

    async fn get_endpoint(&self, id: WebhookId) -> Result<Option<WebhookEndpoint>> {
        Ok(self
            .endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }

    async fn save_endpoint(&self, endpoint: &WebhookEndpoint) -> Result<()> {
        self.endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.id, endpoint.clone());
        Ok(())
    }

    async fn save_delivery(&self, delivery: &WebhookDelivery) -> Result<()> {
        self.deliveries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(delivery.id, delivery.clone());
        Ok(())
    }

    async fn deliveries_for(&self, endpoint_id: WebhookId) -> Result<Vec<WebhookDelivery>> {
        Ok(self
            .deliveries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|d| d.endpoint_id == endpoint_id)
            .cloned()
            .collect())
    }

    async fn due_deliveries(&self, now: &Timestamp, limit: u32) -> Result<Vec<WebhookDelivery>> {
        Ok(self
            .deliveries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|d| {
                !d.state.is_terminal()
                    && match d.next_attempt_at {
                        // Fresh deliveries are due immediately; a delivery
                        // with attempts but no schedule is EXHAUSTED
                        // (dead-lettered), not due.
                        None => d.attempt_count == 0,
                        Some(due) => !due.is_after(now),
                    }
            })
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn store_payload(&self, event_id: EventId, stored: StoredEvent) -> Result<()> {
        let mut payloads = self.payloads.lock().unwrap_or_else(|e| e.into_inner());
        if !payloads.contains_key(&event_id) && payloads.len() >= MAX_STORED_PAYLOADS {
            return Err(AppError::conflict(
                "webhook payload ledger is full; drain deliveries before enqueueing more",
            ));
        }
        payloads.insert(event_id, stored);
        Ok(())
    }

    async fn payload_for(&self, event_id: EventId) -> Result<Option<StoredEvent>> {
        Ok(self
            .payloads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&event_id)
            .cloned())
    }

    async fn remove_endpoint(&self, id: WebhookId) -> Result<bool> {
        Ok(self
            .endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
            .is_some())
    }
}

/// In-memory secret resolver for tests and composition.
#[derive(Debug, Default)]
pub struct InMemorySecretResolver {
    secrets: Mutex<std::collections::BTreeMap<SecretReferenceId, Vec<u8>>>,
}

impl InMemorySecretResolver {
    /// Empty resolver.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds material to a reference id (test/seed convenience).
    pub fn insert(&self, reference: SecretReferenceId, material: &[u8]) {
        self.secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(reference, material.to_vec());
    }
}

#[async_trait]
impl WebhookSecretPort for InMemorySecretResolver {
    async fn resolve(&self, reference: SecretReferenceId) -> Result<Vec<u8>> {
        self.secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&reference)
            .cloned()
            .ok_or_else(|| AppError::not_found("webhook signing secret", reference.to_string()))
    }
}

/// Outcome counters of one [`WebhookDispatcher::flush_due`] pass.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FlushStats {
    /// Deliveries an HTTP attempt was made for.
    pub attempted: u64,
    /// Now terminal-delivered.
    pub delivered: u64,
    /// Failed but eligible for another attempt.
    pub failed_retryable: u64,
    /// Permanently failed (attempt budget exhausted).
    pub exhausted: u64,
    /// Cancelled because the target endpoint no longer exists.
    pub cancelled: u64,
}

/// Store-and-forward outbound webhook dispatcher.
#[derive(Debug)]
pub struct WebhookDispatcher<S, K, C>
where
    S: WebhookStorePort,
    K: WebhookSecretPort,
    C: HttpClientPort,
{
    store: S,
    secrets: K,
    client: GuardedHttpClient<C>,
}

impl<S, K, C> WebhookDispatcher<S, K, C>
where
    S: WebhookStorePort,
    K: WebhookSecretPort,
    C: HttpClientPort,
{
    /// The delivery/endpoint store (inspectors and tests).
    #[must_use]
    pub const fn store_ref(&self) -> &S {
        &self.store
    }
}

impl<S, K, C> WebhookDispatcher<S, K, C>
where
    S: WebhookStorePort,
    K: WebhookSecretPort,
    C: HttpClientPort,
{
    /// Builds a dispatcher over store, secrets and guarded HTTP client.
    #[must_use]
    pub const fn new(store: S, secrets: K, client: GuardedHttpClient<C>) -> Self {
        Self {
            store,
            secrets,
            client,
        }
    }

    /// The guarded client (policy introspection).
    #[must_use]
    pub const fn client(&self) -> &GuardedHttpClient<C> {
        &self.client
    }

    /// Creates pending deliveries for every enabled endpoint of `tenant`
    /// subscribing to `event_type`. Idempotent per (endpoint, event):
    /// re-enqueueing an already tracked pair is a no-op so crash-replays
    /// never duplicate a delivery. Payload retention is size-capped.
    pub async fn enqueue(
        &self,
        tenant: TenantId,
        event_id: EventId,
        event_type: &str,
        payload: &[u8],
    ) -> Result<u64> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(AppError::invalid_field(
                "payload",
                "too_large",
                format!("webhook payload exceeds the {MAX_PAYLOAD_BYTES} byte cap"),
            ));
        }
        self.store
            .store_payload(
                event_id,
                StoredEvent {
                    event_type: event_type.to_owned(),
                    body: payload.to_vec(),
                },
            )
            .await?;

        let payload_hash = {
            use sha2::Digest as _;
            hex::encode(sha2::Sha256::digest(payload))
        };
        let mut enqueued = 0_u64;
        for endpoint in self.store.endpoints_for(tenant).await? {
            if !endpoint.subscribes_to(event_type) {
                continue;
            }
            let tracked = self
                .store
                .deliveries_for(endpoint.id)
                .await?
                .into_iter()
                .any(|d| d.event_id == event_id);
            if tracked {
                continue;
            }
            let delivery = WebhookDelivery::new(endpoint.id, event_id, payload_hash.clone())?;
            self.store.save_delivery(&delivery).await?;
            tracing::debug!(endpoint = %endpoint.id, event = %event_id, "webhook delivery enqueued");
            enqueued += 1;
        }
        Ok(enqueued)
    }

    /// Attempts every due delivery once. Failures never abort the pass —
    /// they're recorded through `record_attempt` (backoff/exhaustion) and
    /// the endpoint circuit breaker (`record_outcome`).
    pub async fn flush_due(&self, now: &Timestamp) -> Result<FlushStats> {
        let mut stats = FlushStats::default();
        for mut delivery in self.store.due_deliveries(now, 1024).await? {
            let Some(mut endpoint) = self.store.get_endpoint(delivery.endpoint_id).await? else {
                delivery.state = DeliveryState::Cancelled;
                delivery.last_error = Some("endpoint removed".to_owned());
                delivery.updated_at = *now;
                self.store.save_delivery(&delivery).await?;
                stats.cancelled += 1;
                continue;
            };
            let Some(stored) = self.store.payload_for(delivery.event_id).await? else {
                delivery.state = DeliveryState::Cancelled;
                delivery.last_error = Some("event payload evicted before delivery".to_owned());
                delivery.updated_at = *now;
                self.store.save_delivery(&delivery).await?;
                stats.cancelled += 1;
                continue;
            };

            let secret = match self
                .secrets
                .resolve(endpoint.signature.secret_reference_id)
                .await
            {
                Ok(secret) => secret,
                Err(err) => {
                    delivery.record_attempt(
                        None,
                        &endpoint.retry_policy,
                        Some(clean_error_fragment(&err.to_string())),
                    );
                    if delivery.attempt_count < endpoint.retry_policy.max_attempts {
                        delivery.next_attempt_at = now.checked_add(policy_backoff(
                            &endpoint.retry_policy,
                            delivery.attempt_count,
                        ));
                    }
                    self.store.save_delivery(&delivery).await?;
                    endpoint.record_outcome(false);
                    self.store.save_endpoint(&endpoint).await?;
                    stats.attempted += 1;
                    count_failure(&delivery, &mut stats, &endpoint.retry_policy);
                    continue;
                },
            };

            let signature = sign_header(now, &stored.body, &secret);
            let request = HttpRequest::post(endpoint.url.clone(), stored.body.clone())
                .with_header("content-type", "application/json")
                .with_header(SIGNATURE_HEADER, &signature)
                .with_header(EVENT_TYPE_HEADER, &stored.event_type)
                .with_header(EVENT_ID_HEADER, &delivery.event_id.to_string())
                .with_header(DELIVERY_ID_HEADER, &delivery.id.to_string());

            stats.attempted += 1;
            match self.client.send(&request).await {
                Ok(response) => {
                    let delivered = response.is_success();
                    delivery.record_attempt(Some(response.status), &endpoint.retry_policy, None);
                    // Domain `record_attempt` anchors its backoff on the wall
                    // clock; re-anchor onto OUR `now` so the scheduler
                    // driving `flush_due` stays fully deterministic.
                    if !delivered && delivery.attempt_count < endpoint.retry_policy.max_attempts {
                        delivery.next_attempt_at = now.checked_add(policy_backoff(
                            &endpoint.retry_policy,
                            delivery.attempt_count,
                        ));
                    }
                    if !delivered
                        && matches!(response.status, 429 | 503)
                        && response.retry_after_seconds().is_some()
                    {
                        honor_retry_after(
                            &mut delivery,
                            now,
                            response.retry_after_seconds().expect("checked"),
                        );
                    }
                    endpoint.record_outcome(delivered);
                    self.store.save_endpoint(&endpoint).await?;
                    self.store.save_delivery(&delivery).await?;
                    if delivered {
                        stats.delivered += 1;
                        tracing::debug!(delivery = %delivery.id, "webhook delivered");
                    } else {
                        count_failure(&delivery, &mut stats, &endpoint.retry_policy);
                        tracing::warn!(delivery = %delivery.id, status = response.status, "webhook delivery failed");
                    }
                },
                Err(err) => {
                    delivery.record_attempt(
                        None,
                        &endpoint.retry_policy,
                        Some(clean_error_fragment(&err.to_string())),
                    );
                    if delivery.attempt_count < endpoint.retry_policy.max_attempts {
                        delivery.next_attempt_at = now.checked_add(policy_backoff(
                            &endpoint.retry_policy,
                            delivery.attempt_count,
                        ));
                    }
                    endpoint.record_outcome(false);
                    self.store.save_endpoint(&endpoint).await?;
                    self.store.save_delivery(&delivery).await?;
                    count_failure(&delivery, &mut stats, &endpoint.retry_policy);
                },
            }
        }
        Ok(stats)
    }
}

/// Applies a provider-supplied `Retry-After`, never shortening the planned
/// backoff and capping hints at [`MAX_RETRY_AFTER_SECS`].
fn honor_retry_after(delivery: &mut WebhookDelivery, now: &Timestamp, retry_after: u64) {
    let capped = Duration::from_secs(retry_after.min(MAX_RETRY_AFTER_SECS));
    let Some(suggested) = now.checked_add(capped) else {
        return;
    };
    if delivery
        .next_attempt_at
        .is_none_or(|planned| suggested.is_after(&planned))
    {
        delivery.next_attempt_at = Some(suggested);
    }
}

/// Mirrors the domain backoff formula (`initial * 2^(attempt-1)`, capped
/// at 5 minutes) so retries can be re-anchored on the caller's clock.
fn policy_backoff(policy: &mas_domain::webhook::WebhookRetryPolicy, attempt: u32) -> Duration {
    let factor = 1_u64 << (attempt.saturating_sub(1).min(6));
    Duration::from_millis(
        policy
            .initial_backoff_ms
            .saturating_mul(factor)
            .min(300_000),
    )
}

/// Renders an error for durable storage: single-line, length-capped.
fn clean_error_fragment(raw: &str) -> String {
    let clean: String = raw
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    clean.chars().take(256).collect()
}

fn count_failure(
    delivery: &WebhookDelivery,
    stats: &mut FlushStats,
    policy: &mas_domain::webhook::WebhookRetryPolicy,
) {
    let exhausted = !delivery.state.is_terminal()
        && delivery.attempt_count >= policy.max_attempts
        && delivery.next_attempt_at.is_none();
    if exhausted {
        stats.exhausted += 1;
    } else {
        stats.failed_retryable += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_guard::{EgressPolicy, HttpResponse};
    use mas_common::ids::SecretReferenceId;
    use mas_domain::value_objects::SafeUrl;
    use std::collections::BTreeSet;

    const SECRET: &[u8] = b"whsec_test_key_material";

    fn at(secs: i64) -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000 + secs).expect("ts")
    }

    #[test]
    fn inbound_signature_roundtrip_and_tamper_rejection() {
        let now = at(0);
        let payload = br#"{"type":"execution.completed"}"#;
        let header = sign_header(&now, payload, SECRET);
        let parts = InboundSignature::parse(&header).expect("parse own header");
        verify_inbound(&parts, payload, SECRET, &now, DEFAULT_REPLAY_TOLERANCE).expect("verifies");

        verify_inbound(
            &parts,
            b"{} mutated",
            SECRET,
            &now,
            DEFAULT_REPLAY_TOLERANCE,
        )
        .expect_err("tampered payload");
        verify_inbound(
            &parts,
            payload,
            b"wrong-secret",
            &now,
            DEFAULT_REPLAY_TOLERANCE,
        )
        .expect_err("wrong secret");
    }

    #[test]
    fn replay_window_rejects_stale_and_future_timestamps() {
        let payload = b"x";
        let old = sign_header(&at(-10_000), payload, SECRET);
        let old_parts = InboundSignature::parse(&old).expect("parse");
        verify_inbound(
            &old_parts,
            payload,
            SECRET,
            &at(0),
            DEFAULT_REPLAY_TOLERANCE,
        )
        .expect_err("stale");

        let future = sign_header(&at(10_000), payload, SECRET);
        let future_parts = InboundSignature::parse(&future).expect("parse");
        verify_inbound(
            &future_parts,
            payload,
            SECRET,
            &at(0),
            DEFAULT_REPLAY_TOLERANCE,
        )
        .expect_err("future-dated");

        let just_ok = sign_header(&at(-299), payload, SECRET);
        let parts = InboundSignature::parse(&just_ok).expect("parse");
        verify_inbound(&parts, payload, SECRET, &at(0), DEFAULT_REPLAY_TOLERANCE)
            .expect("inside window");
    }

    #[test]
    fn header_parser_is_fail_closed() {
        assert!(
            InboundSignature::parse("t=1700000000").is_err(),
            "missing v1"
        );
        assert!(
            InboundSignature::parse("v1=zz,t=1700000000").is_err(),
            "non-hex / wrong length"
        );
        assert!(
            InboundSignature::parse(
                "t=abc,v1=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
            .is_err(),
            "non-numeric t"
        );
        assert!(
            InboundSignature::parse(
                "t=1,t=2,v1=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
            .is_err(),
            "dup t"
        );
        let parsed = InboundSignature::parse("t=1700000000,v1=E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855,kid=v2024")
            .expect("valid with kid and uppercase hex");
        assert_eq!(parsed.key_version.as_deref(), Some("v2024"));
        assert_eq!(
            parsed.signature,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// Scriptable egress client for dispatcher tests.
    #[derive(Debug, Default)]
    struct FakeEgress {
        recorded: Mutex<Vec<HttpRequest>>,
        responses: Mutex<std::collections::VecDeque<HttpResponse>>,
    }

    impl FakeEgress {
        fn push(&self, response: HttpResponse) {
            self.responses
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push_back(response);
        }

        fn recorded(&self) -> Vec<HttpRequest> {
            self.recorded
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
        }
    }

    #[async_trait]
    impl HttpClientPort for FakeEgress {
        async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
            self.recorded
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(request.clone());
            let response = self
                .responses
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop_front()
                .unwrap_or_else(|| HttpResponse::new(200));
            Ok(response)
        }
    }

    struct Fixture {
        tenant: TenantId,
        store: InMemoryWebhookStore,
        secrets: InMemorySecretResolver,
        egress: FakeEgress,
        endpoint_id: WebhookId,
    }

    fn fixture() -> Fixture {
        let tenant = TenantId::new();
        let secret_ref = SecretReferenceId::new();
        let store = InMemoryWebhookStore::new();
        let secrets = InMemorySecretResolver::new();
        secrets.insert(secret_ref, SECRET);
        let mut events = BTreeSet::new();
        events.insert("execution.completed".to_owned());
        let endpoint = WebhookEndpoint::register(
            tenant,
            SafeUrl::parse("https://hooks.example.test/incoming").expect("url"),
            events,
            mas_domain::webhook::SignatureConfig {
                algorithm: mas_domain::webhook::SignatureAlgorithm::HmacSha256,
                secret_reference_id: secret_ref,
                key_version: None,
            },
        )
        .expect("endpoint");
        let endpoint_id = endpoint.id;
        store.insert_endpoint(endpoint);
        Fixture {
            tenant,
            store,
            secrets,
            egress: FakeEgress::default(),
            endpoint_id,
        }
    }

    /// Builds a dispatcher from a fixture, moving store/secrets/egress.
    fn dispatcher_of(
        fixture: Fixture,
    ) -> WebhookDispatcher<InMemoryWebhookStore, InMemorySecretResolver, FakeEgress> {
        let client = GuardedHttpClient::new(fixture.egress, EgressPolicy::default());
        WebhookDispatcher::new(fixture.store, fixture.secrets, client)
    }

    #[tokio::test]
    async fn dispatch_delivers_signed_payloads_and_is_idempotent() {
        let fixture = fixture();
        let tenant = fixture.tenant;
        let endpoint_id = fixture.endpoint_id;
        let dispatcher = dispatcher_of(fixture);
        let event_id = EventId::new();
        let payload = br#"{"execution":"e-123","status":"completed"}"#.to_vec();

        let enqueued = dispatcher
            .enqueue(tenant, event_id, "execution.completed", &payload)
            .await
            .expect("enqueue");
        assert_eq!(enqueued, 1);
        let again = dispatcher
            .enqueue(tenant, event_id, "execution.completed", &payload)
            .await
            .expect("re-enqueue is a no-op");
        assert_eq!(again, 0, "same (endpoint,event) never duplicates");

        let stats = dispatcher.flush_due(&at(0)).await.expect("flush");
        assert_eq!(stats.attempted, 1);
        assert_eq!(stats.delivered, 1);

        let requests = dispatcher.client().inner().recorded();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        let signature = request
            .headers
            .iter()
            .find(|(k, _)| k == SIGNATURE_HEADER)
            .map(|(_, v)| v.clone())
            .expect("signature header present");
        let parts = InboundSignature::parse(&signature).expect("parse signature");
        // The dispatcher flushed at `at(0)`, so verify at the same instant.
        verify_inbound(
            &parts,
            &request.body,
            SECRET,
            &at(0),
            DEFAULT_REPLAY_TOLERANCE,
        )
        .expect("receiver-side verification passes");

        let deliveries = dispatcher_deliveries_folded(&dispatcher, endpoint_id).await;
        assert_eq!(deliveries[0].state, DeliveryState::Delivered);
        assert!(deliveries[0].next_attempt_at.is_none());
        assert_eq!(deliveries[0].payload_hash.len(), 64);
    }

    #[tokio::test]
    async fn failures_back_off_retry_after_and_exhaust() {
        let fixture = fixture();
        let tenant = fixture.tenant;
        let endpoint_id = fixture.endpoint_id;
        {
            let mut ep = fixture
                .store
                .get_endpoint(endpoint_id)
                .await
                .expect("get")
                .expect("present");
            ep.retry_policy = mas_domain::webhook::WebhookRetryPolicy {
                max_attempts: 2,
                initial_backoff_ms: 250,
            };
            fixture.store.save_endpoint(&ep).await.expect("save");
        }
        fixture.egress.push({
            let mut r = HttpResponse::new(429);
            r.headers.push(("retry-after".to_owned(), "120".to_owned()));
            r
        });
        fixture.egress.push(HttpResponse::new(500));

        let dispatcher = dispatcher_of(fixture);
        let event_id = EventId::new();
        dispatcher
            .enqueue(tenant, event_id, "execution.completed", b"{}")
            .await
            .expect("enqueue");

        let now = at(0);
        let first = dispatcher.flush_due(&now).await.expect("flush 1");
        assert_eq!(first.attempted, 1);
        assert_eq!(first.failed_retryable, 1);
        let delivery = dispatcher_deliveries_folded(&dispatcher, endpoint_id).await[0].clone();
        let retry_at = delivery.next_attempt_at.expect("retry scheduled");
        let gap = retry_at
            .duration_since(&now)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        assert_eq!(gap, 120, "Retry-After wins over the 250ms policy backoff");

        // Not due yet.
        let early = dispatcher.flush_due(&at(30)).await.expect("flush early");
        assert_eq!(early.attempted, 0, "backoff is honoured between passes");

        // Due again at t>=120; the 500 exhausts the 2-attempt budget.
        let second = dispatcher.flush_due(&at(130)).await.expect("flush 2");
        assert_eq!(second.attempted, 1);
        assert_eq!(second.exhausted, 1, "attempt budget exhausted");
        let delivery = dispatcher_deliveries_folded(&dispatcher, endpoint_id).await[0].clone();
        assert_eq!(delivery.state, DeliveryState::Failed);
        assert!(delivery.next_attempt_at.is_none());
        assert_eq!(delivery.attempt_count, 2);

        // Third pass has nothing to do.
        let third = dispatcher.flush_due(&at(200)).await.expect("flush 3");
        assert_eq!(third.attempted, 0);
    }

    #[tokio::test]
    async fn unresolved_secret_and_removed_endpoint_are_handled_without_crashing() {
        let fixture = fixture();
        let tenant = fixture.tenant;
        let endpoint_id = fixture.endpoint_id;

        // Second endpoint whose signing secret is NOT in the resolver.
        let unsigned_ref = SecretReferenceId::new();
        let mut events = BTreeSet::new();
        events.insert("execution.completed".to_owned());
        let unsigned = WebhookEndpoint::register(
            tenant,
            SafeUrl::parse("https://hooks2.example.test/in").expect("url"),
            events,
            mas_domain::webhook::SignatureConfig {
                algorithm: mas_domain::webhook::SignatureAlgorithm::HmacSha256,
                secret_reference_id: unsigned_ref,
                key_version: None,
            },
        )
        .expect("endpoint");
        let unsigned_id = unsigned.id;
        fixture.store.insert_endpoint(unsigned);
        let dispatcher = dispatcher_of(fixture);
        let event_id = EventId::new();
        let enqueued = dispatcher
            .enqueue(tenant, event_id, "execution.completed", b"{}")
            .await
            .expect("enqueue");
        assert_eq!(enqueued, 2);

        // Flush 1: healthy endpoint delivers; the unresolvable secret is
        // recorded as a failed attempt with a sanitized error — no panic.
        let stats = dispatcher.flush_due(&at(0)).await.expect("flush");
        assert_eq!(stats.attempted, 2);
        assert_eq!(stats.delivered, 1);
        assert_eq!(stats.failed_retryable, 1);
        let broken = dispatcher
            .store_ref()
            .deliveries_for(unsigned_id)
            .await
            .expect("deliveries");
        assert_eq!(broken[0].attempt_count, 1);
        assert!(matches!(broken[0].state, DeliveryState::Failed));
        assert!(broken[0].last_error.is_some());

        // Enqueue a second event, then retire the healthy endpoint before
        // the next pass: its queued delivery must be cancelled in-flight.
        let second_event = EventId::new();
        dispatcher
            .enqueue(tenant, second_event, "execution.completed", b"{}")
            .await
            .expect("enqueue 2");
        assert!(
            dispatcher
                .store_ref()
                .remove_endpoint(endpoint_id)
                .await
                .expect("remove"),
            "endpoint existed"
        );
        let stats = dispatcher.flush_due(&at(10)).await.expect("flush 2");
        assert_eq!(
            stats.cancelled, 1,
            "orphaned delivery cancelled instead of attempted"
        );

        let orphaned = dispatcher
            .store_ref()
            .deliveries_for(endpoint_id)
            .await
            .expect("deliveries");
        assert!(orphaned.iter().any(|d| d.state == DeliveryState::Cancelled));
    }

    /// Reads deliveries for `endpoint_id` through the dispatcher's store.
    async fn dispatcher_deliveries_folded<S, K, C>(
        dispatcher: &WebhookDispatcher<S, K, C>,
        endpoint_id: WebhookId,
    ) -> Vec<WebhookDelivery>
    where
        S: WebhookStorePort,
        K: WebhookSecretPort,
        C: HttpClientPort,
    {
        dispatcher
            .store_ref()
            .deliveries_for(endpoint_id)
            .await
            .expect("deliveries")
    }
}
