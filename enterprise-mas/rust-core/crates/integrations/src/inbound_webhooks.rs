//! PURPOSE: Canonical authenticated inbound-webhook ingress orchestration.
//!
//! Owning layer: `mas-integrations` public-ingress security boundary.
//!
//! Production role: resolve an operator-configured opaque ingress route,
//! verify an HMAC signature over bounded raw bytes before creating any durable
//! evidence, then hand a verified receipt to the PostgreSQL receipt/outbox
//! adapter for atomic replay prevention and deterministic ordering.
//!
//! Security boundary: callers never provide a tenant id, secret reference, or
//! secret-manager path. A route id is only a lookup key. Invalid signatures,
//! stale timestamps, malformed payloads, unknown routes, and unsupported key
//! versions do not create receipt rows or outbox events.
//!
//! Data ownership: routes belong to deployment configuration or a future
//! canonical ingress registry; this module owns transient verification only;
//! receipt/outbox stores own durable processing facts; the secret manager owns
//! signing-key bytes.
//!
//! Prohibited responsibilities: accepting unsigned events, parsing an event
//! before authenticating it, storing raw secrets, making outbound HTTP calls,
//! fabricating a successful receipt, or bypassing the durable receipt port.

use std::collections::BTreeSet;
use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{InboundWebhookId, OrganizationId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use sha2::{Digest as _, Sha256};

use crate::webhooks::{
    verify_inbound, InboundSignature, WebhookSecretPort, DEFAULT_REPLAY_TOLERANCE,
};

/// A public ingress body is intentionally lower than the generic JSON cap so
/// receipt/outbox processing cannot be used as a bulk-upload side channel.
pub const MAX_INBOUND_WEBHOOK_BYTES: usize = 128 * 1024;
/// A route key is opaque to callers but syntactically constrained for logs and
/// registry implementations that may use it as an indexed lookup key.
pub const MAX_INGRESS_ROUTE_ID_BYTES: usize = 128;
/// External providers frequently use UUIDs, delivery ids, or integer ids; keep
/// the value opaque rather than attempting provider-specific interpretation.
pub const MAX_EXTERNAL_EVENT_ID_BYTES: usize = 256;

/// Tenant-owned configuration for one public inbound webhook endpoint.
/// It holds a reference to a signing secret, never signing material itself.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InboundWebhookEndpoint {
    pub id: InboundWebhookId,
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub source: String,
    pub signing_secret_reference_id: SecretReferenceId,
    /// If present, callbacks must name this exact `kid`.
    pub required_key_version: Option<String>,
    /// Explicit current/previous `kid` allowlist for a rotating endpoint. An
    /// empty set means only an absent `kid` is allowed unless `required` pins
    /// one exact key version.
    pub accepted_key_versions: BTreeSet<String>,
    pub enabled: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl InboundWebhookEndpoint {
    /// Constructs an enabled endpoint from authorized, non-secret request
    /// metadata. The canonical store separately proves the reference is owned
    /// by this endpoint's tenant through a composite foreign key.
    pub fn register(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        source: impl Into<String>,
        signing_secret_reference_id: SecretReferenceId,
        required_key_version: Option<String>,
        accepted_key_versions: BTreeSet<String>,
    ) -> Result<Self> {
        let now = Timestamp::now();
        let endpoint = Self {
            id: InboundWebhookId::new(),
            tenant_id,
            organization_id,
            source: source.into(),
            signing_secret_reference_id,
            required_key_version,
            accepted_key_versions,
            enabled: true,
            created_at: now,
            updated_at: now,
        };
        endpoint.validate()?;
        Ok(endpoint)
    }

    /// Disables public ingress immediately; already-verified receipt/outbox
    /// records remain durable evidence and are not erased.
    pub fn disable(&mut self) {
        self.enabled = false;
        self.updated_at = Timestamp::now();
    }

    /// Re-enables a previously disabled endpoint after an authorized mutation.
    pub fn enable(&mut self) {
        self.enabled = true;
        self.updated_at = Timestamp::now();
    }

    /// Validates only durable endpoint metadata, never secret material.
    pub fn validate(&self) -> Result<()> {
        if self.id.is_nil()
            || self.tenant_id.is_nil()
            || self.organization_id.is_nil()
            || self.signing_secret_reference_id.is_nil()
        {
            return Err(AppError::invalid_field(
                "inbound_webhook_endpoint",
                "required",
                "inbound webhook endpoint requires concrete scope and secret reference ids",
            ));
        }
        validate_source(&self.source)?;
        if self
            .required_key_version
            .as_deref()
            .is_some_and(|version| !is_safe_key_version(version))
            || self.accepted_key_versions.len() > 3
            || self
                .accepted_key_versions
                .iter()
                .any(|version| !is_safe_key_version(version))
        {
            return Err(AppError::invalid_field(
                "key_versions",
                "invalid_format",
                "inbound key versions must be at most three bounded safe tokens",
            ));
        }
        if let Some(required) = self.required_key_version.as_deref() {
            if !self.accepted_key_versions.is_empty()
                && !self.accepted_key_versions.contains(required)
            {
                return Err(AppError::invalid_field(
                    "key_versions",
                    "inconsistent",
                    "a required inbound key version must appear in the accepted key-version set",
                ));
            }
        }
        Ok(())
    }
}

/// Stable keyset cursor over inbound endpoint `created_at DESC, id DESC`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundWebhookEndpointCursor {
    pub created_at: Timestamp,
    pub id: InboundWebhookId,
}

/// Request-plane persistence boundary for tenant-owned ingress endpoint
/// configuration. Public route lookup is intentionally a separate port below.
#[async_trait]
pub trait InboundWebhookEndpointStorePort: fmt::Debug + Send + Sync {
    async fn save(&self, endpoint: &InboundWebhookEndpoint) -> Result<()>;
    async fn get(
        &self,
        tenant_id: TenantId,
        endpoint_id: InboundWebhookId,
    ) -> Result<Option<InboundWebhookEndpoint>>;
    async fn list_page(
        &self,
        tenant_id: TenantId,
        after: Option<InboundWebhookEndpointCursor>,
        limit: u16,
    ) -> Result<Vec<InboundWebhookEndpoint>>;
}

/// Server-owned configuration selected by an opaque public route id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundWebhookRoute {
    pub tenant_id: TenantId,
    pub organization_id: Option<OrganizationId>,
    /// Stable provider/source key recorded with the receipt.
    pub source: String,
    /// Canonical reference resolved by the configured secret-manager bridge.
    pub signing_secret_reference_id: SecretReferenceId,
    /// `Some` requires the signature header to name this exact version.
    pub required_key_version: Option<String>,
    /// Explicit accepted `kid` values during key rotation. The public ingress
    /// rejects a named version outside this bounded server-owned set.
    pub accepted_key_versions: BTreeSet<String>,
}

impl InboundWebhookRoute {
    /// Validates non-secret server-owned route metadata.
    pub fn validate(&self) -> Result<()> {
        if self.tenant_id.is_nil() || self.signing_secret_reference_id.is_nil() {
            return Err(AppError::invalid_field(
                "inbound_webhook_route",
                "required",
                "inbound webhook route requires concrete tenant and secret reference ids",
            ));
        }
        validate_source(&self.source)?;
        if self
            .required_key_version
            .as_deref()
            .is_some_and(|version| !is_safe_key_version(version))
            || self.accepted_key_versions.len() > 3
            || self
                .accepted_key_versions
                .iter()
                .any(|version| !is_safe_key_version(version))
        {
            return Err(AppError::invalid_field(
                "key_versions",
                "invalid_format",
                "inbound route key versions must be bounded safe tokens",
            ));
        }
        if let Some(required) = self.required_key_version.as_deref() {
            if !self.accepted_key_versions.is_empty()
                && !self.accepted_key_versions.contains(required)
            {
                return Err(AppError::invalid_field(
                    "key_versions",
                    "inconsistent",
                    "required inbound key version is absent from route allowlist",
                ));
            }
        }
        Ok(())
    }
}

/// Untrusted public request fields. `route_id` selects deployment-owned
/// configuration; it is never a tenant identifier or secret-manager path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundWebhookRequest {
    pub route_id: String,
    pub external_event_id: String,
    pub sequence_no: Option<i64>,
    pub signature_header: String,
    pub body: Vec<u8>,
}

impl InboundWebhookRequest {
    fn validate_shape(&self) -> Result<()> {
        if !is_safe_route_id(&self.route_id) {
            return Err(AppError::not_found(
                "inbound_webhook_route",
                "route unavailable",
            ));
        }
        if self.external_event_id.is_empty()
            || self.external_event_id.len() > MAX_EXTERNAL_EVENT_ID_BYTES
            || self.external_event_id.chars().any(char::is_control)
        {
            return Err(AppError::invalid_field(
                "external_event_id",
                "invalid_format",
                "external event id must be 1..=256 non-control characters",
            ));
        }
        if self.sequence_no.is_some_and(|sequence| sequence < 0) {
            return Err(AppError::invalid_field(
                "sequence_no",
                "invalid_range",
                "inbound sequence numbers must be non-negative",
            ));
        }
        if self.signature_header.is_empty() || self.signature_header.len() > 512 {
            return Err(AppError::unauthorized(
                "invalid inbound webhook signature header",
            ));
        }
        if self.body.is_empty() || self.body.len() > MAX_INBOUND_WEBHOOK_BYTES {
            return Err(AppError::invalid_field(
                "body",
                "invalid_size",
                "inbound webhook body must be 1..=131072 bytes",
            ));
        }
        Ok(())
    }
}

/// Authenticated, bounded payload passed only to a durable receipt adapter.
/// The raw JSON is retained only because a deferred sequence must later emit
/// the same canonical outbox event. It is not logged or placed in errors.
#[derive(Debug, Clone, PartialEq)]
pub struct VerifiedInboundWebhook {
    pub tenant_id: TenantId,
    pub organization_id: Option<OrganizationId>,
    pub source: String,
    pub external_event_id: String,
    pub sequence_no: Option<i64>,
    pub payload: serde_json::Value,
    pub payload_sha256: String,
    pub signature_key_version: Option<String>,
}

impl VerifiedInboundWebhook {
    /// Defends persistence adapters if an alternate ingress implementation is
    /// ever composed. This does not replace HMAC verification above.
    pub fn validate(&self) -> Result<()> {
        if self.tenant_id.is_nil() {
            return Err(AppError::invalid_field(
                "tenant_id",
                "required",
                "verified inbound webhook requires a concrete tenant",
            ));
        }
        validate_source(&self.source)?;
        if self.external_event_id.is_empty()
            || self.external_event_id.len() > MAX_EXTERNAL_EVENT_ID_BYTES
            || self.external_event_id.chars().any(char::is_control)
            || self.sequence_no.is_some_and(|sequence| sequence < 0)
            || self.payload_sha256.len() != 64
            || !self
                .payload_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self
                .signature_key_version
                .as_deref()
                .is_some_and(|version| !is_safe_key_version(version))
        {
            return Err(AppError::invalid_field(
                "verified_inbound_webhook",
                "invalid_format",
                "verified inbound webhook metadata is invalid",
            ));
        }
        Ok(())
    }
}

/// Durable result with no provider-secret or payload disclosure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundWebhookDisposition {
    /// The receipt was accepted and an outbox event was atomically recorded.
    Accepted,
    /// A sequence gap was recorded; later contiguous arrivals will release it.
    Deferred,
    /// A previously verified matching delivery was already recorded.
    Duplicate,
}

/// Resolves only a server-owned ingress route. Implementations must return
/// `None` for disabled/deleted routes without revealing tenant information.
#[async_trait]
pub trait InboundWebhookRoutePort: fmt::Debug + Send + Sync {
    async fn route(&self, opaque_route_id: &str) -> Result<Option<InboundWebhookRoute>>;
}

/// Atomic receipt + ordering + transactional-outbox persistence boundary.
#[async_trait]
pub trait InboundWebhookReceiptPort: fmt::Debug + Send + Sync {
    /// Records a signature-verified receipt exactly once. For ordered sources,
    /// this must serialize by `(tenant, source)` and atomically emit every
    /// newly contiguous receipt into the transactional outbox.
    async fn record_verified(
        &self,
        webhook: &VerifiedInboundWebhook,
    ) -> Result<InboundWebhookDisposition>;
}

/// Object-safe ingress boundary used by an HTTP process without exposing the
/// concrete route/key/receipt adapters in its application state.
#[async_trait]
pub trait InboundWebhookIngressPort: fmt::Debug + Send + Sync {
    async fn ingest(
        &self,
        request: InboundWebhookRequest,
        now: &Timestamp,
    ) -> Result<InboundWebhookDisposition>;
}

/// Production ingress coordinator. Its generic ports make the security order
/// testable without requiring a running HTTP or secret-manager service.
#[derive(Debug)]
pub struct InboundWebhookIngress<R, K, P>
where
    R: InboundWebhookRoutePort,
    K: WebhookSecretPort,
    P: InboundWebhookReceiptPort,
{
    routes: R,
    keys: K,
    receipts: P,
    replay_tolerance: Duration,
}

impl<R, K, P> InboundWebhookIngress<R, K, P>
where
    R: InboundWebhookRoutePort,
    K: WebhookSecretPort,
    P: InboundWebhookReceiptPort,
{
    /// Uses the standard five-minute two-sided signature tolerance.
    #[must_use]
    pub fn new(routes: R, keys: K, receipts: P) -> Self {
        Self {
            routes,
            keys,
            receipts,
            replay_tolerance: DEFAULT_REPLAY_TOLERANCE,
        }
    }

    /// Allows an operator-tested deployment policy to tighten (not disable)
    /// replay protection. Zero and excessively broad windows are rejected.
    pub fn with_replay_tolerance(mut self, replay_tolerance: Duration) -> Result<Self> {
        if replay_tolerance.is_zero() || replay_tolerance > Duration::from_secs(3_600) {
            return Err(AppError::invalid_field(
                "replay_tolerance",
                "invalid_range",
                "inbound webhook replay tolerance must be between 1 second and 1 hour",
            ));
        }
        self.replay_tolerance = replay_tolerance;
        Ok(self)
    }

    /// Verifies before JSON parsing and before any durable receipt/outbox call.
    pub async fn ingest(
        &self,
        request: InboundWebhookRequest,
        now: &Timestamp,
    ) -> Result<InboundWebhookDisposition> {
        request.validate_shape()?;
        let route = self
            .routes
            .route(&request.route_id)
            .await?
            .ok_or_else(|| AppError::not_found("inbound_webhook_route", "route unavailable"))?;
        route.validate()?;
        let signature = InboundSignature::parse(&request.signature_header)?;
        match signature.key_version.as_deref() {
            Some(key_version)
                if route.required_key_version.as_deref() == Some(key_version)
                    || route.accepted_key_versions.contains(key_version) => {},
            Some(_) => {
                return Err(AppError::unauthorized(
                    "inbound webhook key version is not accepted",
                ));
            },
            None if route.required_key_version.is_some()
                || !route.accepted_key_versions.is_empty() =>
            {
                return Err(AppError::unauthorized(
                    "inbound webhook key version is required",
                ));
            },
            None => {},
        }
        let secret = self
            .keys
            .resolve_version(
                route.signing_secret_reference_id,
                signature.key_version.as_deref(),
            )
            .await
            .map_err(|_| AppError::unauthorized("inbound webhook authentication unavailable"))?;
        verify_inbound(
            &signature,
            &request.body,
            secret.expose(),
            now,
            self.replay_tolerance,
        )?;
        let payload: serde_json::Value = serde_json::from_slice(&request.body).map_err(|_| {
            AppError::invalid_field(
                "body",
                "invalid_json",
                "authenticated inbound webhook body must be valid JSON",
            )
        })?;
        let payload_sha256 = hex::encode(Sha256::digest(&request.body));
        let verified = VerifiedInboundWebhook {
            tenant_id: route.tenant_id,
            organization_id: route.organization_id,
            source: route.source,
            external_event_id: request.external_event_id,
            sequence_no: request.sequence_no,
            payload,
            payload_sha256,
            signature_key_version: signature.key_version,
        };
        verified.validate()?;
        self.receipts.record_verified(&verified).await
    }
}

#[async_trait]
impl<R, K, P> InboundWebhookIngressPort for InboundWebhookIngress<R, K, P>
where
    R: InboundWebhookRoutePort + 'static,
    K: WebhookSecretPort + 'static,
    P: InboundWebhookReceiptPort + 'static,
{
    async fn ingest(
        &self,
        request: InboundWebhookRequest,
        now: &Timestamp,
    ) -> Result<InboundWebhookDisposition> {
        InboundWebhookIngress::ingest(self, request, now).await
    }
}

fn is_safe_route_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_INGRESS_ROUTE_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn validate_source(source: &str) -> Result<()> {
    if source.is_empty()
        || source.len() > 128
        || !source
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(AppError::invalid_field(
            "source",
            "invalid_format",
            "inbound webhook source must be a 1..=128 character safe token",
        ));
    }
    Ok(())
}

fn is_safe_key_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::webhooks::{compute_signature, sign_header_with_key_version, ResolvedWebhookSecret};

    #[derive(Debug)]
    struct Routes(InboundWebhookRoute);

    #[async_trait]
    impl InboundWebhookRoutePort for Routes {
        async fn route(&self, key: &str) -> Result<Option<InboundWebhookRoute>> {
            Ok((key == "provider-route").then(|| self.0.clone()))
        }
    }

    #[derive(Debug)]
    struct Keys;

    #[async_trait]
    impl WebhookSecretPort for Keys {
        async fn resolve(&self, _reference: SecretReferenceId) -> Result<ResolvedWebhookSecret> {
            ResolvedWebhookSecret::new(b"inbound-test-secret".to_vec())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct VersionKeys(Arc<Mutex<Vec<Option<String>>>>);

    #[async_trait]
    impl WebhookSecretPort for VersionKeys {
        async fn resolve(&self, _reference: SecretReferenceId) -> Result<ResolvedWebhookSecret> {
            ResolvedWebhookSecret::new(b"inbound-test-secret".to_vec())
        }

        async fn resolve_version(
            &self,
            reference: SecretReferenceId,
            key_version: Option<&str>,
        ) -> Result<ResolvedWebhookSecret> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(key_version.map(str::to_owned));
            self.resolve(reference).await
        }
    }

    #[derive(Debug, Clone, Default)]
    struct Receipts(Arc<Mutex<Vec<VerifiedInboundWebhook>>>);

    #[async_trait]
    impl InboundWebhookReceiptPort for Receipts {
        async fn record_verified(
            &self,
            webhook: &VerifiedInboundWebhook,
        ) -> Result<InboundWebhookDisposition> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(webhook.clone());
            Ok(InboundWebhookDisposition::Accepted)
        }
    }

    fn route() -> InboundWebhookRoute {
        InboundWebhookRoute {
            tenant_id: TenantId::new(),
            organization_id: None,
            source: "provider.events".to_owned(),
            signing_secret_reference_id: SecretReferenceId::new(),
            required_key_version: None,
            accepted_key_versions: BTreeSet::new(),
        }
    }

    #[tokio::test]
    async fn signature_failure_creates_no_receipt() {
        let receipts = Receipts::default();
        let ingress = InboundWebhookIngress::new(Routes(route()), Keys, receipts.clone());
        let now = Timestamp::now();
        let result = ingress
            .ingest(
                InboundWebhookRequest {
                    route_id: "provider-route".to_owned(),
                    external_event_id: "evt-1".to_owned(),
                    sequence_no: Some(0),
                    signature_header:
                        "t=1,v1=0000000000000000000000000000000000000000000000000000000000000000"
                            .to_owned(),
                    body: br#"{"ok":true}"#.to_vec(),
                },
                &now,
            )
            .await;
        assert!(result.is_err());
        assert!(receipts
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[tokio::test]
    async fn valid_signature_reaches_receipt_after_verification() {
        let receipts = Receipts::default();
        let ingress = InboundWebhookIngress::new(Routes(route()), Keys, receipts.clone());
        let now = Timestamp::now();
        let body = br#"{"kind":"created"}"#.to_vec();
        let signature = compute_signature(now.to_unix_seconds(), &body, b"inbound-test-secret");
        let outcome = ingress
            .ingest(
                InboundWebhookRequest {
                    route_id: "provider-route".to_owned(),
                    external_event_id: "evt-2".to_owned(),
                    sequence_no: Some(0),
                    signature_header: format!("t={},v1={signature}", now.to_unix_seconds()),
                    body,
                },
                &now,
            )
            .await
            .expect("verified ingress");
        assert_eq!(outcome, InboundWebhookDisposition::Accepted);
        let stored = receipts
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].payload_sha256.len(), 64);
    }

    #[tokio::test]
    async fn current_and_previous_allowlisted_kids_resolve_only_the_named_version() {
        let receipts = Receipts::default();
        let keys = VersionKeys::default();
        let mut configured = route();
        configured.required_key_version = Some("current".to_owned());
        configured.accepted_key_versions =
            BTreeSet::from(["current".to_owned(), "previous".to_owned()]);
        let ingress =
            InboundWebhookIngress::new(Routes(configured), keys.clone(), receipts.clone());
        let now = Timestamp::now();
        let body = br#"{"rotation":true}"#.to_vec();
        for (external_event_id, kid) in [("evt-current", "current"), ("evt-previous", "previous")] {
            ingress
                .ingest(
                    InboundWebhookRequest {
                        route_id: "provider-route".to_owned(),
                        external_event_id: external_event_id.to_owned(),
                        sequence_no: None,
                        signature_header: sign_header_with_key_version(
                            &now,
                            &body,
                            b"inbound-test-secret",
                            Some(kid),
                        ),
                        body: body.clone(),
                    },
                    &now,
                )
                .await
                .expect("allowlisted key version must verify");
        }
        assert_eq!(
            *keys
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec![Some("current".to_owned()), Some("previous".to_owned())]
        );
        assert_eq!(
            receipts
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn unknown_or_absent_kid_under_rotation_never_resolves_or_records() {
        let receipts = Receipts::default();
        let keys = VersionKeys::default();
        let mut configured = route();
        configured.accepted_key_versions =
            BTreeSet::from(["current".to_owned(), "previous".to_owned()]);
        let ingress =
            InboundWebhookIngress::new(Routes(configured), keys.clone(), receipts.clone());
        let now = Timestamp::now();
        let body = br#"{"rotation":true}"#.to_vec();
        for (external_event_id, key_version) in
            [("evt-unknown", Some("unknown")), ("evt-absent", None)]
        {
            let result = ingress
                .ingest(
                    InboundWebhookRequest {
                        route_id: "provider-route".to_owned(),
                        external_event_id: external_event_id.to_owned(),
                        sequence_no: None,
                        signature_header: sign_header_with_key_version(
                            &now,
                            &body,
                            b"inbound-test-secret",
                            key_version,
                        ),
                        body: body.clone(),
                    },
                    &now,
                )
                .await;
            assert!(result.is_err());
        }
        assert!(keys
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
        assert!(receipts
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[test]
    fn route_and_source_validation_reject_control_and_path_like_input() {
        assert!(!is_safe_route_id("../route"));
        assert!(validate_source("provider\nsource").is_err());
    }
}
