//! # mas-integrations
//!
//! The integration layer: everything rust-core needs to talk to external
//! systems *safely*, factored so that all I/O sits behind ports and every
//! tenant-visible decision remains deterministic and testable.
//!
//! Layers:
//!
//! * [`manifest`] — versioned **provider manifests**: declarative
//!   descriptors (category, supported auth modes, config key contract).
//!   Manifests are configuration, not credentials.
//! * [`registry`] — the provider-manifest registry and the per-tenant
//!   registry of installed [`mas_domain::Connector`] instances, with
//!   cross-tenant existence hidden.
//! * [`http_guard`] — the **SSRF-hardened egress plane**: [`http_guard::EgressPolicy`]
//!   (scheme/port/host allowlists, resolution pinning), redirect
//!   re-validation of every hop, response size caps, and
//!   [`http_guard::GuardedHttpClient`] wrapping an [`http_guard::HttpClientPort`]. `SafeUrl` is the
//!   first-line guard; this module is the full enforcement layer its
//!   documentation refers to.
//! * [`webhooks`] — HMAC-SHA256 inbound signature verification with replay
//!   windows, and the store-and-forward outbound dispatcher: sign → POST →
//!   record outcome → exponential backoff (honouring `Retry-After`) →
//!   dead-letter, all on top of the domain endpoint circuit breaker.
//! * [`oauth`] — OAuth 2.0 authorization-code + PKCE core: authorization
//!   URLs, one-shot state sessions with TTLs, redacted token bundles, and
//!   the [`oauth::TokenEndpointPort`] trait real provider adapters bind to.
//! * [`connectors`] — the category-driven [`connectors::ConnectorFactory`]:
//!   manifest lookup → auth-mode check → category config validation (with a
//!   blanket no-secret-material-in-config rule) → a persisted
//!   [`mas_domain::Connector`] in `PendingVerification`, plus connection
//!   probes that advance the status lifecycle.
//!
//! Secret material is only ever *referenced* (`SecretReference`) or held in
//! zeroized, redacted wrappers ([`oauth::TokenSecret`]) — never stored in
//! domain objects, connector configs, or errors.

pub mod connectors;
/// Durable lease/fence-safe notification delivery worker and channel ports.
pub mod durable_notifications;
/// Durable OAuth authorization-code state and PKCE orchestration.
pub mod durable_oauth;
/// Durable lease/fence-safe outbound webhook worker; production relay wiring
/// uses this module instead of the in-memory reference dispatcher.
pub mod durable_webhooks;
pub mod http_guard;
/// Signature-first public ingress and durable receipt/outbox boundary.
pub mod inbound_webhooks;
pub mod manifest;
pub mod oauth;
/// Durable fenced OAuth token-material cleanup worker and settlement ports.
pub mod oauth_cleanup;
/// Real guarded OAuth token endpoint adapter.
pub mod oauth_http;
/// Durable callback materialization-intent reconciliation worker and ports.
pub mod oauth_materialization;
/// Static allowlisted OAuth provider profiles and runtime catalog.
pub mod oauth_providers;
/// Durable leased OAuth token refresh worker and settlement ports.
pub mod oauth_refresh;
/// Real guarded HTTP probes for supported provider adapters.
pub mod provider_adapters;
pub mod registry;
/// Canonical concrete Reqwest adapter used beneath the guarded egress port.
pub mod reqwest_egress;
/// Production secret-manager adapters and version-aware webhook secret bridge.
pub mod secrets;
pub mod webhooks;

pub use reqwest_egress::ReqwestEgress;
