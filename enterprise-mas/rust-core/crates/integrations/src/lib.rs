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
pub mod http_guard;
pub mod manifest;
pub mod oauth;
pub mod registry;
pub mod webhooks;
