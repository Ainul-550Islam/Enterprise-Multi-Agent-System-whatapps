//! Security crate: authentication material, secret access control, and the
//! audit trail every security decision must leave behind.
//!
//! # Invariants (normative)
//!
//! 1. **Raw secrets never appear in logs, errors or domain objects.** The
//!    only bytes with authority are [`SecretString`]-wrapped; its `Debug`
//!    output is statically redacted, it zeroizes on drop, and reading it
//!    requires the explicit `expose()` verb.
//! 2. **Secret resolution is gated.** All resolution flows through
//!    [`SecretGate`]: retirement/availability checks, optional TTL caching
//!    and a mandatory audit record of *who resolved what for which
//!    purpose* (never the material itself).
//! 3. **API keys are bearer hashes end-to-end.** Raw keys exist only at
//!    creation time and inside the caller's hand; persistence knows the
//!    SHA-256 digest and the 8-char visible prefix only
//!    ([`ApiKeyService`]). Verification compares in constant time.
//! 4. **Token validation is explicit and narrow.** [`JwtValidator`]
//!    enforces HS256-only verification with issuer/audience/expiry
//!    pinning — `alg: none` and scheme ambiguity are hard failures.
//! 5. **Scopes are the least-privilege surface.** [`ScopeSet`] pattern
//!    matching (`resource:verb`, wildcard tails) is the single rule used by
//!    API keys, tokens and principals.

pub mod api_keys;
pub mod audit;
pub mod principal;
pub mod scopes;
pub mod secret_gate;
pub mod secret_string;
pub mod tokens;

pub use api_keys::{
    ApiKeyService, ApiKeyStorePort, CreatedApiKey, InMemoryApiKeyStore, StoredApiKey,
};
pub use audit::{AuditSinkPort, NoopAuditSink, RecordingAuditSink};
pub use principal::{PrincipalKind, SecurityPrincipal};
pub use scopes::ScopeSet;
pub use secret_gate::{InMemorySecretResolver, SecretGate, SecretResolverPort};
pub use secret_string::{constant_time_eq, SecretString};
pub use tokens::{encode_hs256_for_test, JwtValidator, TokenClaims};
