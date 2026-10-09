//! PURPOSE: The one security crate — principal, scope, authorization,
//! authentication failures, API keys, secrets, audit and internal-service
//! authentication.
//!
//! Owning layer: shared; every crate depends on this one for identity and
//! authorization decisions.
//!
//! Production responsibility: hold the single `SecurityPrincipal` type, the
//! single `AuthorizationService`, the single `AuthoritativeScope`, the single
//! stable failure taxonomy, and the single internal-service HMAC scheme.
//!
//! Security boundary: this crate is pure decision logic. It performs no I/O
//! and reaches no database, so a decision made here is deterministic,
//! testable, and cannot be widened by a caller who controls storage.
//!
//! It must NOT:
//!   • grow a second principal, authorization, or internal-auth type;
//!   • perform I/O — the directory and the repository live elsewhere;
//!   • expose a raw secret through `Debug`, `Display`, or an error message.

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
pub mod auth_failure;
pub mod authorization;
pub mod internal_service;
pub mod jwks;
pub mod principal;
pub mod scope;
pub mod scopes;
pub mod secret_gate;
pub mod secret_string;
pub mod tokens;

pub use api_keys::{
    ApiKeyService, ApiKeyStorePort, CreatedApiKey, InMemoryApiKeyStore, StoredApiKey,
};
pub use audit::{AuditSinkPort, NoopAuditSink, RecordingAuditSink};
pub use auth_failure::{AuthFailure, AuthFailureCode, AuthFailureKind};
pub use authorization::{
    Action, AuthorizationDecision, AuthorizationRequest, AuthorizationService,
    AuthorizationServicePort, DenyByDefault, ResourceKind,
};
pub use internal_service::{
    sign, verify, InternalServiceIdentity, SERVICE_HEADER, SIGNATURE_HEADER,
};
pub use jwks::{JwksConfig, JwksFetchPort, JwsAlgorithm, RemoteJwksValidator};
pub use principal::{PrincipalKind, SecurityPrincipal, TenantMembership};
pub use scope::{machine_plane_scope, AuthoritativeScope};
pub use scopes::ScopeSet;
pub use secret_gate::{InMemorySecretResolver, SecretGate, SecretResolverPort};
pub use secret_string::{constant_time_eq, SecretString};
pub use tokens::{encode_hs256_for_test, JwtValidator, TokenClaims};
