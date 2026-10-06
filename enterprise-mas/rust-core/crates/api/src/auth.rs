//! Bearer-token verification. This crate deliberately does NOT own token
//! cryptography: verification is an injected port so the IVF/security token
//! stacks bind at deploy time, and tests bind a fixture verifier.
//!
//! # One principal, one shape
//!
//! Verification returns [`SecurityPrincipal`] from `mas-security` — the same
//! type the worker, the scheduler, the gRPC surface and the Python bridge
//! produce. There is no second principal model in this crate: a second model
//! was the reason scope could be taken from a header instead of from identity,
//! because the two models did not agree on who a caller was.
//!
//! A verifier's job is therefore bigger than "is this token valid". It must
//! also say *where the token lives*: an API key is bound to one tenant and one
//! organization, and that binding is part of authentication, not a detail to
//! be recovered later from a request header.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_security::SecurityPrincipal;

// Re-exported so the rest of the crate names the canonical principal through
// `crate::auth`, and so a future reader cannot reintroduce a local one by
// accident.
pub use mas_security::PrincipalKind;

/// Verification boundary. `Send + Sync + Debug`, object-safe.
#[async_trait]
pub trait TokenVerifierPort: std::fmt::Debug + Send + Sync {
    /// Verifies a raw bearer token and returns the canonical principal.
    ///
    /// The returned principal must carry whatever tenancy binding the
    /// credential itself proves. Returning a principal with no binding is
    /// legitimate only for identities that genuinely have none (the platform
    /// identity, the machine planes).
    ///
    /// Failures surface `401 UNAUTHENTICATED` with no token material in the
    /// error, and every failure mode shares one body so rejections cannot be
    /// used to probe which part of a credential was wrong.
    async fn verify(&self, token: &str) -> Result<SecurityPrincipal>;
}

/// Development/test verifier: an explicit token→principal table. Never
/// ships to production (the binary refuses to start with it unless the
/// `--insecure-dev-auth` flag is passed — see `main.rs`).
#[derive(Debug, Default)]
pub struct StaticTokenVerifier {
    tokens: std::collections::HashMap<String, SecurityPrincipal>,
}

impl StaticTokenVerifier {
    /// Creates the empty (deny-all) table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a token (builder style for fixtures).
    ///
    /// The principal is supplied whole, so a fixture has to be explicit about
    /// the tenancy it is granting — there is no `subject`-only shortcut that
    /// would leave the binding to be filled in from the request.
    #[must_use]
    pub fn with_token(mut self, token: &str, principal: SecurityPrincipal) -> Self {
        self.tokens.insert(token.to_owned(), principal);
        self
    }
}

#[async_trait]
impl TokenVerifierPort for StaticTokenVerifier {
    async fn verify(&self, token: &str) -> Result<SecurityPrincipal> {
        self.tokens
            .get(token)
            .cloned()
            .ok_or_else(|| AppError::unauthorized("invalid or expired bearer token"))
    }
}

/// Parses the `Authorization` header into a bearer token.
///
/// * absent → `Ok(None)` (public routes),
/// * malformed (`Basic …`, empty, control bytes) → `Err(UNAUTHENTICATED)`.
pub fn bearer_token(raw: Option<&str>) -> Result<Option<String>> {
    let Some(value) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .filter(|t| !t.chars().any(char::is_control))
        .ok_or_else(|| AppError::unauthorized("authorization must be `Bearer <token>`"))?;
    if token.len() > 4096 {
        return Err(AppError::unauthorized("bearer token too long"));
    }
    Ok(Some(token.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::TenantId;
    use mas_security::{PrincipalKind, ScopeSet};

    fn principal() -> SecurityPrincipal {
        SecurityPrincipal::new(
            "jane",
            PrincipalKind::User,
            Some(TenantId::new()),
            ScopeSet::denied(),
        )
    }

    #[tokio::test]
    async fn static_verifier_accepts_only_known_tokens() {
        let verifier = StaticTokenVerifier::new().with_token("tok-1", principal());
        let verified = verifier.verify("tok-1").await.expect("valid");
        assert_eq!(verified.actor_id, "jane");
        assert!(verified.tenant_id.is_some());
        assert!(verifier.verify("forged").await.is_err());
    }

    #[test]
    fn bearer_parsing_rejects_garbage() {
        assert_eq!(bearer_token(None).expect("ok"), None);
        assert_eq!(bearer_token(Some("  ")).expect("ok"), None);
        assert_eq!(
            bearer_token(Some("Bearer tok-42")).expect("ok").as_deref(),
            Some("tok-42")
        );
        assert!(bearer_token(Some("Basic dXNlcg==")).is_err());
        assert!(bearer_token(Some("Bearer")).is_err());
    }
}
