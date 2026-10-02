//! Bearer-token verification. This crate deliberately does NOT own token
//! cryptography: verification is an injected port so the IVF/security token
//! stacks bind at deploy time, and tests bind a fixture verifier.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;

/// What kind of caller a verified token represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    /// Interactive user (sub = user id/handle).
    User,
    /// Machine-to-machine service principal.
    Service,
    /// Long-lived API key principal.
    ApiKey,
}

/// A successfully authenticated caller.
#[derive(Debug, Clone)]
pub struct Principal {
    /// Stable subject claim (user id, service name, key id). Audit-visible.
    pub subject: String,
    /// Caller category → audit actor kind.
    pub kind: PrincipalKind,
}

/// Verification boundary. `Send + Sync + Debug`, object-safe.
#[async_trait]
pub trait TokenVerifierPort: std::fmt::Debug + Send + Sync {
    /// Verifies a raw bearer token; failures surface `401 UNAUTHENTICATED`
    /// with no token material in the error.
    async fn verify(&self, token: &str) -> Result<Principal>;
}

/// Development/test verifier: an explicit token→principal table. Never
/// ships to production (the binary refuses to start with it unless the
/// `--insecure-dev-auth` flag is passed — see `main.rs`).
#[derive(Debug, Default)]
pub struct StaticTokenVerifier {
    tokens: std::collections::HashMap<String, Principal>,
}

impl StaticTokenVerifier {
    /// Creates the empty (deny-all) table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a token (builder style for fixtures).
    #[must_use]
    pub fn with_token(mut self, token: &str, subject: &str, kind: PrincipalKind) -> Self {
        self.tokens.insert(
            token.to_owned(),
            Principal {
                subject: subject.to_owned(),
                kind,
            },
        );
        self
    }
}

#[async_trait]
impl TokenVerifierPort for StaticTokenVerifier {
    async fn verify(&self, token: &str) -> Result<Principal> {
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

    #[tokio::test]
    async fn static_verifier_accepts_only_known_tokens() {
        let verifier = StaticTokenVerifier::new().with_token("tok-1", "jane", PrincipalKind::User);
        let principal = verifier.verify("tok-1").await.expect("valid");
        assert_eq!(principal.subject, "jane");
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
