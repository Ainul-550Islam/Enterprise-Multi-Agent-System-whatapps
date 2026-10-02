//! Production bearer verification: database-backed API keys.
//!
//! Flow per request (the only raw-secret-bearing code path in the API):
//! 1. Reject tokens not shaped like `mas_<64 hex>` (precise fail-closed
//!    shape check; anything else is not this platform's key family).
//! 2. Take the visible prefix (`mas_` + first 8 chars), query candidates.
//! 3. SHA-256 the raw token and compare digests in constant time — never
//!    length-related loops, never logs with secret material.
//! 4. Lifecycle guards (active, unexpired), stamp `last_used_at`
//!    fire-and-forget, produce `Principal{ ApiKey, subject = key id }`.
//!
//! No caches: keys are revocable and revocation must take effect at the
//! next request (the same property that makes `last_used_at` meaningful).

use async_trait::async_trait;
use mas_persistence::repositories::api_keys::ApiKeyStore;

use crate::auth::{Principal, PrincipalKind, TokenVerifierPort};
use mas_common::constants::{API_KEY_PREFIX, API_KEY_VISIBLE_PREFIX_LEN};
use mas_common::error::AppError;
use mas_common::result::Result;

const UNAUTHENTICATED: &str = "unauthenticated";

/// Database-backed verifier for the production bearer boundary.
#[derive(Debug, Clone)]
pub struct PostgresApiKeyVerifier {
    store: ApiKeyStore,
}

impl PostgresApiKeyVerifier {
    /// Binds a verifier to the shared pool-backed store.
    #[must_use]
    pub fn new(store: ApiKeyStore) -> Self {
        Self { store }
    }

    /// Extracts the well-known public shard of a raw key.
    fn visible_prefix(raw: &str) -> String {
        raw.chars()
            .take(API_KEY_PREFIX.len() + API_KEY_VISIBLE_PREFIX_LEN)
            .collect()
    }

    /// Constant-time hex digest comparison (a timing oracle here would
    /// subdivide the digest space — the comparison never short-circuits).
    fn digests_equal(a: &str, b: &str) -> bool {
        let left = a.as_bytes();
        let right = b.as_bytes();
        if left.len() != right.len() {
            return false;
        }
        let mut acc = 0u8;
        for (x, y) in left.iter().zip(right.iter()) {
            acc |= x ^ y;
        }
        acc == 0
    }

    /// Shape guard: `mas_` + exactly 64 lowercase hex characters.
    fn is_shaped_like_platform_key(raw: &str) -> bool {
        let Some(rest) = raw.strip_prefix(API_KEY_PREFIX) else {
            return false;
        };
        rest.len() == 64
            && rest
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    }
}

#[async_trait]
impl TokenVerifierPort for PostgresApiKeyVerifier {
    async fn verify(&self, token: &str) -> Result<Principal> {
        // All failure paths share ONE error body: no oracle refinement.
        let unauthenticated = || AppError::unauthorized(UNAUTHENTICATED);

        if !Self::is_shaped_like_platform_key(token) {
            return Err(unauthenticated());
        }
        let candidates = self
            .store
            .find_by_prefix(&Self::visible_prefix(token))
            .await
            .map_err(|_| unauthenticated())?;
        let digest = mas_security::api_keys::hash_key(token);
        let Some(row) = candidates
            .into_iter()
            .find(|candidate| Self::digests_equal(&candidate.secret_hash, &digest))
        else {
            return Err(unauthenticated());
        };
        if row.status != "active" {
            return Err(unauthenticated());
        }
        if let Some(expires_at) = row.expires_at {
            if expires_at <= chrono::Utc::now() {
                return Err(unauthenticated());
            }
        }
        // Telemetry best-effort: never let the marker write block authN.
        let _ = self.store.touch_last_used(row.id).await;
        Ok(Principal {
            subject: row.id.to_string(),
            kind: PrincipalKind::ApiKey,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shape_guard_pins_the_contracted_format() {
        assert!(PostgresApiKeyVerifier::is_shaped_like_platform_key(
            &format!("mas_{}", "a".repeat(64))
        ));
        assert!(!PostgresApiKeyVerifier::is_shaped_like_platform_key(
            "mas_short"
        ));
        assert!(!PostgresApiKeyVerifier::is_shaped_like_platform_key(
            &format!("mas_{}", "A".repeat(64))
        ));
        assert!(!PostgresApiKeyVerifier::is_shaped_like_platform_key(
            &format!("posix_{}", "a".repeat(64))
        ));
        assert!(!PostgresApiKeyVerifier::is_shaped_like_platform_key(
            &format!("mas_{}", "a".repeat(63))
        ));
    }

    #[test]
    fn digest_compare_is_exact_and_long_length_safe() {
        let digest = "a".repeat(64);
        assert!(PostgresApiKeyVerifier::digests_equal(&digest, &digest));
        assert!(!PostgresApiKeyVerifier::digests_equal(
            &digest,
            &"a".repeat(63)
        ));
        assert!(!PostgresApiKeyVerifier::digests_equal(
            &digest,
            &"b".repeat(64)
        ));
    }

    #[test]
    fn visible_prefix_formula_matches_api_constants() {
        let token = format!("mas_{}", "deadbeef".repeat(8));
        let prefix = PostgresApiKeyVerifier::visible_prefix(&token);
        assert_eq!(
            prefix.len(),
            API_KEY_PREFIX.len() + API_KEY_VISIBLE_PREFIX_LEN
        );
        assert!(token.starts_with(&prefix));
    }
}
