//! PURPOSE: Production API-key verification — the credential-to-principal
//! boundary.
//!
//! Owning layer: `mas-api` (the process edge).
//!
//! Production responsibility: look a presented API key up by its hash,
//! check expiry and revocation, and build the principal — carrying the
//! tenant and organization the KEY is bound to, not the ones the caller
//! asked for.
//!
//! Security boundary: before this module was corrected the caller's
//! `x-tenant-id` header silently overrode the key's own tenant, so any key
//! could act in any tenant. Verification now carries the key's binding, and
//! the request's headers are only ever hints resolved against it.
//!
//! It must NOT:
//!   • take the tenant or organization from the request;
//!   • accept an expired, revoked, or unknown key;
//!   • return a reason that distinguishes "unknown key" from "revoked key".

//! Production bearer verification: database-backed API keys.
//!
//! Flow per request (the only raw-secret-bearing code path in the API):
//! 1. Reject tokens not shaped like `mas_<64 hex>` (precise fail-closed
//!    shape check; anything else is not this platform's key family).
//! 2. Take the visible prefix (`mas_` + first 8 chars), query candidates.
//! 3. SHA-256 the raw token and compare digests in constant time — never
//!    length-related loops, never logs with secret material.
//! 4. Lifecycle guards (active, unexpired), stamp `last_used_at`
//!    fire-and-forget, and produce a **tenant-bound** principal.
//!
//! # What changed and why
//!
//! The previous version returned `Principal { subject: <key id>, kind: ApiKey }`
//! and threw away `ApiKeyRow.tenant_id`. The caller's `x-tenant-id` header then
//! decided the scope, which meant a valid key minted in tenant A could be
//! presented with `x-tenant-id: B`. Verification now carries the key's own
//! tenant and organization into the principal, so the binding survives
//! authentication (spec Phase 8: keys are tenant-bound).
//!
//! No caches: keys are revocable and revocation must take effect at the
//! next request (the same property that makes `last_used_at` meaningful).

use async_trait::async_trait;
use mas_persistence::repositories::api_keys::ApiKeyStore;

use crate::auth::TokenVerifierPort;
use mas_common::constants::{API_KEY_PREFIX, API_KEY_VISIBLE_PREFIX_LEN};
use mas_common::error::AppError;
use mas_common::ids::UserId;
use mas_common::result::Result;
use mas_security::{PrincipalKind, ScopeSet, SecurityPrincipal, TenantMembership};

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

    /// Lifts the row's scopes into a `ScopeSet`.
    ///
    /// A row whose scopes cannot be parsed is a minting or migration bug, and
    /// the safe reading of it is "no scopes" rather than "all scopes".
    fn scopes_from(row: &mas_persistence::repositories::api_keys::ApiKeyRow) -> ScopeSet {
        serde_json::from_value::<Vec<String>>(row.scopes.clone())
            .ok()
            .and_then(|scopes| ScopeSet::new(scopes).ok())
            .unwrap_or_else(ScopeSet::denied)
    }
}

#[async_trait]
impl TokenVerifierPort for PostgresApiKeyVerifier {
    async fn verify(&self, token: &str) -> Result<SecurityPrincipal> {
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
        // The key row decides the tenant. This is the whole point of the
        // change: a bearer secret is bound to the tenant that minted it, and
        // no header on the request can move it.
        let tenant_id = mas_common::ids::TenantId::from_uuid(row.tenant_id);
        // Telemetry best-effort: never let the marker write block authN. The
        // tenant argument is from the verified row, not request metadata.
        let _ = self.store.touch_last_used(row.id, tenant_id).await;
        let organization_id = mas_common::ids::OrganizationId::from_uuid(row.organization_id);

        let mut principal = SecurityPrincipal::new(
            format!("api-key:{}", row.id),
            PrincipalKind::ApiKey,
            Some(tenant_id),
            Self::scopes_from(&row),
        )
        .with_organization(organization_id)
        .with_role(row.role);

        // The key's own binding is a credential grant, expressed in the same
        // shape the resolver uses for memberships so one code path answers
        // "what role here" for every kind of principal.
        principal
            .memberships
            .push(TenantMembership::new(organization_id, tenant_id, row.role));

        if let Some(user_id) = row.user_id {
            // A key minted by a user is still a key: the actor is the key, and
            // the creating user is recorded for audit only. It never widens
            // the key's authority.
            principal
                .roles
                .push(format!("created_by:{}", UserId::from_uuid(user_id)));
        }

        Ok(principal)
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

    /// A key row whose scopes are unusable must not become a wildcard.
    #[test]
    fn unparseable_scopes_degrade_to_no_scopes() {
        let row = mas_persistence::repositories::api_keys::ApiKeyRow {
            id: uuid::Uuid::now_v7(),
            tenant_id: uuid::Uuid::now_v7(),
            organization_id: uuid::Uuid::now_v7(),
            role: mas_domain::MembershipRole::ServiceAccount,
            user_id: None,
            name: "svc".to_owned(),
            prefix: "mas_deadbeef".to_owned(),
            secret_hash: "a".repeat(64),
            scopes: serde_json::json!("not-an-array"),
            status: "active".to_owned(),
            expires_at: None,
            last_used_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let scopes = PostgresApiKeyVerifier::scopes_from(&row);
        assert!(
            !scopes.permits("executions:read"),
            "a malformed scope blob must not grant anything"
        );
    }
}
