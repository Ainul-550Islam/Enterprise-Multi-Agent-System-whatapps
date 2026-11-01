//! Production OIDC bearer-token verification over a remote JWKS endpoint.
//!
//! API keys and OIDC access tokens are deliberately distinct credential
//! families. This module validates only asymmetric, issuer-owned JWTs and
//! turns them into a membership-bound human principal; it never trusts a
//! token's tenant, organization, or role claim as local authorization data.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mas_common::constants::API_KEY_PREFIX;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_security::{
    JwksFetchPort, PrincipalKind, RemoteJwksValidator, ScopeSet, SecurityPrincipal,
};

use crate::auth::TokenVerifierPort;
use crate::prod_auth::PostgresApiKeyVerifier;

/// Maximum response body accepted from the configured JWKS endpoint.
const MAX_HTTP_JWKS_BODY_BYTES: usize = mas_security::jwks::MAX_JWKS_BODY_BYTES;

/// HTTPS JWKS transport with bounded reads, no redirects, and finite timeouts.
///
/// The endpoint is configuration, not request input. Disabling redirects is
/// still important: a compromised or misconfigured IdP endpoint must not turn
/// token verification into a redirect-following SSRF client.
#[derive(Clone)]
pub struct HttpJwksFetcher {
    client: reqwest::Client,
}

impl std::fmt::Debug for HttpJwksFetcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpJwksFetcher")
            .finish_non_exhaustive()
    }
}

impl HttpJwksFetcher {
    /// Builds a bounded TLS-capable JWKS client.
    pub fn new(timeout: Duration) -> Result<Self> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(AppError::validation(
                "JWKS HTTP timeout must be between one millisecond and sixty seconds",
            ));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(timeout.min(Duration::from_secs(10)))
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("mas-api-jwks/1")
            .build()
            .map_err(|_| AppError::internal("could not construct JWKS HTTP client"))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl JwksFetchPort for HttpJwksFetcher {
    async fn fetch(&self, endpoint: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(endpoint)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| AppError::external_service("oidc_jwks", "request failed"))?;
        if !response.status().is_success() {
            return Err(AppError::external_service(
                "oidc_jwks",
                "endpoint returned a non-success status",
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_HTTP_JWKS_BODY_BYTES as u64)
        {
            return Err(AppError::external_service(
                "oidc_jwks",
                "response exceeded the configured body limit",
            ));
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| AppError::external_service("oidc_jwks", "response read failed"))?;
        if body.len() > MAX_HTTP_JWKS_BODY_BYTES {
            return Err(AppError::external_service(
                "oidc_jwks",
                "response exceeded the configured body limit",
            ));
        }
        Ok(body.to_vec())
    }
}

/// OIDC verifier that maps validated access tokens to local human principals.
///
/// The tenancy resolver reloads memberships from the local directory after
/// authentication. Therefore signed `tenant_id`, `org_id`, and role-shaped
/// claims remain non-authoritative metadata; using one as a local binding
/// would let an external IdP claim bypass tenant membership lifecycle rules.
#[derive(Debug, Clone)]
pub struct OidcJwksTokenVerifier {
    validator: Arc<RemoteJwksValidator>,
}

impl OidcJwksTokenVerifier {
    /// Wraps the shared cache-backed validator.
    #[must_use]
    pub fn new(validator: RemoteJwksValidator) -> Self {
        Self {
            validator: Arc::new(validator),
        }
    }

    /// Wraps an already shared cache-backed validator.
    #[must_use]
    pub fn from_shared(validator: Arc<RemoteJwksValidator>) -> Self {
        Self { validator }
    }

    fn principal_from_claims(claims: mas_security::TokenClaims) -> Result<SecurityPrincipal> {
        if claims.subject.trim().is_empty()
            || claims.subject.len() > 512
            || claims.subject.chars().any(char::is_control)
        {
            return Err(AppError::unauthorized("token subject is invalid"));
        }
        let scopes = ScopeSet::new(claims.scopes)
            .map_err(|_| AppError::unauthorized("token scopes are invalid"))?;
        Ok(
            SecurityPrincipal::new(claims.subject, PrincipalKind::User, None, scopes)
                .with_roles(claims.roles)
                .with_expiry(claims.expires_at),
        )
    }
}

#[async_trait]
impl TokenVerifierPort for OidcJwksTokenVerifier {
    async fn verify(&self, token: &str) -> Result<SecurityPrincipal> {
        let claims = self.validator.validate(token).await?;
        Self::principal_from_claims(claims)
    }
}

/// Production bearer multiplexer for the two supported credential families.
///
/// The visible API-key prefix is a non-secret discriminator. Every other
/// bearer value is treated as an external OIDC compact JWS; neither verifier
/// can fall back to the other after it rejects a credential.
#[derive(Debug, Clone)]
pub struct ProductionTokenVerifier {
    api_keys: PostgresApiKeyVerifier,
    oidc: OidcJwksTokenVerifier,
}

impl ProductionTokenVerifier {
    /// Combines database-backed API keys and pinned OIDC JWT verification.
    #[must_use]
    pub const fn new(api_keys: PostgresApiKeyVerifier, oidc: OidcJwksTokenVerifier) -> Self {
        Self { api_keys, oidc }
    }
}

#[async_trait]
impl TokenVerifierPort for ProductionTokenVerifier {
    async fn verify(&self, token: &str) -> Result<SecurityPrincipal> {
        if token.starts_with(API_KEY_PREFIX) {
            self.api_keys.verify(token).await
        } else {
            self.oidc.verify(token).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oidc_claim_tenant_and_organization_never_become_local_bindings() {
        let tenant = mas_common::ids::TenantId::new();
        let organization = mas_common::ids::OrganizationId::new();
        let claims = mas_security::TokenClaims {
            subject: "user-42".to_owned(),
            jwt_id: None,
            issuer: Some("https://issuer.example.test".to_owned()),
            audiences: vec!["mas-api".to_owned()],
            expires_at: mas_common::timestamps::Timestamp::now()
                .checked_add(Duration::from_secs(60))
                .expect("future"),
            not_before: None,
            issued_at: None,
            tenant_id: Some(tenant),
            organization_id: Some(organization),
            roles: vec!["platform-admin".to_owned()],
            scopes: vec!["executions:read".to_owned()],
            extras: serde_json::Map::new(),
        };
        let principal = OidcJwksTokenVerifier::principal_from_claims(claims).expect("principal");
        assert_eq!(principal.kind, PrincipalKind::User);
        assert_eq!(principal.tenant_id, None);
        assert_eq!(principal.organization_id, None);
        assert!(principal.memberships.is_empty());
        assert!(principal.permits("executions:read"));
    }
}
