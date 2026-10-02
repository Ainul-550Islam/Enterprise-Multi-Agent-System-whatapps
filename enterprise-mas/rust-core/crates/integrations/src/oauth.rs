//! OAuth 2.0 authorization-code + PKCE core.
//!
//! This module owns the pure, deterministic parts of an OAuth flow:
//!
//! * [`OAuth2Config`] — validated client configuration (TLS-only endpoints).
//! * [`PkcePair`] — RFC 7636 verifier/challenge generation and
//!   constant-time verification.
//! * [`OAuthFlowStore`] — one-shot, TTL-bound `state` sessions bound to a
//!   tenant + connector, preventing state replay.
//! * [`TokenBundle`]/[`TokenSecret`] — redacted, zeroized token material.
//!   Tokens are never logged, serialized, or stored in domain objects.
//! * [`OAuthClient`] — drives the flow from authorization URL to callback,
//!   delegating the actual provider HTTPS call to [`TokenEndpointPort`].
//!
//! Real provider adapters (which travel through the guarded egress client)
//! implement [`TokenEndpointPort`] outside this crate; tests use fakes.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::value_objects::SafeUrl;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;
use zeroize::Zeroize;

/// Verifier entropy in bytes (RFC 7636 §4.1; 32 bytes encode to exactly
/// 43 base64url characters).
pub const PKCE_VERIFIER_BYTES: usize = 32;
/// Maximum lifetime of an authorization `state` session.
pub const AUTH_SESSION_TTL_SECS: u64 = 600;
/// Proactive expiry window for tokens: a bundle counts as expired
/// this many seconds BEFORE its stated expiry, absorbing clock skew and
/// in-flight request time.
pub const TOKEN_EXPIRY_SKEW_SECS: u64 = 60;

fn urlsafe_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn sha256_hex(data: &[u8]) -> [u8; 32] {
    use sha2::Digest as _;
    sha2::Sha256::digest(data).into()
}

/// Secret string material: redacted `Debug`, zeroized on drop, never
/// serialized. Access is intentional and explicit via [`TokenSecret::expose_secret`].
pub struct TokenSecret(String);

impl TokenSecret {
    /// Wraps material.
    #[must_use]
    pub const fn new(secret: String) -> Self {
        Self(secret)
    }

    /// Explicit, visible-at-call-site access to the raw material.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl Clone for TokenSecret {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl fmt::Debug for TokenSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenSecret([redacted])")
    }
}

impl Drop for TokenSecret {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

/// An RFC 7636 PKCE verifier/challenge pair (S256 only).
#[derive(Debug)]
pub struct PkcePair {
    verifier: TokenSecret,
    /// base64url(SHA-256(verifier)); 43 characters.
    challenge: String,
}

impl PkcePair {
    /// Generates a fresh pair from CSPRNG bytes.
    #[must_use]
    pub fn generate() -> Self {
        use rand::RngCore as _;
        let mut bytes = [0u8; PKCE_VERIFIER_BYTES];
        rand::rng().fill_bytes(&mut bytes);
        let verifier = urlsafe_encode(&bytes);
        debug_assert_eq!(
            verifier.len(),
            43,
            "32 bytes always encode to 43 base64url chars"
        );
        Self {
            challenge: Self::challenge_for(&verifier),
            verifier: TokenSecret::new(verifier),
        }
    }

    /// Computes the S256 challenge for `verifier`.
    #[must_use]
    pub fn challenge_for(verifier: &str) -> String {
        urlsafe_encode(&sha256_hex(verifier.as_bytes()))
    }

    /// Constant-time verification of a candidate verifier against this
    /// pair's challenge.
    #[must_use]
    pub fn verify(&self, candidate_verifier: &str) -> bool {
        let expected = Self::challenge_for(candidate_verifier);
        let a = expected.as_bytes();
        let b = self.challenge.as_bytes();
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }

    /// The challenge to place in the authorization request.
    #[must_use]
    pub fn challenge(&self) -> &str {
        &self.challenge
    }

    /// The verifier (sensitive; consumed once at token exchange).
    #[must_use]
    pub fn verifier(&self) -> &TokenSecret {
        &self.verifier
    }
}

/// Validated OAuth 2.0 client configuration for one provider.
#[derive(Debug, Clone)]
pub struct OAuth2Config {
    pub client_id: String,
    pub authorization_endpoint: SafeUrl,
    pub token_endpoint: SafeUrl,
    pub redirect_uri: SafeUrl,
    pub scopes: BTreeSet<String>,
}

impl OAuth2Config {
    /// Creates a config; all endpoints MUST be TLS.
    pub fn new(
        client_id: impl Into<String>,
        authorization_endpoint: SafeUrl,
        token_endpoint: SafeUrl,
        redirect_uri: SafeUrl,
        scopes: BTreeSet<String>,
    ) -> Result<Self> {
        let client_id = client_id.into();
        mas_common::validation::validate_length("client_id", &client_id, 1, 256)?;
        if !client_id.chars().all(|c| c.is_ascii_graphic() && c != ' ') {
            return Err(AppError::invalid_field(
                "client_id",
                "invalid_format",
                "client_id must be printable and contain no spaces",
            ));
        }
        for (field, url) in [
            ("authorization_endpoint", &authorization_endpoint),
            ("token_endpoint", &token_endpoint),
            ("redirect_uri", &redirect_uri),
        ] {
            if !url.is_tls() {
                return Err(AppError::invalid_field(
                    field,
                    "tls_required",
                    format!("{field} must use https"),
                ));
            }
        }
        for scope in &scopes {
            if scope.is_empty()
                || scope.len() > 128
                || !scope.chars().all(|c| {
                    c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-' | '+' | '/')
                })
            {
                return Err(AppError::invalid_field(
                    "scopes",
                    "invalid_scope_token",
                    format!("invalid scope {scope:?}"),
                ));
            }
        }
        Ok(Self {
            client_id,
            authorization_endpoint,
            token_endpoint,
            redirect_uri,
            scopes,
        })
    }

    /// Builds the authorization URL for a fresh `state` + PKCE challenge.
    pub fn authorization_url(&self, state: &str, pkce: &PkcePair) -> Result<String> {
        let mut url = url::Url::parse(self.authorization_endpoint.as_str()).map_err(|err| {
            AppError::internal(format!("validated SafeUrl failed re-parse: {err}"))
        })?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("response_type", "code");
            query.append_pair("client_id", &self.client_id);
            query.append_pair("redirect_uri", self.redirect_uri.as_str());
            query.append_pair("state", state);
            if !self.scopes.is_empty() {
                let joined = self.scopes.iter().cloned().collect::<Vec<_>>().join(" ");
                query.append_pair("scope", &joined);
            }
            query.append_pair("code_challenge", pkce.challenge());
            query.append_pair("code_challenge_method", "S256");
        }
        Ok(url.into())
    }
}

/// Forge-resistant, unguessable authorization `state` (32 CSPRNG bytes as
/// 64 lowercase hex characters).
#[must_use]
pub fn generate_state() -> String {
    use rand::RngCore as _;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// One pending authorization: everything the `state` token stands for.
pub struct PendingAuthorization {
    pub tenant: TenantId,
    pub connector: Option<ConnectorId>,
    pkce_verifier: TokenSecret,
    pub initiated_at: Timestamp,
}

impl PendingAuthorization {
    /// The stored verifier (consumed at exchange).
    #[must_use]
    pub fn pkce_verifier(&self) -> &TokenSecret {
        &self.pkce_verifier
    }
}

impl fmt::Debug for PendingAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingAuthorization")
            .field("tenant", &self.tenant)
            .field("connector", &self.connector)
            .field("pkce_verifier", &"[redacted]")
            .field("initiated_at", &self.initiated_at)
            .finish()
    }
}

/// One-shot authorization sessions keyed by `state` with a bounded TTL.
#[derive(Debug, Default)]
pub struct OAuthFlowStore {
    sessions: Mutex<BTreeMap<String, PendingAuthorization>>,
}

impl OAuthFlowStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a pending authorization.
    pub fn begin(&self, state: &str, pending: PendingAuthorization) -> Result<()> {
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if sessions.contains_key(state) {
            return Err(AppError::internal(
                "state collision — entropy failure or replay probe",
            ));
        }
        sessions.insert(state.to_owned(), pending);
        Ok(())
    }

    /// Consumes `state` exactly once. Unknown/used states and expired
    /// sessions both reject (expired sessions are also removed).
    pub fn try_complete(&self, state: &str, now: &Timestamp) -> Result<PendingAuthorization> {
        let pending = self
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(state)
            .ok_or_else(|| {
                AppError::invalid_field(
                    "state",
                    "unknown_or_used",
                    "unknown state — already used or never initiated",
                )
            })?;
        let age = now
            .duration_since(&pending.initiated_at)
            .map(|d| d.as_secs())
            .unwrap_or(u64::MAX);
        if age > AUTH_SESSION_TTL_SECS {
            return Err(AppError::unauthorized("authorization session expired"));
        }
        Ok(pending)
    }

    /// Number of pending sessions.
    #[must_use]
    pub fn len(&self) -> usize {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Whether no sessions are pending.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The provider HTTPS endpoint, abstracted. Implementations MUST go
/// through the guarded egress client; secrets pass as `&str` parameters
/// and are never retained by the transport.
#[async_trait]
pub trait TokenEndpointPort: fmt::Debug + Send + Sync {
    /// `grant_type=authorization_code` exchange with PKCE verifier.
    async fn exchange_code(
        &self,
        config: &OAuth2Config,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenBundle>;
    /// `grant_type=refresh_token`.
    async fn refresh(&self, config: &OAuth2Config, refresh_token: &str) -> Result<TokenBundle>;
}

/// Redacted token material returned by a token endpoint.
pub struct TokenBundle {
    access_token: TokenSecret,
    token_type: String,
    /// Absolute expiry (None = non-expiring per provider contract).
    expires_at: Option<Timestamp>,
    refresh_token: Option<TokenSecret>,
    scopes: Vec<String>,
}

impl TokenBundle {
    /// Builds a bundle from provider response parts. `expires_in_secs` is
    /// the provider's lifetime from `now`.
    pub fn from_parts(
        token_type: impl Into<String>,
        access_token: impl Into<String>,
        refresh_token: Option<String>,
        expires_in_secs: Option<u64>,
        scopes: Vec<String>,
        now: &Timestamp,
    ) -> Result<Self> {
        let token_type = token_type.into();
        let access_token = access_token.into();
        if access_token.is_empty() {
            return Err(AppError::validation(
                "provider returned an empty access token",
            ));
        }
        mas_common::validation::validate_length("token_type", &token_type, 1, 64)?;
        let expires_at = match expires_in_secs {
            Some(secs) => Some(
                now.checked_add(Duration::from_secs(secs))
                    .ok_or_else(|| AppError::validation("provider expiry overflows the clock"))?,
            ),
            None => None,
        };
        Ok(Self {
            access_token: TokenSecret::new(access_token),
            token_type,
            expires_at,
            refresh_token: refresh_token.map(TokenSecret::new),
            scopes,
        })
    }

    /// The access token secret.
    #[must_use]
    pub const fn access_token(&self) -> &TokenSecret {
        &self.access_token
    }

    /// Provider token type (typically `Bearer`).
    #[must_use]
    pub fn token_type(&self) -> &str {
        &self.token_type
    }

    /// The refresh token secret, when the provider issued one.
    #[must_use]
    pub const fn refresh_token(&self) -> Option<&TokenSecret> {
        self.refresh_token.as_ref()
    }

    /// Granted scopes.
    #[must_use]
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    /// Whether the bundle should be treated as expired at `now`
    /// (a [`TOKEN_EXPIRY_SKEW_SECS`] proactive window applies).
    #[must_use]
    pub fn is_expired(&self, now: &Timestamp) -> bool {
        self.expires_at.is_some_and(|expiry| {
            match expiry.checked_sub(Duration::from_secs(TOKEN_EXPIRY_SKEW_SECS)) {
                Some(effective) => !now.is_before(&effective),
                None => true,
            }
        })
    }
}

impl fmt::Debug for TokenBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenBundle")
            .field("token_type", &self.token_type)
            .field("access_token", &"[redacted]")
            .field("expires_at", &self.expires_at)
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .field("scopes", &self.scopes)
            .finish()
    }
}

/// Drives authorization-code + PKCE flows from start to callback.
#[derive(Debug)]
pub struct OAuthClient<S: TokenEndpointPort> {
    config: OAuth2Config,
    flows: OAuthFlowStore,
    endpoint: S,
}

impl<S: TokenEndpointPort> OAuthClient<S> {
    /// Builds a client.
    #[must_use]
    pub fn new(config: OAuth2Config, endpoint: S) -> Self {
        Self {
            config,
            flows: OAuthFlowStore::new(),
            endpoint,
        }
    }

    /// The configuration.
    #[must_use]
    pub const fn config(&self) -> &OAuth2Config {
        &self.config
    }

    /// The flow store (introspection/metrics).
    #[must_use]
    pub const fn flows(&self) -> &OAuthFlowStore {
        &self.flows
    }

    /// Step 1: start an authorization. Returns (authorization URL, state).
    /// The PKCE verifier is retained in the flow store under `state`.
    pub fn authorize_url(
        &self,
        tenant: TenantId,
        connector: Option<ConnectorId>,
        now: &Timestamp,
    ) -> Result<(String, String)> {
        let pkce = PkcePair::generate();
        let state = generate_state();
        let url = self.config.authorization_url(&state, &pkce)?;
        self.flows.begin(
            &state,
            PendingAuthorization {
                tenant,
                connector,
                pkce_verifier: TokenSecret::new(pkce.verifier().expose_secret().to_owned()),
                initiated_at: *now,
            },
        )?;
        Ok((url, state))
    }

    /// Step 2: complete a callback. Any provider `error` rejects first
    /// (consuming the session); success exchanges the code using the stored
    /// verifier. `state` can be used exactly once.
    pub async fn handle_callback(
        &self,
        state: &str,
        code: Option<&str>,
        error: Option<&str>,
        now: &Timestamp,
    ) -> Result<TokenBundle> {
        let pending = self.flows.try_complete(state, now)?;
        if let Some(provider_error) = error {
            return Err(AppError::unauthorized(format!(
                "provider denied authorization: {}",
                sanitize(provider_error)
            )));
        }
        let code = code.ok_or_else(|| {
            AppError::invalid_field(
                "code",
                "required",
                "authorization code missing from callback",
            )
        })?;
        self.endpoint
            .exchange_code(&self.config, code, pending.pkce_verifier().expose_secret())
            .await
            .map_err(|err| err.with_context("token exchange failed"))
    }

    /// Refresh an existing bundle.
    pub async fn refresh(&self, bundle: &TokenBundle) -> Result<TokenBundle> {
        let refresh = bundle.refresh_token().ok_or_else(|| {
            AppError::invalid_field(
                "refresh_token",
                "not_applicable",
                "bundle has no refresh token",
            )
        })?;
        self.endpoint
            .refresh(&self.config, refresh.expose_secret())
            .await
            .map_err(|err| err.with_context("token refresh failed"))
    }
}

/// Single-line, length-capped error text for durable/user-visible surfaces.
fn sanitize(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(160)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet as Set;

    fn cfg() -> OAuth2Config {
        let mut scopes = Set::new();
        scopes.insert("read:executions".to_owned());
        scopes.insert("write:webhooks".to_owned());
        OAuth2Config::new(
            "client-42",
            SafeUrl::parse("https://auth.example.test/authorize").expect("url"),
            SafeUrl::parse("https://auth.example.test/token").expect("url"),
            SafeUrl::parse("https://app.example.test/oauth/callback").expect("url"),
            scopes,
        )
        .expect("config")
    }

    #[test]
    fn pkce_pairs_meet_rfc7636_and_verify_constant_time() {
        let pair = PkcePair::generate();
        assert_eq!(pair.challenge().len(), 43);
        assert!(!pair.challenge().contains(['+', '/', '=']));
        assert!(pair.verify(pair.verifier().expose_secret()));
        assert!(!pair.verify("definitely-not-the-verifier"));
        // RFC 7636 appendix B reference vector.
        let known = PkcePair::challenge_for("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk");
        assert_eq!(known, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    #[test]
    fn authorization_url_carries_all_required_parameters() {
        let config = cfg();
        let pkce = PkcePair::generate();
        let url = config.authorization_url("state-abc", &pkce).expect("url");
        let parsed = url::Url::parse(&url).expect("parse built url");
        let params: std::collections::BTreeMap<_, _> = parsed
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(
            params.get("response_type").map(String::as_str),
            Some("code")
        );
        assert_eq!(
            params.get("client_id").map(String::as_str),
            Some("client-42")
        );
        assert_eq!(params.get("state").map(String::as_str), Some("state-abc"));
        assert_eq!(
            params.get("redirect_uri").map(String::as_str),
            Some("https://app.example.test/oauth/callback")
        );
        assert_eq!(
            params.get("code_challenge").map(String::as_str),
            Some(pkce.challenge())
        );
        assert_eq!(
            params.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
        let scope = params.get("scope").expect("scope").clone();
        assert!(scope.contains("read:executions") && scope.contains("write:webhooks"));

        // TLS is enforced on every endpoint kind.
        let insecure = OAuth2Config::new(
            "client-42",
            SafeUrl::parse("http://auth.example.test/authorize").expect("url"),
            config.token_endpoint.clone(),
            config.redirect_uri.clone(),
            Set::new(),
        );
        assert!(insecure.is_err(), "plain http endpoints reject");
    }

    #[derive(Debug)]
    struct FakeTokenEndpoint {
        calls: Mutex<Vec<(String, String)>>,
        expires_in: Option<u64>,
    }

    #[async_trait]
    impl TokenEndpointPort for FakeTokenEndpoint {
        async fn exchange_code(
            &self,
            _config: &OAuth2Config,
            code: &str,
            verifier: &str,
        ) -> Result<TokenBundle> {
            self.calls
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((code.to_owned(), verifier.to_owned()));
            let now = Timestamp::now();
            TokenBundle::from_parts(
                "Bearer",
                "at-live-material",
                Some("rt-material".to_owned()),
                self.expires_in,
                vec!["read:executions".to_owned()],
                &now,
            )
        }

        async fn refresh(
            &self,
            _config: &OAuth2Config,
            refresh_token: &str,
        ) -> Result<TokenBundle> {
            assert!(!refresh_token.is_empty());
            let now = Timestamp::now();
            TokenBundle::from_parts("Bearer", "at-refreshed", None, Some(3600), vec![], &now)
        }
    }

    #[tokio::test]
    async fn flows_are_one_shot_and_ttl_bound() {
        let tenant = TenantId::new();
        let client = OAuthClient::new(
            cfg(),
            FakeTokenEndpoint {
                calls: Mutex::new(Vec::new()),
                expires_in: Some(3600),
            },
        );
        let start = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");

        let (url, state) = client
            .authorize_url(tenant, None, &start)
            .expect("authorize_url");
        assert!(url.contains("state="));
        assert_eq!(client.flows().len(), 1);

        let bundle = client
            .handle_callback(&state, Some("auth-code-1"), None, &start)
            .await
            .expect("exchange succeeds");
        assert_eq!(bundle.token_type(), "Bearer");
        assert!(!bundle.is_expired(&start));
        let debugged = format!("{bundle:?}");
        assert!(
            !debugged.contains("at-live-material"),
            "token material never in Debug"
        );
        assert!(debugged.contains("[redacted]"));

        assert!(client.flows().is_empty());
        assert!(
            client
                .handle_callback(&state, Some("auth-code-1"), None, &start)
                .await
                .is_err(),
            "state is single-use"
        );

        // Expired session rejects at completion time.
        let (_, stale_state) = client
            .authorize_url(tenant, None, &start)
            .expect("second start");
        let after_ttl = start
            .checked_add(Duration::from_secs(AUTH_SESSION_TTL_SECS + 1))
            .expect("ts");
        assert!(
            client
                .handle_callback(&stale_state, Some("code"), None, &after_ttl)
                .await
                .is_err(),
            "stale sessions reject"
        );

        // Provider denial consumes the session and surfaces sanitized.
        let (_, denied_state) = client
            .authorize_url(tenant, None, &start)
            .expect("third start");
        let denied = client
            .handle_callback(
                &denied_state,
                None,
                Some("access_denied\nwith control chars"),
                &start,
            )
            .await;
        assert!(denied.is_err());

        // The fake captured a standards-shaped verifier at exchange time.
        let calls = client_endpoint_calls(&client);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "auth-code-1");
        assert_eq!(calls[0].1.len(), 43, "PKCE verifier forwarded untouched");
    }

    #[tokio::test]
    async fn expiry_skew_makes_tokens_stale_before_their_stated_end() {
        let now = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let bundle =
            TokenBundle::from_parts("Bearer", "at", None, Some(120), vec![], &now).expect("bundle");
        let inside = now
            .checked_add(Duration::from_secs(120 - super::TOKEN_EXPIRY_SKEW_SECS - 5))
            .expect("ts");
        assert!(!bundle.is_expired(&inside), "still effectively valid");
        let edge = now
            .checked_add(Duration::from_secs(120 - super::TOKEN_EXPIRY_SKEW_SECS + 5))
            .expect("ts");
        assert!(bundle.is_expired(&edge), "skew window counts as expired");

        let client = OAuthClient::new(
            cfg(),
            FakeTokenEndpoint {
                calls: Mutex::new(Vec::new()),
                expires_in: None,
            },
        );
        assert!(
            client.refresh(&bundle).await.is_err(),
            "no refresh token on this bundle"
        );
        let with_refresh = TokenBundle::from_parts(
            "Bearer",
            "at",
            Some("rt".to_owned()),
            Some(3600),
            vec![],
            &now,
        )
        .expect("bundle");
        let refreshed = client.refresh(&with_refresh).await.expect("refresh");
        assert_eq!(refreshed.access_token().expose_secret(), "at-refreshed");
    }

    fn client_endpoint_calls(client: &OAuthClient<FakeTokenEndpoint>) -> Vec<(String, String)> {
        client
            .endpoint
            .calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}
