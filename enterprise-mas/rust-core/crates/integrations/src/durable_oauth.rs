//! PURPOSE: Durable OAuth 2.0 authorization-code + PKCE orchestration ports.
//!
//! Owning layer: `mas-integrations` application/infrastructure boundary.
//!
//! Production role: replace process-local OAuth state with a one-shot durable
//! state hash record and external-secret-manager PKCE verifier reference. It
//! drives a real token endpoint port after atomic state consumption; no server
//! restart can make a callback silently succeed or lose its replay protection.
//!
//! Security boundary: raw OAuth state and code verifier never persist in SQL;
//! state is SHA-256 hashed and the verifier is held only by a PKCE secret port.
//! Callback state is consumed before provider error/code processing, so denied,
//! malformed, and successful callbacks are all non-replayable.
//!
//! Data ownership: a durable state store owns metadata and one-shot status;
//! the secret manager owns verifier/token material; provider systems own codes.
//!
//! Prohibited responsibilities: browser redirects, token logging, raw token
//! persistence, accepting callback tenant IDs from request data, or falling
//! back to the in-memory `OAuthFlowStore` in production.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, CredentialId, SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::SecretReference;
use sha2::{Digest as _, Sha256};

use crate::oauth::{
    generate_state, OAuth2Config, PkcePair, TokenBundle, TokenEndpointPort, TokenSecret,
    AUTH_SESSION_TTL_SECS,
};
use crate::oauth_providers::OAuthProviderCatalogPort;

/// Opaque locator persisted for OAuth PKCE material. It is not a Vault route:
/// the material adapter derives its operator-configured external path from this
/// reference id at the I/O boundary.
#[must_use]
pub fn oauth_pkce_reference_locator(reference_id: SecretReferenceId) -> String {
    format!("oauth-pkce/{reference_id}")
}

/// Opaque locator persisted for OAuth access/refresh material. It is not a
/// Vault route: the material adapter derives its operator-configured external
/// path from this reference id at the I/O boundary.
#[must_use]
pub fn oauth_token_reference_locator(reference_id: SecretReferenceId) -> String {
    format!("oauth-token/{reference_id}")
}

/// Durable metadata for a pending authorization. Every field is safe to audit
/// except the hash's relationship to a live callback, which is never logged.
#[derive(Debug, Clone)]
pub struct DurableAuthorizationState {
    pub id: uuid::Uuid,
    pub tenant_id: TenantId,
    pub connector_id: Option<ConnectorId>,
    pub provider: String,
    /// Lowercase SHA-256 hex of the raw browser state.
    pub state_hash: String,
    /// Canonical secret reference holding the PKCE verifier until one-shot use.
    pub pkce_verifier_secret_id: SecretReferenceId,
    pub redirect_uri: String,
    pub requested_scopes: BTreeSet<String>,
    pub expires_at: Timestamp,
    pub created_at: Timestamp,
}

impl DurableAuthorizationState {
    /// Validates safe durable state metadata before persistence.
    pub fn validate(&self) -> Result<()> {
        if self.tenant_id.is_nil() || self.pkce_verifier_secret_id.is_nil() {
            return Err(AppError::invalid_field(
                "oauth_state",
                "required",
                "OAuth state must have concrete tenant and PKCE secret reference ids",
            ));
        }
        if self.provider.is_empty()
            || self.provider.len() > 64
            || !self.provider.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            })
        {
            return Err(AppError::invalid_field(
                "provider",
                "invalid_format",
                "OAuth provider keys must be 1..=64 lowercase token characters",
            ));
        }
        if self.state_hash.len() != 64
            || !self
                .state_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(AppError::invalid_field(
                "state_hash",
                "invalid_format",
                "OAuth state hash must be SHA-256 lowercase hex",
            ));
        }
        if self.redirect_uri.is_empty() || self.redirect_uri.len() > 2_048 {
            return Err(AppError::invalid_field(
                "redirect_uri",
                "invalid_format",
                "OAuth redirect URI must be 1..=2048 characters",
            ));
        }
        if !self.expires_at.is_after(&self.created_at) {
            return Err(AppError::invalid_field(
                "expires_at",
                "invalid_range",
                "OAuth state expiry must be after creation",
            ));
        }
        Ok(())
    }
}

/// Stores and atomically consumes durable state hashes.
#[async_trait]
pub trait OAuthAuthorizationStatePort: fmt::Debug + Send + Sync {
    /// Inserts a fresh state; a hash collision is a hard conflict.
    async fn begin(&self, state: &DurableAuthorizationState) -> Result<()>;
    /// Atomically marks a non-expired state consumed and returns its metadata.
    /// `None` conflates unknown, expired, and already-consumed state values.
    async fn consume(
        &self,
        state_hash: &str,
        now: &Timestamp,
    ) -> Result<Option<DurableAuthorizationState>>;
}

/// Stores and consumes one PKCE verifier through the production secret manager.
#[async_trait]
pub trait OAuthPkceSecretPort: fmt::Debug + Send + Sync {
    /// Stores `verifier` with the same expiry as its durable state and returns
    /// a tenant-owned canonical reference id.
    async fn store_verifier(
        &self,
        tenant_id: TenantId,
        verifier: &TokenSecret,
        expires_at: Timestamp,
    ) -> Result<SecretReferenceId>;
    /// Atomically consumes a verifier reference. `None` means it is unknown,
    /// expired, or already consumed and must reject the callback.
    async fn consume_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<Option<TokenSecret>>;
    /// Irreversibly discards an unconsumed verifier after its state cannot
    /// complete (for example a provider denial or a failed state insert).
    /// Implementations must be idempotent: cleanup races and retries must not
    /// make a callback appear successful.
    async fn discard_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()>;
}

// Trait-object composition is intentional at the API bootstrap boundary. These
// delegations preserve the narrow port contracts while allowing one immutable
// OAuth service to select a provider configuration per callback.
#[async_trait]
impl<T> OAuthAuthorizationStatePort for Arc<T>
where
    T: OAuthAuthorizationStatePort + ?Sized,
{
    async fn begin(&self, state: &DurableAuthorizationState) -> Result<()> {
        (**self).begin(state).await
    }

    async fn consume(
        &self,
        state_hash: &str,
        now: &Timestamp,
    ) -> Result<Option<DurableAuthorizationState>> {
        (**self).consume(state_hash, now).await
    }
}

#[async_trait]
impl<T> OAuthPkceSecretPort for Arc<T>
where
    T: OAuthPkceSecretPort + ?Sized,
{
    async fn store_verifier(
        &self,
        tenant_id: TenantId,
        verifier: &TokenSecret,
        expires_at: Timestamp,
    ) -> Result<SecretReferenceId> {
        (**self)
            .store_verifier(tenant_id, verifier, expires_at)
            .await
    }

    async fn consume_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<Option<TokenSecret>> {
        (**self).consume_verifier(tenant_id, reference_id).await
    }

    async fn discard_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        (**self).discard_verifier(tenant_id, reference_id).await
    }
}

/// External secret-manager material operations needed by a durable PKCE
/// adapter. Metadata insertion/tenant ownership remains in persistence; this
/// port is deliberately unable to query arbitrary secret-manager paths.
#[async_trait]
pub trait OAuthPkceMaterialPort: fmt::Debug + Send + Sync {
    /// Writes a one-time verifier and returns its canonical metadata reference.
    async fn write_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
        verifier: &TokenSecret,
        expires_at: Timestamp,
    ) -> Result<SecretReference>;
    /// Reads and removes a verifier represented by canonical metadata.
    async fn consume_verifier(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<Option<TokenSecret>>;
    /// Removes a verifier that cannot complete. It is idempotent.
    async fn delete_verifier(&self, tenant_id: TenantId, reference: &SecretReference)
        -> Result<()>;
    /// Removes a deterministic verifier path when metadata creation was rolled
    /// back and therefore no reference row can be looked up.
    async fn delete_unstored_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()>;
}

/// Structured external OAuth token material. This intentionally has no serde
/// implementation and redacts every secret in Debug. It exists only between a
/// secret-manager adapter and the narrow refresh/authorization paths.
pub struct OAuthTokenMaterial {
    pub(crate) access_token: TokenSecret,
    pub(crate) refresh_token: Option<TokenSecret>,
    pub(crate) token_type: String,
    pub(crate) expires_at: Option<Timestamp>,
    pub(crate) scopes: Vec<String>,
}

impl OAuthTokenMaterial {
    #[must_use]
    pub fn from_bundle(bundle: &TokenBundle) -> Self {
        Self {
            access_token: TokenSecret::new(bundle.access_token().expose_secret().to_owned()),
            refresh_token: bundle
                .refresh_token()
                .map(|token| TokenSecret::new(token.expose_secret().to_owned())),
            token_type: bundle.token_type().to_owned(),
            expires_at: bundle.expires_at(),
            scopes: bundle.scopes().to_vec(),
        }
    }

    #[must_use]
    pub const fn access_token(&self) -> &TokenSecret {
        &self.access_token
    }

    #[must_use]
    pub const fn refresh_token(&self) -> Option<&TokenSecret> {
        self.refresh_token.as_ref()
    }

    #[must_use]
    pub fn token_type(&self) -> &str {
        &self.token_type
    }

    #[must_use]
    pub const fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    #[must_use]
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
}

impl fmt::Debug for OAuthTokenMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OAuthTokenMaterial")
            .field("access_token", &"[redacted]")
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "[redacted]"),
            )
            .field("token_type", &self.token_type)
            .field("expires_at", &self.expires_at)
            .field("scopes", &self.scopes)
            .finish()
    }
}

/// External secret-manager operations for OAuth credential tokens. PostgreSQL
/// metadata/lifecycle remains in persistence; raw access and refresh token
/// values must never be returned through a generic secret resolver.
#[async_trait]
pub trait OAuthTokenMaterialPort: fmt::Debug + Send + Sync {
    /// Whether this canonical reference was generated by this exact material
    /// store. Lifecycle adapters must not delete a pre-existing generic
    /// connector secret merely because a connector is re-authorized as OAuth.
    fn owns_reference(&self, reference: &SecretReference) -> bool;
    async fn write_tokens(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
        tokens: &TokenBundle,
    ) -> Result<SecretReference>;
    async fn read_tokens(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<Option<OAuthTokenMaterial>>;
    async fn replace_tokens(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
        tokens: &TokenBundle,
    ) -> Result<SecretReference>;
    async fn delete_tokens(&self, tenant_id: TenantId, reference: &SecretReference) -> Result<()>;
    async fn delete_unstored_tokens(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()>;
}

#[async_trait]
impl<T> OAuthTokenMaterialPort for Arc<T>
where
    T: OAuthTokenMaterialPort + ?Sized,
{
    fn owns_reference(&self, reference: &SecretReference) -> bool {
        (**self).owns_reference(reference)
    }

    async fn write_tokens(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
        tokens: &TokenBundle,
    ) -> Result<SecretReference> {
        (**self).write_tokens(tenant_id, reference_id, tokens).await
    }

    async fn read_tokens(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<Option<OAuthTokenMaterial>> {
        (**self).read_tokens(tenant_id, reference).await
    }

    async fn replace_tokens(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
        tokens: &TokenBundle,
    ) -> Result<SecretReference> {
        (**self).replace_tokens(tenant_id, reference, tokens).await
    }

    async fn delete_tokens(&self, tenant_id: TenantId, reference: &SecretReference) -> Result<()> {
        (**self).delete_tokens(tenant_id, reference).await
    }

    async fn delete_unstored_tokens(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        (**self)
            .delete_unstored_tokens(tenant_id, reference_id)
            .await
    }
}

/// Completed durable OAuth exchange. Tokens retain their redacting/zeroizing
/// representation until an authorized credential lifecycle writes them to the
/// external secret manager.
pub struct DurableOAuthCompletion {
    pub authorization: DurableAuthorizationState,
    pub tokens: TokenBundle,
}

impl fmt::Debug for DurableOAuthCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableOAuthCompletion")
            .field("authorization", &self.authorization)
            .field("tokens", &"[redacted]")
            .finish()
    }
}

/// Non-secret result of durable external-token materialization and connector
/// credential binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OAuthCredentialBinding {
    pub credential_id: CredentialId,
    pub secret_reference_id: SecretReferenceId,
}

/// Owns the only production path from a transient OAuth token bundle to a
/// canonical connector credential reference. Implementations must write token
/// bytes to the external secret manager first, compensate on SQL failure, and
/// never serialize tokens into PostgreSQL, audit fields, logs, or errors.
#[async_trait]
pub trait OAuthCredentialPort: fmt::Debug + Send + Sync {
    async fn persist_tokens(
        &self,
        authorization: &DurableAuthorizationState,
        tokens: &TokenBundle,
    ) -> Result<OAuthCredentialBinding>;
}

#[async_trait]
impl<T> OAuthCredentialPort for Arc<T>
where
    T: OAuthCredentialPort + ?Sized,
{
    async fn persist_tokens(
        &self,
        authorization: &DurableAuthorizationState,
        tokens: &TokenBundle,
    ) -> Result<OAuthCredentialBinding> {
        (**self).persist_tokens(authorization, tokens).await
    }
}

#[async_trait]
impl<T> TokenEndpointPort for Arc<T>
where
    T: TokenEndpointPort + ?Sized,
{
    async fn exchange_code(
        &self,
        config: &OAuth2Config,
        code: &str,
        code_verifier: &str,
    ) -> Result<TokenBundle> {
        (**self).exchange_code(config, code, code_verifier).await
    }

    async fn refresh(&self, config: &OAuth2Config, refresh_token: &str) -> Result<TokenBundle> {
        (**self).refresh(config, refresh_token).await
    }
}

/// Production-safe durable OAuth authorization-code client.
#[derive(Debug)]
pub struct DurableOAuthClient<S, P, E>
where
    S: OAuthAuthorizationStatePort,
    P: OAuthPkceSecretPort,
    E: TokenEndpointPort,
{
    config: OAuth2Config,
    provider: String,
    states: S,
    pkce: P,
    endpoint: E,
}

impl<S, P, E> DurableOAuthClient<S, P, E>
where
    S: OAuthAuthorizationStatePort,
    P: OAuthPkceSecretPort,
    E: TokenEndpointPort,
{
    /// Constructs a client for a canonical provider key.
    pub fn new(
        config: OAuth2Config,
        provider: impl Into<String>,
        states: S,
        pkce: P,
        endpoint: E,
    ) -> Result<Self> {
        let provider = provider.into();
        let probe = DurableAuthorizationState {
            id: uuid::Uuid::now_v7(),
            tenant_id: TenantId::new(),
            connector_id: None,
            provider: provider.clone(),
            state_hash: "a".repeat(64),
            pkce_verifier_secret_id: SecretReferenceId::new(),
            redirect_uri: config.redirect_uri.as_str().to_owned(),
            requested_scopes: config.scopes.clone(),
            expires_at: Timestamp::now()
                .checked_add(Duration::from_secs(1))
                .ok_or_else(|| {
                    AppError::internal("timestamp overflow while validating OAuth provider")
                })?,
            created_at: Timestamp::now(),
        };
        probe.validate()?;
        Ok(Self {
            config,
            provider,
            states,
            pkce,
            endpoint,
        })
    }

    /// Starts an authorization. The raw state is returned only for the browser
    /// redirect; persistence receives its hash and a secret reference to the
    /// verifier, never either raw value.
    pub async fn authorize_url(
        &self,
        tenant_id: TenantId,
        connector_id: Option<ConnectorId>,
        now: &Timestamp,
    ) -> Result<(String, String)> {
        if tenant_id.is_nil() {
            return Err(AppError::invalid_field(
                "tenant_id",
                "required",
                "OAuth authorizations require a concrete tenant",
            ));
        }
        let pkce = PkcePair::generate();
        let state = generate_state();
        let expires_at = now
            .checked_add(Duration::from_secs(AUTH_SESSION_TTL_SECS))
            .ok_or_else(|| AppError::internal("OAuth state expiry overflow"))?;
        // Construct the request URL before creating either durable artifact.
        // This makes a configuration failure incapable of leaving a consumed
        // state that points at an already-discarded verifier.
        let authorization_url = self.config.authorization_url(&state, &pkce)?;
        let verifier_reference = self
            .pkce
            .store_verifier(tenant_id, pkce.verifier(), expires_at)
            .await?;
        let durable = DurableAuthorizationState {
            id: uuid::Uuid::now_v7(),
            tenant_id,
            connector_id,
            provider: self.provider.clone(),
            state_hash: hash_state(&state),
            pkce_verifier_secret_id: verifier_reference,
            redirect_uri: self.config.redirect_uri.as_str().to_owned(),
            requested_scopes: self.config.scopes.clone(),
            expires_at,
            created_at: *now,
        };
        if let Err(error) = durable.validate() {
            self.discard_verifier_best_effort(tenant_id, verifier_reference)
                .await;
            return Err(error);
        }
        if let Err(error) = self.states.begin(&durable).await {
            // SQL metadata and the external verifier must not drift apart on
            // a state-write failure. Cleanup is idempotent and intentionally
            // does not expose either the state or verifier in its error path.
            self.discard_verifier_best_effort(tenant_id, verifier_reference)
                .await;
            return Err(error.with_context("durable OAuth state creation failed"));
        }
        Ok((authorization_url, state))
    }

    async fn discard_verifier_best_effort(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) {
        if self
            .pkce
            .discard_verifier(tenant_id, reference_id)
            .await
            .is_err()
        {
            // A retry/cleanup job can reconcile an external-manager outage.
            // Do not replace the security-relevant callback/state result with
            // an opaque cleanup failure, and never log reference/path/token
            // values here.
            tracing::warn!(
                outcome = "cleanup_failed",
                "OAuth PKCE verifier cleanup failed"
            );
        }
    }

    /// Consumes callback state exactly once then exchanges the code through a
    /// real token endpoint. Provider-denied callbacks intentionally consume
    /// state first, eliminating retry/replay ambiguity.
    pub async fn handle_callback(
        &self,
        state: &str,
        code: Option<&str>,
        provider_error: Option<&str>,
        now: &Timestamp,
    ) -> Result<DurableOAuthCompletion> {
        validate_state_input(state)?;
        let durable = self
            .states
            .consume(&hash_state(state), now)
            .await?
            .ok_or_else(|| {
                AppError::unauthorized("unknown, expired, or previously consumed OAuth state")
            })?;
        if durable.provider != self.provider
            || durable.redirect_uri != self.config.redirect_uri.as_str()
        {
            self.discard_verifier_best_effort(durable.tenant_id, durable.pkce_verifier_secret_id)
                .await;
            return Err(AppError::unauthorized(
                "OAuth state did not belong to this provider configuration",
            ));
        }
        if provider_error.is_some() {
            self.discard_verifier_best_effort(durable.tenant_id, durable.pkce_verifier_secret_id)
                .await;
            // Do not echo `error` or `error_description`: both are remote
            // provider input and must never become a public token-flow error.
            return Err(AppError::unauthorized(
                "OAuth provider denied the authorization request",
            ));
        }
        let Some(code) = code else {
            self.discard_verifier_best_effort(durable.tenant_id, durable.pkce_verifier_secret_id)
                .await;
            return Err(AppError::invalid_field(
                "code",
                "required",
                "authorization code missing from OAuth callback",
            ));
        };
        if code.is_empty() || code.len() > 8_192 || code.chars().any(char::is_control) {
            self.discard_verifier_best_effort(durable.tenant_id, durable.pkce_verifier_secret_id)
                .await;
            return Err(AppError::invalid_field(
                "code",
                "invalid_format",
                "authorization code must be bounded printable input",
            ));
        }
        let verifier = self
            .pkce
            .consume_verifier(durable.tenant_id, durable.pkce_verifier_secret_id)
            .await?
            .ok_or_else(|| {
                AppError::unauthorized("OAuth PKCE verifier is unavailable or already used")
            })?;
        let tokens = self
            .endpoint
            .exchange_code(&self.config, code, verifier.expose_secret())
            .await
            .map_err(|error| error.with_context("OAuth token exchange failed"))?;
        Ok(DurableOAuthCompletion {
            authorization: durable,
            tokens,
        })
    }

    /// Handles a one-shot callback and immediately materializes its token
    /// bundle through the canonical credential lifecycle port. Production HTTP
    /// callbacks must call this method rather than retaining or serializing
    /// [`DurableOAuthCompletion`] themselves.
    pub async fn handle_callback_and_persist<C>(
        &self,
        state: &str,
        code: Option<&str>,
        provider_error: Option<&str>,
        now: &Timestamp,
        credentials: &C,
    ) -> Result<OAuthCredentialBinding>
    where
        C: OAuthCredentialPort + ?Sized,
    {
        let completion = self
            .handle_callback(state, code, provider_error, now)
            .await?;
        credentials
            .persist_tokens(&completion.authorization, &completion.tokens)
            .await
    }
}

/// Process-wide durable OAuth façade. It selects only an operator-enabled,
/// source-allowlisted provider profile, then creates a short-lived client for
/// each request. This keeps browser input from selecting storage, token, or
/// endpoint implementations.
#[derive(Debug)]
pub struct DurableOAuthAuthorizationService {
    catalog: Arc<dyn OAuthProviderCatalogPort>,
    states: Arc<dyn OAuthAuthorizationStatePort>,
    pkce: Arc<dyn OAuthPkceSecretPort>,
    endpoint: Arc<dyn TokenEndpointPort>,
    credentials: Arc<dyn OAuthCredentialPort>,
}

impl DurableOAuthAuthorizationService {
    #[must_use]
    pub fn new(
        catalog: Arc<dyn OAuthProviderCatalogPort>,
        states: Arc<dyn OAuthAuthorizationStatePort>,
        pkce: Arc<dyn OAuthPkceSecretPort>,
        endpoint: Arc<dyn TokenEndpointPort>,
        credentials: Arc<dyn OAuthCredentialPort>,
    ) -> Self {
        Self {
            catalog,
            states,
            pkce,
            endpoint,
            credentials,
        }
    }

    async fn client_for(
        &self,
        provider: &str,
    ) -> Result<
        DurableOAuthClient<
            Arc<dyn OAuthAuthorizationStatePort>,
            Arc<dyn OAuthPkceSecretPort>,
            Arc<dyn TokenEndpointPort>,
        >,
    > {
        validate_provider_input(provider)?;
        let config =
            self.catalog.config(provider).await?.ok_or_else(|| {
                AppError::not_found("OAuth provider", "provider is not configured")
            })?;
        DurableOAuthClient::new(
            config,
            provider.to_owned(),
            self.states.clone(),
            self.pkce.clone(),
            self.endpoint.clone(),
        )
    }

    /// Starts a connector-bound OAuth PKCE authorization. Callers must first
    /// authorize the connector under the authenticated tenant; this method
    /// refuses unbound states so a callback can always create one canonical
    /// credential binding.
    pub async fn start(
        &self,
        tenant_id: TenantId,
        connector_id: ConnectorId,
        provider: &str,
        now: &Timestamp,
    ) -> Result<String> {
        if tenant_id.is_nil() || connector_id.is_nil() {
            return Err(AppError::invalid_field(
                "oauth_connector",
                "required",
                "OAuth authorization requires concrete tenant and connector ids",
            ));
        }
        let client = self.client_for(provider).await?;
        let (authorization_url, _state) = client
            .authorize_url(tenant_id, Some(connector_id), now)
            .await?;
        Ok(authorization_url)
    }

    /// Completes a provider callback and persists external token material into
    /// the canonical credential lifecycle before returning. The callback never
    /// exposes a TokenBundle to an HTTP handler or caller.
    pub async fn complete(
        &self,
        provider: &str,
        state: &str,
        code: Option<&str>,
        provider_error: Option<&str>,
        now: &Timestamp,
    ) -> Result<OAuthCredentialBinding> {
        let client = self.client_for(provider).await?;
        client
            .handle_callback_and_persist(
                state,
                code,
                provider_error,
                now,
                self.credentials.as_ref(),
            )
            .await
    }
}

fn validate_provider_input(provider: &str) -> Result<()> {
    if provider.is_empty()
        || provider.len() > 64
        || !provider.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(AppError::invalid_field(
            "provider",
            "invalid_format",
            "OAuth provider key must be a 1..=64 lowercase token",
        ));
    }
    Ok(())
}

/// SHA-256 hex of the browser state. Raw state never reaches durable logging
/// or persistence APIs.
#[must_use]
pub fn hash_state(state: &str) -> String {
    hex::encode(Sha256::digest(state.as_bytes()))
}

fn validate_state_input(state: &str) -> Result<()> {
    if state.len() != 64 || !state.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(AppError::invalid_field(
            "state",
            "invalid_format",
            "OAuth state must be exactly 64 hexadecimal characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_hash_is_deterministic_and_raw_state_is_not_the_hash() {
        let state = "a".repeat(64);
        assert_eq!(hash_state(&state).len(), 64);
        assert_ne!(hash_state(&state), state);
        assert!(validate_state_input(&state).is_ok());
        assert!(validate_state_input("short").is_err());
    }
}
