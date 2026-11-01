//! PURPOSE: Production secret-manager adapters and the webhook-secret bridge.
//!
//! Owning layer: `mas-integrations` infrastructure boundary.
//!
//! Production role: resolve canonical secret references through the single
//! security `SecretGate`, with a real Vault KV HTTP adapter beneath guarded
//! egress. It supports version-pinned reads for outbound retry stability while
//! keeping raw material inside redacting/zeroizing wrappers.
//!
//! Security boundary: reference metadata is tenant-scoped and not equivalent
//! to permission to read a secret. Every material read goes through
//! `SecretGate` (lifecycle check plus audit); Vault tokens are `SecretString`;
//! URLs are constructed from an operator-owned base plus strictly validated
//! reference paths and sent only through `GuardedHttpClient`.
//!
//! Data ownership: persistence owns reference metadata; the external secret
//! manager owns secret bytes and versions. This module owns neither durable
//! secret material nor an in-memory production cache.
//!
//! Prohibited responsibilities: storing raw secrets, accepting secret material
//! from API requests/config files, bypassing audit, directly delivering a
//! webhook, or treating a missing manager response as successful empty bytes.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{
    AuditActor, AuditActorKind, AuditOutcome, Connector, ConnectorAuthMode, SecretAvailability,
    SecretProviderKind, SecretReference, SecretRotationState,
};
use mas_security::audit::security_event;
use mas_security::{AuditSinkPort, SecretGate, SecretResolverPort, SecretString};
use serde_json::Value;

use crate::durable_oauth::{
    oauth_pkce_reference_locator, oauth_token_reference_locator, OAuthPkceMaterialPort,
    OAuthTokenMaterial, OAuthTokenMaterialPort,
};
use crate::http_guard::{GuardedHttpClient, HttpClientPort, HttpRequest};
use crate::oauth::{TokenBundle, TokenSecret};
use crate::provider_adapters::{
    ConnectorCredentialLookupPort, ConnectorCredentialPort, ProviderAuthorization,
};
use crate::webhooks::{ResolvedWebhookSecret, WebhookSecretPort};

/// Tenant-scoped lookup result for reference metadata.
#[derive(Debug, Clone)]
pub struct SecretReferenceRecord {
    /// Tenant that owns the reference and its audited material read.
    pub tenant_id: TenantId,
    /// External-manager pointer and lifecycle metadata; never material.
    pub reference: SecretReference,
}

/// Lookup port implemented by the canonical persistence repository.
#[async_trait]
pub trait SecretReferenceLookupPort: fmt::Debug + Send + Sync {
    /// Resolves a reference id to tenant-owned metadata. A foreign/missing id
    /// returns `None` so callers cannot infer another tenant's configuration.
    async fn get(&self, id: SecretReferenceId) -> Result<Option<SecretReferenceRecord>>;
}

/// Converts canonical secret lookup + `SecretGate` into webhook HMAC material.
///
/// The resolver is deliberately generic instead of keeping a second secret
/// manager abstraction in the webhook module.
#[derive(Debug)]
pub struct SecretGateWebhookResolver<L>
where
    L: SecretReferenceLookupPort,
{
    references: Arc<L>,
    gate: Arc<SecretGate>,
    correlation_id: String,
}

impl<L> SecretGateWebhookResolver<L>
where
    L: SecretReferenceLookupPort,
{
    /// Creates a resolver with a bounded, non-sensitive audit correlation id.
    pub fn new(
        references: Arc<L>,
        gate: Arc<SecretGate>,
        correlation_id: impl Into<String>,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        mas_common::validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        if correlation_id.chars().any(char::is_control) {
            return Err(AppError::invalid_field(
                "correlation_id",
                "invalid_format",
                "secret resolver correlation id cannot contain control characters",
            ));
        }
        Ok(Self {
            references,
            gate,
            correlation_id,
        })
    }

    async fn resolve_at_version(
        &self,
        id: SecretReferenceId,
        key_version: Option<&str>,
    ) -> Result<ResolvedWebhookSecret> {
        let record = self.references.get(id).await?.ok_or_else(|| {
            AppError::not_found("webhook signing secret", "reference unavailable")
        })?;
        let actor = AuditActor::new(AuditActorKind::Service, "integration-relay")?;
        let secret = self
            .gate
            .resolve_for_version(
                &record.reference,
                key_version,
                "webhook_delivery_signing",
                Some(record.tenant_id),
                &actor,
                &self.correlation_id,
            )
            .await?;
        ResolvedWebhookSecret::new(secret.expose().as_bytes().to_vec())
    }
}

#[async_trait]
impl<L> WebhookSecretPort for SecretGateWebhookResolver<L>
where
    L: SecretReferenceLookupPort + 'static,
{
    async fn resolve(&self, reference: SecretReferenceId) -> Result<ResolvedWebhookSecret> {
        self.resolve_at_version(reference, None).await
    }

    async fn resolve_version(
        &self,
        reference: SecretReferenceId,
        key_version: Option<&str>,
    ) -> Result<ResolvedWebhookSecret> {
        self.resolve_at_version(reference, key_version).await
    }
}

/// Resolves connector credentials through the same audited secret gate used
/// by webhook signing. This is the production bridge from a credential
/// reference lifecycle to provider-specific HTTP authorization headers.
#[derive(Debug)]
pub struct SecretGateConnectorCredentialResolver<L>
where
    L: ConnectorCredentialLookupPort,
{
    credentials: Arc<L>,
    gate: Arc<SecretGate>,
    correlation_id: String,
}

impl<L> SecretGateConnectorCredentialResolver<L>
where
    L: ConnectorCredentialLookupPort,
{
    /// Creates the resolver. The correlation id identifies the relay/API
    /// operation in secret access audit records; it must not contain tokens.
    pub fn new(
        credentials: Arc<L>,
        gate: Arc<SecretGate>,
        correlation_id: impl Into<String>,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        mas_common::validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        if correlation_id.chars().any(char::is_control) {
            return Err(AppError::invalid_field(
                "correlation_id",
                "invalid_format",
                "connector secret correlation id cannot contain control characters",
            ));
        }
        Ok(Self {
            credentials,
            gate,
            correlation_id,
        })
    }
}

#[async_trait]
impl<L> ConnectorCredentialPort for SecretGateConnectorCredentialResolver<L>
where
    L: ConnectorCredentialLookupPort + 'static,
{
    async fn resolve_authorization(&self, connector: &Connector) -> Result<ProviderAuthorization> {
        if connector.auth_mode == ConnectorAuthMode::None {
            return Ok(ProviderAuthorization::None);
        }
        let binding = self
            .credentials
            .active_reference(connector)
            .await?
            .ok_or_else(|| {
                AppError::conflict("connector has no active credential reference bound for probing")
            })?;
        if binding.tenant_id != connector.tenant_id || binding.connector_id != connector.id {
            return Err(AppError::database(
                "credential lookup returned a connector binding outside the requested scope",
            ));
        }
        let actor = AuditActor::new(AuditActorKind::Service, "integration-probe")?;
        let material = self
            .gate
            .resolve_for(
                &binding.secret_reference,
                "connector_probe_authorization",
                Some(binding.tenant_id),
                &actor,
                &self.correlation_id,
            )
            .await?;
        match connector.auth_mode {
            ConnectorAuthMode::OAuth2 => Ok(ProviderAuthorization::Bearer(material)),
            ConnectorAuthMode::ApiKey => match connector.provider.as_str() {
                // These vendors use API keys as bearer credentials.
                "slack" | "sendgrid" | "salesforce" | "jira" => {
                    Ok(ProviderAuthorization::Bearer(material))
                },
                // Generic HTTP connectors deliberately use a named API key
                // rather than assuming a provider-specific bearer scheme.
                "generic-webhook" => Ok(ProviderAuthorization::Header {
                    name: "x-api-key".to_owned(),
                    value: material,
                }),
                provider => Err(AppError::invalid_field(
                    "provider",
                    "adapter_not_configured",
                    format!(
                        "provider '{provider}' has no safe HTTP API-key authorization adapter"
                    ),
                )),
            },
            ConnectorAuthMode::Basic => Err(AppError::invalid_field(
                "auth_mode",
                "adapter_not_configured",
                "basic-auth probing requires a protocol-specific adapter and is never synthesized from a generic secret",
            )),
            ConnectorAuthMode::None => Ok(ProviderAuthorization::None),
        }
    }
}

/// Connector authorization resolver for structured OAuth token records.
///
/// OAuth token documents must never flow through `SecretGate`'s generic
/// single-value resolver: doing so could accidentally present an entire JSON
/// document as an HTTP bearer token. The injected material port is required to
/// be an audited lifecycle boundary instead.
#[derive(Debug)]
pub struct OAuthTokenConnectorCredentialResolver<L, M>
where
    L: ConnectorCredentialLookupPort,
    M: OAuthTokenMaterialPort,
{
    credentials: Arc<L>,
    material: Arc<M>,
}

impl<L, M> OAuthTokenConnectorCredentialResolver<L, M>
where
    L: ConnectorCredentialLookupPort,
    M: OAuthTokenMaterialPort,
{
    #[must_use]
    pub fn new(credentials: Arc<L>, material: Arc<M>) -> Self {
        Self {
            credentials,
            material,
        }
    }
}

#[async_trait]
impl<L, M> ConnectorCredentialPort for OAuthTokenConnectorCredentialResolver<L, M>
where
    L: ConnectorCredentialLookupPort + 'static,
    M: OAuthTokenMaterialPort + 'static,
{
    async fn resolve_authorization(&self, connector: &Connector) -> Result<ProviderAuthorization> {
        if connector.auth_mode != ConnectorAuthMode::OAuth2 {
            return Err(AppError::invalid_field(
                "auth_mode",
                "oauth_required",
                "structured OAuth token resolver may only authorize OAuth2 connectors",
            ));
        }
        let binding = self
            .credentials
            .active_reference(connector)
            .await?
            .ok_or_else(|| {
                AppError::conflict(
                    "connector has no active OAuth credential reference bound for probing",
                )
            })?;
        if binding.tenant_id != connector.tenant_id
            || binding.connector_id != connector.id
            || !binding.secret_reference.is_readable()
        {
            return Err(AppError::database(
                "OAuth credential lookup returned an invalid connector binding",
            ));
        }
        let material = self
            .material
            .read_tokens(binding.tenant_id, &binding.secret_reference)
            .await?
            .ok_or_else(|| {
                AppError::conflict(
                    "OAuth credential material is unavailable; reconnect the connector",
                )
            })?;
        if material.expires_at().is_some_and(|expiry| expiry.is_past()) {
            return Err(AppError::conflict(
                "OAuth access token is expired; refresh must complete before provider access",
            ));
        }
        Ok(ProviderAuthorization::Bearer(SecretString::new(
            material.access_token().expose_secret(),
        )))
    }
}

/// Routes OAuth2 connectors to structured-token authorization and every other
/// supported auth mode to the generic audited secret-gate resolver. It avoids
/// an unsafe caller-side choice of resolver.
#[derive(Debug)]
pub struct CompositeConnectorCredentialResolver<G, O>
where
    G: ConnectorCredentialPort,
    O: ConnectorCredentialPort,
{
    generic: G,
    oauth: O,
}

impl<G, O> CompositeConnectorCredentialResolver<G, O>
where
    G: ConnectorCredentialPort,
    O: ConnectorCredentialPort,
{
    #[must_use]
    pub const fn new(generic: G, oauth: O) -> Self {
        Self { generic, oauth }
    }
}

#[async_trait]
impl<G, O> ConnectorCredentialPort for CompositeConnectorCredentialResolver<G, O>
where
    G: ConnectorCredentialPort + 'static,
    O: ConnectorCredentialPort + 'static,
{
    async fn resolve_authorization(&self, connector: &Connector) -> Result<ProviderAuthorization> {
        match connector.auth_mode {
            ConnectorAuthMode::OAuth2 => self.oauth.resolve_authorization(connector).await,
            _ => self.generic.resolve_authorization(connector).await,
        }
    }
}

/// Audits structured OAuth token-material manager operations without ever
/// converting the token document into a generic secret value. This is the
/// required production wrapper around a concrete OAuth token material store.
pub struct AuditedOAuthTokenMaterialStore<M>
where
    M: OAuthTokenMaterialPort,
{
    inner: M,
    audit: Arc<dyn AuditSinkPort>,
    actor: AuditActor,
    correlation_id: String,
}

impl<M> fmt::Debug for AuditedOAuthTokenMaterialStore<M>
where
    M: OAuthTokenMaterialPort,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditedOAuthTokenMaterialStore")
            .field("inner", &self.inner)
            .field("actor", &self.actor)
            .field("correlation_id", &self.correlation_id)
            .finish_non_exhaustive()
    }
}

impl<M> AuditedOAuthTokenMaterialStore<M>
where
    M: OAuthTokenMaterialPort,
{
    pub fn new(
        inner: M,
        audit: Arc<dyn AuditSinkPort>,
        correlation_id: impl Into<String>,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        mas_common::validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        if correlation_id.chars().any(char::is_control) {
            return Err(AppError::invalid_field(
                "correlation_id",
                "invalid_format",
                "OAuth token correlation id cannot contain control characters",
            ));
        }
        Ok(Self {
            inner,
            audit,
            actor: AuditActor::new(AuditActorKind::Service, "oauth-token-lifecycle")?,
            correlation_id,
        })
    }

    async fn audit(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
        action: &str,
        outcome: AuditOutcome,
    ) {
        let event = security_event(
            Some(tenant_id),
            None,
            self.actor.clone(),
            action,
            "secret_reference",
            Some(reference_id.to_string()),
            outcome,
            &self.correlation_id,
        );
        match event {
            Ok(event) => {
                if let Err(error) = self.audit.record(&event).await {
                    tracing::warn!(%error, action, "failed to audit OAuth token material operation");
                }
            },
            Err(error) => {
                tracing::warn!(%error, action, "failed to build OAuth token material audit event")
            },
        }
    }
}

#[async_trait]
impl<M> OAuthTokenMaterialPort for AuditedOAuthTokenMaterialStore<M>
where
    M: OAuthTokenMaterialPort + 'static,
{
    fn owns_reference(&self, reference: &SecretReference) -> bool {
        self.inner.owns_reference(reference)
    }

    async fn write_tokens(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
        tokens: &TokenBundle,
    ) -> Result<SecretReference> {
        let result = self
            .inner
            .write_tokens(tenant_id, reference_id, tokens)
            .await;
        self.audit(
            tenant_id,
            reference_id,
            "secret.oauth_token.write",
            if result.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .await;
        result
    }

    async fn read_tokens(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<Option<OAuthTokenMaterial>> {
        let result = self.inner.read_tokens(tenant_id, reference).await;
        self.audit(
            tenant_id,
            reference.id,
            "secret.oauth_token.read",
            match &result {
                Ok(Some(_)) => AuditOutcome::Success,
                Ok(None) => AuditOutcome::Denied,
                Err(_) => AuditOutcome::Failure,
            },
        )
        .await;
        result
    }

    async fn replace_tokens(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
        tokens: &TokenBundle,
    ) -> Result<SecretReference> {
        let result = self
            .inner
            .replace_tokens(tenant_id, reference, tokens)
            .await;
        self.audit(
            tenant_id,
            reference.id,
            "secret.oauth_token.replace",
            if result.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .await;
        result
    }

    async fn delete_tokens(&self, tenant_id: TenantId, reference: &SecretReference) -> Result<()> {
        let result = self.inner.delete_tokens(tenant_id, reference).await;
        self.audit(
            tenant_id,
            reference.id,
            "secret.oauth_token.delete",
            if result.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .await;
        result
    }

    async fn delete_unstored_tokens(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let result = self
            .inner
            .delete_unstored_tokens(tenant_id, reference_id)
            .await;
        self.audit(
            tenant_id,
            reference_id,
            "secret.oauth_token.delete_unstored",
            if result.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .await;
        result
    }
}

/// Audits OAuth PKCE verifier manager operations. The wrapper is required in
/// production because verifier reads/deletes are security-sensitive one-shot
/// secret access just like structured access/refresh token operations.
pub struct AuditedOAuthPkceMaterialStore<M>
where
    M: OAuthPkceMaterialPort,
{
    inner: M,
    audit: Arc<dyn AuditSinkPort>,
    actor: AuditActor,
    correlation_id: String,
}

impl<M> fmt::Debug for AuditedOAuthPkceMaterialStore<M>
where
    M: OAuthPkceMaterialPort,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditedOAuthPkceMaterialStore")
            .field("inner", &self.inner)
            .field("actor", &self.actor)
            .field("correlation_id", &self.correlation_id)
            .finish_non_exhaustive()
    }
}

impl<M> AuditedOAuthPkceMaterialStore<M>
where
    M: OAuthPkceMaterialPort,
{
    pub fn new(
        inner: M,
        audit: Arc<dyn AuditSinkPort>,
        correlation_id: impl Into<String>,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        mas_common::validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        if correlation_id.chars().any(char::is_control) {
            return Err(AppError::invalid_field(
                "correlation_id",
                "invalid_format",
                "OAuth PKCE correlation id cannot contain control characters",
            ));
        }
        Ok(Self {
            inner,
            audit,
            actor: AuditActor::new(AuditActorKind::Service, "oauth-pkce-lifecycle")?,
            correlation_id,
        })
    }

    async fn audit(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
        action: &str,
        outcome: AuditOutcome,
    ) {
        let event = security_event(
            Some(tenant_id),
            None,
            self.actor.clone(),
            action,
            "secret_reference",
            Some(reference_id.to_string()),
            outcome,
            &self.correlation_id,
        );
        match event {
            Ok(event) => {
                if let Err(error) = self.audit.record(&event).await {
                    tracing::warn!(%error, action, "failed to audit OAuth PKCE material operation");
                }
            },
            Err(error) => {
                tracing::warn!(%error, action, "failed to build OAuth PKCE material audit event")
            },
        }
    }
}

#[async_trait]
impl<M> OAuthPkceMaterialPort for AuditedOAuthPkceMaterialStore<M>
where
    M: OAuthPkceMaterialPort + 'static,
{
    async fn write_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
        verifier: &TokenSecret,
        expires_at: Timestamp,
    ) -> Result<SecretReference> {
        let result = self
            .inner
            .write_verifier(tenant_id, reference_id, verifier, expires_at)
            .await;
        self.audit(
            tenant_id,
            reference_id,
            "secret.oauth_pkce.write",
            if result.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .await;
        result
    }

    async fn consume_verifier(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<Option<TokenSecret>> {
        let result = self.inner.consume_verifier(tenant_id, reference).await;
        self.audit(
            tenant_id,
            reference.id,
            "secret.oauth_pkce.consume",
            match &result {
                Ok(Some(_)) => AuditOutcome::Success,
                Ok(None) => AuditOutcome::Denied,
                Err(_) => AuditOutcome::Failure,
            },
        )
        .await;
        result
    }

    async fn delete_verifier(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<()> {
        let result = self.inner.delete_verifier(tenant_id, reference).await;
        self.audit(
            tenant_id,
            reference.id,
            "secret.oauth_pkce.delete",
            if result.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .await;
        result
    }

    async fn delete_unstored_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let result = self
            .inner
            .delete_unstored_verifier(tenant_id, reference_id)
            .await;
        self.audit(
            tenant_id,
            reference_id,
            "secret.oauth_pkce.delete_unstored",
            if result.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
        )
        .await;
        result
    }
}

/// Vault KV implementation for short-lived OAuth PKCE verifier material.
///
/// The path is derived solely from a generated reference id beneath an
/// operator-owned prefix. A tenant cannot supply a Vault path, mount, URL, or
/// header. PostgreSQL owns the matching `SecretReference` metadata and invokes
/// this type only through the durable OAuth PKCE adapter.
pub struct VaultOAuthPkceMaterialStore<C>
where
    C: HttpClientPort,
{
    client: GuardedHttpClient<C>,
    base_url: mas_domain::value_objects::SafeUrl,
    token: SecretString,
    namespace: Option<String>,
    path_prefix: String,
}

impl<C> fmt::Debug for VaultOAuthPkceMaterialStore<C>
where
    C: HttpClientPort,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VaultOAuthPkceMaterialStore")
            .field("base_url", &self.base_url)
            .field("token", &"[redacted]")
            .field("namespace", &self.namespace)
            .field("path_prefix", &self.path_prefix)
            .finish_non_exhaustive()
    }
}

impl<C> VaultOAuthPkceMaterialStore<C>
where
    C: HttpClientPort,
{
    /// Builds the Vault-backed PKCE material adapter. `path_prefix` is an
    /// operator configuration such as `secret/data/mas/oauth-pkce`; it must be
    /// a normalized relative Vault API path.
    pub fn new(
        client: GuardedHttpClient<C>,
        base_url: mas_domain::value_objects::SafeUrl,
        token: SecretString,
        namespace: Option<String>,
        path_prefix: impl Into<String>,
    ) -> Result<Self> {
        if !base_url.is_tls() || base_url.is_internal_endpoint() {
            return Err(AppError::invalid_field(
                "vault_base_url",
                "tls_required",
                "Vault base URL must be a public HTTPS endpoint",
            ));
        }
        if token.is_empty() {
            return Err(AppError::invalid_field(
                "vault_token",
                "required",
                "Vault token must not be empty",
            ));
        }
        let namespace = namespace.filter(|value| !value.is_empty());
        if namespace
            .as_deref()
            .is_some_and(|value| value.len() > 256 || value.chars().any(char::is_control))
        {
            return Err(AppError::invalid_field(
                "vault_namespace",
                "invalid_format",
                "Vault namespace must be at most 256 printable characters",
            ));
        }
        let path_prefix = path_prefix.into();
        validate_vault_relative_path("vault_pkce_path_prefix", &path_prefix)?;
        Ok(Self {
            client,
            base_url,
            token,
            namespace,
            path_prefix,
        })
    }

    /// Opaque database locator; it deliberately is not a Vault API path.
    fn reference_path(reference_id: SecretReferenceId) -> String {
        oauth_pkce_reference_locator(reference_id)
    }

    /// Actual Vault path is derived only inside this adapter from operator
    /// configuration and the generated id, never loaded from PostgreSQL.
    fn vault_path(&self, reference_id: SecretReferenceId) -> String {
        format!("{}/{}", self.path_prefix, reference_id)
    }

    fn reference_for(&self, reference_id: SecretReferenceId) -> SecretReference {
        SecretReference {
            id: reference_id,
            provider: SecretProviderKind::Vault,
            path: Self::reference_path(reference_id),
            key_version: None,
            rotation_state: SecretRotationState::Current,
            availability: SecretAvailability::Available,
        }
    }

    fn secret_url(
        &self,
        reference: &SecretReference,
    ) -> Result<mas_domain::value_objects::SafeUrl> {
        if reference.provider != SecretProviderKind::Vault {
            return Err(AppError::invalid_field(
                "secret_reference.provider",
                "invalid_provider",
                "OAuth PKCE Vault adapter received a non-Vault reference",
            ));
        }
        if reference.path != Self::reference_path(reference.id) {
            return Err(AppError::invalid_field(
                "secret_reference.path",
                "unexpected_locator",
                "OAuth PKCE reference is not this adapter's opaque locator",
            ));
        }
        let vault_path = self.vault_path(reference.id);
        let mut url = self.base_url.as_url().clone();
        let prefix = url.path().trim_end_matches('/');
        url.set_path(&format!("{prefix}/v1/{vault_path}"));
        url.set_query(None);
        url.set_fragment(None);
        mas_domain::value_objects::SafeUrl::parse(url.as_str())
    }

    fn authorize(&self, request: HttpRequest) -> HttpRequest {
        let mut request = request.with_header("x-vault-token", self.token.expose());
        if let Some(namespace) = &self.namespace {
            request = request.with_header("x-vault-namespace", namespace);
        }
        request
    }

    fn decode_pkce_value(&self, body: &[u8]) -> Result<String> {
        let document: Value = serde_json::from_slice(body).map_err(|_| {
            AppError::external_service(
                "vault",
                "Vault returned malformed JSON for PKCE verifier read",
            )
        })?;
        let value = document
            .pointer("/data/data/value")
            .or_else(|| document.pointer("/data/value"))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AppError::external_service(
                    "vault",
                    "Vault response did not contain the PKCE verifier value",
                )
            })?;
        if value.len() < 43
            || value.len() > 128
            || value.chars().any(char::is_control)
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
            })
        {
            return Err(AppError::external_service(
                "vault",
                "Vault returned an invalid PKCE verifier value",
            ));
        }
        Ok(value.to_owned())
    }

    fn response_key_version(body: &[u8]) -> Option<String> {
        serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|document| {
                document
                    .pointer("/data/version")
                    .or_else(|| document.pointer("/data/metadata/version"))
                    .and_then(Value::as_u64)
            })
            .map(|version| version.to_string())
    }
}

#[async_trait]
impl<C> OAuthPkceMaterialPort for VaultOAuthPkceMaterialStore<C>
where
    C: HttpClientPort + 'static,
{
    async fn write_verifier(
        &self,
        _tenant_id: TenantId,
        reference_id: SecretReferenceId,
        verifier: &TokenSecret,
        expires_at: Timestamp,
    ) -> Result<SecretReference> {
        let mut reference = self.reference_for(reference_id);
        let url = self.secret_url(&reference)?;
        let body = serde_json::to_vec(&serde_json::json!({
            "data": {
                "value": verifier.expose_secret(),
                "expires_at": expires_at.to_rfc3339_millis(),
            }
        }))
        .map_err(|_| AppError::internal("failed to encode Vault PKCE write payload"))?;
        let request = self.authorize(
            HttpRequest::post(url, body).with_header("content-type", "application/json"),
        );
        let response = self.client.send(&request).await?;
        if !response.is_success() {
            return Err(AppError::external_service(
                "vault",
                format!(
                    "Vault PKCE verifier write returned HTTP status {}",
                    response.status
                ),
            ));
        }
        reference.key_version = Self::response_key_version(&response.body);
        Ok(reference)
    }

    async fn consume_verifier(
        &self,
        tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<Option<TokenSecret>> {
        let url = self.secret_url(reference)?;
        let response = self
            .client
            .send(&self.authorize(HttpRequest::get(url.clone())))
            .await?;
        match response.status {
            404 => Ok(None),
            200..=299 => {
                let verifier = TokenSecret::new(self.decode_pkce_value(&response.body)?);
                // Delete before returning material. If deletion cannot be
                // confirmed, drop/zeroize the verifier and fail closed rather
                // than enabling a second usable retrieval.
                self.delete_verifier(tenant_id, reference).await?;
                Ok(Some(verifier))
            },
            status => Err(AppError::external_service(
                "vault",
                format!("Vault PKCE verifier read returned HTTP status {status}"),
            )),
        }
    }

    async fn delete_verifier(
        &self,
        _tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<()> {
        let url = self.secret_url(reference)?;
        let response = self
            .client
            .send(&self.authorize(HttpRequest::delete(url)))
            .await?;
        match response.status {
            200..=299 | 404 => Ok(()),
            status => Err(AppError::external_service(
                "vault",
                format!("Vault PKCE verifier delete returned HTTP status {status}"),
            )),
        }
    }

    async fn delete_unstored_verifier(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let reference = self.reference_for(reference_id);
        self.delete_verifier(tenant_id, &reference).await
    }
}

/// Vault KV implementation for durable OAuth access/refresh token material.
///
/// Unlike the generic `VaultSecretResolver`, this adapter accepts only
/// generated canonical reference ids and a configured prefix. It cannot read
/// arbitrary tenant-selected paths, and its structured JSON is never exposed
/// through a generic `SecretString` resolver.
pub struct VaultOAuthTokenMaterialStore<C>
where
    C: HttpClientPort,
{
    client: GuardedHttpClient<C>,
    base_url: mas_domain::value_objects::SafeUrl,
    token: SecretString,
    namespace: Option<String>,
    path_prefix: String,
}

impl<C> fmt::Debug for VaultOAuthTokenMaterialStore<C>
where
    C: HttpClientPort,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VaultOAuthTokenMaterialStore")
            .field("base_url", &self.base_url)
            .field("token", &"[redacted]")
            .field("namespace", &self.namespace)
            .field("path_prefix", &self.path_prefix)
            .finish_non_exhaustive()
    }
}

impl<C> VaultOAuthTokenMaterialStore<C>
where
    C: HttpClientPort,
{
    /// `path_prefix` is an operator-selected Vault KV API path such as
    /// `secret/data/mas/oauth-tokens`; callers cannot influence it.
    pub fn new(
        client: GuardedHttpClient<C>,
        base_url: mas_domain::value_objects::SafeUrl,
        token: SecretString,
        namespace: Option<String>,
        path_prefix: impl Into<String>,
    ) -> Result<Self> {
        if !base_url.is_tls() || base_url.is_internal_endpoint() {
            return Err(AppError::invalid_field(
                "vault_base_url",
                "tls_required",
                "Vault base URL must be a public HTTPS endpoint",
            ));
        }
        if token.is_empty() {
            return Err(AppError::invalid_field(
                "vault_token",
                "required",
                "Vault token must not be empty",
            ));
        }
        let namespace = namespace.filter(|value| !value.is_empty());
        if namespace
            .as_deref()
            .is_some_and(|value| value.len() > 256 || value.chars().any(char::is_control))
        {
            return Err(AppError::invalid_field(
                "vault_namespace",
                "invalid_format",
                "Vault namespace must be at most 256 printable characters",
            ));
        }
        let path_prefix = path_prefix.into();
        validate_vault_relative_path("vault_oauth_token_path_prefix", &path_prefix)?;
        Ok(Self {
            client,
            base_url,
            token,
            namespace,
            path_prefix,
        })
    }

    /// Opaque database locator; it deliberately is not a Vault API path.
    fn reference_path(reference_id: SecretReferenceId) -> String {
        oauth_token_reference_locator(reference_id)
    }

    /// Actual Vault path is derived only inside this adapter from operator
    /// configuration and the generated id, never loaded from PostgreSQL.
    fn vault_path(&self, reference_id: SecretReferenceId) -> String {
        format!("{}/{}", self.path_prefix, reference_id)
    }

    fn reference_for(&self, reference_id: SecretReferenceId) -> SecretReference {
        SecretReference {
            id: reference_id,
            provider: SecretProviderKind::Vault,
            path: Self::reference_path(reference_id),
            key_version: None,
            rotation_state: SecretRotationState::Current,
            availability: SecretAvailability::Available,
        }
    }

    fn secret_url(
        &self,
        reference: &SecretReference,
    ) -> Result<mas_domain::value_objects::SafeUrl> {
        if reference.provider != SecretProviderKind::Vault {
            return Err(AppError::invalid_field(
                "secret_reference.provider",
                "invalid_provider",
                "OAuth token Vault adapter received a non-Vault reference",
            ));
        }
        if reference.path != Self::reference_path(reference.id) {
            return Err(AppError::invalid_field(
                "secret_reference.path",
                "unexpected_locator",
                "OAuth token reference is not this adapter's opaque locator",
            ));
        }
        let vault_path = self.vault_path(reference.id);
        let mut url = self.base_url.as_url().clone();
        let prefix = url.path().trim_end_matches('/');
        url.set_path(&format!("{prefix}/v1/{vault_path}"));
        url.set_query(None);
        url.set_fragment(None);
        mas_domain::value_objects::SafeUrl::parse(url.as_str())
    }

    fn authorize(&self, request: HttpRequest) -> HttpRequest {
        let mut request = request.with_header("x-vault-token", self.token.expose());
        if let Some(namespace) = &self.namespace {
            request = request.with_header("x-vault-namespace", namespace);
        }
        request
    }

    fn token_body(tokens: &TokenBundle) -> Result<Vec<u8>> {
        validate_token_bundle(tokens)?;
        serde_json::to_vec(&serde_json::json!({
            "data": {
                "access_token": tokens.access_token().expose_secret(),
                "refresh_token": tokens.refresh_token().map(|value| value.expose_secret()),
                "token_type": tokens.token_type(),
                "expires_at": tokens.expires_at().map(|value| value.to_rfc3339_millis()),
                "scopes": tokens.scopes(),
            }
        }))
        .map_err(|_| AppError::internal("failed to encode Vault OAuth token payload"))
    }

    fn decode_tokens(&self, body: &[u8]) -> Result<OAuthTokenMaterial> {
        let document: Value = serde_json::from_slice(body).map_err(|_| {
            AppError::external_service(
                "vault",
                "Vault returned malformed JSON for OAuth token read",
            )
        })?;
        let data = document
            .pointer("/data/data")
            .or_else(|| document.pointer("/data"))
            .and_then(Value::as_object)
            .ok_or_else(|| {
                AppError::external_service(
                    "vault",
                    "Vault OAuth token response did not contain a data object",
                )
            })?;
        let access_token = data
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|value| valid_token_value(value))
            .ok_or_else(|| {
                AppError::external_service(
                    "vault",
                    "Vault OAuth token response contained invalid access material",
                )
            })?;
        let refresh_token = match data.get("refresh_token") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) if valid_token_value(value) => {
                Some(TokenSecret::new(value.to_owned()))
            },
            _ => {
                return Err(AppError::external_service(
                    "vault",
                    "Vault OAuth token response contained invalid refresh material",
                ));
            },
        };
        let token_type = data
            .get("token_type")
            .and_then(Value::as_str)
            .filter(|value| value.len() <= 64 && value.eq_ignore_ascii_case("bearer"))
            .ok_or_else(|| {
                AppError::external_service(
                    "vault",
                    "Vault OAuth token response contained an invalid token type",
                )
            })?
            .to_owned();
        let expires_at = match data.get("expires_at") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(Timestamp::parse_rfc3339(value).map_err(|_| {
                AppError::external_service(
                    "vault",
                    "Vault OAuth token response contained an invalid expiry",
                )
            })?),
            _ => {
                return Err(AppError::external_service(
                    "vault",
                    "Vault OAuth token response contained an invalid expiry",
                ));
            },
        };
        let scopes = data
            .get("scopes")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                AppError::external_service(
                    "vault",
                    "Vault OAuth token response did not contain scopes",
                )
            })?
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                AppError::external_service(
                    "vault",
                    "Vault OAuth token response contained invalid scopes",
                )
            })?;
        validate_token_parts(access_token, refresh_token.as_ref(), &token_type, &scopes)?;
        Ok(OAuthTokenMaterial {
            access_token: TokenSecret::new(access_token.to_owned()),
            refresh_token,
            token_type,
            expires_at,
            scopes,
        })
    }

    fn response_key_version(body: &[u8]) -> Option<String> {
        serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|document| {
                document
                    .pointer("/data/version")
                    .or_else(|| document.pointer("/data/metadata/version"))
                    .and_then(Value::as_u64)
            })
            .map(|version| version.to_string())
    }

    async fn write_at(
        &self,
        reference: &SecretReference,
        tokens: &TokenBundle,
    ) -> Result<SecretReference> {
        let url = self.secret_url(reference)?;
        let body = Self::token_body(tokens)?;
        let request = self.authorize(
            HttpRequest::post(url, body).with_header("content-type", "application/json"),
        );
        let response = self.client.send(&request).await?;
        if !response.is_success() {
            return Err(AppError::external_service(
                "vault",
                format!(
                    "Vault OAuth token write returned HTTP status {}",
                    response.status
                ),
            ));
        }
        let mut updated = reference.clone();
        updated.key_version = Self::response_key_version(&response.body);
        Ok(updated)
    }
}

#[async_trait]
impl<C> OAuthTokenMaterialPort for VaultOAuthTokenMaterialStore<C>
where
    C: HttpClientPort + 'static,
{
    fn owns_reference(&self, reference: &SecretReference) -> bool {
        reference.provider == SecretProviderKind::Vault
            && reference.path == Self::reference_path(reference.id)
    }

    async fn write_tokens(
        &self,
        _tenant_id: TenantId,
        reference_id: SecretReferenceId,
        tokens: &TokenBundle,
    ) -> Result<SecretReference> {
        let reference = self.reference_for(reference_id);
        self.write_at(&reference, tokens).await
    }

    async fn read_tokens(
        &self,
        _tenant_id: TenantId,
        reference: &SecretReference,
    ) -> Result<Option<OAuthTokenMaterial>> {
        let url = self.secret_url(reference)?;
        let response = self
            .client
            .send(&self.authorize(HttpRequest::get(url)))
            .await?;
        match response.status {
            404 => Ok(None),
            200..=299 => self.decode_tokens(&response.body).map(Some),
            status => Err(AppError::external_service(
                "vault",
                format!("Vault OAuth token read returned HTTP status {status}"),
            )),
        }
    }

    async fn replace_tokens(
        &self,
        _tenant_id: TenantId,
        reference: &SecretReference,
        tokens: &TokenBundle,
    ) -> Result<SecretReference> {
        self.write_at(reference, tokens).await
    }

    async fn delete_tokens(&self, _tenant_id: TenantId, reference: &SecretReference) -> Result<()> {
        let url = self.secret_url(reference)?;
        let response = self
            .client
            .send(&self.authorize(HttpRequest::delete(url)))
            .await?;
        match response.status {
            200..=299 | 404 => Ok(()),
            status => Err(AppError::external_service(
                "vault",
                format!("Vault OAuth token delete returned HTTP status {status}"),
            )),
        }
    }

    async fn delete_unstored_tokens(
        &self,
        tenant_id: TenantId,
        reference_id: SecretReferenceId,
    ) -> Result<()> {
        let reference = self.reference_for(reference_id);
        self.delete_tokens(tenant_id, &reference).await
    }
}

fn valid_token_value(value: &str) -> bool {
    !value.is_empty() && value.len() <= 16 * 1024 && !value.chars().any(char::is_control)
}

fn validate_token_bundle(tokens: &TokenBundle) -> Result<()> {
    validate_token_parts(
        tokens.access_token().expose_secret(),
        tokens.refresh_token(),
        tokens.token_type(),
        tokens.scopes(),
    )
}

fn validate_token_parts(
    access_token: &str,
    refresh_token: Option<&TokenSecret>,
    token_type: &str,
    scopes: &[String],
) -> Result<()> {
    if !valid_token_value(access_token)
        || refresh_token.is_some_and(|token| !valid_token_value(token.expose_secret()))
        || token_type.len() > 64
        || !token_type.eq_ignore_ascii_case("bearer")
        || scopes.len() > 128
        || scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > 128
                || !scope.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'.' | b'_' | b'-')
                })
        })
    {
        return Err(AppError::invalid_field(
            "oauth_token_material",
            "invalid_format",
            "OAuth token material failed bounded format validation",
        ));
    }
    Ok(())
}

fn validate_vault_relative_path(field: &str, path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > 900
        || path.starts_with('/')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        || !path.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '/' | '_' | '-' | '.')
        })
    {
        return Err(AppError::invalid_field(
            field,
            "invalid_vault_path",
            "Vault paths must be normalized relative ASCII paths without traversal",
        ));
    }
    Ok(())
}

/// Operator-owned Vault KV resolver configuration.
///
/// `token` is injected from a bootstrapped secret source and is never read from
/// a tenant request. The endpoint must have already passed safe URL parsing;
/// the guarded client re-validates DNS/IP/redirects at every send.
pub struct VaultSecretResolver<C>
where
    C: HttpClientPort,
{
    client: GuardedHttpClient<C>,
    base_url: mas_domain::value_objects::SafeUrl,
    token: SecretString,
    namespace: Option<String>,
    value_field: String,
}

impl<C> fmt::Debug for VaultSecretResolver<C>
where
    C: HttpClientPort,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VaultSecretResolver")
            .field("base_url", &self.base_url)
            .field("token", &"[redacted]")
            .field("namespace", &self.namespace)
            .field("value_field", &self.value_field)
            .finish_non_exhaustive()
    }
}

impl<C> VaultSecretResolver<C>
where
    C: HttpClientPort,
{
    /// Builds a production Vault resolver. Only a TLS safe URL is accepted;
    /// Vault traffic still goes through guarded egress at request time.
    pub fn new(
        client: GuardedHttpClient<C>,
        base_url: mas_domain::value_objects::SafeUrl,
        token: SecretString,
        namespace: Option<String>,
        value_field: impl Into<String>,
    ) -> Result<Self> {
        if !base_url.is_tls() || base_url.is_internal_endpoint() {
            return Err(AppError::invalid_field(
                "vault_base_url",
                "tls_required",
                "Vault base URL must be a tenant-safe public https endpoint",
            ));
        }
        if token.is_empty() {
            return Err(AppError::invalid_field(
                "vault_token",
                "required",
                "Vault token must not be empty",
            ));
        }
        let namespace = namespace.filter(|value| !value.is_empty());
        if namespace.as_deref().is_some_and(|value| {
            value.len() > 256 || value.chars().any(|character| character.is_control())
        }) {
            return Err(AppError::invalid_field(
                "vault_namespace",
                "invalid_format",
                "Vault namespace must be at most 256 printable characters",
            ));
        }
        let value_field = value_field.into();
        if value_field.is_empty()
            || value_field.len() > 128
            || !value_field.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
        {
            return Err(AppError::invalid_field(
                "vault_value_field",
                "invalid_format",
                "Vault value field must be 1..=128 ASCII alphanumeric, underscore, or dash",
            ));
        }
        Ok(Self {
            client,
            base_url,
            token,
            namespace,
            value_field,
        })
    }

    /// Builds the Vault API URL without allowing a reference path to replace
    /// the configured authority or traverse out of `/v1`. The only query this
    /// method adds is an encoded, validated Vault KV version selected from
    /// canonical metadata.
    fn secret_url(
        &self,
        reference: &SecretReference,
    ) -> Result<mas_domain::value_objects::SafeUrl> {
        let path = reference.path.trim();
        if path.is_empty()
            || path.len() > 1024
            || path.starts_with('/')
            || path
                .split('/')
                .any(|segment| segment.is_empty() || segment == "." || segment == "..")
            || !path.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '/' | '_' | '-' | '.')
            })
        {
            return Err(AppError::invalid_field(
                "secret_reference.path",
                "invalid_vault_path",
                "Vault paths must be normalized relative ASCII paths without traversal",
            ));
        }
        let mut url = self.base_url.as_url().clone();
        let prefix = url.path().trim_end_matches('/');
        url.set_path(&format!("{prefix}/v1/{path}"));
        url.set_query(None);
        url.set_fragment(None);
        if let Some(version) = &reference.key_version {
            if version.is_empty()
                || version.len() > 128
                || !version
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            {
                return Err(AppError::invalid_field(
                    "secret_reference.key_version",
                    "invalid_format",
                    "Vault secret key version must be a bounded safe token",
                ));
            }
            // Vault KV v2 accepts the requested historical version in a query
            // parameter. The value is encoded by Url rather than concatenated,
            // so metadata cannot introduce a second query key or fragment.
            url.query_pairs_mut().append_pair("version", version);
        }
        mas_domain::value_objects::SafeUrl::parse(url.as_str())
    }

    fn decode_value(&self, body: &[u8]) -> Result<SecretString> {
        let document: Value = serde_json::from_slice(body).map_err(|_| {
            AppError::external_service("vault", "Vault returned malformed JSON for a secret read")
        })?;
        // Vault KV v2 nests at data.data; KV v1 uses data directly. We accept
        // only a string under the operator-selected value field.
        let value = document
            .pointer("/data/data")
            .or_else(|| document.pointer("/data"))
            .and_then(Value::as_object)
            .and_then(|object| object.get(&self.value_field))
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AppError::external_service(
                    "vault",
                    "Vault response did not contain the configured secret value field",
                )
            })?;
        if value.is_empty() || value.len() > 64 * 1024 {
            return Err(AppError::external_service(
                "vault",
                "Vault returned an empty or oversized secret value",
            ));
        }
        Ok(SecretString::new(value))
    }
}

#[async_trait]
impl<C> SecretResolverPort for VaultSecretResolver<C>
where
    C: HttpClientPort + 'static,
{
    async fn resolve(&self, reference: &SecretReference) -> Result<Option<SecretString>> {
        if reference.provider != SecretProviderKind::Vault {
            return Ok(None);
        }
        let url = self.secret_url(reference)?;
        let mut request = HttpRequest::get(url).with_header("x-vault-token", self.token.expose());
        if let Some(namespace) = &self.namespace {
            request = request.with_header("x-vault-namespace", namespace);
        }
        let response = self.client.send(&request).await?;
        match response.status {
            200..=299 => self.decode_value(&response.body).map(Some),
            404 => Ok(None),
            status => Err(AppError::external_service(
                "vault",
                format!("Vault secret read returned HTTP status {status}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_guard::{HttpClientPort, HttpResponse};
    use mas_common::timestamps::Timestamp;
    use mas_domain::{SecretAvailability, SecretRotationState};

    #[derive(Debug)]
    struct FakeHttp;

    #[async_trait]
    impl HttpClientPort for FakeHttp {
        async fn send(&self, _request: &HttpRequest) -> Result<HttpResponse> {
            Ok(HttpResponse {
                status: 200,
                headers: vec![],
                body: br#"{"data":{"data":{"value":"material"}}}"#.to_vec(),
            })
        }
    }

    fn reference(path: &str) -> SecretReference {
        SecretReference {
            id: SecretReferenceId::new(),
            provider: SecretProviderKind::Vault,
            path: path.to_owned(),
            key_version: Some("7".to_owned()),
            rotation_state: SecretRotationState::Current,
            availability: SecretAvailability::Available,
        }
    }

    #[tokio::test]
    async fn vault_adapter_uses_guarded_transport_and_never_debugs_token() {
        let resolver = VaultSecretResolver::new(
            GuardedHttpClient::new(FakeHttp, crate::http_guard::EgressPolicy::default()),
            mas_domain::value_objects::SafeUrl::parse("https://vault.example.test").expect("url"),
            SecretString::new("vault-token-material"),
            None,
            "value",
        )
        .expect("resolver");
        let secret = resolver
            .resolve(&reference("secret/data/hmac"))
            .await
            .expect("resolve");
        assert_eq!(secret.expect("present").expose(), "material");
        assert!(!format!("{resolver:?}").contains("vault-token-material"));
        assert!(resolver.resolve(&reference("../bad")).await.is_err());
        let mut versioned = reference("secret/data/hmac");
        versioned.key_version = Some("7".to_owned());
        assert!(resolver
            .secret_url(&versioned)
            .expect("versioned Vault URL")
            .as_str()
            .contains("version=7"));
        let _ = Timestamp::now();
    }
}
