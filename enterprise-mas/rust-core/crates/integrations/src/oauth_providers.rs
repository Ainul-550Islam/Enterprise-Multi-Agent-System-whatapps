//! PURPOSE: Static, allowlisted OAuth provider profiles and catalog boundary.
//!
//! Owning layer: `mas-integrations` provider-policy boundary.
//!
//! Production role: turn operator-supplied public client identifiers, callback
//! URLs, and bounded scopes into OAuth 2.0 PKCE configurations only for
//! explicitly supported providers with fixed authorization/token authorities.
//!
//! Security boundary: tenant requests cannot name OAuth endpoints, redirect
//! URIs, client ids, extra authorization parameters, or scopes. Dynamic URLs
//! and unknown providers are rejected before a state/verifier is created.
//!
//! Data ownership: operators own public OAuth registration metadata; the
//! durable OAuth state store owns one-shot state metadata; secret managers own
//! PKCE and token bytes. This module owns no durable data or credentials.
//!
//! Prohibited responsibilities: client-secret support, raw token exchange,
//! tenant-configured endpoint discovery, arbitrary scope escalation, or a
//! fallback profile that pretends an unsupported provider is OAuth capable.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_domain::value_objects::SafeUrl;

use crate::oauth::OAuth2Config;

/// Immutable, built-in provider policy. Endpoints and extra parameters are
/// source-owned so an administrator cannot accidentally turn OAuth into a
/// generic SSRF client through configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OAuthProviderDefinition {
    pub key: &'static str,
    pub authorization_endpoint: &'static str,
    pub token_endpoint: &'static str,
    pub permitted_scopes: &'static [&'static str],
    pub required_authorization_parameters: &'static [(&'static str, &'static str)],
}

const SLACK_SCOPES: &[&str] = &[
    "channels:read",
    "chat:write",
    "groups:read",
    "im:read",
    "mpim:read",
    "users:read",
];
const SALESFORCE_SCOPES: &[&str] = &["api", "offline_access", "refresh_token"];
const JIRA_SCOPES: &[&str] = &[
    "offline_access",
    "read:jira-user",
    "read:jira-work",
    "write:jira-work",
];

const SUPPORTED: &[OAuthProviderDefinition] = &[
    OAuthProviderDefinition {
        key: "slack",
        authorization_endpoint: "https://slack.com/oauth/v2/authorize",
        token_endpoint: "https://slack.com/api/oauth.v2.access",
        permitted_scopes: SLACK_SCOPES,
        required_authorization_parameters: &[],
    },
    OAuthProviderDefinition {
        key: "salesforce",
        authorization_endpoint: "https://login.salesforce.com/services/oauth2/authorize",
        token_endpoint: "https://login.salesforce.com/services/oauth2/token",
        permitted_scopes: SALESFORCE_SCOPES,
        required_authorization_parameters: &[],
    },
    OAuthProviderDefinition {
        key: "jira",
        authorization_endpoint: "https://auth.atlassian.com/authorize",
        token_endpoint: "https://auth.atlassian.com/oauth/token",
        permitted_scopes: JIRA_SCOPES,
        required_authorization_parameters: &[("audience", "api.atlassian.com")],
    },
];

/// Returns the source-owned definition for a provider key.
#[must_use]
pub fn supported_oauth_provider(provider: &str) -> Option<&'static OAuthProviderDefinition> {
    SUPPORTED
        .iter()
        .find(|definition| definition.key == provider)
}

/// Returns every source-owned OAuth provider definition in stable key order.
#[must_use]
pub const fn supported_oauth_providers() -> &'static [OAuthProviderDefinition] {
    SUPPORTED
}

/// Builds one enabled profile after applying fixed provider policy. `client_id`
/// is public OAuth registration metadata, not a client secret.
pub fn build_supported_oauth_config(
    provider: &str,
    client_id: impl Into<String>,
    redirect_uri: SafeUrl,
    scopes: BTreeSet<String>,
) -> Result<OAuth2Config> {
    let definition = supported_oauth_provider(provider).ok_or_else(|| {
        AppError::invalid_field(
            "provider",
            "oauth_not_supported",
            "the connector provider has no supported OAuth PKCE profile",
        )
    })?;
    if !redirect_uri.is_tls() || redirect_uri.is_internal_endpoint() {
        return Err(AppError::invalid_field(
            "oauth_redirect_uri",
            "tls_required",
            "OAuth callback URIs must be public HTTPS endpoints",
        ));
    }
    if scopes.is_empty() || scopes.len() > 16 {
        return Err(AppError::invalid_field(
            "oauth_scopes",
            "invalid_count",
            "OAuth provider scopes must contain between one and sixteen entries",
        ));
    }
    if scopes.iter().any(|scope| {
        !definition
            .permitted_scopes
            .iter()
            .any(|permitted| scope == permitted)
    }) {
        return Err(AppError::invalid_field(
            "oauth_scopes",
            "not_permitted",
            "OAuth configuration requested a scope outside the provider allowlist",
        ));
    }
    let authorization_endpoint = SafeUrl::parse(definition.authorization_endpoint)?;
    let token_endpoint = SafeUrl::parse(definition.token_endpoint)?;
    let parameters = definition
        .required_authorization_parameters
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect::<BTreeMap<_, _>>();
    OAuth2Config::new(
        client_id,
        authorization_endpoint,
        token_endpoint,
        redirect_uri,
        scopes,
    )?
    .with_authorization_parameters(parameters)
}

/// Non-secret runtime OAuth profile selected by a fixed provider key.
#[derive(Debug, Clone)]
pub struct OAuthProviderProfile {
    pub provider: String,
    pub config: OAuth2Config,
}

impl OAuthProviderProfile {
    pub fn new(provider: impl Into<String>, config: OAuth2Config) -> Result<Self> {
        let provider = provider.into();
        if supported_oauth_provider(&provider).is_none() {
            return Err(AppError::invalid_field(
                "provider",
                "oauth_not_supported",
                "OAuth profile provider is not supported by this build",
            ));
        }
        Ok(Self { provider, config })
    }
}

/// Runtime lookup boundary for provider-owned OAuth configuration.
#[async_trait]
pub trait OAuthProviderCatalogPort: fmt::Debug + Send + Sync {
    /// Returns a configured supported profile, or `None` when the operator did
    /// not enable that provider in this deployment. Unknown and disabled are
    /// deliberately indistinguishable to public callback callers.
    async fn config(&self, provider: &str) -> Result<Option<OAuth2Config>>;
}

/// Immutable in-process catalog assembled at production bootstrap.
#[derive(Debug, Clone, Default)]
pub struct StaticOAuthProviderCatalog {
    profiles: Arc<BTreeMap<String, OAuth2Config>>,
}

impl StaticOAuthProviderCatalog {
    pub fn new(profiles: impl IntoIterator<Item = OAuthProviderProfile>) -> Result<Self> {
        let mut values = BTreeMap::new();
        for profile in profiles {
            if values
                .insert(profile.provider.clone(), profile.config)
                .is_some()
            {
                return Err(AppError::invalid_field(
                    "oauth_profiles",
                    "duplicate_provider",
                    "OAuth provider profiles must not contain duplicate providers",
                ));
            }
        }
        Ok(Self {
            profiles: Arc::new(values),
        })
    }

    #[must_use]
    pub fn configured_providers(&self) -> Vec<String> {
        self.profiles.keys().cloned().collect()
    }

    #[must_use]
    pub fn token_hosts(&self) -> BTreeSet<String> {
        self.profiles
            .values()
            .map(|config| config.token_endpoint.host_str().to_ascii_lowercase())
            .collect()
    }
}

#[async_trait]
impl OAuthProviderCatalogPort for StaticOAuthProviderCatalog {
    async fn config(&self, provider: &str) -> Result<Option<OAuth2Config>> {
        Ok(self.profiles.get(provider).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_provider_policy_rejects_unknown_scope_and_provider() {
        let redirect = SafeUrl::parse("https://api.example.test/v1/public/oauth/slack/callback")
            .expect("redirect");
        assert!(build_supported_oauth_config(
            "slack",
            "public-client-id",
            redirect.clone(),
            BTreeSet::from(["chat:write".to_owned()]),
        )
        .is_ok());
        assert!(build_supported_oauth_config(
            "slack",
            "public-client-id",
            redirect,
            BTreeSet::from(["admin:everything".to_owned()]),
        )
        .is_err());
        assert!(supported_oauth_provider("unconfigured").is_none());
    }

    #[test]
    fn jira_profile_owns_required_audience_parameter() {
        let redirect = SafeUrl::parse("https://api.example.test/v1/public/oauth/jira/callback")
            .expect("redirect");
        let config = build_supported_oauth_config(
            "jira",
            "public-client-id",
            redirect,
            BTreeSet::from(["read:jira-user".to_owned()]),
        )
        .expect("config");
        let state = "a".repeat(64);
        let pkce = crate::oauth::PkcePair::generate();
        let url = config
            .authorization_url(&state, &pkce)
            .expect("authorization URL");
        assert!(url.contains("audience=api.atlassian.com"));
    }
}
