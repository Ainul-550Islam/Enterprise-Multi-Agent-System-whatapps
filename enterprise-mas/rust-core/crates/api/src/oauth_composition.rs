//! PURPOSE: Production composition for connector-bound OAuth PKCE lifecycle.
//!
//! Owning layer: `mas-api` process bootstrap boundary.
//!
//! Production role: compose static supported-provider profiles, one dedicated
//! `mas_oauth` callback pool, durable PostgreSQL state/reference adapters,
//! audited Vault PKCE/token material adapters, and guarded OAuth token HTTP.
//!
//! Security boundary: OAuth provider endpoints are source-owned allowlists;
//! callback URLs derive from one operator-owned public API base; raw verifier
//! and token bytes remain in Vault-backed redacting wrappers; public callbacks
//! never reuse the request/RLS pool or accept a tenant/redirect/provider URL.
//!
//! Data ownership: PostgreSQL owns state/reference/credential binding metadata;
//! Vault owns PKCE and token material; provider systems own authorization code
//! and token responses. This composition owns neither raw material nor state.
//!
//! Prohibited responsibilities: in-memory OAuth fallback, client-secret flow,
//! tenant-configured OAuth endpoints, generic Vault reads, public internal
//! URLs, unguarded outbound HTTP, or startup that leaves a live callback route
//! without the durable credential lifecycle behind it.

use std::collections::BTreeSet;
use std::sync::Arc;

use mas_application::oauth_authorization_service::OAuthAuthorizationService;
use mas_integrations::durable_oauth::{
    DurableOAuthAuthorizationService, OAuthAuthorizationStatePort, OAuthCredentialPort,
    OAuthPkceSecretPort, OAuthTokenMaterialPort,
};
use mas_integrations::http_guard::{EgressPolicy, GuardedHttpClient};
use mas_integrations::oauth::TokenEndpointPort;
use mas_integrations::oauth_cleanup::{OAuthTokenCleanupStorePort, OAuthTokenCleanupWorker};
use mas_integrations::oauth_http::HttpTokenEndpoint;
use mas_integrations::oauth_materialization::{
    OAuthMaterializationReconciler, OAuthMaterializationStorePort,
};
use mas_integrations::oauth_providers::{
    build_supported_oauth_config, OAuthProviderCatalogPort, OAuthProviderProfile,
    StaticOAuthProviderCatalog,
};
use mas_integrations::oauth_refresh::{OAuthTokenRefreshStorePort, OAuthTokenRefreshWorker};
use mas_integrations::reqwest_egress::ReqwestEgress;
use mas_integrations::secrets::{
    AuditedOAuthPkceMaterialStore, AuditedOAuthTokenMaterialStore, VaultOAuthPkceMaterialStore,
    VaultOAuthTokenMaterialStore,
};
use mas_persistence::repositories::{
    PostgresAuditLog, PostgresConnectorStore, PostgresOAuthCredentialStore,
    PostgresOAuthMaterializationStore, PostgresOAuthPkceSecretStore, PostgresOAuthStateStore,
    PostgresOAuthTokenCleanupStore, PostgresOAuthTokenRefreshStore,
};
use mas_security::{AuditSinkPort, SecretString};
use sqlx::PgPool;

/// Fully composed OAuth runtime services returned to API-process bootstrap.
pub type OAuthComposition = (
    Arc<OAuthAuthorizationService>,
    Arc<DurableOAuthAuthorizationService>,
    Arc<OAuthTokenRefreshWorker>,
    Arc<OAuthTokenCleanupWorker>,
    Arc<OAuthMaterializationReconciler>,
);

/// Composes both the authenticated authorization-start service and the public
/// callback façade. `oauth_pool` must authenticate as `mas_oauth`.
pub fn compose(
    merger: &mas_common::config::Merger<'_>,
    request_pool: PgPool,
    oauth_pool: PgPool,
    refresh_pool: PgPool,
    audit: Arc<PostgresAuditLog>,
) -> Result<OAuthComposition, String> {
    let catalog = Arc::new(configure_catalog(merger)?);
    let vault = vault_configuration(merger)?;
    let vault_policy = vault_policy(&vault.base_url)?;

    let pkce_prefix = merger.string(
        "MAS_VAULT_OAUTH_PKCE_PATH_PREFIX",
        "secrets.vault.oauth_pkce_path_prefix",
        "secret/data/mas/oauth-pkce",
    );
    let token_prefix = merger.string(
        "MAS_VAULT_OAUTH_TOKEN_PATH_PREFIX",
        "secrets.vault.oauth_token_path_prefix",
        "secret/data/mas/oauth-tokens",
    );
    let security_audit: Arc<dyn AuditSinkPort> = audit.clone();
    let pkce_material = AuditedOAuthPkceMaterialStore::new(
        VaultOAuthPkceMaterialStore::new(
            GuardedHttpClient::new(
                ReqwestEgress::new(vault_policy.clone()),
                vault_policy.clone(),
            ),
            vault.base_url.clone(),
            SecretString::new(vault.token.clone()),
            vault.namespace.clone(),
            pkce_prefix,
        )
        .map_err(|error| format!("OAuth Vault PKCE configuration: {error}"))?,
        security_audit.clone(),
        "oauth-pkce-lifecycle",
    )
    .map_err(|error| format!("OAuth PKCE audit configuration: {error}"))?;
    let token_material: Arc<dyn OAuthTokenMaterialPort> = Arc::new(
        AuditedOAuthTokenMaterialStore::new(
            VaultOAuthTokenMaterialStore::new(
                GuardedHttpClient::new(ReqwestEgress::new(vault_policy.clone()), vault_policy),
                vault.base_url,
                SecretString::new(vault.token),
                vault.namespace,
                token_prefix,
            )
            .map_err(|error| format!("OAuth Vault token configuration: {error}"))?,
            security_audit,
            "oauth-token-lifecycle",
        )
        .map_err(|error| format!("OAuth token audit configuration: {error}"))?,
    );

    let states: Arc<dyn OAuthAuthorizationStatePort> = Arc::new(PostgresOAuthStateStore::new(
        request_pool.clone(),
        oauth_pool.clone(),
    ));
    let pkce: Arc<dyn OAuthPkceSecretPort> = Arc::new(PostgresOAuthPkceSecretStore::new(
        request_pool.clone(),
        oauth_pool.clone(),
        pkce_material,
    ));
    let credentials: Arc<dyn OAuthCredentialPort> = Arc::new(PostgresOAuthCredentialStore::new(
        oauth_pool,
        token_material.clone(),
    ));
    let token_policy = token_policy(&catalog)?;
    let endpoint: Arc<dyn TokenEndpointPort> = Arc::new(HttpTokenEndpoint::new(
        GuardedHttpClient::new(ReqwestEgress::new(token_policy.clone()), token_policy),
    ));
    let refresh_store: Arc<dyn OAuthTokenRefreshStorePort> =
        Arc::new(PostgresOAuthTokenRefreshStore::new(refresh_pool.clone()));
    let cleanup_store: Arc<dyn OAuthTokenCleanupStorePort> =
        Arc::new(PostgresOAuthTokenCleanupStore::new(refresh_pool.clone()));
    let materialization_store: Arc<dyn OAuthMaterializationStorePort> =
        Arc::new(PostgresOAuthMaterializationStore::new(refresh_pool));
    let refresh_worker = Arc::new(OAuthTokenRefreshWorker::new(
        catalog.clone(),
        endpoint.clone(),
        token_material.clone(),
        refresh_store,
    ));
    let cleanup_worker = Arc::new(OAuthTokenCleanupWorker::new(
        token_material.clone(),
        cleanup_store,
    ));
    let materialization_reconciler = Arc::new(OAuthMaterializationReconciler::new(
        token_material,
        materialization_store,
    ));
    let catalog_port: Arc<dyn OAuthProviderCatalogPort> = catalog;
    let callbacks = Arc::new(DurableOAuthAuthorizationService::new(
        catalog_port,
        states,
        pkce,
        endpoint,
        credentials,
    ));
    let authorization = Arc::new(OAuthAuthorizationService::new(
        Box::new(PostgresConnectorStore::new(request_pool)),
        callbacks.clone(),
        Box::new(audit),
    ));
    Ok((
        authorization,
        callbacks,
        refresh_worker,
        cleanup_worker,
        materialization_reconciler,
    ))
}

#[derive(Debug, Clone)]
struct VaultConfiguration {
    base_url: mas_domain::value_objects::SafeUrl,
    token: String,
    namespace: Option<String>,
}

fn vault_configuration(
    merger: &mas_common::config::Merger<'_>,
) -> Result<VaultConfiguration, String> {
    let raw_url = merger.string("MAS_VAULT_URL", "secrets.vault.url", "");
    if raw_url.trim().is_empty() {
        return Err(
            "OAuth production composition requires MAS_VAULT_URL / secrets.vault.url".to_owned(),
        );
    }
    let base_url = mas_domain::value_objects::SafeUrl::parse(&raw_url)
        .map_err(|error| format!("OAuth Vault URL: {error}"))?;
    if !base_url.is_tls() || base_url.is_internal_endpoint() {
        return Err(
            "OAuth Vault URL must be a public HTTPS endpoint; private/internal Vault endpoints are not accepted by this deployment boundary"
                .to_owned(),
        );
    }
    let token = merger.string("MAS_VAULT_TOKEN", "secrets.vault.token", "");
    if token.is_empty() {
        return Err(
            "OAuth production composition requires MAS_VAULT_TOKEN / secrets.vault.token"
                .to_owned(),
        );
    }
    let namespace = merger.string("MAS_VAULT_NAMESPACE", "secrets.vault.namespace", "");
    Ok(VaultConfiguration {
        base_url,
        token,
        namespace: (!namespace.is_empty()).then_some(namespace),
    })
}

fn vault_policy(vault_url: &mas_domain::value_objects::SafeUrl) -> Result<EgressPolicy, String> {
    let port = vault_url
        .as_url()
        .port_or_known_default()
        .ok_or_else(|| "OAuth Vault URL has no effective HTTPS port after validation".to_owned())?;
    Ok(EgressPolicy {
        https_only: true,
        permitted_plain_http_hosts: BTreeSet::new(),
        allowed_ports: Some(BTreeSet::from([port])),
        hostname_allowlist: Some(BTreeSet::from([vault_url.host_str().to_ascii_lowercase()])),
        max_redirects: 0,
        max_body_bytes: 128 * 1024,
        allow_internal_endpoints: false,
    })
}

fn token_policy(catalog: &StaticOAuthProviderCatalog) -> Result<EgressPolicy, String> {
    let hosts = catalog.token_hosts();
    if hosts.is_empty() {
        return Err(
            "OAuth production composition has no enabled provider token endpoint".to_owned(),
        );
    }
    Ok(EgressPolicy {
        https_only: true,
        permitted_plain_http_hosts: BTreeSet::new(),
        allowed_ports: Some(BTreeSet::from([443])),
        hostname_allowlist: Some(hosts),
        max_redirects: 0,
        max_body_bytes: 128 * 1024,
        allow_internal_endpoints: false,
    })
}

fn configure_catalog(
    merger: &mas_common::config::Merger<'_>,
) -> Result<StaticOAuthProviderCatalog, String> {
    let callback_base = merger.string(
        "MAS_OAUTH_CALLBACK_BASE_URL",
        "integrations.oauth.callback_base_url",
        "",
    );
    let registrations = [
        (
            "slack",
            "MAS_OAUTH_SLACK_CLIENT_ID",
            "integrations.oauth.slack.client_id",
            "MAS_OAUTH_SLACK_SCOPES",
            "integrations.oauth.slack.scopes",
            &["channels:read", "chat:write"] as &[&str],
        ),
        (
            "salesforce",
            "MAS_OAUTH_SALESFORCE_CLIENT_ID",
            "integrations.oauth.salesforce.client_id",
            "MAS_OAUTH_SALESFORCE_SCOPES",
            "integrations.oauth.salesforce.scopes",
            &["api", "refresh_token"] as &[&str],
        ),
        (
            "jira",
            "MAS_OAUTH_JIRA_CLIENT_ID",
            "integrations.oauth.jira.client_id",
            "MAS_OAUTH_JIRA_SCOPES",
            "integrations.oauth.jira.scopes",
            &["offline_access", "read:jira-user"] as &[&str],
        ),
    ];
    let configured = registrations
        .iter()
        .filter_map(
            |(provider, client_env, client_key, scopes_env, scopes_key, defaults)| {
                let client_id = merger.string(client_env, client_key, "");
                (!client_id.is_empty()).then_some((
                    *provider,
                    client_id,
                    *scopes_env,
                    *scopes_key,
                    *defaults,
                ))
            },
        )
        .collect::<Vec<_>>();
    if configured.is_empty() {
        return Err(
            "OAuth production composition requires at least one configured MAS_OAUTH_<PROVIDER>_CLIENT_ID"
                .to_owned(),
        );
    }
    if callback_base.is_empty() {
        return Err(
            "OAuth production composition requires MAS_OAUTH_CALLBACK_BASE_URL / integrations.oauth.callback_base_url"
                .to_owned(),
        );
    }
    let callback_base = mas_domain::value_objects::SafeUrl::parse(&callback_base)
        .map_err(|error| format!("OAuth callback base URL: {error}"))?;
    if !callback_base.is_tls() || callback_base.is_internal_endpoint() {
        return Err("OAuth callback base URL must be a public HTTPS endpoint".to_owned());
    }
    let mut profiles = Vec::with_capacity(configured.len());
    for (provider, client_id, scopes_env, scopes_key, defaults) in configured {
        let scopes = merger
            .string_list(scopes_env, scopes_key, defaults)
            .map_err(|error| format!("OAuth {provider} scopes: {error}"))?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let callback = callback_url(&callback_base, provider)?;
        let config = build_supported_oauth_config(provider, client_id, callback, scopes)
            .map_err(|error| format!("OAuth {provider} profile: {error}"))?;
        profiles.push(
            OAuthProviderProfile::new(provider, config)
                .map_err(|error| format!("OAuth {provider} profile: {error}"))?,
        );
    }
    StaticOAuthProviderCatalog::new(profiles)
        .map_err(|error| format!("OAuth provider catalog: {error}"))
}

fn callback_url(
    callback_base: &mas_domain::value_objects::SafeUrl,
    provider: &str,
) -> Result<mas_domain::value_objects::SafeUrl, String> {
    let mut url = callback_base.as_url().clone();
    let prefix = url.path().trim_end_matches('/');
    url.set_path(&format!("{prefix}/v1/public/oauth/{provider}/callback"));
    url.set_query(None);
    url.set_fragment(None);
    mas_domain::value_objects::SafeUrl::parse(url.as_str())
        .map_err(|error| format!("OAuth callback URL for {provider}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_url_is_fixed_under_operator_public_base() {
        let base = mas_domain::value_objects::SafeUrl::parse("https://api.example.test/root")
            .expect("base");
        assert_eq!(
            callback_url(&base, "slack").expect("callback").as_str(),
            "https://api.example.test/root/v1/public/oauth/slack/callback"
        );
    }

    #[test]
    fn vault_policy_has_one_public_authority_and_no_redirects() {
        let vault =
            mas_domain::value_objects::SafeUrl::parse("https://vault.example.test").expect("vault");
        let policy = vault_policy(&vault).expect("policy");
        assert_eq!(policy.max_redirects, 0);
        assert_eq!(
            policy.hostname_allowlist,
            Some(BTreeSet::from(["vault.example.test".to_owned()]))
        );
    }
}
