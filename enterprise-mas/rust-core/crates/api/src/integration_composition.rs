//! PURPOSE: Production composition for tenant connector management, outbound
//! webhook relay, and durable notification delivery.
//!
//! Owning layer: `mas-api` process bootstrap boundary.
//!
//! Production role: assemble the one canonical `IntegrationService` over
//! request-scoped PostgreSQL stores, a bounded fenced outbound relay over the
//! dedicated `mas_integrations` role, and a bounded fenced notification worker
//! over the dedicated `mas_notifications` role. It configures every HTTP path
//! with hostname-pinned guarded egress and fails startup rather than replacing
//! a missing durable or Vault dependency with an in-memory implementation.
//!
//! Security boundary: request stores retain RLS authority; worker stores use
//! only deliberately narrow cross-tenant role pools. Vault is reachable only
//! through a hostname-pinned policy. Customer webhook/provider destinations
//! are public HTTPS destinations only, revalidated at every DNS resolution and
//! redirect hop; private, loopback, metadata, mapped-IPv6, and internal
//! destinations remain forbidden.
//!
//! Data ownership: PostgreSQL owns connector/credential/reference/webhook and
//! notification metadata plus leases/fences. Vault owns raw signing/API-key and
//! OAuth token bytes. This composition owns neither durable records nor secret
//! material.
//!
//! Prohibited responsibilities: handling HTTP requests, accepting tenant input
//! as operator egress policy, storing/cacheing secret bytes, adding unguarded
//! transport paths, implementing unsupported notification channels, or using
//! the ordinary request pool for relay/notification cross-tenant claims.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use mas_application::integration_service::IntegrationService;
use mas_integrations::durable_notifications::{
    DurableNotificationWorker, DurableNotificationWorkerConfig, InAppNotificationAdapter,
    RepresentedNotificationChannelAdapter, RepresentedNotificationRouter,
};
use mas_integrations::durable_webhooks::{DurableWebhookWorker, DurableWebhookWorkerConfig};
use mas_integrations::http_guard::{EgressPolicy, GuardedHttpClient};
use mas_integrations::provider_adapters::HttpProviderProbe;
use mas_integrations::registry::InMemoryProviderRegistry;
use mas_integrations::reqwest_egress::ReqwestEgress;
use mas_integrations::secrets::{
    AuditedOAuthTokenMaterialStore, CompositeConnectorCredentialResolver,
    OAuthTokenConnectorCredentialResolver, SecretGateConnectorCredentialResolver,
    SecretGateWebhookResolver, VaultOAuthTokenMaterialStore, VaultSecretResolver,
};
use mas_persistence::repositories::{
    PostgresConnectorCredentialLookup, PostgresConnectorStore, PostgresNotificationDeliveryStore,
    PostgresRelaySecretReferenceLookup, PostgresSecretReferenceStore, PostgresWebhookRelayStore,
    PostgresWebhookStore,
};
use mas_security::{AuditSinkPort, SecretGate, SecretString};
use sqlx::PgPool;

/// Concrete outbound relay type retained by API bootstrap so it cannot be
/// accidentally constructed around the request pool or an unguarded client.
pub type OutboundWebhookWorker = DurableWebhookWorker<
    PostgresWebhookRelayStore,
    SecretGateWebhookResolver<PostgresRelaySecretReferenceLookup>,
    ReqwestEgress,
>;

/// Concrete notification worker type retained by API bootstrap. The closed
/// router deliberately exposes only the currently represented in-app channel.
pub type NotificationDeliveryWorker =
    DurableNotificationWorker<PostgresNotificationDeliveryStore, RepresentedNotificationRouter>;

/// All integration-plane runtime components plus their bounded polling cadence.
pub struct IntegrationComposition {
    pub service: Arc<IntegrationService>,
    pub outbound_webhook_worker: Arc<OutboundWebhookWorker>,
    pub outbound_webhook_poll: Duration,
    pub notification_delivery_worker: Arc<NotificationDeliveryWorker>,
    pub notification_delivery_poll: Duration,
}

/// Composes the production integration plane.
///
/// `request_pool` must be the normal RLS-enforced `mas_app` pool. In contrast,
/// `integrations_pool` must authenticate as `mas_integrations`, and
/// `notifications_pool` must authenticate as `mas_notifications`; neither
/// worker has a fallback to the request pool.
pub fn compose(
    merger: &mas_common::config::Merger<'_>,
    request_pool: PgPool,
    integrations_pool: PgPool,
    notifications_pool: PgPool,
    audit: Arc<mas_persistence::repositories::audit::PostgresAuditLog>,
) -> Result<IntegrationComposition, String> {
    let vault = vault_configuration(merger)?;
    let vault_policy = vault_policy(&vault.base_url)?;
    let outbound_policy = customer_destination_policy(merger)?;

    let generic_secret_resolver = Arc::new(
        VaultSecretResolver::new(
            GuardedHttpClient::new(
                ReqwestEgress::new(vault_policy.clone()),
                vault_policy.clone(),
            ),
            vault.base_url.clone(),
            SecretString::new(vault.token.clone()),
            vault.namespace.clone(),
            vault.value_field.clone(),
        )
        .map_err(|error| format!("integration Vault configuration: {error}"))?,
    );
    let security_audit: Arc<dyn AuditSinkPort> = audit.clone();
    // There is intentionally no material cache. Every probe/delivery resolves
    // through the secret manager so revocation and rotation take effect on the
    // next operation rather than after an arbitrary process-local TTL.
    let secret_gate = Arc::new(SecretGate::new(
        generic_secret_resolver,
        security_audit.clone(),
    ));

    let credential_lookup = Arc::new(PostgresConnectorCredentialLookup::new(request_pool.clone()));
    let generic_credentials = SecretGateConnectorCredentialResolver::new(
        credential_lookup.clone(),
        secret_gate.clone(),
        "integration-connector-probe",
    )
    .map_err(|error| format!("connector generic credential resolver: {error}"))?;
    let oauth_token_prefix = merger.string(
        "MAS_VAULT_OAUTH_TOKEN_PATH_PREFIX",
        "secrets.vault.oauth_token_path_prefix",
        "secret/data/mas/oauth-tokens",
    );
    let oauth_material = Arc::new(
        AuditedOAuthTokenMaterialStore::new(
            VaultOAuthTokenMaterialStore::new(
                GuardedHttpClient::new(
                    ReqwestEgress::new(vault_policy.clone()),
                    vault_policy.clone(),
                ),
                vault.base_url.clone(),
                SecretString::new(vault.token.clone()),
                vault.namespace.clone(),
                oauth_token_prefix,
            )
            .map_err(|error| format!("connector OAuth Vault configuration: {error}"))?,
            security_audit,
            "integration-connector-oauth-probe",
        )
        .map_err(|error| format!("connector OAuth material audit configuration: {error}"))?,
    );
    let oauth_credentials =
        OAuthTokenConnectorCredentialResolver::new(credential_lookup, oauth_material);
    let probe_credentials =
        CompositeConnectorCredentialResolver::new(generic_credentials, oauth_credentials);
    let probe = HttpProviderProbe::new(
        GuardedHttpClient::new(
            ReqwestEgress::new(outbound_policy.clone()),
            outbound_policy.clone(),
        ),
        probe_credentials,
    );

    let connector_store = PostgresConnectorStore::new(request_pool.clone());
    let webhook_store = PostgresWebhookStore::new(request_pool.clone());
    let secret_references = PostgresSecretReferenceStore::new(request_pool);
    let service = Arc::new(IntegrationService::new(
        Box::new(InMemoryProviderRegistry::with_builtin()),
        Box::new(connector_store),
        Box::new(probe),
        Box::new(webhook_store.clone()),
        Box::new(secret_references),
        Box::new(webhook_store),
        Box::new(audit.clone()),
    ));

    let relay_references = Arc::new(PostgresRelaySecretReferenceLookup::new(
        integrations_pool.clone(),
    ));
    let webhook_signing =
        SecretGateWebhookResolver::new(relay_references, secret_gate, "outbound-webhook-relay")
            .map_err(|error| format!("outbound webhook signing resolver: {error}"))?;
    let outbound_webhook_worker = Arc::new(
        DurableWebhookWorker::new(
            PostgresWebhookRelayStore::new(integrations_pool),
            webhook_signing,
            GuardedHttpClient::new(
                ReqwestEgress::new(outbound_policy),
                customer_destination_policy(merger)?,
            ),
            outbound_worker_config(merger)?,
        )
        .map_err(|error| format!("outbound webhook worker configuration: {error}"))?,
    );
    let outbound_webhook_poll = polling_interval(
        merger,
        "MAS_OUTBOUND_WEBHOOK_POLL_SECONDS",
        "integrations.outbound_webhooks.poll_seconds",
        5,
        "outbound webhook",
    )?;

    let notification_router = RepresentedNotificationRouter::new([
        Arc::new(InAppNotificationAdapter) as Arc<dyn RepresentedNotificationChannelAdapter>,
    ])
    .map_err(|error| format!("notification channel representation: {error}"))?;
    let notification_delivery_worker = Arc::new(
        DurableNotificationWorker::new(
            PostgresNotificationDeliveryStore::new(notifications_pool),
            notification_router,
            notification_worker_config(merger)?,
        )
        .map_err(|error| format!("notification delivery worker configuration: {error}"))?,
    );
    let notification_delivery_poll = polling_interval(
        merger,
        "MAS_NOTIFICATION_DELIVERY_POLL_SECONDS",
        "integrations.notifications.poll_seconds",
        5,
        "notification delivery",
    )?;

    Ok(IntegrationComposition {
        service,
        outbound_webhook_worker,
        outbound_webhook_poll,
        notification_delivery_worker,
        notification_delivery_poll,
    })
}

#[derive(Clone)]
struct VaultConfiguration {
    base_url: mas_domain::value_objects::SafeUrl,
    token: String,
    namespace: Option<String>,
    value_field: String,
}

fn vault_configuration(
    merger: &mas_common::config::Merger<'_>,
) -> Result<VaultConfiguration, String> {
    let raw_url = merger.string("MAS_VAULT_URL", "secrets.vault.url", "");
    if raw_url.trim().is_empty() {
        return Err(
            "integration production composition requires MAS_VAULT_URL / secrets.vault.url"
                .to_owned(),
        );
    }
    let base_url = mas_domain::value_objects::SafeUrl::parse(&raw_url)
        .map_err(|error| format!("integration Vault URL: {error}"))?;
    if !base_url.is_tls() || base_url.is_internal_endpoint() {
        return Err(
            "integration Vault URL must be a public HTTPS endpoint; private/internal Vault endpoints are not accepted by this deployment boundary"
                .to_owned(),
        );
    }
    let token = merger.string("MAS_VAULT_TOKEN", "secrets.vault.token", "");
    if token.trim().is_empty() {
        return Err(
            "integration production composition requires MAS_VAULT_TOKEN / secrets.vault.token"
                .to_owned(),
        );
    }
    let namespace = merger.string("MAS_VAULT_NAMESPACE", "secrets.vault.namespace", "");
    let value_field = merger.string(
        "MAS_VAULT_VALUE_FIELD",
        "secrets.vault.value_field",
        "value",
    );
    Ok(VaultConfiguration {
        base_url,
        token,
        namespace: (!namespace.is_empty()).then_some(namespace),
        value_field,
    })
}

fn vault_policy(vault_url: &mas_domain::value_objects::SafeUrl) -> Result<EgressPolicy, String> {
    let port = vault_url.as_url().port_or_known_default().ok_or_else(|| {
        "integration Vault URL has no effective HTTPS port after validation".to_owned()
    })?;
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

/// Builds the explicitly operator-owned public egress policy for arbitrary
/// customer webhook/provider destinations. A hostname allowlist is purposely
/// absent because registered endpoints are tenant-specific, but all other
/// safeguards stay active and the only ports permitted are reviewed config.
fn customer_destination_policy(
    merger: &mas_common::config::Merger<'_>,
) -> Result<EgressPolicy, String> {
    let configured_ports = merger
        .string_list(
            "MAS_OUTBOUND_WEBHOOK_ALLOWED_PORTS",
            "integrations.outbound_webhooks.allowed_ports",
            &["443"],
        )
        .map_err(|error| format!("outbound webhook allowed ports: {error}"))?;
    let mut allowed_ports = BTreeSet::new();
    for raw_port in configured_ports {
        let port = raw_port.parse::<u16>().map_err(|_| {
            format!("outbound webhook allowed port '{raw_port}' is not a valid u16")
        })?;
        if port == 0 {
            return Err("outbound webhook allowed ports must not include zero".to_owned());
        }
        allowed_ports.insert(port);
    }
    if allowed_ports.is_empty() {
        return Err("outbound webhook allowed ports must not be empty".to_owned());
    }
    let max_body_bytes = merger
        .u64(
            "MAS_OUTBOUND_WEBHOOK_MAX_RESPONSE_BYTES",
            "integrations.outbound_webhooks.max_response_bytes",
            1_048_576,
        )
        .map_err(|error| format!("outbound webhook response cap: {error}"))?;
    let max_body_bytes = usize::try_from(max_body_bytes)
        .ok()
        .filter(|value| (1_024..=8 * 1_024 * 1_024).contains(value))
        .ok_or_else(|| {
            "outbound webhook response cap must be between 1024 and 8388608 bytes".to_owned()
        })?;
    let max_redirects = merger
        .u64(
            "MAS_OUTBOUND_WEBHOOK_MAX_REDIRECTS",
            "integrations.outbound_webhooks.max_redirects",
            3,
        )
        .map_err(|error| format!("outbound webhook redirect cap: {error}"))?;
    let max_redirects = u8::try_from(max_redirects)
        .ok()
        .filter(|value| *value <= 5)
        .ok_or_else(|| "outbound webhook redirect cap must be between 0 and 5".to_owned())?;
    Ok(EgressPolicy {
        https_only: true,
        permitted_plain_http_hosts: BTreeSet::new(),
        allowed_ports: Some(allowed_ports),
        hostname_allowlist: None,
        max_redirects,
        max_body_bytes,
        allow_internal_endpoints: false,
    })
}

fn outbound_worker_config(
    merger: &mas_common::config::Merger<'_>,
) -> Result<DurableWebhookWorkerConfig, String> {
    let config = DurableWebhookWorkerConfig {
        worker_id: worker_id(
            merger.string(
                "MAS_OUTBOUND_WEBHOOK_WORKER_ID",
                "integrations.outbound_webhooks.worker_id",
                "api-outbound-webhook",
            ),
            "outbound webhook",
        )?,
        batch_size: bounded_batch(
            merger,
            "MAS_OUTBOUND_WEBHOOK_BATCH_SIZE",
            "integrations.outbound_webhooks.batch_size",
            32,
            "outbound webhook",
        )?,
        lease: bounded_lease(
            merger,
            "MAS_OUTBOUND_WEBHOOK_LEASE_SECONDS",
            "integrations.outbound_webhooks.lease_seconds",
            60,
            "outbound webhook",
        )?,
    };
    config.validate().map_err(|error| error.to_string())?;
    // HTTP requests have a default 30-second cap. A shorter lease would let a
    // second worker claim an in-flight request, so preserve a positive guard
    // band even if a future caller changes the transport default.
    if config.lease <= Duration::from_secs(30) {
        return Err(
            "outbound webhook worker lease must exceed the 30-second outbound request budget"
                .to_owned(),
        );
    }
    Ok(config)
}

fn notification_worker_config(
    merger: &mas_common::config::Merger<'_>,
) -> Result<DurableNotificationWorkerConfig, String> {
    let config = DurableNotificationWorkerConfig {
        worker_id: worker_id(
            merger.string(
                "MAS_NOTIFICATION_DELIVERY_WORKER_ID",
                "integrations.notifications.worker_id",
                "api-notification-delivery",
            ),
            "notification delivery",
        )?,
        batch_size: bounded_batch(
            merger,
            "MAS_NOTIFICATION_DELIVERY_BATCH_SIZE",
            "integrations.notifications.batch_size",
            32,
            "notification delivery",
        )?,
        lease: bounded_lease(
            merger,
            "MAS_NOTIFICATION_DELIVERY_LEASE_SECONDS",
            "integrations.notifications.lease_seconds",
            60,
            "notification delivery",
        )?,
    };
    config.validate().map_err(|error| error.to_string())?;
    Ok(config)
}

fn worker_id(base: String, plane: &str) -> Result<String, String> {
    if base.is_empty()
        || base.len() > 110
        || !base
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(format!(
            "{plane} worker id must be a 1..=110 ASCII token using letters, digits, dot, underscore, or dash"
        ));
    }
    // A worker name is a durable lease owner. The process suffix prevents two
    // local replicas with the same operator base id from appearing as one owner
    // in diagnostics while retaining a stable, non-secret operator prefix.
    Ok(format!("{base}-{}", std::process::id()))
}

fn bounded_batch(
    merger: &mas_common::config::Merger<'_>,
    environment_key: &str,
    path: &str,
    default: u64,
    plane: &str,
) -> Result<u32, String> {
    merger
        .u64(environment_key, path, default)
        .map_err(|error| format!("{plane} batch configuration: {error}"))
        .and_then(|value| {
            u32::try_from(value)
                .ok()
                .filter(|value| (1..=1_000).contains(value))
                .ok_or_else(|| format!("{plane} batch size must be between 1 and 1000"))
        })
}

fn bounded_lease(
    merger: &mas_common::config::Merger<'_>,
    environment_key: &str,
    path: &str,
    default: u64,
    plane: &str,
) -> Result<Duration, String> {
    merger
        .u64(environment_key, path, default)
        .map_err(|error| format!("{plane} lease configuration: {error}"))
        .and_then(|value| {
            if !(1..=300).contains(&value) {
                return Err(format!("{plane} lease must be between 1 and 300 seconds"));
            }
            Ok(Duration::from_secs(value))
        })
}

fn polling_interval(
    merger: &mas_common::config::Merger<'_>,
    environment_key: &str,
    path: &str,
    default: u64,
    plane: &str,
) -> Result<Duration, String> {
    merger
        .u64(environment_key, path, default)
        .map_err(|error| format!("{plane} poll configuration: {error}"))
        .and_then(|value| {
            if !(1..=3_600).contains(&value) {
                return Err(format!(
                    "{plane} poll interval must be between 1 and 3600 seconds"
                ));
            }
            Ok(Duration::from_secs(value))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_customer_policy_has_no_private_or_internal_escape_hatch() {
        let layered: toml::Value =
            toml::from_str("[integrations.outbound_webhooks]\nallowed_ports = [\"443\"]\n")
                .expect("toml");
        let merger = mas_common::config::Merger::new(&layered);
        let policy = customer_destination_policy(&merger).expect("policy");
        assert!(policy.https_only);
        assert!(!policy.allow_internal_endpoints);
        assert!(policy.permitted_plain_http_hosts.is_empty());
        assert!(policy.hostname_allowlist.is_none());
        assert_eq!(policy.allowed_ports, Some(BTreeSet::from([443])));
    }

    #[test]
    fn worker_ids_are_bounded_before_process_suffixing() {
        assert!(worker_id("relay".to_owned(), "relay").is_ok());
        assert!(worker_id("bad id".to_owned(), "relay").is_err());
        assert!(worker_id("a".repeat(111), "relay").is_err());
    }
}
