//! PURPOSE: Production composition for the canonical inbound-webhook plane.
//!
//! Owning layer: `mas-api` process bootstrap boundary.
//!
//! Production role: wire dedicated PostgreSQL callback/relay pools, canonical
//! endpoint and receipt stores, an audited SecretGate over guarded Vault egress,
//! and the signature-first ingress coordinator. Startup fails if any required
//! role URL or secret-manager configuration is absent or unsafe.
//!
//! Security boundary: `mas_inbound` handles only public route/receipt/outbox
//! facts; `mas_integrations` resolves only secret-reference metadata; Vault is
//! contacted through hostname-pinned guarded egress; raw Vault tokens and HMAC
//! keys remain in SecretString/zeroizing wrappers and never enter logs/DTOs.
//!
//! Data ownership: endpoint/receipt/outbox facts are PostgreSQL-owned,
//! reference metadata is PostgreSQL-owned, and signing bytes are Vault-owned.
//!
//! Prohibited responsibilities: creating fallback in-memory routes/keys,
//! accepting tenant input for pool or Vault location, writing raw secret
//! material to SQL, or handling HTTP callback requests directly.

use std::collections::BTreeSet;
use std::sync::Arc;

use mas_application::inbound_webhook_service::InboundWebhookEndpointService;
use mas_integrations::http_guard::{EgressPolicy, GuardedHttpClient};
use mas_integrations::inbound_webhooks::{InboundWebhookIngress, InboundWebhookIngressPort};
use mas_integrations::reqwest_egress::ReqwestEgress;
use mas_integrations::secrets::{SecretGateWebhookResolver, VaultSecretResolver};
use mas_persistence::repositories::{
    PostgresInboundWebhookEndpointStore, PostgresInboundWebhookReceiptStore,
    PostgresRelaySecretReferenceLookup,
};
use mas_security::{SecretGate, SecretString};
use sqlx::PgPool;

/// Composes all production-only inbound webhook dependencies. The caller has
/// already verified migration ledger state, so the role grants/tables required
/// by these adapters are expected to exist before connection attempts.
pub fn compose(
    merger: &mas_common::config::Merger<'_>,
    request_pool: PgPool,
    inbound_pool: PgPool,
    integrations_pool: PgPool,
    audit: Arc<mas_persistence::repositories::audit::PostgresAuditLog>,
) -> Result<
    (
        Arc<InboundWebhookEndpointService>,
        Arc<dyn InboundWebhookIngressPort>,
    ),
    String,
> {
    let vault_url_raw = merger.string("MAS_VAULT_URL", "secrets.vault.url", "");
    if vault_url_raw.trim().is_empty() {
        return Err(
            "production inbound webhooks require MAS_VAULT_URL / secrets.vault.url".to_owned(),
        );
    }
    let vault_url = mas_domain::value_objects::SafeUrl::parse(&vault_url_raw)
        .map_err(|error| format!("inbound Vault URL: {error}"))?;
    if !vault_url.is_tls() || vault_url.is_internal_endpoint() {
        return Err(
            "inbound Vault URL must be a public HTTPS endpoint; private/internal Vault endpoints are not accepted by this deployment boundary"
                .to_owned(),
        );
    }
    let vault_token = merger.string("MAS_VAULT_TOKEN", "secrets.vault.token", "");
    if vault_token.is_empty() {
        return Err(
            "production inbound webhooks require MAS_VAULT_TOKEN / secrets.vault.token".to_owned(),
        );
    }
    let vault_namespace = merger.string("MAS_VAULT_NAMESPACE", "secrets.vault.namespace", "");
    let value_field = merger.string(
        "MAS_VAULT_VALUE_FIELD",
        "secrets.vault.value_field",
        "value",
    );
    let effective_port = vault_url.as_url().port_or_known_default().ok_or_else(|| {
        "inbound Vault URL has no effective HTTPS port after validation".to_owned()
    })?;
    let policy = EgressPolicy {
        https_only: true,
        permitted_plain_http_hosts: BTreeSet::new(),
        allowed_ports: Some(BTreeSet::from([effective_port])),
        hostname_allowlist: Some(BTreeSet::from([vault_url.host_str().to_owned()])),
        max_redirects: 0,
        max_body_bytes: 64 * 1024,
        allow_internal_endpoints: false,
    };
    let guarded_vault_client = GuardedHttpClient::new(ReqwestEgress::new(policy.clone()), policy);
    let vault_resolver = Arc::new(
        VaultSecretResolver::new(
            guarded_vault_client,
            vault_url,
            SecretString::new(vault_token),
            (!vault_namespace.is_empty()).then_some(vault_namespace),
            value_field,
        )
        .map_err(|error| format!("inbound Vault configuration: {error}"))?,
    );
    let security_audit: Arc<dyn mas_security::AuditSinkPort> = audit.clone();
    // Do not cache signing material in the API callback process. A callback is
    // latency-tolerant, while immediate key revocation/rotation must take
    // effect for the next request rather than after a process-local TTL.
    let gate = Arc::new(SecretGate::new(vault_resolver, security_audit));
    let references = Arc::new(PostgresRelaySecretReferenceLookup::new(integrations_pool));
    let keys = SecretGateWebhookResolver::new(references, gate, "inbound-webhook-ingress")
        .map_err(|error| format!("inbound secret resolver: {error}"))?;
    let endpoint_store =
        PostgresInboundWebhookEndpointStore::new(request_pool, inbound_pool.clone());
    let endpoints = Arc::new(InboundWebhookEndpointService::new(
        Box::new(endpoint_store.clone()),
        Box::new(audit),
    ));
    let receipts = PostgresInboundWebhookReceiptStore::new(inbound_pool);
    let replay_tolerance_seconds = merger
        .u64(
            "MAS_INBOUND_WEBHOOK_REPLAY_TOLERANCE_SECONDS",
            "integrations.inbound_webhooks.replay_tolerance_seconds",
            300,
        )
        .map_err(|error| format!("inbound webhook replay tolerance: {error}"))?;
    let ingress = InboundWebhookIngress::new(endpoint_store, keys, receipts)
        .with_replay_tolerance(std::time::Duration::from_secs(replay_tolerance_seconds))
        .map_err(|error| format!("inbound webhook ingress configuration: {error}"))?;
    Ok((endpoints, Arc::new(ingress)))
}
