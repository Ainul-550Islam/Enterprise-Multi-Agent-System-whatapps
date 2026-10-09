//! PURPOSE: Tenant-scoped enterprise connector and outbound-webhook use cases.
//!
//! Owning layer: `mas-application`.
//!
//! Production role: compose the one canonical provider registry, connector
//! registry, connection probe, endpoint store, durable fanout port, audit sink,
//! and service context into authorization-aware mutations. It makes no direct
//! HTTP call; real probes/delivery arrive through integration ports whose
//! concrete adapters enforce guarded egress.
//!
//! Security boundary: a resolved tenant/organization plus a Developer-or-higher
//! role is required for configuration mutations. Foreign ids are hidden as
//! NotFound. Config is checked against provider manifests and never accepts
//! secret-looking keys; webhook URLs are parsed as tenant-safe `SafeUrl` values
//! and signing authority is a secret reference only.
//!
//! Data ownership: stores own connector, endpoint, payload and delivery rows;
//! the service owns sequencing, authorization and audit semantics only.
//!
//! Prohibited responsibilities: raw credential/OAuth token storage, secret
//! resolution, private-network exceptions, HTTP delivery, fake probe success,
//! in-memory production fallback, and cross-tenant administration.

use std::collections::BTreeSet;

use base64::Engine as _;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, EventId, SecretReferenceId, WebhookId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::membership::MembershipRole;
use mas_domain::value_objects::SafeUrl;
use mas_domain::{AuditActorKind, AuditOutcome, Connector, ConnectorAuthMode, WebhookEndpoint};
use mas_integrations::connectors::{
    clean_error_fragment, validate_category_config, ConnectionProbePort,
};
use mas_integrations::durable_webhooks::OutboundWebhookEnqueuePort;
use mas_integrations::registry::{ConnectorCursor, ConnectorRegistryPort, ProviderRegistryPort};
use mas_integrations::secrets::SecretReferenceLookupPort;

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::stores::{WebhookEndpointCursor, WebhookStorePort};

/// Request to create one canonical connector instance.
#[derive(Debug, Clone)]
pub struct RegisterConnector {
    pub provider: String,
    pub name: String,
    pub display_name: Option<String>,
    pub auth_mode: ConnectorAuthMode,
    pub config: serde_json::Map<String, serde_json::Value>,
}

/// Request to register an outbound webhook endpoint.
#[derive(Debug, Clone)]
pub struct RegisterWebhookEndpoint {
    pub url: String,
    pub subscribed_events: BTreeSet<String>,
    pub secret_reference_id: SecretReferenceId,
    pub signing_key_version: Option<String>,
    pub description: Option<String>,
}

/// Bounded stable connector page returned to API adapters.
#[derive(Debug, Clone)]
pub struct ConnectorPage {
    pub items: Vec<Connector>,
    pub next_cursor: Option<String>,
}

/// Bounded stable outbound-webhook endpoint page returned to API adapters.
#[derive(Debug, Clone)]
pub struct WebhookEndpointPage {
    pub items: Vec<WebhookEndpoint>,
    pub next_cursor: Option<String>,
}

/// The canonical integration-plane service.
#[derive(Debug)]
pub struct IntegrationService {
    providers: Box<dyn ProviderRegistryPort>,
    connectors: Box<dyn ConnectorRegistryPort>,
    probes: Box<dyn ConnectionProbePort>,
    webhooks: Box<dyn WebhookStorePort>,
    /// Request-scoped secret-reference metadata lookup. It proves that a
    /// signing reference belongs to the current tenant before its opaque id
    /// ever becomes durable webhook configuration.
    secret_references: Box<dyn SecretReferenceLookupPort>,
    fanout: Box<dyn OutboundWebhookEnqueuePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl IntegrationService {
    /// Wires the service with concrete ports. Production bootstrap must pass
    /// RLS-scoped PostgreSQL stores, a tenant-bound secret-reference lookup,
    /// and real guarded probe adapters; tests may pass reference ports
    /// explicitly.
    pub fn new(
        providers: Box<dyn ProviderRegistryPort>,
        connectors: Box<dyn ConnectorRegistryPort>,
        probes: Box<dyn ConnectionProbePort>,
        webhooks: Box<dyn WebhookStorePort>,
        secret_references: Box<dyn SecretReferenceLookupPort>,
        fanout: Box<dyn OutboundWebhookEnqueuePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            providers,
            connectors,
            probes,
            webhooks,
            secret_references,
            fanout,
            audit,
        }
    }

    /// Creates a connector only after exact manifest/auth/config validation.
    pub async fn register_connector(
        &self,
        ctx: &ServiceContext,
        request: RegisterConnector,
    ) -> Result<Connector> {
        let (tenant, organization) = self.require_manager(ctx)?;
        let manifest = self
            .providers
            .get(&request.provider)
            .await?
            .ok_or_else(|| {
                AppError::invalid_field("provider", "unknown", "unknown connector provider")
            })?;
        if !manifest.supports_auth(&request.auth_mode) {
            return Err(AppError::invalid_field(
                "auth_mode",
                "unsupported",
                format!(
                    "provider '{}' does not support auth mode {}",
                    manifest.provider, request.auth_mode
                ),
            ));
        }
        manifest.validate_key_contract(&request.config)?;
        validate_category_config(&manifest.category, &request.config)?;
        let mut connector = Connector::register(
            tenant,
            organization,
            manifest.provider,
            request.name,
            request.auth_mode,
        )?;
        connector.display_name = request.display_name;
        connector.config = request.config;
        connector.updated_at = Timestamp::now();
        self.connectors.save(&connector).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "connector.register",
            "connector",
            Some(connector.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(connector)
    }

    /// Performs a real injected connection probe and records a sanitized,
    /// domain-validated result. A success cannot activate an auth-required
    /// connector until a credential lifecycle has bound one.
    pub async fn test_connector(
        &self,
        ctx: &ServiceContext,
        connector_id: ConnectorId,
    ) -> Result<Connector> {
        let (tenant, _) = self.require_manager(ctx)?;
        let mut connector = self
            .connectors
            .get(tenant, connector_id)
            .await?
            .ok_or_else(|| AppError::not_found("connector", connector_id.to_string()))?;
        let result = self.probes.probe(&connector).await;
        match result {
            Ok(()) => connector.record_test_result(true, None)?,
            Err(error) => connector
                .record_test_result(false, Some(clean_error_fragment(&error.to_string())))?,
        }
        self.connectors.save(&connector).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "connector.test",
            "connector",
            Some(connector.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(connector)
    }

    /// Lists only the current tenant's connector instances.
    pub async fn list_connectors(&self, ctx: &ServiceContext) -> Result<Vec<Connector>> {
        let tenant = ctx.require_tenant()?;
        self.connectors.list(tenant).await
    }

    /// Returns a bounded stable connector page. The opaque cursor embeds only
    /// prior ordering values; the tenant remains exclusively in `ctx`/RLS.
    pub async fn list_connectors_page(
        &self,
        ctx: &ServiceContext,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<ConnectorPage> {
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "connector page limit must be between 1 and 200",
            ));
        }
        let tenant = ctx.require_tenant()?;
        let after = cursor.map(decode_connector_cursor).transpose()?;
        let mut items = self
            .connectors
            .list_page(tenant, after, limit.saturating_add(1))
            .await?;
        let has_more = items.len() > usize::from(limit);
        if has_more {
            items.truncate(usize::from(limit));
        }
        let next_cursor = if has_more {
            items.last().map(|connector| {
                encode_connector_cursor(ConnectorCursor {
                    created_at: connector.created_at,
                    id: connector.id,
                })
            })
        } else {
            None
        };
        Ok(ConnectorPage { items, next_cursor })
    }

    /// Gets a connector with foreign existence hidden.
    pub async fn get_connector(
        &self,
        ctx: &ServiceContext,
        connector_id: ConnectorId,
    ) -> Result<Connector> {
        let tenant = ctx.require_tenant()?;
        self.connectors
            .get(tenant, connector_id)
            .await?
            .ok_or_else(|| AppError::not_found("connector", connector_id.to_string()))
    }

    /// Registers a tenant-safe, secret-reference-only outbound endpoint.
    pub async fn register_webhook(
        &self,
        ctx: &ServiceContext,
        request: RegisterWebhookEndpoint,
    ) -> Result<WebhookEndpoint> {
        let (tenant, _) = self.require_manager(ctx)?;
        let secret_reference = self
            .secret_references
            .get(request.secret_reference_id)
            .await?
            .filter(|record| {
                record.tenant_id == tenant && record.reference.id == request.secret_reference_id
            })
            .ok_or_else(|| {
                AppError::not_found("secret_reference", request.secret_reference_id.to_string())
            })?;
        if !secret_reference.reference.is_readable() {
            return Err(AppError::conflict(
                "webhook signing secret is not readable; complete or repair its rotation before registering an endpoint",
            ));
        }
        // The production relay is intentionally composed only with the Vault
        // resolver. Accepting a different reference provider would create an
        // endpoint that can never sign successfully, so reject it at the
        // request boundary instead of deferring failure to a worker.
        if secret_reference.reference.provider != mas_domain::SecretProviderKind::Vault {
            return Err(AppError::invalid_field(
                "secret_reference_id",
                "unsupported_provider",
                "outbound webhook signing currently requires a Vault secret reference",
            ));
        }
        if request.signing_key_version.is_some()
            && request.signing_key_version != secret_reference.reference.key_version
        {
            return Err(AppError::invalid_field(
                "signing_key_version",
                "reference_mismatch",
                "signing key version must match the canonical secret-reference version",
            ));
        }
        let url = SafeUrl::parse(&request.url)?;
        let mut endpoint = WebhookEndpoint::register(
            tenant,
            url,
            request.subscribed_events,
            mas_domain::webhook::SignatureConfig {
                algorithm: mas_domain::webhook::SignatureAlgorithm::HmacSha256,
                secret_reference_id: request.secret_reference_id,
                // Capture the canonical reference version when the endpoint is
                // registered. The durable fanout copies this value into every
                // delivery, preventing a retry from silently switching keys.
                key_version: secret_reference.reference.key_version,
            },
        )?;
        endpoint.description = request.description;
        self.webhooks.save(&endpoint).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "webhook.register",
            "webhook",
            Some(endpoint.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(endpoint)
    }

    /// Enables or disables an endpoint after a tenant-scoped fetch.
    pub async fn set_webhook_enabled(
        &self,
        ctx: &ServiceContext,
        webhook_id: WebhookId,
        enabled: bool,
    ) -> Result<WebhookEndpoint> {
        self.require_manager(ctx)?;
        let tenant = ctx.require_tenant()?;
        let mut endpoint = self
            .webhooks
            .get(webhook_id)
            .await?
            .filter(|endpoint| endpoint.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("webhook", webhook_id.to_string()))?;
        if enabled {
            endpoint.enable();
        } else {
            endpoint.disable();
        }
        self.webhooks.save(&endpoint).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            if enabled {
                "webhook.enable"
            } else {
                "webhook.disable"
            },
            "webhook",
            Some(endpoint.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(endpoint)
    }

    /// Lists endpoints in the caller's tenant.
    pub async fn list_webhooks(&self, ctx: &ServiceContext) -> Result<Vec<WebhookEndpoint>> {
        let tenant = ctx.require_tenant()?;
        self.webhooks.list(tenant).await
    }

    /// Returns a bounded stable outbound endpoint page. The signing-reference
    /// id remains server-only and does not participate in cursor state.
    pub async fn list_webhooks_page(
        &self,
        ctx: &ServiceContext,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<WebhookEndpointPage> {
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "webhook page limit must be between 1 and 200",
            ));
        }
        let tenant = ctx.require_tenant()?;
        let after = cursor.map(decode_webhook_cursor).transpose()?;
        let mut items = self
            .webhooks
            .list_page(tenant, after, limit.saturating_add(1))
            .await?;
        let has_more = items.len() > usize::from(limit);
        if has_more {
            items.truncate(usize::from(limit));
        }
        let next_cursor = if has_more {
            items.last().map(|endpoint| {
                encode_webhook_cursor(WebhookEndpointCursor {
                    created_at: endpoint.created_at,
                    id: endpoint.id,
                })
            })
        } else {
            None
        };
        Ok(WebhookEndpointPage { items, next_cursor })
    }

    /// Atomically persists an exact outbound event payload plus one delivery
    /// per matching endpoint. This is the only production fanout entry point.
    pub async fn enqueue_webhook_event(
        &self,
        ctx: &ServiceContext,
        event_id: EventId,
        event_type: &str,
        body: &[u8],
    ) -> Result<u64> {
        let tenant = ctx.require_tenant()?;
        let created = self
            .fanout
            .enqueue_event(tenant, event_id, event_type, body)
            .await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "webhook.enqueue",
            "webhook_event",
            Some(event_id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(created)
    }

    fn require_manager(
        &self,
        ctx: &ServiceContext,
    ) -> Result<(mas_common::ids::TenantId, mas_common::ids::OrganizationId)> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        // System/service paths are authenticated and authorized by their
        // caller-specific boundary; human/API-key request traffic must carry a
        // resolved tenant membership at least Developer.
        if !matches!(
            ctx.actor.kind,
            AuditActorKind::System | AuditActorKind::Service
        ) {
            let role = ctx.require_resolved_role()?;
            if role.rank() < MembershipRole::Developer.rank() {
                return Err(AppError::forbidden(
                    "developer, admin, or owner role is required to manage integrations",
                ));
            }
        }
        Ok((tenant, organization))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ConnectorCursorWire {
    created_at_ms: i64,
    id: ConnectorId,
}

fn encode_connector_cursor(cursor: ConnectorCursor) -> String {
    let wire = ConnectorCursorWire {
        created_at_ms: cursor.created_at.to_unix_ms(),
        id: cursor.id,
    };
    let serialized =
        serde_json::to_vec(&wire).expect("connector cursor serialization is infallible");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serialized)
}

fn decode_connector_cursor(encoded: &str) -> Result<ConnectorCursor> {
    let wire: ConnectorCursorWire = decode_cursor_wire(encoded, "connector")?;
    Ok(ConnectorCursor {
        created_at: cursor_timestamp(wire.created_at_ms, "connector")?,
        id: wire.id,
    })
}

#[derive(serde::Serialize, serde::Deserialize)]
struct WebhookCursorWire {
    created_at_ms: i64,
    id: WebhookId,
}

fn encode_webhook_cursor(cursor: WebhookEndpointCursor) -> String {
    let wire = WebhookCursorWire {
        created_at_ms: cursor.created_at.to_unix_ms(),
        id: cursor.id,
    };
    let serialized = serde_json::to_vec(&wire).expect("webhook cursor serialization is infallible");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serialized)
}

fn decode_webhook_cursor(encoded: &str) -> Result<WebhookEndpointCursor> {
    let wire: WebhookCursorWire = decode_cursor_wire(encoded, "webhook")?;
    Ok(WebhookEndpointCursor {
        created_at: cursor_timestamp(wire.created_at_ms, "webhook")?,
        id: wire.id,
    })
}

fn decode_cursor_wire<T>(encoded: &str, resource: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if encoded.is_empty()
        || encoded.len() > 512
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(AppError::invalid_field(
            "cursor",
            "invalid_format",
            format!("{resource} cursor is invalid"),
        ));
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| {
            AppError::invalid_field(
                "cursor",
                "invalid_format",
                format!("{resource} cursor is invalid"),
            )
        })?;
    serde_json::from_slice(&decoded).map_err(|_| {
        AppError::invalid_field(
            "cursor",
            "invalid_format",
            format!("{resource} cursor is invalid"),
        )
    })
}

fn cursor_timestamp(milliseconds: i64, resource: &str) -> Result<Timestamp> {
    Timestamp::from_unix_ms(milliseconds).map_err(|_| {
        AppError::invalid_field(
            "cursor",
            "invalid_format",
            format!("{resource} cursor is invalid"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use mas_common::ids::{OrganizationId, TenantId};
    use mas_domain::{
        SecretAvailability, SecretProviderKind, SecretReference, SecretRotationState,
    };
    use mas_integrations::registry::{InMemoryConnectorRegistry, InMemoryProviderRegistry};
    use mas_integrations::secrets::SecretReferenceRecord;
    use std::sync::Arc;

    #[derive(Debug)]
    struct RejectingProbe;

    #[async_trait::async_trait]
    impl ConnectionProbePort for RejectingProbe {
        async fn probe(&self, _connector: &Connector) -> Result<()> {
            Err(AppError::external_service("provider", "unreachable"))
        }
    }

    #[derive(Debug, Default)]
    struct MemoryWebhookStore;

    #[async_trait::async_trait]
    impl WebhookStorePort for MemoryWebhookStore {
        async fn save(&self, _endpoint: &WebhookEndpoint) -> Result<()> {
            Ok(())
        }
        async fn get(&self, _id: WebhookId) -> Result<Option<WebhookEndpoint>> {
            Ok(None)
        }
        async fn list(&self, _tenant: mas_common::ids::TenantId) -> Result<Vec<WebhookEndpoint>> {
            Ok(Vec::new())
        }
        async fn list_page(
            &self,
            _tenant: mas_common::ids::TenantId,
            _after: Option<crate::stores::WebhookEndpointCursor>,
            _limit: u16,
        ) -> Result<Vec<WebhookEndpoint>> {
            Ok(Vec::new())
        }
    }

    #[derive(Debug, Default)]
    struct MissingSecretReferenceLookup;

    #[async_trait::async_trait]
    impl SecretReferenceLookupPort for MissingSecretReferenceLookup {
        async fn get(&self, _id: SecretReferenceId) -> Result<Option<SecretReferenceRecord>> {
            Ok(None)
        }
    }

    #[derive(Debug)]
    struct FixedSecretReferenceLookup {
        record: Option<SecretReferenceRecord>,
    }

    #[async_trait::async_trait]
    impl SecretReferenceLookupPort for FixedSecretReferenceLookup {
        async fn get(&self, _id: SecretReferenceId) -> Result<Option<SecretReferenceRecord>> {
            Ok(self.record.clone())
        }
    }

    #[derive(Debug, Default)]
    struct MemoryFanout;

    #[async_trait::async_trait]
    impl OutboundWebhookEnqueuePort for MemoryFanout {
        async fn enqueue_event(
            &self,
            _tenant_id: mas_common::ids::TenantId,
            _event_id: EventId,
            _event_type: &str,
            _body: &[u8],
        ) -> Result<u64> {
            Ok(0)
        }
    }

    fn service() -> IntegrationService {
        service_with_references(Box::new(MissingSecretReferenceLookup))
    }

    fn service_with_references(
        references: Box<dyn SecretReferenceLookupPort>,
    ) -> IntegrationService {
        IntegrationService::new(
            Box::new(InMemoryProviderRegistry::with_builtin()),
            Box::new(InMemoryConnectorRegistry::new()),
            Box::new(RejectingProbe),
            Box::new(MemoryWebhookStore),
            references,
            Box::new(MemoryFanout),
            Box::new(Arc::new(InMemoryAuditSink::new())),
        )
    }

    fn vault_reference(tenant_id: TenantId, version: Option<&str>) -> SecretReferenceRecord {
        SecretReferenceRecord {
            tenant_id,
            reference: SecretReference {
                id: SecretReferenceId::new(),
                provider: SecretProviderKind::Vault,
                path: "secret/data/mas/webhook-signing".to_owned(),
                key_version: version.map(str::to_owned),
                rotation_state: SecretRotationState::Current,
                availability: SecretAvailability::Available,
            },
        }
    }

    #[tokio::test]
    async fn unknown_provider_never_becomes_a_fake_connector() {
        let service = service();
        let context = ServiceContext::system("integration-test", &Timestamp::now())
            .expect("context")
            .with_scope(TenantId::new(), OrganizationId::new());
        let result = service
            .register_connector(
                &context,
                RegisterConnector {
                    provider: "unknown-provider".to_owned(),
                    name: "primary".to_owned(),
                    display_name: None,
                    auth_mode: ConnectorAuthMode::None,
                    config: Default::default(),
                },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn webhook_registration_proves_reference_tenant_and_captures_canonical_version() {
        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let owned_reference = vault_reference(tenant, Some("version-7"));
        let reference_id = owned_reference.reference.id;
        let service = service_with_references(Box::new(FixedSecretReferenceLookup {
            record: Some(owned_reference),
        }));
        let context = ServiceContext::system("integration-test", &Timestamp::now())
            .expect("context")
            .with_scope(tenant, organization);
        let endpoint = service
            .register_webhook(
                &context,
                RegisterWebhookEndpoint {
                    url: "https://hooks.example.test/events".to_owned(),
                    subscribed_events: BTreeSet::from(["execution.completed".to_owned()]),
                    secret_reference_id: reference_id,
                    signing_key_version: None,
                    description: None,
                },
            )
            .await
            .expect("owned readable Vault reference is accepted");
        assert_eq!(endpoint.signature.secret_reference_id, reference_id);
        assert_eq!(endpoint.signature.key_version.as_deref(), Some("version-7"));

        let foreign_reference = vault_reference(TenantId::new(), Some("version-8"));
        let foreign_service = service_with_references(Box::new(FixedSecretReferenceLookup {
            record: Some(foreign_reference.clone()),
        }));
        let result = foreign_service
            .register_webhook(
                &context,
                RegisterWebhookEndpoint {
                    url: "https://hooks.example.test/events".to_owned(),
                    subscribed_events: BTreeSet::from(["execution.completed".to_owned()]),
                    secret_reference_id: foreign_reference.reference.id,
                    signing_key_version: None,
                    description: None,
                },
            )
            .await;
        assert!(
            result.is_err(),
            "foreign signing references must stay hidden"
        );
    }

    #[test]
    fn connector_status_type_remains_explicit() {
        assert_eq!(
            mas_common::enums::ConnectorStatus::PendingVerification.to_string(),
            "pending_verification"
        );
    }
}
