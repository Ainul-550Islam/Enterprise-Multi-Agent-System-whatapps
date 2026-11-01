//! PURPOSE: Authorized tenant-facing management use cases for public inbound
//! webhook endpoints.
//!
//! Owning layer: `mas-application` integration configuration boundary.
//!
//! Production role: let an authorized tenant manager register, list, inspect,
//! enable, and disable canonical inbound endpoint metadata while preserving a
//! secret-reference-only boundary and a mandatory audit trail.
//!
//! Security boundary: tenant and organization come only from ServiceContext;
//! human/API-key callers require a resolved Developer-or-higher membership;
//! foreign endpoints are hidden as NotFound; raw signing keys never enter this
//! service or its DTOs.
//!
//! Data ownership: the endpoint repository owns durable configuration and
//! keyset ordering; SecretReference metadata owns key pointers; this service
//! owns authorization, reload-after-save, cursor handling, and audit actions.
//!
//! Prohibited responsibilities: signature verification, public route lookup,
//! secret-manager access, raw key/token persistence, outbound HTTP, direct
//! outbox writes, or a production in-memory fallback.

use std::collections::BTreeSet;

use base64::Engine as _;
use mas_common::error::AppError;
use mas_common::ids::{InboundWebhookId, SecretReferenceId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{AuditActorKind, AuditOutcome, MembershipRole};
use mas_integrations::inbound_webhooks::{
    InboundWebhookEndpoint, InboundWebhookEndpointCursor, InboundWebhookEndpointStorePort,
};
use serde::{Deserialize, Serialize};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;

/// Non-secret input for registering an inbound endpoint.
#[derive(Debug, Clone)]
pub struct RegisterInboundWebhookEndpoint {
    pub source: String,
    pub signing_secret_reference_id: SecretReferenceId,
    pub required_key_version: Option<String>,
    pub accepted_key_versions: BTreeSet<String>,
}

/// Stable keyset page for endpoint configuration APIs.
#[derive(Debug, Clone)]
pub struct InboundWebhookEndpointPage {
    pub items: Vec<InboundWebhookEndpoint>,
    pub next_cursor: Option<String>,
}

/// Canonical inbound endpoint management service.
#[derive(Debug)]
pub struct InboundWebhookEndpointService {
    endpoints: Box<dyn InboundWebhookEndpointStorePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl InboundWebhookEndpointService {
    #[must_use]
    pub fn new(
        endpoints: Box<dyn InboundWebhookEndpointStorePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self { endpoints, audit }
    }

    /// Registers an enabled endpoint after scope/role validation. The returned
    /// record is reloaded so database timestamps are authoritative.
    pub async fn register(
        &self,
        ctx: &ServiceContext,
        input: RegisterInboundWebhookEndpoint,
    ) -> Result<InboundWebhookEndpoint> {
        let (tenant_id, organization_id) = self.require_manager(ctx)?;
        let endpoint = InboundWebhookEndpoint::register(
            tenant_id,
            organization_id,
            input.source,
            input.signing_secret_reference_id,
            input.required_key_version,
            input.accepted_key_versions,
        )?;
        self.endpoints.save(&endpoint).await?;
        let canonical = self
            .endpoints
            .get(tenant_id, endpoint.id)
            .await?
            .ok_or_else(|| AppError::internal("inbound endpoint disappeared after save"))?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "inbound_webhook.register",
            "inbound_webhook_endpoint",
            Some(canonical.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(canonical)
    }

    /// Reads a single tenant endpoint without disclosing foreign existence.
    pub async fn get(
        &self,
        ctx: &ServiceContext,
        endpoint_id: InboundWebhookId,
    ) -> Result<InboundWebhookEndpoint> {
        let tenant_id = ctx.require_tenant()?;
        self.endpoints
            .get(tenant_id, endpoint_id)
            .await?
            .ok_or_else(|| AppError::not_found("inbound_webhook_endpoint", endpoint_id.to_string()))
    }

    /// Lists tenant endpoint metadata through a bounded stable keyset cursor.
    pub async fn list(
        &self,
        ctx: &ServiceContext,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<InboundWebhookEndpointPage> {
        let tenant_id = ctx.require_tenant()?;
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "inbound webhook endpoint page limit must be between 1 and 200",
            ));
        }
        let after = cursor.map(decode_cursor).transpose()?;
        let mut items = self
            .endpoints
            .list_page(tenant_id, after, limit.saturating_add(1))
            .await?;
        let has_more = items.len() > usize::from(limit);
        if has_more {
            items.truncate(usize::from(limit));
        }
        let next_cursor = if has_more {
            items.last().map(|endpoint| {
                encode_cursor(InboundWebhookEndpointCursor {
                    created_at: endpoint.created_at,
                    id: endpoint.id,
                })
            })
        } else {
            None
        };
        Ok(InboundWebhookEndpointPage { items, next_cursor })
    }

    /// Enables or disables one endpoint and returns the database-authoritative
    /// row. Disabling does not erase accepted receipt/outbox evidence.
    pub async fn set_enabled(
        &self,
        ctx: &ServiceContext,
        endpoint_id: InboundWebhookId,
        enabled: bool,
    ) -> Result<InboundWebhookEndpoint> {
        let (tenant_id, _) = self.require_manager(ctx)?;
        let mut endpoint = self
            .endpoints
            .get(tenant_id, endpoint_id)
            .await?
            .ok_or_else(|| {
                AppError::not_found("inbound_webhook_endpoint", endpoint_id.to_string())
            })?;
        if enabled {
            endpoint.enable();
        } else {
            endpoint.disable();
        }
        self.endpoints.save(&endpoint).await?;
        let canonical = self
            .endpoints
            .get(tenant_id, endpoint_id)
            .await?
            .ok_or_else(|| AppError::internal("inbound endpoint disappeared after save"))?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            if enabled {
                "inbound_webhook.enable"
            } else {
                "inbound_webhook.disable"
            },
            "inbound_webhook_endpoint",
            Some(endpoint_id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(canonical)
    }

    fn require_manager(
        &self,
        ctx: &ServiceContext,
    ) -> Result<(mas_common::ids::TenantId, mas_common::ids::OrganizationId)> {
        let tenant_id = ctx.require_tenant()?;
        let organization_id = ctx.require_organization()?;
        if !matches!(
            ctx.actor.kind,
            AuditActorKind::System | AuditActorKind::Service
        ) {
            let role = ctx.require_resolved_role()?;
            if role.rank() < MembershipRole::Developer.rank() {
                return Err(AppError::forbidden(
                    "developer, admin, or owner role is required to manage inbound webhooks",
                ));
            }
        }
        Ok((tenant_id, organization_id))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct CursorWire {
    created_at_ms: i64,
    id: InboundWebhookId,
}

fn encode_cursor(cursor: InboundWebhookEndpointCursor) -> String {
    let wire = CursorWire {
        created_at_ms: cursor.created_at.to_unix_ms(),
        id: cursor.id,
    };
    let serialized = serde_json::to_vec(&wire).expect("cursor serialization is infallible");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serialized)
}

fn decode_cursor(encoded: &str) -> Result<InboundWebhookEndpointCursor> {
    if encoded.is_empty() || encoded.len() > 512 {
        return Err(AppError::invalid_field(
            "cursor",
            "invalid_format",
            "inbound webhook endpoint cursor is invalid",
        ));
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| {
            AppError::invalid_field(
                "cursor",
                "invalid_format",
                "inbound webhook endpoint cursor is invalid",
            )
        })?;
    let wire: CursorWire = serde_json::from_slice(&decoded).map_err(|_| {
        AppError::invalid_field(
            "cursor",
            "invalid_format",
            "inbound webhook endpoint cursor is invalid",
        )
    })?;
    Ok(InboundWebhookEndpointCursor {
        created_at: Timestamp::from_unix_ms(wire.created_at_ms).map_err(|_| {
            AppError::invalid_field(
                "cursor",
                "invalid_format",
                "inbound webhook endpoint cursor is invalid",
            )
        })?,
        id: wire.id,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::audit::InMemoryAuditSink;
    use mas_domain::AuditActor;

    #[derive(Debug, Default)]
    struct Store(Mutex<BTreeMap<InboundWebhookId, InboundWebhookEndpoint>>);

    #[async_trait]
    impl InboundWebhookEndpointStorePort for Store {
        async fn save(&self, endpoint: &InboundWebhookEndpoint) -> Result<()> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(endpoint.id, endpoint.clone());
            Ok(())
        }

        async fn get(
            &self,
            tenant_id: mas_common::ids::TenantId,
            endpoint_id: InboundWebhookId,
        ) -> Result<Option<InboundWebhookEndpoint>> {
            Ok(self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&endpoint_id)
                .filter(|endpoint| endpoint.tenant_id == tenant_id)
                .cloned())
        }

        async fn list_page(
            &self,
            tenant_id: mas_common::ids::TenantId,
            after: Option<InboundWebhookEndpointCursor>,
            limit: u16,
        ) -> Result<Vec<InboundWebhookEndpoint>> {
            let mut endpoints: Vec<_> = self
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .values()
                .filter(|endpoint| endpoint.tenant_id == tenant_id)
                .filter(|endpoint| {
                    after.is_none_or(|cursor| {
                        endpoint.created_at < cursor.created_at
                            || (endpoint.created_at == cursor.created_at && endpoint.id < cursor.id)
                    })
                })
                .cloned()
                .collect();
            endpoints.sort_by(|left, right| {
                right
                    .created_at
                    .cmp(&left.created_at)
                    .then_with(|| right.id.cmp(&left.id))
            });
            endpoints.truncate(usize::from(limit));
            Ok(endpoints)
        }
    }

    fn manager() -> ServiceContext {
        let tenant_id = mas_common::ids::TenantId::new();
        let organization_id = mas_common::ids::OrganizationId::new();
        ServiceContext::new(
            AuditActor::new(AuditActorKind::User, "manager").expect("actor"),
            Some(tenant_id),
            Some(organization_id),
            "inbound-webhook-test",
            &Timestamp::now(),
        )
        .expect("context")
        .with_resolved_role(Some(MembershipRole::Developer))
    }

    #[tokio::test]
    async fn registration_audits_and_cursors_round_trip() {
        let service = InboundWebhookEndpointService::new(
            Box::new(Store::default()),
            Box::new(InMemoryAuditSink::new()),
        );
        let context = manager();
        let endpoint = service
            .register(
                &context,
                RegisterInboundWebhookEndpoint {
                    source: "provider.events".to_owned(),
                    signing_secret_reference_id: SecretReferenceId::new(),
                    required_key_version: Some("v1".to_owned()),
                    accepted_key_versions: BTreeSet::from(["v1".to_owned()]),
                },
            )
            .await
            .expect("register");
        let page = service.list(&context, None, 20).await.expect("list");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].id, endpoint.id);
        let cursor = encode_cursor(InboundWebhookEndpointCursor {
            created_at: endpoint.created_at,
            id: endpoint.id,
        });
        assert_eq!(decode_cursor(&cursor).expect("decode").id, endpoint.id);
    }

    #[tokio::test]
    async fn viewer_cannot_manage_inbound_endpoints() {
        let service = InboundWebhookEndpointService::new(
            Box::new(Store::default()),
            Box::new(InMemoryAuditSink::new()),
        );
        let context = manager().with_resolved_role(Some(MembershipRole::Viewer));
        assert!(service
            .register(
                &context,
                RegisterInboundWebhookEndpoint {
                    source: "provider.events".to_owned(),
                    signing_secret_reference_id: SecretReferenceId::new(),
                    required_key_version: None,
                    accepted_key_versions: BTreeSet::new(),
                },
            )
            .await
            .is_err());
    }
}
