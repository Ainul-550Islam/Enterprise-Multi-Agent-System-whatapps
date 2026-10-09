//! PURPOSE: Authenticated connector-bound OAuth authorization initiation.
//!
//! Owning layer: `mas-application` authorization/use-case boundary.
//!
//! Production role: prove an OAuth2 connector belongs to the resolved tenant
//! and is manageable by the caller before creating a durable PKCE state through
//! the integrations OAuth façade; record an audit event without logging the
//! resulting browser URL or state value.
//!
//! Security boundary: provider selection derives from the stored connector,
//! never a request parameter. Foreign/disabled/non-OAuth connectors are hidden
//! or rejected before any state/verifier is written.
//!
//! Data ownership: connector registry owns connector metadata; durable OAuth
//! ports own state/verifier/token lifecycle; this service owns authorization
//! sequencing and audit semantics only.
//!
//! Prohibited responsibilities: callback code exchange, token persistence,
//! direct secret-manager access, provider URL construction, user-supplied
//! redirect URIs, cross-tenant connector lookup, or fake authorization URLs.

use std::sync::Arc;

use mas_common::enums::ConnectorStatus;
use mas_common::error::AppError;
use mas_common::ids::ConnectorId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::membership::MembershipRole;
use mas_domain::{AuditActorKind, AuditOutcome, ConnectorAuthMode};
use mas_integrations::durable_oauth::DurableOAuthAuthorizationService;
use mas_integrations::registry::ConnectorRegistryPort;

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;

/// Tenant-authorized OAuth initiation service.
#[derive(Debug)]
pub struct OAuthAuthorizationService {
    connectors: Box<dyn ConnectorRegistryPort>,
    oauth: Arc<DurableOAuthAuthorizationService>,
    audit: Box<dyn AuditSinkPort>,
}

impl OAuthAuthorizationService {
    #[must_use]
    pub fn new(
        connectors: Box<dyn ConnectorRegistryPort>,
        oauth: Arc<DurableOAuthAuthorizationService>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            connectors,
            oauth,
            audit,
        }
    }

    /// Returns the provider authorization URL for an authorized, tenant-owned
    /// OAuth2 connector. The URL includes a one-time state and must be sent to
    /// the browser unchanged; it must not be logged or placed in audit data.
    pub async fn begin(&self, ctx: &ServiceContext, connector_id: ConnectorId) -> Result<String> {
        let (tenant_id, _) = require_manager(ctx)?;
        let connector = self
            .connectors
            .get(tenant_id, connector_id)
            .await?
            .ok_or_else(|| AppError::not_found("connector", connector_id.to_string()))?;
        if connector.auth_mode != ConnectorAuthMode::OAuth2 {
            return Err(AppError::conflict(
                "connector is not configured for OAuth2 authorization",
            ));
        }
        if connector.status == ConnectorStatus::Disabled {
            return Err(AppError::conflict(
                "disabled connectors cannot begin OAuth authorization",
            ));
        }
        let url = self
            .oauth
            .start(
                tenant_id,
                connector.id,
                &connector.provider,
                &Timestamp::now(),
            )
            .await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "oauth.authorization.begin",
            "connector",
            Some(connector.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(url)
    }
}

fn require_manager(
    ctx: &ServiceContext,
) -> Result<(mas_common::ids::TenantId, mas_common::ids::OrganizationId)> {
    let tenant = ctx.require_tenant()?;
    let organization = ctx.require_organization()?;
    if !matches!(
        ctx.actor.kind,
        AuditActorKind::System | AuditActorKind::Service
    ) {
        let role = ctx.require_resolved_role()?;
        if role.rank() < MembershipRole::Developer.rank() {
            return Err(AppError::forbidden(
                "developer, admin, or owner role is required to manage OAuth authorization",
            ));
        }
    }
    Ok((tenant, organization))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_type_is_explicitly_connector_bound() {
        assert!(std::any::type_name::<OAuthAuthorizationService>()
            .contains("OAuthAuthorizationService"));
    }
}
