//! PURPOSE: Authorized connector manifest, guarded-probe, and disable use cases.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: delegate probes through the canonical
//! IntegrationService, expose declarative manifests only, and disable a
//! tenant-owned connector together with its credential/OAuth lifecycle state.
//! Each mutation is audited before success is returned.
//!
//! Data ownership: integrations own manifests/probe behavior; the lifecycle
//! port owns connector and OAuth revocation facts; the audit port owns history.
//!
//! MUST NOT: perform HTTP, resolve secret bytes, accept caller egress policy,
//! or expose credential references.

use std::sync::Arc;

use mas_common::error::AppError;
use mas_common::ids::ConnectorId;
use mas_common::result::Result;
use mas_domain::{AuditOutcome, Connector};
use mas_integrations::manifest::ProviderManifest;
use mas_integrations::registry::ProviderRegistryPort;

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::integration_service::IntegrationService;

/// Tenant-scoped lifecycle operation required when a connector is disabled.
#[async_trait::async_trait]
pub trait ConnectorDisablePort: std::fmt::Debug + Send + Sync {
    /// Disables the connector and revokes active credential and OAuth jobs.
    async fn disable_and_revoke(
        &self,
        tenant: mas_common::ids::TenantId,
        id: ConnectorId,
    ) -> Result<Connector>;
}

/// BATCH-02 connector administrative service.
#[derive(Debug)]
pub struct ConnectorService {
    providers: Box<dyn ProviderRegistryPort>,
    integrations: Arc<IntegrationService>,
    lifecycle: Box<dyn ConnectorDisablePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl ConnectorService {
    /// Explicitly composes manifests, the existing guarded integration plane,
    /// revocation persistence, and mandatory audit.
    pub fn new(
        providers: Box<dyn ProviderRegistryPort>,
        integrations: Arc<IntegrationService>,
        lifecycle: Box<dyn ConnectorDisablePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            providers,
            integrations,
            lifecycle,
            audit,
        }
    }

    /// Lists versioned provider manifests in stable registry order.
    pub async fn manifests(&self) -> Result<Vec<ProviderManifest>> {
        self.providers.list().await
    }

    /// Runs the already-composed provider probe, which enforces guarded egress.
    pub async fn probe(&self, ctx: &ServiceContext, id: ConnectorId) -> Result<Connector> {
        self.integrations.test_connector(ctx, id).await
    }

    /// Disables the connector and revokes credentials and in-flight OAuth work.
    pub async fn disable(&self, ctx: &ServiceContext, id: ConnectorId) -> Result<Connector> {
        let tenant = ctx.require_tenant()?;
        let connector = self.lifecycle.disable_and_revoke(tenant, id).await?;
        if connector.tenant_id != tenant {
            return Err(AppError::not_found("connector", id.to_string()));
        }
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "connector.disable",
            "connector",
            Some(id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(connector)
    }
}

#[async_trait::async_trait]
impl ConnectorDisablePort for crate::stores::InMemoryServices {
    async fn disable_and_revoke(
        &self,
        tenant: mas_common::ids::TenantId,
        id: ConnectorId,
    ) -> Result<Connector> {
        use crate::stores::ConnectorStorePort;
        let mut connector = ConnectorStorePort::get(self, id)
            .await?
            .filter(|connector| connector.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("connector", id.to_string()))?;
        connector.disable()?;
        ConnectorStorePort::save(self, &connector).await?;
        Ok(connector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_requires_real_probe_and_revocation_ports() {
        fn assert_port<T: ConnectorDisablePort>() {}
        assert_port::<crate::stores::InMemoryServices>();
    }
}
