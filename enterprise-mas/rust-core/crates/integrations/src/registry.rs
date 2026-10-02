//! Registries: provider manifests and tenant-installed connectors.
//!
//! Both layers ship with in-memory reference implementations so the rest of
//! rust-core (and tests) never needs a backing store. The persistence crate
//! can provide the SQLx implementations behind the same ports.
//!
//! Tenant isolation rule: lookups are always tenant-scoped; a connector id
//! belonging to another tenant is indistinguishable from non-existence.

use async_trait::async_trait;
use mas_common::ids::{ConnectorId, TenantId};
use mas_common::result::Result;
use mas_domain::Connector;
use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::manifest::{builtin_manifests, ConnectorCategory, ProviderManifest};

/// Read/write port over the provider manifest store.
#[async_trait]
pub trait ProviderRegistryPort: std::fmt::Debug + Send + Sync {
    /// Looks up a manifest by exact provider key.
    async fn get(&self, provider: &str) -> Result<Option<ProviderManifest>>;
    /// Lists all registered manifests, ordered by provider key.
    async fn list(&self) -> Result<Vec<ProviderManifest>>;
    /// Registers or replaces a manifest (keyed by `manifest.provider`).
    async fn register(&self, manifest: ProviderManifest) -> Result<()>;
}

/// Read/write port over installed tenant connectors.
#[async_trait]
pub trait ConnectorRegistryPort: std::fmt::Debug + Send + Sync {
    /// Tenant-scoped lookup; foreign ids return `Ok(None)`.
    async fn get(&self, tenant: TenantId, id: ConnectorId) -> Result<Option<Connector>>;
    /// Lists all connectors installed by `tenant`, ordered by id.
    async fn list(&self, tenant: TenantId) -> Result<Vec<Connector>>;
    /// Inserts or replaces a connector (keyed by (tenant, id)).
    async fn save(&self, connector: &Connector) -> Result<()>;
    /// Removes a tenant-scoped connector. Returns whether it existed.
    async fn delete(&self, tenant: &TenantId, id: ConnectorId) -> Result<bool>;
}

/// In-memory provider manifest store.
#[derive(Debug, Default)]
pub struct InMemoryProviderRegistry {
    manifests: Mutex<BTreeMap<String, ProviderManifest>>,
}

impl InMemoryProviderRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registry pre-seeded with [`builtin_manifests`].
    #[must_use]
    pub fn with_builtin() -> Self {
        let registry = Self::new();
        let mut guard = registry.manifests.lock().unwrap_or_else(|e| e.into_inner());
        for manifest in builtin_manifests() {
            guard.insert(manifest.provider.clone(), manifest);
        }
        drop(guard);
        registry
    }

    /// Number of registered manifests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.manifests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convenience: manifests of a given category.
    #[must_use]
    pub fn by_category(&self, category: &ConnectorCategory) -> Vec<ProviderManifest> {
        self.manifests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|m| &m.category == category)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl ProviderRegistryPort for InMemoryProviderRegistry {
    async fn get(&self, provider: &str) -> Result<Option<ProviderManifest>> {
        Ok(self
            .manifests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(provider)
            .cloned())
    }

    async fn list(&self) -> Result<Vec<ProviderManifest>> {
        Ok(self
            .manifests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect())
    }

    async fn register(&self, manifest: ProviderManifest) -> Result<()> {
        self.manifests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(manifest.provider.clone(), manifest);
        Ok(())
    }
}

/// In-memory tenant-scoped connector store.
#[derive(Debug, Default)]
pub struct InMemoryConnectorRegistry {
    connectors: Mutex<BTreeMap<(TenantId, ConnectorId), Connector>>,
}

impl InMemoryConnectorRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of stored connectors across tenants.
    #[must_use]
    pub fn len(&self) -> usize {
        self.connectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Connectors of a tenant filtered by provider key.
    #[must_use]
    pub fn list_provider(&self, tenant: TenantId, provider: &str) -> Vec<Connector> {
        self.connectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range((tenant, ConnectorId::nil())..)
            .take_while(|((t, _), _)| *t == tenant)
            .map(|(_, c)| c)
            .filter(|c| c.provider == provider)
            .cloned()
            .collect()
    }
}

#[async_trait]
impl ConnectorRegistryPort for InMemoryConnectorRegistry {
    async fn get(&self, tenant: TenantId, id: ConnectorId) -> Result<Option<Connector>> {
        Ok(self
            .connectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(tenant, id))
            .cloned())
    }

    async fn list(&self, tenant: TenantId) -> Result<Vec<Connector>> {
        Ok(self
            .connectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .range((tenant, ConnectorId::nil())..)
            .take_while(|((t, _), _)| *t == tenant)
            .map(|(_, c)| c.clone())
            .collect())
    }

    async fn save(&self, connector: &Connector) -> Result<()> {
        self.connectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((connector.tenant_id, connector.id), connector.clone());
        Ok(())
    }

    async fn delete(&self, tenant: &TenantId, id: ConnectorId) -> Result<bool> {
        Ok(self
            .connectors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(*tenant, id))
            .is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::enums::ConnectorStatus;
    use mas_common::ids::OrganizationId;
    use mas_domain::ConnectorAuthMode;

    fn connector(tenant: TenantId) -> Connector {
        let org = OrganizationId::new();
        Connector::register(tenant, org, "slack", "primary", ConnectorAuthMode::OAuth2)
            .expect("connector")
    }

    #[tokio::test]
    async fn provider_registry_seeds_builtins_and_registers_new() {
        let registry = InMemoryProviderRegistry::with_builtin();
        assert!(registry.len() >= 8);
        let slack = registry.get("slack").await.expect("get").expect("present");
        assert_eq!(slack.category, ConnectorCategory::Messaging);
        assert!(registry.get("nope").await.expect("get").is_none());

        let custom = ProviderManifest::new(
            "hubspot",
            "HubSpot",
            ConnectorCategory::Crm,
            vec![ConnectorAuthMode::OAuth2],
        )
        .expect("manifest");
        registry.register(custom).await.expect("register");
        assert!(registry.get("hubspot").await.expect("get").is_some());
        assert!(!registry.by_category(&ConnectorCategory::Crm).is_empty());
    }

    #[tokio::test]
    async fn connector_registry_enforces_tenant_scoping() {
        let registry = InMemoryConnectorRegistry::new();
        let tenant_a = TenantId::new();
        let tenant_b = TenantId::new();

        let mut owned = connector(tenant_a);
        registry.save(&owned).await.expect("save");
        assert_eq!(registry.len(), 1);
        assert!(!registry.is_empty());

        assert!(registry
            .get(tenant_a, owned.id)
            .await
            .expect("get")
            .is_some());
        assert!(
            registry
                .get(tenant_b, owned.id)
                .await
                .expect("get")
                .is_none(),
            "cross-tenant existence must be hidden"
        );
        assert_eq!(registry.list(tenant_b).await.expect("list").len(), 0);

        owned.status = ConnectorStatus::Active;
        registry.save(&owned).await.expect("replace");
        let fetched = registry
            .get(tenant_a, owned.id)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(fetched.status, ConnectorStatus::Active);

        assert!(!registry.delete(&tenant_b, owned.id).await.expect("delete"));
        assert!(registry.delete(&tenant_a, owned.id).await.expect("delete"));
        assert!(registry.is_empty());
        assert_eq!(registry.list_provider(tenant_a, "slack").len(), 0);
    }
}
