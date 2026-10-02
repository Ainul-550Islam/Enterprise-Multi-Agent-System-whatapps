//! Policy store port + in-memory implementation.
//!
//! The store is the policy *source of truth* (Postgres behind this port in
//! production). [`PolicyStorePort::publish`] compiles the definition through
//! the strict compiler before activation — an invalid definition can never
//! become active, regardless of the caller path.

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, PolicyId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_domain::{Policy, PolicyScope, PolicyType};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

/// Selection for evaluation-time reads.
#[derive(Debug, Clone, Default)]
pub struct PolicyQuery {
    /// Tenant context; `None` → only global policies.
    pub tenant_id: Option<TenantId>,
    pub organization_id: Option<OrganizationId>,
    pub project_id: Option<ProjectId>,
    /// Restrict by functional type (authorization/execution/…).
    pub policy_type: Option<PolicyType>,
}

impl PolicyQuery {
    #[must_use]
    pub fn for_context(
        tenant_id: TenantId,
        organization_id: Option<OrganizationId>,
        project_id: Option<ProjectId>,
    ) -> Self {
        Self {
            tenant_id: Some(tenant_id),
            organization_id,
            project_id,
            policy_type: None,
        }
    }

    #[must_use]
    pub const fn with_type(mut self, policy_type: PolicyType) -> Self {
        self.policy_type = Some(policy_type);
        self
    }
}

#[async_trait::async_trait]
pub trait PolicyStorePort: Send + Sync + fmt::Debug {
    // -- reads ---------------------------------------------------------------
    async fn get(&self, policy_id: PolicyId) -> Result<Option<Policy>>;

    /// All *enforceable* (published + active) policies applicable to the
    /// query scope: global policies, then the matching organization/tenant/
    /// project-scoped ones. Never returns another tenant's policies.
    async fn list_applicable(&self, query: &PolicyQuery) -> Result<Vec<Policy>>;

    /// Admin read: tenant's own policies (any state).
    async fn list_for_tenant(&self, tenant_id: Option<TenantId>) -> Result<Vec<Policy>>;

    // -- lifecycle -----------------------------------------------------------
    /// Insert-or-update the authored policy (draft edits).
    async fn upsert(&self, policy: &Policy) -> Result<()>;

    /// Compiles the current definition and activates the policy. Fails when
    /// compilation fails — nothing becomes active then.
    async fn publish(&self, policy_id: PolicyId, actor: &str) -> Result<Policy>;

    async fn deactivate(&self, policy_id: PolicyId, actor: &str) -> Result<Policy>;

    /// Hard delete; active policies must be deactivated first.
    async fn delete(&self, tenant_id: Option<TenantId>, policy_id: PolicyId) -> Result<Policy>;
}

/// In-memory store: deterministic, tenant-safe, compile-gated publishing.
#[derive(Debug, Default)]
pub struct InMemoryPolicyStore {
    policies: Mutex<BTreeMap<PolicyId, Policy>>,
}

impl InMemoryPolicyStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<PolicyId, Policy>> {
        self.policies.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Test/admin hook: preloaded published policies.
    pub async fn seed_published(&self, policy: &Policy) -> Result<Policy> {
        self.upsert(policy).await?;
        self.publish(policy.id, "seed").await
    }
}

#[async_trait::async_trait]
impl PolicyStorePort for InMemoryPolicyStore {
    async fn get(&self, policy_id: PolicyId) -> Result<Option<Policy>> {
        Ok(self.lock().get(&policy_id).cloned())
    }

    async fn list_applicable(&self, query: &PolicyQuery) -> Result<Vec<Policy>> {
        let store = self.lock();
        let mut matches: Vec<Policy> = store
            .values()
            .filter(|policy| policy.is_enforced())
            .filter(|policy| {
                query
                    .policy_type
                    .as_ref()
                    .is_none_or(|t| policy.policy_type == *t)
            })
            .filter(|policy| match policy.scope {
                PolicyScope::Global => true,
                PolicyScope::Organization => {
                    query.organization_id.is_some()
                        && policy.organization_id == query.organization_id
                },
                PolicyScope::Tenant => {
                    query.tenant_id.is_some() && policy.tenant_id == query.tenant_id
                },
                PolicyScope::Project | PolicyScope::Agent => {
                    query.project_id.is_some()
                        && policy.project_id == query.project_id
                        && policy.tenant_id == query.tenant_id
                },
            })
            .cloned()
            .collect();
        // Deterministic evaluation order: priority desc, then id asc.
        matches.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.id.to_string().cmp(&b.id.to_string()))
        });
        Ok(matches)
    }

    async fn list_for_tenant(&self, tenant_id: Option<TenantId>) -> Result<Vec<Policy>> {
        Ok(self
            .lock()
            .values()
            .filter(|policy| policy.tenant_id == tenant_id)
            .cloned()
            .collect())
    }

    async fn upsert(&self, policy: &Policy) -> Result<()> {
        policy.validate_definition()?;
        self.lock().insert(policy.id, policy.clone());
        Ok(())
    }

    async fn publish(&self, policy_id: PolicyId, actor: &str) -> Result<Policy> {
        let mut store = self.lock();
        let policy = store
            .get_mut(&policy_id)
            .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))?;
        let compiled = crate::compiler::compile(policy)?;
        policy.publish(compiled.checksum)?;
        let policy = policy.clone();
        drop(store);
        tracing::info!(%policy_id, actor, checksum = %policy.compiled_checksum.clone().unwrap_or_default(), "policy published");
        Ok(policy)
    }

    async fn deactivate(&self, policy_id: PolicyId, actor: &str) -> Result<Policy> {
        let mut store = self.lock();
        let policy = store
            .get_mut(&policy_id)
            .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))?;
        policy.deactivate();
        let policy = policy.clone();
        drop(store);
        tracing::info!(%policy_id, actor, "policy deactivated");
        Ok(policy)
    }

    async fn delete(&self, tenant_id: Option<TenantId>, policy_id: PolicyId) -> Result<Policy> {
        let mut store = self.lock();
        let policy = store
            .get(&policy_id)
            .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))?;
        if policy.tenant_id != tenant_id {
            // Never reveal cross-tenant existence.
            return Err(AppError::not_found("policy", policy_id.to_string()));
        }
        if policy.is_enforced() {
            return Err(AppError::conflict(
                "active policies must be deactivated before deletion",
            ));
        }
        store
            .remove(&policy_id)
            .ok_or_else(|| AppError::not_found("policy", policy_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::OrganizationId;
    use serde_json::json;

    fn policy(
        tenant: Option<TenantId>,
        org: Option<OrganizationId>,
        project: Option<ProjectId>,
        name: &str,
        policy_type: PolicyType,
        scope: PolicyScope,
        definition: serde_json::Value,
    ) -> Policy {
        policy_with_priority(
            tenant,
            org,
            project,
            name,
            policy_type,
            scope,
            0,
            definition,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn policy_with_priority(
        tenant: Option<TenantId>,
        org: Option<OrganizationId>,
        project: Option<ProjectId>,
        name: &str,
        policy_type: PolicyType,
        scope: PolicyScope,
        priority: i32,
        definition: serde_json::Value,
    ) -> Policy {
        Policy::new(
            tenant,
            org,
            project,
            name,
            policy_type,
            scope,
            priority,
            definition,
        )
        .expect("policy")
    }

    fn allow_all_def() -> serde_json::Value {
        json!({
            "default_effect": "deny",
            "rules": [{"id": "allow", "effect": "allow"}]
        })
    }

    #[tokio::test]
    async fn publish_compiles_before_activating() {
        let store = InMemoryPolicyStore::new();
        let tenant = TenantId::new();
        let org = OrganizationId::new();
        let good = policy(
            Some(tenant),
            None,
            None,
            "good",
            PolicyType::Authorization,
            PolicyScope::Tenant,
            allow_all_def(),
        );
        store.upsert(&good).await.expect("upsert");
        let published = store.publish(good.id, "tester").await.expect("publish");
        assert!(published.is_enforced());
        assert!(published.compiled_checksum.is_some());

        let bad = policy(
            Some(tenant),
            None,
            None,
            "bad",
            PolicyType::Authorization,
            PolicyScope::Tenant,
            json!({"rules": [{"id": "x", "effect": "magic"}]}),
        );
        store.upsert(&bad).await.expect("upsert bad (drafts leg)");
        assert!(store.publish(bad.id, "tester").await.is_err());
        assert!(
            !store
                .get(bad.id)
                .await
                .expect("get")
                .expect("some")
                .is_enforced(),
            "failed compile must not activate"
        );
        let _ = org; // scope coherence already covered in domain tests
    }

    #[tokio::test]
    async fn tenant_isolation_and_scope_order_hold() {
        let store = InMemoryPolicyStore::new();
        let tenant_a = TenantId::new();
        let tenant_b = TenantId::new();
        let global = policy(
            None,
            None,
            None,
            "global",
            PolicyType::Compliance,
            PolicyScope::Global,
            allow_all_def(),
        );
        let for_a = policy_with_priority(
            Some(tenant_a),
            None,
            None,
            "a",
            PolicyType::Execution,
            PolicyScope::Tenant,
            10,
            allow_all_def(),
        );
        let for_b = policy(
            Some(tenant_b),
            None,
            None,
            "b",
            PolicyType::Execution,
            PolicyScope::Tenant,
            allow_all_def(),
        );
        for p in [&global, &for_a, &for_b] {
            store.seed_published(p).await.expect("seed");
        }
        let query = PolicyQuery::for_context(tenant_a, None, None);
        let applicable = store.list_applicable(&query).await.expect("list");
        let names: Vec<_> = applicable.iter().map(|p| p.name.as_str()).collect();
        // No type filter: tenant policy "a" (execution) first by presumed order,
        // plus every applicable global policy regardless of type.
        assert_eq!(names, vec!["a", "global"]);
        let for_tenant_b = store
            .list_applicable(&PolicyQuery::for_context(tenant_b, None, None))
            .await
            .expect("list b");
        assert!(for_tenant_b.iter().all(|p| p.name != "a"));
        // delete rules
        let active_del = store.delete(Some(tenant_a), for_a.id).await;
        assert!(active_del.is_err(), "active deletion rejected");
        store
            .deactivate(for_a.id, "tester")
            .await
            .expect("deactivate");
        store
            .delete(Some(tenant_a), for_a.id)
            .await
            .expect("delete");
        assert!(
            store.delete(Some(tenant_b), global.id).await.is_err(),
            "tenant B cannot delete the global policy (cross ownership hidden)"
        );
    }
}
