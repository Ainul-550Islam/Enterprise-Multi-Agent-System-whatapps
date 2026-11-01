//! Adapter from the orchestration `PolicyPort` onto the [`PolicyEngine`].
//!
//! Contracts, orchestration and execution callers never see the DSL: they
//! speak `(context, action, resource, attributes)` — PPR (principal-path-
//! restriction) — and receive a [`PolicyDecision`]. Transform patches are
//! surfaced via the decision itself (`PolicyDecision::Transform` plus the
//! engine's [`Evaluation`] for callers that opt in via
//! [`EnginePolicyPort::evaluate_full`]).

use crate::engine::{Evaluation, PolicyEngine};
use crate::request::{EvaluationContext, EvaluationRequest, EvaluationSubject};
use mas_common::enums::PolicyDecision;
use mas_common::result::Result;
use mas_orchestration::engine::PolicyPort;
use mas_orchestration::runtime_context::RuntimeContext;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

/// Exposes the engine where the runtime expects a `PolicyPort`.
pub struct EnginePolicyPort {
    engine: Arc<PolicyEngine>,
}

impl fmt::Debug for EnginePolicyPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnginePolicyPort")
            .field("engine", &self.engine)
            .finish()
    }
}

impl EnginePolicyPort {
    pub fn new(engine: Arc<PolicyEngine>) -> Self {
        Self { engine }
    }

    /// Full evaluation (provenance, transform patches, matched rules).
    pub async fn evaluate_full(
        &self,
        context: &RuntimeContext,
        action: &str,
        resource: &str,
        attributes: &Value,
    ) -> Result<Evaluation> {
        let request = request_from(context, action, resource, attributes)?;
        self.engine.evaluate(&request).await
    }
}

/// Maps a runtime context into canonical evaluation input.
fn request_from(
    context: &RuntimeContext,
    action: &str,
    resource: &str,
    attributes: &Value,
) -> Result<EvaluationRequest> {
    let subject = EvaluationSubject::new(context.actor_id())?.with_roles([] as [&str; 0]);
    let scope = EvaluationContext::new(context.tenant_id(), context.environment())
        .with_organization(context.organization_id())
        .with_project(context.project_id());
    EvaluationRequest::new(subject, action, resource, attributes.clone(), scope)
}

#[async_trait::async_trait]
impl PolicyPort for EnginePolicyPort {
    async fn evaluate(
        &self,
        context: &RuntimeContext,
        action: &str,
        resource: &str,
        attributes: &Value,
    ) -> Result<PolicyDecision> {
        Ok(self
            .evaluate_full(context, action, resource, attributes)
            .await?
            .decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::InMemoryPolicyStore;
    use mas_common::enums::Environment;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId};
    use mas_domain::{Policy, PolicyScope, PolicyType};
    use serde_json::json;

    #[tokio::test]
    async fn runtime_context_decisions_pass_through() {
        let tenant = TenantId::new();
        let org = OrganizationId::new();
        let project = ProjectId::new();
        let store = Arc::new(InMemoryPolicyStore::new());
        let policy = Policy::new(
            Some(tenant),
            Some(org),
            Some(project),
            "gate",
            PolicyType::Execution,
            PolicyScope::Project,
            1,
            json!({"rules": [{"id": "deny-tools", "effect": "deny",
                               "actions": ["tool.invoke"], "reason": "tools disabled"}]}),
        )
        .expect("policy");
        store.seed_published(&policy).await.expect("seed");

        let engine = Arc::new(PolicyEngine::new(store));
        let port = EnginePolicyPort::new(engine);
        let context = RuntimeContext::new(tenant, org, project, "alice", Environment::Development)
            .expect("context");

        let decision = port
            .evaluate(&context, "tool.invoke", "tool:x", &json!({}))
            .await
            .expect("evaluate");
        assert_eq!(decision, PolicyDecision::Deny);
        let decision = port
            .evaluate(&context, "task.submit", "task", &json!({}))
            .await
            .expect("evaluate");
        assert_eq!(
            decision,
            PolicyDecision::Deny,
            "default-deny with no rule matching"
        );
        // Provenance path exposes the rule + reason.
        let full = port
            .evaluate_full(&context, "tool.invoke", "tool:x", &json!({}))
            .await
            .expect("full");
        assert_eq!(full.reason.as_deref(), Some("tools disabled"));
        assert_eq!(full.matched.len(), 1);
    }
}
