//! PURPOSE: Tenant-scoped policy lifecycle and dry-run use cases, delegating compilation/evaluation to the canonical `mas-policy` engine.
//!
//! Owning layer: `mas-application`.
//! Production responsibility: create and edit drafts, compile-gate publication, make rollback append-only at the policy-version boundary available to the store, and evaluate without writing decisions or charging quota.
//! Data ownership: policy definitions are owned by the configured PolicyStorePort; audit events by AuditSinkPort.
//! MUST NOT: accept tenant ownership from the request, authorize from UI claims, persist a dry-run decision, or consume quota during evaluation.

use mas_common::{error::AppError, ids::PolicyId, result::Result};
use mas_contracts::policy::{
    CreatePolicyRequest, EvaluatePolicyRequest, PolicyDecisionResponse, PolicyResponse,
    UpdatePolicyRequest,
};
use mas_domain::{AuditOutcome, Policy, PolicyScope, PolicyType};
use mas_policy::{
    engine::PolicyEngine,
    request::{EvaluationContext, EvaluationRequest, EvaluationSubject},
    store::PolicyStorePort,
};
use std::sync::Arc;

use crate::{
    audit::{record_mutation, AuditSinkPort},
    context::ServiceContext,
};

/// Policy lifecycle use cases.
#[derive(Debug)]
pub struct PolicyService {
    store: Arc<dyn PolicyStorePort>,
    engine: Arc<PolicyEngine>,
    audit: Arc<dyn AuditSinkPort>,
}

impl PolicyService {
    /// Composes over the canonical persistent store, engine, and mandatory audit sink.
    pub fn new(
        store: Arc<dyn PolicyStorePort>,
        engine: Arc<PolicyEngine>,
        audit: Arc<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            store,
            engine,
            audit,
        }
    }

    /// Lists only policies belonging to the request's resolved tenant.
    pub async fn list(&self, ctx: &ServiceContext) -> Result<Vec<PolicyResponse>> {
        let tenant = ctx.require_tenant()?;
        let policies = self.store.list_for_tenant(Some(tenant)).await?;
        Ok(policies.into_iter().map(to_response).collect())
    }

    /// Reads one policy; a foreign tenant's policy is indistinguishable from a missing row.
    pub async fn get(&self, ctx: &ServiceContext, id: PolicyId) -> Result<PolicyResponse> {
        let tenant = ctx.require_tenant()?;
        let policy = self
            .store
            .get(id)
            .await?
            .filter(|policy| policy.tenant_id == Some(tenant))
            .ok_or_else(|| AppError::not_found("policy", id.to_string()))?;
        Ok(to_response(policy))
    }

    /// Creates an inactive policy draft owned by the server-resolved tenant and organization.
    pub async fn create(
        &self,
        ctx: &ServiceContext,
        request: CreatePolicyRequest,
    ) -> Result<PolicyResponse> {
        request.validate()?;
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let scope = request.scope.parse::<PolicyScope>()?;
        let kind = request.policy_type.parse::<PolicyType>()?;
        let (tenant_id, organization_id, project_id) = match scope {
            PolicyScope::Global => {
                return Err(AppError::forbidden(
                    "tenant policy APIs cannot create global policies",
                ))
            },
            PolicyScope::Organization => (Some(tenant), Some(organization), None),
            PolicyScope::Tenant => (Some(tenant), Some(organization), None),
            PolicyScope::Project | PolicyScope::Agent => {
                let project = request.project_id.ok_or_else(|| {
                    AppError::invalid_field(
                        "project_id",
                        "required",
                        "project scope requires a project id",
                    )
                })?;
                (Some(tenant), Some(organization), Some(project))
            },
        };
        let mut policy = Policy::new(
            tenant_id,
            organization_id,
            project_id,
            request.name,
            kind,
            scope,
            request.priority,
            request.definition,
        )?;
        policy.description = request.description;
        self.store.upsert(&policy).await?;
        record_mutation(
            ctx,
            self.audit.as_ref(),
            "policy.create",
            "policy",
            Some(policy.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(to_response(policy))
    }

    /// Updates only an existing tenant-owned draft. Definition changes deactivate it and bump its version.
    pub async fn update(
        &self,
        ctx: &ServiceContext,
        id: PolicyId,
        request: UpdatePolicyRequest,
    ) -> Result<PolicyResponse> {
        request.validate()?;
        let tenant = ctx.require_tenant()?;
        let mut policy = self
            .store
            .get(id)
            .await?
            .filter(|policy| policy.tenant_id == Some(tenant))
            .ok_or_else(|| AppError::not_found("policy", id.to_string()))?;
        if let Some(name) = request.name {
            policy.name = name;
        }
        if let Some(description) = request.description {
            policy.description = Some(description);
        }
        if let Some(priority) = request.priority {
            policy.set_priority(priority)?;
        }
        if let Some(definition) = request.definition {
            policy.update_definition(definition)?;
        }
        if request.active == Some(true) {
            return Err(AppError::validation(
                "publish a policy through the publish endpoint",
            ));
        }
        if request.active == Some(false) {
            policy.deactivate();
        }
        self.store.upsert(&policy).await?;
        record_mutation(
            ctx,
            self.audit.as_ref(),
            "policy.update",
            "policy",
            Some(id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(to_response(policy))
    }

    /// Compiles and publishes through the canonical store, then audits the mutation.
    pub async fn publish(&self, ctx: &ServiceContext, id: PolicyId) -> Result<PolicyResponse> {
        let tenant = ctx.require_tenant()?;
        let current = self
            .store
            .get(id)
            .await?
            .filter(|policy| policy.tenant_id == Some(tenant))
            .ok_or_else(|| AppError::not_found("policy", id.to_string()))?;
        let published = self.store.publish(id, &ctx.actor.id).await?;
        if published.tenant_id != Some(tenant) {
            return Err(AppError::not_found("policy", id.to_string()));
        }
        record_mutation(
            ctx,
            self.audit.as_ref(),
            "policy.publish",
            "policy",
            Some(id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        let _previous_version = current.version;
        Ok(to_response(published))
    }

    /// Rollback is an append-only new draft version based on the caller-selected known revision.
    /// The current store contract has no version-history read; the safe supported operation is therefore to restore the last authored definition supplied by `target_definition`.
    pub async fn rollback(
        &self,
        ctx: &ServiceContext,
        id: PolicyId,
        target_definition: serde_json::Value,
    ) -> Result<PolicyResponse> {
        let tenant = ctx.require_tenant()?;
        let mut policy = self
            .store
            .get(id)
            .await?
            .filter(|policy| policy.tenant_id == Some(tenant))
            .ok_or_else(|| AppError::not_found("policy", id.to_string()))?;
        policy.update_definition(target_definition)?;
        self.store.upsert(&policy).await?;
        let rolled_back = self.store.publish(id, &ctx.actor.id).await?;
        record_mutation(
            ctx,
            self.audit.as_ref(),
            "policy.rollback",
            "policy",
            Some(id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(to_response(rolled_back))
    }

    /// Evaluates with the existing policy engine; this path never inserts a decision or touches quota counters.
    pub async fn evaluate(
        &self,
        ctx: &ServiceContext,
        id: PolicyId,
        request: EvaluatePolicyRequest,
    ) -> Result<PolicyDecisionResponse> {
        request.validate()?;
        let tenant = ctx.require_tenant()?;
        self.store
            .get(id)
            .await?
            .filter(|policy| policy.tenant_id == Some(tenant))
            .ok_or_else(|| AppError::not_found("policy", id.to_string()))?;
        let organization = ctx.require_organization()?;
        let roles = ctx
            .resolved_role
            .as_ref()
            .map(|role| vec![role.as_str().to_owned()])
            .unwrap_or_default();
        let subject = EvaluationSubject::new(ctx.actor.id.clone())?.with_roles(roles);
        let context = EvaluationContext::new(tenant, mas_common::enums::Environment::Production)
            .with_organization(organization);
        let attributes =
            serde_json::json!({"resource": request.resource, "environment": request.environment});
        let evaluation_request = EvaluationRequest::new(
            subject,
            request.action,
            "policy:evaluate",
            attributes,
            context,
        )?;
        let evaluation = self.engine.evaluate(&evaluation_request).await?;
        let reasons = evaluation.reason.into_iter().collect();
        let transformed = evaluation.transform.map(serde_json::Value::Object);
        Ok(PolicyDecisionResponse {
            decision: evaluation.decision,
            reasons,
            policy_ids: evaluation
                .matched
                .into_iter()
                .map(|matched| matched.policy_id)
                .collect(),
            approval_required: None,
            retry_after_ms: None,
            transformed,
        })
    }
}

fn to_response(policy: Policy) -> PolicyResponse {
    PolicyResponse {
        id: policy.id,
        tenant_id: policy.tenant_id,
        project_id: policy.project_id,
        name: policy.name,
        policy_type: policy.policy_type.to_string(),
        scope: policy.scope.to_string(),
        priority: policy.priority,
        active: policy.active,
        version: policy.version,
        definition: policy.definition,
        created_at: policy.created_at,
        updated_at: policy.updated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;
    use mas_policy::store::InMemoryPolicyStore;

    #[test]
    fn contract_policy_scope_values_are_canonical() {
        assert_eq!(
            "tenant".parse::<PolicyScope>().expect("scope"),
            PolicyScope::Tenant
        );
        assert_eq!(
            "execution".parse::<PolicyType>().expect("type"),
            PolicyType::Execution
        );
    }

    #[test]
    fn service_requires_canonical_engine_and_store() {
        let store = Arc::new(InMemoryPolicyStore::new());
        let engine = Arc::new(PolicyEngine::new(store.clone()));
        let _service = PolicyService::new(store, engine, Arc::new(InMemoryAuditSink::new()));
    }
}
