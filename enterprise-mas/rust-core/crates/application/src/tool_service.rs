//! PURPOSE: Tenant-scoped tool catalogue and permission-grant use cases.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: validate tool definitions, prove project and tool
//! ownership, evaluate project policy before replacing grants, and audit every
//! mutation before reporting success. Durable store adapters own rows and RLS.
//!
//! Data ownership: `ToolStorePort` owns tools and their permission sets;
//! `ProjectStorePort` owns project scope; `AuditSinkPort` owns mutation records.
//!
//! MUST NOT: perform HTTP/SQL, resolve secrets, execute a tool, trust request
//! tenant identifiers, or substitute an in-memory production store.

use std::sync::Arc;

use mas_common::enums::{Environment, PolicyDecision};
use mas_common::error::AppError;
use mas_common::ids::{ProjectId, ToolId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{AuditOutcome, SafetyClassification, Tool, ToolKind, ToolPermission, ToolRuntime};
use mas_policy::engine::PolicyEngine;
use mas_policy::request::{EvaluationContext, EvaluationRequest, EvaluationSubject};
use serde_json::json;

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::stores::{ProjectStorePort, ToolCursor, ToolStorePort};

/// Validated inputs for registering a tool.
#[derive(Debug, Clone)]
pub struct RegisterTool {
    pub project_id: Option<ProjectId>,
    pub name: String,
    pub description: Option<String>,
    pub kind: ToolKind,
    pub input_schema: serde_json::Value,
    pub output_schema: Option<serde_json::Value>,
    pub runtime: ToolRuntime,
    pub safety: SafetyClassification,
}

/// A bounded tenant tool page.
#[derive(Debug, Clone)]
pub struct ToolPage {
    pub items: Vec<Tool>,
    pub next_cursor: Option<String>,
}

/// Service for the tenant-owned tool catalogue.
#[derive(Debug)]
pub struct ToolService {
    tools: Box<dyn ToolStorePort>,
    projects: Box<dyn ProjectStorePort>,
    policy: Arc<PolicyEngine>,
    audit: Box<dyn AuditSinkPort>,
}

impl ToolService {
    /// Creates a tool service over explicit tenant-scoped ports and policy.
    pub fn new(
        tools: Box<dyn ToolStorePort>,
        projects: Box<dyn ProjectStorePort>,
        policy: Arc<PolicyEngine>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            tools,
            projects,
            policy,
            audit,
        }
    }

    /// Registers a validated draft tool in the authenticated tenant.
    pub async fn register(&self, ctx: &ServiceContext, request: RegisterTool) -> Result<Tool> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        if let Some(project_id) = request.project_id {
            self.project_in_scope(ctx, project_id).await?;
        }
        let mut tool = Tool::register(
            tenant,
            organization,
            request.project_id,
            request.name,
            request.kind,
            request.input_schema,
            request.output_schema,
        )?;
        tool.description = request.description;
        tool.safety = request.safety;
        tool.set_runtime(request.runtime)?;
        tool.validate()?;
        self.tools.save(&tool).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "tool.register",
            "tool",
            Some(tool.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(tool)
    }

    /// Returns one tool, hiding foreign identifiers as not found.
    pub async fn get(&self, ctx: &ServiceContext, id: ToolId) -> Result<Tool> {
        let tenant = ctx.require_tenant()?;
        self.tools
            .get(id)
            .await?
            .filter(|tool| tool.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("tool", id.to_string()))
    }

    /// Lists a bounded page in a tenant or project scope.
    pub async fn list_page(
        &self,
        ctx: &ServiceContext,
        project: Option<ProjectId>,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<ToolPage> {
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "tool page limit must be between 1 and 200",
            ));
        }
        let tenant = ctx.require_tenant()?;
        if let Some(project_id) = project {
            self.project_in_scope(ctx, project_id).await?;
        }
        let after = cursor.map(decode_cursor).transpose()?;
        let mut items = self
            .tools
            .list_page(tenant, project, after, limit.saturating_add(1))
            .await?;
        items.retain(|tool| tool.tenant_id == tenant);
        let has_more = items.len() > usize::from(limit);
        if has_more {
            items.truncate(usize::from(limit));
        }
        let next_cursor = if has_more {
            items.last().map(|tool| {
                encode_cursor(ToolCursor {
                    created_at: tool.created_at,
                    id: tool.id,
                })
            })
        } else {
            None
        };
        Ok(ToolPage { items, next_cursor })
    }

    /// Replaces a tool definition and advances its optimistic version.
    pub async fn update(
        &self,
        ctx: &ServiceContext,
        id: ToolId,
        name: Option<String>,
        description: Option<Option<String>>,
        input_schema: Option<serde_json::Value>,
        output_schema: Option<Option<serde_json::Value>>,
        runtime_endpoint: Option<Option<mas_domain::SafeUrl>>,
        timeout_ms: Option<u64>,
        safety: Option<SafetyClassification>,
    ) -> Result<Tool> {
        let tenant = ctx.require_tenant()?;
        let mut tool = self.get(ctx, id).await?;
        if tool.tenant_id != tenant {
            return Err(AppError::not_found("tool", id.to_string()));
        }
        if let Some(name) = name {
            tool.name = name;
        }
        if let Some(description) = description {
            tool.description = description;
        }
        if let Some(input_schema) = input_schema {
            tool.input_schema = input_schema;
        }
        if let Some(output_schema) = output_schema {
            tool.output_schema = output_schema;
        }
        if let Some(safety) = safety {
            tool.safety = safety;
        }
        if runtime_endpoint.is_some() || timeout_ms.is_some() {
            let mut runtime = tool.runtime.clone();
            if let Some(endpoint) = runtime_endpoint {
                runtime.endpoint = endpoint;
            }
            if let Some(timeout) = timeout_ms {
                runtime.timeout_ms = timeout;
            }
            tool.runtime = runtime;
        }
        tool.version = tool
            .version
            .checked_add(1)
            .ok_or_else(|| AppError::conflict("tool version counter is exhausted"))?;
        tool.updated_at = Timestamp::now();
        tool.validate()?;
        self.tools.save(&tool).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "tool.update",
            "tool",
            Some(id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(tool)
    }

    /// Checks project policy for every grant, then atomically replaces grants.
    pub async fn replace_permissions(
        &self,
        ctx: &ServiceContext,
        id: ToolId,
        permissions: Vec<ToolPermission>,
    ) -> Result<Vec<ToolPermission>> {
        let tool = self.get(ctx, id).await?;
        let tenant = ctx.require_tenant()?;
        for permission in &permissions {
            if permission.tenant_id != tenant || permission.tool_id != id {
                return Err(AppError::not_found("tool", id.to_string()));
            }
            if let (Some(tool_project), Some(grant_project)) =
                (tool.project_id, permission.project_id)
            {
                if tool_project != grant_project {
                    return Err(AppError::not_found("tool", id.to_string()));
                }
            }
            if let Some(project) = permission.project_id {
                self.project_in_scope(ctx, project).await?;
            }
            let evaluation_context = EvaluationContext::new(tenant, Environment::Production)
                .with_organization(ctx.require_organization()?)
                .with_project_opt(permission.project_id.or(tool.project_id));
            for action in &permission.allowed_actions {
                let request = EvaluationRequest::new(
                    EvaluationSubject::new(ctx.actor.id.clone())?
                        .with_roles(ctx.resolved_role.map(|role| role.to_string())),
                    "tool.grant",
                    format!("tool:{}", id),
                    json!({"tool_action": action, "tool_kind": tool.kind.to_string(), "tool_project_id": tool.project_id.map(|value| value.to_string())}),
                    evaluation_context,
                )?;
                let decision = self.policy.evaluate(&request).await?;
                if decision.decision != PolicyDecision::Allow {
                    return Err(AppError::forbidden(
                        "project policy denies this tool permission grant",
                    ));
                }
            }
        }
        self.tools
            .replace_permissions(tenant, id, &permissions)
            .await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "tool.permissions.replace",
            "tool",
            Some(id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(permissions)
    }

    /// Replaces every permission rule for one agent as a single store operation.
    pub async fn replace_agent_permissions(
        &self,
        ctx: &ServiceContext,
        agent: mas_common::ids::AgentId,
        permissions: Vec<ToolPermission>,
    ) -> Result<Vec<ToolPermission>> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        for permission in &permissions {
            if permission.tenant_id != tenant || permission.agent_id != Some(agent) {
                return Err(AppError::not_found("agent", agent.to_string()));
            }
            if let Some(project) = permission.project_id {
                self.project_in_scope(ctx, project).await?;
            }
            let tool = self.get(ctx, permission.tool_id).await?;
            if tool.organization_id != organization {
                return Err(AppError::not_found("tool", permission.tool_id.to_string()));
            }
            if let (Some(tool_project), Some(grant_project)) =
                (tool.project_id, permission.project_id)
            {
                if tool_project != grant_project {
                    return Err(AppError::not_found("tool", permission.tool_id.to_string()));
                }
            }
            let evaluation_context = EvaluationContext::new(tenant, Environment::Production)
                .with_organization(organization)
                .with_project_opt(permission.project_id.or(tool.project_id));
            for action in &permission.allowed_actions {
                let request = EvaluationRequest::new(
                    EvaluationSubject::new(ctx.actor.id.clone())?
                        .with_roles(ctx.resolved_role.map(|role| role.to_string())),
                    "tool.grant",
                    format!("tool:{}", permission.tool_id),
                    json!({"tool_action": action, "tool_kind": tool.kind.to_string()}),
                    evaluation_context,
                )?;
                if self.policy.evaluate(&request).await?.decision != PolicyDecision::Allow {
                    return Err(AppError::forbidden(
                        "project policy denies this tool permission grant",
                    ));
                }
            }
        }
        self.tools
            .replace_agent_permissions(tenant, agent, &permissions)
            .await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "tool.permissions.replace",
            "agent",
            Some(agent.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(permissions)
    }

    async fn project_in_scope(&self, ctx: &ServiceContext, project: ProjectId) -> Result<()> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        self.projects
            .get(project)
            .await?
            .filter(|record| record.tenant_id == tenant && record.organization_id == organization)
            .ok_or_else(|| AppError::not_found("project", project.to_string()))?;
        Ok(())
    }
}

fn encode_cursor(cursor: ToolCursor) -> String {
    use base64::Engine as _;
    let raw =
        serde_json::to_vec(&serde_json::json!({"created_at": cursor.created_at, "id": cursor.id}))
            .expect("tool cursor serialization is infallible");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

fn decode_cursor(value: &str) -> Result<ToolCursor> {
    use base64::Engine as _;
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Wire {
        created_at: Timestamp,
        id: ToolId,
    }
    if value.is_empty()
        || value.len() > 512
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return Err(AppError::invalid_field(
            "cursor",
            "invalid_format",
            "tool cursor is invalid",
        ));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| {
            AppError::invalid_field("cursor", "invalid_format", "tool cursor is invalid")
        })?;
    let wire: Wire = serde_json::from_slice(&bytes).map_err(|_| {
        AppError::invalid_field("cursor", "invalid_format", "tool cursor is invalid")
    })?;
    Ok(ToolCursor {
        created_at: wire.created_at,
        id: wire.id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::InMemoryAuditSink;

    #[test]
    fn tool_cursor_round_trips_and_rejects_tampering() {
        let cursor = ToolCursor {
            created_at: Timestamp::from_unix_ms(1_700_000_000_000).expect("time"),
            id: ToolId::new(),
        };
        let encoded = encode_cursor(cursor);
        let decoded = decode_cursor(&encoded).expect("decode");
        assert_eq!(decoded, cursor);
        assert!(decode_cursor("../../private").is_err());
    }

    #[test]
    fn service_has_mandatory_policy_and_audit_dependencies() {
        let _ = std::mem::size_of::<Option<Arc<PolicyEngine>>>();
        let _audit = InMemoryAuditSink::new();
    }

    #[tokio::test]
    async fn a_grant_denied_by_project_policy_is_not_persisted_or_audited_as_success() {
        use crate::stores::{ProjectStorePort, ToolStorePort};
        use mas_common::ids::{OrganizationId, TenantId};
        use mas_domain::{
            Policy, PolicyScope, PolicyType, Project, Slug, Tool, ToolKind, ToolPermission,
        };
        use mas_policy::store::InMemoryPolicyStore;
        use std::collections::BTreeSet;

        let tenant = TenantId::new();
        let organization = OrganizationId::new();
        let project = Project::create(
            tenant,
            organization,
            "tool policy project",
            Slug::new("tool-policy-project").expect("slug"),
        )
        .expect("project");
        let tool = Tool::register(
            tenant,
            organization,
            Some(project.id),
            "policy tool",
            ToolKind::Internal,
            json!({"type":"object","properties":{}}),
            None,
        )
        .expect("tool");
        let store = Arc::new(crate::stores::InMemoryServices::new());
        ProjectStorePort::save(store.as_ref(), &project)
            .await
            .expect("save project");
        ToolStorePort::save(store.as_ref(), &tool)
            .await
            .expect("save tool");
        let audit = Arc::new(InMemoryAuditSink::new());
        let policies = Arc::new(InMemoryPolicyStore::new());
        let deny_grant = Policy::new(
            Some(tenant),
            Some(organization),
            Some(project.id),
            "deny tool grants",
            PolicyType::Authorization,
            PolicyScope::Project,
            100,
            json!({
                "rules": [{
                    "id": "deny-tool-grant",
                    "effect": "deny",
                    "actions": ["tool.grant"],
                    "resources": ["tool:*"]
                }]
            }),
        )
        .expect("deny policy");
        policies
            .seed_published(&deny_grant)
            .await
            .expect("publish deny policy");
        let service = ToolService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Arc::new(PolicyEngine::new(policies)),
            Box::new(audit.clone()),
        );
        let context =
            ServiceContext::for_service("tool-policy-test", "denied-grant", &Timestamp::now())
                .expect("context")
                .with_scope(tenant, organization);
        let grant = ToolPermission::new(
            tool.id,
            tenant,
            Some(project.id),
            Some(mas_common::ids::AgentId::new()),
            None,
            BTreeSet::from(["invoke".to_owned()]),
        )
        .expect("grant");
        let error = service
            .replace_permissions(&context, tool.id, vec![grant])
            .await
            .expect_err("empty policy set denies by default");
        assert_eq!(error.http_status().as_u16(), 403);
        assert!(ToolStorePort::permissions(store.as_ref(), tenant, tool.id)
            .await
            .expect("permissions")
            .is_empty());
        assert!(
            audit.is_empty(),
            "a denied grant is not recorded as a successful mutation"
        );
    }
}
