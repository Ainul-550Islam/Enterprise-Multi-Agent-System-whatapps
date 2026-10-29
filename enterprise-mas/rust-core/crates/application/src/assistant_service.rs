//! PURPOSE: Tenant-scoped built-in assistant submission through the durable execution pipeline.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: bootstrap the immutable system assistant once per tenant and submit
//! messages through `ExecutionService`, preserving authorization, quota, audit, and AgentBridge.
//!
//! Data ownership: tenant/project/agent/version stores and the execution lifecycle ports.
//!
//! MUST NOT: call Python, perform network I/O, expose secrets, or bypass execution policy/quota.

use mas_common::error::AppError;
use mas_common::ids::{AgentId, ProjectId, TenantId, UserId};
use mas_common::result::Result;
use mas_domain::value_objects::Slug;
use mas_domain::{Agent, AgentOrigin, AgentVersion, AuditOutcome};
use sha2::Digest;
use std::sync::Arc;

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;
use crate::execution_service::{ExecutionService, SubmissionOutcome};
use crate::stores::{AgentStorePort, AgentVersionStorePort, ProjectStorePort};

const ASSISTANT_SLUG: &str = "platform-assistant";
const MAX_MESSAGE_BYTES: usize = 16_000;

/// Built-in assistant orchestration over existing tenant and execution ports.
#[derive(Debug)]
pub struct AssistantService {
    projects: Box<dyn ProjectStorePort>,
    agents: Box<dyn AgentStorePort>,
    versions: Box<dyn AgentVersionStorePort>,
    executions: Arc<ExecutionService>,
    audit: Box<dyn AuditSinkPort>,
}

impl AssistantService {
    /// Composes the assistant use case over the same stores and execution service as the API.
    pub fn new(
        projects: Box<dyn ProjectStorePort>,
        agents: Box<dyn AgentStorePort>,
        versions: Box<dyn AgentVersionStorePort>,
        executions: Arc<ExecutionService>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self {
            projects,
            agents,
            versions,
            executions,
            audit,
        }
    }

    /// Creates or resolves the tenant's reserved system agent and submits an ordinary execution.
    pub async fn submit(
        &self,
        ctx: &ServiceContext,
        message: &str,
        idempotency_key: Option<&str>,
    ) -> Result<SubmissionOutcome> {
        validate_message(message)?;
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let mut projects = self
            .projects
            .list(tenant)
            .await?
            .into_iter()
            .filter(|project| project.organization_id == organization)
            .collect::<Vec<_>>();
        projects.sort_by_key(|project| project.id);
        if projects.is_empty() {
            return Err(AppError::not_found(
                "platform assistant",
                tenant.to_string(),
            ));
        }

        let mut existing = None;
        for project in &projects {
            if let Some(agent) = self.agents.get_by_slug(project.id, ASSISTANT_SLUG).await? {
                if agent.tenant_id != tenant
                    || agent.organization_id != organization
                    || agent.project_id != project.id
                {
                    return Err(AppError::not_found(
                        "platform assistant",
                        tenant.to_string(),
                    ));
                }
                if agent.origin != AgentOrigin::System || !agent.is_executable() {
                    return Err(AppError::conflict("reserved platform assistant slug is occupied by a non-system or inactive agent"));
                }
                if existing.is_some() {
                    return Err(AppError::conflict(
                        "tenant has more than one built-in platform assistant",
                    ));
                }
                existing = Some((project.id, agent.id));
            }
        }

        let (project, agent) = match existing {
            Some(existing) => existing,
            None => {
                self.bootstrap(ctx, tenant, organization, projects[0].id)
                    .await?
            },
        };
        self.executions
            .submit(
                ctx,
                project,
                None,
                Some(agent),
                serde_json::json!({"message": message}),
                idempotency_key,
            )
            .await
    }

    async fn bootstrap(
        &self,
        ctx: &ServiceContext,
        tenant: TenantId,
        organization: mas_common::ids::OrganizationId,
        project: ProjectId,
    ) -> Result<(ProjectId, AgentId)> {
        let assistant = Agent::create_system_published(
            tenant,
            organization,
            project,
            "Platform Assistant",
            Slug::new(ASSISTANT_SLUG)?,
            None,
        )?;
        let snapshot = assistant_snapshot();
        let canonical = serde_json::to_vec(&snapshot).map_err(|error| {
            AppError::serialization(format!("platform assistant configuration: {error}"))
        })?;
        let checksum = hex::encode(sha2::Sha256::digest(&canonical));
        let version = AgentVersion::publish(
            assistant.id,
            1,
            checksum,
            snapshot,
            authenticated_user(ctx)?,
        )?;
        self.agents.save(&assistant).await?;
        self.versions.save(&version).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "agent.platform_assistant.bootstrap",
            "agent",
            Some(assistant.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok((project, assistant.id))
    }
}

fn authenticated_user(ctx: &ServiceContext) -> Result<UserId> {
    if ctx.actor.kind != mas_domain::AuditActorKind::User {
        return Err(AppError::forbidden(
            "assistant submission requires an authenticated user principal",
        ));
    }
    let user = UserId::parse_str(&ctx.actor.id)
        .map_err(|_| AppError::forbidden("authenticated user id is invalid"))?;
    if user.is_nil() {
        return Err(AppError::forbidden("authenticated user id is invalid"));
    }
    Ok(user)
}

fn validate_message(message: &str) -> Result<()> {
    mas_common::validation::validate_length("message", message, 1, MAX_MESSAGE_BYTES)?;
    if message.trim().is_empty() {
        return Err(AppError::invalid_field(
            "message",
            "required",
            "assistant message must not be blank",
        ));
    }
    Ok(())
}

fn assistant_snapshot() -> serde_json::Value {
    serde_json::json!({
        "description": "Built-in assistant for authenticated tenant requests.",
        "model_hint": null,
        "system_prompt": "You are the platform assistant. Answer the user's request clearly, safely, and honestly. Respect tenant data boundaries and never claim to have performed actions that were not executed.",
        "limits": {"max_steps": 25, "max_tool_calls": 50, "max_delegation_depth": 0, "timeout_seconds": null},
        "tags": ["platform", "assistant"],
        "allowed_tools": []
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assistant_message_validation_rejects_blank_and_oversized_input() {
        assert!(validate_message("please help").is_ok());
        assert!(validate_message(" \n ").is_err());
        assert!(validate_message(&"x".repeat(MAX_MESSAGE_BYTES + 1)).is_err());
    }

    #[test]
    fn bootstrap_snapshot_is_versionable_and_does_not_enable_tools() {
        let snapshot = assistant_snapshot();
        assert!(snapshot.is_object());
        assert_eq!(snapshot["allowed_tools"], serde_json::json!([]));
        assert!(snapshot["system_prompt"].as_str().is_some());
        let checksum = hex::encode(sha2::Sha256::digest(
            serde_json::to_vec(&snapshot).expect("snapshot"),
        ));
        assert_eq!(checksum.len(), 64);
    }
}
