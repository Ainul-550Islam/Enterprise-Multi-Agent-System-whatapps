//! Tenant-scoped approval queue wrapper around the canonical policy state machine.

use std::sync::Arc;

use mas_common::error::AppError;
use mas_common::ids::ApprovalRequestId;
use mas_common::result::Result;
use mas_contracts::approval::{ApprovalQuery, ApprovalResponse};
use mas_domain::{AuditEvent, AuditOutcome};
use mas_policy::approval::{
    ApprovalRequest, ApprovalService as PolicyApprovalService, ApprovalStatus, ApprovalStorePort,
};

use crate::audit::AuditSinkPort;
use crate::context::ServiceContext;

/// Tenant-scoped, audited facade over `mas_policy::approval::ApprovalService`.
#[derive(Debug)]
pub struct ApprovalApplicationService {
    store: Arc<dyn ApprovalStorePort>,
    state_machine: PolicyApprovalService,
    audit: Arc<dyn AuditSinkPort>,
}

impl ApprovalApplicationService {
    /// Wires the shared persistent approval port and mandatory audit sink.
    pub fn new(store: Arc<dyn ApprovalStorePort>, audit: Arc<dyn AuditSinkPort>) -> Self {
        Self {
            state_machine: PolicyApprovalService::new(store.clone()),
            store,
            audit,
        }
    }

    /// Lists the current tenant's pending or decided requests. Due expiries
    /// are materialized before the list is returned.
    pub async fn list(
        &self,
        ctx: &ServiceContext,
        query: &ApprovalQuery,
    ) -> Result<Vec<ApprovalResponse>> {
        query.validate()?;
        let tenant = ctx.require_tenant()?;
        for expired in self.state_machine.sweep_expired(tenant).await? {
            self.record_decision(ctx, &expired, "approval.expire", "request expired")
                .await?;
        }
        let mut requests = self.store.list_for_tenant(tenant).await?;
        if let Some(status) = query.status.as_deref() {
            requests.retain(|request| request.status.as_str() == status);
        }
        requests.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| right.created_at.cmp(&left.created_at))
        });
        requests.truncate(usize::from(query.limit.unwrap_or(50)));
        Ok(requests.iter().map(approval_response).collect())
    }

    /// Applies an authenticated approve/deny decision. Requester self-voting,
    /// duplicate votes, role allow-lists, quorum and expiry remain governed by
    /// the canonical policy state machine.
    pub async fn decide(
        &self,
        ctx: &ServiceContext,
        id: ApprovalRequestId,
        approve: bool,
        reason: &str,
    ) -> Result<ApprovalResponse> {
        let tenant = ctx.require_tenant()?;
        let existing = self
            .store
            .get(id)
            .await?
            .filter(|request| request.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("approval request", id.to_string()))?;
        if existing.status != ApprovalStatus::Pending {
            return Err(AppError::conflict("approval request is already decided"));
        }
        let actor = ctx.actor.id.clone();
        let approver_role = Some(ctx.require_resolved_role()?.to_string());
        if actor == existing.subject_actor {
            return Err(AppError::forbidden(
                "the requesting actor cannot decide their own approval request",
            ));
        }
        let status = if approve {
            self.state_machine
                .approve_with_reason(id, actor, approver_role, Some(reason.to_owned()))
                .await?
        } else {
            self.state_machine
                .reject_with_reason(id, actor, approver_role, Some(reason.to_owned()))
                .await?
        };
        let decided = self
            .store
            .get(id)
            .await?
            .filter(|request| request.tenant_id == tenant)
            .ok_or_else(|| AppError::not_found("approval request", id.to_string()))?;
        let action = if approve {
            "approval.approve"
        } else {
            "approval.deny"
        };
        self.record_decision(ctx, &decided, action, reason).await?;
        debug_assert_eq!(status, decided.status);
        Ok(approval_response(&decided))
    }

    /// Raises an approval request through the existing domain state machine.
    pub async fn raise(
        &self,
        ctx: &ServiceContext,
        request: ApprovalRequest,
    ) -> Result<ApprovalResponse> {
        let tenant = ctx.require_tenant()?;
        if request.tenant_id != tenant {
            return Err(AppError::not_found("tenant", request.tenant_id.to_string()));
        }
        let request = self.state_machine.raise(request).await?;
        self.record_decision(ctx, &request, "approval.raise", "approval required")
            .await?;
        Ok(approval_response(&request))
    }

    async fn record_decision(
        &self,
        ctx: &ServiceContext,
        request: &ApprovalRequest,
        action: &str,
        reason: &str,
    ) -> Result<()> {
        let mut event = AuditEvent::new(
            Some(request.tenant_id),
            request.organization_id,
            ctx.actor.clone(),
            action,
            "approval_request",
            Some(request.id.to_string()),
            AuditOutcome::Success,
            ctx.correlation_id.clone(),
        )?;
        event.metadata.insert(
            "decision_reason".to_owned(),
            serde_json::Value::String(reason.chars().take(1024).collect()),
        );
        event.metadata.insert(
            "status".to_owned(),
            serde_json::Value::String(request.status.to_string()),
        );
        self.audit.record(&event).await
    }
}

fn approval_response(request: &ApprovalRequest) -> ApprovalResponse {
    ApprovalResponse {
        id: request.id,
        tenant_id: request.tenant_id,
        subject_actor: request.subject_actor.clone(),
        action: request.action.clone(),
        resource: request.resource.clone(),
        reason: request.reason.clone(),
        quorum: request.quorum,
        approvals: u16::try_from(request.approvals.len()).unwrap_or(u16::MAX),
        rejections: u16::try_from(request.rejections.len()).unwrap_or(u16::MAX),
        status: request.status.to_string(),
        created_at: request.created_at,
        updated_at: request.updated_at,
        expires_at: request.expires_at,
    }
}
