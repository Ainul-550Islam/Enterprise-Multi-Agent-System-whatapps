//! Durable, RLS-scoped approval aggregate repository.
//!
//! The policy crate owns every state transition. This adapter persists the
//! complete aggregate in `approval_requests.request_payload`, keeps the queue
//! projection indexed for operational queries, and appends each newly cast
//! vote to the immutable evidence table. It never reconstructs a request from
//! lossy legacy columns: malformed or pre-parity rows fail closed instead.

use mas_common::error::AppError;
use mas_common::ids::{ApprovalRequestId, TenantId};
use mas_common::result::Result;
use mas_policy::approval::{ApprovalRequest, ApprovalStatus, ApprovalStorePort, ApprovalVote};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// PostgreSQL store for policy approval requests.
#[derive(Debug, Clone)]
pub struct PostgresApprovalStore {
    pool: PgPool,
}

impl PostgresApprovalStore {
    /// Binds the approval store to the application's request-scoped pool.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Pool accessor for composition diagnostics; application queries always
    /// execute through the request-scoped transaction, never this pool.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn locked_current(
        &self,
        tenant: TenantId,
        id: ApprovalRequestId,
    ) -> Result<Option<ApprovalRequest>> {
        ensure_tenant_scope(tenant)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT request_payload
             FROM approval_requests
             WHERE tenant_id = $1 AND id = $2
             FOR UPDATE",
        )
        .bind(tenant.into_uuid())
        .bind(id.into_uuid())
        .fetch_optional(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| request_from_payload(row, tenant, id))
            .transpose()
    }

    async fn append_new_votes(
        &self,
        current: &ApprovalRequest,
        next: &ApprovalRequest,
    ) -> Result<()> {
        let current_votes = all_votes(current);
        let next_votes = all_votes(next);
        if next_votes.len() < current_votes.len() || !next_votes.starts_with(&current_votes) {
            return Err(AppError::conflict(
                "approval vote history is append-only and cannot be rewritten",
            ));
        }
        if next_votes.len().saturating_sub(current_votes.len()) > 1 {
            return Err(AppError::conflict(
                "an approval transition may record at most one vote",
            ));
        }
        let Some(vote) = next_votes.get(current_votes.len()) else {
            return Ok(());
        };
        let decision = if next.approvals.iter().any(|candidate| candidate == *vote) {
            "approve"
        } else {
            "deny"
        };
        let mut scoped = crate::scoped_request::connection().await?;
        sqlx::query(
            "INSERT INTO approval_votes (
                tenant_id, request_id, approver_id, approver_actor,
                approver_role, decision, reason, created_at, vote_at
             ) VALUES ($1, $2, NULL, $3, $4, $5, $6, $7, $8)",
        )
        .bind(next.tenant_id.into_uuid())
        .bind(next.id.into_uuid())
        .bind(&vote.approver)
        .bind(&vote.approver_role)
        .bind(decision)
        .bind(&vote.reason)
        .bind(vote.at.into_datetime())
        .bind(vote.at.into_datetime())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ApprovalStorePort for PostgresApprovalStore {
    async fn insert(&self, request: &ApprovalRequest) -> Result<()> {
        ensure_tenant_scope(request.tenant_id)?;
        let organization = request.organization_id.ok_or_else(|| {
            AppError::invalid_field(
                "organization_id",
                "required",
                "durable approval requests require a resolved organization",
            )
        })?;
        let payload = serde_json::to_value(request).map_err(|error| {
            AppError::serialization(format!("serialize approval request: {error}"))
        })?;
        let mut scoped = crate::scoped_request::connection().await?;
        sqlx::query(
            "INSERT INTO approval_requests (
                id, tenant_id, organization_id, policy_id, origin, resource_type,
                resource_id, requested_by, status, quorum, expires_at,
                decision_reason, context, request_payload, created_at, updated_at
             ) VALUES (
                $1, $2, $3, $4, 'policy', 'approval', $1::text, NULL, $5,
                $6, $7, $8, '{}'::jsonb, $9, $10, $11
             )",
        )
        .bind(request.id.into_uuid())
        .bind(request.tenant_id.into_uuid())
        .bind(organization.into_uuid())
        .bind(
            request
                .origin
                .as_ref()
                .map(|origin| origin.policy_id.into_uuid()),
        )
        .bind(database_status(request.status))
        .bind(i32::from(request.quorum))
        .bind(
            request
                .expires_at
                .map(|timestamp| timestamp.into_datetime()),
        )
        .bind(&request.reason)
        .bind(payload)
        .bind(request.created_at.into_datetime())
        .bind(request.updated_at.into_datetime())
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn update(&self, request: &ApprovalRequest) -> Result<()> {
        ensure_tenant_scope(request.tenant_id)?;
        let current = self
            .locked_current(request.tenant_id, request.id)
            .await?
            .ok_or_else(|| AppError::not_found("approval request", request.id.to_string()))?;
        validate_update(&current, request)?;
        self.append_new_votes(&current, request).await?;
        let payload = serde_json::to_value(request).map_err(|error| {
            AppError::serialization(format!("serialize approval request: {error}"))
        })?;
        let mut scoped = crate::scoped_request::connection().await?;
        let result = sqlx::query(
            "UPDATE approval_requests
             SET status = $3,
                 expires_at = $4,
                 decision_reason = $5,
                 request_payload = $6
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(request.tenant_id.into_uuid())
        .bind(request.id.into_uuid())
        .bind(database_status(request.status))
        .bind(
            request
                .expires_at
                .map(|timestamp| timestamp.into_datetime()),
        )
        .bind(&request.reason)
        .bind(payload)
        .execute(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        if result.rows_affected() != 1 {
            return Err(AppError::not_found(
                "approval request",
                request.id.to_string(),
            ));
        }
        Ok(())
    }

    async fn get(&self, id: ApprovalRequestId) -> Result<Option<ApprovalRequest>> {
        let tenant = crate::scoped_request::current_scope()?.tenant_id();
        self.locked_current(tenant, id).await
    }

    async fn list_pending(&self, tenant_id: TenantId) -> Result<Vec<ApprovalRequest>> {
        ensure_tenant_scope(tenant_id)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, request_payload
             FROM approval_requests
             WHERE tenant_id = $1 AND status = 'pending'
             ORDER BY updated_at ASC, created_at ASC",
        )
        .bind(tenant_id.into_uuid())
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let id: Uuid = row.try_get("id").map_err(map_sqlx)?;
                request_from_payload(row, tenant_id, ApprovalRequestId::from_uuid(id))
            })
            .collect()
    }

    async fn list_for_tenant(&self, tenant_id: TenantId) -> Result<Vec<ApprovalRequest>> {
        ensure_tenant_scope(tenant_id)?;
        let mut scoped = crate::scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, request_payload
             FROM approval_requests
             WHERE tenant_id = $1
             ORDER BY updated_at DESC, created_at DESC",
        )
        .bind(tenant_id.into_uuid())
        .fetch_all(scoped.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.into_iter()
            .map(|row| {
                let id: Uuid = row.try_get("id").map_err(map_sqlx)?;
                request_from_payload(row, tenant_id, ApprovalRequestId::from_uuid(id))
            })
            .collect()
    }
}

fn ensure_tenant_scope(tenant: TenantId) -> Result<()> {
    if crate::scoped_request::current_scope()?.tenant_id() != tenant {
        return Err(AppError::not_found("tenant", tenant.to_string()));
    }
    Ok(())
}

fn database_status(status: ApprovalStatus) -> &'static str {
    match status {
        ApprovalStatus::Pending => "pending",
        ApprovalStatus::Approved => "approved",
        // 0027 named the storage state `denied`; policy's public/domain spelling
        // is `rejected`. The payload remains canonical and therefore preserves
        // the public contract without an unsafe destructive migration.
        ApprovalStatus::Rejected => "denied",
        ApprovalStatus::Expired => "expired",
        ApprovalStatus::Cancelled => "cancelled",
    }
}

fn request_from_payload(
    row: sqlx::postgres::PgRow,
    tenant: TenantId,
    id: ApprovalRequestId,
) -> Result<ApprovalRequest> {
    let payload: serde_json::Value = row.try_get("request_payload").map_err(map_sqlx)?;
    let request: ApprovalRequest = serde_json::from_value(payload).map_err(|error| {
        AppError::database(format!(
            "approval request {id} has malformed canonical payload: {error}"
        ))
    })?;
    if request.id != id || request.tenant_id != tenant {
        return Err(AppError::database(
            "approval request payload identity does not match its scoped row",
        ));
    }
    Ok(request)
}

fn all_votes(request: &ApprovalRequest) -> Vec<&ApprovalVote> {
    request
        .approvals
        .iter()
        .chain(request.rejections.iter())
        .collect()
}

fn validate_update(current: &ApprovalRequest, next: &ApprovalRequest) -> Result<()> {
    if current.id != next.id
        || current.tenant_id != next.tenant_id
        || current.organization_id != next.organization_id
        || current.project_id != next.project_id
        || current.subject_actor != next.subject_actor
        || current.action != next.action
        || current.resource != next.resource
        || current.origin != next.origin
        || current.reason != next.reason
        || current.quorum != next.quorum
        || current.approver_roles != next.approver_roles
        || current.created_at != next.created_at
        || current.expires_at != next.expires_at
    {
        return Err(AppError::conflict(
            "approval identity and governance requirements are immutable after creation",
        ));
    }
    let current_votes = all_votes(current);
    let next_votes = all_votes(next);
    if next_votes.len() < current_votes.len() || !next_votes.starts_with(&current_votes) {
        return Err(AppError::conflict(
            "approval vote history is append-only and cannot be rewritten",
        ));
    }
    match (current.status, next.status) {
        (ApprovalStatus::Pending, ApprovalStatus::Pending)
        | (ApprovalStatus::Pending, ApprovalStatus::Approved)
        | (ApprovalStatus::Pending, ApprovalStatus::Rejected)
        | (ApprovalStatus::Pending, ApprovalStatus::Expired)
        | (ApprovalStatus::Pending, ApprovalStatus::Cancelled)
        | (ApprovalStatus::Approved, ApprovalStatus::Approved)
        | (ApprovalStatus::Rejected, ApprovalStatus::Rejected)
        | (ApprovalStatus::Expired, ApprovalStatus::Expired)
        | (ApprovalStatus::Cancelled, ApprovalStatus::Cancelled) => Ok(()),
        _ => Err(AppError::conflict(
            "terminal approval request state cannot be changed",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::OrganizationId;
    use mas_common::timestamps::Timestamp;
    use std::collections::BTreeSet;

    fn request() -> ApprovalRequest {
        ApprovalRequest {
            id: ApprovalRequestId::new(),
            tenant_id: TenantId::new(),
            organization_id: Some(OrganizationId::new()),
            project_id: None,
            subject_actor: "requester".to_owned(),
            action: "tool.invoke".to_owned(),
            resource: "tool:billing".to_owned(),
            origin: None,
            reason: Some("high risk".to_owned()),
            quorum: 1,
            approver_roles: BTreeSet::new(),
            approvals: Vec::new(),
            rejections: Vec::new(),
            status: ApprovalStatus::Pending,
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
            expires_at: None,
        }
    }

    #[test]
    fn terminal_transition_and_vote_rewrite_are_refused() {
        let mut current = request();
        current.status = ApprovalStatus::Approved;
        let mut next = current.clone();
        next.status = ApprovalStatus::Pending;
        assert!(validate_update(&current, &next).is_err());

        let mut current = request();
        current.approvals.push(ApprovalVote {
            approver: "operator".to_owned(),
            approver_role: Some("operator".to_owned()),
            reason: Some("safe".to_owned()),
            at: Timestamp::now(),
        });
        let mut next = current.clone();
        next.approvals[0].approver = "rewritten".to_owned();
        assert!(validate_update(&current, &next).is_err());
    }

    #[test]
    fn rejected_maps_to_legacy_database_spelling() {
        assert_eq!(database_status(ApprovalStatus::Rejected), "denied");
    }
}
