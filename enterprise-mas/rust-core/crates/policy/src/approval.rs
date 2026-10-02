//! Approval requests: raised by `require_approval` rules, decided by quorum.
//!
//! Invariants:
//! * pending → approved/rejected/expired/cancelled only; terminal is final,
//! * one vote per approver (approvals and rejections share identity),
//! * quorum `≥ 1` approvals required; a rejection needs no quorum — any
//!   single rejection vetoes (human-in-the-loop is a safety gate),
//! * only actors holding a whitelisted approver role may vote (empty
//!   whitelist = any authenticated operator),
//! * requests can expire; expiry is applied explicitly (by the service or
//!   a sweeper) so "silent decisions" never happen.

use mas_common::error::AppError;
use mas_common::ids::{ApprovalRequestId, OrganizationId, PolicyId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};

string_enum! {
    /// Approval lifecycle.
    ApprovalStatus {
        Pending => "pending",
        Approved => "approved",
        Rejected => "rejected",
        Expired => "expired",
        Cancelled => "cancelled",
    }
}

impl ApprovalStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

/// One approver's vote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalVote {
    pub approver: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approver_role: Option<String>,
    pub at: Timestamp,
}

/// Which rule raised this request (audit provenance).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalOrigin {
    pub policy_id: PolicyId,
    pub rule_id: String,
}

/// An approval request raised by a require-approval policy rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: ApprovalRequestId,
    pub tenant_id: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    /// Actor whose action is gated.
    pub subject_actor: String,
    pub action: String,
    pub resource: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<ApprovalOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Approvals needed; `>= 1`.
    pub quorum: u16,
    /// Roles allowed to vote (empty = any operator).
    #[serde(default)]
    pub approver_roles: BTreeSet<String>,
    pub approvals: Vec<ApprovalVote>,
    pub rejections: Vec<ApprovalVote>,
    pub status: ApprovalStatus,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
}

impl ApprovalRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn raise(
        tenant_id: TenantId,
        organization_id: Option<OrganizationId>,
        project_id: Option<ProjectId>,
        subject_actor: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
    ) -> Result<Self> {
        let subject_actor = subject_actor.into();
        let action = action.into();
        let resource = resource.into();
        validation::validate_non_empty("subject_actor", &subject_actor)?;
        validation::validate_non_empty("action", &action)?;
        validation::validate_non_empty("resource", &resource)?;
        Ok(Self {
            id: ApprovalRequestId::new(),
            tenant_id,
            organization_id,
            project_id,
            subject_actor,
            action,
            resource,
            origin: None,
            reason: None,
            quorum: 1,
            approver_roles: BTreeSet::new(),
            approvals: Vec::new(),
            rejections: Vec::new(),
            status: ApprovalStatus::Pending,
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
            expires_at: None,
        })
    }

    #[must_use]
    pub fn with_origin(mut self, policy_id: PolicyId, rule_id: impl Into<String>) -> Self {
        self.origin = Some(ApprovalOrigin {
            policy_id,
            rule_id: rule_id.into(),
        });
        self
    }

    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    pub fn with_quorum(mut self, quorum: u16) -> Result<Self> {
        if quorum == 0 {
            return Err(AppError::invalid_field(
                "quorum",
                "out_of_range",
                "quorum must be at least 1",
            ));
        }
        self.quorum = quorum;
        Ok(self)
    }

    pub fn with_approver_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.approver_roles = roles.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_expiry(mut self, expires_at: Timestamp) -> Result<Self> {
        if !expires_at.is_future() {
            return Err(AppError::invalid_field(
                "expires_at",
                "in_past",
                "approval expiry must be in the future",
            ));
        }
        self.expires_at = Some(expires_at);
        Ok(self)
    }

    #[must_use]
    pub const fn status(&self) -> ApprovalStatus {
        self.status
    }

    /// Votes remaining for quorum (0 once reachable-or-reached).
    #[must_use]
    pub fn remaining_for_quorum(&self) -> u16 {
        self.quorum
            .saturating_sub(u16::try_from(self.approvals.len()).unwrap_or(u16::MAX))
    }

    fn ensure_votable(&self, approver: &str, role: Option<&str>) -> Result<()> {
        if self.status != ApprovalStatus::Pending {
            return Err(AppError::conflict(format!(
                "approval request is {} and cannot be voted on",
                self.status
            )));
        }
        if let Some(expiry) = &self.expires_at {
            if expiry.is_past() {
                return Err(AppError::conflict(
                    "approval request has expired (transition not yet applied)",
                ));
            }
        }
        if !self.approver_roles.is_empty() {
            let role = role.ok_or_else(|| {
                AppError::forbidden("this approval requires an approver role to vote")
            })?;
            if !self.approver_roles.contains(role) {
                return Err(AppError::forbidden(format!(
                    "role '{role}' is not whitelisted to approve this request"
                )));
            }
        }
        if approver == self.subject_actor {
            return Err(AppError::forbidden(
                "the requesting actor cannot approve their own request",
            ));
        }
        let voted = self
            .approvals
            .iter()
            .chain(self.rejections.iter())
            .any(|vote| vote.approver == approver);
        if voted {
            return Err(AppError::conflict(format!(
                "'{approver}' has already voted on this request"
            )));
        }
        Ok(())
    }

    /// Casts an approval vote; reaching quorum approves immediately.
    pub fn approve(
        &mut self,
        approver: impl Into<String>,
        role: Option<String>,
    ) -> Result<ApprovalStatus> {
        let approver = approver.into();
        validation::validate_non_empty("approver", &approver)?;
        self.ensure_votable(&approver, role.as_deref())?;
        self.approvals.push(ApprovalVote {
            approver,
            approver_role: role,
            at: Timestamp::now(),
        });
        self.updated_at = Timestamp::now();
        if self.remaining_for_quorum() == 0 {
            self.status = ApprovalStatus::Approved;
        }
        Ok(self.status)
    }

    /// Casts a rejection vote; any single rejection vetoes the request.
    pub fn reject(
        &mut self,
        approver: impl Into<String>,
        role: Option<String>,
    ) -> Result<ApprovalStatus> {
        let approver = approver.into();
        validation::validate_non_empty("approver", &approver)?;
        self.ensure_votable(&approver, role.as_deref())?;
        self.rejections.push(ApprovalVote {
            approver,
            approver_role: role,
            at: Timestamp::now(),
        });
        self.status = ApprovalStatus::Rejected;
        self.updated_at = Timestamp::now();
        Ok(self.status)
    }

    /// Caller-initiated withdrawal.
    pub fn cancel(&mut self) -> Result<()> {
        if self.status != ApprovalStatus::Pending {
            return Err(AppError::conflict(format!(
                "only pending requests can be cancelled (current: {})",
                self.status
            )));
        }
        self.status = ApprovalStatus::Cancelled;
        self.updated_at = Timestamp::now();
        Ok(())
    }

    /// Applies expiry if due; returns whether a transition happened.
    pub fn expire_if_due(&mut self) -> bool {
        if self.status == ApprovalStatus::Pending
            && self.expires_at.as_ref().is_some_and(|ts| ts.is_past())
        {
            self.status = ApprovalStatus::Expired;
            self.updated_at = Timestamp::now();
            return true;
        }
        false
    }
}

/// Persistence port for approval requests.
#[async_trait::async_trait]
pub trait ApprovalStorePort: Send + Sync + fmt::Debug {
    async fn insert(&self, request: &ApprovalRequest) -> Result<()>;
    async fn update(&self, request: &ApprovalRequest) -> Result<()>;
    async fn get(&self, id: ApprovalRequestId) -> Result<Option<ApprovalRequest>>;
    /// Pending requests for a tenant (dashboards / sweepers).
    async fn list_pending(&self, tenant_id: TenantId) -> Result<Vec<ApprovalRequest>>;
}

/// In-memory approval store with tenant isolation on reads.
#[derive(Debug, Default)]
pub struct InMemoryApprovalStore {
    requests: Mutex<BTreeMap<ApprovalRequestId, ApprovalRequest>>,
}

impl InMemoryApprovalStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ApprovalStorePort for InMemoryApprovalStore {
    async fn insert(&self, request: &ApprovalRequest) -> Result<()> {
        let mut store = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        if store.contains_key(&request.id) {
            return Err(AppError::conflict(format!(
                "approval request {} already exists",
                request.id
            )));
        }
        store.insert(request.id, request.clone());
        Ok(())
    }

    async fn update(&self, request: &ApprovalRequest) -> Result<()> {
        let mut store = self.requests.lock().unwrap_or_else(|e| e.into_inner());
        match store.contains_key(&request.id) {
            true => {
                store.insert(request.id, request.clone());
                Ok(())
            },
            false => Err(AppError::not_found(
                "approval request",
                request.id.to_string(),
            )),
        }
    }

    async fn get(&self, id: ApprovalRequestId) -> Result<Option<ApprovalRequest>> {
        Ok(self
            .requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }

    async fn list_pending(&self, tenant_id: TenantId) -> Result<Vec<ApprovalRequest>> {
        Ok(self
            .requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|request| {
                request.tenant_id == tenant_id && request.status == ApprovalStatus::Pending
            })
            .cloned()
            .collect())
    }
}

/// Service façade: load–mutate–persist with expiry application.
#[derive(Debug)]
pub struct ApprovalService {
    store: Arc<dyn ApprovalStorePort>,
}

impl ApprovalService {
    pub fn new(store: Arc<dyn ApprovalStorePort>) -> Self {
        Self { store }
    }

    pub async fn raise(&self, request: ApprovalRequest) -> Result<ApprovalRequest> {
        self.store.insert(&request).await?;
        Ok(request)
    }

    /// Approver approves; persisted before returning.
    pub async fn approve(
        &self,
        id: ApprovalRequestId,
        approver: impl Into<String>,
        role: Option<String>,
    ) -> Result<ApprovalStatus> {
        self.vote(id, approver, role, true).await
    }

    pub async fn reject(
        &self,
        id: ApprovalRequestId,
        approver: impl Into<String>,
        role: Option<String>,
    ) -> Result<ApprovalStatus> {
        self.vote(id, approver, role, false).await
    }

    async fn vote(
        &self,
        id: ApprovalRequestId,
        approver: impl Into<String>,
        role: Option<String>,
        approve: bool,
    ) -> Result<ApprovalStatus> {
        let mut request = self
            .store
            .get(id)
            .await?
            .ok_or_else(|| AppError::not_found("approval request", id.to_string()))?;
        request.expire_if_due(); // expiry transition before voting
        let status = if approve {
            request.approve(approver.into(), role)?
        } else {
            request.reject(approver.into(), role)?
        };
        self.store.update(&request).await?;
        Ok(status)
    }

    /// Sweep pending requests of a tenant; returns newly expired ones.
    pub async fn sweep_expired(&self, tenant_id: TenantId) -> Result<Vec<ApprovalRequest>> {
        let mut expired = Vec::new();
        for mut request in self.store.list_pending(tenant_id).await? {
            if request.expire_if_due() {
                self.store.update(&request).await?;
                expired.push(request);
            }
        }
        Ok(expired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::OrganizationId;
    use std::time::Duration;

    fn base_request(tenant: TenantId) -> ApprovalRequest {
        ApprovalRequest::raise(
            tenant,
            Some(OrganizationId::new()),
            None,
            "alice",
            "tool.invoke",
            "tool:hammer",
        )
        .expect("request")
    }

    #[tokio::test]
    async fn quorum_roles_and_self_approval_are_enforced() {
        let tenant = TenantId::new();
        let service = ApprovalService::new(Arc::new(InMemoryApprovalStore::new()));
        let request = base_request(tenant)
            .with_quorum(2)
            .expect("quorum")
            .with_approver_roles(["approver"]);
        service.raise(request.clone()).await.expect("raise");

        // Self-approval blocked.
        assert!(service
            .approve(request.id, "alice", Some("approver".into()))
            .await
            .is_err());
        // Wrong role blocked.
        assert!(service.approve(request.id, "bob", None).await.is_err());
        // First approver: still pending (quorum 2).
        let status = service
            .approve(request.id, "bob", Some("approver".into()))
            .await
            .expect("vote 1");
        assert_eq!(status, ApprovalStatus::Pending);
        // Double vote blocked.
        assert!(service
            .approve(request.id, "bob", Some("approver".into()))
            .await
            .is_err());
        // Second approver reaches quorum.
        let status = service
            .approve(request.id, "carol", Some("approver".into()))
            .await
            .expect("vote 2");
        assert_eq!(status, ApprovalStatus::Approved);
        // Terminal: further votes rejected.
        assert!(service
            .reject(request.id, "dave", Some("approver".into()))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn single_rejection_vetoes_and_expiry_is_swept() {
        let tenant = TenantId::new();
        let service = ApprovalService::new(Arc::new(InMemoryApprovalStore::new()));
        let request = base_request(tenant).with_quorum(2).expect("quorum");
        service.raise(request.clone()).await.expect("raise");
        service
            .approve(request.id, "bob", None)
            .await
            .expect("partial quorum");
        let status = service
            .reject(request.id, "carol", None)
            .await
            .expect("veto");
        assert_eq!(status, ApprovalStatus::Rejected);

        // Expiry: a near-instant expiring request (create in the future then
        // warp by mutation — expiry must be applied by the sweep).
        let stale = base_request(tenant);
        let mut stale = stale;
        stale.expires_at = Some(
            Timestamp::now()
                .checked_add(Duration::from_millis(1))
                .expect("future"),
        );
        service.raise(stale.clone()).await.expect("raise stale");
        tokio::time::sleep(Duration::from_millis(10)).await;
        let expired = service.sweep_expired(tenant).await.expect("sweep");
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].status, ApprovalStatus::Expired);
        assert!(service.approve(stale.id, "bob", None).await.is_err());
    }
}
