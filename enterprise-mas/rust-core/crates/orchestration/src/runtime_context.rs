//! The immutable-per-request execution context threaded through the whole
//! pipeline (API → engine → runtime → node → tool/agent invocation).
//!
//! Identity fields (`tenant_id`, `organization_id`, `actor_id`) are set once
//! at the boundary and never mutated; runtime fields are added by explicit
//! child-context constructors.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::time::Duration;

use crate::cancellation::CancellationToken;
use crate::timeout::Deadline;

/// Execution-scoped context carried end to end.
#[derive(Debug, Clone)]
pub struct RuntimeContext {
    // -- identity (immutable once constructed) --
    tenant_id: TenantId,
    organization_id: OrganizationId,
    project_id: ProjectId,
    actor_id: String,
    environment: mas_common::enums::Environment,

    // -- correlation --
    correlation_id: String,
    causation_id: Option<String>,
    request_id: Option<uuid::Uuid>,

    // -- execution scope --
    execution_id: Option<ExecutionId>,
    parent_execution_id: Option<ExecutionId>,

    // -- authorization --
    permissions: BTreeSet<String>,

    // -- runtime --
    deadline: Option<Deadline>,
    cancellation: CancellationToken,
    /// Free-form execution data (workflow variables, rendered inputs).
    /// Non-secret by policy; redaction is enforced before logging.
    data: serde_json::Map<String, serde_json::Value>,
}

/// Serializable snapshot used for checkpoints and cross-process handoff.
/// The live `CancellationToken` is not serializable and is re-created fresh on
/// restore (its semantics are re-established by the recovering engine).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeContextSnapshot {
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub actor_id: String,
    pub environment: mas_common::enums::Environment,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub request_id: Option<uuid::Uuid>,
    pub execution_id: Option<ExecutionId>,
    pub parent_execution_id: Option<ExecutionId>,
    pub permissions: BTreeSet<String>,
    pub deadline_epoch_ms: Option<i64>,
    pub data: serde_json::Map<String, serde_json::Value>,
}

impl RuntimeContext {
    /// Starts a new root context at the request boundary.
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        actor_id: impl Into<String>,
        environment: mas_common::enums::Environment,
    ) -> Result<Self> {
        let actor_id = actor_id.into();
        if tenant_id.is_nil() || organization_id.is_nil() || project_id.is_nil() {
            return Err(AppError::invalid_field(
                "runtime_context",
                "required",
                "tenant, organization and project must be concrete",
            ));
        }
        mas_common::validation::validate_non_empty("actor_id", &actor_id)?;
        Ok(Self {
            tenant_id,
            organization_id,
            project_id,
            actor_id,
            environment,
            correlation_id: uuid::Uuid::now_v7().to_string(),
            causation_id: None,
            request_id: None,
            execution_id: None,
            parent_execution_id: None,
            permissions: BTreeSet::new(),
            deadline: None,
            cancellation: CancellationToken::new(),
            data: serde_json::Map::new(),
        })
    }

    /// Derives a context bound to an execution (keeps identity/correlation,
    /// links the new execution and a child cancellation token).
    #[must_use]
    pub fn for_execution(&self, execution_id: ExecutionId, parent: Option<ExecutionId>) -> Self {
        let deadline = self.deadline;
        Self {
            tenant_id: self.tenant_id,
            organization_id: self.organization_id,
            project_id: self.project_id,
            actor_id: self.actor_id.clone(),
            environment: self.environment,
            correlation_id: self.correlation_id.clone(),
            causation_id: Some(self.correlation_id.clone()),
            request_id: self.request_id,
            execution_id: Some(execution_id),
            parent_execution_id: parent,
            permissions: self.permissions.clone(),
            deadline,
            cancellation: self.cancellation.child(),
            data: self.data.clone(),
        }
    }

    /// Derives a tied-together context for a workflow node / step: identical
    /// identity and correlation, but with its own child cancellation token so
    /// cancelling the step subtree never cancels the parent execution.
    #[must_use]
    pub fn child_scope(&self) -> Self {
        let mut scoped = self.clone();
        scoped.cancellation = self.cancellation.child();
        scoped
    }

    // -- accessors (identity has no setters) ---------------------------------

    #[must_use]
    pub const fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
    #[must_use]
    pub const fn organization_id(&self) -> OrganizationId {
        self.organization_id
    }
    #[must_use]
    pub const fn project_id(&self) -> ProjectId {
        self.project_id
    }
    #[must_use]
    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }
    #[must_use]
    pub const fn environment(&self) -> mas_common::enums::Environment {
        self.environment
    }
    #[must_use]
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
    #[must_use]
    pub fn causation_id(&self) -> Option<&str> {
        self.causation_id.as_deref()
    }
    #[must_use]
    pub const fn request_id(&self) -> Option<uuid::Uuid> {
        self.request_id
    }
    #[must_use]
    pub const fn execution_id(&self) -> Option<ExecutionId> {
        self.execution_id
    }
    #[must_use]
    pub const fn parent_execution_id(&self) -> Option<ExecutionId> {
        self.parent_execution_id
    }
    #[must_use]
    pub const fn permissions(&self) -> &BTreeSet<String> {
        &self.permissions
    }
    #[must_use]
    pub const fn deadline(&self) -> Option<Deadline> {
        self.deadline
    }
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
    #[must_use]
    pub const fn data(&self) -> &serde_json::Map<String, serde_json::Value> {
        &self.data
    }

    // -- deliberate context builders ------------------------------------------

    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = correlation_id.into();
        self
    }

    #[must_use]
    pub fn with_request_id(mut self, request_id: uuid::Uuid) -> Self {
        self.request_id = Some(request_id);
        self
    }

    #[must_use]
    pub fn with_permissions(mut self, permissions: BTreeSet<String>) -> Self {
        self.permissions = permissions;
        self
    }

    #[must_use]
    pub fn with_deadline(mut self, deadline: Deadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.deadline = Some(Deadline::after(timeout));
        self
    }

    /// Sets/merges a data value in the execution context.
    pub fn set_data(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.data.insert(key.into(), value);
    }

    // -- guards ----------------------------------------------------------------

    /// Fails with `Timeout` when the deadline passed, or `Cancelled` when
    /// cancellation was requested (cancellation wins — it is explicit).
    pub fn guard_active(&self) -> Result<()> {
        self.cancellation.throw_if_cancelled()?;
        if let Some(deadline) = self.deadline {
            deadline.check_deadline("runtime context")?;
        }
        Ok(())
    }

    /// Remaining time until the deadline (`None` = unbounded).
    #[must_use]
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline.and_then(|deadline| deadline.remaining())
    }

    /// Freezes the context into a serializable snapshot for checkpoints.
    #[must_use]
    pub fn snapshot(&self) -> RuntimeContextSnapshot {
        RuntimeContextSnapshot {
            tenant_id: self.tenant_id,
            organization_id: self.organization_id,
            project_id: self.project_id,
            actor_id: self.actor_id.clone(),
            environment: self.environment,
            correlation_id: self.correlation_id.clone(),
            causation_id: self.causation_id.clone(),
            request_id: self.request_id,
            execution_id: self.execution_id,
            parent_execution_id: self.parent_execution_id,
            permissions: self.permissions.clone(),
            deadline_epoch_ms: self.deadline.map(|d| d.expires_at().to_unix_ms()),
            data: self.data.clone(),
        }
    }

    /// Restores a context from a snapshot with a fresh cancellation token.
    pub fn from_snapshot(snapshot: RuntimeContextSnapshot) -> Result<Self> {
        let deadline = snapshot
            .deadline_epoch_ms
            .map(|ms| Timestamp::from_unix_ms(ms).map(Deadline::at))
            .transpose()?;
        Ok(Self {
            tenant_id: snapshot.tenant_id,
            organization_id: snapshot.organization_id,
            project_id: snapshot.project_id,
            actor_id: snapshot.actor_id,
            environment: snapshot.environment,
            correlation_id: snapshot.correlation_id,
            causation_id: snapshot.causation_id,
            request_id: snapshot.request_id,
            execution_id: snapshot.execution_id,
            parent_execution_id: snapshot.parent_execution_id,
            permissions: snapshot.permissions,
            deadline,
            cancellation: CancellationToken::new(),
            data: snapshot.data,
        })
    }
}
