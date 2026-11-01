//! One tenant-scoped entry point for governed agent/workflow rollbacks.
//!
//! The resource services own version validation and atomic commits. This
//! facade dispatches the HTTP use cases and exposes a safe, bounded history
//! projection without returning idempotency keys or mutable domain rows.

use std::sync::Arc;

use mas_common::error::AppError;
use mas_common::ids::{AgentId, WorkflowId};
use mas_common::result::Result;
use mas_common::validation;
use mas_contracts::rollback::{
    RollbackHistoryItemDto, RollbackHistoryQuery, RollbackRequest, RollbackResponseDto,
};

use crate::agent_service::AgentService;
use crate::context::ServiceContext;
use crate::stores::RollbackStorePort;
use crate::workflow_service::WorkflowService;

/// Orchestrates rollback entry points and history over one shared store.
#[derive(Debug)]
pub struct RollbackApplicationService {
    agents: Arc<AgentService>,
    workflows: Arc<WorkflowService>,
    records: Box<dyn RollbackStorePort>,
}

impl RollbackApplicationService {
    /// Composes the resource use cases and their common rollback history port.
    pub fn new(
        agents: Arc<AgentService>,
        workflows: Arc<WorkflowService>,
        records: Box<dyn RollbackStorePort>,
    ) -> Self {
        Self {
            agents,
            workflows,
            records,
        }
    }

    /// Rolls an agent back by publishing a new immutable version.
    pub async fn rollback_agent(
        &self,
        ctx: &ServiceContext,
        id: AgentId,
        request: &RollbackRequest,
    ) -> Result<RollbackResponseDto> {
        self.agents.rollback_version(ctx, id, request).await
    }

    /// Rolls a workflow back by publishing a new immutable graph snapshot.
    pub async fn rollback_workflow(
        &self,
        ctx: &ServiceContext,
        id: WorkflowId,
        request: &RollbackRequest,
    ) -> Result<RollbackResponseDto> {
        self.workflows.rollback_version(ctx, id, request).await
    }

    /// Lists the caller's tenant history newest-first. Foreign tenant history
    /// is never queried or returned.
    pub async fn list(
        &self,
        ctx: &ServiceContext,
        query: &RollbackHistoryQuery,
    ) -> Result<Vec<RollbackHistoryItemDto>> {
        query.validate()?;
        let tenant = ctx.require_tenant()?;
        let records = self.records.list(tenant, query.limit.unwrap_or(50)).await?;
        records
            .into_iter()
            .map(|record| {
                validation::validate_length("reason", &record.reason, 1, 1024).map_err(|_| {
                    AppError::database("rollback history contains an invalid reason")
                })?;
                Ok(RollbackHistoryItemDto {
                    id: record.id,
                    resource_kind: record.resource_kind.to_string(),
                    resource_id: record.resource_id.to_string(),
                    from_version: record.from_version,
                    to_version: record.to_version,
                    actor_kind: record.actor.kind.to_string(),
                    actor_id: record.actor.id,
                    reason: record.reason,
                    created_at: record.created_at,
                })
            })
            .collect()
    }
}
