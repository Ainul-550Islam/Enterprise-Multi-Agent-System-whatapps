//! PURPOSE:
//! Resolve Rust-owned, tenant-scoped agent versions on the canonical worker
//! execution path before forwarding through the existing signed AgentBridge.
//!
//! This module does not create a parallel execution path and does not grant
//! the durable worker broad agent-table access. The catalog read is performed
//! through `PgServices` inside `with_authoritative_scope`, which arms the
//! existing FORCE-RLS transaction for the task's authoritative tenant,
//! organization, and project.

use std::fmt;
use std::sync::Arc;

use mas_application::agent_runtime_catalog::AgentRuntimeCatalogService;
use mas_common::error::AppError;
use mas_common::ids::AgentVersionId;
use mas_common::result::Result;
use mas_execution::agent_bridge::{
    AgentBridge, AgentRunRequest, AgentRunVerdict, AgentRuntimeSnapshot,
    AgentRuntimeSnapshotResolver,
};
use mas_persistence::repositories::services::PgServices;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::AuthoritativeScope;
use sqlx::PgPool;

/// PostgreSQL-backed, RLS-scoped implementation of the runtime snapshot port.
#[derive(Debug, Clone)]
pub struct PostgresAgentRuntimeSnapshotResolver {
    pool: PgPool,
    catalog: Arc<AgentRuntimeCatalogService>,
}

impl PostgresAgentRuntimeSnapshotResolver {
    /// Composes the catalog over the worker's existing application stores.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        let stores = PgServices::new(pool.clone());
        let catalog = AgentRuntimeCatalogService::new(Box::new(stores.clone()), Box::new(stores));
        Self {
            pool,
            catalog: Arc::new(catalog),
        }
    }
}

#[async_trait::async_trait]
impl AgentRuntimeSnapshotResolver for PostgresAgentRuntimeSnapshotResolver {
    async fn resolve(&self, request: &AgentRunRequest) -> Result<AgentRuntimeSnapshot> {
        request.require_authoritative_scope()?;
        let tenant_id = request.tenant_id.ok_or_else(|| {
            AppError::validation("agent runtime resolution requires a tenant scope")
        })?;
        let organization_id = request.organization_id.ok_or_else(|| {
            AppError::validation("agent runtime resolution requires an organization scope")
        })?;
        let project_id = request.project_id.ok_or_else(|| {
            AppError::validation("agent runtime resolution requires a project scope")
        })?;
        let requested_version = request
            .agent_version
            .as_deref()
            .map(str::parse::<AgentVersionId>)
            .transpose()
            .map_err(|error| {
                AppError::invalid_field(
                    "agent_version",
                    "invalid_ref",
                    format!("agent_version must be a published version UUID: {error}"),
                )
            })?;

        // This actor labels an internal worker lookup only; authorization comes
        // from the canonical task context and the armed RLS transaction, not
        // from an untrusted request claim or this diagnostic actor string.
        let scope = AuthoritativeScope::resolved(
            tenant_id,
            organization_id,
            "worker-agent-runtime-catalog",
        )
        .with_project(project_id);
        let catalog = self.catalog.clone();
        with_authoritative_scope(&self.pool, scope, || async move {
            catalog
                .resolve(
                    tenant_id,
                    organization_id,
                    project_id,
                    request.agent_id,
                    requested_version,
                )
                .await
        })
        .await
    }
}

/// Bridge decorator that guarantees every production request carries a fresh
/// Rust-resolved snapshot before it reaches the existing signed transport.
#[derive(Clone)]
pub struct ResolvedAgentBridge {
    resolver: Arc<dyn AgentRuntimeSnapshotResolver>,
    inner: Arc<dyn AgentBridge>,
}

impl ResolvedAgentBridge {
    /// Wraps the already-configured production bridge without changing its
    /// identity, authentication, deadlines, or request-signing implementation.
    #[must_use]
    pub fn new(
        resolver: Arc<dyn AgentRuntimeSnapshotResolver>,
        inner: Arc<dyn AgentBridge>,
    ) -> Self {
        Self { resolver, inner }
    }
}

impl fmt::Debug for ResolvedAgentBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedAgentBridge")
            .field("inner", &self.inner.name())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl AgentBridge for ResolvedAgentBridge {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn run(&self, mut request: AgentRunRequest) -> Result<AgentRunVerdict> {
        request.require_authoritative_scope()?;
        let snapshot = self.resolver.resolve(&request).await?;
        ensure_snapshot_matches_request(&request, &snapshot)?;

        // Pin the selected database version into the actual wire request. This
        // makes the exact selection explicit in Python context/audit metadata,
        // as well as in the signed snapshot body.
        request.agent_version = Some(snapshot.agent_version_id.to_string());
        request.agent_snapshot = Some(snapshot);
        self.inner.run(request).await
    }

    async fn health(&self) -> Result<()> {
        self.inner.health().await
    }
}

fn ensure_snapshot_matches_request(
    request: &AgentRunRequest,
    snapshot: &AgentRuntimeSnapshot,
) -> Result<()> {
    let expected_tenant = request.tenant_id.ok_or_else(|| {
        AppError::validation("agent run request is missing the authoritative tenant scope")
    })?;
    let expected_organization = request.organization_id.ok_or_else(|| {
        AppError::validation("agent run request is missing the authoritative organization scope")
    })?;
    let expected_project = request.project_id.ok_or_else(|| {
        AppError::validation("agent run request is missing the authoritative project scope")
    })?;

    if snapshot.agent_id != request.agent_id
        || snapshot.tenant_id != expected_tenant
        || snapshot.organization_id != expected_organization
        || snapshot.project_id != expected_project
    {
        return Err(AppError::internal(
            "agent runtime resolver returned a snapshot outside the request scope",
        ));
    }
    if let Some(requested_version) = request.agent_version.as_deref() {
        let requested_version = requested_version
            .parse::<AgentVersionId>()
            .map_err(|error| {
                AppError::invalid_field(
                    "agent_version",
                    "invalid_ref",
                    format!("agent_version must be a published version UUID: {error}"),
                )
            })?;
        if snapshot.agent_version_id != requested_version {
            return Err(AppError::not_found(
                "agent version",
                requested_version.to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{
        AgentId, AgentVersionId, ExecutionId, OrganizationId, ProjectId, TenantId,
    };
    use mas_domain::ResourceLimits;
    use mas_execution::agent_bridge::{AgentRuntimeConfiguration, AgentRuntimeLimits, AgentUsage};
    use mas_orchestration::cancellation::CancellationToken;
    use mas_orchestration::timeout::Deadline;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct FixedResolver {
        snapshot: AgentRuntimeSnapshot,
    }

    #[async_trait::async_trait]
    impl AgentRuntimeSnapshotResolver for FixedResolver {
        async fn resolve(&self, _request: &AgentRunRequest) -> Result<AgentRuntimeSnapshot> {
            Ok(self.snapshot.clone())
        }
    }

    #[derive(Debug, Default)]
    struct RecordingBridge {
        seen: Mutex<Option<AgentRunRequest>>,
    }

    #[async_trait::async_trait]
    impl AgentBridge for RecordingBridge {
        fn name(&self) -> &'static str {
            "recording-test-bridge"
        }

        async fn run(&self, request: AgentRunRequest) -> Result<AgentRunVerdict> {
            *self.seen.lock().expect("recording mutex") = Some(request);
            Ok(AgentRunVerdict::Succeeded {
                output: json!({"ok": true}),
                usage: AgentUsage::default(),
                model_key: None,
                finish_reason: None,
            })
        }
    }

    fn snapshot(
        agent_id: AgentId,
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
        version_id: AgentVersionId,
    ) -> AgentRuntimeSnapshot {
        AgentRuntimeSnapshot {
            agent_id,
            tenant_id,
            organization_id,
            project_id,
            agent_version_id: version_id,
            version_number: 7,
            configuration_checksum: "a".repeat(64),
            name: "Snapshot Agent".to_owned(),
            slug: "snapshot-agent".to_owned(),
            kind: "standard".to_owned(),
            status: "active".to_owned(),
            origin: "tenant".to_owned(),
            read_only: false,
            configuration: AgentRuntimeConfiguration {
                description: Some("test runtime".to_owned()),
                model_hint: Some("provider/model-v7".to_owned()),
                system_prompt: "Immutable version seven".to_owned(),
                limits: AgentRuntimeLimits::default(),
                tags: vec!["test".to_owned()],
                allowed_tools: vec!["search".to_owned()],
            },
        }
    }

    fn request(
        agent_id: AgentId,
        tenant_id: TenantId,
        organization_id: OrganizationId,
        project_id: ProjectId,
    ) -> AgentRunRequest {
        AgentRunRequest {
            agent_id,
            agent_version: None,
            agent_snapshot: None,
            input: json!({"question": "test"}),
            correlation_id: "test-correlation".to_owned(),
            deadline: Deadline::after(std::time::Duration::from_secs(30)).expires_at(),
            limits: ResourceLimits::default(),
            token_budget_max: 0,
            tenant_id: Some(tenant_id),
            organization_id: Some(organization_id),
            project_id: Some(project_id),
            execution_id: Some(ExecutionId::new()),
            task_id: None,
            task_attempt_id: None,
            request_id: None,
            idempotency_key: None,
            cancellation: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn decorator_pins_and_forwards_the_exact_snapshot() {
        let agent_id = AgentId::new();
        let tenant_id = TenantId::new();
        let organization_id = OrganizationId::new();
        let project_id = ProjectId::new();
        let version_id = AgentVersionId::new();
        let snapshot = snapshot(agent_id, tenant_id, organization_id, project_id, version_id);
        let resolver = Arc::new(FixedResolver {
            snapshot: snapshot.clone(),
        });
        let inner = Arc::new(RecordingBridge::default());
        let bridge = ResolvedAgentBridge::new(resolver, inner.clone());

        let result = bridge
            .run(request(agent_id, tenant_id, organization_id, project_id))
            .await
            .expect("forward signed-bridge request");
        assert!(matches!(result, AgentRunVerdict::Succeeded { .. }));

        let seen = inner.seen.lock().expect("recording mutex");
        let seen = seen.as_ref().expect("inner bridge received request");
        assert_eq!(
            seen.agent_version.as_deref(),
            Some(version_id.to_string().as_str())
        );
        assert_eq!(seen.agent_snapshot.as_ref(), Some(&snapshot));
    }

    #[tokio::test]
    async fn decorator_rejects_a_resolver_result_from_another_tenant() {
        let agent_id = AgentId::new();
        let tenant_id = TenantId::new();
        let organization_id = OrganizationId::new();
        let project_id = ProjectId::new();
        let snapshot = snapshot(
            agent_id,
            TenantId::new(),
            organization_id,
            project_id,
            AgentVersionId::new(),
        );
        let resolver = Arc::new(FixedResolver { snapshot });
        let inner = Arc::new(RecordingBridge::default());
        let bridge = ResolvedAgentBridge::new(resolver, inner.clone());

        assert!(bridge
            .run(request(agent_id, tenant_id, organization_id, project_id))
            .await
            .is_err());
        assert!(inner.seen.lock().expect("recording mutex").is_none());
    }

    #[tokio::test]
    async fn explicit_version_must_match_the_resolved_snapshot() {
        let agent_id = AgentId::new();
        let tenant_id = TenantId::new();
        let organization_id = OrganizationId::new();
        let project_id = ProjectId::new();
        let snapshot = snapshot(
            agent_id,
            tenant_id,
            organization_id,
            project_id,
            AgentVersionId::new(),
        );
        let resolver = Arc::new(FixedResolver { snapshot });
        let inner = Arc::new(RecordingBridge::default());
        let bridge = ResolvedAgentBridge::new(resolver, inner.clone());
        let mut request = request(agent_id, tenant_id, organization_id, project_id);
        request.agent_version = Some(AgentVersionId::new().to_string());

        assert!(bridge.run(request).await.is_err());
        assert!(inner.seen.lock().expect("recording mutex").is_none());
    }
}
