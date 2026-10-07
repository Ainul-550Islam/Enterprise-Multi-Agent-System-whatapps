//! PURPOSE: Process state shared by every handler — the application services,
//! the token verifier, the health registry and build identity.
//!
//! Owning layer: `mas-api` (the process edge).
//!
//! Production responsibility: build the object graph exactly once at bootstrap
//! and share it `Arc`-wise, so there is one instance of each service per
//! process and no per-request construction cost or per-request divergence.
//!
//! It must NOT:
//!   • hold per-request or per-tenant mutable state (that belongs on the
//!     request extensions / service context);
//!   • expose a constructor that silently substitutes an in-memory store when
//!     the production store is unavailable.

//! Process state shared by every handler: the five application services
//! (Arc-shared, since they own boxed store ports), the token verifier, the
//! health registry, and build identity. Constructed ONCE at bootstrap.

use std::sync::Arc;

use mas_application::agent_service::AgentService;
use mas_application::execution_service::ExecutionService;
use mas_application::rollback_service::RollbackApplicationService;
use mas_application::schedule_service::ScheduleService;
use mas_application::tenancy_service::TenancyService;
use mas_application::workflow_service::WorkflowService;
use mas_observability::health::HealthRegistry;
use sqlx::PgPool;

use crate::auth::TokenVerifierPort;
use crate::scope_resolution::RequestScopeResolver;

/// Everything a request may touch.
#[derive(Clone)]
pub struct AppState {
    /// Deployment/service identity for health + logs.
    pub service_name: Arc<str>,
    /// Crate/binary version for health payloads.
    pub service_version: Arc<str>,
    /// Bearer-token verification boundary.
    pub verifier: Arc<dyn TokenVerifierPort>,
    /// Request-scope resolution from identity.
    ///
    /// `None` only in the in-memory development lane, which has no directory
    /// to consult. A production process always has one, and
    /// `middleware::authenticated_layer` refuses to resolve scope without it —
    /// it does not fall back to the caller's claims.
    pub scope_resolver: Option<RequestScopeResolver>,
    /// Tenancy/identity use-cases.
    pub tenancy: Arc<TenancyService>,
    /// Agent lifecycle use-cases.
    pub agents: Arc<AgentService>,
    /// Workflow lifecycle use-cases.
    pub workflows: Arc<WorkflowService>,
    /// Agent/workflow rollback dispatch and tenant-scoped history.
    pub rollbacks: Option<Arc<RollbackApplicationService>>,
    /// Execution lifecycle use-cases.
    pub executions: Arc<ExecutionService>,
    /// Schedule lifecycle use-cases.
    pub schedules: Arc<ScheduleService>,
    /// The canonical control plane (entitlement → policy → quota).
    pub control_plane: Arc<mas_application::control_plane::ControlPlane>,
    /// Where control-plane mutations are recorded.
    ///
    /// Shared with the tenancy/agent/workflow services rather than created
    /// here, so one request's audit trail is one sequence regardless of which
    /// service wrote which line. A control-plane change that is not audited is
    /// indistinguishable from tampering, so this is not optional.
    pub audit: Arc<dyn mas_application::audit::AuditSinkPort>,
    /// The Postgres control-plane stores.
    ///
    /// `None` in the in-memory development lane, which has no database. The
    /// billing and usage endpoints that read these return a clear error in
    /// that lane rather than a plausible empty result: an empty usage list
    /// from a lane that cannot meter is indistinguishable from a tenant that
    /// consumed nothing.
    pub control_plane_stores:
        Option<Arc<mas_persistence::repositories::control_plane::ControlPlaneStores>>,
    /// Liveness/readiness probes (observability plane).
    pub health: Arc<HealthRegistry>,
    /// The runtime PostgreSQL pool, when the process is running on
    /// PostgreSQL.
    ///
    /// `None` only in the in-memory lane. When it is `Some`, the middleware
    /// opens one transaction per request with row-level security armed, and
    /// every repository call in that request runs inside it.
    pub runtime_pool: Option<PgPool>,
    /// Shared secret used to authenticate internal service calls.
    ///
    /// `None` means every internal route refuses, rather than trusting the
    /// network. A production process must have one.
    pub internal_secret: Option<mas_security::SecretString>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("service_name", &self.service_name)
            .field("service_version", &self.service_version)
            .finish_non_exhaustive()
    }
}

impl AppState {
    /// Installs the rollback use case after the shared agent/workflow services
    /// have been composed over their common history store.
    #[must_use]
    pub fn with_rollbacks(mut self, rollbacks: Arc<RollbackApplicationService>) -> Self {
        self.rollbacks = Some(rollbacks);
        self
    }

    /// The canonical control plane.
    #[must_use]
    pub fn control_plane(&self) -> &Arc<mas_application::control_plane::ControlPlane> {
        &self.control_plane
    }

    /// The Postgres control-plane stores, when this lane has a database.
    ///
    /// Returns `Err(Unavailable)` rather than `None` so a handler turns it into
    /// a deterministic error instead of an empty result that reads as "no
    /// data".
    pub fn control_plane_stores(
        &self,
    ) -> mas_common::result::Result<
        Arc<mas_persistence::repositories::control_plane::ControlPlaneStores>,
    > {
        self.control_plane_stores.clone().ok_or_else(|| {
            mas_common::error::AppError::internal(
                "control-plane data requires the database lane; this deployment has none",
            )
        })
    }

    /// Composition root (services arrive fully wired).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        service_name: &str,
        service_version: &str,
        verifier: Arc<dyn TokenVerifierPort>,
        scope_resolver: Option<RequestScopeResolver>,
        tenancy: Arc<TenancyService>,
        agents: Arc<AgentService>,
        workflows: Arc<WorkflowService>,
        executions: Arc<ExecutionService>,
        schedules: Arc<ScheduleService>,
        control_plane: Arc<mas_application::control_plane::ControlPlane>,
        audit: Arc<dyn mas_application::audit::AuditSinkPort>,
        control_plane_stores: Option<
            Arc<mas_persistence::repositories::control_plane::ControlPlaneStores>,
        >,
        health: Arc<HealthRegistry>,
        runtime_pool: Option<PgPool>,
        internal_secret: Option<mas_security::SecretString>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: service_version.into(),
            verifier,
            scope_resolver,
            tenancy,
            agents,
            workflows,
            rollbacks: None,
            executions,
            schedules,
            control_plane,
            audit,
            control_plane_stores,
            health,
            runtime_pool,
            internal_secret,
        }
    }
}

/// Shared in-memory wiring for this crate's tests (and any downstream
/// handler tests): all services over ONE `InMemoryServices` +
/// `InMemoryAuditSink`, plus a `StaticTokenVerifier` with `test-token`.
#[cfg(any(test, feature = "testutils"))]
pub mod test_support {
    use std::sync::Arc;

    use mas_application::agent_service::AgentService;
    use mas_application::audit::InMemoryAuditSink;
    use mas_application::durable_memory::InMemoryDurableExecution;
    use mas_application::execution_service::ExecutionService;
    use mas_application::schedule_service::ScheduleService;
    use mas_application::stores::InMemoryServices;
    use mas_application::tenancy_service::TenancyService;
    use mas_application::workflow_service::WorkflowService;
    use mas_observability::health::HealthRegistry;

    use super::{AppState, RollbackApplicationService};
    use crate::auth::StaticTokenVerifier;
    use mas_security::{PrincipalKind, ScopeSet, SecurityPrincipal};

    /// Fresh, fully-wired state over one shared in-memory store.
    pub fn inmemory_state() -> AppState {
        inmemory_state_with_store().0
    }

    /// Fresh in-memory state and the shared backing store, for tests that need
    /// to seed read models written by an external runtime.
    pub fn inmemory_state_with_store() -> (AppState, Arc<InMemoryServices>) {
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let control_plane =
            Arc::new(mas_application::control_plane::build_in_memory_control_plane());
        let tenancy = Arc::new(TenancyService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
            Box::new(control_plane.entitlement_store()),
        ));
        let agents = Arc::new(
            AgentService::new(
                Box::new(store.clone()),
                Box::new(store.clone()),
                Box::new(store.clone()),
                Box::new(audit.clone()),
            )
            .with_rollback_store(Box::new(store.clone())),
        );
        let workflows = Arc::new(
            WorkflowService::new(
                Box::new(store.clone()),
                Box::new(store.clone()),
                Box::new(store.clone()),
                Box::new(audit.clone()),
            )
            .with_rollback_store(Box::new(store.clone())),
        );
        let rollbacks = Arc::new(RollbackApplicationService::new(
            Arc::clone(&agents),
            Arc::clone(&workflows),
            Box::new(store.clone()),
        ));
        // The durable port is mandatory. The in-memory implementation is
        // accepted here only because this is the crate's test harness — the
        // production composition root wires `PgDurableExecution`.
        let durable = Arc::new(InMemoryDurableExecution::new());
        let executions = Arc::new(
            ExecutionService::new(
                Box::new(store.clone()),
                Box::new(store.clone()),
                Box::new(store.clone()),
                Box::new(store.clone()),
                Box::new(audit.clone()),
                durable,
                Arc::clone(&control_plane),
            )
            .with_step_store(Box::new(store.clone())),
        );
        let schedules = Arc::new(ScheduleService::new(
            Box::new(store.clone()),
            Box::new(audit.clone()),
        ));
        let verifier = Arc::new(StaticTokenVerifier::new().with_token(
            "test-token",
            SecurityPrincipal::new(
                uuid::Uuid::now_v7().to_string(),
                PrincipalKind::User,
                None,
                ScopeSet::denied(),
            ),
        ));
        // The test lane resolves scope from identity too, over the same
        // in-memory store the tenancy service writes to. Resolving from the
        // header here would make this crate's tests unable to detect the
        // defect this phase exists to fix.
        let scope_resolver = Some(crate::scope_resolution::RequestScopeResolver::new(
            mas_tenancy::TenantResolver::new(
                Arc::new(crate::inmemory_directory::InMemoryTenantDirectory::new(
                    store.clone(),
                )),
                Arc::new(crate::inmemory_directory::InMemoryMembershipDirectory::new(
                    store.clone(),
                )),
            ),
        ));
        let state = AppState::new(
            "mas-api-test",
            env!("CARGO_PKG_VERSION"),
            verifier,
            scope_resolver,
            tenancy,
            agents,
            workflows,
            executions,
            schedules,
            Arc::clone(&control_plane),
            audit.clone() as Arc<dyn mas_application::audit::AuditSinkPort>,
            // No database in this lane, so no control-plane stores: the
            // billing and usage endpoints answer with a deterministic error
            // rather than an empty result that reads as "nothing consumed".
            None,
            Arc::new(HealthRegistry::new()),
            None,
            // Internal routes in the in-memory test lane are exercised with a
            // fixed secret that is not a production credential.
            Some(mas_security::SecretString::new(
                "inmemory-internal-secret".to_owned(),
            )),
        )
        .with_rollbacks(rollbacks);
        (state, store)
    }
}
