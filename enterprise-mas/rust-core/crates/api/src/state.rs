//! Process state shared by every handler: the five application services
//! (Arc-shared, since they own boxed store ports), the token verifier, the
//! health registry, and build identity. Constructed ONCE at bootstrap.

use std::sync::Arc;

use mas_application::agent_service::AgentService;
use mas_application::execution_service::ExecutionService;
use mas_application::schedule_service::ScheduleService;
use mas_application::tenancy_service::TenancyService;
use mas_application::workflow_service::WorkflowService;
use mas_observability::health::HealthRegistry;

use crate::auth::TokenVerifierPort;

/// Everything a request may touch.
#[derive(Clone)]
pub struct AppState {
    /// Deployment/service identity for health + logs.
    pub service_name: Arc<str>,
    /// Crate/binary version for health payloads.
    pub service_version: Arc<str>,
    /// Bearer-token verification boundary.
    pub verifier: Arc<dyn TokenVerifierPort>,
    /// Tenancy/identity use-cases.
    pub tenancy: Arc<TenancyService>,
    /// Agent lifecycle use-cases.
    pub agents: Arc<AgentService>,
    /// Workflow lifecycle use-cases.
    pub workflows: Arc<WorkflowService>,
    /// Execution lifecycle use-cases.
    pub executions: Arc<ExecutionService>,
    /// Schedule lifecycle use-cases.
    pub schedules: Arc<ScheduleService>,
    /// Liveness/readiness probes (observability plane).
    pub health: Arc<HealthRegistry>,
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
    /// Composition root (services arrive fully wired).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        service_name: &str,
        service_version: &str,
        verifier: Arc<dyn TokenVerifierPort>,
        tenancy: Arc<TenancyService>,
        agents: Arc<AgentService>,
        workflows: Arc<WorkflowService>,
        executions: Arc<ExecutionService>,
        schedules: Arc<ScheduleService>,
        health: Arc<HealthRegistry>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: service_version.into(),
            verifier,
            tenancy,
            agents,
            workflows,
            executions,
            schedules,
            health,
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
    use mas_application::execution_service::ExecutionService;
    use mas_application::schedule_service::ScheduleService;
    use mas_application::stores::InMemoryServices;
    use mas_application::tenancy_service::TenancyService;
    use mas_application::workflow_service::WorkflowService;
    use mas_observability::health::HealthRegistry;

    use super::AppState;
    use crate::auth::{PrincipalKind, StaticTokenVerifier};

    /// Fresh, fully-wired state over one shared in-memory store.
    pub fn inmemory_state() -> AppState {
        let store = Arc::new(InMemoryServices::new());
        let audit = Arc::new(InMemoryAuditSink::new());
        let tenancy = Arc::new(TenancyService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        ));
        let agents = Arc::new(AgentService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        ));
        let workflows = Arc::new(WorkflowService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        ));
        let executions = Arc::new(ExecutionService::new(
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(store.clone()),
            Box::new(audit.clone()),
        ));
        let schedules = Arc::new(ScheduleService::new(Box::new(store), Box::new(audit)));
        let verifier = Arc::new(StaticTokenVerifier::new().with_token(
            "test-token",
            &uuid::Uuid::now_v7().to_string(),
            PrincipalKind::User,
        ));
        AppState::new(
            "mas-api-test",
            env!("CARGO_PKG_VERSION"),
            verifier,
            tenancy,
            agents,
            workflows,
            executions,
            schedules,
            Arc::new(HealthRegistry::new()),
        )
    }
}
