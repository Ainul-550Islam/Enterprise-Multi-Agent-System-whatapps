//! [`ServiceContext`]: the acting principal, scope and correlation data
//! threaded through every use-case call.

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use mas_domain::{AuditActor, AuditActorKind};

/// The request-scoped context every application service receives.
///
/// * `actor` — the audited principal (user/api-key/service/system).
/// * `tenant_id`/`organization_id` — the *proven* scope. Services use
///   [`ServiceContext::require_tenant`]/[`ServiceContext::require_organization`]
///   for tenant-scoped use-cases; untenanted contexts fail explicitly.
/// * `correlation_id` — end-to-end correlation echoed into audit events
///   and downstream execution records.
/// * `requested_at` — when the request entered the system (clock injection
///   keeps use-cases deterministic in tests).
#[derive(Debug, Clone)]
pub struct ServiceContext {
    pub actor: AuditActor,
    pub tenant_id: Option<TenantId>,
    pub organization_id: Option<OrganizationId>,
    pub correlation_id: String,
    pub requested_at: Timestamp,
}

impl ServiceContext {
    /// Validates the free-form parts.
    pub fn new(
        actor: AuditActor,
        tenant_id: Option<TenantId>,
        organization_id: Option<OrganizationId>,
        correlation_id: impl Into<String>,
        requested_at: &Timestamp,
    ) -> Result<Self> {
        let correlation_id = correlation_id.into();
        validation::validate_non_empty("correlation_id", &correlation_id)?;
        validation::validate_length("correlation_id", &correlation_id, 1, 256)?;
        Ok(Self {
            actor,
            tenant_id,
            organization_id,
            correlation_id,
            requested_at: *requested_at,
        })
    }

    /// The platform system principal (daemons, migrations, bootstraps).
    pub fn system(correlation_id: impl Into<String>, requested_at: &Timestamp) -> Result<Self> {
        Self::new(
            AuditActor::new(AuditActorKind::System, "system").expect("system actor"),
            None,
            None,
            correlation_id,
            requested_at,
        )
    }

    /// A service-to-service principal (e.g. `scheduler-service`).
    pub fn for_service(
        service_name: &str,
        correlation_id: impl Into<String>,
        requested_at: &Timestamp,
    ) -> Result<Self> {
        mas_common::validation::validate_length("service_name", service_name, 1, 64)?;
        Self::new(
            AuditActor::new(AuditActorKind::Service, service_name)?,
            None,
            None,
            correlation_id,
            requested_at,
        )
    }

    /// Convenience: clone with a concrete tenant/organization scope.
    #[must_use]
    pub fn with_scope(self, tenant: TenantId, organization: OrganizationId) -> Self {
        Self {
            tenant_id: Some(tenant),
            organization_id: Some(organization),
            ..self
        }
    }

    /// The proven tenant scope, or an explicit error for tenant-scoped
    /// use-cases.
    pub fn require_tenant(&self) -> Result<TenantId> {
        self.tenant_id
            .ok_or_else(|| AppError::forbidden("this use-case requires a tenant-scoped principal"))
    }

    /// The proven organization scope, or an explicit error.
    pub fn require_organization(&self) -> Result<OrganizationId> {
        self.organization_id.ok_or_else(|| {
            AppError::forbidden("this use-case requires an organization-scoped principal")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    #[test]
    fn contexts_validate_and_scope_explicitly() {
        let system = ServiceContext::system("boot-1", &now()).expect("system ctx");
        assert!(system.require_tenant().is_err(), "system is untenanted");

        let tenant = TenantId::new();
        let org = OrganizationId::new();
        let scoped = system.clone().with_scope(tenant, org);
        assert_eq!(scoped.require_tenant().expect("tenant"), tenant);
        assert_eq!(scoped.require_organization().expect("org"), org);

        let service =
            ServiceContext::for_service("scheduler-service", "tick-7", &now()).expect("service");
        assert!(service.correlation_id == "tick-7");

        assert!(
            ServiceContext::system("x".repeat(300), &now()).is_err(),
            "oversized correlation ids reject"
        );
        assert!(ServiceContext::for_service("", "c", &now()).is_err());
    }
}
