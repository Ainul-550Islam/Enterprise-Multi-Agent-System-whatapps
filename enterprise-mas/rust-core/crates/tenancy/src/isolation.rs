//! Isolation guards: every domain object crossing a trust boundary is
//! re-checked against the resolved context. Failures return NotFound (so
//! cross-tenant existence stays hidden) and are reported for audits/metrics.

use crate::context::ResolvedTenantContext;
use mas_common::error::AppError;
use mas_common::ids::{ProjectId, TenantId};
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// A tenant-scoped object (implemented by domain aggregates).
pub trait TenantScoped {
    /// Tenant the object belongs to.
    fn scoped_tenant(&self) -> Option<TenantId>;
    /// Project (when the object is project-scoped).
    fn scoped_project(&self) -> Option<ProjectId> {
        None
    }
    /// Type label for error reports.
    fn scoped_type(&self) -> &'static str {
        "resource"
    }
}

#[async_trait::async_trait]
pub trait IsolationPolicyPort: Send + Sync + std::fmt::Debug {
    /// Called when an isolation assertion fails (audit event, alerting).
    async fn on_violation(&self, report: &IsolationReport) -> Result<()>;
}

/// What failed (handy for audit records; contains no payload data).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsolationReport {
    pub resource_type: &'static str,
    pub expected_tenant: TenantId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found_tenant: Option<TenantId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_project: Option<ProjectId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub found_project: Option<ProjectId>,
}

/// The isolation service: counters + pluggable violation sink.
#[derive(Debug)]
pub struct IsolationService {
    violations: AtomicU64,
    checks: AtomicU64,
}

impl Default for IsolationService {
    fn default() -> Self {
        Self::new()
    }
}

impl IsolationService {
    pub fn new() -> Self {
        Self {
            violations: AtomicU64::new(0),
            checks: AtomicU64::new(0),
        }
    }

    #[must_use]
    pub fn violation_count(&self) -> u64 {
        self.violations.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn check_count(&self) -> u64 {
        self.checks.load(Ordering::Relaxed)
    }

    /// Asserts `object` belongs to `context`; returns it on success.
    /// Tenant mismatch → NotFound; project mismatch → NotFound.
    pub fn ensure_in_context<T: TenantScoped>(
        &self,
        context: &ResolvedTenantContext,
        object: T,
    ) -> Result<T> {
        self.checks.fetch_add(1, Ordering::Relaxed);
        let found_tenant = object.scoped_tenant();
        let found_project = object.scoped_project();
        let tenant_ok = found_tenant.is_none_or(|tenant| tenant == context.tenant_id);
        let project_ok = match context.project_id {
            None => true,
            Some(expected) => found_project.is_none_or(|found| found == expected),
        };
        if tenant_ok && project_ok {
            return Ok(object);
        }
        self.violations.fetch_add(1, Ordering::Relaxed);
        let report = IsolationReport {
            resource_type: object.scoped_type(),
            expected_tenant: context.tenant_id,
            found_tenant,
            expected_project: context.project_id,
            found_project: found_project.filter(|_| project_ok),
        };
        tracing::warn!(?report, "cross-tenant isolation violation");
        Err(AppError::not_found(
            report.resource_type,
            format!("resource is not part of the current tenant context (trace {report:?})"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_domain::MembershipRole;

    #[derive(Debug)]
    struct FakeResource {
        tenant: Option<TenantId>,
        project: Option<ProjectId>,
    }
    impl TenantScoped for FakeResource {
        fn scoped_tenant(&self) -> Option<TenantId> {
            self.tenant
        }
        fn scoped_project(&self) -> Option<ProjectId> {
            self.project
        }
        fn scoped_type(&self) -> &'static str {
            "fake_resource"
        }
    }

    fn context(tenant: TenantId, project: Option<ProjectId>) -> ResolvedTenantContext {
        ResolvedTenantContext {
            tenant_id: tenant,
            organization_id: mas_common::ids::OrganizationId::new(),
            project_id: project,
            tenant_status: mas_common::enums::TenantStatus::Active,
            roles: std::collections::BTreeSet::from([MembershipRole::Operator]),
            is_system: false,
        }
    }

    #[test]
    fn matching_objects_pass_and_violations_count() {
        let service = IsolationService::new();
        let tenant = TenantId::new();
        let project = ProjectId::new();
        let context = context(tenant, Some(project));

        let good = FakeResource {
            tenant: Some(tenant),
            project: Some(project),
        };
        service.ensure_in_context(&context, good).expect("pass");

        let wrong_tenant = FakeResource {
            tenant: Some(TenantId::new()),
            project: Some(project),
        };
        let err = service
            .ensure_in_context(&context, wrong_tenant)
            .expect_err("blocked");
        assert_eq!(err.error_code(), "RESOURCE_NOT_FOUND");
        assert_eq!(service.violation_count(), 1);
        assert_eq!(service.check_count(), 2);

        let wrong_project = FakeResource {
            tenant: Some(tenant),
            project: Some(ProjectId::new()),
        };
        assert!(service.ensure_in_context(&context, wrong_project).is_err());
        assert_eq!(service.violation_count(), 2);
    }
}
