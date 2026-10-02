//! Row-Level Security session context.
//!
//! The SQL policies (in the migration set) read three GUCs:
//!
//! * `app.current_tenant` — tenant scope for owned tables;
//! * `app.current_org` — organization scope for org-level catalog tables;
//! * `app.current_user` — self-service scope for user/session tables.
//!
//! [`RlsContext`] is the only code that sets them, always transaction-local
//! (`set_config(..., true)`), always as bound parameters (no string-built
//! SQL), and it **fails closed**: with no scope at all, tenant-owned tables
//! expose zero rows. Service roles that legitimately cross tenants (outbox
//! dispatcher, audit writer) run under dedicated BYPASSRLS roles matching the
//! migration comments — not through this type.

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId, UserId};
use mas_common::result::Result;
use sqlx::PgConnection;

use crate::error::map_sqlx;

/// The tenant GUC name (must match the policies in the migration set).
pub const TENANT_GUC: &str = "app.current_tenant";
/// The organization GUC name.
pub const ORG_GUC: &str = "app.current_org";
/// The user GUC name.
pub const USER_GUC: &str = "app.current_user";

/// The scope an RLS-protected unit of work runs under.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RlsContext {
    /// Tenant scope (most traffic).
    pub tenant: Option<TenantId>,
    /// Organization scope (org catalog operations).
    pub organization: Option<OrganizationId>,
    /// Self-service user scope.
    pub user: Option<UserId>,
}

/// One GUC-setting statement plus its single bound parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsStatement {
    /// The parameter `SELECT set_config('<guc>', $1, true)` text.
    pub sql: &'static str,
    /// The GUC being set.
    pub guc: &'static str,
    /// The bound uuid value.
    pub value: String,
}

impl RlsContext {
    /// Tenant-scoped work (the common case).
    #[must_use]
    pub fn for_tenant(tenant: TenantId) -> Self {
        Self {
            tenant: Some(tenant),
            organization: None,
            user: None,
        }
    }

    /// Organization-catalog work (adding/removing tenants, member lists).
    #[must_use]
    pub fn for_organization(organization: OrganizationId) -> Self {
        Self {
            organization: Some(organization),
            ..Self::default()
        }
    }

    /// Org catalog work that also lands inside one tenant.
    #[must_use]
    pub fn for_organization_with_tenant(organization: OrganizationId, tenant: TenantId) -> Self {
        Self {
            tenant: Some(tenant),
            organization: Some(organization),
            user: None,
        }
    }

    /// Self-service user work (profile, sessions).
    #[must_use]
    pub fn for_user(user: UserId) -> Self {
        Self {
            user: Some(user),
            ..Self::default()
        }
    }

    /// True when no scope at all is set — such a context must never reach
    /// the database for tenant-owned data (policies also fail closed).
    #[must_use]
    pub fn is_unscoped(&self) -> bool {
        self.tenant.is_none() && self.organization.is_none() && self.user.is_none()
    }

    /// The transaction-local GUC statements, in stable order.
    ///
    /// Every statement is `SELECT set_config('<guc>', $1, true)` — the
    /// `true` pins it to the current transaction, and the value is a bound
    /// parameter, so injection through ids is impossible by construction.
    #[must_use]
    pub fn statements(&self) -> Vec<RlsStatement> {
        let mut out = Vec::with_capacity(3);
        if let Some(tenant) = &self.tenant {
            out.push(RlsStatement {
                sql: "SELECT set_config('app.current_tenant', $1, true)",
                guc: TENANT_GUC,
                value: tenant.to_database_string(),
            });
        }
        if let Some(org) = &self.organization {
            out.push(RlsStatement {
                sql: "SELECT set_config('app.current_org', $1, true)",
                guc: ORG_GUC,
                value: org.to_database_string(),
            });
        }
        if let Some(user) = &self.user {
            out.push(RlsStatement {
                sql: "SELECT set_config('app.current_user', $1, true)",
                guc: USER_GUC,
                value: user.to_database_string(),
            });
        }
        out
    }

    /// Applies the context to an open transaction. Refuses unscoped contexts:
    /// forgetting to scope must fail, not silently query zero rows later.
    pub async fn apply(&self, executor: &mut PgConnection) -> Result<()> {
        if self.is_unscoped() {
            return Err(AppError::forbidden(
                "refusing to run against the database without an RLS scope",
            ));
        }
        for statement in self.statements() {
            sqlx::query(statement.sql)
                .bind(statement.value)
                .execute(&mut *executor)
                .await
                .map_err(map_sqlx)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unscoped_contexts_fail_closed() {
        let ctx = RlsContext::default();
        assert!(ctx.is_unscoped());
        assert!(ctx.statements().is_empty());
    }

    #[test]
    fn tenant_context_emits_transaction_local_statement() {
        let tenant = TenantId::new();
        let ctx = RlsContext::for_tenant(tenant);
        assert!(!ctx.is_unscoped());
        let statements = ctx.statements();
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].guc, TENANT_GUC);
        assert_eq!(
            statements[0].sql, "SELECT set_config('app.current_tenant', $1, true)",
            "values must be bound parameters, never string-interpolated"
        );
        assert!(!statements[0].sql.contains(&tenant.to_database_string()));
        assert_eq!(statements[0].value, tenant.to_database_string());
    }

    #[test]
    fn combined_org_tenant_context_emits_both() {
        let org = OrganizationId::new();
        let tenant = TenantId::new();
        let ctx = RlsContext::for_organization_with_tenant(org, tenant);
        let gucs: Vec<&str> = ctx.statements().iter().map(|s| s.guc).collect();
        assert_eq!(gucs, [TENANT_GUC, ORG_GUC]);

        let org_only = RlsContext::for_organization(org);
        assert_eq!(org_only.statements().len(), 1);
        assert_eq!(org_only.statements()[0].guc, ORG_GUC);

        let user = RlsContext::for_user(UserId::new());
        assert_eq!(user.statements().len(), 1);
        assert_eq!(user.statements()[0].guc, USER_GUC);
    }
}
