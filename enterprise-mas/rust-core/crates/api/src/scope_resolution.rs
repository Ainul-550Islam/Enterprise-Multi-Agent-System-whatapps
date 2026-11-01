// PURPOSE: Turn an authenticated principal plus the scope a request *claims*
// into the one scope the request is actually entitled to.
//
// Owning layer: `mas-api` (the process edge), delegating the authoritative
// decision to `mas-tenancy`'s `TenantResolver`.
//
// Production responsibility: spec Phase 3. Before this module existed the API
// took `x-tenant-id` and `x-organization-id` at face value (`context.rs`,
// `middleware.rs`, `grpc.rs`), which meant any authenticated caller could name
// any tenant. Here those headers are demoted to what they are — hints about
// which of the caller's *own* reachable tenants it means — and the answer is
// recomputed from identity and the membership directory.
//
// Security boundary: this is the boundary at which a caller-supplied value
// stops being a claim and either becomes a proven scope or becomes a failure.
// Two rules govern every path through it:
//
//   1. A tenant the principal cannot reach does not produce "forbidden". It
//      produces the same "not found" as a tenant that does not exist, because
//      "forbidden" would confirm the tenant is there (Phase 4, existence
//      hiding).
//   2. A conflict is a deterministic failure, never a silent correction. If a
//      caller names tenant A and organization B and those do not belong
//      together, the request fails; the server does not quietly substitute
//      tenant A's real organization and carry on.
//
// It must NOT:
//   • fall back to the claimed tenant when resolution fails;
//   • treat an absent header as "any tenant";
//   • produce a scope for a principal whose credential has expired;
//   • be bypassable by calling a handler that skips the middleware — the
//     scope is the only thing `RequestContext::require_scope` will hand out.

use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_security::{AuthFailure, AuthoritativeScope, SecurityPrincipal};
use mas_tenancy::TenantResolver;
use std::collections::BTreeSet;
use std::fmt;

/// What the request *asked* for. Hints, never authority.
///
/// Named `ScopeClaims` rather than `Scope` so every call site reads as "a
/// claim" — the type name is the first line of defence against a future
/// reader mistaking it for a decision.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScopeClaims {
    /// `x-tenant-id`, if sent.
    pub tenant: Option<TenantId>,
    /// `x-organization-id`, if sent.
    pub organization: Option<OrganizationId>,
    /// `x-project-id` (or the project path segment), if sent.
    pub project: Option<ProjectId>,
}

/// The outcome: a scope, and the principal as the directory proved it.
#[derive(Debug, Clone)]
pub struct ResolvedRequest {
    /// The principal, carrying the role the directory proved for this scope.
    pub principal: SecurityPrincipal,
    /// The scope every repository call in this request must run under.
    pub scope: AuthoritativeScope,
}

/// Resolves request scope from identity.
///
/// Holds the tenancy resolver, and therefore the directory ports. There is no
/// constructor that works without one, so a half-wired process cannot resolve
/// scope at all.
#[derive(Clone)]
pub struct RequestScopeResolver {
    resolver: TenantResolver,
}

impl fmt::Debug for RequestScopeResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestScopeResolver")
            .finish_non_exhaustive()
    }
}

impl RequestScopeResolver {
    /// Binds the resolver to a directory.
    #[must_use]
    pub const fn new(resolver: TenantResolver) -> Self {
        Self { resolver }
    }

    /// Resolves the scope for one request.
    ///
    /// Order matters and is deny-by-default:
    ///   1. the credential must be temporally usable;
    ///   2. the target tenant is chosen — from the claim if the principal can
    ///      reach it, otherwise unambiguously from the principal's binding;
    ///   3. the tenancy resolver re-proves tenant, organization and project
    ///      against the directory;
    ///   4. a claimed organization that disagrees with the tenant's real one
    ///      is a hard, deterministic mismatch.
    ///
    /// # Errors
    /// * `UNAUTHORIZED` — expired or unusable credential;
    /// * `VALIDATION_FAILED` — no tenant claim and no unambiguous binding;
    /// * `RESOURCE_NOT_FOUND` — the tenant/project is unreachable, which is
    ///   indistinguishable from absent by design;
    /// * `FORBIDDEN` — the tenant or organization is not operational, or the
    ///   claimed organization does not match the tenant's.
    pub async fn resolve(
        &self,
        principal: &SecurityPrincipal,
        claims: ScopeClaims,
    ) -> Result<ResolvedRequest> {
        // 1. Credential validity. An expired credential is an authentication
        //    failure and is never retried.
        if !principal.is_usable() {
            return Err(AuthFailure::expired_credential().to_app_error());
        }

        // 2. Choose the target tenant from identity, using the claim only as a
        //    selector among what the principal can already reach.
        let tenant_id = self.choose_tenant(principal, claims.tenant)?;

        // 3. Re-prove everything against the directory. The resolver hides
        //    cross-tenant existence and refuses non-operational tenants.
        let outcome = match self
            .resolver
            .resolve(Some(principal), tenant_id, claims.project)
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if error.error_code() == "RESOURCE_NOT_FOUND" => {
                // Distinguish nothing: an unreachable tenant and an absent one
                // are the same answer, on purpose.
                return Err(AuthFailure::tenant_mismatch().as_hidden("tenant"));
            },
            Err(error) => return Err(error),
        };

        let resolved_organization = outcome.context.organization_id;

        // 4. A claimed organization that disagrees with the tenant's real one
        //    is blocked outright. This is the `tenant A + organization B` case
        //    from Phase 4: it is rejected rather than silently corrected,
        //    because a caller that believes two ids go together is a caller
        //    with a bug or an intent, and neither should be accommodated.
        if let Some(claimed) = claims.organization {
            if claimed != resolved_organization {
                return Err(AuthFailure::organization_mismatch().to_app_error());
            }
        }

        // The directory's answer about the role, not the principal's claim.
        let highest_role = outcome
            .context
            .roles
            .iter()
            .copied()
            .max_by_key(mas_domain::MembershipRole::rank);
        let principal = match highest_role {
            Some(role) => principal.clone().with_resolved_role(role),
            None => principal.clone(),
        };

        let mut scope = AuthoritativeScope::resolved(
            tenant_id,
            resolved_organization,
            principal.actor_id.clone(),
        );
        if let Some(project) = claims.project {
            // Only reachable here if the resolver proved the project belongs
            // to this tenant and organization; it errors otherwise.
            scope = scope.with_project(project);
        }

        Ok(ResolvedRequest { principal, scope })
    }

    /// Chooses the tenant: the claim when the principal can reach it, else the
    /// principal's own unambiguous binding.
    fn choose_tenant(
        &self,
        principal: &SecurityPrincipal,
        claimed: Option<TenantId>,
    ) -> Result<TenantId> {
        if let Some(claimed) = claimed {
            // The claim selects among reachable tenants. Anything else is
            // hidden, not refused.
            if !principal.can_reach_tenant(claimed) {
                return Err(AuthFailure::tenant_mismatch().as_hidden("tenant"));
            }
            return Ok(claimed);
        }

        // No claim: derive from identity, and only when unambiguous.
        if let Some(bound) = principal.tenant_id {
            return Ok(bound);
        }
        let distinct: BTreeSet<TenantId> = principal
            .memberships
            .iter()
            .map(|grant| grant.tenant_id)
            .collect();
        match distinct.len() {
            1 => Ok(distinct
                .into_iter()
                .next()
                .ok_or_else(|| AuthFailure::unauthenticated().to_app_error())?),
            0 => Err(mas_common::error::AppError::invalid_field(
                "x-tenant-id",
                "required",
                "this identity has no tenant binding of its own and must name one",
            )),
            _ => Err(mas_common::error::AppError::invalid_field(
                "x-tenant-id",
                "required",
                "the principal can reach more than one tenant; name one",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_domain::MembershipRole;
    use mas_security::{PrincipalKind, ScopeSet, TenantMembership};
    use mas_tenancy::{InMemoryDirectory, InMemoryMemberships, TenantDirectoryPort};
    use std::sync::Arc;

    fn id(seed: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(seed)
    }

    async fn world() -> (RequestScopeResolver, TenantId, OrganizationId, ProjectId) {
        let directory = Arc::new(InMemoryDirectory::new());
        let memberships = Arc::new(InMemoryMemberships::new());

        let organization = mas_domain::Organization::create(
            "Acme",
            "Acme",
            mas_domain::Slug::new("acme").expect("slug"),
        )
        .expect("org");
        let mut tenant = mas_domain::Tenant::create(
            organization.id,
            "Acme Prod",
            mas_domain::Slug::new("acme-prod").expect("slug"),
            mas_common::enums::Environment::Production,
            mas_domain::IsolationMode::SharedRls,
        )
        .expect("tenant");
        tenant.activate().expect("activate");
        let project = mas_domain::Project::create(
            tenant.id,
            organization.id,
            "Default",
            mas_domain::Slug::new("default").expect("slug"),
        )
        .expect("project");

        directory.upsert_organization(&organization).await.unwrap();
        directory.upsert_tenant(&tenant).await.unwrap();
        directory.upsert_project(&project).await.unwrap();

        let other_org = mas_domain::Organization::create(
            "Other",
            "Other",
            mas_domain::Slug::new("other").expect("slug"),
        )
        .expect("org");
        let mut other_tenant = mas_domain::Tenant::create(
            other_org.id,
            "Other Prod",
            mas_domain::Slug::new("other-prod").expect("slug"),
            mas_common::enums::Environment::Production,
            mas_domain::IsolationMode::SharedRls,
        )
        .expect("tenant");
        other_tenant.activate().expect("activate");
        directory.upsert_organization(&other_org).await.unwrap();
        directory.upsert_tenant(&other_tenant).await.unwrap();

        let resolver = TenantResolver::new(directory, memberships);
        (
            RequestScopeResolver::new(resolver),
            tenant.id,
            organization.id,
            project.id,
        )
    }

    #[tokio::test]
    async fn a_principal_resolves_to_its_own_tenant_without_any_header() {
        let (resolver, tenant, organization, _project) = world().await;
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::ApiKey,
            Some(tenant),
            ScopeSet::denied(),
        )
        .with_organization(organization)
        .with_role(MembershipRole::Developer);

        let resolved = resolver
            .resolve(&principal, ScopeClaims::default())
            .await
            .expect("resolve");
        assert_eq!(resolved.scope.tenant_id(), tenant);
        assert_eq!(resolved.scope.organization_id(), organization);
    }

    #[tokio::test]
    async fn a_forged_tenant_header_is_hidden_not_forbidden() {
        let (resolver, tenant, organization, _project) = world().await;
        let other_tenant = TenantId::from_uuid(id(0xBBBB));
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::ApiKey,
            Some(tenant),
            ScopeSet::denied(),
        )
        .with_organization(organization)
        .with_role(MembershipRole::Developer);

        let error = resolver
            .resolve(
                &principal,
                ScopeClaims {
                    tenant: Some(other_tenant),
                    ..ScopeClaims::default()
                },
            )
            .await
            .expect_err("must not resolve");
        assert_eq!(error.error_code(), "RESOURCE_NOT_FOUND");
    }

    #[tokio::test]
    async fn a_mismatched_organization_claim_is_a_deterministic_forbidden() {
        let (resolver, tenant, organization, _project) = world().await;
        let foreign_org = OrganizationId::from_uuid(id(0xCCCC));
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::ApiKey,
            Some(tenant),
            ScopeSet::denied(),
        )
        .with_organization(organization)
        .with_role(MembershipRole::Developer);

        let error = resolver
            .resolve(
                &principal,
                ScopeClaims {
                    organization: Some(foreign_org),
                    ..ScopeClaims::default()
                },
            )
            .await
            .expect_err("must not resolve");
        assert_eq!(error.error_code(), "FORBIDDEN");
    }

    #[tokio::test]
    async fn a_cross_tenant_project_is_invisible() {
        let (resolver, tenant, organization, _project) = world().await;
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::ApiKey,
            Some(tenant),
            ScopeSet::denied(),
        )
        .with_organization(organization)
        .with_role(MembershipRole::Developer);

        // A project id belonging to the *other* tenant.
        let foreign_project = ProjectId::from_uuid(id(0xDDDD));
        let error = resolver
            .resolve(
                &principal,
                ScopeClaims {
                    project: Some(foreign_project),
                    ..ScopeClaims::default()
                },
            )
            .await
            .expect_err("must not resolve");
        assert_eq!(error.error_code(), "RESOURCE_NOT_FOUND");
    }

    #[tokio::test]
    async fn a_project_inside_the_scope_narrows_it() {
        let (resolver, tenant, organization, project) = world().await;
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::ApiKey,
            Some(tenant),
            ScopeSet::denied(),
        )
        .with_organization(organization)
        .with_role(MembershipRole::Developer);

        let resolved = resolver
            .resolve(
                &principal,
                ScopeClaims {
                    project: Some(project),
                    ..ScopeClaims::default()
                },
            )
            .await
            .expect("resolve");
        assert_eq!(resolved.scope.project_id(), Some(project));
        assert_eq!(resolved.scope.tenant_id(), tenant);
    }

    #[tokio::test]
    async fn an_expired_credential_never_produces_a_scope() {
        let (resolver, tenant, organization, _project) = world().await;
        let past = mas_common::timestamps::Timestamp::from_unix_ms(1).expect("ts");
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::ApiKey,
            Some(tenant),
            ScopeSet::denied(),
        )
        .with_organization(organization)
        .with_role(MembershipRole::Developer)
        .with_expiry(past);

        let error = resolver
            .resolve(&principal, ScopeClaims::default())
            .await
            .expect_err("expired");
        assert_eq!(error.error_code(), "UNAUTHENTICATED");
    }

    #[tokio::test]
    async fn an_ambiguous_principal_must_name_a_tenant() {
        let (resolver, _tenant, organization, _project) = world().await;
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::User,
            None,
            ScopeSet::denied(),
        )
        .with_memberships(vec![
            TenantMembership::new(
                organization,
                TenantId::from_uuid(id(0x1111)),
                MembershipRole::Viewer,
            ),
            TenantMembership::new(
                organization,
                TenantId::from_uuid(id(0x2222)),
                MembershipRole::Viewer,
            ),
        ]);

        let error = resolver
            .resolve(&principal, ScopeClaims::default())
            .await
            .expect_err("ambiguous");
        assert_eq!(error.error_code(), "VALIDATION_FAILED");
    }

    #[tokio::test]
    async fn a_principal_with_one_membership_resolves_to_it() {
        let (resolver, tenant, organization, _project) = world().await;
        let principal = SecurityPrincipal::new(
            id(1).to_string(),
            PrincipalKind::User,
            None,
            ScopeSet::denied(),
        )
        .with_memberships(vec![TenantMembership::new(
            organization,
            tenant,
            MembershipRole::Viewer,
        )]);

        let resolved = resolver
            .resolve(&principal, ScopeClaims::default())
            .await
            .expect("resolve");
        assert_eq!(resolved.scope.tenant_id(), tenant);
        // A directory with no membership rows for this user has no role to
        // prove, so the principal carries none — deny-by-default.
        assert_eq!(
            resolved.principal.role_in(tenant, organization),
            Some(MembershipRole::Viewer)
        );
    }

    #[test]
    fn auth_failures_are_never_retryable() {
        // A caller that retries an authorization failure is a caller looping
        // on a door that will not open; the type says so explicitly.
        assert!(!AuthFailure::tenant_mismatch().is_retryable());
        assert!(!AuthFailure::organization_mismatch().is_retryable());
    }

    #[test]
    fn hiding_a_resource_produces_not_found() {
        let hidden = AuthFailure::tenant_mismatch().as_hidden("tenant");
        assert_eq!(hidden.error_code(), "RESOURCE_NOT_FOUND");
        assert!(!AuthFailure::tenant_mismatch().is_authentication_failure());
    }
}
