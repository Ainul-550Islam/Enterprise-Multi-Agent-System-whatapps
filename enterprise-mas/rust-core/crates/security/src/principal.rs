// PURPOSE: The ONE canonical principal — the authenticated actor, what it is
// bound to, and what it has been *proven* to be allowed to reach.
//
// Owning layer: `mas-security`. Every entrypoint (HTTP, gRPC, CLI, worker,
// scheduler, internal service call, Python bridge) produces exactly this type,
// and every authorization decision consumes exactly this type.
//
// Production responsibility: spec Phase 2 forbids separate incompatible
// principal models, and Phase 3 forbids trusting a scope merely because a
// caller named it. This type is how both are honoured: the scope a principal
// carries is the scope the *server* resolved from the credential and its
// memberships, never the scope the request asked for.
//
// Two shapes of binding exist, because two kinds of credential exist:
//   * **credential-bound** — an API key or service identity is issued *inside*
//     one tenant (`tenant_id`), and that binding is itself authoritative;
//   * **membership-bound** — a human user holds `(organization, tenant, role)`
//     grants looked up server-side at authentication time.
//
// [`SecurityPrincipal::role_in`] answers "what role does this principal hold
// in that (tenant, organization)?" for both shapes, and returns `None` — never
// a default — when the answer is "none".
//
// Security boundary: a principal is immutable once built except for
// [`SecurityPrincipal::prove_project`], which only ever *narrows* what has
// already been proven by a directory lookup. There is no setter that widens
// scope, and no path that lets a caller-supplied value become a membership.
//
// It must NOT:
//   • carry a role or scope that came from a request header, body or query —
//     those are claims, and a claim is not a grant;
//   • expose a mutable setter for `tenant_id`, `organization_id` or
//     `memberships` after construction;
//   • treat "no binding" as "any tenant". An unbound human principal matches
//     nothing; only the explicit platform identity is intentionally unscoped,
//     and it is audited as such.

use crate::scopes::ScopeSet;
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_domain::MembershipRole;
use serde::{Deserialize, Serialize};

string_enum! {
    /// Credential class backing a principal.
    ///
    /// Spec Phase 2 requires these to be distinguishable: each kind reaches the
    /// database under a different rule, and each is audited differently.
    PrincipalKind {
        /// A human user, authenticated by token, holding memberships.
        User => "user",
        /// A machine principal acting on behalf of a tenant.
        ServiceAccount => "service_account",
        /// A long-lived bearer API key, bound to exactly one tenant.
        ApiKey => "api_key",
        /// A short-lived actor token (JWT).
        Token => "token",
        /// Internal system identity (`"system"`). The only intentionally
        /// unscoped lane; every use is audited.
        System => "system",
        /// The worker plane. Tenant comes from the durable task row it owns,
        /// never from a caller.
        Worker => "worker",
        /// The scheduler plane. Tenant comes from the durable schedule row.
        Scheduler => "scheduler",
    }
}

/// One proven `(organization, tenant, role)` grant.
///
/// This is the unit of authoritative tenancy: it is produced by a directory
/// lookup on the server, not by anything a caller can name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantMembership {
    pub organization_id: OrganizationId,
    pub tenant_id: TenantId,
    pub role: MembershipRole,
}

impl TenantMembership {
    /// One grant.
    #[must_use]
    pub const fn new(
        organization_id: OrganizationId,
        tenant_id: TenantId,
        role: MembershipRole,
    ) -> Self {
        Self {
            organization_id,
            tenant_id,
            role,
        }
    }
}

/// An authenticated caller with its capability surface.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityPrincipal {
    /// Who authenticated: user id, token `sub`, API key id, `system`, or a
    /// worker/scheduler replica id. Audit-visible.
    pub actor_id: String,
    pub kind: PrincipalKind,
    /// The tenant the credential is bound to (`None` for membership-bound
    /// human principals and for the platform identity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    /// The organization, when the credential carries one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<OrganizationId>,
    /// Proven `(organization, tenant, role)` grants. Authoritative for
    /// membership-bound principals; empty for credential-bound ones.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memberships: Vec<TenantMembership>,
    /// The role a credential-bound principal carries inside its own tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<MembershipRole>,
    /// Free-form role labels carried on external tokens (never authoritative).
    #[serde(default)]
    pub roles: Vec<String>,
    pub scopes: ScopeSet,
    /// Credential expiry; `is_usable()` fails past this point.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Timestamp>,
    /// Projects proven, by a directory lookup, to belong to a tenant this
    /// principal is bound to. Only ever narrows.
    #[serde(default, skip)]
    proven_projects: Vec<(TenantId, ProjectId)>,
    /// The role the tenancy resolver proved from the membership directory for
    /// the scope currently in effect.
    ///
    /// This is the *only* role on a principal that outranks its memberships,
    /// and it outranks them because it is the directory's answer, not the
    /// principal's claim. A human user's `memberships` may be stale; the
    /// directory is re-read for every resolution. Set exclusively by
    /// [`Self::with_resolved_role`].
    #[serde(default, skip)]
    resolved_role: Option<MembershipRole>,
}

impl SecurityPrincipal {
    /// Builds a credential-bound principal (API keys, service identities,
    /// internal services). Membership-bound principals add grants with
    /// [`Self::with_memberships`].
    #[must_use]
    pub fn new(
        actor_id: impl Into<String>,
        kind: PrincipalKind,
        tenant_id: Option<TenantId>,
        scopes: ScopeSet,
    ) -> Self {
        Self {
            actor_id: actor_id.into(),
            kind,
            tenant_id,
            organization_id: None,
            memberships: Vec::new(),
            role: None,
            roles: Vec::new(),
            scopes,
            expires_at: None,
            proven_projects: Vec::new(),
            resolved_role: None,
        }
    }

    /// The internal platform identity: intentionally unscoped, fully audited.
    #[must_use]
    pub fn system() -> Self {
        Self::new("system", PrincipalKind::System, None, ScopeSet::all())
    }

    /// A worker-plane identity. Its tenant authority is the durable task row
    /// it holds a lease on, so it is constructed without a tenant binding.
    #[must_use]
    pub fn worker(replica: impl Into<String>) -> Self {
        Self::new(replica, PrincipalKind::Worker, None, ScopeSet::denied())
    }

    /// A scheduler-plane identity, likewise bound to durable rows.
    #[must_use]
    pub fn scheduler(replica: impl Into<String>) -> Self {
        Self::new(replica, PrincipalKind::Scheduler, None, ScopeSet::denied())
    }

    #[must_use]
    pub const fn with_organization(mut self, organization_id: OrganizationId) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    /// Attaches proven grants. This is the authoritative tenancy of a
    /// membership-bound principal.
    #[must_use]
    pub fn with_memberships(mut self, memberships: Vec<TenantMembership>) -> Self {
        self.memberships = memberships;
        self
    }

    /// Attaches the single role a credential-bound principal holds.
    #[must_use]
    pub const fn with_role(mut self, role: MembershipRole) -> Self {
        self.role = Some(role);
        self
    }

    #[must_use]
    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles = roles.into_iter().map(Into::into).collect();
        self
    }

    #[must_use]
    pub const fn with_expiry(mut self, expires_at: Timestamp) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Sets the credential expiry, when there is one.
    ///
    /// A credential with no expiry stays valid until revoked — which is a
    /// deliberate minting decision, not an oversight, so `None` must not be
    /// mistaken for "expired".
    #[must_use]
    pub const fn with_optional_expiry(mut self, expires_at: Option<Timestamp>) -> Self {
        self.expires_at = expires_at;
        self
    }

    /// Whether this principal carries its own tenancy binding rather than
    /// deriving one from memberships.
    ///
    /// A credential-bound principal (API key, service account, short-lived
    /// token) was issued *inside* a tenant and the credential row is the
    /// authority on where. A membership-bound principal (a human user) has no
    /// authority of its own: every grant it holds comes from the membership
    /// directory, which means a stale or forged grant on the principal itself
    /// must never be believed.
    #[must_use]
    pub fn is_credential_bound(&self) -> bool {
        matches!(
            self.kind,
            PrincipalKind::ApiKey | PrincipalKind::ServiceAccount | PrincipalKind::Token
        )
    }

    /// Whether this principal is tied to any tenancy at all.
    ///
    /// `false` means "reaches whatever the machine plane points it at" — the
    /// worker and scheduler identities, whose tenant authority is the durable
    /// row they hold a lease on, and the platform identity.
    #[must_use]
    pub fn is_bound(&self) -> bool {
        self.tenant_id.is_some() || self.organization_id.is_some() || !self.memberships.is_empty()
    }

    /// Whether this principal can reach `tenant_id` at all.
    ///
    /// An unbound principal reaches whatever it is aimed at; a bound one
    /// reaches only the tenant it is bound to, either directly or through one
    /// of its memberships. `false` must be answered by hiding the tenant's
    /// existence, never by refusing loudly — a refusal would confirm the
    /// tenant is there.
    #[must_use]
    pub fn can_reach_tenant(&self, tenant_id: TenantId) -> bool {
        if !self.is_bound() {
            return true;
        }
        if self.tenant_id == Some(tenant_id) {
            return true;
        }
        self.memberships
            .iter()
            .any(|membership| membership.tenant_id == tenant_id)
    }

    /// Attaches the role the tenancy resolver proved from the directory.
    ///
    /// Only the resolver calls this, and only with a role it read from the
    /// membership directory for the scope being resolved. It is not a way to
    /// grant a role: it is the record of a grant that already exists.
    ///
    /// Security boundary: setting this overrides the principal's own
    /// memberships. That is correct here and nowhere else, which is why no
    /// other constructor on this type touches it.
    #[must_use]
    pub const fn with_resolved_role(mut self, role: MembershipRole) -> Self {
        self.resolved_role = Some(role);
        self
    }

    /// Records that a project was proven, by directory lookup, to belong to a
    /// tenant this principal is bound to. Narrowing only.
    pub fn prove_project(&mut self, tenant_id: TenantId, project_id: ProjectId) {
        if self.role_in_tenant(tenant_id).is_some() || self.tenant_id == Some(tenant_id) {
            self.proven_projects.push((tenant_id, project_id));
        }
    }

    /// Whether a project has been proven for that tenant.
    #[must_use]
    pub fn project_is_proven(&self, tenant_id: TenantId, project_id: ProjectId) -> bool {
        self.proven_projects.contains(&(tenant_id, project_id))
    }

    /// Whether the credential itself is temporally usable *now*.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        self.expires_at.as_ref().is_none_or(Timestamp::is_future)
    }

    /// Whether this is one of the intentionally unscoped internal planes.
    #[must_use]
    pub fn is_platform_identity(&self) -> bool {
        matches!(self.kind, PrincipalKind::System)
    }

    /// Whether this principal is a worker or scheduler plane identity. Those
    /// are authenticated but derive their tenant from durable rows, so they
    /// are never authorized through the membership path.
    #[must_use]
    pub fn is_machine_plane(&self) -> bool {
        matches!(self.kind, PrincipalKind::Worker | PrincipalKind::Scheduler)
    }

    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|held| held == role)
    }

    #[must_use]
    pub fn permits(&self, scope: &str) -> bool {
        self.scopes.permits(scope)
    }

    /// Whether the credential is bound to any tenant at all.
    #[must_use]
    pub fn is_bound_to_any_tenant(&self) -> bool {
        self.tenant_id.is_some() || !self.memberships.is_empty()
    }

    /// The grant this principal holds in one tenant, if any.
    #[must_use]
    pub fn role_in_tenant(&self, tenant_id: TenantId) -> Option<MembershipRole> {
        // A directory-proven role wins over anything the principal carries.
        if let Some(role) = self.resolved_role {
            return Some(role);
        }
        if let Some(membership) = self
            .memberships
            .iter()
            .find(|grant| grant.tenant_id == tenant_id)
        {
            return Some(membership.role);
        }
        if self.tenant_id == Some(tenant_id) {
            return self.role;
        }
        None
    }

    /// The role this principal holds in one `(tenant, organization)` pair.
    ///
    /// Both halves must match. A grant in tenant A says nothing about tenant
    /// B even inside the same organization, and the same tenant reached
    /// through the wrong organization is not the same authority.
    #[must_use]
    pub fn role_in(
        &self,
        tenant_id: TenantId,
        organization_id: OrganizationId,
    ) -> Option<MembershipRole> {
        // A directory-proven role wins over anything the principal carries.
        if let Some(role) = self.resolved_role {
            return Some(role);
        }
        if let Some(membership) = self
            .memberships
            .iter()
            .find(|grant| grant.tenant_id == tenant_id && grant.organization_id == organization_id)
        {
            return Some(membership.role);
        }
        // Credential-bound: the binding must match, and an explicit
        // organization on the credential must agree too.
        if self.tenant_id == Some(tenant_id)
            && self
                .organization_id
                .is_none_or(|bound| bound == organization_id)
        {
            return self.role;
        }
        None
    }

    /// The tenants this principal can reach. Empty for the platform identity
    /// (which is intentionally unscoped) and for an unbound, unauthenticated
    /// principal (which can reach nothing).
    #[must_use]
    pub fn reachable_tenants(&self) -> Vec<TenantId> {
        let mut out: Vec<TenantId> = self.memberships.iter().map(|m| m.tenant_id).collect();
        if let Some(tenant) = self.tenant_id {
            if !out.contains(&tenant) {
                out.push(tenant);
            }
        }
        out
    }

    /// The tenant invariant: a principal bound to tenant A cannot be checked
    /// against tenant B's context by construction.
    #[must_use]
    pub fn tenant_matches(&self, tenant_id: TenantId) -> bool {
        self.tenant_id == Some(tenant_id)
            || self
                .memberships
                .iter()
                .any(|grant| grant.tenant_id == tenant_id)
    }

    /// The single gate used by the legacy call sites: usable + tenant match +
    /// scope. The authorization service is the authority; this exists so
    /// existing checks keep their meaning during the migration.
    #[must_use]
    pub fn checked(&self, tenant_id: TenantId, scope: &str) -> bool {
        self.is_usable() && self.tenant_matches(tenant_id) && self.permits(scope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn tenant(seed: u128) -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(seed))
    }
    fn org(seed: u128) -> OrganizationId {
        OrganizationId::from_uuid(uuid::Uuid::from_u128(seed))
    }

    #[test]
    fn gate_composes_expiry_tenant_and_scope() {
        let principal = SecurityPrincipal::new(
            "user-1",
            PrincipalKind::User,
            Some(tenant(1)),
            ScopeSet::new(["executions:read"]).expect("scopes"),
        );
        assert!(principal.checked(tenant(1), "executions:read"));
        assert!(!principal.checked(tenant(1), "executions:cancel"));
        assert!(!principal.checked(tenant(2), "executions:read"));
        assert!(principal.is_usable());

        let expired = principal.clone().with_expiry(
            Timestamp::now()
                .checked_sub(Duration::from_secs(1))
                .expect("past"),
        );
        assert!(!expired.is_usable());
        assert!(!expired.checked(tenant(1), "executions:read"));
    }

    #[test]
    fn system_principal_is_break_glass() {
        let system = SecurityPrincipal::system();
        assert!(system.is_usable());
        assert!(system.permits("anything:here"));
        assert!(
            !system.tenant_matches(tenant(9)),
            "system is tenant-agnostic"
        );
        assert!(system.is_platform_identity());
    }

    #[test]
    fn membership_and_credential_bindings_both_resolve() {
        let membership_bound =
            SecurityPrincipal::new("user-1", PrincipalKind::User, None, ScopeSet::denied())
                .with_memberships(vec![
                    TenantMembership::new(org(1), tenant(1), MembershipRole::Admin),
                    TenantMembership::new(org(2), tenant(2), MembershipRole::Viewer),
                ]);
        assert_eq!(
            membership_bound.role_in(tenant(1), org(1)),
            Some(MembershipRole::Admin)
        );
        assert_eq!(
            membership_bound.role_in(tenant(2), org(2)),
            Some(MembershipRole::Viewer)
        );
        // The right tenant through the WRONG organization is not a grant.
        assert_eq!(membership_bound.role_in(tenant(1), org(2)), None);
        assert_eq!(membership_bound.role_in(tenant(3), org(1)), None);
        assert_eq!(membership_bound.reachable_tenants().len(), 2);

        let credential_bound = SecurityPrincipal::new(
            "key-1",
            PrincipalKind::ApiKey,
            Some(tenant(1)),
            ScopeSet::denied(),
        )
        .with_organization(org(1))
        .with_role(MembershipRole::ServiceAccount);
        assert_eq!(
            credential_bound.role_in(tenant(1), org(1)),
            Some(MembershipRole::ServiceAccount)
        );
        assert_eq!(credential_bound.role_in(tenant(1), org(2)), None);
    }

    #[test]
    fn an_unbound_principal_reaches_nothing() {
        let anonymous =
            SecurityPrincipal::new("nobody", PrincipalKind::Token, None, ScopeSet::denied());
        assert!(!anonymous.is_bound_to_any_tenant());
        assert!(anonymous.reachable_tenants().is_empty());
        assert_eq!(anonymous.role_in(tenant(1), org(1)), None);
    }

    #[test]
    fn proving_a_project_only_narrows() {
        let mut principal = SecurityPrincipal::new(
            "user-1",
            PrincipalKind::User,
            Some(tenant(1)),
            ScopeSet::denied(),
        );
        let project = ProjectId::from_uuid(uuid::Uuid::from_u128(7));
        // A project in a tenant this principal is NOT bound to is never
        // recorded, so a later check cannot be talked into accepting it.
        principal.prove_project(tenant(2), project);
        assert!(!principal.project_is_proven(tenant(2), project));

        principal.prove_project(tenant(1), project);
        assert!(principal.project_is_proven(tenant(1), project));
    }

    #[test]
    fn machine_planes_are_distinguishable() {
        assert!(SecurityPrincipal::worker("w-1").is_machine_plane());
        assert!(SecurityPrincipal::scheduler("s-1").is_machine_plane());
        assert!(!SecurityPrincipal::system().is_machine_plane());
    }
}
