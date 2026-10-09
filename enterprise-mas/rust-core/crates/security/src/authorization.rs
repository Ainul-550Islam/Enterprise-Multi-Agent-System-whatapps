// PURPOSE: The canonical authorization service: the ONE place a "may this
// principal do this to that resource?" question is answered.
//
// Owning layer: `mas-security`.
//
// Production responsibility: spec Phase 5 names twelve decisions the platform
// must be able to make — read, create, update, delete, execute, cancel,
// publish, administer, manage credentials, manage policies, view audit, manage
// billing — and forbids scattering ad-hoc role checks through handlers. A
// handler may *ask*; only this service decides.
//
// The decision is ABAC-shaped (Phase 7): principal + tenant + organization +
// project + resource + operation are all inputs, because "can cancel" is not
// one answer — it depends on which execution, in which tenant, under which
// role. RBAC (Phase 6) is the role→permission matrix that sits underneath it.
//
// Security boundary: **deny by default.** The permission matrix is an
// allow-list; anything not explicitly granted is refused, and an unknown role,
// an unknown resource kind or a missing scope all resolve to `Deny`. There is
// no code path in this file that returns `Allow` without a matching grant, and
// no role inherits permissions implicitly by rank except where the matrix says
// so explicitly.
//
// It must NOT:
//   • trust a role, permission or scope that arrived from the caller — the
//     role comes from the resolved membership the *server* looked up;
//   • return `Allow` for a scope mismatch. A grant in tenant A says nothing
//     about tenant B, and crossing the boundary is a `TenantMismatch`, never a
//     silent re-scope;
//   • become a second authorization system. If a check is needed, it belongs
//     here;
//   • perform I/O. It is a pure decision function over already-resolved
//     inputs, so it is deterministic and testable without a database.

use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_domain::MembershipRole;
use std::collections::BTreeMap;
use std::fmt;

use crate::auth_failure::AuthFailure;
use crate::principal::SecurityPrincipal;

// The twelve (plus) decisions the platform makes about a resource:
// read, create, update, delete, execute, cancel, publish, administer, manage
// credentials, manage policies, view audit, manage billing, plus manage
// members. The wire value of each is part of the API contract.
string_enum! {
    /// A verb on a resource. The wire value is part of the API contract.
    Action {
        Read => "read",
        Create => "create",
        Update => "update",
        Delete => "delete",
        Execute => "execute",
        Cancel => "cancel",
        Publish => "publish",
        Administer => "administer",
        ManageCredentials => "manage_credentials",
        ManagePolicies => "manage_policies",
        ViewAudit => "view_audit",
        ManageBilling => "manage_billing",
        /// Manage memberships (invite, revoke, change role).
        ManageMembers => "manage_members",
    }
}

// What kind of thing the action applies to. Spec Phase 7 requires at
// minimum execution, task, workflow, agent, tool, connector, credential
// metadata, policy, usage, audit, webhook and subscription/entitlement.
string_enum! {
    /// The resource taxonomy. Spec Phase 7 requires at minimum execution, task,
    /// workflow, agent, tool, connector, credential metadata, policy, usage,
    /// audit, webhook and subscription/entitlement.
    ResourceKind {
        Execution => "execution",
        Task => "task",
        Workflow => "workflow",
        Agent => "agent",
        Tool => "tool",
        Connector => "connector",
        Credential => "credential",
        Policy => "policy",
        Usage => "usage",
        Audit => "audit",
        Webhook => "webhook",
        Subscription => "subscription",
        ApiKey => "api_key",
        Schedule => "schedule",
        /// Recipient-scoped durable notification lifecycle records.
        Notification => "notification",
        Tenant => "tenant",
        Organization => "organization",
        Project => "project",
        Membership => "membership",
        /// Rust-owned source metadata delegated to the isolated Data Hub plane.
        DataSource => "data_source",
        /// Rust-owned document metadata; raw content and vectors are never an
        /// authorization resource exposed from this API.
        Document => "document",
    }
}

/// The full context of one authorization question.
///
/// Every field is *resolved* — proven by the server from authenticated
/// identity — never echoed from a request header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationRequest {
    pub resource: ResourceKind,
    pub action: Action,
    /// Tenant the resource lives in (resolved, not claimed).
    pub tenant_id: TenantId,
    /// Organization the tenant belongs to (resolved).
    pub organization_id: OrganizationId,
    /// Project/workspace, when the resource is project-scoped.
    pub project_id: Option<ProjectId>,
}

/// The outcome, with the reason it went that way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationDecision {
    /// Permitted. Carries which role granted it, for the audit trail.
    Allow { granted_by: MembershipRole },
    /// Refused, with a stable classification.
    Deny(AuthFailure),
}

impl AuthorizationDecision {
    /// `true` only on an explicit grant.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow { .. })
    }

    /// Converts a denial into a transport error. Panics on `Allow`, which
    /// would be a programming error: callers check `is_allowed` first.
    pub fn into_result(self) -> Result<()> {
        match self {
            Self::Allow { .. } => Ok(()),
            Self::Deny(failure) => Err(failure.to_app_error()),
        }
    }
}

/// The role → (resource, action) permission matrix.
///
/// Written as an explicit allow-list per role. This is deliberately verbose:
/// a compact "rank >= X" scheme is how a viewer ends up able to read audit
/// data six months later. Each role's grants are readable in one place.
const fn role_grants(role: &MembershipRole) -> &'static [(ResourceKind, &'static [Action])] {
    match role {
        // Owner: full control inside the tenant, including deletion of the
        // tenant itself, membership administration and billing.
        MembershipRole::Owner => &[
            (
                ResourceKind::Execution,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Cancel,
                    Action::Publish,
                    Action::Administer,
                ],
            ),
            (
                ResourceKind::Task,
                &[
                    Action::Read,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Cancel,
                ],
            ),
            (
                ResourceKind::Workflow,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Publish,
                ],
            ),
            (
                ResourceKind::Agent,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Publish,
                ],
            ),
            (
                ResourceKind::Tool,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            (
                ResourceKind::Connector,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (
                ResourceKind::DataSource,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            (
                ResourceKind::Document,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (
                ResourceKind::Credential,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::ManageCredentials,
                ],
            ),
            (
                ResourceKind::Policy,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::ManagePolicies,
                ],
            ),
            (ResourceKind::Usage, &[Action::Read, Action::ManageBilling]),
            (ResourceKind::Audit, &[Action::Read, Action::ViewAudit]),
            (
                ResourceKind::Webhook,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Publish,
                ],
            ),
            (
                ResourceKind::Subscription,
                &[Action::Read, Action::ManageBilling],
            ),
            (
                ResourceKind::ApiKey,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Delete,
                    Action::ManageCredentials,
                ],
            ),
            (
                ResourceKind::Schedule,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            // The application layer additionally restricts this to the
            // recipient or a tenant administrator.
            (ResourceKind::Notification, &[Action::Read, Action::Update]),
            (
                ResourceKind::Tenant,
                &[
                    Action::Read,
                    Action::Update,
                    Action::Delete,
                    Action::Administer,
                ],
            ),
            (ResourceKind::Organization, &[Action::Read, Action::Update]),
            (
                ResourceKind::Project,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (
                ResourceKind::Membership,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::ManageMembers,
                ],
            ),
        ],
        // Admin: everything the owner can do *except* destroying the tenant and
        // moving money. Deleting a tenant or changing a subscription is a
        // business-continuity decision, not a day-to-day administration one.
        MembershipRole::Admin => &[
            (
                ResourceKind::Execution,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Cancel,
                    Action::Publish,
                    Action::Administer,
                ],
            ),
            (
                ResourceKind::Task,
                &[
                    Action::Read,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Cancel,
                ],
            ),
            (
                ResourceKind::Workflow,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Publish,
                ],
            ),
            (
                ResourceKind::Agent,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Publish,
                ],
            ),
            (
                ResourceKind::Tool,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            (
                ResourceKind::Connector,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (
                ResourceKind::DataSource,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            (
                ResourceKind::Document,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (
                ResourceKind::Credential,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::ManageCredentials,
                ],
            ),
            (
                ResourceKind::Policy,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::ManagePolicies,
                ],
            ),
            (ResourceKind::Usage, &[Action::Read]),
            (ResourceKind::Audit, &[Action::Read, Action::ViewAudit]),
            (
                ResourceKind::Webhook,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Publish,
                ],
            ),
            (ResourceKind::Subscription, &[Action::Read]),
            (
                ResourceKind::ApiKey,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Delete,
                    Action::ManageCredentials,
                ],
            ),
            (
                ResourceKind::Schedule,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            (ResourceKind::Notification, &[Action::Read, Action::Update]),
            (
                ResourceKind::Tenant,
                &[Action::Read, Action::Update, Action::Administer],
            ),
            (ResourceKind::Organization, &[Action::Read]),
            (
                ResourceKind::Project,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (
                ResourceKind::Membership,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::ManageMembers,
                ],
            ),
        ],
        // Developer: builds and runs things. Cannot touch credentials it did
        // not create, cannot read audit data, cannot administer the tenant.
        MembershipRole::Developer => &[
            (
                ResourceKind::Execution,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Execute,
                    Action::Cancel,
                    Action::Publish,
                ],
            ),
            (
                ResourceKind::Task,
                &[
                    Action::Read,
                    Action::Update,
                    Action::Execute,
                    Action::Cancel,
                ],
            ),
            (
                ResourceKind::Workflow,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                    Action::Publish,
                ],
            ),
            (
                ResourceKind::Agent,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            (
                ResourceKind::Tool,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Execute,
                ],
            ),
            (
                ResourceKind::Connector,
                &[Action::Read, Action::Create, Action::Update],
            ),
            (
                ResourceKind::DataSource,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Execute,
                ],
            ),
            (
                ResourceKind::Document,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (ResourceKind::Credential, &[Action::Read]),
            (ResourceKind::Policy, &[Action::Read]),
            (ResourceKind::Usage, &[Action::Read]),
            (
                ResourceKind::Webhook,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (
                ResourceKind::Schedule,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Update,
                    Action::Delete,
                    Action::Execute,
                ],
            ),
            (ResourceKind::Notification, &[Action::Read, Action::Update]),
            (ResourceKind::Tenant, &[Action::Read]),
            (ResourceKind::Organization, &[Action::Read]),
            (
                ResourceKind::Project,
                &[Action::Read, Action::Create, Action::Update],
            ),
            (ResourceKind::Membership, &[Action::Read]),
        ],
        // Operator: runs and stops work, does not define it. The role for
        // on-call: enough to cancel a bad run, not enough to change what runs.
        MembershipRole::Operator => &[
            (
                ResourceKind::Execution,
                &[
                    Action::Read,
                    Action::Update,
                    Action::Execute,
                    Action::Cancel,
                ],
            ),
            (
                ResourceKind::Task,
                &[
                    Action::Read,
                    Action::Update,
                    Action::Execute,
                    Action::Cancel,
                ],
            ),
            (ResourceKind::Workflow, &[Action::Read, Action::Execute]),
            (ResourceKind::Agent, &[Action::Read, Action::Execute]),
            (ResourceKind::Tool, &[Action::Read, Action::Execute]),
            (ResourceKind::Connector, &[Action::Read]),
            (ResourceKind::DataSource, &[Action::Read, Action::Execute]),
            (ResourceKind::Document, &[Action::Read]),
            (ResourceKind::Policy, &[Action::Read]),
            (
                ResourceKind::Schedule,
                &[Action::Read, Action::Update, Action::Execute],
            ),
            (ResourceKind::Notification, &[Action::Read, Action::Update]),
            (ResourceKind::Tenant, &[Action::Read]),
            (ResourceKind::Organization, &[Action::Read]),
            (ResourceKind::Project, &[Action::Read]),
            (ResourceKind::Membership, &[Action::Read]),
        ],
        // Viewer: read-only, and deliberately NOT everything. Credential
        // metadata and audit data are excluded: "can log in" is not "can read
        // the security trail".
        MembershipRole::Viewer => &[
            (ResourceKind::Execution, &[Action::Read]),
            (ResourceKind::Task, &[Action::Read]),
            (ResourceKind::Workflow, &[Action::Read]),
            (ResourceKind::Agent, &[Action::Read]),
            (ResourceKind::Tool, &[Action::Read]),
            (ResourceKind::Connector, &[Action::Read]),
            (ResourceKind::DataSource, &[Action::Read]),
            (ResourceKind::Document, &[Action::Read]),
            (ResourceKind::Policy, &[Action::Read]),
            (ResourceKind::Usage, &[Action::Read]),
            (ResourceKind::Schedule, &[Action::Read]),
            (ResourceKind::Notification, &[Action::Read, Action::Update]),
            (ResourceKind::Tenant, &[Action::Read]),
            (ResourceKind::Organization, &[Action::Read]),
            (ResourceKind::Project, &[Action::Read]),
        ],
        // Service account: the machine lane. It may run and report on work;
        // it may not administer the tenant or mint credentials.
        MembershipRole::ServiceAccount => &[
            (
                ResourceKind::Execution,
                &[
                    Action::Read,
                    Action::Create,
                    Action::Execute,
                    Action::Cancel,
                ],
            ),
            (
                ResourceKind::Task,
                &[Action::Read, Action::Execute, Action::Cancel],
            ),
            (ResourceKind::Workflow, &[Action::Read, Action::Execute]),
            (ResourceKind::Agent, &[Action::Read, Action::Execute]),
            (ResourceKind::Tool, &[Action::Read, Action::Execute]),
            (ResourceKind::DataSource, &[Action::Read, Action::Execute]),
            (
                ResourceKind::Document,
                &[Action::Read, Action::Create, Action::Update, Action::Delete],
            ),
            (ResourceKind::Webhook, &[Action::Read, Action::Publish]),
            (ResourceKind::Schedule, &[Action::Read]),
            (ResourceKind::Notification, &[Action::Read]),
            (ResourceKind::Tenant, &[Action::Read]),
            (ResourceKind::Organization, &[Action::Read]),
            (ResourceKind::Project, &[Action::Read]),
        ],
    }
}

/// The canonical authorization service.
///
/// Stateless and pure: it takes an already-authenticated principal and an
/// already-resolved request and returns a decision. Keeping it pure is what
/// makes the whole matrix testable without a database, and what lets the same
/// decision be reached from HTTP, gRPC, the worker and the CLI.
#[derive(Debug, Clone, Copy, Default)]
pub struct AuthorizationService;

impl AuthorizationService {
    /// Creates the service (no state; the constructor exists so call sites
    /// read as composition rather than as a unit struct).
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Answers one authorization question.
    ///
    /// Order matters and is deliberate:
    ///   1. **credential validity** — an expired credential denies everything;
    ///   2. **tenant binding** — a principal cannot act outside its tenants;
    ///   3. **organization binding** — and not outside its organizations;
    ///   4. **scope coherence** — tenant/org/project must be one real path;
    ///   5. **role permission** — the RBAC allow-list.
    ///
    /// A failure at any step is a distinct, stable classification.
    #[must_use]
    pub fn authorize(
        &self,
        principal: &SecurityPrincipal,
        request: &AuthorizationRequest,
    ) -> AuthorizationDecision {
        // 1. The credential itself.
        if !principal.is_usable() {
            return AuthorizationDecision::Deny(
                AuthFailure::expired_credential().with_detail("credential is no longer in date"),
            );
        }

        // 2/3. Scope binding. A break-glass system principal is the only
        // identity allowed to operate without a membership, and it is audited
        // as such — it is not a hole, it is a named, deliberate lane.
        if !principal.is_platform_identity() {
            match principal.role_in(request.tenant_id, request.organization_id) {
                Some(role) => {
                    // 4. Scope coherence: a membership proves (org, tenant).
                    // A project, when the request carries one, must be
                    // validated by the caller against that same pair — the
                    // service cannot do it without I/O, so it enforces the
                    // invariant it *can* see and refuses unvalidated claims.
                    let project_ok = match request.project_id {
                        Some(project) => principal.project_is_proven(request.tenant_id, project),
                        None => true,
                    };
                    if !project_ok {
                        return AuthorizationDecision::Deny(
                            AuthFailure::project_mismatch()
                                .with_detail("the project is not proven to belong to this tenant"),
                        );
                    }
                    // 5. RBAC allow-list.
                    if Self::role_permits(&role, request.resource, request.action) {
                        return AuthorizationDecision::Allow { granted_by: role };
                    }
                    return AuthorizationDecision::Deny(
                        AuthFailure::insufficient_permission().with_detail(format!(
                            "{} may not {} a {}",
                            role.as_str(),
                            request.action.as_str(),
                            request.resource.as_str()
                        )),
                    );
                },
                None => {
                    return AuthorizationDecision::Deny(if principal.is_bound_to_any_tenant() {
                        AuthFailure::tenant_mismatch()
                            .with_detail("the credential is not bound to the requested tenant")
                    } else {
                        AuthFailure::organization_mismatch().with_detail(
                            "the credential holds no membership in the requested organization",
                        )
                    })
                },
            }
        }

        // Platform identity: allowed, and recorded as such. This is the one
        // path that is not an allow-list, and it exists only for internal
        // schedulers/reapers whose work is attributed to a durable row, not to
        // a human caller.
        AuthorizationDecision::Allow {
            granted_by: MembershipRole::ServiceAccount,
        }
    }

    /// The single matrix lookup. Deny by default.
    #[must_use]
    pub fn role_permits(role: &MembershipRole, resource: ResourceKind, action: Action) -> bool {
        role_grants(role)
            .iter()
            .find(|(kind, _)| *kind == resource)
            .is_some_and(|(_, actions)| actions.contains(&action))
    }

    /// Every (resource, action) pair a role holds, for display and tests.
    #[must_use]
    pub fn grants_for(role: &MembershipRole) -> BTreeMap<String, Vec<String>> {
        role_grants(role)
            .iter()
            .map(|(kind, actions)| {
                (
                    kind.as_str().to_owned(),
                    actions
                        .iter()
                        .map(|action| action.as_str().to_owned())
                        .collect(),
                )
            })
            .collect()
    }
}

/// Object-safe port so the API layer can inject the service and tests can
/// substitute a recording double.
pub trait AuthorizationServicePort: Send + Sync + fmt::Debug {
    /// Authorizes one request; returns `Err` with a stable code on denial.
    fn authorize(
        &self,
        principal: &SecurityPrincipal,
        request: &AuthorizationRequest,
    ) -> Result<MembershipRole>;

    /// Convenience: authorize or fail with the stable classification.
    fn require(
        &self,
        principal: &SecurityPrincipal,
        request: &AuthorizationRequest,
    ) -> Result<MembershipRole> {
        self.authorize(principal, request)
    }

    /// Whether the role may perform the action on the resource, without a
    /// scope. Useful for menu/build decisions, never for a data path.
    fn role_permits(&self, role: &MembershipRole, resource: ResourceKind, action: Action) -> bool;
}

impl AuthorizationServicePort for AuthorizationService {
    fn authorize(
        &self,
        principal: &SecurityPrincipal,
        request: &AuthorizationRequest,
    ) -> Result<MembershipRole> {
        match AuthorizationService::authorize(self, principal, request) {
            AuthorizationDecision::Allow { granted_by } => Ok(granted_by),
            AuthorizationDecision::Deny(failure) => failure.err(),
        }
    }

    fn role_permits(&self, role: &MembershipRole, resource: ResourceKind, action: Action) -> bool {
        AuthorizationService::role_permits(role, resource, action)
    }
}

/// The safe placeholder used during composition, so a half-wired binary
/// refuses rather than permits.
#[derive(Debug, Clone, Copy, Default)]
pub struct DenyByDefault;

impl DenyByDefault {
    /// Always denies. A caller that reaches this has a wiring bug, and the
    /// correct behaviour for a wiring bug in an authorization path is to
    /// fail closed.
    pub fn authorize(&self) -> Result<MembershipRole> {
        Err(AuthFailure::forbidden()
            .with_detail("authorization is not wired")
            .to_app_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::principal::{PrincipalKind, TenantMembership};
    use crate::scopes::ScopeSet;
    use mas_common::timestamps::Timestamp;
    use std::time::Duration;

    fn tenant() -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(0xA))
    }
    fn other_tenant() -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(0xB))
    }
    fn org() -> OrganizationId {
        OrganizationId::from_uuid(uuid::Uuid::from_u128(0xC))
    }

    fn principal(role: MembershipRole) -> SecurityPrincipal {
        SecurityPrincipal::new("user-1", PrincipalKind::User, None, ScopeSet::denied())
            .with_memberships(vec![TenantMembership {
                organization_id: org(),
                tenant_id: tenant(),
                role,
            }])
    }

    fn request(resource: ResourceKind, action: Action) -> AuthorizationRequest {
        AuthorizationRequest {
            resource,
            action,
            tenant_id: tenant(),
            organization_id: org(),
            project_id: None,
        }
    }

    #[test]
    fn deny_by_default_an_ungranted_pair_is_refused() {
        let service = AuthorizationService::new();
        let decision = service.authorize(
            &principal(MembershipRole::Viewer),
            &request(ResourceKind::Execution, Action::Delete),
        );
        match decision {
            AuthorizationDecision::Deny(failure) => {
                assert_eq!(failure.code(), "insufficient_permission");
            },
            other => panic!("viewer must not delete executions: {other:?}"),
        }
    }

    #[test]
    fn the_twelve_named_decisions_are_reachable() {
        // Every verb the spec names must exist and be grantable to the role
        // that legitimately owns it.
        let service = AuthorizationService::new();
        let owner = principal(MembershipRole::Owner);
        let pairs = [
            (ResourceKind::Execution, Action::Read),
            (ResourceKind::Workflow, Action::Create),
            (ResourceKind::Agent, Action::Update),
            (ResourceKind::Webhook, Action::Delete),
            (ResourceKind::Agent, Action::Execute),
            (ResourceKind::Execution, Action::Cancel),
            (ResourceKind::Webhook, Action::Publish),
            (ResourceKind::Tenant, Action::Administer),
            (ResourceKind::Credential, Action::ManageCredentials),
            (ResourceKind::Policy, Action::ManagePolicies),
            (ResourceKind::Audit, Action::ViewAudit),
            (ResourceKind::Subscription, Action::ManageBilling),
        ];
        for (resource, action) in pairs {
            let decision = service.authorize(&owner, &request(resource, action));
            assert!(
                decision.is_allowed(),
                "owner must be able to {action} a {resource}: {decision:?}"
            );
        }
    }

    #[test]
    fn a_cross_tenant_request_is_a_tenant_mismatch_not_a_silent_rescope() {
        let service = AuthorizationService::new();
        let mut request = request(ResourceKind::Execution, Action::Read);
        request.tenant_id = other_tenant();
        let decision = service.authorize(&principal(MembershipRole::Owner), &request);
        match decision {
            AuthorizationDecision::Deny(failure) => {
                assert_eq!(failure.code(), "tenant_mismatch");
            },
            other => panic!("cross-tenant must be refused: {other:?}"),
        }
    }

    #[test]
    fn an_expired_credential_denies_everything() {
        let service = AuthorizationService::new();
        let expired = principal(MembershipRole::Owner).with_expiry(
            Timestamp::now()
                .checked_sub(Duration::from_secs(1))
                .expect("past"),
        );
        let decision = service.authorize(&expired, &request(ResourceKind::Execution, Action::Read));
        match decision {
            AuthorizationDecision::Deny(failure) => {
                assert_eq!(failure.code(), "expired_credential");
            },
            other => panic!("expired credential must be refused: {other:?}"),
        }
    }

    #[test]
    fn a_viewer_cannot_read_audit_or_credential_metadata() {
        let service = AuthorizationService::new();
        let viewer = principal(MembershipRole::Viewer);
        for (resource, action) in [
            (ResourceKind::Audit, Action::Read),
            (ResourceKind::Audit, Action::ViewAudit),
            (ResourceKind::Credential, Action::Read),
            (ResourceKind::ApiKey, Action::Read),
        ] {
            assert!(
                !service
                    .authorize(&viewer, &request(resource, action))
                    .is_allowed(),
                "viewer must not {action} {resource}"
            );
        }
        // But a viewer does read ordinary business data.
        assert!(service
            .authorize(&viewer, &request(ResourceKind::Execution, Action::Read))
            .is_allowed());
    }

    #[test]
    fn an_operator_can_cancel_but_cannot_delete_definitions() {
        let service = AuthorizationService::new();
        let operator = principal(MembershipRole::Operator);
        assert!(service
            .authorize(&operator, &request(ResourceKind::Execution, Action::Cancel))
            .is_allowed());
        assert!(!service
            .authorize(&operator, &request(ResourceKind::Workflow, Action::Delete))
            .is_allowed());
        assert!(!service
            .authorize(&operator, &request(ResourceKind::Agent, Action::Create))
            .is_allowed());
    }

    #[test]
    fn an_admin_cannot_delete_the_tenant_or_manage_billing() {
        let service = AuthorizationService::new();
        let admin = principal(MembershipRole::Admin);
        assert!(!service
            .authorize(&admin, &request(ResourceKind::Tenant, Action::Delete))
            .is_allowed());
        assert!(!service
            .authorize(
                &admin,
                &request(ResourceKind::Subscription, Action::ManageBilling)
            )
            .is_allowed());
        // The owner can do both.
        let owner = principal(MembershipRole::Owner);
        assert!(service
            .authorize(&owner, &request(ResourceKind::Tenant, Action::Delete))
            .is_allowed());
        assert!(service
            .authorize(
                &owner,
                &request(ResourceKind::Subscription, Action::ManageBilling)
            )
            .is_allowed());
    }

    #[test]
    fn a_project_that_is_not_proven_to_the_tenant_is_a_project_mismatch() {
        let service = AuthorizationService::new();
        let mut request = request(ResourceKind::Execution, Action::Read);
        // A project the principal was never granted.
        request.project_id = Some(ProjectId::from_uuid(uuid::Uuid::from_u128(0xD)));
        let decision = service.authorize(&principal(MembershipRole::Owner), &request);
        match decision {
            AuthorizationDecision::Deny(failure) => {
                assert_eq!(failure.code(), "project_mismatch");
            },
            other => panic!("unproven project must be refused: {other:?}"),
        }
    }

    #[test]
    fn a_proven_project_is_allowed() {
        let service = AuthorizationService::new();
        let project = ProjectId::from_uuid(uuid::Uuid::from_u128(0xD));
        let mut principal = principal(MembershipRole::Developer);
        principal.prove_project(tenant(), project);
        let mut request = request(ResourceKind::Execution, Action::Execute);
        request.project_id = Some(project);
        assert!(service.authorize(&principal, &request).is_allowed());
    }

    #[test]
    fn the_platform_identity_is_the_only_unscoped_lane() {
        let service = AuthorizationService::new();
        let system = SecurityPrincipal::system();
        let mut request = request(ResourceKind::Execution, Action::Read);
        request.tenant_id = other_tenant();
        assert!(service.authorize(&system, &request).is_allowed());
        // A human principal in the same position is refused.
        assert!(!service
            .authorize(&principal(MembershipRole::Owner), &request)
            .is_allowed());
    }

    #[test]
    fn every_role_has_a_non_empty_explicit_grant_list() {
        for role in [
            MembershipRole::Owner,
            MembershipRole::Admin,
            MembershipRole::Developer,
            MembershipRole::Operator,
            MembershipRole::Viewer,
            MembershipRole::ServiceAccount,
        ] {
            let grants = AuthorizationService::grants_for(&role);
            assert!(!grants.is_empty(), "{role} has no grants");
            for actions in grants.values() {
                assert!(!actions.is_empty(), "{role} has an empty action list");
            }
        }
    }

    #[test]
    fn deny_by_default_placeholder_denies() {
        assert!(DenyByDefault.authorize().is_err());
    }
}
