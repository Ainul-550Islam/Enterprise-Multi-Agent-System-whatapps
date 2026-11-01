//! PURPOSE: Authenticated identity projection for the UI, using the server-resolved scope and canonical security matrix.
//!
//! Owning layer: `mas-api` transport edge.
//! Production responsibility: return a safe identity/permission hint for the current authenticated request.
//! Data ownership: identity and scope come from verified middleware state; no token or secret reference is returned.
//! MUST NOT: treat permissions as authorization, accept caller-supplied identity/scope, or expose credential material.

use axum::extract::State;
use mas_common::{
    error::AppError,
    ids::{ApiKeyId, UserId},
};
use mas_contracts::auth::{AuthMeResponse, AuthenticatedActor};
use mas_security::{Action, AuthorizationService, PrincipalKind, ResourceKind};

use crate::{
    handlers::Ctx,
    response::{ok, ApiError, ApiResult},
    state::AppState,
};

/// `GET /v1/auth/me` — the authenticated actor and current server-resolved scope.
pub async fn me(State(_state): State<AppState>, Ctx(ctx): Ctx) -> ApiResult<AuthMeResponse> {
    let principal = ctx
        .principal
        .as_ref()
        .ok_or_else(|| ApiError::new(AppError::unauthorized("authentication required"), &ctx))?;
    let actor = match principal.kind {
        PrincipalKind::User => AuthenticatedActor::User {
            user_id: principal.actor_id.parse::<UserId>().map_err(|_| {
                ApiError::new(
                    AppError::internal("authenticated user identity is malformed"),
                    &ctx,
                )
            })?,
        },
        PrincipalKind::ApiKey => AuthenticatedActor::ApiKey {
            // Producers mint the actor id as `api-key:<uuid>` (prod_auth and
            // the security mint path); the HTTP projection needs the bare id.
            api_key_id: principal
                .actor_id
                .strip_prefix("api-key:")
                .unwrap_or(&principal.actor_id)
                .parse::<ApiKeyId>()
                .map_err(|_| {
                    ApiError::new(
                        AppError::internal("authenticated API key identity is malformed"),
                        &ctx,
                    )
                })?,
        },
        _ => AuthenticatedActor::Service {
            service_name: principal.actor_id.clone(),
        },
    };
    let tenant_id = ctx.scope.as_ref().map(|scope| scope.tenant_id());
    let organization_id = ctx.scope.as_ref().map(|scope| scope.organization_id());
    let role = match (tenant_id, organization_id) {
        (Some(tenant), Some(org)) => principal.role_in(tenant, org),
        _ => None,
    };
    let mut roles = Vec::new();
    let mut permissions = Vec::new();
    if let Some(role) = role {
        roles.push(role.as_str().to_owned());
        for resource in all_resources() {
            for action in all_actions() {
                if AuthorizationService::role_permits(&role, *resource, *action) {
                    permissions.push(format!("{}:{}", resource.as_str(), action.as_str()));
                }
            }
        }
    }
    roles.sort();
    roles.dedup();
    permissions.sort();
    Ok(ok(
        &ctx,
        AuthMeResponse {
            subject: principal.actor_id.clone(),
            actor,
            tenant_id,
            organization_id,
            roles,
            permissions,
        },
    ))
}

fn all_resources() -> &'static [ResourceKind] {
    &[
        ResourceKind::Execution,
        ResourceKind::Task,
        ResourceKind::Workflow,
        ResourceKind::Agent,
        ResourceKind::Tool,
        ResourceKind::Connector,
        ResourceKind::Credential,
        ResourceKind::Policy,
        ResourceKind::Usage,
        ResourceKind::Audit,
        ResourceKind::Webhook,
        ResourceKind::Subscription,
        ResourceKind::ApiKey,
        ResourceKind::Schedule,
        ResourceKind::Notification,
        ResourceKind::Tenant,
        ResourceKind::Organization,
        ResourceKind::Project,
        ResourceKind::Membership,
        ResourceKind::DataSource,
        ResourceKind::Document,
    ]
}

fn all_actions() -> &'static [Action] {
    &[
        Action::Read,
        Action::Create,
        Action::Update,
        Action::Delete,
        Action::Execute,
        Action::Cancel,
        Action::Publish,
        Action::Administer,
        Action::ManageCredentials,
        Action::ManagePolicies,
        Action::ViewAudit,
        Action::ManageBilling,
        Action::ManageMembers,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_security::ScopeSet;

    #[test]
    fn permission_hints_are_computed_from_the_canonical_matrix() {
        let permissions = AuthorizationService::grants_for(&mas_domain::MembershipRole::Owner);
        assert!(permissions
            .get("audit")
            .is_some_and(|verbs| verbs.iter().any(|verb| verb == "view_audit")));
        let viewer_permissions =
            AuthorizationService::grants_for(&mas_domain::MembershipRole::Viewer);
        assert!(!viewer_permissions
            .get("audit")
            .is_some_and(|verbs| verbs.iter().any(|verb| verb == "view_audit")));
        assert!(!viewer_permissions
            .get("policy")
            .is_some_and(|verbs| verbs.iter().any(|verb| verb == "manage_policies")));
        let principal = mas_security::SecurityPrincipal::new(
            "service",
            PrincipalKind::ServiceAccount,
            None,
            ScopeSet::denied(),
        );
        assert!(principal.roles.is_empty());
    }
}
