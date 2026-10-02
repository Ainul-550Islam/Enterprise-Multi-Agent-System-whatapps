//! Tenancy/identity routes.

use axum::extract::State;
use axum::Json;
use mas_common::enums::Environment;
use mas_common::ids::UserId;
use mas_common::result::Result as MasResult;
use mas_domain::{IsolationMode, MembershipRole};
use serde::{Deserialize, Serialize};

use crate::context::RequestContext;
use crate::handlers::Ctx;
use crate::response::{created, ApiResult, IntoApiResult};
use crate::state::AppState;

// ---------------------------------------------------------------------------
// Request bodies (api-local wire DTOs; the stable machine contract for
// tenancy is the id set returned below).
// ---------------------------------------------------------------------------

/// `POST /v1/organizations` body.
#[derive(Debug, Deserialize)]
pub struct RegisterOrganizationBody {
    /// Legal entity name.
    pub legal_name: String,
    /// Human display name.
    pub display_name: String,
    /// Globally unique slug.
    pub slug: String,
}

/// `POST /v1/tenants` body.
#[derive(Debug, Deserialize)]
pub struct RegisterTenantBody {
    /// Tenant name.
    pub name: String,
    /// Org-unique slug.
    pub slug: String,
    /// `development | staging | production`.
    pub environment: String,
    /// `shared_rls | schema_per_tenant | dedicated_database`.
    pub isolation: String,
}

/// `POST /v1/projects` body.
#[derive(Debug, Deserialize)]
pub struct RegisterProjectBody {
    /// Project name.
    pub name: String,
    /// Project slug.
    pub slug: String,
}

/// `POST /v1/users` body.
#[derive(Debug, Deserialize)]
pub struct RegisterUserBody {
    /// Login email.
    pub email: String,
    /// Display name.
    pub display_name: String,
}

/// `POST /v1/memberships` body.
#[derive(Debug, Deserialize)]
pub struct InviteMembershipBody {
    /// Invited user id.
    pub user_id: UserId,
    /// `owner | admin | developer | operator | viewer | service_account`.
    pub role: String,
}

// ---------------------------------------------------------------------------
// Response DTOs
// ---------------------------------------------------------------------------

/// Organization summary on the wire.
#[derive(Debug, Clone, Serialize)]
pub struct OrganizationResponse {
    /// Id.
    pub id: String,
    /// Legal name.
    pub legal_name: String,
    /// Display name.
    pub display_name: String,
    /// Slug.
    pub slug: String,
    /// Status.
    pub status: String,
}

/// Tenant summary.
#[derive(Debug, Clone, Serialize)]
pub struct TenantResponse {
    /// Id.
    pub id: String,
    /// Owning organization id.
    pub organization_id: String,
    /// Name.
    pub name: String,
    /// Slug.
    pub slug: String,
    /// Environment.
    pub environment: String,
    /// Isolation mode.
    pub isolation: String,
    /// Status.
    pub status: String,
}

/// Project summary.
#[derive(Debug, Clone, Serialize)]
pub struct ProjectResponse {
    /// Id.
    pub id: String,
    /// Tenant id.
    pub tenant_id: String,
    /// Organization id.
    pub organization_id: String,
    /// Name.
    pub name: String,
    /// Slug.
    pub slug: String,
}

/// User summary.
#[derive(Debug, Clone, Serialize)]
pub struct UserResponse {
    /// Id.
    pub id: String,
    /// Email (public surface; no secrets exist on this type).
    pub email: String,
    /// Display name.
    pub display_name: String,
    /// Status.
    pub status: String,
}

/// Membership summary.
#[derive(Debug, Clone, Serialize)]
pub struct MembershipResponse {
    /// Id.
    pub id: String,
    /// User id.
    pub user_id: String,
    /// Tenant id (None for org-wide memberships).
    pub tenant_id: Option<String>,
    /// Roles.
    pub roles: Vec<String>,
    /// Status.
    pub status: String,
}

fn wl_enum<T: serde::de::DeserializeOwned>(field: &'static str, raw: &str) -> MasResult<T> {
    serde_json::from_value::<T>(serde_json::Value::String(raw.to_owned())).map_err(|_| {
        mas_common::error::AppError::invalid_field(field, "invalid", "not a recognized value")
    })
}

fn org_response(organization: &mas_domain::Organization) -> OrganizationResponse {
    OrganizationResponse {
        id: organization.id.to_string(),
        legal_name: organization.legal_name.clone(),
        display_name: organization.display_name.clone(),
        slug: organization.slug.as_str().to_owned(),
        status: organization.status.to_string(),
    }
}

fn tenant_response(tenant: &mas_domain::Tenant) -> TenantResponse {
    TenantResponse {
        id: tenant.id.to_string(),
        organization_id: tenant.organization_id().to_string(),
        name: tenant.name.clone(),
        slug: tenant.slug.as_str().to_owned(),
        environment: tenant.environment.to_string(),
        isolation: tenant.isolation.to_string(),
        status: tenant.status.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `POST /v1/organizations` — system-level registration (org routes do not
/// require scope headers).
pub async fn register_organization(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<RegisterOrganizationBody>,
) -> ApiResult<OrganizationResponse> {
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .tenancy
        .register_organization(
            &service_ctx,
            &body.legal_name,
            &body.display_name,
            &body.slug,
        )
        .await
        .map(|org| created(&ctx, org_response(&org)))
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `GET /v1/organizations`.
pub async fn list_organizations(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<OrganizationResponse>> {
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .tenancy
        .list_organizations(&service_ctx)
        .await
        .map(|orgs| orgs.iter().map(org_response).collect::<Vec<_>>())
        .into_api(&ctx)
}

/// `POST /v1/tenants` — requires `X-Organization-Id`.
pub async fn register_tenant(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<RegisterTenantBody>,
) -> ApiResult<TenantResponse> {
    let environment: Environment = wl_enum("environment", &body.environment)
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let isolation: IsolationMode = wl_enum("isolation", &body.isolation)
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .tenancy
        .register_tenant(&service_ctx, &body.name, &body.slug, environment, isolation)
        .await
        .map(|tenant| created(&ctx, tenant_response(&tenant)))
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `GET /v1/tenants` — requires `X-Organization-Id`.
pub async fn list_tenants(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<TenantResponse>> {
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .tenancy
        .list_tenants(&service_ctx)
        .await
        .map(|tenants| tenants.iter().map(tenant_response).collect::<Vec<_>>())
        .into_api(&ctx)
}

/// `POST /v1/projects` — requires both scope headers.
pub async fn register_project(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<RegisterProjectBody>,
) -> ApiResult<ProjectResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .tenancy
        .register_project(&service_ctx, &body.name, &body.slug)
        .await
        .map(|project| {
            created(
                &ctx,
                ProjectResponse {
                    id: project.id.to_string(),
                    tenant_id: project.tenant_id.to_string(),
                    organization_id: project.organization_id.to_string(),
                    name: project.name.clone(),
                    slug: project.slug.as_str().to_owned(),
                },
            )
        })
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `POST /v1/users` — open self-registration (authenticated at the service
/// level, system context).
pub async fn register_user(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<RegisterUserBody>,
) -> ApiResult<UserResponse> {
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .tenancy
        .register_user(&service_ctx, &body.email, &body.display_name)
        .await
        .map(|user| {
            created(
                &ctx,
                UserResponse {
                    id: user.id.to_string(),
                    email: user.email.to_string(),
                    display_name: user.display_name.clone(),
                    status: user.status.to_string(),
                },
            )
        })
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `POST /v1/memberships` — invite into the caller's tenant scope.
pub async fn invite_membership(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<InviteMembershipBody>,
) -> ApiResult<MembershipResponse> {
    scoped(&ctx)?;
    let role: MembershipRole =
        wl_enum("role", &body.role).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .tenancy
        .invite_membership(&service_ctx, body.user_id, role)
        .await
        .map(|membership| {
            created(
                &ctx,
                MembershipResponse {
                    id: membership.id.to_string(),
                    user_id: membership.user_id.to_string(),
                    tenant_id: membership.tenant_id.map(|t| t.to_string()),
                    roles: membership.roles.iter().map(|r| r.to_string()).collect(),
                    status: membership.status.to_string(),
                },
            )
        })
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

fn scoped(ctx: &RequestContext) -> std::result::Result<&RequestContext, crate::response::ApiError> {
    ctx.require_scope()
        .map_err(|e| crate::response::ApiError::new(e, ctx))
}
