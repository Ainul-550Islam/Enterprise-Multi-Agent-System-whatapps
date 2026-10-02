//! Agent routes.

use axum::extract::{Path, Query, State};
use axum::Json;
use mas_common::ids::{AgentId, UserId};
use mas_contracts::agent::{AgentResponse, CreateAgentRequest};
use mas_domain::AgentKind;
use serde::Deserialize;

use crate::handlers::Ctx;
use crate::response::{created, ApiResult, IntoApiResult};
use crate::state::AppState;

/// `POST /v1/agents` — registers a `Draft` agent (body: stable contract
/// `CreateAgentRequest`; slug uniqueness is per project).
pub async fn register(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<CreateAgentRequest>,
) -> ApiResult<serde_json::Value> {
    scoped(&ctx)?;
    body.validate()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let kind = serde_json::from_value::<AgentKind>(serde_json::Value::String(body.kind.clone()))
        .map_err(|_| {
            crate::response::ApiError::new(
                mas_common::error::AppError::invalid_field(
                    "kind",
                    "invalid",
                    "expected standard | supervisor | tool_user",
                ),
                &ctx,
            )
        })?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .agents
        .register(&service_ctx, body.project_id, &body.name, &body.slug, kind)
        .await
        .map(|agent| {
            created(
                &ctx,
                serde_json::to_value(mas_application::dto::agent_response(&agent)) // dto has no api deps
                    .expect("serializable"),
            )
        })
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `POST /v1/agents/{agent_id}/versions` body.
#[derive(Debug, Deserialize)]
pub struct PublishVersionBody {
    /// Canonical lowercase-hex SHA-256 of the versioned config (64 chars).
    pub configuration_checksum: String,
    /// Versioned orchestration snapshot (opaque config + capabilities).
    pub configuration_snapshot: serde_json::Value,
    /// Who published (defaults to the authenticated subject).
    #[serde(default)]
    pub published_by: Option<UserId>,
}

/// `POST /v1/agents/{agent_id}/versions` — publishes the next immutable
/// version (monotonic, checksum-validated by the domain).
pub async fn publish_version(
    State(state): State<AppState>,
    Path(agent_id): Path<AgentId>,
    Ctx(ctx): Ctx,
    Json(body): Json<PublishVersionBody>,
) -> ApiResult<serde_json::Value> {
    scoped(&ctx)?;
    let published_by = match body.published_by {
        Some(user) => user,
        None => mas_common::ids::UserId::parse_str(&ctx.principal.subject).map_err(|_| {
            crate::response::ApiError::new(
                mas_common::error::AppError::invalid_field(
                    "published_by",
                    "invalid",
                    "provide an explicit user id (subject is not a uuid)",
                ),
                &ctx,
            )
        })?,
    };
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .agents
        .publish_version(
            &service_ctx,
            agent_id,
            &body.configuration_checksum,
            body.configuration_snapshot,
            published_by,
        )
        .await
        .map(|version| {
            created(
                &ctx,
                serde_json::json!({
                    "id": version.id().to_string(),
                    "agent_id": version.agent_id().to_string(),
                    "version_number": version.version_number(),
                    "configuration_checksum": version.configuration_checksum(),
                    "deployment_status": version.deployment_status().to_string(),
                    "published_by": version.published_by().to_string(),
                }),
            )
        })
        .map_err(|e| crate::response::ApiError::new(e, &ctx))
}

/// `GET /v1/agents/{agent_id}` — wire `AgentResponse` (foreign tenants 404).
pub async fn get(
    State(state): State<AppState>,
    Path(agent_id): Path<AgentId>,
    Ctx(ctx): Ctx,
) -> ApiResult<AgentResponse> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    state
        .agents
        .get_response(&service_ctx, agent_id)
        .await
        .into_api(&ctx)
}

/// List query: `GET /v1/agents?project_id=…`.
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// Project within scope.
    pub project_id: mas_common::ids::ProjectId,
}

/// `GET /v1/agents`.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<AgentResponse>> {
    scoped(&ctx)?;
    let service_ctx = ctx
        .service_context()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let agents = state
        .agents
        .list(&service_ctx, query.project_id)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let responses = agents
        .iter()
        .map(mas_application::dto::agent_response)
        .collect::<Vec<_>>();
    Ok(crate::response::ok(&ctx, responses))
}

fn scoped(
    ctx: &crate::context::RequestContext,
) -> std::result::Result<(), crate::response::ApiError> {
    ctx.require_scope()
        .map(|_| ())
        .map_err(|e| crate::response::ApiError::new(e, ctx))
}
