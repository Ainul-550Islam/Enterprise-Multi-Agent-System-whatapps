//! PURPOSE: Thin authenticated HTTP adapters for tenant tool definitions and
//! agent tool-permission grants.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production responsibility: validate wire payloads, authorize with the
//! canonical matrix, make one `ToolService` call, and return stable envelopes.
//! Tool and project existence is resolved by the application store under the
//! caller's RLS scope.
//!
//! Data ownership: `ToolService` owns the use cases; this module owns no
//! durable state.
//!
//! MUST NOT: execute tools, perform SQL, resolve secrets, trust tenant headers,
//! or implement policy decisions in the handler.

use axum::extract::{Path, Query, State};
use axum::Json;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, ProjectId, ToolId};
use mas_contracts::tool::{
    RegisterToolRequest, ReplaceToolPermissionsRequest, ToolListQuery, ToolPageResponse,
    ToolPermissionGrantDto, ToolPermissionResponse, ToolResponse, UpdateToolRequest,
};
use mas_domain::{SafetyClassification, ToolKind, ToolPermission, ToolRuntime};

use crate::handlers::Ctx;
use crate::response::{created, ok, ApiError, ApiResult};
use crate::state::AppState;

/// `POST /v1/tools`.
pub async fn register(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(request): Json<RegisterToolRequest>,
) -> ApiResult<ToolResponse> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorize(&ctx, mas_security::Action::Create, request.project_id)?;
    let kind = request.kind.parse::<ToolKind>().map_err(|_| {
        ApiError::new(
            AppError::invalid_field("kind", "invalid_enum_value", "unknown tool kind"),
            &ctx,
        )
    })?;
    let safety = request
        .safety
        .parse::<SafetyClassification>()
        .map_err(|_| {
            ApiError::new(
                AppError::invalid_field(
                    "safety",
                    "invalid_enum_value",
                    "unknown safety classification",
                ),
                &ctx,
            )
        })?;
    let endpoint = request
        .endpoint_url
        .as_deref()
        .map(mas_domain::SafeUrl::parse)
        .transpose()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let runtime = ToolRuntime {
        endpoint,
        timeout_ms: request
            .timeout_ms
            .unwrap_or(mas_common::constants::DEFAULT_TOOL_TIMEOUT_MS),
        sandbox_required: kind == ToolKind::Code,
        settings: serde_json::Map::new(),
    };
    state
        .tool_service(&ctx)?
        .register(
            &service_context,
            mas_application::tool_service::RegisterTool {
                project_id: request.project_id,
                name: request.name,
                description: request.description,
                kind,
                input_schema: request.input_schema.0,
                output_schema: request.output_schema.map(|schema| schema.0),
                runtime,
                safety,
            },
        )
        .await
        .map(|tool| created(&ctx, tool_response(&tool)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/tools`.
pub async fn list(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<ToolListQuery>,
) -> ApiResult<ToolPageResponse> {
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorize(&ctx, mas_security::Action::Read, query.project_id)?;
    state
        .tool_service(&ctx)?
        .list_page(
            &service_context,
            query.project_id,
            query.cursor.as_deref(),
            query.page_limit(),
        )
        .await
        .map(|page| {
            ok(
                &ctx,
                ToolPageResponse {
                    items: page.items.iter().map(tool_response).collect(),
                    next_cursor: page.next_cursor,
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/tools/{id}`.
pub async fn get(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<ToolId>,
) -> ApiResult<ToolResponse> {
    let service_context = authorize(&ctx, mas_security::Action::Read, None)?;
    state
        .tool_service(&ctx)?
        .get(&service_context, id)
        .await
        .map(|tool| ok(&ctx, tool_response(&tool)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `PATCH /v1/tools/{id}`.
pub async fn update(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<ToolId>,
    Json(request): Json<UpdateToolRequest>,
) -> ApiResult<ToolResponse> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = authorize(&ctx, mas_security::Action::Update, None)?;
    let endpoint = request
        .endpoint_url
        .map(|value| {
            value
                .map(|url| mas_domain::SafeUrl::parse(&url))
                .transpose()
        })
        .transpose()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let safety = request
        .safety
        .map(|value| value.parse::<SafetyClassification>())
        .transpose()
        .map_err(|_| {
            ApiError::new(
                AppError::invalid_field(
                    "safety",
                    "invalid_enum_value",
                    "unknown safety classification",
                ),
                &ctx,
            )
        })?;
    state
        .tool_service(&ctx)?
        .update(
            &service_context,
            id,
            request.name,
            request.description,
            request.input_schema.map(|schema| schema.0),
            request
                .output_schema
                .map(|schema| schema.map(|value| value.0)),
            endpoint,
            request.timeout_ms,
            safety,
        )
        .await
        .map(|tool| ok(&ctx, tool_response(&tool)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `PUT /v1/agents/{agent_id}/tool-permissions`.
pub async fn replace_agent_permissions(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(agent_id): Path<AgentId>,
    Json(request): Json<ReplaceToolPermissionsRequest>,
) -> ApiResult<Vec<ToolPermissionResponse>> {
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_context = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Agent,
        mas_security::Action::Update,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    let grants = request
        .grants
        .into_iter()
        .map(|grant| permission_from_dto(agent_id, &ctx, grant))
        .collect::<Result<Vec<_>, _>>()?;
    state
        .tool_service(&ctx)?
        .replace_agent_permissions(&service_context, agent_id, grants)
        .await
        .map(|permissions| {
            ok(
                &ctx,
                permissions.into_iter().map(permission_response).collect(),
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

fn permission_from_dto(
    agent_id: AgentId,
    ctx: &crate::context::RequestContext,
    grant: ToolPermissionGrantDto,
) -> Result<ToolPermission, ApiError> {
    let scope = ctx
        .require_scope()
        .map_err(|error| ApiError::new(error, ctx))?;
    if grant.tool_id.is_nil() {
        return Err(ApiError::new(
            AppError::invalid_field("tool_id", "required", "tool id must not be nil"),
            ctx,
        ));
    }
    let role = grant
        .role
        .map(|value| value.parse::<mas_domain::MembershipRole>())
        .transpose()
        .map_err(|error| ApiError::new(error, ctx))?;
    ToolPermission::new(
        grant.tool_id,
        scope.tenant_id(),
        grant.project_id,
        Some(agent_id),
        role,
        grant.allowed_actions,
    )
    .map_err(|error| ApiError::new(error, ctx))
}

fn authorize(
    ctx: &crate::context::RequestContext,
    action: mas_security::Action,
    project: Option<ProjectId>,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::Tool,
        action,
        project,
    )
    .map_err(|error| ApiError::new(error, ctx))
}

fn permission_response(permission: ToolPermission) -> ToolPermissionResponse {
    ToolPermissionResponse {
        tool_id: permission.tool_id,
        tenant_id: permission.tenant_id,
        project_id: permission.project_id,
        agent_id: permission.agent_id,
        role: permission.role.map(|role| role.to_string()),
        allowed_actions: permission.allowed_actions,
        created_at: permission.created_at,
    }
}

fn tool_response(tool: &mas_domain::Tool) -> ToolResponse {
    ToolResponse {
        id: tool.id,
        tenant_id: tool.tenant_id,
        project_id: tool.project_id,
        name: tool.name.clone(),
        description: tool.description.clone(),
        kind: tool.kind.to_string(),
        safety: tool.safety.to_string(),
        status: tool.status,
        input_schema: mas_contracts::tool::ToolSchemaDto(tool.input_schema.clone()),
        output_schema: tool
            .output_schema
            .clone()
            .map(mas_contracts::tool::ToolSchemaDto),
        version: tool.version,
        created_at: tool.created_at,
        updated_at: tool.updated_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_mapping_never_serializes_runtime_endpoint_or_settings() {
        let mut tool = mas_domain::Tool::register(
            mas_common::ids::TenantId::new(),
            mas_common::ids::OrganizationId::new(),
            None,
            "metadata-only-tool",
            ToolKind::Internal,
            serde_json::json!({"type":"object","properties":{}}),
            None,
        )
        .expect("tool");
        tool.runtime.endpoint = Some(
            mas_domain::SafeUrl::parse("https://tools.example.invalid/run").expect("endpoint"),
        );
        tool.runtime.settings.insert(
            "credential_reference".to_owned(),
            serde_json::json!("vault/path"),
        );
        let json = serde_json::to_value(tool_response(&tool)).expect("response");
        assert!(json.get("runtime").is_none());
        assert!(json.get("endpoint").is_none());
        assert!(json.get("credential_reference").is_none());
    }
}
