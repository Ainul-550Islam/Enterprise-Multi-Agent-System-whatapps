//! Tonic gRPC services for `mas.api.v1` — exact semantic mirror of the
//! HTTP handlers (same context construction, same use-cases, same error
//! surface). Metadata keys equal the HTTP header names.

use mas_application::context::ServiceContext;
use mas_common::error::AppError;
use mas_common::ids::{AgentId, OrganizationId, ProjectId, TenantId, UserId, WorkflowId};
use mas_common::result::Result as MasResult;
use tonic::{Code, Request, Response, Status};

use crate::auth::{bearer_token, TokenVerifierPort};
use crate::context::{RequestContext, CORRELATION_HEADER, ORGANIZATION_HEADER, TENANT_HEADER};
use crate::state::AppState;

pub mod gen {
    //! Generated `mas.api.v1` stubs (build.rs).
    tonic::include_proto!("mas.api.v1");
}

// ---------------------------------------------------------------------------
// Transport glue
// ---------------------------------------------------------------------------

fn meta_str<'a>(metadata: &'a tonic::metadata::MetadataMap, key: &str) -> Option<&'a str> {
    metadata.get(key).and_then(|value| value.to_str().ok())
}

#[allow(clippy::result_large_err)] // tonic statuses are the wire error; boxing them changes semantics
fn to_status(err: AppError) -> Status {
    let code = match &err {
        AppError::Validation { .. } => Code::InvalidArgument,
        AppError::NotFound { .. } => Code::NotFound,
        AppError::Unauthorized(_) => Code::Unauthenticated,
        AppError::Forbidden(_) => Code::PermissionDenied,
        AppError::Conflict(_) => Code::AlreadyExists,
        AppError::RateLimited(_) => Code::ResourceExhausted,
        AppError::Timeout(_) => Code::DeadlineExceeded,
        AppError::Cancelled(_) => Code::Cancelled,
        AppError::ExternalService { .. } => Code::Unavailable,
        _ => Code::Internal,
    };
    tracing::warn!(code = %err.error_code(), grpc_code = ?code, "grpc call failed");
    Status::new(code, err.public_message())
}

#[allow(clippy::result_large_err)]
fn parse_id<T: mas_common::ids::TypedId>(field: &'static str, raw: &str) -> Result<T, Status> {
    uuid::Uuid::parse_str(raw)
        .map(T::from_uuid)
        .map_err(|_| Status::new(Code::InvalidArgument, format!("{field}: not a valid uuid")))
}

/// Builds a verified [`RequestContext`] from grpc metadata (auth + scope +
/// correlation), exactly mirroring the HTTP middleware.
#[allow(clippy::result_large_err)] // tonic idiom: Status is the transport error
async fn context_from(
    state: &AppState,
    metadata: &tonic::metadata::MetadataMap,
) -> Result<RequestContext, Status> {
    let token = bearer_token(meta_str(metadata, "authorization")).map_err(to_status)?;
    let Some(token) = token else {
        return Err(Status::new(Code::Unauthenticated, "missing bearer token"));
    };
    let principal = verify(state.verifier.as_ref(), &token)
        .await
        .map_err(to_status)?;
    let tenant =
        RequestContext::parse_scope_header(TENANT_HEADER, meta_str(metadata, TENANT_HEADER))
            .map_err(to_status)?
            .map(TenantId::from_uuid);
    let organization = RequestContext::parse_scope_header(
        ORGANIZATION_HEADER,
        meta_str(metadata, ORGANIZATION_HEADER),
    )
    .map_err(to_status)?
    .map(OrganizationId::from_uuid);
    Ok(RequestContext {
        principal,
        tenant_id: tenant,
        organization_id: organization,
        request_id: uuid::Uuid::now_v7(),
        correlation_id: RequestContext::correlation_or_generated(meta_str(
            metadata,
            CORRELATION_HEADER,
        )),
    })
}

async fn verify(
    verifier: &dyn TokenVerifierPort,
    token: &str,
) -> MasResult<crate::auth::Principal> {
    verifier.verify(token).await
}

#[allow(clippy::result_large_err)]
fn scoped(ctx: &RequestContext) -> Result<ServiceContext, Status> {
    let ctx = ctx.require_scope().map_err(to_status)?;
    ctx.service_context().map_err(to_status)
}

#[allow(clippy::result_large_err)]
fn unscoped(ctx: &RequestContext) -> Result<ServiceContext, Status> {
    ctx.service_context().map_err(to_status)
}

#[allow(clippy::result_large_err)]
fn parse_str_id(field: &'static str, raw: &str) -> Result<uuid::Uuid, Status> {
    uuid::Uuid::parse_str(raw)
        .map_err(|_| Status::new(Code::InvalidArgument, format!("{field}: not a valid uuid")))
}
fn execution_reply(execution: &mas_domain::Execution, replayed: bool) -> gen::ExecutionReply {
    gen::ExecutionReply {
        execution_id: execution.id.to_string(),
        status: execution.status.to_string(),
        tenant_id: execution.tenant_id.to_string(),
        workflow_id: execution.workflow_id.map(|id| id.to_string()),
        agent_id: execution.agent_id.map(|id| id.to_string()),
        root_execution_id: execution.root_execution_id.to_string(),
        correlation_id: execution.correlation_id.clone(),
        cancellation_requested: execution.cancellation_requested(),
        replayed,
        failure: execution.failure.clone().unwrap_or_default(),
        output_json: execution
            .output
            .as_ref()
            .map(|value| value.to_string())
            .unwrap_or_default(),
    }
}

fn agent_reply(agent: &mas_domain::Agent) -> gen::AgentReply {
    gen::AgentReply {
        id: agent.id.to_string(),
        project_id: agent.project_id.to_string(),
        tenant_id: agent.tenant_id.to_string(),
        name: agent.name.clone(),
        slug: agent.slug.as_str().to_owned(),
        kind: agent.kind.to_string(),
        status: agent.status.to_string(),
        current_version: agent.current_version.unwrap_or(0),
    }
}

// ---------------------------------------------------------------------------
// Service bundle
// ---------------------------------------------------------------------------

/// Holds everything a grpc method needs (identity + the five services).
#[derive(Clone)]
pub struct GrpcServices {
    state: AppState,
}

impl std::fmt::Debug for GrpcServices {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcServices").finish_non_exhaustive()
    }
}

impl GrpcServices {
    /// Wraps the shared application state.
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    /// Generated server descriptor: tenancy.
    pub fn tenancy_server(&self) -> gen::tenancy_api_server::TenancyApiServer<Self> {
        gen::tenancy_api_server::TenancyApiServer::new(self.clone())
    }

    /// Generated server descriptor: agents.
    pub fn agent_server(&self) -> gen::agent_api_server::AgentApiServer<Self> {
        gen::agent_api_server::AgentApiServer::new(self.clone())
    }

    /// Generated server descriptor: workflows.
    pub fn workflow_server(&self) -> gen::workflow_api_server::WorkflowApiServer<Self> {
        gen::workflow_api_server::WorkflowApiServer::new(self.clone())
    }

    /// Generated server descriptor: executions.
    pub fn execution_server(&self) -> gen::execution_api_server::ExecutionApiServer<Self> {
        gen::execution_api_server::ExecutionApiServer::new(self.clone())
    }

    /// Generated server descriptor: schedules.
    pub fn schedule_server(&self) -> gen::schedule_api_server::ScheduleApiServer<Self> {
        gen::schedule_api_server::ScheduleApiServer::new(self.clone())
    }
}

// ---------------------------------------------------------------------------
// TenancyApi
// ---------------------------------------------------------------------------
#[tonic::async_trait]
impl gen::tenancy_api_server::TenancyApi for GrpcServices {
    async fn register_organization(
        &self,
        request: Request<gen::RegisterOrganizationRequest>,
    ) -> Result<Response<gen::OrganizationReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = unscoped(&ctx)?;
        let msg = request.into_inner();
        let organization = self
            .state
            .tenancy
            .register_organization(&service_ctx, &msg.legal_name, &msg.display_name, &msg.slug)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::OrganizationReply {
            id: organization.id.to_string(),
            legal_name: organization.legal_name.clone(),
            display_name: organization.display_name.clone(),
            slug: organization.slug.as_str().to_owned(),
        }))
    }

    async fn list_organizations(
        &self,
        request: Request<gen::Empty>,
    ) -> Result<Response<gen::ListOrganizationsReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = unscoped(&ctx)?;
        let organizations = self
            .state
            .tenancy
            .list_organizations(&service_ctx)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ListOrganizationsReply {
            organizations: organizations
                .iter()
                .map(|org| gen::OrganizationReply {
                    id: org.id.to_string(),
                    legal_name: org.legal_name.clone(),
                    display_name: org.display_name.clone(),
                    slug: org.slug.as_str().to_owned(),
                })
                .collect(),
        }))
    }

    async fn register_tenant(
        &self,
        request: Request<gen::RegisterTenantRequest>,
    ) -> Result<Response<gen::TenantReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = unscoped(&ctx)?;
        let msg = request.into_inner();
        let environment = serde_json::from_value::<mas_common::enums::Environment>(
            serde_json::Value::String(msg.environment.clone()),
        )
        .map_err(|_| {
            Status::new(
                Code::InvalidArgument,
                "environment: development | staging | production",
            )
        })?;
        let isolation = serde_json::from_value::<mas_domain::IsolationMode>(
            serde_json::Value::String(msg.isolation.clone()),
        )
        .map_err(|_| {
            Status::new(
                Code::InvalidArgument,
                "isolation: shared_rls | schema_per_tenant | dedicated_database",
            )
        })?;
        let tenant = self
            .state
            .tenancy
            .register_tenant(&service_ctx, &msg.name, &msg.slug, environment, isolation)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::TenantReply {
            id: tenant.id.to_string(),
            name: tenant.name.clone(),
            slug: tenant.slug.as_str().to_owned(),
            environment: tenant.environment.to_string(),
            isolation: tenant.isolation.to_string(),
        }))
    }

    async fn list_tenants(
        &self,
        request: Request<gen::Empty>,
    ) -> Result<Response<gen::ListTenantsReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = unscoped(&ctx)?;
        let tenants = self
            .state
            .tenancy
            .list_tenants(&service_ctx)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ListTenantsReply {
            tenants: tenants
                .iter()
                .map(|tenant| gen::TenantReply {
                    id: tenant.id.to_string(),
                    name: tenant.name.clone(),
                    slug: tenant.slug.as_str().to_owned(),
                    environment: tenant.environment.to_string(),
                    isolation: tenant.isolation.to_string(),
                })
                .collect(),
        }))
    }

    async fn register_project(
        &self,
        request: Request<gen::RegisterProjectRequest>,
    ) -> Result<Response<gen::ProjectReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let project = self
            .state
            .tenancy
            .register_project(&service_ctx, &msg.name, &msg.slug)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ProjectReply {
            id: project.id.to_string(),
            name: project.name.clone(),
            slug: project.slug.as_str().to_owned(),
        }))
    }

    async fn invite_membership(
        &self,
        request: Request<gen::InviteMembershipRequest>,
    ) -> Result<Response<gen::MembershipReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let user_id: UserId = parse_id("user_id", &msg.user_id)?;
        let role = serde_json::from_value::<mas_domain::MembershipRole>(serde_json::Value::String(
            msg.role.clone(),
        ))
        .map_err(|_| {
            Status::new(
                Code::InvalidArgument,
                "role: owner | admin | developer | operator | viewer | service_account",
            )
        })?;
        let membership = self
            .state
            .tenancy
            .invite_membership(&service_ctx, user_id, role)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::MembershipReply {
            id: membership.id.to_string(),
            user_id: membership.user_id.to_string(),
            role: membership
                .roles
                .iter()
                .next()
                .map(ToString::to_string)
                .unwrap_or_default(),
            status: membership.status.to_string(),
        }))
    }
}

// ---------------------------------------------------------------------------
// AgentApi
// ---------------------------------------------------------------------------
#[tonic::async_trait]
impl gen::agent_api_server::AgentApi for GrpcServices {
    async fn register_agent(
        &self,
        request: Request<gen::RegisterAgentRequest>,
    ) -> Result<Response<gen::AgentReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let project_id: ProjectId = parse_id("project_id", &msg.project_id)?;
        let kind = serde_json::from_value::<mas_domain::AgentKind>(serde_json::Value::String(
            msg.kind.clone(),
        ))
        .map_err(|_| {
            Status::new(
                Code::InvalidArgument,
                "kind: standard | supervisor | tool_user",
            )
        })?;
        let agent = self
            .state
            .agents
            .register(&service_ctx, project_id, &msg.name, &msg.slug, kind)
            .await
            .map_err(to_status)?;
        Ok(Response::new(agent_reply(&agent)))
    }

    async fn publish_agent_version(
        &self,
        request: Request<gen::PublishAgentVersionRequest>,
    ) -> Result<Response<gen::AgentVersionReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let agent_id: AgentId = parse_id("agent_id", &msg.agent_id)?;
        let user_id: UserId = parse_id("published_by", &msg.published_by)?;
        let snapshot: serde_json::Value = serde_json::from_str(&msg.configuration_snapshot_json)
            .map_err(|_| {
                Status::new(
                    Code::InvalidArgument,
                    "configuration_snapshot_json: not valid JSON",
                )
            })?;
        let version = self
            .state
            .agents
            .publish_version(
                &service_ctx,
                agent_id,
                &msg.configuration_checksum,
                snapshot,
                user_id,
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::AgentVersionReply {
            id: version.id().to_string(),
            version_number: version.version_number(),
            checksum: version.configuration_checksum().to_owned(),
        }))
    }

    async fn get_agent(
        &self,
        request: Request<gen::GetAgentRequest>,
    ) -> Result<Response<gen::AgentReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let agent_id: AgentId = parse_id("agent_id", &request.into_inner().agent_id)?;
        let agent = self
            .state
            .agents
            .get(&service_ctx, agent_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(agent_reply(&agent)))
    }

    async fn list_agents(
        &self,
        request: Request<gen::ListAgentsRequest>,
    ) -> Result<Response<gen::ListAgentsReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let project_id: ProjectId = parse_id("project_id", &request.into_inner().project_id)?;
        let agents = self
            .state
            .agents
            .list(&service_ctx, project_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ListAgentsReply {
            agents: agents.iter().map(agent_reply).collect(),
        }))
    }
}

// ---------------------------------------------------------------------------
// WorkflowApi
// ---------------------------------------------------------------------------
#[tonic::async_trait]
impl gen::workflow_api_server::WorkflowApi for GrpcServices {
    async fn create_workflow(
        &self,
        request: Request<gen::CreateWorkflowRequest>,
    ) -> Result<Response<gen::WorkflowReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let project_id: ProjectId = parse_id("project_id", &msg.project_id)?;
        let workflow = self
            .state
            .workflows
            .create(&service_ctx, project_id, &msg.name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::WorkflowReply {
            id: workflow.id.to_string(),
            project_id: workflow.project_id.to_string(),
            name: workflow.name.clone(),
            node_count: u32::try_from(workflow.graph.nodes.len()).unwrap_or(u32::MAX),
            graph_version: workflow.published_version.unwrap_or(0),
        }))
    }

    async fn update_workflow_graph(
        &self,
        request: Request<gen::UpdateWorkflowGraphRequest>,
    ) -> Result<Response<gen::UpdateWorkflowGraphReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let workflow_id: WorkflowId = parse_id("workflow_id", &msg.workflow_id)?;
        let mut nodes = Vec::with_capacity(msg.nodes.len());
        for node in &msg.nodes {
            let node_type = serde_json::from_value::<mas_domain::WorkflowNodeType>(
                serde_json::Value::String(node.node_type.clone()),
            )
            .map_err(|_| {
                Status::new(
                    Code::InvalidArgument,
                    "node_type: not a recognized node type",
                )
            })?;
            let config: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(&node.config_json).map_err(|_| {
                    Status::new(Code::InvalidArgument, "config_json: not a JSON object")
                })?;
            let mut built = mas_domain::WorkflowNode::new(&node.node_key, node_type, &node.name)
                .map_err(to_status)?;
            built.config = config;
            nodes.push(built);
        }
        let mut edges = Vec::with_capacity(msg.edges.len());
        for edge in &msg.edges {
            edges.push(
                mas_domain::WorkflowEdge::new(&edge.from_key, &edge.to_key).map_err(to_status)?,
            );
        }
        let order = self
            .state
            .workflows
            .update_graph(&service_ctx, workflow_id, nodes, edges)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::UpdateWorkflowGraphReply {
            workflow_id: workflow_id.to_string(),
            topological_order: order,
        }))
    }

    async fn get_workflow(
        &self,
        request: Request<gen::GetWorkflowRequest>,
    ) -> Result<Response<gen::WorkflowReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let workflow_id: WorkflowId = parse_id("workflow_id", &request.into_inner().workflow_id)?;
        let workflow = self
            .state
            .workflows
            .get(&service_ctx, workflow_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::WorkflowReply {
            id: workflow.id.to_string(),
            project_id: workflow.project_id.to_string(),
            name: workflow.name.clone(),
            node_count: u32::try_from(workflow.graph.nodes.len()).unwrap_or(u32::MAX),
            graph_version: workflow.published_version.unwrap_or(0),
        }))
    }

    async fn list_workflows(
        &self,
        request: Request<gen::ListWorkflowsRequest>,
    ) -> Result<Response<gen::ListWorkflowsReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let project_id: ProjectId = parse_id("project_id", &request.into_inner().project_id)?;
        let workflows = self
            .state
            .workflows
            .list(&service_ctx, project_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ListWorkflowsReply {
            workflows: workflows
                .iter()
                .map(|workflow| gen::WorkflowReply {
                    id: workflow.id.to_string(),
                    project_id: workflow.project_id.to_string(),
                    name: workflow.name.clone(),
                    node_count: u32::try_from(workflow.graph.nodes.len()).unwrap_or(u32::MAX),
                    graph_version: workflow.published_version.unwrap_or(0),
                })
                .collect(),
        }))
    }
}

// ---------------------------------------------------------------------------
// ExecutionApi
// ---------------------------------------------------------------------------
#[tonic::async_trait]
impl gen::execution_api_server::ExecutionApi for GrpcServices {
    async fn submit_execution(
        &self,
        request: Request<gen::SubmitExecutionRequest>,
    ) -> Result<Response<gen::ExecutionReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let idempotency_key = meta_str(request.metadata(), crate::context::IDEMPOTENCY_HEADER)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned);
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let project_id: ProjectId = parse_id("project_id", &msg.project_id)?;
        let workflow_id: Option<WorkflowId> = match msg.workflow_id {
            Some(raw) => Some(parse_id("workflow_id", &raw)?),
            None => None,
        };
        let agent_id: Option<AgentId> = match msg.agent_id {
            Some(raw) => Some(parse_id("agent_id", &raw)?),
            None => None,
        };
        let input: serde_json::Value = serde_json::from_str(&msg.input_json)
            .map_err(|_| Status::new(Code::InvalidArgument, "input_json: not valid JSON"))?;
        let outcome = self
            .state
            .executions
            .submit(
                &service_ctx,
                project_id,
                workflow_id,
                agent_id,
                input,
                idempotency_key.as_deref(),
            )
            .await
            .map_err(to_status)?;
        Ok(Response::new(execution_reply(
            &outcome.execution,
            outcome.replayed,
        )))
    }

    async fn transition_execution(
        &self,
        request: Request<gen::TransitionExecutionRequest>,
    ) -> Result<Response<gen::ExecutionReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let execution_id: mas_common::ids::ExecutionId =
            parse_id("execution_id", &msg.execution_id)?;
        let action = gen::TransitionAction::try_from(msg.action)
            .unwrap_or(gen::TransitionAction::Unspecified);
        let execution = match action {
            gen::TransitionAction::Start => {
                self.state
                    .executions
                    .start(&service_ctx, execution_id)
                    .await
            },
            gen::TransitionAction::Pause => {
                self.state
                    .executions
                    .pause(&service_ctx, execution_id)
                    .await
            },
            gen::TransitionAction::Resume => {
                self.state
                    .executions
                    .resume(&service_ctx, execution_id)
                    .await
            },
            gen::TransitionAction::Cancel => {
                self.state
                    .executions
                    .request_cancellation(&service_ctx, execution_id)
                    .await
            },
            gen::TransitionAction::Unspecified => {
                return Err(Status::new(Code::InvalidArgument, "action is required"));
            },
        }
        .map_err(to_status)?;
        Ok(Response::new(execution_reply(&execution, false)))
    }

    async fn get_execution(
        &self,
        request: Request<gen::GetExecutionRequest>,
    ) -> Result<Response<gen::ExecutionReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let execution_id: mas_common::ids::ExecutionId =
            parse_id("execution_id", &request.into_inner().execution_id)?;
        let execution = self
            .state
            .executions
            .get(&service_ctx, execution_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(execution_reply(&execution, false)))
    }

    async fn list_executions(
        &self,
        request: Request<gen::Empty>,
    ) -> Result<Response<gen::ListExecutionsReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let executions = self
            .state
            .executions
            .list(&service_ctx)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ListExecutionsReply {
            executions: executions
                .iter()
                .map(|e| execution_reply(e, false))
                .collect(),
        }))
    }
}

// ---------------------------------------------------------------------------
// ScheduleApi
// ---------------------------------------------------------------------------
#[tonic::async_trait]
impl gen::schedule_api_server::ScheduleApi for GrpcServices {
    async fn register_schedule(
        &self,
        request: Request<gen::RegisterScheduleRequest>,
    ) -> Result<Response<gen::ScheduleReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let project_id = match msg.project_id {
            Some(raw) => Some(parse_id::<ProjectId>("project_id", &raw)?),
            None => None,
        };
        let kind = match msg.kind.as_str() {
            "interval" => mas_domain::ScheduleKind::Interval {
                every_seconds: msg.every_seconds,
            },
            other => {
                return Err(Status::new(
                    Code::InvalidArgument,
                    format!("unsupported schedule kind '{other}' (interval)"),
                ));
            },
        };
        let template: serde_json::Value =
            serde_json::from_str(&msg.task_template_json).map_err(|_| {
                Status::new(Code::InvalidArgument, "task_template_json: not valid JSON")
            })?;
        let schedule = self
            .state
            .schedules
            .register(&service_ctx, project_id, &msg.name, kind, template)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ScheduleReply {
            id: schedule.id.to_string(),
            name: schedule.name.clone(),
            status: schedule.status.to_string(),
        }))
    }

    async fn set_schedule_state(
        &self,
        request: Request<gen::SetScheduleStateRequest>,
    ) -> Result<Response<gen::ScheduleReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let msg = request.into_inner();
        let schedule_id = parse_str_id("schedule_id", &msg.schedule_id)
            .map(mas_common::ids::ScheduleId::from_uuid)?;
        let schedule = match gen::ScheduleStateAction::try_from(msg.action) {
            Ok(gen::ScheduleStateAction::Pause) => {
                self.state.schedules.pause(&service_ctx, schedule_id).await
            },
            Ok(gen::ScheduleStateAction::Resume) => {
                self.state.schedules.resume(&service_ctx, schedule_id).await
            },
            Ok(gen::ScheduleStateAction::Disable) => {
                self.state
                    .schedules
                    .disable(&service_ctx, schedule_id)
                    .await
            },
            _ => return Err(Status::new(Code::InvalidArgument, "action is required")),
        }
        .map_err(to_status)?;
        Ok(Response::new(gen::ScheduleReply {
            id: schedule.id.to_string(),
            name: schedule.name.clone(),
            status: schedule.status.to_string(),
        }))
    }

    async fn list_schedules(
        &self,
        request: Request<gen::Empty>,
    ) -> Result<Response<gen::ListSchedulesReply>, Status> {
        let ctx = context_from(&self.state, request.metadata()).await?;
        let service_ctx = scoped(&ctx)?;
        let schedules = self
            .state
            .schedules
            .list(&service_ctx)
            .await
            .map_err(to_status)?;
        Ok(Response::new(gen::ListSchedulesReply {
            schedules: schedules
                .iter()
                .map(|schedule| gen::ScheduleReply {
                    id: schedule.id.to_string(),
                    name: schedule.name.clone(),
                    status: schedule.status.to_string(),
                })
                .collect(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Tests (in-process, no sockets — the tonic service traits are plain async)
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::inmemory_state;
    use mas_common::timestamps::Timestamp;
    use tonic::metadata::MetadataMap;

    fn authed_metadata(tenant: TenantId, organization: OrganizationId) -> MetadataMap {
        let mut metadata = MetadataMap::new();
        metadata.insert("x-tenant-id", tenant.to_string().parse().expect("meta"));
        metadata.insert(
            "x-organization-id",
            organization.to_string().parse().expect("meta"),
        );
        metadata.insert("x-correlation-id", "grpc-it-1".parse().expect("meta"));
        metadata.insert(
            tonic::metadata::MetadataKey::from_static("authorization"),
            tonic::metadata::MetadataValue::from_static("Bearer test-token"),
        );
        metadata
    }

    async fn seed() -> (AppState, TenantId, OrganizationId, ProjectId) {
        let state = inmemory_state();
        let system =
            ServiceContext::for_service("seed", "grpc-seed-1", &Timestamp::now()).expect("ctx");
        let organization = state
            .tenancy
            .register_organization(&system, "Acme Ltd.", "Acme", "acme-grpc")
            .await
            .expect("org");
        let org_scoped = system.clone().with_scope(TenantId::new(), organization.id);
        let tenant = state
            .tenancy
            .register_tenant(
                &org_scoped,
                "Prod",
                "prod-grpc",
                mas_common::enums::Environment::Production,
                mas_domain::IsolationMode::SharedRls,
            )
            .await
            .expect("tenant");
        let tenant_scoped = system.clone().with_scope(tenant.id, organization.id);
        let project = state
            .tenancy
            .register_project(&tenant_scoped, "Core", "core-grpc")
            .await
            .expect("project");
        (state, tenant.id, organization.id, project.id)
    }

    #[tokio::test]
    async fn unauthenticated_calls_fail_closed() {
        let (state, tenant, organization, _project) = seed().await;
        let services = GrpcServices::new(state);

        let mut metadata = authed_metadata(tenant, organization);
        metadata.remove("authorization");
        let request = Request::from_parts(metadata, tonic::Extensions::default(), gen::Empty {});
        use gen::execution_api_server::ExecutionApi;
        let denial = services.list_executions(request).await.expect_err("401");
        assert_eq!(denial.code(), Code::Unauthenticated);

        // Malformed scope uuid → InvalidArgument, not a panic.
        let mut metadata = authed_metadata(tenant, organization);
        metadata.insert("x-tenant-id", "not-a-uuid".parse().expect("meta"));
        let request = Request::from_parts(metadata, tonic::Extensions::default(), gen::Empty {});
        let bad_scope = services
            .list_executions(request)
            .await
            .expect_err("invalid argument");
        assert_eq!(bad_scope.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn foreign_scope_hides_existence() {
        let (state, tenant, organization, _project) = seed().await;
        let tenant_scoped = ServiceContext::for_service("seed", "grpc-seed-2", &Timestamp::now())
            .expect("ctx")
            .with_scope(tenant, organization);
        let schedule = state
            .schedules
            .register(
                &tenant_scoped,
                None,
                "nightly",
                mas_domain::ScheduleKind::Interval {
                    every_seconds: 3_600,
                },
                serde_json::json!({"wf": "x"}),
            )
            .await
            .expect("register");
        let services = GrpcServices::new(state);

        let foreign_metadata = authed_metadata(TenantId::new(), organization);
        let request = Request::from_parts(
            foreign_metadata,
            tonic::Extensions::default(),
            gen::SetScheduleStateRequest {
                schedule_id: schedule.id.to_string(),
                action: gen::ScheduleStateAction::Pause as i32,
            },
        );
        use gen::schedule_api_server::ScheduleApi;
        let hidden = services.set_schedule_state(request).await.expect_err("404");
        assert_eq!(hidden.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn execution_submit_honors_metadata_idempotency() {
        let (state, tenant, organization, project) = seed().await;
        let tenant_scoped = ServiceContext::for_service("seed", "grpc-seed-3", &Timestamp::now())
            .expect("ctx")
            .with_scope(tenant, organization);
        let workflow = state
            .workflows
            .create(&tenant_scoped, project, "g-flow")
            .await
            .expect("workflow");
        let services = GrpcServices::new(state);

        use gen::execution_api_server::ExecutionApi;
        let make = |meta: MetadataMap| {
            Request::from_parts(
                meta,
                tonic::Extensions::default(),
                gen::SubmitExecutionRequest {
                    project_id: project.to_string(),
                    workflow_id: Some(workflow.id.to_string()),
                    agent_id: None,
                    input_json: "{\"job\": \"g\"}".to_owned(),
                },
            )
        };
        let mut first_metadata = authed_metadata(tenant, organization);
        first_metadata.insert("idempotency-key", "grpc-idem-7".parse().expect("meta"));
        let first = services
            .submit_execution(make(first_metadata))
            .await
            .expect("submit");
        let first_ref = first.get_ref();
        assert!(!first_ref.replayed);
        assert_eq!(first_ref.correlation_id, "grpc-it-1", "correlation echoes");

        let mut replay_metadata = authed_metadata(tenant, organization);
        replay_metadata.insert("idempotency-key", "grpc-idem-7".parse().expect("meta"));
        let second = services
            .submit_execution(make(replay_metadata))
            .await
            .expect("replay");
        assert!(second.get_ref().replayed, "same idempotency key replays");
        assert_eq!(second.get_ref().execution_id, first_ref.execution_id);
    }
}
